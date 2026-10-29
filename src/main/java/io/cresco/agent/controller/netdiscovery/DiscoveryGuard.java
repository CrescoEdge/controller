package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Limits on what an UNAUTHENTICATED peer can make a discovery listener (or a discovery client reading an answer) spend.
 *
 * <p>Everything that arrives on the discovery port is parsed before the peer has proved anything, so every step is
 * bounded: the frame ({@link DiscoveryWire#MAX_OBJECT_BYTES}), the gunzipped {@code discovery_node}
 * ({@link #MAX_NODE_BYTES}, checked while inflating, before any JSON parse), how many connections one address and all
 * addresses may hold open ({@link Admission}), how long a connection may stay open or idle, and how often refused input
 * is logged. {@code MsgEvent.getCompressedParam} is NOT used here: it inflates up to 128 MiB, which one ~160 KB frame
 * reaches.
 */
public final class DiscoveryGuard {

    private DiscoveryGuard() { }

    /**
     * Largest gunzipped {@code discovery_node} accepted. A real one is a few KiB (measured: about 1 KiB without
     * certificates, about 5 KiB carrying both a 3072-bit leaf and a CA chain); the worst plausible case (two 3-certificate
     * chains of 8192-bit RSA) is under 30 KiB.
     */
    public static final int MAX_NODE_BYTES = 64 * 1024;

    /** Defaults for {@link Admission} (config keys in brackets). */
    public static final int DEFAULT_MAX_CONNECTIONS = 128;              // discovery_max_connections
    public static final int DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 16;   // discovery_max_connections_per_address
    public static final int DEFAULT_MAX_CONNECTION_SECONDS = 30;        // discovery_max_connection_seconds
    public static final int DEFAULT_IDLE_SECONDS = 10;                  // discovery_idle_seconds

    /** Most entries a UDP discovery client keeps from one listen window. */
    public static final int MAX_DISCOVERED_PER_ROUND = 256;

    /** Input that was refused because it is larger than allowed. */
    public static final class TooLarge extends IOException {
        TooLarge(String what) { super(what); }
    }

    /**
     * Base64-decode and gunzip {@code value}, refusing (with {@link TooLarge}) as soon as the output passes {@code cap}
     * bytes. Allocates at most {@code cap} plus one read buffer, whatever the compression ratio.
     */
    public static String inflate(String value, int cap) throws IOException {
        if (value == null) return null;
        byte[] gz;
        try {
            gz = Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IOException("not base64");
        }
        try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(gz), 4096)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(cap, Math.max(1024, gz.length * 4)));
            byte[] buf = new byte[8192];
            int total = 0, n;
            while ((n = in.read(buf, 0, Math.min(buf.length, cap + 1 - total))) > 0) {
                total += n;
                if (total > cap) throw new TooLarge("gunzipped discovery field is larger than " + cap + " bytes");
                out.write(buf, 0, n);
            }
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    /**
     * The {@code discovery_node} of a discovery message, inflated with the {@link #MAX_NODE_BYTES} cap; {@code null}
     * when the message carries none. Throws when it is too large or malformed.
     */
    public static DiscoveryNode discoveryNode(Gson gson, MsgEvent me) throws IOException {
        if (me == null) return null;
        String raw = me.getParam("discovery_node");
        if (raw == null) return null;
        String json = inflate(raw, MAX_NODE_BYTES);
        try {
            return gson.fromJson(json, DiscoveryNode.class);
        } catch (RuntimeException e) {
            throw new IOException("malformed discovery_node");
        }
    }

    /** The UTF-8 text of a datagram (only its received bytes), trimmed as the discovery code always did. */
    public static String datagramText(java.net.DatagramPacket p) {
        return new String(p.getData(), p.getOffset(), p.getLength(), StandardCharsets.UTF_8).trim();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Logging refused input: a flood of bad frames must not become a flood of log lines (or stack traces).

    private static final Map<String, long[]> LAST_LOG = new ConcurrentHashMap<>();   // fixed keys only
    private static final long LOG_EVERY_MS = 10_000;

    /**
     * Log {@code message} at WARN at most once per 10 s for {@code kind} (a fixed string, never peer input), with how
     * many were suppressed in between. Returns whether it logged.
     */
    public static boolean warnLimited(CLogger logger, String kind, String message) {
        long now = System.currentTimeMillis();
        long[] st = LAST_LOG.computeIfAbsent(kind, k -> new long[]{0L, 0L});
        synchronized (st) {
            if (now - st[0] < LOG_EVERY_MS) { st[1]++; return false; }
            long suppressed = st[1];
            st[0] = now; st[1] = 0;
            if (logger != null) logger.warn(message + (suppressed > 0 ? " (" + suppressed + " similar suppressed)" : ""));
            return true;
        }
    }

    /** The peer address of a channel, or a fixed key when it is not an IP socket (tests). */
    static String addressKey(SocketAddress a) {
        if (a instanceof InetSocketAddress) {
            InetAddress ia = ((InetSocketAddress) a).getAddress();
            if (ia != null) return ia.getHostAddress();
            return ((InetSocketAddress) a).getHostString();
        }
        return String.valueOf(a == null ? "?" : a.getClass().getSimpleName());
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * First handler of every discovery listener channel: refuses a connection over the per-address or total limit and
     * closes a connection open longer than the lifetime limit; its {@link #idleCloser()} closes an idle one. One instance
     * per listener, shared by its channels.
     */
    @ChannelHandler.Sharable
    public static final class Admission extends ChannelInboundHandlerAdapter {

        private static final AttributeKey<String> ADMITTED = AttributeKey.valueOf("cresco.discovery.admitted");

        private final int maxTotal, maxPerAddress;
        private final long maxLifetimeMs;
        private final CLogger logger;
        private final AtomicInteger total = new AtomicInteger();
        private final ConcurrentHashMap<String, AtomicInteger> perAddress = new ConcurrentHashMap<>();
        private final AtomicLong refused = new AtomicLong(), closedIdle = new AtomicLong(), closedLifetime = new AtomicLong();

        public Admission(int maxTotal, int maxPerAddress, long maxLifetimeMs, CLogger logger) {
            this.maxTotal = Math.max(1, maxTotal);
            this.maxPerAddress = Math.max(1, maxPerAddress);
            this.maxLifetimeMs = Math.max(1, maxLifetimeMs);
            this.logger = logger;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String key = addressKey(ctx.channel().remoteAddress());
            if (!admit(key)) {
                refused.incrementAndGet();
                warnLimited(logger, "discovery-admission", "Discovery: refused a connection from " + key + ": over the limit of "
                        + maxPerAddress + " per address / " + maxTotal + " in total (discovery_max_connections_per_address,"
                        + " discovery_max_connections)");
                ctx.close();
                return;
            }
            ctx.channel().attr(ADMITTED).set(key);
            ctx.executor().schedule(() -> {
                if (ctx.channel().isOpen()) {
                    closedLifetime.incrementAndGet();
                    warnLimited(logger, "discovery-lifetime", "Discovery: closed a connection from " + key + " open longer than "
                            + maxLifetimeMs / 1000 + " s");
                    ctx.close();
                }
            }, maxLifetimeMs, TimeUnit.MILLISECONDS);
            super.channelActive(ctx);
        }

        private boolean admit(String key) {
            if (total.incrementAndGet() > maxTotal) {
                total.decrementAndGet();
                return false;
            }
            AtomicInteger n = perAddress.computeIfAbsent(key, k -> new AtomicInteger());
            if (n.incrementAndGet() > maxPerAddress) {
                release(key);
                return false;
            }
            return true;
        }

        private void release(String key) {
            total.decrementAndGet();
            perAddress.computeIfPresent(key, (k, n) -> n.decrementAndGet() <= 0 ? null : n);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            String key = ctx.channel().attr(ADMITTED).getAndSet(null);
            if (key != null) release(key);
            super.channelInactive(ctx);
        }

        /** Place after the {@code IdleStateHandler}: closes the channel on its idle events (counted). */
        public ChannelHandler idleCloser() { return idleCloser; }

        private final ChannelHandler idleCloser = new CloseOnIdle(closedIdle);

        /** Open connections now (all addresses). */
        public int open() { return total.get(); }

        /** Open connections now from one address. */
        public int open(String address) {
            AtomicInteger n = perAddress.get(address);
            return n == null ? 0 : n.get();
        }

        /** Distinct addresses with an open connection (bounded by {@code maxTotal}). */
        public int addresses() { return perAddress.size(); }

        public long refused() { return refused.get(); }

        public long closedIdle() { return closedIdle.get(); }

        public long closedLifetime() { return closedLifetime.get(); }
    }

    /** Closes the channel on an {@link IdleStateEvent}; place it after the {@code IdleStateHandler}. */
    @ChannelHandler.Sharable
    public static final class CloseOnIdle extends ChannelInboundHandlerAdapter {
        /** For discovery client channels (they also have read/write timeouts). */
        public static final CloseOnIdle INSTANCE = new CloseOnIdle(new AtomicLong());

        private final AtomicLong closed;

        CloseOnIdle(AtomicLong closed) { this.closed = closed; }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                closed.incrementAndGet();
                ctx.close();
                return;
            }
            super.userEventTriggered(ctx, evt);
        }
    }
}
