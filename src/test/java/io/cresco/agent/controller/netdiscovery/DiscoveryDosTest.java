package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pre-authentication resource limits on the discovery listener and clients: bounded inflation of {@code discovery_node},
 * idle and long-lived connections closed, connection floods bounded, and discovery between old and new nodes unchanged.
 */
class DiscoveryDosTest {

    private static final Gson GSON = new Gson();
    private EventLoopGroup group;

    @BeforeEach
    void up() { group = new NioEventLoopGroup(2); }

    @AfterEach
    void down() throws Exception { group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync(); }

    /** Controller state of a test node: fixed identity, never active. */
    static final class State extends ControllerStateImp {
        final String region, agent;
        final boolean regional;
        State(PluginBuilder pb, String region, String agent, boolean regional) {
            super(pb, null); this.region = region; this.agent = agent; this.regional = regional;
        }
        @Override public ControllerMode getControllerState() { return regional ? ControllerMode.REGION : ControllerMode.AGENT; }
        @Override public boolean isActive() { return false; }
        @Override public boolean isFailed() { return true; }
        @Override public boolean isRegionalController() { return regional; }
        @Override public boolean isGlobalController() { return false; }
        @Override public String getRegion() { return region; }
        @Override public String getAgent() { return agent; }
        @Override public String getAgentPath() { return region + "_" + agent; }
    }

    static ControllerEngine engine(String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("discovery_secret_agent", "agent-secret");
        if (extra != null) cfg.putAll(extra);
        String dir = Files.createTempDirectory("dos-" + agent).toString();
        cfg.put("public_key_directory", dir + "/public-keys");
        PluginBuilder pb = TestAgentService.plugin(dir, cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = new State(pb, region, agent, regional);
        return e;
    }

    private static long allocated() {
        return ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean()).getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    /** gzip of {@code mib} MiB of one repeated byte, streamed (never held uncompressed), base64'd as MsgEvent does. */
    private static String bomb(int mib) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] chunk = new byte[1 << 20];
        java.util.Arrays.fill(chunk, (byte) 'A');
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write("{\"discovery_type\":\"AGENT\",\"broadcast_validator\":\"".getBytes());
            for (int i = 0; i < mib; i++) gz.write(chunk);
            gz.write("\"}".getBytes());
        }
        return Base64.getEncoder().encodeToString(bos.toByteArray());
    }

    private static String frameWith(String compressedNode) {
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, "x", "y", "z", "pre-auth");
        me.setParam("discovery_node", compressedNode);
        return GSON.toJson(me);
    }

    /** The third review's measurement (on main's API): one ~160 KB frame used to allocate ~741 MiB before any check. */
    @Test
    void oneUnauthenticatedDiscoveryFrameCannotAllocateHundredsOfMegabytes() throws Exception {
        ControllerEngine responder = engine("rgA", "rc", true, null);
        String frame = frameWith(bomb(120));
        System.out.println("frame size (what the attacker sends) = " + frame.length() + " bytes");
        TCPDiscoveryEngineHandler h = new TCPDiscoveryEngineHandler(responder);
        System.gc();
        long a0 = allocated();
        try { h.channelRead(null, frame); } catch (Exception ignore) { /* null ctx */ }
        long mb = (allocated() - a0) >> 20;
        System.out.println("allocated by one pre-auth frame on the responder: " + mb + " MiB");
        assertTrue(frame.length() < DiscoveryWire.MAX_OBJECT_BYTES);
        assertTrue(mb < 8, "one unauthenticated discovery frame of " + frame.length() + " bytes made the responder allocate " + mb + " MiB");
    }

    @Test
    void aGzipBombIsRefusedWhileInflatingAndTheConnectionIsClosed() throws Exception {
        String b = bomb(256);
        long a0 = allocated();
        assertThrows(DiscoveryGuard.TooLarge.class, () -> DiscoveryGuard.inflate(b, DiscoveryGuard.MAX_NODE_BYTES));
        long kib = (allocated() - a0) >> 10;
        System.out.println("bounded inflate of a 256 MiB bomb (" + b.length() + " bytes base64) allocated " + kib + " KiB");
        assertTrue(kib < 4096, "bounded inflate allocated " + kib + " KiB");

        // a bomb split over many gzip members is bounded the same way
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 40; i++) {
            try (GZIPOutputStream gz = new GZIPOutputStream(new OutputStream() {
                @Override public void write(int x) { bos.write(x); }
                @Override public void write(byte[] x, int o, int l) { bos.write(x, o, l); }
            })) { gz.write(new byte[8192]); }
        }
        assertThrows(DiscoveryGuard.TooLarge.class,
                () -> DiscoveryGuard.inflate(Base64.getEncoder().encodeToString(bos.toByteArray()), DiscoveryGuard.MAX_NODE_BYTES));

        // through the responder's handler: the channel is closed and nothing is answered
        ControllerEngine responder = engine("rgA", "rc", true, null);
        EmbeddedChannel ch = new EmbeddedChannel(new TCPDiscoveryEngineHandler(responder));
        ch.writeInbound(frameWith(bomb(120)));
        assertFalse(ch.isOpen(), "the responder kept a connection open after a gzip bomb");
        assertNull(ch.readOutbound());
        // malformed input (not base64, not gzip, not JSON) is refused the same way, without a stack trace per frame
        for (String bad : new String[]{"%%%", Base64.getEncoder().encodeToString("plain".getBytes())}) {
            EmbeddedChannel c2 = new EmbeddedChannel(new TCPDiscoveryEngineHandler(responder));
            c2.writeInbound(frameWith(bad));
            assertFalse(c2.isOpen());
        }
    }

    @Test
    void aRealDiscoveryNodeIsFarBelowTheCap() throws Exception {
        ControllerEngine client = engine("rgA", "a1", false, null);
        client.setCertificateManager(new CertificateManager(client));
        DiscoveryProcessor p = new DiscoveryProcessor(client);
        DiscoveryNode plain = p.generateBroadCastDiscovery(DiscoveryType.AGENT, false);
        DiscoveryNode certified = p.generateBroadCastDiscovery(DiscoveryType.AGENT, true);
        assertNotNull(certified.discover_cert);
        // the answer carries the responder's chain too: two chains in one node
        certified.discovered_cert = certified.discover_cert;
        certified.setDiscovered("10.0.0.1", 32005, "10.0.0.2", 40000, "rgA", "rc", 3, "v");
        certified.discover_cert = client.getCertificateManager().getJsonFromCerts(client.getCertificateManager().getPublicCertificate());
        int plainBytes = GSON.toJson(plain).length(), certBytes = GSON.toJson(certified).length();
        System.out.println("discovery_node sizes: no certificate = " + plainBytes + " bytes, two certificate chains = " + certBytes
                + " bytes; cap = " + DiscoveryGuard.MAX_NODE_BYTES);
        assertTrue(certBytes * 4 < DiscoveryGuard.MAX_NODE_BYTES, "the cap leaves less than 4x headroom over a real node");
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, "rgA", "a1", "p", "req");
        me.setCompressedParam("discovery_node", GSON.toJson(certified));
        DiscoveryNode back = DiscoveryGuard.discoveryNode(GSON, me);
        assertEquals(certified.discover_cert, back.discover_cert);
        assertEquals(certified.discovered_cert, back.discovered_cert);
    }

    // -- real sockets ---------------------------------------------------------------------------------------------

    private Channel listen(ControllerEngine e, DiscoveryGuard.Admission a, int idleSeconds) throws Exception {
        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(TCPDiscoveryEngine.childInitializer(a, idleSeconds, e, null))
                .bind("127.0.0.1", 0).sync().channel();
    }

    private static int port(Channel server) { return ((InetSocketAddress) server.localAddress()).getPort(); }

    /** true when the peer closed {@code s} (EOF), false when it is still open after {@code waitMs}. */
    private static boolean closedByPeer(Socket s, int waitMs) throws Exception {
        s.setSoTimeout(waitMs);
        try {
            return s.getInputStream().read() == -1;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (java.io.IOException e) {
            return true;                                            // reset
        }
    }

    private static void eventually(java.util.function.BooleanSupplier cond, long ms, String what) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > end) fail(what);
            Thread.sleep(20);
        }
    }

    @Test
    void idleAndLongLivedConnectionsAreClosed() throws Exception {
        ControllerEngine e = engine("rgA", "rc", true, null);
        DiscoveryGuard.Admission a = new DiscoveryGuard.Admission(100, 100, 2500, null);
        Channel server = listen(e, a, 1);
        try (Socket idle = new Socket("127.0.0.1", port(server)); Socket trickle = new Socket("127.0.0.1", port(server))) {
            long t0 = System.currentTimeMillis();
            java.util.concurrent.atomic.AtomicLong trickleClosedAt = new java.util.concurrent.atomic.AtomicLong();
            Thread sender = new Thread(() -> {
                try {
                    OutputStream out = trickle.getOutputStream();
                    out.write(new byte[]{0, 2, 0, 0});             // a 128 KiB frame announced, then a byte every 200 ms
                    out.flush();
                    while (!closedByPeer(trickle, 200)) { out.write(5); out.flush(); }
                } catch (Exception gone) { /* closed */ }
                trickleClosedAt.set(System.currentTimeMillis());
            });
            sender.start();
            assertTrue(closedByPeer(idle, 4000), "an idle discovery connection was not closed");
            long idleMs = System.currentTimeMillis() - t0;
            sender.join(8000);
            long lifeMs = trickleClosedAt.get() - t0;
            System.out.println("idle connection closed after " + idleMs + " ms; never-idle trickle closed after " + lifeMs + " ms");
            assertTrue(trickleClosedAt.get() > 0, "a connection that trickles a partial frame stayed open past the lifetime limit");
            assertTrue(lifeMs >= 2400, "the trickle was closed before the lifetime limit: " + lifeMs + " ms");
            eventually(() -> a.open() == 0, 2000, "connection count not released: " + a.open());
            assertTrue(a.closedIdle() >= 1);
            assertTrue(a.closedLifetime() >= 1);
        } finally {
            server.close().sync();
        }
    }

    @Test
    void aConnectionFloodIsBoundedPerAddressAndInTotal() throws Exception {
        ControllerEngine e = engine("rgA", "rc", true, null);
        for (int[] lim : new int[][]{{1000, 4}, {6, 1000}}) {
            DiscoveryGuard.Admission a = new DiscoveryGuard.Admission(lim[0], lim[1], 30_000, null);
            Channel server = listen(e, a, 30);
            List<Socket> flood = new ArrayList<>();
            try {
                for (int i = 0; i < 24; i++) flood.add(new Socket("127.0.0.1", port(server)));
                int allowed = Math.min(lim[0], lim[1]);
                eventually(() -> a.refused() == 24 - allowed, 3000, "refused " + a.refused() + " of " + (24 - allowed));
                int open = 0;
                for (Socket s : flood) if (!closedByPeer(s, 100)) open++;
                System.out.println("limits total=" + lim[0] + " perAddress=" + lim[1] + ": " + open + " of 24 flood connections held open");
                assertEquals(allowed, open);
                assertEquals(allowed, a.open());
                assertEquals(allowed, a.open("127.0.0.1"));
                for (Socket s : flood) s.close();
                eventually(() -> a.open() == 0 && a.addresses() == 0, 3000, "slots not released: " + a.open());
                // the slots are usable again
                try (Socket again = new Socket("127.0.0.1", port(server))) {
                    assertFalse(closedByPeer(again, 200), "a connection after the flood ended was refused");
                }
            } finally {
                for (Socket s : flood) s.close();
                server.close().sync();
            }
        }
    }

    @Test
    void oldAndNewNodesStillDiscoverEachOther() throws Exception {
        ControllerEngine responder = engine("rgA", "rc", true, null);
        DiscoveryGuard.Admission a = new DiscoveryGuard.Admission(16, 4, 30_000, null);
        Channel server = listen(responder, a, 10);
        try {
            ControllerEngine joiner = engine("rgA", "a1", false, null);

            // a new node (this code) as the client
            List<DiscoveryNode> found = new TCPDiscoveryStatic(joiner).discover(DiscoveryType.AGENT, 3000, "127.0.0.1", port(server), false);
            assertEquals(1, found.size(), "a new client did not discover the responder");
            assertEquals("rc", found.get(0).discovered_agent);

            // an old node: Netty's own ObjectEncoder/ObjectDecoder and the library's getCompressedParam
            LinkedBlockingQueue<String> answers = new LinkedBlockingQueue<>();
            DiscoveryNode req = new DiscoveryProcessor(joiner).generateBroadCastDiscovery(DiscoveryType.AGENT, false);
            MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, "rgA", "a1", "p", "Discovery request.");
            me.setCompressedParam("discovery_node", GSON.toJson(req));
            Channel old = new Bootstrap().group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ObjectEncoder(), new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                            new ChannelInboundHandlerAdapter() {
                                @Override public void channelRead(ChannelHandlerContext ctx, Object msg) { answers.add((String) msg); }
                            });
                }
            }).connect("127.0.0.1", port(server)).sync().channel();
            old.writeAndFlush(GSON.toJson(me)).sync();
            String answer = answers.poll(5, TimeUnit.SECONDS);
            assertNotNull(answer, "an old client got no answer");
            DiscoveryNode got = GSON.fromJson(GSON.fromJson(answer, MsgEvent.class).getCompressedParam("discovery_node"), DiscoveryNode.class);
            assertEquals("rc", got.discovered_agent);
            assertTrue(new DiscoveryProcessor(joiner).isValidatedAuthenication(got), "the old client could not validate the answer");
            old.close().sync();
        } finally {
            server.close().sync();
        }
    }

    @Test
    void aClientRefusesAnOversizedAnswerAndAChainOfMoreThanThreeCertificates() throws Exception {
        // a "responder" that answers with a bomb: the client inflates at most the cap and finds nothing
        ControllerEngine joiner = engine("rgA", "a1", false, null);
        String bombFrame = frameWith(bomb(64));
        Channel evil = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override protected void initChannel(SocketChannel ch) {
                DiscoveryWire.install(ch.pipeline());
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) { ctx.writeAndFlush(bombFrame); }
                });
            }
        }).bind("127.0.0.1", 0).sync().channel();
        try {
            List<DiscoveryNode> found = new TCPDiscoveryStatic(joiner).discover(DiscoveryType.AGENT, 3000, "127.0.0.1", port(evil), false);
            assertTrue(found.isEmpty());
        } finally {
            evil.close().sync();
        }

        joiner.setCertificateManager(new CertificateManager(joiner));
        CertificateManager cm = joiner.getCertificateManager();
        String one = cm.getJsonFromCerts(cm.getPublicCertificate());
        String[] c = GSON.fromJson(one, String[].class);
        String four = GSON.toJson(new String[]{c[0], c[0], c[0], c[0]});
        assertNull(cm.getCertsfromJson(four), "a chain of four certificates was accepted");
        assertNotNull(cm.getCertsfromJson(one)[0]);
        assertNull(cm.getCertsfromJson("not json ["));
    }
}
