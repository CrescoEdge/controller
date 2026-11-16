package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TCPDiscoveryStatic {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    /** What became of one discovery attempt at a responder. */
    public enum Outcome {
        /** it answered and the exchange completed */ ANSWERED,
        /** it said it is busy and after how long to ask again */ BUSY,
        /** it answered, and the exchange did not complete (refused, or not authentic: the reason is logged where it was seen) */ INCOMPLETE,
        /** the connection opened and nothing came back before it closed or timed out */ NO_ANSWER,
        /** no connection: nothing listens there, or it cannot be reached */ UNREACHABLE
    }

    /** The last attempt of this instance: what a caller needs for one clear line and for when to try again. */
    public static final class Attempt {
        public volatile Outcome outcome = Outcome.NO_ANSWER;
        public volatile String host = "?";
        public volatile int port;
        public volatile long elapsedMs;
        /** {@link Outcome#BUSY}: the milliseconds the responder asked for; else -1. */
        public volatile long busyRetryMs = -1;
        /** {@link Outcome#UNREACHABLE}: why. */
        public volatile String detail;
        volatile boolean heard;
    }

    private volatile Attempt last = new Attempt();

    /** What became of this instance's last {@code discover} call. */
    public Attempt lastAttempt() { return last; }

    public TCPDiscoveryStatic(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryStatic.class.getName(),CLogger.Level.Info);
    }

    public List<DiscoveryNode> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress, boolean sendCert) throws Exception {
        int discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
        return discover(disType, discoveryTimeout, hostAddress, discoveryPort, sendCert);
    }

    public List<DiscoveryNode> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress, int discoveryPort, Boolean sendCert) throws Exception  {
        if (!Boolean.TRUE.equals(sendCert)) {
            List<DiscoveryNode> found = discover(disType, discoveryTimeout, hostAddress, discoveryPort, false, false, null, false);
            if ((found == null || found.isEmpty()) && DiscoveryAuth.Mode.of(plugin.getConfig()) == DiscoveryAuth.Mode.COMPAT
                    && disType == DiscoveryType.AGENT) {
                // the upgrade window: a responder that is not upgraded answers only an earlier build's locator
                found = discover(disType, discoveryTimeout, hostAddress, discoveryPort, false, false, null, true);
                if (found != null && !found.isEmpty()) {
                    logger.warn("Discovery: " + hostAddress + ":" + discoveryPort + " answered only the LEGACY locator (it is not"
                            + " upgraded); using it because the upgrade window is open");
                }
            }
            return found;
        }
        // certificate exchanges are discovery v2 (mutually authenticated, replay-proof); a responder that only speaks
        // the legacy exchange is used only with discovery_auth=compat
        java.util.concurrent.atomic.AtomicBoolean legacyPeer = new java.util.concurrent.atomic.AtomicBoolean();
        List<DiscoveryNode> found = discover(disType, discoveryTimeout, hostAddress, discoveryPort, true, true, legacyPeer, false);
        if (legacyPeer.get()) {
            DiscoveryAuth.Mode mode = DiscoveryAuth.Mode.of(plugin.getConfig());
            if (mode == DiscoveryAuth.Mode.COMPAT && disType == DiscoveryType.AGENT) {
                logger.warn("Discovery: " + hostAddress + ":" + discoveryPort + " speaks only the LEGACY certificate exchange;"
                        + " using it because discovery_auth=compat (upgrade that node, then remove compat)");
                return discover(disType, discoveryTimeout, hostAddress, discoveryPort, true, false, null, true);
            }
            logger.error("Discovery: " + hostAddress + ":" + discoveryPort + " speaks only the legacy, unauthenticated certificate"
                    + " exchange; refusing it. Upgrade that node, or set discovery_auth=compat on this one during the upgrade");
        }
        return found;
    }

    private List<DiscoveryNode> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress, int discoveryPort, boolean sendCert,
                                         boolean v2, java.util.concurrent.atomic.AtomicBoolean legacyPeer, boolean legacyFormat) throws Exception {
        final Attempt attempt = new Attempt();
        attempt.host = hostAddress;
        attempt.port = discoveryPort;
        last = attempt;
        final long started = System.nanoTime();
        boolean connected = false;
        // Configure SSL.

        final List<DiscoveryNode> discoveredList = new ArrayList<>();

        //boolean isSSL = false;

        //don't remove to allow for ssl discovery
        /*
            final SslContext sslCtx;
            if (isSSL) {
                sslCtx = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } else {
                sslCtx = null;
            }
        */

            //int idleTimeout = (discoveryTimeout/1000) * 2;
            //int idleTimeout = discoveryTimeout;
            int idleTimeout = discoveryTimeout * 2;
            int writeTimeout = discoveryTimeout * 2;
            int readTimeout = discoveryTimeout * 2;

            EventLoopGroup group = new NioEventLoopGroup(1);
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ChannelPipeline p = ch.pipeline()
                                        .addFirst(new WriteTimeoutHandler(writeTimeout, TimeUnit.MILLISECONDS))
                                        .addFirst(new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS))
                                        .addLast(new IdleStateHandler(idleTimeout,idleTimeout,idleTimeout))
                                        .addLast(DiscoveryGuard.CloseOnIdle.INSTANCE);

                                /*
                                if (sslCtx != null) {
                                    p.addLast(sslCtx.newHandler(ch.alloc(), hostAddress, discoveryPort));
                                }

                                */
                                // String-only decoding: a responder (or anyone answering in its place) is unauthenticated
                                DiscoveryWire.install(p);
                                p.addLast(new TCPDiscoveryStaticHandler(controllerEngine, discoveredList, disType, hostAddress, discoveryPort, sendCert, v2, legacyPeer)
                                        .legacyFormat(legacyFormat).attempt(attempt));
                            }
                        });

                //this is the connection timeout
                b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, discoveryTimeout);
                b.option(ChannelOption.SO_KEEPALIVE, true);

                /*
                for (Map.Entry<ChannelOption<?>, Object> option : b.register().channel().config().getOptions().entrySet()) {
                    System.out.println("Option [" + option.getKey().name() + "]: " + option.getValue().toString());
                }
                 */

                // Start the connection attempt.
                io.netty.channel.Channel ch;
                try {
                    ch = b.connect(hostAddress, discoveryPort).sync().channel();
                } catch (Exception notConnected) {
                    attempt.outcome = Outcome.UNREACHABLE;
                    attempt.detail = notConnected.getMessage();
                    throw notConnected;
                }
                connected = true;
                ch.closeFuture().sync();

            } finally {
                attempt.elapsedMs = (System.nanoTime() - started) / 1_000_000L;
                if (connected) {
                    attempt.outcome = !discoveredList.isEmpty() ? Outcome.ANSWERED : attempt.busyRetryMs >= 0 ? Outcome.BUSY
                            : attempt.heard ? Outcome.INCOMPLETE : Outcome.NO_ANSWER;
                }
                group.shutdownGracefully();
            }

        return discoveredList;
    }

    public List<DiscoveryNode> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress) {
        List<DiscoveryNode> dList = null;
        try {

            dList = discover(disType, discoveryTimeout, hostAddress, false);

        } catch(Exception ex) {
            logger.error("discover() " + ex.getMessage());
            //StringWriter errors = new StringWriter();
            //ex.printStackTrace(new PrintWriter(errors));
            //logger.error(errors.toString());
        }
        return dList;
    }

}
