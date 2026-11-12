package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleStateHandler;


public class TCPDiscoveryEngine implements Runnable {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private boolean isSSL = false;
    private int discoveryPort;
    private final DiscoveryGuard.Admission admission;
    private final int idleSeconds;
    private final DiscoveryJobs jobs;
    private final int maxRequestBytes;
    private static volatile DiscoveryJobs liveJobs;

    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;
    private static ChannelFuture cf;

    public TCPDiscoveryEngine(ControllerEngine controllerEngine)   {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        logger.trace("Initializing");

        //isSSL = plugin.getConfig().getBooleanParam("netdiscoveryssl",false);
        //enable when client supports
        isSSL = false;
        discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
        admission = admission(plugin, logger);
        idleSeconds = Math.max(1, plugin.getConfig().getIntegerParam("discovery_idle_seconds", DiscoveryGuard.DEFAULT_IDLE_SECONDS));
        maxRequestBytes = plugin.getConfig().getIntegerParam("discovery_max_request_bytes", DiscoveryWire.MAX_REQUEST_BYTES);
        jobs = jobs(plugin, logger);
        liveJobs = jobs;
        controllerEngine.setDiscoveryJobs(jobs, admission);
        final com.google.gson.Gson gson = new com.google.gson.Gson();
        // a connection over a limit is told to come back (in about the time the queue takes), not closed without a word
        admission.answerWhenRefused(() -> TCPDiscoveryEngineHandler.busyFrame(gson, plugin,
                Math.max(1_000L, jobs.serviceMicros() * Math.max(1, jobs.queued()) / 1_000L / jobs.workers()), "too many connections"));
        logger.info("Started DiscoveryTCPEngine");
    }

    public void run() {
        // Configure SSL.
        try {

            final SslContext sslCtx;
            if (isSSL) {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            } else {
                sslCtx = null;
            }


            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap b = new ServerBootstrap();

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(childInitializer(admission, idleSeconds, controllerEngine, sslCtx, jobs, maxRequestBytes));

            controllerEngine.setTCPDiscoveryActive(true);

            // Bind and start to accept incoming connections.
            //b.bind(discoveryPort).sync().channel().closeFuture().sync();
            cf = b.bind(discoveryPort).sync().channel().closeFuture().sync();

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }


    /** The listener's connection limits, from config (see {@link DiscoveryGuard}). */
    static DiscoveryGuard.Admission admission(PluginBuilder plugin, CLogger logger) {
        int total = plugin.getConfig().getIntegerParam("discovery_max_connections", DiscoveryGuard.DEFAULT_MAX_CONNECTIONS);
        int perAddress = plugin.getConfig().getIntegerParam("discovery_max_connections_per_address", DiscoveryGuard.DEFAULT_MAX_CONNECTIONS_PER_ADDRESS);
        int seconds = plugin.getConfig().getIntegerParam("discovery_max_connection_seconds", DiscoveryGuard.DEFAULT_MAX_CONNECTION_SECONDS);
        return new DiscoveryGuard.Admission(total, perAddress, seconds * 1000L, logger);
    }

    /**
     * The responder's channel pipeline: the connection limits first (nothing else is set up for a refused connection),
     * idle detection (a channel that reads nothing for idleSeconds is closed; one that trickles is closed by the
     * lifetime limit), the String-only codec (this port is unauthenticated, see
     * DiscoveryWire) and the handler.
     */
    static ChannelInitializer<SocketChannel> childInitializer(DiscoveryGuard.Admission admission, int idleSeconds,
                                                              ControllerEngine controllerEngine, SslContext sslCtx) {
        return childInitializer(admission, idleSeconds, controllerEngine, sslCtx, null);
    }

    /**
     * The responder's workers and its queue's bound, from config: {@code discovery_workers} (default a quarter of the
     * cpus, 2 to 16), {@code discovery_max_wait_ms} (default 5000: a request whose turn would come later is answered
     * "busy"); it holds at most as many requests as the listener holds connections, and at most
     * {@code discovery_max_queued_bytes_per_address} (32 MiB) of one address and {@code discovery_max_queued_bytes}
     * (256 MiB) in all -- and never more than a sixteenth of the heap in all, an eighth of that for one address.
     */
    static DiscoveryJobs jobs(PluginBuilder plugin, CLogger logger) {
        int cpus = Runtime.getRuntime().availableProcessors();
        int workers = plugin.getConfig().getIntegerParam("discovery_workers", Math.max(2, Math.min(16, cpus / 4)));
        int waitMs = plugin.getConfig().getIntegerParam("discovery_max_wait_ms", DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        int total = plugin.getConfig().getIntegerParam("discovery_max_connections", DiscoveryGuard.DEFAULT_MAX_CONNECTIONS);
        // (never more than a sixteenth of this JVM's heap in all, nor more than an eighth of that for one address)
        long bytes = DiscoveryJobs.maxBytesFor(plugin.getConfig().getLongParam("discovery_max_queued_bytes", DiscoveryJobs.DEFAULT_MAX_BYTES));
        long perAddress = DiscoveryJobs.maxBytesPerAddressFor(
                plugin.getConfig().getLongParam("discovery_max_queued_bytes_per_address", DiscoveryJobs.DEFAULT_MAX_BYTES_PER_ADDRESS), bytes);
        return new DiscoveryJobs(workers, waitMs, total, perAddress, bytes, logger);
    }

    static ChannelInitializer<SocketChannel> childInitializer(DiscoveryGuard.Admission admission, int idleSeconds,
                                                              ControllerEngine controllerEngine, SslContext sslCtx, DiscoveryJobs jobs) {
        return childInitializer(admission, idleSeconds, controllerEngine, sslCtx, jobs, DiscoveryWire.MAX_REQUEST_BYTES);
    }

    static ChannelInitializer<SocketChannel> childInitializer(DiscoveryGuard.Admission admission, int idleSeconds,
                                                              ControllerEngine controllerEngine, SslContext sslCtx, DiscoveryJobs jobs,
                                                              int maxRequestBytes) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(admission);
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc()));
                }
                // Nothing read AND nothing written for idleSeconds. Written too: a joiner's allowance for its next frame runs
                // from this responder's answer, not from the joiner's last frame -- the time its request waited in the queue
                // was taken from it (a hello that waited 1.5 s of a 2 s allowance left 0.5 s for the proof).
                p.addLast(new IdleStateHandler(0, 0, idleSeconds));
                p.addLast(admission.idleCloser());
                DiscoveryWire.install(p, maxRequestBytes);
                p.addLast(new TCPDiscoveryEngineHandler(controllerEngine, jobs));
            }
        };
    }

    public static void shutdown() {
        DiscoveryJobs j = liveJobs;
        if (j != null) j.shutdown();
        try {

            try {
                cf.channel().closeFuture().sync();
            } catch (Exception ex) {
                //eat
            }
            try {
                cf.channel().close();
            } catch (Exception ex) {
                //eat
            }
            try {
                cf.channel().parent().close();
            }catch (Exception ex) {
                //eat
            }

            workerGroup.shutdownGracefully().sync();
            while(!workerGroup.isShutdown()) {
                Thread.sleep(1000);
            }

            bossGroup.shutdownGracefully().sync();
            while(!bossGroup.isShutdown()) {
                Thread.sleep(1000);
            }


        } catch(Exception ex) {
            //logger.error(ex.getMessage());
            ex.printStackTrace();
        }
    }

}