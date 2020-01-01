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
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
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

    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;

    public TCPDiscoveryEngine(ControllerEngine controllerEngine)   {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        logger.trace("Initializing");

        //isSSL = plugin.getConfig().getBooleanParam("netdiscoveryssl",false);
        //enable when client supports
        isSSL = false;
        discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
        logger.error("Started DiscoveryTCPEngine");
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
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }
                            p.addLast(new IdleStateHandler(30,30,30));
                            p.addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new TCPDiscoveryEngineHandler(controllerEngine));
                        }
                    });

            controllerEngine.setTCPDiscoveryActive(true);

            // Bind and start to accept incoming connections.
            //b.bind(discoveryPort).sync().channel().closeFuture().sync();
            ChannelFuture cf = b.bind(discoveryPort).sync();

            cf.channel().closeFuture().sync();


        } catch(Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void shutdown() {
        try {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        } catch(Exception ex) {
            //logger.error(ex.getMessage());
            ex.printStackTrace();
        }
    }

}