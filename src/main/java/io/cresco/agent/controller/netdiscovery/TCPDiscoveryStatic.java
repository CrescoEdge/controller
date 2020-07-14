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
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TCPDiscoveryStatic {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

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

            int idleTimeout = (discoveryTimeout/1000) * 2;

            EventLoopGroup group = new NioEventLoopGroup(1);
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ChannelPipeline p = ch.pipeline()
                                        //.addFirst("write_timeout", new WriteTimeoutHandler(discoveryTimeout, TimeUnit.MILLISECONDS))
                                        //.addFirst("read_timeout", new ReadTimeoutHandler(discoveryTimeout, TimeUnit.MILLISECONDS))
                                        .addLast(new IdleStateHandler(idleTimeout,idleTimeout,idleTimeout));

                                /*
                                if (sslCtx != null) {
                                    p.addLast(sslCtx.newHandler(ch.alloc(), hostAddress, discoveryPort));
                                }

                                */
                                p.addLast(
                                        new ObjectEncoder(),
                                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                        new TCPDiscoveryStaticHandler(controllerEngine, discoveredList, disType, hostAddress, discoveryPort, sendCert));
                            }
                        });

                //this is the connection timeout
                b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, discoveryTimeout);
                b.option(ChannelOption.SO_KEEPALIVE, true);
                for (Map.Entry<ChannelOption<?>, Object> option : b.register().channel().config().getOptions().entrySet()) {
                    System.out.println("Option [" + option.getKey().name() + "]: " + option.getValue().toString());
                }

                // Start the connection attempt.
                b.connect(hostAddress, discoveryPort).sync().channel().closeFuture().sync();

            } finally {
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
