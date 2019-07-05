package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TCPDiscoveryStatic {
    //private static final Logger logger = LoggerFactory.getLogger(UDPDiscoveryStatic.class);

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public TCPDiscoveryStatic(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryStatic.class.getName(),CLogger.Level.Info);
    }

    public List<MsgEvent> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress, Boolean sendCert) throws Exception  {
        // Configure SSL.

        List<MsgEvent> discoveredList = new ArrayList<>();

        //boolean isSSL = plugin.getConfig().getBooleanParam("netdiscoveryssl",false);
        boolean isSSL = false;
        int discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);

        if(serverListening(hostAddress,discoveryPort)) {

            final SslContext sslCtx;
            if (isSSL) {
                sslCtx = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } else {
                sslCtx = null;
            }

            EventLoopGroup group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ChannelPipeline p = ch.pipeline()
                                        .addFirst("write_timeout", new WriteTimeoutHandler(discoveryTimeout, TimeUnit.MILLISECONDS))
                                        .addFirst("read_timeout", new ReadTimeoutHandler(discoveryTimeout, TimeUnit.MILLISECONDS));
                                if (sslCtx != null) {
                                    p.addLast(sslCtx.newHandler(ch.alloc(), hostAddress, discoveryPort));
                                }
                                p.addLast(
                                        new ObjectEncoder(),
                                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                        new TCPDiscoveryStaticHandler(controllerEngine, discoveredList, disType, hostAddress, discoveryPort, sendCert));
                            }
                        });

                // Start the connection attempt.
                b.connect(hostAddress, discoveryPort).sync().channel().closeFuture().sync();

            } finally {
                group.shutdownGracefully();
            }
        }
        return discoveredList;
    }

    public List<MsgEvent> discover(DiscoveryType disType, int discoveryTimeout, String hostAddress) {
        List<MsgEvent> dList = null;
        try {
            dList = discover(disType, discoveryTimeout, hostAddress, false);

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return dList;
    }

    private boolean serverListening(String host, int port)
    {
        Socket s = null;
        try
        {
            s = new Socket(host, port);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
        finally
        {
            if(s != null)
                try {s.close();}
                catch(Exception e){}
        }
    }


}
