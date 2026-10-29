package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;

public class TCPDiscoveryEngineHandler extends ChannelInboundHandlerAdapter {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private DiscoveryProcessor discoveryProcessor;
    private DiscoveryAuth.Msg hello, challenge;             // this connection's v2 exchange

    public TCPDiscoveryEngineHandler(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngineHandler.class.getName(),CLogger.Level.Info);
        this.gson = new Gson();
        this.discoveryProcessor = new DiscoveryProcessor(controllerEngine);

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        DiscoveryNode discoveryNode = null;
        try {
            MsgEvent rme = gson.fromJson((String)msg, MsgEvent.class);
            if (rme != null && rme.getParam(DiscoveryAuth.PARAM) != null) {
                readV2(ctx, rme.getParam(DiscoveryAuth.PARAM));
                return;
            }
            if(rme != null) {
                if(rme.paramsContains("discovery_node")) {
                    if (rme.getParam("discovery_node") != null) {
                        discoveryNode = gson.fromJson(rme.getCompressedParam("discovery_node"), DiscoveryNode.class);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(" failed to marshal discovery {}" + ex.getMessage());
        }

        if (discoveryNode != null) {

            String localAddress = ((InetSocketAddress)ctx.channel().localAddress()).getAddress().getHostAddress();
            int localPort = ((InetSocketAddress)ctx.channel().localAddress()).getPort();
            String remoteAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
            int remotePort = ((InetSocketAddress)ctx.channel().remoteAddress()).getPort();


            discoveryNode = discoveryProcessor.processIncomingBroadCastDiscovery(discoveryNode, localAddress, localPort, remoteAddress, remotePort);

            MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Static discovery response.");
            me.setCompressedParam("discovery_node",gson.toJson(discoveryNode));

            ctx.write(gson.toJson(me));


            //MsgEvent me = gson.fromJson((String)msg, MsgEvent.class);
            //    MsgEvent rme = processMessage(me);
            //ctx.write(gson.toJson(rme));
        }

    }

    private void readV2(ChannelHandlerContext ctx, String json) {
        String remote = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        DiscoveryAuth.Msg m;
        try {
            m = gson.fromJson(json, DiscoveryAuth.Msg.class);
        } catch (Exception e) {
            ctx.close();
            return;
        }
        DiscoveryAuth.Msg reply = null;
        boolean last = true;
        if (m != null && "hello".equals(m.step) && hello == null) {
            reply = discoveryProcessor.serverChallenge(m, remote);
            if (reply != null) { hello = m; challenge = reply; last = false; }
        } else if (m != null && "proof".equals(m.step) && hello != null && challenge != null) {
            reply = discoveryProcessor.serverResult(hello, challenge, m, remote);
            hello = null; challenge = null;                  // one proof per challenge
        }
        if (reply == null) { ctx.close(); return; }
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Discovery v2.");
        me.setParam(DiscoveryAuth.PARAM, gson.toJson(reply));
        if (last) ctx.writeAndFlush(gson.toJson(me)).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        else ctx.writeAndFlush(gson.toJson(me));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0");
        ctx.flush();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0.0");
        //ctx.fireChannelInactive();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0.1");
        //ctx.close();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 1");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("exeception Thread" + Thread.currentThread() + " 0");
        logger.error("TCPDiscoveryEngineHandler.exceptionCaught", cause);
        ctx.close();
        logger.error("exeception Thread" + Thread.currentThread() + " 1");
    }


}
