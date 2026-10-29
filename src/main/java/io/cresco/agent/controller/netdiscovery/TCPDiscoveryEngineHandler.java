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
            // pre-auth: the frame is bounded by DiscoveryWire, the gunzipped node by DiscoveryGuard (never the
            // 128 MiB MsgEvent.getCompressedParam cap)
            MsgEvent rme = gson.fromJson((String)msg, MsgEvent.class);
            discoveryNode = DiscoveryGuard.discoveryNode(gson, rme);
        } catch (Exception ex) {
            DiscoveryGuard.warnLimited(logger, "discovery-tcp-refused", "Discovery: refused a request from "
                    + remote(ctx) + ": " + ex.getMessage());
            if (ctx != null) ctx.close();
            return;
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

    private static String remote(ChannelHandlerContext ctx) {
        return ctx == null ? "?" : DiscoveryGuard.addressKey(ctx.channel().remoteAddress());
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
        // pre-auth input (an oversized or refused frame): one rate-limited line, no stack trace per peer frame
        DiscoveryGuard.warnLimited(logger, "discovery-tcp-error", "Discovery: closed a connection from " + remote(ctx)
                + ": " + cause.getClass().getSimpleName() + ": " + cause.getMessage());
        logger.debug("TCPDiscoveryEngineHandler.exceptionCaught", cause);
        ctx.close();
    }


}
