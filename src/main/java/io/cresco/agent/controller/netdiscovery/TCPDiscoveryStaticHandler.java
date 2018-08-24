package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.List;

public class TCPDiscoveryStaticHandler extends ChannelInboundHandlerAdapter {

    private List<MsgEvent> discoveredList;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DiscoveryType disType;
    private String hostAddress;
    private int discoveryPort;
    private boolean sendCert;
    private DiscoveryCrypto discoveryCrypto;



    public TCPDiscoveryStaticHandler(ControllerEngine controllerEngine, List<MsgEvent> discoveredList, DiscoveryType disType, String hostAddress, int discoveryPort, boolean sendCert) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryStaticHandler.class.getName(),CLogger.Level.Info);
        this.discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        this.discoveredList = discoveredList;
        this.disType = disType;
        this.hostAddress = hostAddress;
        this.discoveryPort = discoveryPort;
        this.sendCert = sendCert;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the first message if this handler is a client-side handler.
        MsgEvent me = genDiscoverMsg();
        if(me != null) {
            //send initial message
            ctx.writeAndFlush(genDiscoverMsg());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        MsgEvent rme = (MsgEvent)msg;
        String host = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
        discoveredList.add(rme);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
        discoveredList = null;
    }

    public MsgEvent genDiscoverMsg() {
        MsgEvent sme = null;
        try {

            sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");
            sme.setParam("discover_ip", hostAddress);
            sme.setParam("src_region", this.plugin.getRegion());
            sme.setParam("src_agent", this.plugin.getAgent());
            if(sendCert) {
                sme.setParam("public_cert", controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()));
            }
            if (disType == DiscoveryType.AGENT || disType == DiscoveryType.REGION || disType == DiscoveryType.GLOBAL) {
                logger.trace("Discovery Type = {}", disType.name());
                sme.setParam("discovery_type", disType.name());
            } else {
                logger.trace("Discovery type unknown");
                sme = null;
            }
            //set for static discovery
            sme.setParam("discovery_static_agent","true");

            //set crypto message for discovery
            sme.setParam("discovery_validator",generateValidateMessage(sme));



        } catch (Exception ex) {
            logger.error("TCPDiscoveryStatic discover Error: " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("TCPDiscoveryStatic discover Dump: " + errors);
        }

        return sme;
    }

    private String generateValidateMessage(MsgEvent sme) {
        String encryptedString = null;
        try {

            String discoverySecret = null;
            if (sme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
            }

            String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
            encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return encryptedString;
    }


}
