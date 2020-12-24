package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.List;

public class TCPDiscoveryStaticHandler extends ChannelInboundHandlerAdapter {

    private List<DiscoveryNode> discoveredList;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DiscoveryType disType;
    private String hostAddress;
    private int discoveryPort;
    private boolean sendCert;
    private Gson gson;
    private DiscoveryProcessor discoveryProcessor;

    public TCPDiscoveryStaticHandler(ControllerEngine controllerEngine, List<DiscoveryNode> discoveredList, DiscoveryType disType, String hostAddress, int discoveryPort, boolean sendCert) {

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryStaticHandler.class.getName(),CLogger.Level.Info);
        this.discoveredList = discoveredList;
        this.disType = disType;
        this.hostAddress = hostAddress;
        this.discoveryPort = discoveryPort;
        this.sendCert = sendCert;
        this.gson = new Gson();
        this.discoveryProcessor = new DiscoveryProcessor(controllerEngine);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {

        // Send the first message if this handler is a client-side handler.
        MsgEvent me = genDiscoverMsg();

        //MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, "src_agent","src_agent",null,"dst_region","dst_agent",null,true,true);
        if(me != null) {
            //send initial message
            ctx.writeAndFlush(gson.toJson(me));
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String remoteHost = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
        processIncoming((String)msg, remoteHost);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
        ctx.close();
        //ctx.fireChannelInactive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //ctx.close();
        //System.out.println("channelInactive Thread" + Thread.currentThread() + " 0");
        //ctx.close();
        //System.out.println("channelInactive Thread" + Thread.currentThread() + " 1");
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


            DiscoveryNode discoveryNode = discoveryProcessor.generateBroadCastDiscovery(disType, sendCert);

            if(discoveryNode != null) {
                sme.setCompressedParam("discovery_node", gson.toJson(discoveryNode));
            }

            /*
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
            if(sme != null) {
                //set for static discovery
                sme.setParam("discovery_static_agent", "true");
                //set crypto message for discovery
                sme.setParam("discovery_validator", generateValidateMessage(sme));
            }
             */

        } catch (Exception ex) {
            logger.error("TCPDiscoveryStatic discover Error: " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("TCPDiscoveryStatic discover Dump: " + errors);
        }

        return sme;
    }

    private synchronized void processIncoming(String json, String remoteAddress) {

            try {
                logger.trace("Incoming JSON: " + json);
                MsgEvent me = gson.fromJson(json, MsgEvent.class);
                DiscoveryNode discoveryNode = null;
                if (me != null) {

                    if(me.paramsContains("discovery_node")) {
                        discoveryNode = gson.fromJson(me.getCompressedParam("discovery_node"), DiscoveryNode.class);
                        logger.trace("discovery_node_json: " + me.getCompressedParam("discovery_node"));
                        if(discoveryNode != null) {
                            logger.info("Discovery Node Found: " + discoveryNode.discovered_ip + " latency: " + discoveryNode.getDiscoveryLatency());

                            if (remoteAddress.contains("%")) {
                                String[] remoteScope = remoteAddress.split("%");
                                remoteAddress = remoteScope[0];
                            }

                            if (discoveryNode.discovered_ip != null) {

                                logger.trace("discovered_ip: " + discoveryNode.discovered_ip);
                                logger.trace("remoteAddress: " + remoteAddress);
                                if(discoveryNode.discovered_ip.equals(remoteAddress)) {

                                    if(discoveryProcessor.isValidatedAuthenication(discoveryNode)) {
                                        //discoveredList.add(discoveryNode);
                                        if(discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVERED) {
                                            discoveredList.add(discoveryNode);
                                        } else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFIED) {
                                            discoveredList.add(discoveryNode);
                                            if(discoveryProcessor.setCertTrust(discoveryNode.getDiscoveredPath(),discoveryNode.discovered_cert)) {
                                                logger.info("Added Static discovered host to discoveredList.");
                                            } else {
                                                logger.error("Could not set Trust");
                                            }
                                        } else {
                                            logger.error("processIncomingDiscoveryNode() discoveryNode.nodeType: " + discoveryNode.nodeType.name() + " !UNKNOWN!");
                                        }
                                    }

                                } else {
                                    logger.error("discoveryNode.discovered_ip: " + discoveryNode.discovered_ip + " != remoteAddress: " + remoteAddress);
                                }

                            } else {
                                logger.error("check shared key : discoveryNode.discovered_ip: == NULL for remoteAddress: " + remoteAddress);
                            }

                        } else {
                            logger.error("discovery node == null");
                        }

                    } else {
                        logger.error("NO DISCOVERY NODE");
                    }

                }
            } catch (Exception ex) {
                logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                logger.trace(sw.toString());
            }
    }

}
