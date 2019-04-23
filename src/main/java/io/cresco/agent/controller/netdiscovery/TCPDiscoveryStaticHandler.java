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
import java.security.cert.Certificate;
import java.util.List;
import java.util.UUID;

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
    private Gson gson;


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
        gson = new Gson();
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
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent", UUID.randomUUID().toString());
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region", UUID.randomUUID().toString());
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global", UUID.randomUUID().toString());
            }

            String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
            encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return encryptedString;
    }

    private synchronized void processIncoming(String json, String remoteAddress) {
        try {
            MsgEvent me = gson.fromJson(json, MsgEvent.class);
            if (me != null) {

                if (remoteAddress.contains("%")) {
                    String[] remoteScope = remoteAddress.split("%");
                    remoteAddress = remoteScope[0];
                }
                logger.info("Processing packet for {} {}_{}", remoteAddress, me.getParam("src_region"), me.getParam("src_agent"));
                me.setParam("dst_ip", remoteAddress);
                me.setParam("dst_region", me.getParam("src_region"));
                me.setParam("dst_agent", me.getParam("src_agent"));
                me.setParam("validated_authenication",ValidatedAuthenication(me));
                discoveredList.add(me);
                if(me.getParam("public_cert") != null) {
                    logger.info("public_cert Exists");
                    String remoteAgentPath = me.getParam("src_region") + "_" + me.getParam("src_agent");
                    if(setCertTrust(remoteAgentPath,me.getParam("public_cert"))) {
                        logger.info("Added Static discovered host to discoveredList.");
                        logger.info("discoveredList contains " + discoveredList.size() + " items.");
                    } else {
                        logger.info("Could not set Trust");
                    }
                } else {
                    logger.info("processIncoming() : no cert found");
                }
                //sme.setParam("public_cert", agentcontroller.getCertificateManager().getJsonFromCerts(agentcontroller.getCertificateManager().getPublicCertificate()));

            }
        } catch (Exception ex) {
            logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
        }

    }

    private String ValidatedAuthenication(MsgEvent rme) {
        String decryptedString = null;
        try {

            String discoverySecret = null;
            if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent", UUID.randomUUID().toString());
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region", UUID.randomUUID().toString());
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global", UUID.randomUUID().toString());
            }
            if(rme.getParam("validated_authenication") != null) {
                decryptedString = discoveryCrypto.decrypt(rme.getParam("validated_authenication"), discoverySecret);
            }
            else {
                logger.error("[validated_authenication] record not found!");
                logger.error(rme.getParams().toString());
            }

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return decryptedString;
    }

    private boolean setCertTrust(String remoteAgentPath, String remoteCertString) {
        boolean isSet = false;
        try {
            Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
            controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath,certs);
            isSet = true;

        } catch(Exception ex) {
            logger.error("configureCertTrust Error " + ex.getMessage());
        }
        return isSet;
    }


}
