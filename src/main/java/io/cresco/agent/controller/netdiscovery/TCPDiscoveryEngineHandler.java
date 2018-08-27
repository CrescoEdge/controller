package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.security.cert.Certificate;

/**
 * Handles both client-side and server-side handler depending on which
 * constructor was called.
 */
public class TCPDiscoveryEngineHandler extends ChannelInboundHandlerAdapter {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;
    private int state = 0;

    public TCPDiscoveryEngineHandler(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngineHandler.class.getName(),CLogger.Level.Info);
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        gson = new Gson();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the client.

        try {

            MsgEvent me = gson.fromJson((String)msg, MsgEvent.class);
                MsgEvent rme = processMessage(me);
                ctx.write(gson.toJson(rme));
        } catch(Exception ex) {
            logger.error("channelRead" + ex.getMessage());
            ctx.close();
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private MsgEvent processMessage(MsgEvent rme) {

        MsgEvent me = null;
        try {
            logger.trace("Static Discovery Status = " + rme.getParam("discovery_static_agent"));

            if (rme.getParam("discovery_type") != null) {
                if (rme.getParam("discovery_type").equals(DiscoveryType.NETWORK.name())) {
                    logger.debug("{}", "network discovery");
                    me = getNetworkMsg(rme); //generate payload
                }
                if(controllerEngine.cstate.isRegionalController()) {
                    if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                        logger.debug("{}", "agent discovery");
                        me = getAgentMsg(rme); //generate payload
                    } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                        logger.debug("{}", "regional discovery");
                        me = getRegionMsg(rme);
                    } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                        //if this is not a global controller, don't respond
                        if(controllerEngine.cstate.isGlobalController()) {
                            logger.debug("{}", "global discovery");
                            me = getGlobalMsg(rme);
                        }
                    }
                }
            }

        } catch(Exception ex) {
            logger.error("TCPDiscovery processMessage Error: " + ex.toString());
        }

        return me;
    }

    private MsgEvent getNetworkMsg(MsgEvent rme) {
        MsgEvent me = null;
        try {

            logger.trace("getNetworkMsg : " + rme.getParams().toString());

            if (rme.getParam("src_region") != null) {
                me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                me.setParam("dst_region", plugin.getRegion());
                me.setParam("dst_agent", rme.getParam("src_agent"));
                me.setParam("src_region", plugin.getRegion());
                me.setParam("src_agent", plugin.getAgent());
                me.setParam("dst_ip", rme.getParam("src_ip"));
                me.setParam("dst_port", rme.getParam("src_port"));
                me.setParam("agent_count", String.valueOf(controllerEngine.reachableAgents().size()));
                me.setParam("discovery_type", DiscoveryType.NETWORK.name());
                me.setParam("broadcast_ts", rme.getParam("broadcast_ts"));

                logger.debug("getAgentMsg = " + me.getParams().toString());

            }
            else {
                if(rme.getParam("src_region") == null) {
                    logger.trace("getAgentMsg : Invalid src_region");
                }
            }

        } catch (Exception ex) {
            logger.error("getAgentMsg " + ex.getMessage());
        }
        return me;
    }

    private MsgEvent getAgentMsg(MsgEvent rme) {
        MsgEvent me = null;
        try {

            logger.trace("getAgentMsg : " + rme.getParams().toString());
            //determine if we should respond to request
            //String validateMsgEvent(rme)
            //       validatedAuthenication

            if (controllerEngine.reachableAgents().size() < plugin.getConfig().getIntegerParam("max_region_size",Integer.MAX_VALUE))  {

                String validatedAuthenication = validateMsgEvent(rme); //create auth string

                if (validatedAuthenication != null) {
                    //if (rme.getParam("src_region").equals("init")) {
                    //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
                    me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                    me.setParam("dst_region", plugin.getRegion());
                    me.setParam("dst_agent", rme.getParam("src_agent"));
                    me.setParam("src_region", plugin.getRegion());
                    me.setParam("src_agent", plugin.getAgent());
                    me.setParam("dst_ip", rme.getParam("src_ip"));
                    me.setParam("dst_port", rme.getParam("src_port"));
                    me.setParam("agent_count", String.valueOf(controllerEngine.reachableAgents().size()));
                    me.setParam("discovery_type", DiscoveryType.AGENT.name());
                    me.setParam("validated_authenication",validatedAuthenication);

                    me.setParam("broadcast_ts", rme.getParam("broadcast_ts"));
                    logger.debug("getAgentMsg = " + me.getParams().toString());
                    //return message exist, if cert exist add it and include ours
                    if(rme.getParam("public_cert") != null) {
                        String remoteAgentPath = plugin.getRegion() + "_" + me.getParam("dst_agent");
                        String localCertString = configureCertTrust(remoteAgentPath,rme.getParam("public_cert"));
                        if(localCertString != null) {
                            me.setParam("public_cert",localCertString);
                        }
                    }
                }
                else {
                    if(rme.getParam("src_region") == null) {
                        logger.trace("getAgentMsg : Invalid src_region");
                    }
                    if(validatedAuthenication == null) {
                        logger.trace("getAgentMsg : validatedAuthenication == null");
                    }
                }
                    /*
                    else {

                        logger.error("src_region=" + rme.getParam("src_region") + " validatedAuthenication=" + validatedAuthenication);
                        if ((rme.getParam("src_region").equals(agentcontroller.getRegion())) && agentcontroller.cstate.isRegionalController()()) {
                            logger.error("{}", "!reconnect attempt!");
                        }

                    }
                    */
            }
            else {
                logger.debug("Agent count too hight.. not responding to discovery");
            }

        } catch (Exception ex) {
            logger.error("getAgentMsg " + ex.getMessage());
        }
        return me;
    }

    private MsgEvent getGlobalMsg(MsgEvent rme) {
        MsgEvent me = null;
        try {
            if (controllerEngine.cstate.isRegionalController()) {

                String validatedAuthenication = validateMsgEvent(rme); //create auth string
                if (validatedAuthenication != null) {

                    //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
                    me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                    me.setParam("dst_region", plugin.getRegion());
                    me.setParam("dst_agent", rme.getParam("src_agent"));
                    me.setParam("src_region", plugin.getRegion());
                    me.setParam("src_agent", plugin.getAgent());
                    me.setParam("src_plugin", plugin.getPluginID());
                    me.setParam("dst_ip", rme.getParam("src_ip"));
                    me.setParam("dst_port", rme.getParam("src_port"));
                    me.setParam("agent_count", String.valueOf(controllerEngine.reachableAgents().size()));
                    me.setParam("discovery_type", DiscoveryType.GLOBAL.name());
                    me.setParam("broadcast_ts", rme.getParam("broadcast_ts"));
                    me.setParam("validated_authenication", validatedAuthenication);
                    //return message exist, if cert exist add it and include ours
                    if(rme.getParam("public_cert") != null) {
                        String remoteAgentPath = me.getParam("dst_region") + "-global";
                        String localCertString = configureCertTrust(remoteAgentPath,rme.getParam("public_cert"));
                        if(localCertString != null) {
                            me.setParam("public_cert",localCertString);
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("getGlobalMsg " + ex.getMessage());
        }
        return me;
    }

    private MsgEvent getRegionMsg(MsgEvent rme) {
        MsgEvent me = null;
        try {
            if (controllerEngine.cstate.isRegionalController()) {

                String validatedAuthenication = validateMsgEvent(rme); //create auth string
                if (validatedAuthenication != null) {

                    //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
                    me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                    me.setParam("dst_region", plugin.getRegion());
                    me.setParam("dst_agent", rme.getParam("src_agent"));
                    me.setParam("src_region", plugin.getRegion());
                    me.setParam("src_agent", plugin.getAgent());
                    me.setParam("dst_ip", rme.getParam("src_ip"));
                    me.setParam("dst_port", rme.getParam("src_port"));
                    me.setParam("agent_count", String.valueOf(controllerEngine.reachableAgents().size()));
                    me.setParam("discovery_type", DiscoveryType.REGION.name());
                    me.setParam("broadcast_ts", rme.getParam("broadcast_ts"));
                    me.setParam("validated_authenication", validatedAuthenication);
                    //return message exist, if cert exist add it and include ours
                    if(rme.getParam("public_cert") != null) {
                        String remoteAgentPath = me.getParam("dst_region");
                        String localCertString = configureCertTrust(remoteAgentPath,rme.getParam("public_cert"));
                        if(localCertString != null) {
                            me.setParam("public_cert",localCertString);
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("getRegionalMsg " + ex.getMessage());
        }
        return me;
    }

    private String configureCertTrust(String remoteAgentPath, String remoteCertString) {
        String localCertString = null;
        try {
            Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
            controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath,certs);
            controllerEngine.getBroker().updateTrustManager();
            localCertString = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
        } catch(Exception ex) {
            logger.error("configureCertTrust Error " + ex.getMessage());
        }
        return localCertString;
    }

    private String validateMsgEvent(MsgEvent rme) {

        String validatedAuthenication = null;
        String groupName = null;
        try {
            String discoverySecret = null;
            if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
                groupName = "agent";
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
                groupName = "region";
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
                groupName = "global";
            }

            String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
            String discoveryValidator = rme.getParam("discovery_validator");
            String decryptedString = discoveryCrypto.decrypt(discoveryValidator,discoverySecret);

            if(decryptedString != null) {
                if (decryptedString.equals(verifyMessage)) {
                    //agentcontroller.brokerUserNameAgent
                    //isValidated = true;
                    //String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
                    //encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);
                    validatedAuthenication = discoveryCrypto.encrypt(controllerEngine.brokerUserNameAgent + "," + controllerEngine.brokerPasswordAgent + "," + groupName, discoverySecret);
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return validatedAuthenication ;
    }



}
