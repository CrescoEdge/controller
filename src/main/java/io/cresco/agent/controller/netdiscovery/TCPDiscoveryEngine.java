package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.cert.Certificate;

public class TCPDiscoveryEngine implements Runnable {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;
    private CLogger logger;
    private int discoveryPort;
    protected Thread       runningThread= null;
    protected static ServerSocket serverSocket = null;
    protected static boolean      isStopped    = false;

    public TCPDiscoveryEngine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(TCPDiscoveryEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(),CLogger.Level.Info);
        logger.trace("Initializing");
        //this.agentcontroller = agentcontroller;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        gson = new Gson();
        this.discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
    }

    public TCPDiscoveryEngine(ControllerEngine controllerEngine, int discoveryPort) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(TCPDiscoveryEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(),CLogger.Level.Info);
        //logger.trace("Initializing");
        //this.agentcontroller = agentcontroller;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        gson = new Gson();
        this.discoveryPort = discoveryPort;
    }

    private class WorkerRunnable implements Runnable{

        protected Socket clientSocket = null;
        protected String serverText   = null;
        protected ControllerEngine controllerEngine = null;

        public WorkerRunnable(ControllerEngine controllerEngine, Socket clientSocket, String serverText) {
            this.clientSocket = clientSocket;
            this.serverText   = serverText;
            this.controllerEngine = controllerEngine;
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

        public void run() {
            InputStream input = null;
            OutputStream output = null;
            ObjectOutputStream oos = null;
            ObjectInputStream ois = null;

            try {
                input  = clientSocket.getInputStream();
                output = clientSocket.getOutputStream();

                ois = new ObjectInputStream(input);
                String message = (String) ois.readObject();

                MsgEvent me = null;

                try {
                    MsgEvent rme = gson.fromJson(message, MsgEvent.class);
                    me = processMessage(rme);
                } catch (Exception ex) {
                    logger.error(getClass().getName() + " failed to marshal discovery {}" + ex.getMessage());
                }

                if(me !=null) {
                    //let the client know what IP has been used
                    me.setParam("src_ip", clientSocket.getRemoteSocketAddress().toString());
                    me.setParam("src_port", String.valueOf(clientSocket.getPort()));

                    oos = new ObjectOutputStream(output);
                    //message out
                    String json = gson.toJson(me);

                    oos.writeObject(json);
                    oos.flush();

                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {

                    if(oos != null) {
                        oos.close();
                    }
                    if(ois != null) {
                        ois.close();
                    }
                    if(output != null) {
                        output.close();
                    }
                    if(input != null) {
                        input.close();
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                }


            }
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

                if (controllerEngine.reachableAgents().size() < plugin.getConfig().getIntegerParam("max_region_size",250))  {

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
                    groupName = "io/cresco/agent";
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

    public void run(){
        synchronized(this){
            this.runningThread = Thread.currentThread();
        }
        openServerSocket();
        controllerEngine.setTCPDiscoveryActive(true);
        while(! isStopped()){
            try {
                new Thread(
                        new WorkerRunnable(controllerEngine,
                                serverSocket.accept(), "Multithreaded Server")
                ).start();

            } catch (IOException e) {
                if(isStopped()) {
                    System.out.println("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException(
                        "Error accepting client connection", e);
            }
        }
        System.out.println("Server Stopped.") ;
    }


    /*
    public void run(){
        synchronized(this){
            this.runningThread = Thread.currentThread();
        }
        openServerSocket();
        controllerEngine.setTCPDiscoveryActive(true);
        while(! isStopped()){
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
            } catch (IOException e) {
                if(isStopped()) {
                    System.out.println("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException(
                        "Error accepting client connection", e);
            }
            new Thread(
                    new WorkerRunnable(controllerEngine,
                            clientSocket, "Multithreaded Server")
            ).start();
        }
        System.out.println("Server Stopped.") ;
    }
    */
    public static synchronized void shutdown(){
        isStopped = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private synchronized boolean isStopped() {
        return isStopped;
    }

    public synchronized void stop(){
        isStopped = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private void openServerSocket() {
        try {
            serverSocket = new ServerSocket(this.discoveryPort);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port 8080", e);
        }
    }

}