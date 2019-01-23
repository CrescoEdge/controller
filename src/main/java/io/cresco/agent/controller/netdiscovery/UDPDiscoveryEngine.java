package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.*;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UDPDiscoveryEngine implements Runnable {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private static Map<NetworkInterface, MulticastSocket> workers = new ConcurrentHashMap<>();
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;
    private CLogger logger;
    private int discoveryPort;

    public UDPDiscoveryEngine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(UDPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(UDPDiscoveryEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(),CLogger.Level.Info);
        logger.trace("Initializing");
        //this.agentcontroller = agentcontroller;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        gson = new Gson();
        this.discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
    }

    public UDPDiscoveryEngine(ControllerEngine controllerEngine, int discoveryPort) {

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(UDPDiscoveryEngine.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(UDPDiscoveryEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(),CLogger.Level.Info);
        logger.trace("Initializing");
        //this.agentcontroller = agentcontroller;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        gson = new Gson();
        this.discoveryPort = discoveryPort;
    }

    public static void shutdown() {
        for (Map.Entry<NetworkInterface, MulticastSocket> entry : workers.entrySet()) {
            entry.getValue().close();
        }
    }

    public void run() {
        logger.info("Initialized");
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                logger.debug("Found: " + networkInterface.getDisplayName());
                Thread thread = new Thread(new DiscoveryEngineWorker(networkInterface, controllerEngine));
                thread.start();
            }
            controllerEngine.setUDPDiscoveryActive(true);
            logger.trace("Shutdown");
        } catch (Exception ex) {
            logger.error("Run {}", ex.getMessage());
        }
    }

    private class DiscoveryEngineWorker implements Runnable {
        private NetworkInterface networkInterface;
        private MulticastSocket socket;
        private ControllerEngine controllerEngine;
        private PluginBuilder plugin;

        public DiscoveryEngineWorker(NetworkInterface networkInterface, ControllerEngine controllerEngine) {
            this.networkInterface = networkInterface;
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
            //this.agentcontroller = agentcontroller;
        }

        public void shutdown() {
            socket.close();
        }

        public void run() {
            logger.debug("Creating worker [{}]", networkInterface.getDisplayName());
            try {
                if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual()) {
                //if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint()) {
                    logger.trace("Discovery Engine Worker [" + networkInterface.getDisplayName() + "] initialized");
                    //logger.trace("Init [{}]", networkInterface.getDisplayName());
                    SocketAddress sa;
                    if (plugin.isIPv6()) {
                        sa = new InetSocketAddress("[::]", discoveryPort);
                    } else {
                        sa = new InetSocketAddress("0.0.0.0", discoveryPort);
                    }
                    socket = new MulticastSocket(null);
                    socket.bind(sa);
                    workers.put(networkInterface, socket);
                    //ditry hack for possible race condition
                    while(!workers.containsKey(networkInterface)) {
                        Thread.sleep(1000);
                    }
                    logger.trace("Bound to interface [{}] address [::]", networkInterface.getDisplayName());

                    if (plugin.isIPv6()) {
                        //find to network and site multicast addresses
                        //SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),discoveryPort);
                        //socket.joinGroup(saj, networkInterface);
                        SocketAddress saj2 = new InetSocketAddress(Inet6Address.getByName("ff02::1:c"), discoveryPort);
                        socket.joinGroup(saj2, networkInterface);
                    }

                    while (!workers.get(networkInterface).isClosed()) {
                        //System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");

                        //Receive a packet
                        byte[] recvBuf = new byte[15000];
                        DatagramPacket recPacket = new DatagramPacket(recvBuf, recvBuf.length);

                        try {
                            synchronized (socket) {
                                socket.receive(recPacket); //rec broadcast packet, could be IPv6 or IPv4
                                logger.trace("Received Discovery packet!");
                            }

                            synchronized (socket) {
                                DatagramPacket sendPacket = sendPacket(recPacket);
                                if (sendPacket != null) {
                                    socket.send(sendPacket);
                                    logger.trace("Sent Discovery packet!");
                                    controllerEngine.responds.incrementAndGet();
                                }
                                else {
                                    logger.trace("Can't generate sendPacket for " + recPacket.getAddress());
                                }

                            }
                        } catch (IOException e) {
                            logger.trace("Socket closed");
                        }
                    }
                    logger.trace("Discovery Engine Worker [" + networkInterface.getDisplayName() + "] has shutdown");
                }
                else {
                    logger.debug("Not listening on " + networkInterface.getDisplayName());
                }
            } catch (Exception ex) {
                logger.error("Run : Interface = {} : Error = {}", networkInterface.getDisplayName(), ex.getMessage());
                ex.printStackTrace();
            }
        }

        private String stripIPv6Address(InetAddress address) {
            String sAddress = null;
            try {
                sAddress = address.getHostAddress();
                if (sAddress.contains("%")) {
                    String[] aScope = sAddress.split("%");
                    sAddress = aScope[0];
                }
            } catch (Exception ex) {
                logger.error("stripIPv6Address {}", ex.getMessage());
            }
            return sAddress;
        }

        private String getSourceAddress(Map<String, String> intAddr, String remoteAddress) {
            String sAddress = null;
            try {
                for (Map.Entry<String, String> entry : intAddr.entrySet()) {
                    String cdirAddress = entry.getKey() + "/" + entry.getValue();
                    logger.trace("getSourceAddress : cdirAddress: " + cdirAddress + " remoteAddress: " + remoteAddress);
                    CIDRUtils cutil = new CIDRUtils(cdirAddress);
                    if (cutil.isInRange(remoteAddress)) {
                        sAddress = entry.getKey();
                        logger.trace("Found address in range : cdirAddress: " + cdirAddress + " == remoteAddress: " + remoteAddress);
                    }
                }
            } catch (Exception ex) {
                logger.error("getSourceAddress {}", ex.getMessage());
            }
            return sAddress;
        }

        private Map<String, String> getInterfaceAddresses() {
            Map<String, String> intAddr = null;
            try {
                intAddr = new HashMap<>();
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    Short aPrefix = interfaceAddress.getNetworkPrefixLength();
                    String hostAddress = stripIPv6Address(interfaceAddress.getAddress());
                    intAddr.put(hostAddress, aPrefix.toString());
                }
            } catch (Exception ex) {
                logger.error("stripIPv6Address {}", ex.getMessage());
            }
            return intAddr;
        }

        private synchronized DatagramPacket sendPacket(DatagramPacket packet) {
            synchronized (packet) {
                logger.trace("sendpacket 0");
                Map<String, String> intAddr = getInterfaceAddresses();
                InetAddress returnAddr = packet.getAddress();
                logger.trace("sendpacket returnAddr: " + returnAddr.getHostName());
                String remoteAddress = stripIPv6Address(returnAddr);
                logger.trace("sendpacket returnAddr-stripped: " + remoteAddress);

                boolean isGlobal = true;
                if (returnAddr instanceof Inet6Address)
                    isGlobal = !returnAddr.isSiteLocalAddress() && !returnAddr.isLinkLocalAddress();
                logger.trace("sendpacket 1");
                //logger.trace("Discovery packet from " + remoteAddress + " to " + sourceAddress);

                String sourceAddress = getSourceAddress(intAddr, remoteAddress); //determine send address
                //packet.getSocketAddress().
                if(sourceAddress == null) {
                    logger.trace("No local interface subnet address found for " + remoteAddress);
                    logger.trace("SocketAddress " + packet.getSocketAddress().toString());
                }

                //if (!(intAddr.containsKey(remoteAddress)) && (isGlobal) && (sourceAddress != null)) {
                if (!(intAddr.containsKey(remoteAddress)) && (isGlobal)) {
                        //Packet received
                    //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
                    //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

                    //See if the packet holds the right command (message)
                    String message = new String(packet.getData()).trim();
                    logger.trace("sendpacket 2");

                    MsgEvent rme = null;

                    try {
                        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
                        //check that the message can be marshaled into a MsgEvent
                        //System.out.println(getClass().getName() + "0.0 " + Thread.currentThread().getId());
                        try {
                            rme = gson.fromJson(message, MsgEvent.class);
                        } catch (Exception ex) {
                            logger.error(getClass().getName() + " failed to marshal discovery {}" + ex.getMessage());
                        }
                        //System.out.println(getClass().getName() + "0.1 " + Thread.currentThread().getId());
                        if (rme != null) {
                            //check for static discovery
                            //&& (sourceAddress != null)


                            if ((sourceAddress != null) || (rme.getParam("discovery_static_agent") != null)) {
                            //if (sourceAddress != null) {

                                logger.trace("Static Discovery Status = " + rme.getParam("discovery_static_agent"));

                                rme.setParam("src_ip", remoteAddress);
                                rme.setParam("src_port", String.valueOf(packet.getPort()));

                                MsgEvent me = null;

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

                                if (me != null) {

                                    String json = gson.toJson(me);
                                    logger.trace(me.getParams().toString());
                                    byte[] sendData = json.getBytes();
                                    //returnAddr = InetAddress.getByName(me.getParam("dst_ip"));
                                    int returnPort = Integer.parseInt(me.getParam("dst_port"));
                                    logger.debug("returnAddr: " + remoteAddress + " returnPort: " + returnPort);
                                    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
                                    packet.setData(sendData);
                                    packet.setLength(sendData.length);
                                    packet.setAddress(returnAddr);
                                    packet.setPort(returnPort);
                                    logger.trace("sendpacket 3");

                                } else {
                                    packet = null; //make sure packet is null
                                    logger.trace("(me != null)");
                                    logger.trace("sendpacket 4e");


                                }
                            } else {
                                packet = null;
                                logger.trace("(sourceAddress != null) || (rme.getParam(\"discovery_static_agent\") != null)");
                            }
                        } else {
                            packet = null;
                        }
                    } catch (Exception ex) {
                        logger.error("sendPacket() " + ex.getMessage());
                        StringWriter errors = new StringWriter();
                        ex.printStackTrace(new PrintWriter(errors));
                        logger.error(errors.toString());
                        packet = null;
                    }
                } else {
                    packet = null;
                }
                logger.trace("sendpacket 5");

                return packet;
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
                if (controllerEngine.reachableAgents().size() < plugin.getConfig().getIntegerParam("max_region_size",Integer.MAX_VALUE))  {

                    String validatedAuthenication = validateMsgEvent(rme); //create auth string


                    if ((rme.getParam("src_region") != null) && (validatedAuthenication != null)) {
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
                            //String remoteAgentPath = plugin.getRegion() + "_" + me.getParam("dst_agent");
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
                            //String remoteAgentPath = plugin.getRegion() + "_" + me.getParam("dst_agent");
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

}