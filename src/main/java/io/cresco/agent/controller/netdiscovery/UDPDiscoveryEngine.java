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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class UDPDiscoveryEngine implements Runnable {
    private static Thread discoveryEngineWorkerThread;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private static Map<NetworkInterface, MulticastSocket> workers = new ConcurrentHashMap<>();
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;
    private CLogger logger;
    private int discoveryPort;
    private AtomicBoolean lockPacket = new AtomicBoolean();
    private DiscoveryProcessor discoveryProcessor;

    public UDPDiscoveryEngine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(UDPDiscoveryEngine.class.getName(),CLogger.Level.Info);
        this.discoveryProcessor = new DiscoveryProcessor(controllerEngine);
        this.gson = new Gson();
        this.discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
        logger.info("Started DiscoveryUDPEngine");

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
            MulticastSocket multicastSocket = entry.getValue();
            multicastSocket.close();
            while(!multicastSocket.isClosed()) {
                System.out.println("waiting on multicast socket to close");
            }
            while(discoveryEngineWorkerThread.isAlive()) {
                System.out.println("waiting on discovery thread to close");
            }
        }
    }

    public void run() {
        logger.info("Initialized");
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                logger.debug("Found: " + networkInterface.getDisplayName());
                discoveryEngineWorkerThread = new Thread(new DiscoveryEngineWorker(networkInterface, controllerEngine));
                discoveryEngineWorkerThread.start();
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
            synchronized (lockPacket) {

                Map<String, String> intAddr = getInterfaceAddresses();
                InetAddress returnAddr = packet.getAddress();
                //address from the sending agent
                String remoteAddress = stripIPv6Address(returnAddr);
                int remotePort = packet.getPort();

                boolean isRemoteBroadcast = true;
                if (returnAddr instanceof Inet6Address)
                    isRemoteBroadcast = !returnAddr.isSiteLocalAddress() && !returnAddr.isLinkLocalAddress();

                //address of discovered agent
                String localAddress = null;

                if((intAddr != null) && (remoteAddress != null)) {
                    localAddress = getSourceAddress(intAddr, remoteAddress); //determine rec address
                }

                if(localAddress == null) {
                    logger.trace("No local interface subnet address found for " + remoteAddress);
                    logger.trace("SocketAddress " + packet.getSocketAddress().toString());
                }

                if(intAddr == null) {
                    intAddr = new HashMap<>();
                }

                if (!(intAddr.containsKey(remoteAddress)) && (isRemoteBroadcast) && (localAddress != null)) {

                    String message = new String(packet.getData()).trim();

                    DiscoveryNode discoveryNode = null;

                    try {

                        try {
                            MsgEvent rme = gson.fromJson(message, MsgEvent.class);
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

                                //discoveryNode = processDiscoveryNode(discoveryNode,localAddress, remoteAddress, remotePort);
                                discoveryNode = discoveryProcessor.processIncomingBroadCastDiscovery(discoveryNode,localAddress,discoveryPort,remoteAddress, remotePort);

                                if (discoveryNode != null) {

                                    int returnPort = remotePort;

                                    MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                                    me.setCompressedParam("discovery_node",gson.toJson(discoveryNode));

                                    String json = gson.toJson(me);
                                    byte[] sendData = json.getBytes();
                                    logger.debug("returnAddr: " + remoteAddress + " returnPort: " + returnPort);

                                    packet.setData(sendData);
                                    packet.setLength(sendData.length);
                                    packet.setAddress(returnAddr);
                                    packet.setPort(returnPort);

                                } else {
                                    packet = null; //make sure packet is null
                                    logger.trace("(me != null)");

                                }
                            }
                         else {
                            logger.error("DiscoveryNode = NULL");
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

                return packet;
            }
        }

    }

}