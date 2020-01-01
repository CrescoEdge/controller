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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

class DiscoveryClientWorkerIPv4 {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DatagramSocket c;
    private Gson gson;
    private Timer timer;
    private int discoveryTimeout;
    private String broadCastNetwork;
    private DiscoveryType disType;
    private boolean timerActive = false;
    private List<DiscoveryNode> discoveredList;
    private DiscoveryCrypto discoveryCrypto;
    private int broadcast_rec_port;
    private DiscoveryProcessor discoveryProcessor;
    private AtomicBoolean lockPacket = new AtomicBoolean();

    DiscoveryClientWorkerIPv4(ControllerEngine controllerEngine, DiscoveryType disType, int discoveryTimeout, String broadCastNetwork) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientWorkerIPv4.class.getName(),CLogger.Level.Info);
        this.gson = new Gson();
        this.discoveryTimeout = discoveryTimeout;
        this.broadCastNetwork = broadCastNetwork;
        this.disType = disType;
        this.discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        this.broadcast_rec_port = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
        this.discoveryProcessor = new DiscoveryProcessor(controllerEngine);
    }

    DiscoveryClientWorkerIPv4(ControllerEngine controllerEngine, DiscoveryType disType, int discoveryTimeout, String broadCastNetwork, int discoveryPort) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientWorkerIPv4.class.getName(),CLogger.Level.Info);
        this.gson = new Gson();
        this.discoveryTimeout = discoveryTimeout;
        this.broadCastNetwork = broadCastNetwork;
        this.disType = disType;
        this.discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        this.broadcast_rec_port = discoveryPort;

    }

    private class StopListenerTask extends TimerTask {
        public void run() {
            try {
                logger.trace("Closing Listener... Timeout: " + discoveryTimeout + " SysTime: " + System.currentTimeMillis());

                //user timer to close socket
                c.close();
                timer.cancel();
                timerActive = false;
            } catch (Exception ex) {
                logger.error("StopListenerTask {}", ex.getMessage());
            }
        }
    }

    private synchronized void processIncoming(DatagramPacket packet) {
        synchronized (lockPacket) {
            String json = new String(packet.getData()).trim();
            try {
                MsgEvent me = gson.fromJson(json, MsgEvent.class);
                DiscoveryNode discoveryNode = null;
                if (me != null) {

                    if(me.paramsContains("discovery_node")) {
                        discoveryNode = gson.fromJson(me.getCompressedParam("discovery_node"), DiscoveryNode.class);


                        if(discoveryNode != null) {
                            logger.info("Discovery Node Found: " + discoveryNode.discovered_ip + " latency: " + discoveryNode.getDiscoveryLatency());

                            String remoteAddress = packet.getAddress().getHostAddress();
                            if (remoteAddress.contains("%")) {
                                String[] remoteScope = remoteAddress.split("%");
                                remoteAddress = remoteScope[0];
                            }

                            if(discoveryNode.discovered_ip.equals(remoteAddress)) {

                                if(discoveryProcessor.isValidatedAuthenication(discoveryNode)) {
                                    discoveredList.add(discoveryNode);
                                }

                            } else {
                                logger.error("discoveryNode.discovered_ip: " + discoveryNode.discovered_ip + " != remoteAddress: " + remoteAddress);
                            }
                        } else {
                            logger.error("discoveryNode == null");
                        }

                    } else {
                        logger.error("NO DISCOVERY NODE");
                    }

                }
            } catch (Exception ex) {
                logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
            }
        }
    }

    public List<DiscoveryNode> discover() {
        // Find the server using UDP broadcast
        logger.debug(disType.toString() + " Discovery (IPv4) started ");
        try {
            discoveredList = new ArrayList<>();

            // Broadcast the message over all the network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    continue; // Don't want to broadcast to the loopback interface
                }
                logger.trace("Getting interfaceAddresses for interface {}", networkInterface.getDisplayName());
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    try {

                        if (!(interfaceAddress.getAddress() instanceof Inet4Address)) {
                            continue;
                        }

                        logger.trace("Trying address {} for interface {}", interfaceAddress.getAddress().toString(), networkInterface.getDisplayName());

                        InetAddress inAddr = interfaceAddress.getBroadcast();

                        if (inAddr == null) {
                            logger.trace("Not a broadcast");
                            continue;
                        }

                        logger.trace("Creating DatagramSocket", inAddr.toString());
                        c = new DatagramSocket(null);
                        logger.trace("Setting broadcast to true on DatagramSocket", inAddr.toString());
                        c.setBroadcast(true);

                        timer = new Timer();
                        timer.schedule(new StopListenerTask(), discoveryTimeout);
                        timerActive = true;

                        MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");

                        DiscoveryNode discoveryNode = discoveryProcessor.generateBroadCastDiscovery(disType,false);

                        if(discoveryNode != null) {

                            sme.setCompressedParam("discovery_node", gson.toJson(discoveryNode));
                            logger.trace("Building sendPacket for {}", inAddr.toString());
                            String sendJson = gson.toJson(sme);
                            byte[] sendData = sendJson.getBytes();
                            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName(broadCastNetwork), broadcast_rec_port);
                            synchronized (c) {
                                c.send(sendPacket);
                                logger.info("Sent sendPacket via {}", inAddr.toString());
                            }
                            while (!c.isClosed()) {
                                logger.info("Listening " + inAddr.toString() + " Timeout: " + discoveryTimeout + " SysTime: " + System.currentTimeMillis());
                                try {
                                    byte[] recvBuf = new byte[15000];
                                    DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
                                    synchronized (c) {
                                        c.receive(receivePacket);
                                        logger.info("Received packet");
                                        if (timerActive) {
                                            logger.trace("Restarting listening timer");
                                            timer.schedule(new StopListenerTask(), discoveryTimeout);
                                        }
                                    }
                                    synchronized (receivePacket) {
                                        processIncoming(receivePacket);
                                    }
                                } catch (SocketException se) {
                                    // Eat the message, this is normal
                                } catch (Exception e) {
                                    logger.error("discovery {}", e.getMessage());
                                }
                            }

                        } else {
                            logger.error("discover() discoveryNode == null");
                        }

                    } catch (SocketException se) {
                        logger.error("getDiscoveryMap : SocketException {}", se.getMessage());
                    } catch (IOException ie) {
                        // Eat the exception, closing the port
                    } catch (Exception e) {
                        StringWriter errors = new StringWriter();
                        e.printStackTrace(new PrintWriter(errors));
                        logger.error("getDiscoveryMap {}", e.getMessage());
                        logger.error("getDiscoveryMap {}", errors.toString());
                        //return errors.toString();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("while not closed: {}", ex.getMessage());
        }
        return discoveredList;
    }

}
