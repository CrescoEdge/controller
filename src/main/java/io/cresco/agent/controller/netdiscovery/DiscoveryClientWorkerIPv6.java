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

public class DiscoveryClientWorkerIPv6 {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DatagramSocket c;
    private Gson gson;
    public Timer timer;
    public int discoveryTimeout;
    public String multiCastNetwork;
    public DiscoveryType disType;
    private boolean timerActive = false;
    private List<MsgEvent> discoveredList;
    private DiscoveryCrypto discoveryCrypto;
    private int discoveryPort;
    private AtomicBoolean lockPacket = new AtomicBoolean();


    public DiscoveryClientWorkerIPv6(ControllerEngine controllerEngine, DiscoveryType disType, int discoveryTimeout, String multiCastNetwork) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientWorkerIPv6.class.getName(),CLogger.Level.Info);
        gson = new Gson();
        this.discoveryTimeout = discoveryTimeout;
        this.multiCastNetwork = multiCastNetwork;
        this.disType = disType;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        this.discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);

    }

    public DiscoveryClientWorkerIPv6(ControllerEngine controllerEngine, DiscoveryType disType, int discoveryTimeout, String multiCastNetwork, int discoveryPort) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientWorkerIPv6.class.getName(),CLogger.Level.Info);
        gson = new Gson();
        this.discoveryTimeout = discoveryTimeout;
        this.multiCastNetwork = multiCastNetwork;
        this.disType = disType;
        discoveryCrypto = new DiscoveryCrypto(controllerEngine);
        this.discoveryPort = discoveryPort;
    }

    private class StopListnerTask extends TimerTask {
        public void run() {
            try {
                //user timer to close socket
                c.close();
                timer.cancel();
                timerActive = false;

            } catch (Exception ex) {
                logger.error("StopListnerTask {}", ex.getMessage());
            }
        }
    }

    private synchronized void processIncoming(DatagramPacket packet) {
        synchronized (lockPacket) {
            String json = new String(packet.getData()).trim();
            try {
                MsgEvent me = gson.fromJson(json, MsgEvent.class);
                if (me != null) {

                    me.setParam("broadcast_latency", String.valueOf(System.currentTimeMillis()-Long.parseLong(me.getParam("broadcast_ts"))));

                    String remoteAddress = packet.getAddress().getHostAddress();
                    if (remoteAddress.contains("%")) {
                        String[] remoteScope = remoteAddress.split("%");
                        remoteAddress = remoteScope[0];
                    }
                    logger.trace("Processing packet for {} {}_{}", remoteAddress, me.getParam("src_region"), me.getParam("src_agent"));
                    me.setParam("src_ip", me.getParam("dst_ip"));
                    me.setParam("src_port", me.getParam("dst_port"));
                    me.setParam("dst_ip", remoteAddress);
                    me.setParam("dst_port", String.valueOf(packet.getPort()));
                    me.setParam("dst_region", me.getParam("src_region"));
                    me.setParam("dst_agent", me.getParam("src_agent"));
                    me.setParam("src_region", plugin.getRegion());
                    me.setParam("src_agent", plugin.getAgent());
                    if (disType == DiscoveryType.AGENT || disType == DiscoveryType.REGION || disType == DiscoveryType.GLOBAL) {
                        me.setParam("validated_authenication", ValidatedAuthenication(me));
                    }

                    discoveredList.add(me);


                }
            } catch (Exception ex) {
                logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
            }
        }
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

    List<MsgEvent> discover() {
        // Find the server using UDP broadcast
        //logger.info("Discovery (IPv6) started");
        logger.debug(disType.toString() + " Discovery (IPv6) started ");
        try {
            discoveredList = new ArrayList<>();
            // Broadcast the message over all the network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();

                //if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    //if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    continue; // Don't want to broadcast to the loopback interface
                }

                if (networkInterface.supportsMulticast()) {
                    for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                        try {
                            //if((interfaceAddress.getAddress() instanceof Inet6Address) && !interfaceAddress.getAddress().isLinkLocalAddress())
                            InetAddress inAddr = interfaceAddress.getAddress();
                            boolean isGlobal = !inAddr.isSiteLocalAddress() && !inAddr.isLinkLocalAddress();

                            if ((inAddr instanceof Inet6Address) && isGlobal) {
                                //if(inAddr instanceof Inet6Address)

                                //c = new MulticastSocket(null);
                                c = new DatagramSocket(null);
                                //c.setReuseAddress(true);
                                //System.out.println("prebind1");
                                String hostAddress = interfaceAddress.getAddress().getHostAddress();
                                if (hostAddress.contains("%")) {
                                    String[] hostScope = hostAddress.split("%");
                                    hostAddress = hostScope[0];
                                }
                                SocketAddress sa = new InetSocketAddress(hostAddress, 0);
                                //System.out.println("prebind2");

                                c.bind(sa);
                                //System.out.println("prebind3");

                                //start timer to clost discovery
                                timer = new Timer();
                                timer.schedule(new StopListnerTask(), discoveryTimeout);
                                timerActive = true;
                                MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");
                                sme.setParam("broadcast_ip", multiCastNetwork);
                                sme.setParam("broadcast_interface", networkInterface.getDisplayName());
                                sme.setParam("broadcast_ts", String.valueOf(System.currentTimeMillis()));
                                sme.setParam("src_region", this.plugin.getRegion());
                                sme.setParam("src_agent", this.plugin.getAgent());

                                //set crypto message for discovery

                                if (disType == DiscoveryType.AGENT || disType == DiscoveryType.REGION || disType == DiscoveryType.GLOBAL) {
                                    logger.trace("Discovery Type = {}", disType.name());
                                    sme.setParam("discovery_type", disType.name());
                                    //set crypto message for discovery
                                    sme.setParam("discovery_validator",generateValidateMessage(sme));
                                } else if(disType == DiscoveryType.NETWORK) {
                                    sme.setParam("discovery_type", disType.name());
                                    logger.trace("Discovery Type = {}", disType.name());
                                } else {
                                    logger.trace("Discovery type unknown");
                                    sme = null;
                                }

                                if (sme != null) {
                                    logger.trace("Building sendPacket for {}", inAddr.toString());
                                    String sendJson = gson.toJson(sme);
                                    byte[] sendData = sendJson.getBytes();
                                    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName(broadCastNetwork), discoveryPort);
                                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName(multiCastNetwork), discoveryPort);

                                    synchronized (c) {
                                        c.send(sendPacket);
                                        logger.trace("Sent sendPacket via {}", inAddr.toString());
                                    }
                                    while (!c.isClosed()) {
                                        logger.trace("Listening " +  inAddr.toString() + " Timeout: " + discoveryTimeout + " SysTime: " + System.currentTimeMillis());
                                        try {
                                            byte[] recvBuf = new byte[15000];
                                            DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
                                            synchronized (c) {
                                                c.receive(receivePacket);
                                                logger.trace("Received packet");
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
                                }


                            }
                        } catch (IOException ie) {
                            //eat exception we are closing port
                            //System.out.println("DiscoveryClientWorkerIPv6 : getDiscoveryMap IO Error : " + ie.getMessage());
                        } catch (Exception e) {
                            StringWriter errors = new StringWriter();
                            e.printStackTrace(new PrintWriter(errors));
                            logger.error("getDiscoveryMap {}", e.getMessage());
                            logger.error("getDiscoveryMap {}", errors.toString());
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("while not closed: " + ex.getMessage());
        }
        return discoveredList;
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
}
