package io.cresco.agent.controller.netdiscovery;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryClientIPv4 {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public DiscoveryClientIPv4(ControllerEngine controllerEngine) {

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientIPv4.class.getName(),CLogger.Level.Info);
    }

    public List<MsgEvent> getDiscoveryResponse(DiscoveryType disType, int discoveryTimeout) {
        List<MsgEvent> discoveryList = new ArrayList<>();
        try {
            while (controllerEngine.isClientDiscoveryActiveIPv4()) {
                logger.debug("Discovery already underway, waiting..");
                Thread.sleep(2500);
            }
            controllerEngine.setClientDiscoveryActiveIPv4(true);
            //Searching local network 255.255.255.255
            String broadCastNetwork = "255.255.255.255";

            DiscoveryClientWorkerIPv4 dcw = new DiscoveryClientWorkerIPv4(controllerEngine, disType, discoveryTimeout, broadCastNetwork);
            //populate map with possible peers
            logger.debug("Searching {}", broadCastNetwork);
            discoveryList.addAll(dcw.discover());


        } catch (Exception ex) {
            //System.out.println("IPv4 getDiscoveryMap " + ex.getMessage());
            logger.error("getDiscoveryMap {}", ex.getMessage());

        }
        controllerEngine.setClientDiscoveryActiveIPv4(false);
        return discoveryList;
    }

    public boolean isReachable(String hostname) {
        boolean reachable = false;
        try {
            InetAddress address = InetAddress.getByName(hostname);

            reachable = address.isReachable(10000);

        } catch (Exception ex) {
            logger.error("DiscoveryClient : isReachable : Error " + ex.toString());
        }
        return reachable;
    }


}
