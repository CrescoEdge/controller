package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryClientIPv6 {
    //private int discoveryTimeout;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public DiscoveryClientIPv6(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DiscoveryClientIPv6.class.getName(),CLogger.Level.Info);

    }

    public List<MsgEvent> getDiscoveryResponse(DiscoveryType disType, int discoveryTimeout) {
        List<MsgEvent> discoveryList = new ArrayList<>();
        try {


            while (controllerEngine.isClientDiscoveryActiveIPv6()) {
                logger.debug("Discovery already underway, waiting..");
                Thread.sleep(2500);
            }
            controllerEngine.setClientDiscoveryActiveIPv6(true);
            //Searching local network [ff02::1:c]
            String multiCastNetwork = "ff02::1:c";
            DiscoveryClientWorkerIPv6 dcw = new DiscoveryClientWorkerIPv6(controllerEngine, disType, discoveryTimeout, multiCastNetwork);
            //populate map with possible peers
            logger.debug("Searching {}", multiCastNetwork);
            discoveryList.addAll(dcw.discover());

        } catch (Exception ex) {
            logger.error("getDiscoveryMap {}", ex.getMessage());

        }
        controllerEngine.setClientDiscoveryActiveIPv6(false);
        return discoveryList;
    }

    public boolean isReachable(String hostname) {
        boolean reachable = false;
        try {
            //also, this fails for an invalid address, like "www.sjdosgoogle.com1234sd"
            //InetAddress[] addresses = InetAddress.getAllByName("www.google.com");
            Inet6Address address = (Inet6Address) Inet6Address.getByName(hostname);
            reachable = address.isReachable(10000);
        } catch (Exception ex) {
            logger.error("isReachable {}", ex.getMessage());
        }
        return reachable;
    }


}
