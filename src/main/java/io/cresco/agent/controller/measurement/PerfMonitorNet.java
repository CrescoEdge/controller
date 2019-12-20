package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.*;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class PerfMonitorNet {


    public Timer timer;
    private boolean running = false;
    private Gson gson;
    private boolean polling = false;
    private List<DiscoveryNetworkTopoNode> dnListStatic;

    private DiscoveryClientIPv4 ip4dc;
    private DiscoveryClientIPv6 ip6dc;

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;


    public PerfMonitorNet(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfMonitorNet.class.getName(),CLogger.Level.Info);
        gson = new Gson();
        dnListStatic = new ArrayList<>();
    }

    public PerfMonitorNet start() {
        if (this.running) return this;
        Long interval = plugin.getConfig().getLongParam("perftimer", 10000L);

        /*
        MsgEvent initial = new MsgEvent(MsgEvent.Type.INFO, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Performance Monitoring timer set to " + interval + " milliseconds.");
        initial.setParam("src_region", plugin.getRegion());
        initial.setParam("src_agent", plugin.getAgent());
        initial.setParam("src_plugin", plugin.getPluginID());
        initial.setParam("dst_region", plugin.getRegion());
        initial.setParam("dst_agent", plugin.getAgent());
        initial.setParam("dst_plugin", plugin.getPluginID());
        initial.setParam("is_regional",Boolean.TRUE.toString());
        initial.setParam("is_global",Boolean.TRUE.toString());
        plugin.sendMsgEvent(initial);
        */

        timer = new Timer();
        timer.scheduleAtFixedRate(new PerfMonitorTask(controllerEngine), 500L, interval);
        return this;
    }

    public PerfMonitorNet restart() {
        if (running) timer.cancel();
        running = false;
        return start();
    }

    public void stop() {
        timer.cancel();
        running = false;
    }

    public String getStaticNetworkDiscovery(List<String> iplist) {
        String static_network_map = null;
        try {
            List<MsgEvent> discoveryList = new ArrayList<>();
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);

            for(String ip : iplist) {
                discoveryList.addAll(ds.discover(DiscoveryType.NETWORK, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout",10000), ip));
            }

            List<DiscoveryNetworkTopoNode> dnList = new ArrayList<>();
            for(MsgEvent me : discoveryList) {
                dnList.add(new DiscoveryNetworkTopoNode(me.getParam("src_ip"),me.getParam("src_port"),me.getParam("src_region"),me.getParam("src_agent"),me.getParam("dst_ip"),me.getParam("dst_port"),me.getParam("dst_region"),me.getParam("dst_agent"),me.getParam("broadcast_ts"),me.getParam("broadcast_latency"),me.getParam("agent_count")));
            }

            dnListStatic.addAll(dnList);

            static_network_map = gson.toJson(dnList);


        } catch(Exception ex) {
            logger.error("getStaticNetworkDiscovery() " + ex.getMessage());
        }
        return static_network_map;
    }

    private List<MsgEvent> getNetworkDiscoveryList() {


        List<MsgEvent> discoveryList = null;
        polling = true;
        try {

            discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                if(ip6dc == null) {
                    ip6dc = new DiscoveryClientIPv6(controllerEngine);
                }
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(ip6dc.getDiscoveryResponse(DiscoveryType.NETWORK, plugin.getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 10000)));
                logger.debug("IPv6 Broker count = {} " + discoveryList.size());
            }
            if(ip4dc == null) {
                ip4dc = new DiscoveryClientIPv4(controllerEngine);
            }
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(ip4dc.getDiscoveryResponse(DiscoveryType.NETWORK, plugin.getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 10000)));
            logger.debug("Broker count = {} " + discoveryList.size());

            //for (MsgEvent me : discoveryList) {
            //    logger.debug(me.getParams().toString());
            //}

        }
        catch(Exception ex) {
            logger.error("getNetworkDiscoveryList() " + ex.getMessage());
        }
        polling = false;
        return discoveryList;
    }


    private class PerfMonitorTask extends TimerTask {

        private ControllerEngine controllerEngine;
        private PluginBuilder plugin;

        PerfMonitorTask(ControllerEngine controllerEngine) {
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
        }

        public void run() {

            if(!polling) {

                MsgEvent tick = plugin.getKPIMsgEvent();
                /*
                MsgEvent tick = new MsgEvent(MsgEvent.Type.KPI, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Performance Monitoring tick.");
                tick.setParam("src_region", plugin.getRegion());
                tick.setParam("src_agent", plugin.getAgent());
                tick.setParam("src_plugin", plugin.getPluginID());
                tick.setParam("dst_region", plugin.getRegion());
                tick.setParam("dst_agent", plugin.getAgent());
                tick.setParam("dst_plugin", "plugin/0");
                tick.setParam("is_regional",Boolean.TRUE.toString());
                tick.setParam("is_global",Boolean.TRUE.toString());
                */
                tick.setParam("resource_id", plugin.getConfig().getStringParam("resource_id", "netdiscovery_resource"));
                tick.setParam("inode_id", plugin.getConfig().getStringParam("inode_id", "netdiscovery_inode"));

                    List<MsgEvent> discoveryList = getNetworkDiscoveryList();
                    List<DiscoveryNetworkTopoNode> dnList = new ArrayList<>();
                    for(MsgEvent me : discoveryList) {
                        dnList.add(new DiscoveryNetworkTopoNode(me.getParam("src_ip"),me.getParam("src_port"),me.getParam("src_region"),me.getParam("src_agent"),me.getParam("dst_ip"),me.getParam("dst_port"),me.getParam("dst_region"),me.getParam("dst_agent"),me.getParam("broadcast_ts"),me.getParam("broadcast_latency"),me.getParam("agent_count")));
                    }

                    //include any static entries
                    dnList.addAll(dnListStatic);

                    String discoveryListString = gson.toJson(dnList);

                    tick.setCompressedParam("perf", discoveryListString);

                    plugin.msgIn(tick);

            }
        }
    }
}
