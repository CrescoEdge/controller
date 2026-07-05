package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

public class PerfControllerMonitor {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Type crescoType;
    private MeasurementEngine me;
    private Gson gson;

    public PerfControllerMonitor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfControllerMonitor.class.getName(),CLogger.Level.Info);
        this.me = controllerEngine.getMeasurementEngine();

        gson = new Gson();
        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        //start metrics for controller
        initJVMMetrics();
        initControllerMetrics();
        initCEPMetrics();
        initRegionalMetrics();
        initGlobalMetrics();

    }

    // CEP (Siddhi) runs in-process in the controller's DataPlaneService (the standalone cep plugin
    // was removed). Expose its active-query count as a gauge so it folds into getmetricinventory,
    // consistent with how every plugin exposes its own MeasurementEngine gauges.
    public void initCEPMetrics() {
        try {
            io.micrometer.core.instrument.Gauge
                    .builder("cep.queries.active", controllerEngine, ce -> {
                        io.cresco.library.data.DataPlaneService dps = ce.getDataPlaneService();
                        return (dps instanceof io.cresco.agent.data.DataPlaneServiceImpl)
                                ? ((io.cresco.agent.data.DataPlaneServiceImpl) dps).getActiveCEPCount() : 0;
                    })
                    .description("Active Complex-Event-Processing queries in the embedded Siddhi engine.")
                    .register(me.getCrescoMeterRegistry());
            me.setExisting("cep.queries.active", "cep");
        } catch (Exception ex) {
            logger.error("initCEPMetrics ", ex);
        }
    }

    public void shutdown() {
        try {

            me.shutdown();

        } catch (Exception ex) {
            logger.error("PerfControllerMonitor.shutdown ", ex);
        }

    }

    public String getResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;
        try
        {
            if((actionRegion != null) && (actionAgent != null)) {
                queryReturn = getAgentResourceInfo(actionRegion,actionAgent);
            } else if (actionRegion != null) {
                queryReturn = getRegionResourceInfo(actionRegion);
            } else {
                queryReturn = getRegionResourceInfo(null);
            }

        } catch(Exception ex) {
            logger.error("getResourceInfo() ", ex);
        }

        return queryReturn;

    }

    /**
     * B-2 metrics unification: ONE cross-bundle metrics inventory for this node. Merges (1) the
     * controller's own Micrometer metrics (jvm, processor, netlink, controller gauges/timers via
     * {@link MeasurementEngine#getAllMetrics()}), (2) each local plugin's Micrometer metrics — pulled
     * with the standard {@code getmetrics} EXEC, so wsapi/stunnel/etc. fold into the same view — and
     * (3) the resource summary (cpu/mem/disk, i.e. processor performance) from the sysinfo/KPI path.
     * Returns unified JSON. Plugins that don't answer {@code getmetrics} are simply skipped.
     */
    public String getMetricInventory(boolean includePlugins, boolean includeResource) {
        return getMetricInventory("node", includePlugins, includeResource);
    }

    /**
     * Scope-aware unified inventory. {@code scope=node} is this controller only; {@code region} fans out
     * to every agent in this controller's region; {@code global} fans out across the whole mesh. Fan-out
     * reuses the ordinary EXEC/RPC path (getmetricinventory scope=node to each agent's controller) so it
     * adds no new routing surface and never blocks the control plane; unreachable/slow nodes are skipped.
     */
    public String getMetricInventory(String scope, boolean includePlugins, boolean includeResource) {
        String s = (scope == null) ? "node" : scope.toLowerCase();
        java.util.Map<String, Object> inventory = nodeInventory(includePlugins, includeResource);
        inventory.put("scope", s);
        try {
            if (s.equals("region") || s.equals("global")) {
                java.util.Map<String, Object> children = fanOut(s, includePlugins, includeResource);
                if (!children.isEmpty()) { inventory.put("children", children); }
            }
        } catch (Exception ex) {
            logger.error("getMetricInventory fan-out", ex);
        }
        return gson.toJson(inventory);
    }

    /** This controller's own metrics + its LOCAL plugins + (opt) resource summary — no cross-node RPC. */
    private java.util.Map<String, Object> nodeInventory(boolean includePlugins, boolean includeResource) {
        java.util.Map<String, Object> inventory = new java.util.LinkedHashMap<>();
        try {
            String region = plugin.getRegion(), agent = plugin.getAgent();
            java.util.Map<String, Object> bySource = new java.util.LinkedHashMap<>();

            // 1) controller's own Micrometer metrics (all groups) -- always, fast, no RPC
            if (me != null) {
                bySource.put(region + "_" + agent + ":io.cresco.agent.controller", me.getAllMetrics());
            }

            // 2) each LOCAL plugin's metrics via the standard getmetrics EXEC (cross-bundle unification).
            //    Timeout is generous enough for plugins whose collection is not instantaneous (sysinfo's
            //    OSHI enumeration of filesystems/NICs can take >0.5s on the first call).
            if (includePlugins) {
                int pluginTimeoutMs = plugin.getConfig().getIntegerParam("metrics_rpc_timeout_ms", 2500);
                java.util.List<String> pluginIds = controllerEngine.getGDB().getNodeList(region, agent);
                if (pluginIds != null) {
                    for (String pid : pluginIds) {
                        try {
                            io.cresco.library.messaging.MsgEvent req =
                                    plugin.getGlobalPluginMsgEvent(io.cresco.library.messaging.MsgEvent.Type.EXEC, region, agent, pid);
                            req.setParam("action", "getmetrics");
                            io.cresco.library.messaging.MsgEvent resp = plugin.sendRPC(req, pluginTimeoutMs);
                            String m = (resp != null) ? resp.getParam("metrics") : null;
                            if (m != null) {
                                bySource.put(region + "_" + agent + ":" + pid, gson.fromJson(m, Object.class));
                            }
                        } catch (Exception ignore) { /* plugin does not expose metrics -> skip */ }
                    }
                }
            }

            inventory.put("node", region + "_" + agent);
            inventory.put("metrics_by_source", bySource);

            // 3) resource summary (cpu/mem/disk) from the sysinfo path (opt-in, back-compat with resourceinfo)
            if (includeResource) {
                try {
                    String res = getResourceInfo(region, agent);
                    if (res != null) inventory.put("resource_summary", gson.fromJson(res, Object.class));
                } catch (Exception ignore) { }
            }
        } catch (Exception ex) {
            logger.error("nodeInventory", ex);
        }
        return inventory;
    }

    /** Fan getmetricinventory(scope=node) out to each agent in region/global scope; skip self + failures. */
    private java.util.Map<String, Object> fanOut(String scope, boolean includePlugins, boolean includeResource) {
        java.util.Map<String, Object> children = new java.util.LinkedHashMap<>();
        String myRegion = plugin.getRegion(), myAgent = plugin.getAgent();
        java.util.List<String[]> targets = new java.util.ArrayList<>();
        try {
            if (scope.equals("region")) {
                java.util.List<String> agents = controllerEngine.getGDB().getNodeList(myRegion, null);
                if (agents != null) { for (String a : agents) { targets.add(new String[]{myRegion, a}); } }
            } else { // global: every agent in every region
                java.util.List<String> regions = controllerEngine.getGDB().getNodeList(null, null);
                if (regions != null) {
                    for (String r : regions) {
                        java.util.List<String> agents = controllerEngine.getGDB().getNodeList(r, null);
                        if (agents != null) { for (String a : agents) { targets.add(new String[]{r, a}); } }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("fanOut target enumeration", ex);
        }
        final int childTimeoutMs = plugin.getConfig().getIntegerParam("metrics_fanout_timeout_ms", 12000);
        // Query children CONCURRENTLY: each child runs its own node-scoped inventory (which itself may
        // wait on a couple of non-metric plugins), so serial fan-out would stack those waits and blow the
        // caller's budget. A short-lived thread per child keeps whole-mesh scope bounded by the slowest
        // single node, not the sum.
        java.util.List<Thread> threads = new java.util.ArrayList<>();
        final java.util.Map<String, Object> synched = java.util.Collections.synchronizedMap(children);
        for (String[] t : targets) {
            final String r = t[0], a = t[1];
            if (r == null || a == null || (r.equals(myRegion) && a.equals(myAgent))) { continue; }
            Thread th = new Thread(() -> {
                try {
                    io.cresco.library.messaging.MsgEvent req =
                            plugin.getGlobalAgentMsgEvent(io.cresco.library.messaging.MsgEvent.Type.EXEC, r, a);
                    req.setParam("action", "getmetricinventory");
                    req.setParam("action_scope", "node");
                    req.setParam("action_include_plugins", includePlugins ? "true" : "false");
                    if (includeResource) { req.setParam("action_include_resource", "true"); }
                    io.cresco.library.messaging.MsgEvent resp = plugin.sendRPC(req, childTimeoutMs);
                    String mi = (resp != null) ? resp.getParam("metricinventory") : null;
                    if (mi != null) { synched.put(r + "_" + a, gson.fromJson(mi, Object.class)); }
                } catch (Exception ignore) { /* unreachable/slow node -> skip */ }
            }, "metric-fanout-" + r + "_" + a);
            th.setDaemon(true);
            threads.add(th);
            th.start();
        }
        long deadline = System.currentTimeMillis() + childTimeoutMs + 1000L;
        for (Thread th : threads) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) { break; }
            try { th.join(remaining); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        return children;
    }

    private String getRegionResourceInfo(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();


            //List<String> inodeKPIList = dbe.getINodeKPIList(actionRegion,null);
            List<String> agentList = controllerEngine.getGDB().getNodeList(actionRegion, null);
            for(String agent : agentList) {

                String sysInfoJson= getSysInfo(actionRegion,agent);
                if(sysInfoJson != null) {

                    Type type = new TypeToken<Map<String, List<Map<String, String>>>>() {
                    }.getType();

                    Map<String, List<Map<String, String>>> myMap = gson.fromJson(sysInfoJson, type);

                    cpu_core_count += Long.parseLong(myMap.get("cpu").get(0).get("cpu-logical-count"));

                    memoryAvailable += Long.parseLong(myMap.get("mem").get(0).get("memory-available"));
                    memoryTotal += Long.parseLong(myMap.get("mem").get(0).get("memory-total"));

                    for (Map<String, String> fsMap : myMap.get("fs")) {
                        diskAvailable += Long.parseLong(fsMap.get("available-space"));
                        diskTotal += Long.parseLong(fsMap.get("total-space"));
                    }
                }

            }

            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("regionresourceinfo",regionArray);

            queryReturn = gson.toJson(queryMap);


        } catch(Exception ex) {
            logger.error("getRegionResourceInfo() ", ex);
        }

        return queryReturn;

    }

    private String getAgentResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String, List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();



            Map<String,String> resourceTotal = new HashMap<>();
            String perfString = getSysInfo(actionRegion,actionAgent);
            if(perfString != null) {
                resourceTotal.put("perf", perfString);
            }
            regionArray.add(resourceTotal);


            /*
            List<String> inodeKPIList = dbe.getINodeKPIList(actionRegion,actionAgent);
            for(String str : inodeKPIList) {
                Map<String,String> resourceTotal = new HashMap<>();
                resourceTotal.put("perf", dbe.uncompressString(str));
                regionArray.add(resourceTotal);

            }
            */

            queryMap.put("agentresourceinfo",regionArray);
            queryReturn = gson.toJson(queryMap);

        } catch(Exception ex) {
            logger.error("getAgentResourceInfo() ", ex);
        }

        return queryReturn;

    }

    public String getSysInfo(String regionId, String agentId) {
        String response = null;
        try {

            String returnString = controllerEngine.getGDB().getPluginListByType("pluginname", "io.cresco.sysinfo");

            Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(returnString, crescoType);

            if (myRepoMap != null) {

                    if (myRepoMap.containsKey("plugins")) {

                        for (Map<String, String> repoMap : myRepoMap.get("plugins")) {

                            String region = repoMap.get("region");
                            String agent = repoMap.get("agent");
                            String pluginID = repoMap.get("pluginid");

                            if(regionId.equals(region) && agentId.equals(agent)) {

                            //logger.error("SEND :" + region + " " + agent + " " + pluginID + " data");
                                /*
                                MsgEvent benchInfoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                                benchInfoRequest.setParam("action", "getbenchmark");
                                MsgEvent benchInfoResponse = plugin.sendRPC(benchInfoRequest);
                                String benchString = benchInfoResponse.getCompressedParam("bench");
                                logger.error(benchString);
                            */
                            MsgEvent sysInfoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                            sysInfoRequest.setParam("action", "getsysinfo");

                            // Bound this RPC: an unbounded sendRPC here can stall the whole metric
                            // inventory / resourceinfo query if the sysinfo plugin is slow or mid-restart.
                            int sysInfoTimeoutMs = plugin.getConfig().getIntegerParam("resource_rpc_timeout_ms", 3000);
                            MsgEvent sysInfoResponse = plugin.sendRPC(sysInfoRequest, sysInfoTimeoutMs);

                            if (sysInfoResponse != null) {
                                String perfString = sysInfoResponse.getCompressedParam("perf");
                                if (perfString != null) {
                                    //logger.info("perfString: " + perfString);
                                    response = perfString;
                                } else {
                                    response = "{Error}";
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("getSysInfo ", ex);
        }
        return response;
    }

    public String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) {
        String returnString = null;
        returnString = getKPIInfo(actionRegion,actionAgent,actionPluginId);
        logger.debug("String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) " + returnString);

        return returnString;
    }

    public String getKPIInfo(String regionId, String agentId, String pluginId) {
        String response = null;
        try {

            //response = kpiCache.getIfPresent(regionId + "." + agentId + "." + pluginId);
            //response = kpiCache.getIfPresent(regionId + "." + agentId);
            response = "{ERROR NO LONGER IMPLEMENTED}";
            response = getControllerInfoMap();

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return response;
    }

    public String getControllerInfoMap() {

        String returnStr = null;
        try {

            Map<String,List<Map<String,String>>> info = new HashMap<>();
            info.put("controller", controllerEngine.getMeasurementEngine().getMetricGroupList("controller"));

            Map<String,String> metricsMap = new HashMap<>();
            metricsMap.put("name","controller_group");
            metricsMap.put("metrics",gson.toJson(info));

            List<Map<String,String>> metricsList = new ArrayList<>();
            metricsList.add(metricsMap);

            returnStr = gson.toJson(metricsList);

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return returnStr;
    }

    private void initJVMMetrics() {

        new ClassLoaderMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmMemoryMetrics().bindTo(me.getCrescoMeterRegistry());
        //not sure why this is disabled, perhaps not useful
        //new JvmGcMetrics().bindTo(me.getCrescoMeterRegistry());
        new ProcessorMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmThreadMetrics().bindTo(me.getCrescoMeterRegistry());


        Map<String,String> internalMap = new HashMap<>();

        internalMap.put("jvm.memory.max", "jvm");
        internalMap.put("jvm.memory.used", "jvm");
        internalMap.put("jvm.memory.committed", "jvm");
        internalMap.put("jvm.buffer.memory.used", "jvm");
        internalMap.put("jvm.threads.daemon", "jvm");
        internalMap.put("jvm.threads.live", "jvm");
        internalMap.put("jvm.threads.peak", "jvm");
        internalMap.put("jvm.classes.loaded", "jvm");
        internalMap.put("jvm.classes.unloaded", "jvm");
        internalMap.put("jvm.buffer.total.capacity", "jvm");
        internalMap.put("jvm.buffer.count", "jvm");
        //creates issue with windows
        //internalMap.put("system.load.average.1m", "jvm");
        internalMap.put("system.cpu.count", "jvm");
        internalMap.put("system.cpu.usage", "jvm");
        internalMap.put("process.cpu.usage", "jvm");

        for (Map.Entry<String, String> entry : internalMap.entrySet()) {
            String name = entry.getKey();
            String group = entry.getValue();
            me.setExisting(name,group);
        }

    }

    private void initControllerMetrics() {
        me.setTimer("message.transaction.time", "The timer for messages", "controller");
    }

    // B-2 metrics unification: un-stubbed. The original gauges referenced schedule queues that were
    // removed; per the "bind to signals by role, not to a specific field" rule these now track live,
    // always-present role signals. Registered against the shared Micrometer registry and surfaced via
    // MeasurementEngine#getAllMetrics() with setExisting (same pattern as initJVMMetrics). Harmless on
    // roles where the signal is empty (a region's brokered map is empty on a leaf agent -> reads 0).
    public void initRegionalMetrics() {
        try {
            io.micrometer.core.instrument.Gauge
                    .builder("brokered.agent.count", controllerEngine,
                            ce -> { var m = ce.getBrokeredAgents(); return m == null ? 0 : m.size(); })
                    .description("Agents currently brokered by this regional controller.")
                    .register(me.getCrescoMeterRegistry());
            me.setExisting("brokered.agent.count", "regional");
        } catch (Exception ex) {
            logger.error("initRegionalMetrics ", ex);
        }
    }

    public void initGlobalMetrics() {
        try {
            io.micrometer.core.instrument.Gauge
                    .builder("reachable.agent.count", controllerEngine,
                            ce -> { var l = ce.reachableAgents(); return l == null ? 0 : l.size(); })
                    .description("Agents reachable from this controller's view of the mesh.")
                    .register(me.getCrescoMeterRegistry());
            me.setExisting("reachable.agent.count", "global");

            io.micrometer.core.instrument.Gauge
                    .builder("incoming.candidate.brokers", controllerEngine,
                            ce -> { var q = ce.getIncomingCanidateBrokers(); return q == null ? 0 : q.size(); })
                    .description("Depth of the discovery candidate-broker queue awaiting processing.")
                    .register(me.getCrescoMeterRegistry());
            me.setExisting("incoming.candidate.brokers", "global");
        } catch (Exception ex) {
            logger.error("initGlobalMetrics ", ex);
        }
    }


}
