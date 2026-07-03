package io.cresco.agent.controller.capability;

import com.google.gson.Gson;
import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutor;
import io.cresco.library.capability.CapabilityScanner;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

/**
 * Fabric-wide capability inventory — the LLM-tool-catalog analogue of {@code PerfControllerMonitor}'s
 * metric inventory. For a node it emits: the three controller tiers' action descriptors (Agent/Regional/
 * Global, scanned statically from their annotations), every LOCAL plugin's capability document (pulled via
 * the standard {@code getcapabilities} EXEC), and the node's OSGi service/package surface. {@code scope}
 * = node|region|global fans out to children exactly like the metric inventory. The result is one JSON
 * document a client can convert straight into LLM tool definitions.
 */
public class CapabilityInventory {

    private final ControllerEngine controllerEngine;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final Gson gson = new Gson();

    public CapabilityInventory(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(CapabilityInventory.class.getName(), CLogger.Level.Info);
    }

    public String getCapabilityInventory(String scope, boolean includePlugins, boolean includeOsgi) {
        String s = (scope == null) ? "node" : scope.toLowerCase();
        java.util.Map<String, Object> inventory = nodeInventory(includePlugins, includeOsgi);
        inventory.put("scope", s);
        try {
            if (s.equals("region") || s.equals("global")) {
                java.util.Map<String, Object> children = fanOut(s, includePlugins, includeOsgi);
                if (!children.isEmpty()) { inventory.put("children", children); }
            }
        } catch (Exception ex) {
            logger.error("getCapabilityInventory fan-out", ex);
        }
        return gson.toJson(inventory);
    }

    /** This node's controller-tier action docs + local plugin capability docs + OSGi surface. */
    private java.util.Map<String, Object> nodeInventory(boolean includePlugins, boolean includeOsgi) {
        java.util.Map<String, Object> inventory = new java.util.LinkedHashMap<>();
        try {
            String region = plugin.getRegion(), agent = plugin.getAgent();
            java.util.Map<String, Object> bySource = new java.util.LinkedHashMap<>();

            // 1) controller tiers — static annotation scan, no instance needed. Describes the whole
            //    control API (agent/regional/global); the per-action `target` says which tier to route to.
            bySource.put(region + "_" + agent + ":io.cresco.agent.controller:agent",
                    CapabilityScanner.scanActions(AgentExecutor.class));
            bySource.put(region + "_" + agent + ":io.cresco.agent.controller:regional",
                    CapabilityScanner.scanActions(RegionalExecutor.class));
            bySource.put(region + "_" + agent + ":io.cresco.agent.controller:global",
                    CapabilityScanner.scanActions(GlobalExecutor.class));

            // 2) each LOCAL plugin's capability document via the standard getcapabilities EXEC
            if (includePlugins) {
                int timeout = plugin.getConfig().getIntegerParam("capability_rpc_timeout_ms", 2500);
                java.util.List<String> pluginIds = controllerEngine.getGDB().getNodeList(region, agent);
                if (pluginIds != null) {
                    for (String pid : pluginIds) {
                        try {
                            MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pid);
                            req.setParam("action", "getcapabilities");
                            MsgEvent resp = plugin.sendRPC(req, timeout);
                            String caps = (resp != null) ? resp.getParam("capabilities") : null;
                            if (caps != null) {
                                bySource.put(region + "_" + agent + ":" + pid, gson.fromJson(caps, Object.class));
                            }
                        } catch (Exception ignore) { /* plugin doesn't self-describe -> skip */ }
                    }
                }
            }

            inventory.put("node", region + "_" + agent);
            inventory.put("capabilities_by_source", bySource);

            // 3) OSGi service/package surface of this node
            if (includeOsgi) {
                try {
                    inventory.put("osgi", CapabilityScanner.scanOsgi(plugin.getBundleContext()));
                } catch (Exception ignore) { }
            }
        } catch (Exception ex) {
            logger.error("nodeInventory", ex);
        }
        return inventory;
    }

    /** Fan getcapabilityinventory(scope=node) out to each agent in region/global scope, concurrently. */
    private java.util.Map<String, Object> fanOut(String scope, boolean includePlugins, boolean includeOsgi) {
        java.util.Map<String, Object> children = new java.util.LinkedHashMap<>();
        String myRegion = plugin.getRegion(), myAgent = plugin.getAgent();
        java.util.List<String[]> targets = new java.util.ArrayList<>();
        try {
            if (scope.equals("region")) {
                java.util.List<String> agents = controllerEngine.getGDB().getNodeList(myRegion, null);
                if (agents != null) { for (String a : agents) { targets.add(new String[]{myRegion, a}); } }
            } else {
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
        final int childTimeoutMs = plugin.getConfig().getIntegerParam("capability_fanout_timeout_ms", 12000);
        java.util.List<Thread> threads = new java.util.ArrayList<>();
        final java.util.Map<String, Object> synched = java.util.Collections.synchronizedMap(children);
        for (String[] t : targets) {
            final String r = t[0], a = t[1];
            if (r == null || a == null || (r.equals(myRegion) && a.equals(myAgent))) { continue; }
            Thread th = new Thread(() -> {
                try {
                    MsgEvent req = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, r, a);
                    req.setParam("action", "getcapabilityinventory");
                    req.setParam("action_scope", "node");
                    req.setParam("action_include_plugins", includePlugins ? "true" : "false");
                    req.setParam("action_include_osgi", includeOsgi ? "true" : "false");
                    MsgEvent resp = plugin.sendRPC(req, childTimeoutMs);
                    String ci = (resp != null) ? resp.getParam("capabilityinventory") : null;
                    if (ci != null) { synched.put(r + "_" + a, gson.fromJson(ci, Object.class)); }
                } catch (Exception ignore) { /* unreachable/slow node -> skip */ }
            }, "capability-fanout-" + r + "_" + a);
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
}
