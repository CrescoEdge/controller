package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * A controller updates only its own records in its parent's registry. An agent registers, unregisters and keeps alive
 * itself in its region ({@code agent_enable}, {@code agent_disable}, WATCHDOG); a region does the same for itself and
 * its agents at the global ({@code region_enable}, {@code region_disable}, {@code regionalimport}, WATCHDOG). The
 * records a message names -- {@code region_watchdog_update}, {@code agent_watchdog_update}, {@code unregister_*} and
 * every entry of {@code regionconfigs}/{@code agentconfigs}/{@code pluginconfigs} -- must be the caller's: so holding
 * the infra role of one node no longer rewrites another node's registration.
 *
 * <p>At the global only the controller of a region (the one that joined the global) speaks for it: an ordinary agent's
 * stamp names the region too. Every unregistration names its records: an agent unregisters itself (never its whole
 * region), a region its own agents, and a plugin record is removed only together with its agent's name (DBEngine never
 * deletes a bare plugin id).
 *
 * <p>An {@code admin} caller (whom the action table admits in scope) and {@code anonymous} (insecure mode) are not
 * bound to one node; an admin's plugin unregistration still names its agent. The action table decides who may call at
 * all; this decides which records.
 */
public final class RegistryBinding {

    private RegistryBinding() { }

    private static final Gson GSON = new Gson();
    private static final Type CONFIGS = new TypeToken<Map<String, List<Map<String, String>>>>() { }.getType();

    /** Does {@code m} update the registry (so {@link #refusal} applies)? */
    public static boolean applies(MsgEvent m) {
        if (m.getMsgType() == MsgEvent.Type.WATCHDOG) return true;
        String a = m.getParam("action");
        if (a == null) return false;
        switch (a) {
            case "agent_enable": case "agent_disable": case "region_enable": case "region_disable": case "regionalimport":
                return m.getMsgType() == MsgEvent.Type.CONFIG;
            default:
                return false;
        }
    }

    /**
     * Why {@code caller} may not make the registry update {@code m}, or null when it may.
     *
     * @param atGlobal    the update is made at the global (a region reports itself and its agents) rather than at a
     *                    region (an agent reports itself)
     * @param regionOfAgent the region an agent is registered in here (null when unknown), for plugin records at the global
     */
    public static String refusal(MsgEvent m, Caller caller, boolean atGlobal, java.util.function.Function<String, String> regionOfAgent) {
        return refusal(m, caller, atGlobal, regionOfAgent, null);
    }

    /**
     * As above. {@code regionController}: at the global, is this controller the controller of its region (the one that
     * joined this global, see {@code ControllerEngine.speaksForRegion})? Only that controller speaks for its region at the
     * global; without it (null) no controller does (fail closed: an ordinary agent of a region is no region).
     */
    public static String refusal(MsgEvent m, Caller caller, boolean atGlobal, java.util.function.Function<String, String> regionOfAgent,
                                 java.util.function.Predicate<Caller> regionController) {
        String[] configs = new String[3];
        String[] names = {"regionconfigs", "agentconfigs", "pluginconfigs"};
        for (int i = 0; i < 3; i++) {
            try {
                configs[i] = m.paramsContains(names[i]) ? m.getCompressedParam(names[i]) : null;
            } catch (RuntimeException e) {
                return "unreadable " + names[i];
            }
        }
        return refusal(caller, atGlobal, regionOfAgent, regionController, m.getParam("region_watchdog_update"), m.getParam("agent_watchdog_update"),
                m.getParam("plugin_watchdog_update"), m.getParam("unregister_region_id"), m.getParam("unregister_agent_id"),
                m.getParam("unregister_plugin_id"), configs[0], configs[1], configs[2]);
    }

    /**
     * A state update on the dataplane ({@code update_mode} AGENT at a region, REGION at the global): its verified writer
     * ({@code cresco_caller}, stamped by the writer's controller and checked by the broker) must be the node it reports.
     */
    public static String dataplaneRefusal(Caller writer, boolean atGlobal, java.util.function.Function<String, String> regionOfAgent,
                                          String regionId, String agentId, String regionconfigs, String agentconfigs, String pluginconfigs) {
        return dataplaneRefusal(writer, atGlobal, regionOfAgent, null, regionId, agentId, regionconfigs, agentconfigs, pluginconfigs);
    }

    /** As above, with the global's {@code regionController} check (see {@link #refusal(MsgEvent, Caller, boolean, java.util.function.Function, java.util.function.Predicate)}). */
    public static String dataplaneRefusal(Caller writer, boolean atGlobal, java.util.function.Function<String, String> regionOfAgent,
                                          java.util.function.Predicate<Caller> regionController,
                                          String regionId, String agentId, String regionconfigs, String agentconfigs, String pluginconfigs) {
        return refusal(writer, atGlobal, regionOfAgent, regionController, atGlobal ? (regionId == null ? "" : regionId) : null,
                atGlobal ? null : (agentId == null ? "" : agentId), null, null, null, null, regionconfigs, agentconfigs, pluginconfigs);
    }

    private static final Type PARAMS = new TypeToken<Map<String, String>>() { }.getType();

    /**
     * {@code agentconfigs} with every agent's {@code configparams} naming {@code tenant} as its {@code tenant_id}: an agent
     * row carries the tenant its controller was verified in (the caller of its registration or keep-alive), never one it
     * claims. Listings ({@code listagents}) show it, so a client resolves a destination's tenant. Unchanged when it cannot
     * be read.
     */
    public static String withVerifiedTenant(String agentconfigs, String tenant) {
        if (agentconfigs == null || tenant == null) return agentconfigs;
        try {
            Map<String, List<Map<String, String>>> configs = GSON.fromJson(agentconfigs, CONFIGS);
            if (configs == null) return agentconfigs;
            for (List<Map<String, String>> entries : configs.values()) {
                if (entries == null) continue;
                for (Map<String, String> e : entries) {
                    if (e == null) continue;
                    Map<String, String> params = null;
                    String cp = e.get("configparams");
                    if (cp != null) {
                        try { params = GSON.fromJson(cp, PARAMS); } catch (RuntimeException ignore) { params = null; }
                    }
                    if (params == null) params = new java.util.HashMap<>();
                    params.put("tenant_id", tenant);
                    e.put("configparams", GSON.toJson(params));
                }
            }
            return GSON.toJson(configs);
        } catch (RuntimeException e) {
            return agentconfigs;
        }
    }

    static String refusal(Caller caller, boolean atGlobal, java.util.function.Function<String, String> regionOfAgent,
                          java.util.function.Predicate<Caller> regionController,
                          String rw, String aw, String pw, String ur, String ua, String up,
                          String regionconfigs, String agentconfigs, String pluginconfigs) {
        if (caller == null) return "no verified caller";
        if (caller.isAnonymous()) return null;
        // a plugin record is removed only with its agent: DBEngine never deletes a bare plugin id (review F3)
        if (up != null && ua == null) return "unregistering plugin " + up + " without naming its agent, from " + caller;
        if (caller.hasRole(Caller.ROLE_ADMIN)) return null;
        if (caller.getKind() != Caller.Kind.CONTROLLER) return "only a controller updates the registry, not " + caller;
        String region = caller.getRegion(), agent = caller.getAgent();
        if (atGlobal && (regionController == null || !regionController.test(caller))) {
            // an ordinary agent's stamp names its region too: only the controller that joined this global speaks for it
            return "at the global only the controller of region " + region + " updates its records, not " + caller;
        }
        if (rw != null && !rw.equals(region)) return "keep-alive for region " + rw + " from " + caller;
        if (aw != null && !aw.equals(agent)) return "keep-alive for agent " + aw + " from " + caller;
        if (pw != null) return "a plugin keep-alive is not made through the registry by " + caller;
        if (ur != null && !ur.equals(region)) return "unregistering region " + ur + " from " + caller;
        if (!atGlobal && ua != null && !ua.equals(agent)) return "unregistering agent " + ua + " from " + caller;
        if (!atGlobal && ur != null && ua == null) return "an agent unregisters itself, not its region " + ur + ", " + caller;
        if (atGlobal && ua != null) {
            String r = regionOfAgent == null ? null : regionOfAgent.apply(ua);
            if (r == null || !r.equals(region)) {
                return "unregistering agent " + ua + (r == null ? " (not registered here)" : " of region " + r) + " from " + caller;
            }
        }
        // regionconfigs: entries name their region; agentconfigs: keyed by region, entries name agents;
        // pluginconfigs: keyed by agent (DBEngine.nodeUpdateStatus)
        String[] names = {"regionconfigs", "agentconfigs", "pluginconfigs"};
        String[] values = {regionconfigs, agentconfigs, pluginconfigs};
        for (int i = 0; i < 3; i++) {
            String p = names[i];
            if (values[i] == null) continue;
            Map<String, List<Map<String, String>>> configs;
            try {
                configs = GSON.fromJson(values[i], CONFIGS);
            } catch (RuntimeException e) {
                return "malformed " + p;
            }
            if (configs == null) continue;
            for (Map.Entry<String, List<Map<String, String>>> entry : configs.entrySet()) {
                String key = entry.getKey();
                if ("agentconfigs".equals(p) && key != null && !key.equals(region)) {
                    return p + " is for region " + key + ", the caller is " + caller;
                }
                if ("pluginconfigs".equals(p) && key != null) {
                    if (!atGlobal && !key.equals(agent)) return p + " is for agent " + key + ", the caller is " + caller;
                    if (atGlobal && !key.equals(agent)) {
                        String r = regionOfAgent == null ? null : regionOfAgent.apply(key);
                        if (r != null && !r.equals(region)) return p + " is for agent " + key + " of region " + r + ", the caller is " + caller;
                    }
                }
                if (entry.getValue() == null) continue;
                for (Map<String, String> e : entry.getValue()) {
                    if (e == null) continue;
                    String r = e.get("region_id"), ag = e.get("agent_id");
                    if (r != null && !r.equals(region)) return p + " names region " + r + ", the caller is " + caller;
                    if (!atGlobal && ag != null && !ag.equals(agent)) return p + " names agent " + ag + ", the caller is " + caller;
                }
            }
        }
        return null;
    }

}
