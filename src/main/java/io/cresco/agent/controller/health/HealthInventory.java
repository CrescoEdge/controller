package io.cresco.agent.controller.health;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Builds this node's health inventory as JSON — the queryable parallel to the unified metric
 * inventory ({@code getmetricinventory}). Every plugin/controller-tier that registers a Felix
 * {@code HealthCheck} is discovered by {@link CrescoHealthExecutor}; this serializes the current
 * snapshot ({@link CrescoHealthExecutor#all()} + {@link CrescoHealthExecutor#aggregate}) so a
 * client can read fabric health over MsgEvent exactly the way it reads metrics.
 */
public final class HealthInventory {

    private static final Gson GSON = new Gson();

    private HealthInventory() {}

    /** Node-scoped health inventory JSON: {node, aggregate, checks:[{name,status,rawStatus,message,tags,lastRunTs}]}. */
    public static String node(ControllerEngine ce) {
        Map<String, Object> out = new TreeMap<>();
        try {
            String node = null;
            try {
                node = ce.getPluginBuilder().getRegion() + "_" + ce.getPluginBuilder().getAgent();
            } catch (Exception ignore) {}
            out.put("node", node);

            CrescoHealthExecutor hx = ce.getHealthExecutor();
            if (hx == null) {
                out.put("aggregate", "UNKNOWN");
                out.put("checks", new ArrayList<>());
                out.put("status_desc", "health executor not started");
                return GSON.toJson(out);
            }

            out.put("aggregate", String.valueOf(hx.aggregate()));
            List<Map<String, Object>> checks = new ArrayList<>();
            for (HealthResult r : hx.all()) {
                Map<String, Object> row = new TreeMap<>();
                row.put("name", r.name);
                row.put("status", String.valueOf(r.status));
                row.put("rawStatus", String.valueOf(r.rawStatus));
                row.put("message", r.message);
                row.put("tags", (r.tags != null) ? new ArrayList<>(r.tags) : new ArrayList<>());
                row.put("lastRunTs", r.lastRunTs);
                checks.add(row);
            }
            out.put("checks", checks);
        } catch (Exception ex) {
            out.put("error", String.valueOf(ex.getMessage()));
        }
        return GSON.toJson(out);
    }
}
