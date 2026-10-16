package io.cresco.agent.controller.globalscheduler;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The one shape of an agent {@code pluginadd} request built from a CADL node, shared by the global scheduler
 * ({@link PollAddPipeline} then {@link ResourceScheduler}) and the regional local scheduler.
 *
 * <p>The agent's pluginadd reads the plugin's config from {@code configparams} (gzip-base64 JSON of a string
 * map) and the node's edges, when it has any, from {@code edges} (gzip-base64). The config is the node's
 * params minus its placement ({@code location_region}, {@code location_agent}) and its edges, with
 * {@code persistence_code} defaulting to 10 for CADL nodes.
 */
public final class PluginAddRequest {

    private static final Gson GSON = new Gson();

    private PluginAddRequest() { }

    /** The plugin config of a CADL node with these params. {@code params} is not modified. */
    public static Map<String, String> nodeConfig(Map<String, String> params) {
        Map<String, String> cfg = new LinkedHashMap<>();
        if (params != null) cfg.putAll(params);
        //todo persistance should be set by the application explicity
        //if <9 node will persist, so we will make that the default for CADL plugins
        cfg.putIfAbsent("persistence_code", "10");
        cfg.remove("location_region");
        cfg.remove("location_agent");
        cfg.remove("edges");
        return cfg;
    }

    /** {@link #nodeConfig} as the JSON that travels (compressed) in {@code configparams}. */
    public static String nodeConfigJson(Map<String, String> params) {
        return GSON.toJson(nodeConfig(params));
    }

    /**
     * Makes {@code me} (a CONFIG message addressed to the target agent) the pluginadd of a CADL node with
     * these params: {@code action}, compressed {@code configparams}, and compressed {@code edges} when the
     * node has edges. Nothing else of the node goes on the message.
     */
    public static MsgEvent fill(MsgEvent me, Map<String, String> params) {
        me.setParam("action", "pluginadd");
        me.setCompressedParam("configparams", nodeConfigJson(params));
        String edges = (params != null) ? params.get("edges") : null;
        if (edges != null) me.setCompressedParam("edges", edges);
        return me;
    }
}
