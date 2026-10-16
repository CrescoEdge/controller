package io.cresco.agent.controller.regionalcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalscheduler.PollAddPipeline;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.library.app.gNode;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region-local pipeline (rpipelinesubmit on a regional controller) is placed by the region itself: it sends
 * each node's pluginadd straight to the target agent. The agent's pluginadd reads the plugin's config from
 * {@code configparams} (gzip-base64 JSON) and its edges from {@code edges}, exactly as the global scheduler
 * sends them; the regional path used to spread the node params over the message instead, so the agent could
 * not read the config.
 */
class RegionalPluginAddTest {

    private static final Gson GSON = new Gson();
    private static final String EDGES = "{\"e1\":\"n1->n2\"}";

    private static Map<String, String> nodeParams() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("pluginname", "io.cresco.example");
        p.put("jarfile", "example.jar");
        p.put("example_key", "example-value");
        p.put("location_region", "r1");
        p.put("location_agent", "a2");
        p.put("edges", EDGES);
        return p;
    }

    private static String cadl(Map<String, String> params) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type", "dummy");
        node.put("node_name", "n1");
        node.put("node_id", "n1");
        node.put("params", params);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("pipeline_id", "rp-1");
        payload.put("pipeline_name", "rp-1");
        payload.put("nodes", List.of(node));
        payload.put("edges", List.of());
        return GSON.toJson(payload);
    }

    private static List<MsgEvent> submit(Map<String, String> params) {
        List<MsgEvent> sent = new ArrayList<>();
        PluginBuilder plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>(), sent::add);
        ControllerEngine engine = new ControllerEngine(null, plugin, null, null);
        MsgEvent req = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a1", null, "r1", "a1", null, true, true);
        req.setParam("action", "rpipelinesubmit");
        req.setParam("cadl", cadl(params));
        MsgEvent reply = new RegionalExecutor(engine).executeCONFIG(req);
        assertEquals("true", reply.getParam("scheduled_regionally"), reply.getParams().toString());
        return sent;
    }

    private static Map<String, String> json(String s) {
        return GSON.fromJson(s, new TypeToken<Map<String, String>>() { }.getType());
    }

    @Test
    void theRegionalPluginaddCarriesTheConfigWhereTheAgentReadsIt() {
        List<MsgEvent> sent = submit(nodeParams());
        assertEquals(1, sent.size(), sent.toString());
        MsgEvent add = sent.get(0);
        assertEquals("pluginadd", add.getParam("action"));
        assertEquals("r1", add.getDstRegion());
        assertEquals("a2", add.getDstAgent());

        String cfg = add.getCompressedParam("configparams");
        assertNotNull(cfg, "no configparams: " + add.getParams());
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("pluginname", "io.cresco.example");
        expected.put("jarfile", "example.jar");
        expected.put("example_key", "example-value");
        expected.put("persistence_code", "10");   // the scheduler's default for CADL nodes
        assertEquals(expected, json(cfg));
        assertEquals(EDGES, add.getCompressedParam("edges"));

        // the config is not spread over the message any more
        for (String k : List.of("pluginname", "jarfile", "example_key", "location_region", "location_agent", "persistence_code")) {
            assertNull(add.getParam(k), k + " at top level: " + add.getParams());
        }
    }

    @Test
    void theRegionalPluginaddIsTheGlobalSchedulersPluginadd() {
        MsgEvent regional = submit(nodeParams()).get(0);

        // the global path for the same node: PollAddPipeline's addplugin, then ResourceScheduler's pluginadd
        PluginBuilder plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>());
        ControllerEngine engine = new ControllerEngine(null, plugin, null, null);
        gNode node = new gNode("dummy", "n1", "n1", nodeParams());
        MsgEvent global = new ResourceScheduler(engine)
                .pluginAddFor(PollAddPipeline.schedulingRequest(node, "rp-1"));

        assertEquals(global.getDstRegion(), regional.getDstRegion());
        assertEquals(global.getDstAgent(), regional.getDstAgent());
        assertEquals(global.getParam("action"), regional.getParam("action"));
        assertEquals(json(global.getCompressedParam("configparams")), json(regional.getCompressedParam("configparams")));
        assertEquals(global.getCompressedParam("edges"), regional.getCompressedParam("edges"));
        Map<String, String> extra = new HashMap<>(regional.getParams());
        extra.keySet().removeAll(global.getParams().keySet());
        assertEquals(Map.of("no_cost_route", "1"), extra, "regional-only params");
    }

    @Test
    void theAgentReadsTheRegionalConfigAndRepliesWithoutIt() {
        MsgEvent add = submit(nodeParams()).get(0);
        // no PluginAdmin here, so the add itself fails -- but only after the config was read (a missing
        // configparams fails earlier, on parsing it, with no PluginAdmin involved)
        ControllerEngine engine = new ControllerEngine(null,
                TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>()), null, null);
        MsgEvent reply = new AgentExecutor(engine).executeCONFIG(add);
        assertEquals("9", reply.getParam("status_code"));
        String error = String.valueOf(reply.getParam("error"));
        assertTrue(error.contains("getPluginAdmin"), "the config was not read: " + error);
        assertNull(reply.getParam("configparams"), reply.getParams().toString());
    }

    @Test
    void anExplicitPersistenceCodeIsKept() {
        Map<String, String> p = nodeParams();
        p.put("persistence_code", "5");
        p.remove("edges");
        MsgEvent add = submit(p).get(0);
        assertEquals("5", json(add.getCompressedParam("configparams")).get("persistence_code"));
        assertNull(add.getParam("edges"));
    }
}
