package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An iNode's plugin config travels to the agent in pluginadd (and to the global in addplugin) and must not come
 * back in the reply: the reply goes to whoever asked (a wsapi client, the scheduler) and into their logs, and
 * it was the whole config, secrets included, gzip-base64 (GaiaKeep OUT-03).
 */
class PluginAddReplyRedactionTest {

    private static final String CONFIG = "{\"pluginname\":\"io.cresco.gfs\",\"gfs_secret\":\"s3cr3t-E\"}";

    private static PluginBuilder plugin() {
        return TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>());
    }

    private static MsgEvent config(String action) {
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a1", null, "r1", "a1", null, true, true);
        me.setParam("action", action);
        me.setCompressedParam("configparams", CONFIG);
        return me;
    }

    @Test
    void thePluginaddReplyCarriesNoConfig() {
        // no PluginAdmin: the add fails, the failure path is the one that used to echo the request whole
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        MsgEvent reply = new AgentExecutor(engine).executeCONFIG(config("pluginadd"));
        assertNotNull(reply);
        assertEquals("9", reply.getParam("status_code"));
        assertNull(reply.getParam("configparams"), reply.getParams().toString());
    }

    @Test
    void theAddpluginReplyCarriesNoConfig() {
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        MsgEvent req = config("addplugin");
        req.setParam("inode_id", "i-1");
        req.setParam("resource_id", "p-1");
        MsgEvent reply = new GlobalExecutor(engine).executeCONFIG(req);
        assertNotNull(reply);
        assertNull(reply.getParam("configparams"), reply.getParams().toString());
    }
}
