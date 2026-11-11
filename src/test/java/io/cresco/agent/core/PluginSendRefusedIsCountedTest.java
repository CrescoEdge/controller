package io.cresco.agent.core;

import io.cresco.agent.controller.communication.MsgCounts;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.MockBundle;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A bundle's send as a plugin it does not run is dropped by the agent (its line is written once a minute): it is counted
 * as a message this node was handed and as one it dropped, so that it is a number, and every message stays accounted for.
 */
class PluginSendRefusedIsCountedTest {

    @Test
    void aSendAsAPluginTheBundleDoesNotRunIsCountedAsOriginatedAndDropped() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "false");
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg);
        ControllerEngine engine = new ControllerEngine(null, pb, null, null);
        AgentServiceImpl service = new AgentServiceImpl();
        Field f = AgentServiceImpl.class.getDeclaredField("controllerEngine");
        f.setAccessible(true);
        f.set(service, engine);

        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin-other", "r1", "a1", null, false, false);
        m.setParam("action", "noop");
        for (int i = 0; i < 3; i++) service.msgOutFromBundle(new MockBundle("io.cresco.some", "1.0"), "plugin-other", m);

        MsgCounts c = engine.msgCounts();
        assertEquals(3, c.droppedCount(MsgCounts.Drop.SENDER_NOT_PLUGIN));
        assertEquals(3, c.droppedCount());
        assertEquals(3, c.originatedCount(), "dropped without being counted as handed to this node: the account of its messages would not add up");
        assertEquals(0, c.deliveredCount());
    }
}
