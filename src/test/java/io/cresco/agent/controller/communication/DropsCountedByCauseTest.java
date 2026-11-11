package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A message that ends on a node without reaching its recipient is counted there, once, under the cause a person would
 * act on, and logged at that cause's first occurrence and then at most every 10 s. Until now a drop was a log line at
 * best (some sites said nothing), and none was a number.
 */
class DropsCountedByCauseTest {

    private static ControllerEngine agent(String agent, Map<String, Object> extra) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "false");         // what is counted here does not depend on who sent it
        cfg.put("tenant_namespacing", "false");
        cfg.put("msgin_fault_wait_ms", "100");
        cfg.put("route_ready_wait_ms", "100");
        if (extra != null) cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> { }, "r1", agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, "r1", agent);
        return e;
    }

    private static MsgEvent msg(String fromAgent, String toRegion, String toAgent, String toPlugin) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", fromAgent, null, toRegion, toAgent, toPlugin, false, false);
        m.setParam("action", "noop");
        return m;
    }

    private static long only(MsgCounts c, MsgCounts.Drop cause) {
        for (MsgCounts.Drop d : MsgCounts.Drop.values()) {
            assertEquals(d == cause ? c.droppedCount(d) : 0L, c.droppedCount(d), "dropped as " + d);
        }
        return c.droppedCount(cause);
    }

    @Test
    void aMessageRoutedTooOftenIsCountedAsALoop() {
        ControllerEngine e = agent("d1", null);
        MsgEvent m = msg("d1", "r1", "d1", null);
        m.setParam("ttl", String.valueOf(MsgRouter.TTL_MAX + 1));
        e.msgIn(m);
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.TTL));
        assertEquals(1, e.msgCounts().droppedCount());
        assertEquals(0, e.msgCounts().deliveredCount());
    }

    @Test
    void aMessageForAPluginOfANodeWithoutAPluginAdminIsCounted() {
        ControllerEngine e = agent("d2", null);
        e.msgIn(msg("d9", "r1", "d2", "plugin-x"), true);
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.PLUGIN_ADMIN_UNAVAILABLE));
    }

    @Test
    void aSendWithoutAProducerIsCountedOnceThoughTheRouterReportsItToo() {
        ControllerEngine e = agent("d3", null);
        // straight to the router: the engine would first wait for the parent link (counted in the next test)
        new MsgRouter(e).route(msg("d3", "r1", "d8", null));
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.NO_SENDER), "the router's 'route unavailable' counted the failed send again");
        assertEquals(1, e.msgCounts().droppedCount());
        assertEquals(0, e.msgCounts().relayedCount());
    }

    @Test
    void aMessageThatWaitsOutTheParentLinksFaultIsCounted() {
        ControllerEngine e = agent("d4", null);
        e.msgIn(msg("d4", "r1", "d8", null));                // no link to a parent broker: it waits msgin_fault_wait_ms
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.PARENT_LINK_DOWN));
    }

    @Test
    void aMessageForTheRegionalControllerOfAnAgentThatKnowsNoneIsCountedAfterTheWait() {
        ControllerEngine e = agent("d5", null);
        // a regional message of this agent's own controller: for its regional controller, which it has not joined
        MsgEvent m = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "d5", null, "r1", "d5", null, true, false);
        new MsgRouter(e).route(m);
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.PARENT_UNKNOWN));
    }

    @Test
    void aMessageTheRouterFailsOnIsCounted() {
        ControllerEngine e = agent("d6", null);
        MsgEvent noParams = new Gson().fromJson("{\"msgType\":\"EXEC\",\"dst_region\":\"r1\",\"dst_agent\":\"d6\"}", MsgEvent.class);
        new MsgRouter(e).route(noParams);
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.ROUTE_ERROR));
    }

    @Test
    void aMessageTakenOffTheWireAsTheNodeStopsIsCounted() throws Exception {
        ControllerEngine e = agent("d11", null);
        java.lang.reflect.Field pool = ControllerEngine.class.getDeclaredField("msgInProcessQueue");
        pool.setAccessible(true);
        ((java.util.concurrent.ExecutorService) pool.get(e)).shutdown();         // as stop() does
        e.msgInThreaded(msg("d9", "r1", "d11", null));       // the pool would discard it without a word
        assertEquals(1, only(e.msgCounts(), MsgCounts.Drop.NODE_STOPPING));
        assertEquals(0, e.msgCounts().deliveredCount());
    }

    @Test
    void theFirstDropOfEachCauseIsLoggedAtOnceWithTheNodesTotalAndTheCausesOwnCount() {
        assertEquals(10_000L, MsgCounts.LINE_EVERY_MS);
        ControllerEngine e = agent("d7", null);
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        e.msgCounts().lineTap = lines::add;
        for (int i = 0; i < 3; i++) e.msgIn(msg("d9", "r1", "d7", "plugin-x"), true);
        assertEquals(1, lines.size(), "three drops of one cause within 10 s: " + lines);
        assertEquals("MESSAGE DROPPED: a message for a plugin while this node's plugin admin is not available [EXEC noop] r1_d9/null -> r1_d7/plugin-x"
                + " (1 dropped so far; 1 of this kind, plugin_admin_unavailable; each kind is logged at most every 10 s)", lines.get(0));
        // another cause is logged at once, never behind the first one's line
        MsgEvent loop = msg("d7", "r1", "d7", null);
        loop.setParam("ttl", "-5");
        e.msgIn(loop);
        assertEquals(2, lines.size(), "the first drop of another cause was not logged at once: " + lines);
        assertTrue(lines.get(1).startsWith("MESSAGE DROPPED: a message routed too many times"), lines.get(1));
        assertTrue(lines.get(1).endsWith(" (4 dropped so far; 1 of this kind, ttl; each kind is logged at most every 10 s)"), lines.get(1));
        assertEquals(4, e.msgCounts().droppedCount());
    }

    @Test
    void whatAMessageSaysOfItselfDoesNotWriteTheLog() {
        ControllerEngine e = agent("d12", null);
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        e.msgCounts().lineTap = lines::add;
        MsgEvent m = msg("d9\nMESSAGE DROPPED: forged", "r1", "d12", "plugin-x");
        m.setParam("action", "noop]\n2026-11-10 ERROR something else happened [");
        e.msgIn(m, true);
        assertEquals(1, lines.size());
        assertFalse(lines.get(0).contains("\n") || lines.get(0).contains("forged") || lines.get(0).contains("something else"), lines.get(0));
        assertTrue(lines.get(0).contains(" [EXEC (") && lines.get(0).contains(" chars)] r1_("), "not written by its length: " + lines.get(0));
    }

    @Test
    void aFilePartIsCountedByItselfAndNeverAmongTheMessagesDropped() {
        ControllerEngine e = agent("d10", null);
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        e.msgCounts().lineTap = lines::add;
        e.msgCounts().dropped(MsgCounts.Drop.FILE_PART, 1, "a part for no file group this node holds, group", "g1");
        assertEquals(1, e.msgCounts().droppedCount(MsgCounts.Drop.FILE_PART));
        assertEquals(0, e.msgCounts().droppedCount(), "a file part is no MsgEvent: it would break the account of the messages");
        assertFalse(MsgCounts.Drop.FILE_PART.ofMessage());
        assertEquals("MESSAGE DROPPED: a file part this node did not take [a part for no file group this node holds, group g1]"
                + " (0 dropped so far; 1 of this kind, file_part; each kind is logged at most every 10 s)", lines.get(0));
    }
}
