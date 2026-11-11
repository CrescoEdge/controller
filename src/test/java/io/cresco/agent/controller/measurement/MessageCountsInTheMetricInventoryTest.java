package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.MsgCounts;
import io.cresco.agent.controller.communication.MsgSigning;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What became of a node's messages is in the metric inventory a client queries ({@code getmetricinventory}), under
 * stable names: a test bed gates on the numbers, where it could only read log lines before.
 */
class MessageCountsInTheMetricInventoryTest {

    private static ControllerEngine node(String agent) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> { }, "r1", agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, "r1", agent);
        return e;
    }

    /** Refuse one unsigned message, drop one (a loop), originate and deliver one. */
    private static void traffic(ControllerEngine e, String agent) {
        MsgEvent unsigned = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a9", null, "r1", agent, null, false, false);
        unsigned.setParam("action", "noop");
        assertFalse(e.msgSigning().verify(unsigned, io.cresco.library.security.Caller.controller("t1", "r1", "a9"), false).ok());
        MsgEvent loop = new MsgEvent(MsgEvent.Type.EXEC, "r1", agent, null, "r1", agent, null, false, false);
        loop.setParam("action", "noop");
        loop.setParam("ttl", "50");
        e.msgIn(loop);
        MsgEvent own = new MsgEvent(MsgEvent.Type.EXEC, "r1", agent, null, "r1", agent, null, false, false);
        own.setParam("action", "noop");
        e.stampSelf(own);
        e.msgIn(own);
    }

    @Test
    void everyCountHasAMeterUnderAStableNameAndTheMeterReadsTheCount() {
        ControllerEngine e = node("m1");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Map<String, String> names = MsgMetrics.register(registry, e);
        Set<String> expected = new HashSet<>(List.of("msg.originated", "msg.relayed", "msg.delivered", "msg.held", "msg.hold.full",
                "msg.refused", "msg.dropped", "msg.sign.failed", "msg.trust.refused", "msg.filepart.dropped", "msg.tenant.unresolved",
                "msg.session.lost.told", "msg.session.lost.heard", "msg.session.lost.retried", "msg.send.held",
                "msg.route.held", "msg.route.mismatch", "msg.route.unread.patterns", "msg.expired", "msg.expired.on_arrival",
                "msg.expired.by_consumer", "broker.dlq.depth",
                "broker.caller.stripped", "broker.bridge.withheld", "broker.bridge.transit.withheld", "broker.bridge.discarded", "broker.anonymous.discarded",
                "broker.recipient.withheld", "broker.quarantine.connections"));
        for (String reason : List.of("unsigned", "not_for_this_node", "no_recipient", "too_old", "ahead_of_clock", "replay",
                "replay_memory_full", "credential", "revoked", "session_key", "bad_signature", "caller_mismatch", "other")) {
            expected.add("msg.refused." + reason);
        }
        for (String cause : List.of("ttl", "route_error", "tier_refused", "cross_tenant", "sender_unbound", "plugin_admin_unavailable", "plugin_unknown",
                "plugin_inactive", "controller_not_ready", "controller_stopping", "no_global", "parent_unknown",
                "source_route_not_relayed", "parent_link_down", "no_route", "no_sender", "hold_full", "parent_changed", "no_destination", "send_failed", "misrouted",
                "inbox_error", "file_group_refused", "file_group_expired", "hold_closed", "node_stopping", "sender_not_plugin")) {
            expected.add("msg.dropped." + cause);
        }
        assertEquals(expected, names.keySet(), "a name a test bed reads changed, or a reason or cause has no meter");
        for (MsgSigning.Refusal r : MsgSigning.Refusal.values()) assertTrue(names.containsKey(r.metric()), r + " has no meter");
        for (MsgCounts.Drop d : MsgCounts.Drop.values()) assertTrue(names.containsKey(d.metric()), d + " has no meter");
        for (Map.Entry<String, String> n : names.entrySet()) {
            assertEquals(n.getKey().startsWith("broker.") ? "broker" : "msg", n.getValue(), "the inventory group of " + n.getKey());
        }

        traffic(e, "m1");
        assertEquals(1.0, registry.get("msg.refused").functionCounter().count());
        assertEquals(1.0, registry.get("msg.refused.unsigned").functionCounter().count());
        assertEquals(0.0, registry.get("msg.refused.replay").functionCounter().count());
        assertEquals(1.0, registry.get("msg.dropped").functionCounter().count());
        assertEquals(1.0, registry.get("msg.dropped.ttl").functionCounter().count());
        assertEquals(1.0, registry.get("msg.originated").functionCounter().count());
        assertEquals(1.0, registry.get("msg.delivered").functionCounter().count());
        assertEquals(0.0, registry.get("msg.relayed").functionCounter().count());
        // no broker on this node: its counts read 0, not an error
        assertEquals(0.0, registry.get("broker.caller.stripped").functionCounter().count());
        assertEquals(0.0, registry.get("broker.quarantine.connections").gauge().value());
    }

    @Test
    @SuppressWarnings("unchecked")
    void theCountsAreInTheInventoryAClientQueries() {
        ControllerEngine e = node("m2");
        e.setMeasurementEngine(new MeasurementEngine(e.getPluginBuilder()));
        PerfControllerMonitor monitor = new PerfControllerMonitor(e);
        traffic(e, "m2");
        Map<String, Object> inventory = new Gson().fromJson(monitor.getMetricInventory("node", false, false), Map.class);
        Map<String, Object> bySource = (Map<String, Object>) inventory.get("metrics_by_source");
        Map<String, List<Map<String, String>>> groups = (Map<String, List<Map<String, String>>>) bySource.get("r1_m2:io.cresco.agent.controller");
        Map<String, String> msg = new HashMap<>(), brokerGroup = new HashMap<>();
        for (Map<String, String> m : groups.get("msg")) msg.put(m.get("name"), m.get("count"));
        for (Map<String, String> m : groups.get("broker")) brokerGroup.put(m.get("name"), m.get("count"));
        assertEquals("1.0", msg.get("msg.refused"), "in the inventory: " + msg);
        assertEquals("1.0", msg.get("msg.refused.unsigned"));
        assertEquals("0.0", msg.get("msg.refused.replay"));
        assertEquals("1.0", msg.get("msg.dropped"));
        assertEquals("1.0", msg.get("msg.dropped.ttl"));
        assertEquals("0.0", msg.get("msg.dropped.plugin_inactive"));
        assertEquals("1.0", msg.get("msg.originated"));
        assertEquals("1.0", msg.get("msg.delivered"));
        assertEquals("0.0", msg.get("msg.relayed"));
        assertEquals("0.0", msg.get("msg.held"));
        assertTrue(brokerGroup.containsKey("broker.caller.stripped") && brokerGroup.containsKey("broker.bridge.withheld")
                && brokerGroup.containsKey("broker.bridge.discarded"), "the broker's counts are not in the inventory: " + brokerGroup.keySet());
    }

    @Test
    void aBrokersCountNeverGoesBackWhenTheNodeMakesItsBrokerAnew() {
        long[] counted = {7};
        MsgMetrics.BrokerCount c = new MsgMetrics.BrokerCount(a -> counted[0]);
        assertEquals(0, c.read(null), "no broker yet");
        io.cresco.agent.controller.communication.CrescoAuthorizationBroker first = broker(), second = broker();
        assertEquals(7, c.read(first));
        counted[0] = 9;
        assertEquals(9, c.read(first));
        counted[0] = 2;                                      // another broker, counting from zero
        assertEquals(11, c.read(second), "the count went back when the broker was made anew");
        assertEquals(11, c.read(null), "the count went back when the broker stopped");
    }

    private static io.cresco.agent.controller.communication.CrescoAuthorizationBroker broker() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg);
        return new io.cresco.agent.controller.communication.CrescoAuthorizationBroker(pb, null);
    }
}
