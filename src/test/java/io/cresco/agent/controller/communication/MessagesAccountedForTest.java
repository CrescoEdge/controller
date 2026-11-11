package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.jms.Session;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every control-plane message is accounted for: summed over the nodes, what they originated is what they delivered,
 * refused and dropped. A relay and a hold are not ends (a relayed message ends at its next node, a held one when it is
 * judged), and each is counted by itself. Nodes with their real inbox and producer on one broker.
 */
class MessagesAccountedForTest {

    private static final String VM = "vm://accounted";
    private BrokerService broker;
    private final List<ControllerEngine> nodes = new ArrayList<>();

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("accounted");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterEach
    void stop() throws Exception {
        for (ControllerEngine e : nodes) {
            try { e.getActiveClient().shutdown(); } catch (Exception ignore) { }
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /** A node of region r1 with its inbox and its producer on the broker; {@code security}: it verifies what it receives. */
    private ControllerEngine node(String agent, boolean security, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", String.valueOf(security));
        cfg.put("tenant_namespacing", "false");
        cfg.put("tenant_id", "t1");
        cfg.put("route_ready_wait_ms", "100");
        if (extra != null) cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> { }, "r1", agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, "r1", agent);
        nodes.add(e);
        return e;
    }

    private static void online(ControllerEngine e) {
        assertTrue(e.getActiveClient().initActiveAgentConsumer(e.cstate.getAgentPath(), VM + "?create=false"));
        assertTrue(e.getActiveClient().initActiveAgentProducer(VM + "?create=false"));
    }

    private static void await(java.util.concurrent.Callable<Boolean> c, String what) throws Exception {
        long end = System.currentTimeMillis() + 10_000;
        while (!c.call() && System.currentTimeMillis() < end) Thread.sleep(10);
        assertTrue(c.call(), what);
    }

    private static MsgEvent msg(String from, String to, String toPlugin) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", from, null, "r1", to, toPlugin, false, false);
        m.setParam("action", "noop");
        return m;
    }

    /** As the controller's own send does (AgentServiceImpl.msgOut): stamped as this node's, then routed. */
    private static void send(ControllerEngine from, MsgEvent m) {
        from.stampSelf(m);
        from.msgIn(m);
    }

    private long sum(java.util.function.ToLongFunction<ControllerEngine> f) {
        long n = 0;
        for (ControllerEngine e : nodes) n += f.applyAsLong(e);
        return n;
    }

    @Test
    void whatTheNodesOriginatedIsWhatTheyDeliveredRefusedAndDropped() throws Exception {
        ControllerEngine a = node("a1", false, null), b = node("a2", false, null);
        online(a);
        online(b);
        for (int i = 0; i < 5; i++) send(a, msg("a1", "a2", null));              // delivered to b's controller
        for (int i = 0; i < 3; i++) send(a, msg("a1", "a2", "plugin-x"));        // b runs no plugins: dropped at b
        send(b, msg("a2", "a2", null));                                          // b to itself: never leaves b
        MsgEvent loop = msg("a1", "a1", null);
        loop.setParam("ttl", "99");
        send(a, loop);                                                           // dropped where it started

        await(() -> b.msgCounts().deliveredCount() == 6 && b.msgCounts().droppedCount() == 3, "b: delivered "
                + b.msgCounts().deliveredCount() + " of 6, dropped " + b.msgCounts().droppedCount() + " of 3");
        assertEquals(9, a.msgCounts().originatedCount());
        assertEquals(1, b.msgCounts().originatedCount());
        assertEquals(1, a.msgCounts().droppedCount(MsgCounts.Drop.TTL));
        assertEquals(3, b.msgCounts().droppedCount(MsgCounts.Drop.PLUGIN_ADMIN_UNAVAILABLE));
        assertEquals(0, a.msgCounts().deliveredCount());
        assertEquals(sum(e -> e.msgCounts().originatedCount()),
                sum(e -> e.msgCounts().deliveredCount()) + sum(e -> e.msgSigning().refusedCount()) + sum(e -> e.msgCounts().droppedCount()),
                "a message is not accounted for");
        assertEquals(0, sum(e -> e.msgCounts().relayedCount()), "nothing went through a third node");
    }

    @Test
    void aMessageCarriedOnForAnotherNodeIsRelayedAndEndsAtThatNode() throws Exception {
        ControllerEngine a = node("a1", false, null), b = node("a2", false, null), c = node("a3", false, null);
        online(a);
        online(b);
        online(c);
        // a's message for c reaches b's inbox (as a broker network delivers it through b): b carries it on
        MsgEvent m = msg("a1", "a3", null);
        a.stampSelf(m);
        try (ActiveMQConnection raw = (ActiveMQConnection) new ActiveMQConnectionFactory(VM + "?create=false").createConnection()) {
            raw.start();
            Session s = raw.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createQueue("r1_a2")).send(s.createTextMessage(new com.google.gson.Gson().toJson(m)));
        }
        await(() -> c.msgCounts().deliveredCount() == 1, "the message b carried on did not reach c");
        assertEquals(1, b.msgCounts().relayedCount());
        assertEquals(0, b.msgCounts().deliveredCount(), "a relay is not a delivery");
        assertEquals(0, b.msgCounts().droppedCount());
        assertEquals(0, c.msgCounts().relayedCount());
        // what a node originates and sends is not relayed
        send(a, msg("a1", "a3", null));
        await(() -> c.msgCounts().deliveredCount() == 2, "a's own message did not reach c");
        assertEquals(0, a.msgCounts().relayedCount());
        assertEquals(sum(e -> e.msgCounts().originatedCount()),
                sum(e -> e.msgCounts().deliveredCount()) + sum(e -> e.msgSigning().refusedCount()) + sum(e -> e.msgCounts().droppedCount()));
    }

    @Test
    void aSendTheBrokerDoesNotTakeIsCountedOnceWhereItFailed() throws Exception {
        ControllerEngine a = node("a1", false, null);
        online(a);
        broker.stop();
        broker.waitUntilStopped();
        // straight to the router (the engine would first wait for the parent link to come back)
        new MsgRouter(a).route(msg("a1", "a2", null));
        assertEquals(1, a.msgCounts().droppedCount(MsgCounts.Drop.SEND_FAILED));
        assertEquals(1, a.msgCounts().droppedCount(), "the failed send was counted again by what reported it");
        assertEquals(0, a.msgCounts().relayedCount());
    }

    @Test
    void aMessageTheInboxHoldsAndThenRefusesIsCountedHeldOnceAndRefusedUnderItsReason() throws Exception {
        // b verifies what it receives, and has joined a region whose trust data has not arrived: it can judge nobody yet
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("msg_signing_hold_ms", "300");
        ControllerEngine b = node("a2", true, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) b.cstate;
        st.parentRegion = "r1";
        st.parentAgent = "rc";
        assertTrue(b.msgSigning().required());
        assertFalse(b.trustReady());
        online(b);
        // 300 unsigned messages of another node's controller, as its broker stamped them
        int sent = 300, room = 2048 / TrustHold.SENDER_DIVISOR;
        try (ActiveMQConnection raw = (ActiveMQConnection) new ActiveMQConnectionFactory(VM + "?create=false").createConnection()) {
            raw.start();
            Session s = raw.createSession(false, Session.AUTO_ACKNOWLEDGE);
            jakarta.jms.MessageProducer p = s.createProducer(s.createQueue("r1_a2"));
            for (int i = 0; i < sent; i++) {
                jakarta.jms.TextMessage t = s.createTextMessage(new com.google.gson.Gson().toJson(msg("a7", "a2", null)));
                t.setStringProperty(CallerStamps.P_CALLER, "controller:t1/r1/a7");
                t.setStringProperty(CallerStamps.P_TENANT, "t1");
                t.setStringProperty(CallerStamps.P_ROLES, "infra");
                p.send(t);
            }
        }
        await(() -> b.msgSigning().refusedCount() == sent, "refused " + b.msgSigning().refusedCount() + " of " + sent);
        assertEquals(sent, b.msgSigning().refusedCount(MsgSigning.Refusal.UNSIGNED), "held or not, each is refused under its reason");
        assertEquals(room, b.msgCounts().heldCount(), "one sender's share of the hold, each message counted once");
        assertEquals(sent - room, b.msgCounts().notHeldCount(), "the messages the hold had no room for");
        assertEquals(0, b.msgCounts().deliveredCount());
        assertEquals(0, b.msgCounts().droppedCount(), "a refused message is not also a dropped one");
    }

    @Test
    void whatTheInboxHoldsWhenItClosesIsCountedAsDropped() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("msg_signing_hold_ms", "60000");
        ControllerEngine b = node("a2", true, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) b.cstate;
        st.parentRegion = "r1";
        st.parentAgent = "rc";
        online(b);
        try (ActiveMQConnection raw = (ActiveMQConnection) new ActiveMQConnectionFactory(VM + "?create=false").createConnection()) {
            raw.start();
            Session s = raw.createSession(false, Session.AUTO_ACKNOWLEDGE);
            jakarta.jms.MessageProducer p = s.createProducer(s.createQueue("r1_a2"));
            for (int i = 0; i < 4; i++) {
                jakarta.jms.TextMessage t = s.createTextMessage(new com.google.gson.Gson().toJson(msg("a7", "a2", null)));
                t.setStringProperty(CallerStamps.P_CALLER, "controller:t1/r1/a7");
                t.setStringProperty(CallerStamps.P_TENANT, "t1");
                t.setStringProperty(CallerStamps.P_ROLES, "infra");
                p.send(t);
            }
        }
        await(() -> b.msgCounts().heldCount() == 4, "held " + b.msgCounts().heldCount() + " of 4");
        b.getActiveClient().shutdown();                      // the inbox closes with them
        assertEquals(4, b.msgCounts().droppedCount(MsgCounts.Drop.HOLD_CLOSED));
        assertEquals(0, b.msgSigning().refusedCount());
    }
}
