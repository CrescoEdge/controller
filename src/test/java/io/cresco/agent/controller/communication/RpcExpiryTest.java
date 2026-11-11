package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An RPC request lives in a broker as long as its caller waits (plus the clock margin), not five minutes. Before, every
 * message left with a time to live of 300 s: a request that waited in a queue for a consumer that came back was delivered,
 * and run, long after its caller had given up.
 */
class RpcExpiryTest {

    private static final String VM = "vm://localhost";
    private static final long USUAL = 300_000L, MARGIN = 60_000L;

    private BrokerService broker;

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterEach
    void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    /** An RPC request of r1/a1's plugin for r2/a9 that states (or not) how long its caller waits. */
    private static MsgEvent request(MsgEvent.Type type, String action, String timeout) {
        MsgEvent m = new MsgEvent(type, "r1", "a1", "plugin/9", "r2", "a9", null, false, false);
        if (action != null) m.setParam("action", action);
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a1-plugin/9", "call-1");
        if (timeout != null) m.setParam(RpcExpiry.TIMEOUT_PARAM, timeout);
        return m;
    }

    @Test
    void aRequestLivesItsCallersTimeoutPlusTheClockMargin() {
        assertEquals(5_000 + MARGIN, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "ping", "5000"), USUAL, MARGIN));
        assertEquals(30_000 + MARGIN, RpcExpiry.ttlMs(request(MsgEvent.Type.CONFIG, "x", "30000"), USUAL, MARGIN));
        assertEquals(5_000, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", "5000"), USUAL, 0), "the margin is the operator's to set");
    }

    @Test
    void aRequestNeverLivesLongerThanMessagesDidBefore() {
        assertEquals(USUAL, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", "3600000"), USUAL, MARGIN), "an hour's timeout");
        assertEquals(USUAL, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", String.valueOf(Long.MAX_VALUE)), USUAL, MARGIN));
        assertEquals(5_000 + MARGIN, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", "5000"), 0, MARGIN), "0 is JMS for 'for ever'");
    }

    @Test
    void whatStatesNoTimeoutOrIsNoRequestKeepsTheUsualLife() {
        assertEquals(USUAL, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", null), USUAL, MARGIN), "a caller of an earlier build");
        for (String nonsense : new String[]{"", "soon", "0", "-5", "1e3"}) {
            assertEquals(USUAL, RpcExpiry.ttlMs(request(MsgEvent.Type.EXEC, "x", nonsense), USUAL, MARGIN), "'" + nonsense + "'");
        }
        MsgEvent reply = request(MsgEvent.Type.EXEC, "x", "5000");
        reply.setReturn();                                                   // the answer carries the request's parameters back
        assertEquals(USUAL, RpcExpiry.ttlMs(reply, USUAL, MARGIN), "an answer is not cut short by the timeout of its request");
        MsgEvent notRpc = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r2", "a9", null, false, false);
        notRpc.setParam(RpcExpiry.TIMEOUT_PARAM, "5000");
        assertEquals(USUAL, RpcExpiry.ttlMs(notRpc, USUAL, MARGIN));
        MsgEvent failed = request(MsgEvent.Type.EXEC, "x", "5000");
        failed.setParam(MsgRouter.ROUTE_UNAVAILABLE, "REMOTE_REGION");
        assertEquals(USUAL, RpcExpiry.ttlMs(failed, USUAL, MARGIN), "the error reply of a router");
    }

    @Test
    void theMarginIsTheClockDifferenceTheSigningRuleAccepts() {
        Map<String, Object> cfg = new HashMap<>();
        assertEquals(60_000L, RpcExpiry.marginMs(TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg).getConfig()));
        cfg.put("msg_signature_max_future_ms", "20000");
        assertEquals(20_000L, RpcExpiry.marginMs(TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg).getConfig()),
                "clocks allowed to differ by less: a smaller margin");
        cfg.put("rpc_expiry_margin_ms", "90000");
        assertEquals(90_000L, RpcExpiry.marginMs(TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg).getConfig()));
    }

    /** The life (expiry minus timestamp, both the sender's clock) the broker holds a message of {@code producer} with. */
    private long lifeOnTheWire(AgentProducer producer, MsgEvent m, String queue) throws Exception {
        Connection c = new ActiveMQConnectionFactory(VM + "?create=false").createConnection();
        try {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            assertTrue(producer.sendMessage(m));
            Message got = consumer.receive(5000);
            assertNotNull(got, "nothing arrived on " + queue);
            return got.getJMSExpiration() - got.getJMSTimestamp();
        } finally {
            c.close();
        }
    }

    @Test
    void aRequestLeavesWithThatExpiryOnBothSendersAndEverythingElseAsBefore() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_namespacing", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg);
        ControllerEngine engine = new ControllerEngine(null, plugin, null, null);
        AgentProducer producer = new AgentProducer(engine, VM, engine.getActiveClient());
        try {
            // the control-plane sender (EXEC, CONFIG, pings)
            assertEquals(5_000 + MARGIN, lifeOnTheWire(producer, request(MsgEvent.Type.EXEC, "ping", "5000"), "T.t1.r2_a9"),
                    "a ping of a 5 s timeout was given the life of every message (300 s)");
            assertEquals(USUAL, lifeOnTheWire(producer, request(MsgEvent.Type.EXEC, "x", null), "T.t1.r2_a9"));
            // the per-destination workers (INFO and the other telemetry types)
            assertEquals(2_500 + MARGIN, lifeOnTheWire(producer, request(MsgEvent.Type.INFO, null, "2500"), "T.t1.r2_a9"));
            assertEquals(USUAL, lifeOnTheWire(producer, request(MsgEvent.Type.INFO, null, null), "T.t1.r2_a9"));
            MsgEvent reply = request(MsgEvent.Type.EXEC, "x", "5000");
            reply.setReturn();
            assertEquals(USUAL, lifeOnTheWire(producer, reply, "T.t1.r1_a1"), "an answer was given its request's short life");
        } finally {
            producer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void theControllersOwnTimedCallsSayHowLongTheyWait() {
        java.util.List<MsgEvent> out = new java.util.concurrent.CopyOnWriteArrayList<>();
        PluginBuilder plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>(), out::add);
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r2", "a9", null, true, false);
        ping.setParam("action", "ping");
        assertNull(RpcExpiry.call(plugin, ping, 200), "nobody answers: the call times out");
        assertEquals(1, out.size());
        assertEquals("200", out.get(0).getParam(RpcExpiry.TIMEOUT_PARAM));
        assertEquals(200, RpcExpiry.statedTimeoutMs(out.get(0)), "it is an RPC request that states its caller's timeout");
    }
}
