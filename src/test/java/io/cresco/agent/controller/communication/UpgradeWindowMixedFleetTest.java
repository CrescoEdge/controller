package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.security.TenantNamespace;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The upgrade window with nodes of an earlier 1.3 build, which run without tenant namespacing (they write their
 * controller's RAW inbox and listen on their own raw inbox): the root cause of the mixed-fleet failure was that an upgraded
 * controller consumed and addressed only {@code T.<tenant>.} queues, so an earlier build's registration landed in a queue
 * nobody read. Inside the window the controller also consumes its raw inbox, marks the sender as legacy (from the broker's
 * {@code agent:} stamp) and answers it on its raw inbox; outside the window, none of that.
 */
class UpgradeWindowMixedFleetTest {

    private static final String VM = "vm://localhost";
    private static final String RAW = TestAgentService.AGENT_PATH;          // r1_a1: this controller's raw inbox
    private static final String WINDOW_END = LocalDate.now(ZoneOffset.UTC).plusDays(7).toString();

    private BrokerService broker;

    private static PluginBuilder plugin(boolean window) {
        Map<String, Object> m = new HashMap<>();
        m.put("tenant_namespacing", "true");
        m.put("tenant_id", "t1");
        if (window) {
            m.put("discovery_auth", "compat");
            m.put("discovery_compat_until", WINDOW_END);
        }
        return TestAgentService.plugin(System.getProperty("java.io.tmpdir"), m);
    }

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        // no authorization plugin: the test writes the broker's agent: stamp itself (stamping is CallerStampingBrokerTest's)
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterEach
    void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private long enqueued(String queue) throws Exception {
        org.apache.activemq.broker.region.Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getDestinationStatistics().getEnqueues().getCount();
    }

    private long dequeued(String queue) throws Exception {
        org.apache.activemq.broker.region.Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getDestinationStatistics().getDequeues().getCount();
    }

    private static void await(java.util.concurrent.Callable<Boolean> c, String what) throws Exception {
        long end = System.currentTimeMillis() + 10_000;
        while (!c.call() && System.currentTimeMillis() < end) Thread.sleep(20);
        assertTrue(c.call(), what);
    }

    /** What an earlier build sends: its registration, on the raw inbox, stamped by the broker as {@code agent:} (here set directly). */
    private void legacySend(String fromAgent) throws Exception {
        ActiveMQConnection vm = (ActiveMQConnection) new ActiveMQConnectionFactory(VM + "?create=false").createConnection();
        try {
            vm.start();
            Session s = vm.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MsgEvent m = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", fromAgent, null, "r1", "a1", null, false, false);
            jakarta.jms.TextMessage t = s.createTextMessage(new com.google.gson.Gson().toJson(m));
            t.setStringProperty(CallerStamps.P_CALLER, "agent:t1/r1/" + fromAgent);
            t.setStringProperty(CallerStamps.P_TENANT, "t1");
            t.setStringProperty(CallerStamps.P_ROLES, "user");
            s.createProducer(s.createQueue(RAW)).send(t);
        } finally {
            vm.close();
        }
    }

    @Test
    void insideTheWindowTheControllerConsumesItsRawInboxAndAnswersALegacyNodeOnItsRawInbox() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(true), null, null);
        assertTrue(engine.legacyNodesAdmitted());
        AgentConsumer consumer = new AgentConsumer(engine, TenantNamespace.wildcard(RAW), VM);
        AgentProducer producer = new AgentProducer(engine, VM, engine.getActiveClient());
        try {
            assertEquals(RAW, consumer.legacyInbox(VM), "inside the window the raw inbox is consumed too");
            legacySend("a9");
            await(() -> dequeued(RAW) == 1, "the controller did not consume an earlier build's message on its raw inbox");
            await(() -> engine.legacyNodes().isLegacy("r1_a9"), "the sender was not marked legacy");

            // what the controller sends to it goes to its raw inbox (where an earlier build listens), not T.t1.r1_a9
            MsgEvent reply = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a9", null, false, false);
            assertTrue(producer.sendMessage(reply));
            await(() -> enqueued("r1_a9") == 1, "a message to a legacy node did not go to its raw inbox");
            assertEquals(0, enqueued(TenantNamespace.qualify("t1", "r1_a9")));

            // the node upgraded: it now speaks as its own controller -- the mark is gone, messages to it are namespaced
            engine.legacyNodes().upgraded("r1", "a9");
            assertFalse(engine.legacyNodes().isLegacy("r1_a9"));
            assertTrue(producer.sendMessage(new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a9", null, false, false)));
            await(() -> enqueued(TenantNamespace.qualify("t1", "r1_a9")) == 1, "a message to an upgraded node was not namespaced");
        } finally {
            producer.shutdown();
            consumer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void outsideTheWindowNothingOfItHappens() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(false), null, null);
        assertFalse(engine.legacyNodesAdmitted());
        AgentConsumer consumer = new AgentConsumer(engine, TenantNamespace.wildcard(RAW), VM);
        AgentProducer producer = new AgentProducer(engine, VM, engine.getActiveClient());
        try {
            assertNull(consumer.legacyInbox(VM), "the raw inbox was consumed outside the window");
            assertNull(consumer.legacyInbox("failover:(nio+ssl://10.0.0.1:32010)"));
            legacySend("a9");
            Thread.sleep(500);
            assertEquals(0, dequeued(RAW), "an earlier build's message was consumed outside the window");
            assertFalse(engine.legacyNodes().isLegacy("r1_a9"));
            // a mark left from a window that closed at run time no longer routes anything raw
            engine.legacyNodes().seen("r1", "a9");
            assertTrue(producer.sendMessage(new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a9", null, false, false)));
            await(() -> enqueued(TenantNamespace.qualify("t1", "r1_a9")) == 1, "outside the window a message was not namespaced");
            assertEquals(0, enqueued("r1_a9"));
        } finally {
            producer.shutdown();
            consumer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void legacyMarksAreBoundedAndCleared() {
        LegacyNodes n = new LegacyNodes();
        assertFalse(n.isLegacy("r1_a1"), "nothing is legacy before anything was seen");
        n.seen("r1", "a1");
        n.seen(null, "a2");
        assertTrue(n.isLegacy("r1_a1"));
        assertFalse(n.isLegacy("null_a2"));
        n.upgraded("r1", "a1");
        assertFalse(n.isLegacy("r1_a1"));
        for (int i = 0; i < LegacyNodes.MAX_NODES + 50; i++) n.seen("r1", "n" + i);
        assertTrue(n.size() <= LegacyNodes.MAX_NODES, "the legacy set grew past its bound: " + n.size());
        n.clear();
        assertFalse(n.isLegacy("r1_n7"));
    }

    @Test
    void theWindowLetsANodeOfTheRegionWriteItsControllersRawInboxAndNothingElse() throws Exception {
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(false));
        BrokerService b = new BrokerService();
        b.setBrokerName("r1_rc");
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setPlugins(new BrokerPlugin[]{authz});
        b.start();
        try {
            boolean[] open = {true};
            authz.setUpgradeWindow(() -> open[0]);
            CrescoIdentity otherTenant = CrescoIdentity.of("t2", "r1", "a9", null);
            assertTrue(authz.legacyRawInboxWrite(otherTenant, "r1_rc"), "an earlier build of another tenant of this region writes its controller's raw inbox");
            assertFalse(authz.legacyRawInboxWrite(CrescoIdentity.of("t2", "r2", "a9", null), "r1_rc"), "a node of another region");
            assertFalse(authz.legacyRawInboxWrite(otherTenant, "r1_a2"), "another node's raw inbox");
            assertFalse(authz.legacyRawInboxWrite(otherTenant, "T.t1.r1_rc"), "the namespaced inbox follows the tenant policy");
            assertFalse(authz.legacyRawInboxWrite(null, "r1_rc"), "an unverified connection");
            open[0] = false;
            assertFalse(authz.legacyRawInboxWrite(otherTenant, "r1_rc"), "outside the window");
        } finally {
            b.stop();
        }
    }
}
