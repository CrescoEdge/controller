package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import com.google.gson.Gson;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * controller#24 and #25: a dead dedicated control connection recovers on its own. The inbox consumer rebuilds when its
 * socket dies (including a real broker restart) and shutdown wins over a rebuild; the producer drops a worker whose
 * own connection failed, and every worker when the base URI failed.
 */
class ControlPlaneRecoveryTest {

    private static final String INBOX = "r1_a1";
    private BrokerService broker;
    private int port;
    private String uri;

    private static PluginBuilder plugin() {
        Map<String, Object> m = new HashMap<>();
        m.put("agentconsumer_dedicated_connection", "true");
        return TestAgentService.plugin(System.getProperty("java.io.tmpdir"), m);
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("recovery");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://127.0.0.1:" + port);
        broker.start();
        broker.waitUntilStarted();
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        uri = "tcp://127.0.0.1:" + port;
        startBroker();
        AgentConsumer.REBUILD_INITIAL_BACKOFF_MS = 100;
        AgentConsumer.REBUILD_MAX_BACKOFF_MS = 500;
    }

    @AfterEach
    void tearDown() throws Exception {
        AgentConsumer.REBUILD_INITIAL_BACKOFF_MS = 2_000;
        AgentConsumer.REBUILD_MAX_BACKOFF_MS = 30_000;
        stopBroker();
    }

    private int brokerClients() throws Exception {
        return broker.getBroker().getClients().length;
    }

    private long dequeued(String queue) throws Exception {
        org.apache.activemq.broker.region.Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getDestinationStatistics().getDequeues().getCount();
    }

    /** Send one control message to the inbox from an outside client (another agent). */
    private void sendToInbox() throws Exception {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(uri);
        try (Connection c = f.createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MsgEvent me = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a1", null, false, false);
            s.createProducer(s.createQueue(INBOX)).send(s.createTextMessage(new Gson().toJson(me)));
        }
    }

    private void awaitDequeued(long n, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (dequeued(INBOX) < n && System.currentTimeMillis() < end) Thread.sleep(25);
        assertEquals(n, dequeued(INBOX), "the inbox consumed the control message");
    }

    private static void await(String what, long ms, java.util.function.BooleanSupplier cond) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(25);
        assertTrue(cond.getAsBoolean(), what);
    }

    @Test
    void aFailedInboxConnectionIsRebuiltWithoutARestart() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        AgentConsumer consumer = new AgentConsumer(engine, INBOX, uri);
        try {
            assertTrue(consumer.isDedicatedConnection());
            sendToInbox();
            awaitDequeued(1, 10_000);
            ActiveMQConnection before = consumer.getConnection();
            // the dedicated socket dies (what a broker or hub restart does): its failure listener fires
            before.getExceptionListener().onException(new JMSException("simulated inbox socket loss"));
            assertFalse(consumer.isConnectionActive() && consumer.getConnection() == before,
                    "a failed inbox is not reported active");
            await("the inbox was rebuilt", 10_000, () -> consumer.rebuilds() == 1 && consumer.isConnectionActive());
            assertNotSame(before, consumer.getConnection(), "a fresh dedicated connection");
            sendToInbox();
            awaitDequeued(2, 10_000);
        } finally {
            consumer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void theInboxDrainsAfterARealBrokerRestart() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        AgentConsumer consumer = new AgentConsumer(engine, INBOX, uri);
        try {
            sendToInbox();
            awaitDequeued(1, 10_000);
            stopBroker();
            Thread.sleep(500);
            startBroker();
            sendToInbox();                                   // queued on the restarted broker: the old consumer is gone
            awaitDequeued(1, 60_000);                        // the restarted broker counts from zero
            assertTrue(consumer.isConnectionActive(), "the inbox is live again with no agent restart");
        } finally {
            consumer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void shutdownDuringARebuildEndsItAndLeavesNoSockets() throws Exception {
        AgentConsumer.REBUILD_INITIAL_BACKOFF_MS = 1_500;      // the rebuild waits: shutdown lands inside it
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        AgentConsumer consumer = new AgentConsumer(engine, INBOX, uri);
        try {
            int withInbox = brokerClients();
            consumer.getConnection().getExceptionListener().onException(new JMSException("simulated inbox socket loss"));
            consumer.shutdown();
            Thread.sleep(2_500);                                 // past the rebuild's backoff
            assertEquals(0, consumer.rebuilds(), "shutdown won: no rebuild ran");
            assertEquals(withInbox - 1, brokerClients(), "the inbox's socket is gone and no new one was opened");
        } finally {
            engine.getActiveClient().shutdown();
        }
    }

    @Test
    void aWorkerWhoseOwnConnectionFailedIsDroppedAndABaseFailureDropsAll() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(), null, null);
        AgentProducer producer = new AgentProducer(engine, uri, engine.getActiveClient());
        String other = uri + "?jms.prefetchPolicy.all=10";       // the same broker under a distinct connection URI
        try {
            producer.workers().put("dstA", new ActiveProducerWorker(engine, "dstA", uri));
            producer.workers().put("dstB", new ActiveProducerWorker(engine, "dstB", other));
            producer.invalidateWorkersForURI(other);
            assertTrue(producer.workers().containsKey("dstA"), "a worker on a healthy connection is kept");
            assertFalse(producer.workers().containsKey("dstB"), "the worker whose own connection failed is dropped");
            producer.workers().put("dstB", new ActiveProducerWorker(engine, "dstB", other));
            producer.invalidateWorkersForURI(uri);
            assertTrue(producer.workers().isEmpty(), "a base-URI failure drops every worker");
        } finally {
            producer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }
}
