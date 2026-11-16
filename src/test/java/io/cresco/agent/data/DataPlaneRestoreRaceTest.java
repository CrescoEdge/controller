package io.cresco.agent.data;

import io.cresco.agent.controller.communication.ActiveClient;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.TopicType;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * F-11 at the dataplane: what an agent's recovery does at once -- the controller restores its listeners
 * (updateConnections) while producer workers connect for the messages held meanwhile, or plugins add listeners -- makes
 * ONE broker connection and ONE dataplane session, and every listener is live afterwards. The fabric matrix's tenants
 * cell logged "DataPlaneServiceImpl.addMessageListener error" on rekey-region about one run in four: two threads each
 * made the broker connection, and the second closed the first's.
 */
class DataPlaneRestoreRaceTest {

    private static final String URI = "vm://f11dp?create=false";
    private static final int ROUNDS = 10;

    private BrokerService broker;
    private Connection writer;

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("f11dp");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterEach
    void stop() throws Exception {
        if (writer != null) try { writer.close(); } catch (Exception ignore) { }
        broker.stop();
        broker.waitUntilStopped();
    }

    private static void await(AtomicInteger n, int atLeast) throws InterruptedException {
        for (long end = System.currentTimeMillis() + 5000; n.get() < atLeast && System.currentTimeMillis() < end; ) Thread.sleep(10);
    }

    /** Run every piece of {@code work} on its own thread, all released together; each one's failure is the test's. */
    private static void together(List<Callable<Object>> work) throws Exception {
        CyclicBarrier go = new CyclicBarrier(work.size());
        ExecutorService pool = Executors.newFixedThreadPool(work.size());
        try {
            List<Future<Object>> fs = new ArrayList<>();
            for (Callable<Object> w : work) fs.add(pool.submit(() -> { go.await(); return w.call(); }));
            for (Future<Object> f : fs) f.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    /** An agent's controller on the broker with its inbox up (the dataplane waits for it). */
    private static ControllerEngine agent(String name) throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rdp", name, null);
        assertTrue(e.getActiveClient().initActiveAgentConsumer("rdp_" + name, URI));
        return e;
    }

    /** A plain client of the broker publishing on {@code dps}'s agent topic. */
    private MessageProducer publisher(DataPlaneServiceImpl dps) throws Exception {
        writer = new ActiveMQConnectionFactory(URI).createConnection();
        writer.start();
        Session s = writer.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return s.createProducer(s.createTopic(dps.getTopicPhysicalName(TopicType.AGENT)));
    }

    /** A message carrying the int properties {@code nameValue} (name, value, name, value, ...). */
    private static void send(MessageProducer p, Object... nameValue) throws Exception {
        Message m = new org.apache.activemq.command.ActiveMQMessage();
        for (int i = 0; i < nameValue.length; i += 2) m.setIntProperty((String) nameValue[i], (Integer) nameValue[i + 1]);
        p.send(m);
    }

    @Test
    void listenersRestoredWhileProducerWorkersConnectAreAllLive() throws Exception {
        ControllerEngine e = agent("a1");
        ActiveClient ac = e.getActiveClient();
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(e, URI);
        int listeners = 4, workers = 4;
        AtomicInteger[] got = new AtomicInteger[listeners];
        for (int k = 0; k < listeners; k++) {
            AtomicInteger n = got[k] = new AtomicInteger();
            assertNotNull(dps.addMessageListener(TopicType.AGENT, m -> n.incrementAndGet(), "f11k = " + k));
        }
        MessageProducer p = publisher(dps);

        for (int round = 1; round <= ROUNDS; round++) {
            // the agent's broker connection failed and was taken out (its failover gave up): recovery makes a new one
            ac.handleConnectionFailure(URI);
            List<ActiveMQSession> workerSessions = new CopyOnWriteArrayList<>();
            List<Callable<Object>> recovery = new ArrayList<>();
            recovery.add(() -> { dps.updateConnections(URI); return null; });          // the controller restores its listeners
            for (int w = 0; w < workers; w++) {                                          // workers for the held messages connect
                recovery.add(() -> workerSessions.add(ac.createSession(URI, false, Session.AUTO_ACKNOWLEDGE)));
            }
            together(recovery);

            for (ActiveMQSession s : workerSessions) {
                assertNotNull(s, "round " + round + ": a producer worker got no session");
                assertFalse(s.isClosed(), "round " + round + ": a producer worker's session was closed under it (the restore's"
                        + " new connection replaced the one it was made on)");
            }
            for (int k = 0; k < listeners; k++) send(p, "f11k", k);
            for (int k = 0; k < listeners; k++) {
                await(got[k], round);
                assertEquals(round, got[k].get(), "round " + round + ": restored listener " + k + " is not live (its consumer was"
                        + " made on a connection another thread replaced and closed)");
            }
        }
    }

    @Test
    void listenersAddedTogetherShareOneSessionSoAReconnectLeavesEachOneSubscription() throws Exception {
        ControllerEngine e = agent("a2");
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(e, URI);
        MessageProducer p = publisher(dps);
        int callers = 8;

        for (int round = 1; round <= ROUNDS; round++) {
            dps.updateConnections(URI);           // a reconnect with no listener to restore: the next callers make the session
            AtomicInteger[] got = new AtomicInteger[callers];
            String[] ids = new String[callers];
            List<Callable<Object>> adds = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                int me = i;
                AtomicInteger n = got[i] = new AtomicInteger();
                String selector = "f11r = " + round + " AND f11i = " + i;
                adds.add(() -> ids[me] = dps.addMessageListener(TopicType.AGENT, m -> n.incrementAndGet(), selector));
            }
            together(adds);                       // plugins (re)adding their listeners at once
            for (int i = 0; i < callers; i++) assertNotNull(ids[i], "round " + round + ": listener " + i + " was not added");

            // a reconnect closes the dataplane's session and restores every listener: one made on a session of its own
            // (a second caller's, which the field no longer held) kept its consumer, and its listener had two subscriptions
            dps.updateConnections(URI);
            for (int i = 0; i < callers; i++) send(p, "f11r", round, "f11i", i);
            for (int i = 0; i < callers; i++) await(got[i], 1);
            Thread.sleep(200);                    // a second copy would be here by now
            for (int i = 0; i < callers; i++) {
                assertEquals(1, got[i].get(), "round " + round + ": listener " + i + " took " + got[i].get() + " copies of one message");
            }
            for (String id : ids) dps.removeMessageListener(id);
        }
    }
}
