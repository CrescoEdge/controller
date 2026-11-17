package io.cresco.agent.data;

import io.cresco.agent.controller.communication.ActiveClient;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.TopicType;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The review of F-11 (ea2e9d1), dataplane side. (1) The dataplane session was made under the lock a reconnect
 * (updateConnections) needs to switch the URI: an agent rejoining at a NEW URI waited for every dataplane caller's
 * connect to the former, dead broker, one failover cycle each. (2) A listener added while a reconnect restored the
 * listeners was subscribed on the new session and then restored a second time: every message twice, and one
 * subscription left after its removal. In-JVM brokers only.
 */
class DataPlaneReconnectTest {

    private static final String NEW = "vm://f11r_new?create=false";
    /** The former parent's broker, as an agent dials it (failover), and gone for good: each connect takes ~2 s to fail. */
    private static final String OLD = "failover:(vm://f11r_old?create=false)?maxReconnectAttempts=3&startupMaxReconnectAttempts=3"
            + "&initialReconnectDelay=1000&useExponentialBackOff=false";

    private BrokerService oldBroker, newBroker;
    private Connection writer;

    private static BrokerService broker(String name) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.start();
        b.waitUntilStarted();
        return b;
    }

    @BeforeEach
    void start() throws Exception {
        oldBroker = broker("f11r_old");
        newBroker = broker("f11r_new");
    }

    @AfterEach
    void stop() throws Exception {
        if (writer != null) try { writer.close(); } catch (Exception ignore) { }
        for (BrokerService b : new BrokerService[]{oldBroker, newBroker}) {
            if (b != null && b.isStarted()) { b.stop(); b.waitUntilStopped(); }
        }
    }

    private Session writerOnNew() throws Exception {
        writer = new ActiveMQConnectionFactory(NEW).createConnection();
        writer.start();
        return writer.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static void await(AtomicInteger n, int atLeast, long ms) throws InterruptedException {
        for (long end = System.currentTimeMillis() + ms; n.get() < atLeast && System.currentTimeMillis() < end; ) Thread.sleep(10);
    }

    @Test
    void aRejoinAtANewUriDoesNotWaitForConnectsToTheFormerBroker() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rr", "a1", null);
        ActiveClient ac = e.getActiveClient();
        assertTrue(ac.initActiveAgentConsumer("rr_a1", NEW));           // the inbox is up on the new parent's broker first
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(e, OLD);
        AtomicInteger got = new AtomicInteger();
        assertNotNull(dps.addMessageListener(TopicType.AGENT, m -> got.incrementAndGet(), null), "a listener on the former broker");

        // the former parent is gone for good and its pooled connection was taken out (its exception listener does that)
        oldBroker.stop();
        oldBroker.waitUntilStopped();
        ac.handleConnectionFailure(OLD);

        // dataplane callers (the state export's timer, plugin sends) need a session while the URI is still the former one
        List<Thread> callers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Thread t = new Thread(() -> { try { dps.createMapMessage(); } catch (Exception ignore) { } }, "f11r-caller-" + i);
            t.setDaemon(true);
            t.start();
            callers.add(t);
        }
        Thread.sleep(200);

        long t0 = System.nanoTime();
        dps.updateConnections(NEW);                                     // the agent's recovery thread
        long took = (System.nanoTime() - t0) / 1_000_000L;

        Session s = writerOnNew();
        MessageProducer p = s.createProducer(s.createTopic(dps.getTopicPhysicalName(TopicType.AGENT)));
        p.send(s.createMessage());
        await(got, 1, 5000);
        for (Thread t : callers) t.join(30_000);

        assertTrue(took < 1000, "the agent's recovery (updateConnections) took " + took
                + " ms: it waited for connects to the former, unreachable broker");
        assertEquals(1, got.get(), "the listener restored on the new broker took " + got.get() + " messages of one");
        // a caller that asked before the reconnect is not left with a session of the former broker
        jakarta.jms.MapMessage after = dps.createMapMessage();
        assertNotNull(after, "no session after the reconnect");
    }

    @Test
    void aListenerAddedDuringARestoreIsSubscribedOnce() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rr", "a2", null);
        assertTrue(e.getActiveClient().initActiveAgentConsumer("rr_a2", NEW));
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(e, NEW);

        // a listener busy in onMessage holds the restore in its close of the former session (JMS waits for it)
        CountDownLatch inside = new CountDownLatch(1), release = new CountDownLatch(1);
        assertNotNull(dps.addMessageListener(TopicType.AGENT, m -> {
            inside.countDown();
            try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignore) { }
        }, "f11rk = 1"));
        Session s = writerOnNew();
        MessageProducer p = s.createProducer(s.createTopic(dps.getTopicPhysicalName(TopicType.AGENT)));
        jakarta.jms.Message m1 = s.createMessage();
        m1.setIntProperty("f11rk", 1);
        p.send(m1);
        assertTrue(inside.await(5, TimeUnit.SECONDS));

        Thread restore = new Thread(() -> dps.updateConnections(NEW), "f11r-restore");
        restore.start();
        Thread.sleep(300);                                              // the restore is under way

        AtomicInteger got = new AtomicInteger();
        String id = dps.addMessageListener(TopicType.AGENT, m -> got.incrementAndGet(), "f11rk = 2");
        assertNotNull(id);
        release.countDown();
        restore.join(10_000);
        assertFalse(restore.isAlive());

        jakarta.jms.Message m2 = s.createMessage();
        m2.setIntProperty("f11rk", 2);
        p.send(m2);
        await(got, 1, 3000);
        Thread.sleep(300);                                              // a second copy would be here by now
        assertEquals(1, got.get(), "a listener added during a restore took " + got.get() + " copies of one message");

        dps.removeMessageListener(id);
        jakarta.jms.Message m3 = s.createMessage();
        m3.setIntProperty("f11rk", 2);
        p.send(m3);
        Thread.sleep(500);
        assertEquals(1, got.get(), "a removed listener still took messages (a second subscription was left)");
    }
}
