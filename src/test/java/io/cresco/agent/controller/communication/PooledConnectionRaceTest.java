package io.cresco.agent.controller.communication;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * F-11: the pooled broker connection of a URI is made by one caller at a time. On an agent's recovery two threads (the
 * dataplane restoring its listeners, a producer worker for a held message) found no connection in the same millisecond
 * and each made one; the second to finish replaced the first in the map and closed it, with the sessions and consumers
 * made on it ("DataPlaneServiceImpl.addMessageListener error" in the fabric matrix's tenants cell, rekey-region). And a
 * connection's failure, reported after it was replaced, closed its successor.
 */
class PooledConnectionRaceTest {

    private static final String URI = "vm://f11pool?create=false";

    private BrokerService broker;

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("f11pool");
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

    /** What {@code n} threads released together each got from {@code make}. */
    static <T> List<T> together(int n, Callable<T> make) throws Exception {
        CyclicBarrier go = new CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<T>> made = new ArrayList<>();
            for (int i = 0; i < n; i++) made.add(pool.submit(() -> { go.await(); return make.call(); }));
            List<T> out = new ArrayList<>();
            for (Future<T> f : made) out.add(f.get(60, TimeUnit.SECONDS));
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void callersThatFindNoConnectionTogetherShareTheOneTheFirstMade() throws Exception {
        ActiveClient ac = TlsTestSupport.engine("rf", "a1", null).getActiveClient();
        for (int round = 0; round < 10; round++) {
            ac.handleConnectionFailure(URI);       // the pooled connection failed and was taken out: the next caller makes one
            List<ActiveMQSession> sessions = together(8, () -> ac.createSession(URI, false, Session.AUTO_ACKNOWLEDGE));
            Set<jakarta.jms.Connection> connections = Collections.newSetFromMap(new IdentityHashMap<>());
            for (ActiveMQSession s : sessions) {
                assertNotNull(s, "round " + round + ": no session");
                assertFalse(s.isClosed(), "round " + round + ": a session was closed under its owner (another caller's new"
                        + " connection replaced the one it was made on)");
                connections.add(s.getConnection());
            }
            assertEquals(1, connections.size(), "round " + round + ": " + connections.size() + " broker connections made for one URI");
        }
    }

    @Test
    void aLateFailureOfAReplacedConnectionLeavesItsSuccessorOpen() throws Exception {
        ActiveClient ac = TlsTestSupport.engine("rf", "a2", null).getActiveClient();
        ActiveMQConnection first = (ActiveMQConnection) ac.createSession(URI, false, Session.AUTO_ACKNOWLEDGE).getConnection();
        ExceptionListener firstsListener = first.getExceptionListener();
        assertNotNull(firstsListener);

        // its transport failed: a session's check takes it out first, and the next caller makes the successor
        ac.handleConnectionFailure(URI);
        ActiveMQSession onSecond = ac.createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQConnection second = (ActiveMQConnection) onSecond.getConnection();
        assertNotSame(first, second);

        // the first connection's own listener reports that failure only now
        firstsListener.onException(new JMSException("transport failed (reported late)"));
        assertFalse(second.isClosed(), "the late failure of the replaced connection closed its successor");
        assertFalse(onSecond.isClosed(), "a session on the successor was closed");
        assertSame(second, ac.createSession(URI, false, Session.AUTO_ACKNOWLEDGE).getConnection(),
                "the successor is no longer the pooled connection");
    }
}
