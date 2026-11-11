package io.cresco.agent.controller.communication;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.SessionId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a node knows of its broker's routes ({@link RouteGate}): an inbox has a route while something consumes it there;
 * one that lost its last consumer is known to have none one grace later; a request is held only inside that grace.
 * Before, nothing knew: a request for a node behind a dead bridge waited in the queue until its caller gave up.
 */
class RouteGateTest {

    private static final long GRACE = 1500L;
    /** The moment a request arrives, long after everything the tests let happen before it. */
    private static final long NOW = 500_000L;
    private static final AtomicInteger ids = new AtomicInteger();

    private static ConsumerId consumer() {
        return new ConsumerId(new SessionId(new ConnectionId("c" + ids.incrementAndGet()), 1), 1);
    }

    private static RouteGate bound(long at) {
        RouteGate g = new RouteGate(GRACE, 4);
        g.bound(at);
        return g;
    }

    @Test
    void anInboxSomethingConsumesHasARoute() {
        RouteGate g = bound(1_000);
        g.consumerAdded(consumer(), new ActiveMQQueue("T.t1.r2_a21"));        // an agent's own inbox, or a bridge's demand for it
        g.consumerAdded(consumer(), new ActiveMQQueue("T.*.r2_rc2"));         // a controller's inbox in every tenant
        g.consumerAdded(consumer(), new ActiveMQQueue("r3_a31"));             // a node of an earlier build: its raw inbox
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW));
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_rc2", NOW));
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t9.r2_rc2", NOW), "a controller reads its inbox in every tenant's namespace");
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("r3_a31", NOW));
        assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("T.t2.r2_a21", NOW), "another tenant's queue of that name is not what a21 consumes");
        assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a22", NOW));
    }

    @Test
    void anInboxThatLostItsLastConsumerIsKnownGoneOneGraceLater() {
        RouteGate g = bound(1_000);
        ConsumerId first = consumer(), second = consumer();
        g.consumerAdded(first, new ActiveMQQueue("T.*.r2_rc2"));
        g.consumerAdded(second, new ActiveMQQueue("T.*.r2_rc2"));
        g.consumerRemoved(first, 50_000);
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_rc2", NOW), "one consumer is left");
        g.consumerRemoved(second, 60_000);
        assertEquals(60_000 + GRACE, g.noRouteFrom("T.t1.r2_rc2", NOW), "the grace counts from the loss, not from the request");
        g.consumerRemoved(second, 70_000);
        assertEquals(60_000 + GRACE, g.noRouteFrom("T.t1.r2_rc2", NOW), "a consumer is counted out once");
        g.consumerAdded(consumer(), new ActiveMQQueue("T.*.r2_rc2"));
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_rc2", NOW), "the bridge is back");
    }

    @Test
    void anInboxThisBrokerNeverSawAConsumerLeaveIsWorthOneGraceFromTheRequest() {
        // a bridge that is just being built: its demand reaches the broker a moment after it, and a request that overtook
        // it must wait for it, not fail -- however long the broker has been up
        RouteGate g = bound(1_000);
        assertEquals(NOW + GRACE, g.noRouteFrom("T.t1.r9_a1", NOW));
        ConsumerId bridge = consumer();
        g.consumerAdded(bridge, new ActiveMQQueue("T.t1.r9_a1"));
        g.consumerRemoved(bridge, 60_000);
        assertEquals(60_000 + GRACE, g.noRouteFrom("T.t1.r9_a1", NOW), "now it saw it leave");
        g.bound(90_000);                                                     // the node made its broker anew: it knows nothing
        assertEquals(NOW + GRACE, g.noRouteFrom("T.t1.r9_a1", NOW));
    }

    @Test
    void whatTheGateCannotKnowIsNotJudged() {
        RouteGate none = new RouteGate(GRACE, 4);
        assertFalse(none.active());
        assertEquals(RouteGate.ROUTE, none.noRouteFrom("T.t1.r2_a21", NOW), "a node without a broker (an agent) judges nothing");
        RouteGate g = bound(1_000);
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.some.other.queue", NOW), "not a node's inbox");
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("r2", NOW), "a region's name is no inbox");
        g.unbound();
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW), "the broker stopped");
    }

    @Test
    void aPatternTheGateDoesNotCountStopsItForTheQueuesThatPatternCanMatchOnlyAndIsReported() {
        RouteGate g = bound(1_000);
        java.util.List<String> warned = new java.util.ArrayList<>();
        g.setWarn(warned::add);
        assertEquals(0, g.unreadPatterns());
        ConsumerId everything = consumer(), again = consumer();
        g.consumerAdded(everything, new ActiveMQQueue("T.t1.>"));             // a tenant's node reading every queue of its tenant
        g.consumerAdded(again, new ActiveMQQueue("T.t1.>"));
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW), "a pattern the gate does not count may take this inbox");
        assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("T.t2.r2_a21", NOW), "one tenant's pattern switched the gate off for another's inboxes");
        assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("r2_a21", NOW));
        assertEquals(2, g.unreadPatterns(), "the gauge: consumers that read by such a pattern");
        assertEquals(1, warned.size(), "one WARN while such a pattern is present: " + warned);
        assertTrue(warned.get(0).contains("'T.t1.>'"), warned.get(0));
        g.consumerRemoved(everything, 2_000);
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW), "one of the two consumers is left");
        g.consumerRemoved(again, 2_000);
        assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW), "the pattern is gone: the gate judges again");
        assertEquals(0, g.unreadPatterns());
        // more patterns than the gate asks one by one: nothing is judged
        for (int i = 0; i <= RouteGate.PATTERNS_MAX; i++) g.consumerAdded(consumer(), new ActiveMQQueue("T.x" + i + ".>"));
        assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t2.r2_a21", NOW));
    }

    @Test
    void aConsumerTheBrokerHasAndTheGateDoesNotIsReported() {
        RouteGate g = bound(1_000);
        java.util.List<String> warned = new java.util.ArrayList<>();
        g.setWarn(warned::add);
        g.mismatch("T.t1.r2_a21");
        g.mismatch("T.t1.r2_a21");
        assertEquals(2, g.mismatchCount());
        assertEquals(1, warned.size(), "logged at most every 10 s");
        assertTrue(warned.get(0).contains("T.t1.r2_a21"));
    }

    @Test
    void aHeldRequestGoesOnTheMomentItsConsumerIsBack() throws Exception {
        RouteGate g = bound(1_000);
        CountDownLatch sent = new CountDownLatch(1);
        AtomicInteger gaveUp = new AtomicInteger();
        long now = System.currentTimeMillis();
        assertTrue(g.hold("T.t1.r2_a21", "r2_a21", now + 5_000, now, sent::countDown, gaveUp::incrementAndGet));
        assertEquals(1, g.heldNow());
        g.consumerAdded(consumer(), new ActiveMQQueue("T.t1.r2_a21"));
        assertTrue(sent.await(2, TimeUnit.SECONDS), "the request waited although its route was back");
        Thread.sleep(100);
        assertEquals(0, gaveUp.get());
        assertEquals(0, g.heldNow());
        assertEquals(1, g.heldCount());
        g.stop();
    }

    @Test
    void aHeldRequestIsGivenUpWhenItsTimeComesAndOnlyThen() throws Exception {
        RouteGate g = bound(1_000);
        CountDownLatch gaveUp = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger();
        long now = System.currentTimeMillis();
        assertTrue(g.hold("T.t1.r2_a21", "r2_a21", now + 300, now, sent::incrementAndGet, gaveUp::countDown));
        assertFalse(gaveUp.await(150, TimeUnit.MILLISECONDS), "given up before its time");
        assertTrue(gaveUp.await(2, TimeUnit.SECONDS), "never given up: its caller would wait out its timeout");
        g.consumerAdded(consumer(), new ActiveMQQueue("T.t1.r2_a21"));
        Thread.sleep(200);
        assertEquals(0, sent.get(), "a request whose caller was told there is no route was sent on afterwards");
        assertEquals(0, g.heldNow());
        g.stop();
    }

    @Test
    void aConsumerOfAnotherNodeReleasesNothingAndTheHoldIsBounded() throws Exception {
        RouteGate g = bound(1_000);
        AtomicInteger sent = new AtomicInteger(), gaveUp = new AtomicInteger();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 4; i++) {
            assertTrue(g.hold("T.t1.r2_a21", "r2_a21", now + 60_000, now, sent::incrementAndGet, gaveUp::incrementAndGet));
        }
        assertFalse(g.hold("T.t1.r2_a21", "r2_a21", now + 60_000, now, sent::incrementAndGet, gaveUp::incrementAndGet),
                "a fifth request was held where four are the most: it must be answered at once");
        assertFalse(g.hold("T.t1.r2_a21", "r2_a21", now, now, sent::incrementAndGet, gaveUp::incrementAndGet), "nothing to wait for");
        g.consumerAdded(consumer(), new ActiveMQQueue("T.t1.r2_a22"));
        Thread.sleep(200);
        assertEquals(0, sent.get(), "another node's consumer released a21's requests");
        g.stop();                                                            // the node stops: every held request is answered
        assertEquals(4, gaveUp.get());
        assertEquals(0, g.heldNow());
        // ... and none is held from now on: its caller answers it at once, and no thread is started for it
        assertFalse(g.hold("T.t1.r2_a21", "r2_a21", now + 60_000, System.currentTimeMillis(), sent::incrementAndGet, gaveUp::incrementAndGet),
                "a request was held by a gate that had stopped");
        assertEquals(4, gaveUp.get());
    }

    @Test
    void oneDeadNodesCallersCannotTakeEveryPlace() {
        RouteGate g = new RouteGate(GRACE, 10, 3);                           // ten places, three a node
        g.bound(1_000);
        AtomicInteger sent = new AtomicInteger(), gaveUp = new AtomicInteger();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) assertTrue(g.hold("T.t1.r2_a21", "r2_a21", now + 60_000, now, sent::incrementAndGet, gaveUp::incrementAndGet));
        assertFalse(g.hold("T.t1.r2_a21", "r2_a21", now + 60_000, now, sent::incrementAndGet, gaveUp::incrementAndGet),
                "a fourth request for one node was held where three a node are the most");
        assertTrue(g.hold("T.t1.r3_a31", "r3_a31", now + 60_000, now, sent::incrementAndGet, gaveUp::incrementAndGet),
                "the callers of one node left no place for another's");
        assertEquals(4, g.heldNow());
        g.stop();
        assertEquals(4, gaveUp.get());
    }

    @Test
    void aConsumerThatJoinedJustBeforeTheRequestWasHeldStillReleasesIt() throws Exception {
        RouteGate g = bound(1_000);
        CountDownLatch sent = new CountDownLatch(1);
        long now = System.currentTimeMillis();
        // the caller looked (no consumer), then the bridge came back, then the caller entered its hold
        g.consumerAdded(consumer(), new ActiveMQQueue("T.*.r2_rc2"));
        assertTrue(g.hold("T.t1.r2_rc2", "r2_rc2", now + 30_000, now, sent::countDown, () -> { }));
        assertTrue(sent.await(2, TimeUnit.SECONDS), "held for a route that was already back");
        g.stop();
    }

    private static void await(java.util.function.BooleanSupplier c, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + 5_000;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(10);
        assertTrue(c.getAsBoolean(), what);
    }

    @Test
    void theBrokerTellsTheGateOfEveryConsumerThatJoinsOrIsCutOff() throws Exception {
        RouteGate g = new RouteGate(GRACE, 4);
        BrokerService broker = new BrokerService();
        broker.setBrokerName("routegate");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setPlugins(new BrokerPlugin[]{g.plugin()});
        broker.start();
        broker.waitUntilStarted();
        try {
            assertTrue(g.active());
            long started = System.currentTimeMillis();
            assertNotEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_rc2", NOW));
            Connection c = new ActiveMQConnectionFactory("vm://routegate?create=false").createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer inbox = s.createConsumer(s.createQueue("T.*.r2_rc2"));
            MessageConsumer agent = s.createConsumer(s.createQueue("T.t1.r2_a21"));
            assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_rc2", NOW));
            assertEquals(RouteGate.ROUTE, g.noRouteFrom("T.t1.r2_a21", NOW));
            inbox.close();
            await(() -> g.noRouteFrom("T.t1.r2_rc2", NOW) != RouteGate.ROUTE, "a consumer that closed still counted as a route");
            long known = g.noRouteFrom("T.t1.r2_rc2", NOW);
            assertTrue(known >= started + GRACE && known <= System.currentTimeMillis() + GRACE, "the grace counts from the loss: " + known);
            assertNotNull(agent);
            c.close();                                                       // the connection goes with its consumers on it
            await(() -> g.noRouteFrom("T.t1.r2_a21", NOW) != RouteGate.ROUTE, "a consumer whose connection went still counted as a route");
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}
