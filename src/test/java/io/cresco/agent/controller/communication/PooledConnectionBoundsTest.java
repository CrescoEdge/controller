package io.cresco.agent.controller.communication;

import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The review of F-11 (ea2e9d1): one caller at a time made a URI's pooled connection, but under a lock held across the
 * whole connect. Callers of an unreachable broker then each ran a full failover cycle in turn, a connect that never
 * ended held every other caller of the URI for ever (uninterruptibly), and a lock was kept for every URI ever asked
 * for. Now one attempt runs per URI and every caller that finds it running takes its outcome, a failure included; no
 * caller waits past broker_connect_wait_ms, and the attempt itself is bounded by broker_connect_response_timeout_ms.
 * And a dedicated connection's failure (a send lane, a shard) no longer closes the pooled connection of its URI.
 */
class PooledConnectionBoundsTest {

    private final List<BrokerService> brokers = new ArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void stop() throws Exception {
        release.countDown();
        for (BrokerService b : brokers) {
            b.stop();
            b.waitUntilStopped();
        }
    }

    private BrokerService broker(String name, BrokerPlugin... plugins) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        if (plugins.length > 0) b.setPlugins(plugins);
        b.start();
        b.waitUntilStarted();
        brokers.add(b);
        return b;
    }

    /** A broker that takes its first connection but does not answer it until the test ends (counting its connections). */
    private BrokerService muteFirst(String name, AtomicInteger adds) throws Exception {
        return broker(name, next -> new BrokerFilter(next) {
            @Override
            public void addConnection(ConnectionContext ctx, ConnectionInfo info) throws Exception {
                if (adds.getAndIncrement() == 0) release.await(60, TimeUnit.SECONDS);
                super.addConnection(ctx, info);
            }
        });
    }

    /** A failover URI around an in-JVM broker that does not exist: each connect attempt fails at once, 3 of them 1 s apart. */
    private static String unreachable(String name) {
        return "failover:(vm://" + name + "?create=false)?maxReconnectAttempts=3&startupMaxReconnectAttempts=3"
                + "&initialReconnectDelay=1000&useExponentialBackOff=false";
    }

    private static long msSince(long t0) {
        return (System.nanoTime() - t0) / 1_000_000L;
    }

    /** How long each of {@code n} callers released together took. */
    private static List<Long> elapsedTogether(int n, Callable<?> call) throws Exception {
        CyclicBarrier go = new CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<Long>> fs = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                fs.add(pool.submit(() -> {
                    go.await();
                    long t0 = System.nanoTime();
                    call.call();
                    return msSince(t0);
                }));
            }
            List<Long> out = new ArrayList<>();
            for (Future<Long> f : fs) out.add(f.get(120, TimeUnit.SECONDS));
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void callersOfAnUnreachableBrokerShareOneAttemptAndItsFailure() throws Exception {
        ActiveClient ac = TlsTestSupport.engine("rb", "b1", null).getActiveClient();
        String uri = unreachable("f11b_nobroker");
        long t0 = System.nanoTime();
        assertNull(ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE));
        long one = msSince(t0);

        List<Long> each = elapsedTogether(4, () -> ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE));
        long worst = each.stream().mapToLong(Long::longValue).max().orElse(0);
        assertTrue(worst < one + 1000, "4 callers together waited " + each + " ms; one connect attempt takes " + one
                + " ms: they did not share one attempt (ea2e9d1: one full failover cycle each, in turn)");
        assertEquals(0, perUri(ac), "an attempt that ended is still registered");
    }

    @Test
    void aConnectThatNeverEndsHoldsNoOtherCallerPastTheBound() throws Exception {
        AtomicInteger adds = new AtomicInteger();
        muteFirst("f11b_hang", adds);
        ActiveClient ac = TlsTestSupport.engine("rb", "b2", Map.of("broker_connect_wait_ms", "1500")).getActiveClient();
        String uri = "vm://f11b_hang?create=false";
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            Future<ActiveMQSession> first = pool.submit(() -> ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE));
            while (adds.get() == 0) Thread.sleep(10);                      // its connect is under way and gets no answer

            long t0 = System.nanoTime();
            Future<ActiveMQSession> second = pool.submit(() -> ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE));
            AtomicInteger interruptedReturned = new AtomicInteger(-1);
            Thread third = new Thread(() -> {
                ActiveMQSession s = ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE);
                interruptedReturned.set(s == null && Thread.currentThread().isInterrupted() ? 1 : 0);
            }, "f11b-interrupted");
            third.start();
            Thread.sleep(200);
            long ti = System.nanoTime();
            third.interrupt();
            third.join(5000);
            assertFalse(third.isAlive(), "a caller waiting for another's connect could not be interrupted");
            assertTrue(msSince(ti) < 1000, "an interrupted caller took " + msSince(ti) + " ms to return");
            assertEquals(1, interruptedReturned.get(), "an interrupted caller returns null with its interrupt status kept");

            ActiveMQSession got = null;
            try {
                got = second.get(5, TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException held) {
                fail("a caller of the URI was still held after " + msSince(t0) + " ms by a connect that never ends (bound 1500 ms)");
            }
            long waited = msSince(t0);
            assertNull(got, "the attempt it waited for did not end: it goes on without a connection");
            assertTrue(waited >= 1400 && waited < 4000, "waited " + waited + " ms for a bound of 1500 ms");

            // the stuck attempt no longer holds the URI: the next caller makes its own, which the broker answers
            ActiveMQSession fresh = ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(fresh, "after a caller gave up on a stuck connect, the URI stayed held by it");

            // the stuck connect is answered at last: it keeps nothing and closes nothing; its caller takes the one in use
            release.countDown();
            ActiveMQSession late = first.get(30, TimeUnit.SECONDS);
            assertFalse(fresh.isClosed(), "the late connect closed the connection made meanwhile");
            assertFalse(((ActiveMQConnection) fresh.getConnection()).isClosed());
            assertNotNull(late);
            assertSame(fresh.getConnection(), late.getConnection(), "the late connect replaced the connection in use");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aConnectTheBrokerNeverAnswersFailsWithinTheResponseTimeout() throws Exception {
        AtomicInteger adds = new AtomicInteger();
        muteFirst("f11b_mute", adds);
        ActiveClient ac = TlsTestSupport.engine("rb", "b3", Map.of("broker_connect_response_timeout_ms", "1000")).getActiveClient();
        String uri = "vm://f11b_mute?create=false";
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            long t0 = System.nanoTime();
            Future<ActiveMQSession> first = pool.submit(() -> ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE));
            ActiveMQSession got = null;
            try {
                got = first.get(5, TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException held) {
                fail("a connect the broker never answers held its caller " + msSince(t0) + " ms (response timeout 1000 ms)");
            }
            assertNull(got);
            assertNotNull(ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE), "the next connect, which the broker answers");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aDedicatedConnectionsFailureLeavesThePooledOneOpen() throws Exception {
        broker("f11b_ded");
        ActiveClient ac = TlsTestSupport.engine("rb", "b4", null).getActiveClient();
        String uri = "vm://f11b_ded?create=false";
        ActiveMQSession pooled = ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQSession lane = ac.createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE);
        assertNotSame(pooled.getConnection(), lane.getConnection());

        ((ActiveMQConnection) lane.getConnection()).getExceptionListener().onException(new jakarta.jms.JMSException("lane transport failed"));
        assertFalse(((ActiveMQConnection) pooled.getConnection()).isClosed(),
                "a dedicated (send lane, shard) connection's failure closed the pooled connection of its URI");
        assertFalse(pooled.isClosed(), "and the dataplane session on it");
        assertTrue(((ActiveMQConnection) lane.getConnection()).isClosed(), "the failed connection itself is closed");
        assertSame(pooled.getConnection(), ac.createSession(uri, false, Session.AUTO_ACKNOWLEDGE).getConnection());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nothingIsKeptPerUriOnceItsAttemptEndsNorAfterShutdown() throws Exception {
        ActiveClient ac = TlsTestSupport.engine("rb", "b5", null).getActiveClient();
        for (int i = 0; i < 50; i++) {
            assertNull(ac.createSession("vm://f11b_none" + i + "?create=false", false, Session.AUTO_ACKNOWLEDGE));
        }
        int afterAttempts = perUri(ac);
        ac.shutdown();
        assertEquals(0, afterAttempts, "something is kept for each of 50 URIs whose connect attempt ended");
        assertEquals(0, perUri(ac), "something is kept per URI after shutdown()");
    }

    /** The per-URI connect state: ea2e9d1 kept a lock per URI (connectLocks); the attempts running now (connecting). */
    @SuppressWarnings("unchecked")
    private static int perUri(ActiveClient ac) throws Exception {
        int n = 0;
        for (String name : new String[]{"connectLocks", "connecting"}) {
            try {
                java.lang.reflect.Field f = ActiveClient.class.getDeclaredField(name);
                f.setAccessible(true);
                n += ((Map<String, ?>) f.get(ac)).size();
            } catch (NoSuchFieldException none) {
                // not in this build
            }
        }
        return n;
    }
}
