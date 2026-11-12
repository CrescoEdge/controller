package io.cresco.agent.controller.netdiscovery;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The discovery responder's queue: requests are answered off the thread that read them, the addresses that wait are
 * served in turn (one that floods cannot keep another from being answered), a host with hundreds of requests is served
 * and not refused, and a queue that is full says so with a time to come back -- nothing is dropped without an answer.
 */
class DiscoveryJobsTest {

    private static void await(CountDownLatch l, String what) throws InterruptedException {
        assertTrue(l.await(20, TimeUnit.SECONDS), what);
    }

    @Test
    void aRequestIsAnsweredByAWorkerNotByTheThreadThatReadIt() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(2, 5000, 1000, null);
        try {
            CountDownLatch done = new CountDownLatch(1);
            String[] ranOn = new String[1];
            assertEquals(0, jobs.submit("10.0.0.1", () -> { ranOn[0] = Thread.currentThread().getName(); done.countDown(); }));
            await(done, "the request was not answered");
            assertTrue(ranOn[0].startsWith("discovery-join-"), "answered on " + ranOn[0]);
            assertNotEquals(Thread.currentThread().getName(), ranOn[0]);
        } finally {
            jobs.shutdown();
        }
    }

    /** One address with hundreds of requests waiting: another address's request is answered after ONE of them, not all. */
    @Test
    void anAddressThatFloodsDoesNotKeepAnotherFromBeingAnswered() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 600_000, 10_000, null);
        try {
            CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1), all = new CountDownLatch(402);
            List<String> order = Collections.synchronizedList(new ArrayList<>());
            // the one worker is held, so that everything below is queued before anything is answered
            assertEquals(0, jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } all.countDown(); }));
            await(started, "the worker did not start");
            for (int i = 0; i < 400; i++) assertEquals(0, jobs.submit("10.0.0.66", () -> { order.add("flood"); all.countDown(); }));
            assertEquals(0, jobs.submit("10.0.0.7", () -> { order.add("other"); all.countDown(); }));
            assertEquals(401, jobs.queued());
            assertEquals(2, jobs.addressesWaiting());
            gate.countDown();
            await(all, "not every request was answered: " + jobs.served());
            int at = order.indexOf("other");
            assertTrue(at >= 0 && at <= 1, "the other address was answered after " + at + " of the flooding address's 400 requests");
            assertEquals(402, jobs.served());
            assertEquals(0, jobs.busy());
        } finally {
            jobs.shutdown();
        }
    }

    /** Fails before the change in spirit: 16 connections per address were the limit, the rest closed without a word. */
    @Test
    void aHostThatStartsHundredsOfAgentsIsServedNotRefused() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(4, DiscoveryJobs.DEFAULT_MAX_WAIT_MS, DiscoveryGuard.DEFAULT_MAX_CONNECTIONS, null);
        try {
            int n = 250;
            CountDownLatch all = new CountDownLatch(n);
            AtomicInteger busy = new AtomicInteger();
            for (int i = 0; i < n; i++) {
                if (jobs.submit("10.30.0.16", () -> { try { Thread.sleep(2); } catch (InterruptedException ignore) { } all.countDown(); }) != 0) {
                    busy.incrementAndGet();
                    all.countDown();
                }
            }
            await(all, "not every request was answered");
            assertEquals(0, busy.get(), "requests of one host that starts " + n + " agents at once were sent away");
            assertTrue(DiscoveryGuard.DEFAULT_MAX_CONNECTIONS_PER_ADDRESS >= n, "the listener would send their connections away");
            assertTrue(DiscoveryGuard.DEFAULT_MAX_CONNECTIONS_PER_ADDRESS * 8 <= DiscoveryGuard.DEFAULT_MAX_CONNECTIONS,
                    "one address may hold more than an eighth of the listener");
        } finally {
            jobs.shutdown();
        }
    }

    @Test
    void aQueueThatIsFullSaysWhenToComeBackAndDropsNothing() throws Exception {
        // one worker, a wait of at most 40 ms: at the first guess of 5 ms a request, the ninth of an address is too many
        DiscoveryJobs jobs = new DiscoveryJobs(1, 40, 10_000, null);
        try {
            CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1);
            AtomicInteger ran = new AtomicInteger();
            assertEquals(0, jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } }));
            await(started, "the worker did not start");
            int queued = 0, sentAway = 0;
            long retry = 0;
            for (int i = 0; i < 50; i++) {
                long r = jobs.submit("10.0.0.9", ran::incrementAndGet);
                if (r == 0) queued++; else { sentAway++; retry = r; }
            }
            assertTrue(queued >= 1 && queued < 50, "queued " + queued);
            assertEquals(50, queued + sentAway, "a request was neither queued nor told to come back");
            assertEquals(sentAway, jobs.busy());
            assertTrue(retry >= DiscoveryJobs.MIN_RETRY_MS && retry <= DiscoveryJobs.MAX_RETRY_MS, "come back in " + retry + " ms");
            // an address that has nothing waiting is still taken: the full queue is the flooding address's own
            assertEquals(0, jobs.submit("10.0.0.10", ran::incrementAndGet), "another address was sent away because one filled its queue");
            gate.countDown();
            long end = System.currentTimeMillis() + 20_000;
            while (ran.get() < queued + 1 && System.currentTimeMillis() < end) Thread.sleep(10);
            assertEquals(queued + 1, ran.get(), "what was queued was not all answered");
            // a hard bound on what is held, whatever the wait allows
            DiscoveryJobs small = new DiscoveryJobs(1, 600_000, 3, null);
            CountDownLatch g2 = new CountDownLatch(1), s2 = new CountDownLatch(1);
            small.submit("hold", () -> { s2.countDown(); try { g2.await(); } catch (InterruptedException ignore) { } });
            await(s2, "the worker did not start");
            int held = 0;
            for (int i = 0; i < 10; i++) if (small.submit("a" + i, () -> { }) == 0) held++;
            assertEquals(3, held, "more requests were held than the bound");
            g2.countDown();
            small.shutdown();
        } finally {
            jobs.shutdown();
        }
    }

    /** A request is an unauthenticated frame of up to 256 KiB: what waits is bounded in bytes, per address and in all. */
    @Test
    void whatWaitsIsBoundedInBytesPerAddressAndInAll() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 600_000, 100_000, 1_000_000, 2_500_000, null);
        try {
            CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1);
            jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } });
            await(started, "the worker did not start");
            int a = 0, b = 0, c = 0;
            for (int i = 0; i < 20; i++) if (jobs.submit("10.0.0.1", 256 * 1024, () -> { }) == 0) a++;
            assertEquals(3, a, "one address holds more than its bound of bytes");            // 3 x 256 KiB under 1,000,000
            for (int i = 0; i < 20; i++) if (jobs.submit("10.0.0.2", 256 * 1024, () -> { }) == 0) b++;
            for (int i = 0; i < 20; i++) if (jobs.submit("10.0.0.3", 256 * 1024, () -> { }) == 0) c++;
            assertEquals(9, a + b + c, "all addresses hold more than the bound of bytes");   // 9 x 256 KiB under 2,500,000
            assertTrue(jobs.queuedBytes() <= 2_500_000);
            for (int i = 0; i < 200; i++) assertEquals(0, jobs.submit("10.0.0.4", 500, () -> { }), "a request of real size was sent away");
            gate.countDown();
            long end = System.currentTimeMillis() + 20_000;
            while (jobs.queued() > 0 && System.currentTimeMillis() < end) Thread.sleep(10);
            assertEquals(0, jobs.queuedBytes(), "bytes not given back");
            assertEquals(0, jobs.submit("10.0.0.1", 256 * 1024, () -> { }), "the address's bound was not given back");
        } finally {
            jobs.shutdown();
        }
    }

    /**
     * The proof of a challenge this responder wrote is not sent away because the queue's wait has grown since its hello:
     * the hello's work (and the joiner's signature) would be thrown away. The count and the bytes still bound it.
     * Fails before: a proof was admitted like a new hello.
     */
    @Test
    void aRequestThatContinuesAnExchangeIsNotSentAwayForTheWait() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 40, 20, 4_000, 8_000, null);
        try {
            CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1);
            jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } });
            await(started, "the worker did not start");
            int queued = 0;
            while (jobs.submit("10.0.0.9", 100, false, () -> { }, null) == 0) queued++;       // until the wait bound sends one away
            assertTrue(queued >= 1 && queued < 19, "queued " + queued);
            assertTrue(jobs.submit("10.0.0.9", 100, false, () -> { }, null) > 0, "a new request is sent away: the wait is over its bound");
            CountDownLatch proof = new CountDownLatch(1);
            assertEquals(0, jobs.submit("10.0.0.9", 100, true, proof::countDown, ms -> fail("a continuing request was told it is too late")),
                    "the proof of an exchange already answered was sent away for the wait");
            // ... but not past the bytes of its address, nor past the count
            assertTrue(jobs.submit("10.0.0.9", 4_000, true, () -> { }, null) > 0, "a continuing request passed its address's bound of bytes");
            while (jobs.queued() < 20) assertEquals(0, jobs.submit("a" + jobs.queued(), 0, true, () -> { }, null));
            assertTrue(jobs.submit("10.0.0.77", 0, true, () -> { }, null) > 0, "a continuing request passed the bound of requests held");
            gate.countDown();
            await(proof, "the continuing request was not answered");
        } finally {
            jobs.shutdown();
        }
    }

    /**
     * A wait is an estimate made when a request is queued. When the service slows afterwards, a request whose turn comes
     * later than the bound is told so then, with a time to come back -- not answered late, not left to its connection's
     * lifetime. Fails before: every queued request ran, however late.
     */
    @Test
    void aRequestWhoseTurnComesLaterThanTheBoundIsToldSoThen() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 150, 1000, null);
        try {
            CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1), done = new CountDownLatch(4);
            AtomicInteger ran = new AtomicInteger(), late = new AtomicInteger(), continued = new AtomicInteger();
            java.util.concurrent.atomic.AtomicLong comeBack = new java.util.concurrent.atomic.AtomicLong();
            jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } });
            await(started, "the worker did not start");
            for (int i = 0; i < 3; i++) {                     // queued on an estimate of a few ms each
                assertEquals(0, jobs.submit("10.0.0." + i, 0, false, () -> { ran.incrementAndGet(); done.countDown(); },
                        ms -> { late.incrementAndGet(); comeBack.set(ms); done.countDown(); }));
            }
            assertEquals(0, jobs.submit("10.0.0.3", 0, true, () -> { continued.incrementAndGet(); done.countDown(); }, ms -> fail("continuing")));
            Thread.sleep(400);                                // the worker is held far past the 150 ms bound
            gate.countDown();
            await(done, "requests neither answered nor told to come back");
            assertEquals(0, ran.get(), "a request was answered although its turn came later than the bound");
            assertEquals(3, late.get());
            assertEquals(3, jobs.late());
            assertEquals(1, continued.get(), "the request that continues an exchange was not answered");
            assertTrue(comeBack.get() >= DiscoveryJobs.MIN_RETRY_MS && comeBack.get() <= DiscoveryJobs.MAX_RETRY_MS, "come back in " + comeBack.get());
            // a request whose turn comes in time is answered as before
            CountDownLatch quick = new CountDownLatch(1);
            assertEquals(0, jobs.submit("10.0.0.5", 0, false, quick::countDown, ms -> fail("told too late: " + ms)));
            await(quick, "a request in time was not answered");
        } finally {
            jobs.shutdown();
        }
    }

    /** What may wait is never more than a sixteenth of the heap in all, and one address never more than an eighth of that. */
    @Test
    void theBoundsOfBytesFollowTheHeap() {
        long heap = Runtime.getRuntime().maxMemory();
        assertEquals(Math.min(DiscoveryJobs.DEFAULT_MAX_BYTES, heap / 16), DiscoveryJobs.maxBytesFor(DiscoveryJobs.DEFAULT_MAX_BYTES));
        assertEquals(heap / 16, DiscoveryJobs.maxBytesFor(Long.MAX_VALUE), "a configured bound above a sixteenth of the heap was taken");
        assertEquals(1_000, DiscoveryJobs.maxBytesFor(1_000), "a smaller configured bound was not kept");
        long all = DiscoveryJobs.maxBytesFor(DiscoveryJobs.DEFAULT_MAX_BYTES);
        assertTrue(DiscoveryJobs.maxBytesPerAddressFor(DiscoveryJobs.DEFAULT_MAX_BYTES_PER_ADDRESS, all) <= all / 8);
        assertEquals(125, DiscoveryJobs.maxBytesPerAddressFor(Long.MAX_VALUE, 1_000));
    }

    @Test
    void afterShutdownNothingIsQueuedAndWhatWaitedIsAnswered() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 600_000, 100, null);
        CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1), done = new CountDownLatch(3);
        jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } done.countDown(); });
        await(started, "the worker did not start");
        jobs.submit("a", done::countDown);
        jobs.submit("b", done::countDown);
        jobs.shutdown();
        // nothing to come back to: not "busy, come back in 30 s" (the listener may still accept for a moment)
        assertEquals(DiscoveryJobs.CLOSED, jobs.submit("c", () -> fail("queued after shutdown")));
        assertEquals(0, jobs.busy(), "a joiner that arrived after the stop was counted, and told, busy");
        gate.countDown();
        await(done, "what waited at shutdown was not answered");
    }
}
