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

    @Test
    void afterShutdownNothingIsQueuedAndWhatWaitedIsAnswered() throws Exception {
        DiscoveryJobs jobs = new DiscoveryJobs(1, 600_000, 100, null);
        CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(1), done = new CountDownLatch(3);
        jobs.submit("hold", () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } done.countDown(); });
        await(started, "the worker did not start");
        jobs.submit("a", done::countDown);
        jobs.submit("b", done::countDown);
        jobs.shutdown();
        assertTrue(jobs.submit("c", () -> fail("queued after shutdown")) > 0);
        gate.countDown();
        await(done, "what waited at shutdown was not answered");
    }
}
