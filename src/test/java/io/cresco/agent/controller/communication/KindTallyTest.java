package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What refusals and drops are counted and logged with: each kind has its own count and its own log line, due at the
 * kind's first occurrence and then at most once per interval. One counter and one line per 10 s for everything hid whole
 * kinds behind whichever came first.
 */
class KindTallyTest {

    @Test
    void aKindsLineIsDueAtItsFirstCountThenAtMostOncePerInterval() {
        AtomicLong clock = new AtomicLong(1_000_000L);
        KindTally t = new KindTally(3, 10_000L, clock::get);
        assertTrue(t.count(0), "the first of a kind is not logged at once");
        for (int i = 0; i < 50; i++) {
            clock.addAndGet(100L);
            assertFalse(t.count(0), "logged again " + (i + 1) * 100 + " ms after its line");
        }
        assertEquals(51, t.of(0), "a count that is not logged is still a count");
        clock.set(1_000_000L + 9_999L);
        assertFalse(t.count(0));
        clock.set(1_000_000L + 10_000L);
        assertTrue(t.count(0), "not logged again once the interval has passed");
        assertFalse(t.count(0));
        assertEquals(54, t.of(0));
    }

    @Test
    void aKindThatHappensAllTheTimeNeverHidesAnother() {
        AtomicLong clock = new AtomicLong(5_000L);
        KindTally t = new KindTally(3, 10_000L, clock::get);
        assertTrue(t.count(0));
        for (int i = 0; i < 1000; i++) t.count(0);
        clock.addAndGet(1L);
        assertTrue(t.count(1), "the first of another kind waited behind the first kind's line");
        assertFalse(t.count(1));
        assertTrue(t.count(2));
        assertEquals(1001, t.of(0));
        assertEquals(2, t.of(1));
        assertEquals(1, t.of(2));
    }

    @Test
    void severalAtOnceAreCountedTogetherAndAClockThatWentBackDoesNotSilenceAKind() {
        AtomicLong clock = new AtomicLong(50_000L);
        KindTally t = new KindTally(1, 10_000L, clock::get);
        assertTrue(t.add(0, 7));
        assertEquals(7, t.of(0));
        clock.set(20_000L);                                  // the clock went back 30 s
        assertTrue(t.count(0), "silent until the clock catches up with the last line");
        assertFalse(t.count(0));
    }

    @Test
    void countsFromManyThreadsAreExactAndOneLineIsDue() throws Exception {
        KindTally t = new KindTally(2, 60_000L);
        int threads = 8, each = 20_000;
        AtomicInteger due = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1), done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            Thread th = new Thread(() -> {
                try {
                    start.await();
                    for (int k = 0; k < each; k++) if (t.count(1)) due.incrementAndGet();
                } catch (InterruptedException ignore) {
                } finally {
                    done.countDown();
                }
            });
            th.setDaemon(true);
            th.start();
        }
        start.countDown();
        done.await();
        assertEquals((long) threads * each, t.of(1));
        assertEquals(1, due.get(), "the first of a kind was logged by several threads, or by none");
        assertEquals(0, t.of(0));
    }
}
