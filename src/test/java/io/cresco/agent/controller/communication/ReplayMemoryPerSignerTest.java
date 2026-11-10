package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The replay memory keeps what a node accepted from each signer apart: one signer, however much it sends and whatever
 * time it signs with, never has another signer's messages refused. In one shared memory a single authenticated node could
 * fill it with messages signed ahead of the clock and have every other signer refused until that time had passed.
 */
class ReplayMemoryPerSignerTest {

    static final long AGE = 300_000L, FUTURE = 60_000L, MAX = 64 * 256, PER_SIGNER = MAX / ReplayMemory.SIGNER_DIVISOR;
    static final long FLOODER = 7, HONEST = 9;

    @Test
    void aFloodSignedAheadOfTheClockNeverRefusesAnotherSigner() {
        ReplayMemory mem = new ReplayMemory(AGE, FUTURE, MAX, null);
        Random rnd = new Random(1);
        long now = 1_800_000_000_000L;
        int fresh = 0, full = 0;
        for (int i = 0; i < 4 * MAX; i++) {
            ReplayMemory.Result r = mem.record(rnd.nextLong() | 1, now + FUTURE - 1000, now, FLOODER);
            if (r == ReplayMemory.Result.FRESH) fresh++;
            else if (r == ReplayMemory.Result.FULL) full++;
            else fail("a flooder's new message: " + r);
        }
        assertEquals(PER_SIGNER, fresh, "the flooder holds its own memory's bound and no more");
        assertEquals(4 * MAX - PER_SIGNER, full, "the rest of the flood is refused");
        // every other signer: as if the flood had not happened, now and while the flood's signed time has not passed
        for (long dt : new long[]{0, 10_000, 40_000, 59_000, 120_000, 299_000}) {
            for (int i = 0; i < 200; i++) {
                assertEquals(ReplayMemory.Result.FRESH, mem.record(rnd.nextLong() | 1, now + dt, now + dt, HONEST),
                        "an honest signer's message " + dt / 1000 + " s after the flood");
            }
        }
        assertTrue(mem.size() <= MAX, "past the bound: " + mem.size());
    }

    @Test
    void aSignerPastItsBoundShortensItsOwnAgeOnly() {
        ReplayMemory mem = new ReplayMemory(AGE, FUTURE, MAX, null);
        Random rnd = new Random(2);
        long now = 1_800_000_000_000L;
        long oldOfBusy = rnd.nextLong() | 1, oldOfQuiet = rnd.nextLong() | 1;
        assertEquals(ReplayMemory.Result.FRESH, mem.record(oldOfBusy, now - 200_000, now, FLOODER));
        for (int i = 0; i < 3 * PER_SIGNER; i++) mem.record(rnd.nextLong() | 1, now - (i % 4) * 40_000L, now, FLOODER);
        assertEquals(ReplayMemory.Result.TOO_OLD, mem.record(rnd.nextLong() | 1, now - 200_000, now, FLOODER),
                "the busy signer's own old messages are now refused by age");
        assertNotEquals(ReplayMemory.Result.FRESH, mem.record(oldOfBusy, now - 200_000, now, FLOODER), "a replay after eviction was accepted");
        assertEquals(ReplayMemory.Result.FRESH, mem.record(oldOfQuiet, now - 200_000, now, HONEST), "another signer's old message is not affected");
        assertEquals(ReplayMemory.Result.REPLAY, mem.record(oldOfQuiet, now - 200_000, now + 1, HONEST));
    }

    @Test
    void theBoundHoldsOverAllSignersTogether() {
        ReplayMemory mem = new ReplayMemory(AGE, FUTURE, MAX, null);
        Random rnd = new Random(3);
        long now = 1_800_000_000_000L;
        int full = 0;
        for (long signer = 1; signer <= 20; signer++) {
            for (int i = 0; i < PER_SIGNER; i++) {
                ReplayMemory.Result r = mem.record(rnd.nextLong() | 1, now, now, signer);
                if (r == ReplayMemory.Result.FULL) full++;
                else assertEquals(ReplayMemory.Result.FRESH, r);
                assertTrue(mem.size() <= MAX, "past the bound: " + mem.size());
            }
        }
        assertEquals(20 * PER_SIGNER - MAX, full, "what does not fit is refused, never accepted unremembered");
    }

    @Test
    void aSignersMemoryGoesOnceNothingItHoldsCouldStillBeAccepted() {
        ReplayMemory mem = new ReplayMemory(AGE, FUTURE, MAX, null);
        Random rnd = new Random(4);
        long now = 1_800_000_000_000L;
        long d = rnd.nextLong() | 1;
        for (int i = 0; i < 100; i++) mem.record(rnd.nextLong() | 1, now, now, FLOODER);
        assertEquals(ReplayMemory.Result.FRESH, mem.record(d, now + FUTURE, now, FLOODER));
        assertEquals(2, signersAfter(mem, now + 1000));
        // while the latest time it holds can still be accepted (signed FUTURE ahead, acceptable a max-age more): kept
        long stillAccepted = now + FUTURE + AGE - 1000;
        assertEquals(2, signersAfter(mem, stillAccepted));
        assertEquals(ReplayMemory.Result.REPLAY, mem.record(d, now + FUTURE, stillAccepted, FLOODER), "a replay within the accepted time");
        // ... and from then counted from that last message; once past it, gone
        long gone = stillAccepted + AGE + FUTURE + 3 * (AGE / 8);
        assertEquals(1, signersAfter(mem, gone), "the idle signer's memory was kept");
        assertTrue(mem.size() < 50, "its entries still count: " + mem.size());
    }

    /** Signers remembered after another signer's message at {@code now} (which runs the sweep). */
    private static int signersAfter(ReplayMemory mem, long now) {
        mem.record(now | 1, now, now, HONEST);
        return mem.signers();
    }

    @Test
    void oneCopyOnlyIsAcceptedAtOnce() throws Exception {
        ReplayMemory mem = new ReplayMemory(AGE, FUTURE, 1_000_000L, null);
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(16);
        try {
            Random rnd = new Random(5);
            for (int round = 0; round < 200; round++) {
                long d = rnd.nextLong() | 1, t = System.currentTimeMillis();
                java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
                java.util.List<java.util.concurrent.Future<ReplayMemory.Result>> fs = new java.util.ArrayList<>();
                for (int i = 0; i < 16; i++) fs.add(pool.submit(() -> { go.await(); return mem.record(d, t, t, HONEST); }));
                go.countDown();
                int fresh = 0;
                for (java.util.concurrent.Future<ReplayMemory.Result> f : fs) if (f.get() == ReplayMemory.Result.FRESH) fresh++;
                assertEquals(1, fresh, "copies accepted at once");
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
