package io.cresco.agent.controller.communication;

/**
 * The replay memory of message authentication (D4): every signature and MAC a node accepted, remembered while a copy of
 * it could still be accepted -- that is, while its SIGNED time is within {@code maxAgeMs} of the receiver's clock. A replay
 * carries the same signed time as the original, so each entry is kept in the bucket of its signing time
 * ({@code maxAgeMs / 8} wide), and a check looks in that one bucket: O(1). A bucket is dropped once every time it holds is
 * older than {@code maxAgeMs} (its messages are then refused by their age), so the memory holds about one max-age of
 * traffic.
 *
 * <p><b>Compact.</b> An entry is the first 64 bits of the SHA-256 of the signature or MAC (a false "replay" of a legitimate
 * message is a 2^-64 event per entry held), in open-addressing tables of primitive longs: 8 to 21 bytes an entry.
 *
 * <p><b>Atomic.</b> The digests are split over {@value #STRIPES} stripes, each with its own lock: the check and the record
 * are one step under the lock of the digest's stripe, so two copies of one message can never both be accepted.
 *
 * <p><b>Bounded, failing closed.</b> At most {@code maxEntries} are held. A stripe that is full drops its OLDEST bucket and
 * from then on refuses every message signed in or before it (as too old): memory never grows past the bound and a replay
 * is never accepted -- under a flood beyond the bound, the age a message may have shrinks instead (logged).
 */
final class ReplayMemory {

    static final int STRIPES = 64;

    /** Why {@link #record} refused. */
    enum Result { FRESH, REPLAY, TOO_OLD, CLOCK, FULL }

    interface Log { void line(String l); }

    private final long bucketMs;
    private final int ring;
    private final int perStripeMax;
    private final Stripe[] stripes = new Stripe[STRIPES];
    private final Log log;
    private volatile long lastLog;

    ReplayMemory(long maxAgeMs, long maxEntries, Log log) {
        this.bucketMs = Math.max(1_000L, maxAgeMs / 8);
        // the signed times a receiver accepts span 2 max-ages (either way of its clock): those buckets, and one more each side
        this.ring = (int) ((2 * maxAgeMs) / bucketMs) + 3;
        this.perStripeMax = (int) Math.max(256L, Math.min(Integer.MAX_VALUE / 2, maxEntries / STRIPES));
        this.log = log;
        for (int i = 0; i < STRIPES; i++) stripes[i] = new Stripe(ring);
    }

    /**
     * Record the authenticator whose 64-bit digest is {@code d}, signed at {@code signedAt}, as it is accepted now: FRESH
     * when it was not held (it is held from now on), else why it is refused.
     */
    Result record(long d, long signedAt, long now) {
        if (d == 0) d = 1;                                       // 0 marks an empty slot
        long b = Math.floorDiv(signedAt, bucketMs);
        Stripe s = stripes[(int) (d >>> 58)];
        String note = null;
        Result r;
        synchronized (s) {
            r = s.record(d, b, perStripeMax);
            if (s.evicted != 0) {
                note = "MESSAGE SIGNATURE: the replay memory is full (" + perStripeMax + " entries in a stripe, msg_replay_memory_max="
                        + (long) perStripeMax * STRIPES + "): messages signed before " + new java.util.Date((s.floor) * bucketMs)
                        + " are now refused as too old (" + (now - s.floor * bucketMs) / 1000 + " s instead of the max age)";
                s.evicted = 0;
            }
        }
        if (note != null && log != null && now - lastLog > 60_000L) {
            lastLog = now;
            log.line(note);
        }
        return r;
    }

    /** Entries held (diagnostics, tests). */
    long size() {
        long n = 0;
        for (Stripe s : stripes) synchronized (s) { n += s.size; }
        return n;
    }

    /** The first 64 bits of a SHA-256 digest. */
    static long digest64(byte[] sha256) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (sha256[i] & 0xffL);
        return v;
    }

    private static final class Stripe {
        final long[] idx;
        final LongSet[] sets;
        long floor = Long.MIN_VALUE;
        long size;
        long evicted;

        Stripe(int ring) {
            idx = new long[ring];
            sets = new LongSet[ring];
            java.util.Arrays.fill(idx, Long.MIN_VALUE);
        }

        Result record(long d, long b, int max) {
            if (b < floor) return Result.TOO_OLD;
            int slot = (int) Math.floorMod(b, (long) idx.length);
            if (idx[slot] != b) {
                if (idx[slot] > b) return Result.CLOCK;          // later times hold the slot: this clock went back a long way
                if (sets[slot] != null) size -= sets[slot].size;
                sets[slot] = null;                               // its times are all older than any accepted now
                idx[slot] = b;
            }
            LongSet set = sets[slot];
            if (set == null) sets[slot] = set = new LongSet();
            if (!set.add(d)) return Result.REPLAY;
            size++;
            while (size > max) {
                // full: drop the oldest bucket held, and refuse from now on what was signed in or before it
                int oldest = -1;
                for (int i = 0; i < idx.length; i++) {
                    if (sets[i] != null && sets[i].size > 0 && (oldest < 0 || idx[i] < idx[oldest])) oldest = i;
                }
                if (oldest < 0 || oldest == slot) {
                    // a flood within one bucket's time: never past the bound -- this one is not accepted
                    set.remove(d);
                    size--;
                    return Result.FULL;
                }
                size -= sets[oldest].size;
                floor = Math.max(floor, idx[oldest] + 1);
                sets[oldest] = null;
                idx[oldest] = Long.MIN_VALUE;
                evicted++;
            }
            return Result.FRESH;
        }
    }

    /** An open-addressing set of non-zero longs (linear probing, grows at 3/4 load). */
    static final class LongSet {
        private long[] t = new long[64];
        int size;

        boolean add(long v) {
            if ((size + 1) * 4L > t.length * 3L) grow();
            int mask = t.length - 1, i = mix(v) & mask;
            while (t[i] != 0) {
                if (t[i] == v) return false;
                i = (i + 1) & mask;
            }
            t[i] = v;
            size++;
            return true;
        }

        void remove(long v) {
            int mask = t.length - 1, i = mix(v) & mask;
            while (t[i] != 0 && t[i] != v) i = (i + 1) & mask;
            if (t[i] == 0) return;
            t[i] = 0;
            size--;
            // re-insert the rest of the cluster (linear probing)
            for (int j = (i + 1) & mask; t[j] != 0; j = (j + 1) & mask) {
                long x = t[j];
                t[j] = 0;
                int k = mix(x) & mask;
                while (t[k] != 0) k = (k + 1) & mask;
                t[k] = x;
            }
        }

        boolean contains(long v) {
            int mask = t.length - 1, i = mix(v) & mask;
            while (t[i] != 0) {
                if (t[i] == v) return true;
                i = (i + 1) & mask;
            }
            return false;
        }

        private void grow() {
            long[] old = t;
            t = new long[old.length * 2];
            int mask = t.length - 1;
            for (long v : old) {
                if (v == 0) continue;
                int i = mix(v) & mask;
                while (t[i] != 0) i = (i + 1) & mask;
                t[i] = v;
            }
        }

        private static int mix(long v) { return (int) (v ^ (v >>> 29) ^ (v >>> 47)); }
    }
}
