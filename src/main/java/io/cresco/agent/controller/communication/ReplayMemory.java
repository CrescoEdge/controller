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
 * message is a 2^-64 event per entry held), in open-addressing tables of primitive longs: 8 to 21 bytes an entry in a
 * busy bucket, and about a kilobyte for a signer that sends little (its ring and a small table per bucket it wrote).
 *
 * <p><b>Atomic.</b> The digests are split over {@value #STRIPES} stripes, each with its own lock: the check and the record
 * are one step under the lock of the digest's stripe, so two copies of one message can never both be accepted.
 *
 * <p><b>Bounded, failing closed.</b> At most {@code maxEntries} are held. A stripe that is full drops its OLDEST bucket and
 * from then on refuses every message signed in or before it (as too old): memory never grows past the bound and a replay
 * is never accepted -- under a flood beyond the bound, the age a message may have shrinks instead (logged).
 *
 * <p><b>Each signer has its own.</b> What a node accepts from one signer is remembered apart from what it accepts from
 * every other ({@link #record(long, long, long, long)}): one ring of buckets per signer, at most
 * {@code maxEntries / }{@value #SIGNER_DIVISOR} entries each, and {@code maxEntries} in all. A signer that sends more than
 * its own memory holds shortens the age ITS messages may have, and no other signer's. In one shared memory a single
 * signer could fill it -- and, with signed times ahead of the clock, keep it full: such entries stay until their time has
 * passed -- and so have every other signer's messages refused for minutes.
 *
 * <p><b>A signer's memory drains by time.</b> A bucket whose signing times are all older than the max age holds nothing
 * that could still be accepted: it is dropped at that signer's next message and by a sweep (once a bucket's time), and a
 * signer's memory that is empty and idle goes. When every signer's memory TOGETHER holds the bound, the oldest signing
 * times go in every signer's memory at once ({@link #makeRoom}): from then on a message signed before them is refused as
 * too old, and fresh messages are accepted -- the age a message may have shrinks, as in the shared memory. Only a flood
 * that fills the bound within one bucket's time is refused outright (FULL).
 */
final class ReplayMemory {

    static final int STRIPES = 64;
    /** One signer's memory holds at most the whole bound divided by this. */
    static final int SIGNER_DIVISOR = 8;
    /** Signers remembered at once; past it a new signer's messages are refused until one goes (fails closed). */
    static final int SIGNERS_MAX = 200_000;

    /** Why {@link #record} refused. */
    enum Result { FRESH, REPLAY, TOO_OLD, CLOCK, FULL }

    interface Log { void line(String l); }

    private final long bucketMs;
    private final int ring;
    private final int perStripeMax;
    private final Stripe[] stripes = new Stripe[STRIPES];
    private final Log log;
    private volatile long lastLog;
    // ---- one memory per signer
    private final java.util.concurrent.ConcurrentHashMap<Long, Stripe> signers = new java.util.concurrent.ConcurrentHashMap<>();
    private final long maxEntries, retainMs, maxAgeMs;
    private final int perSignerMax;
    /** Bucket index: signing times before it are refused in every signer's memory (raised when all together hold the bound). */
    private volatile long globalFloor = Long.MIN_VALUE;
    private final Object roomLock = new Object();
    /** Entries held in the signers' memories, kept beside them: read on every message without their locks. */
    private final java.util.concurrent.atomic.LongAdder signerHeld = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.AtomicBoolean sweeping = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile long lastSweep;

    ReplayMemory(long maxAgeMs, long maxEntries, Log log) {
        this(maxAgeMs, maxAgeMs, maxEntries, log);
    }

    /** {@code maxFutureMs}: how far ahead of the receiver's clock an accepted signed time may be (at most the max age). */
    ReplayMemory(long maxAgeMs, long maxFutureMs, long maxEntries, Log log) {
        this.maxAgeMs = maxAgeMs;
        this.bucketMs = Math.max(1_000L, maxAgeMs / 8);
        // the signed times a receiver accepts span 2 max-ages (either way of its clock): those buckets, and one more each side
        this.ring = (int) ((2 * maxAgeMs) / bucketMs) + 3;
        this.perStripeMax = (int) Math.max(256L, Math.min(Integer.MAX_VALUE / 2, maxEntries / STRIPES));
        this.log = log;
        for (int i = 0; i < STRIPES; i++) stripes[i] = new Stripe(ring);
        this.maxEntries = (long) perStripeMax * STRIPES;
        this.perSignerMax = (int) Math.max(256L, this.maxEntries / SIGNER_DIVISOR);
        // the last signed time a signer's memory can hold is maxFuture after its last message, acceptable a max-age more
        this.retainMs = maxAgeMs + Math.min(maxAgeMs, Math.max(0L, maxFutureMs)) + 2 * bucketMs;
    }

    /**
     * As {@link #record(long, long, long)}, in the memory of {@code signer} (a digest of its key; 0: the shared memory).
     * FULL when the bound of signers is reached, or when every signer's memory together holds the bound and no older
     * signing time is left to give up (a flood within one bucket's time).
     */
    Result record(long d, long signedAt, long now, long signer) {
        if (signer == 0) return record(d, signedAt, now);
        if (d == 0) d = 1;
        long b = Math.floorDiv(signedAt, bucketMs);
        // once a bucket's time (also after the clock was set back by more than that; a caller whose reading of the clock
        // is a moment older than another's is not that)
        if (now - lastSweep > bucketMs || lastSweep - now > bucketMs) sweep(now);
        String note = null;
        Result r;
        boolean madeRoom = false;
        while (true) {
            Stripe s = signers.get(signer);
            if (s == null) {
                if (signers.size() >= SIGNERS_MAX) {
                    note("MESSAGE SIGNATURE: the replay memory holds its bound of signers (" + SIGNERS_MAX
                            + "): messages of signers not heard lately are refused until one goes", now);
                    return Result.FULL;
                }
                Stripe made = new Stripe(ring);
                s = signers.putIfAbsent(signer, made);
                if (s == null) s = made;
            }
            boolean full = false;
            synchronized (s) {
                if (s.gone) continue;                            // swept meanwhile: take (or make) the present one
                // what this signer holds that no message could repeat any more (signed longer ago than the max age, or
                // before the oldest signing time every memory keeps) goes first: O(ring), once a bucket's time
                long keep = keep(now);
                if (s.floor < keep) {
                    long before = s.size;
                    s.expire(keep);
                    if (s.size != before) signerHeld.add(s.size - before);
                }
                if (signerHeld.sum() >= maxEntries && !s.holds(d, b)) {
                    full = true;
                    r = Result.FULL;
                } else {
                    long before = s.size;
                    r = s.record(d, b, perSignerMax);
                    if (s.size != before) signerHeld.add(s.size - before);
                    if (r == Result.FRESH || r == Result.REPLAY) s.lastRecord = now;
                    if (s.evicted != 0) {
                        note = "MESSAGE SIGNATURE: one signer's replay memory is full (" + perSignerMax + " entries, an eighth of"
                                + " msg_replay_memory_max=" + maxEntries + "): ITS messages signed before "
                                + new java.util.Date(s.floor * bucketMs) + " are now refused as too old ("
                                + (now - s.floor * bucketMs) / 1000 + " s instead of the max age); other signers are not affected";
                        s.evicted = 0;
                    }
                }
            }
            if (full && !madeRoom && makeRoom(now)) {
                madeRoom = true;                                 // once: the oldest signing times went everywhere
                continue;
            }
            if (full) {
                note = "MESSAGE SIGNATURE: the replay memory is full (" + signerHeld.sum() + " entries, msg_replay_memory_max="
                        + maxEntries + ") with signing times of the present moment alone: messages are refused until they age";
            }
            break;
        }
        if (note != null) note(note, now);
        return r;
    }

    /** The oldest bucket a signer's memory keeps now: older ones hold only signing times past the max age, or given up. */
    private long keep(long now) {
        return Math.max(Math.floorDiv(now - maxAgeMs, bucketMs), globalFloor);
    }

    /**
     * Every signer's memory together holds the bound: give up the oldest signing times in ALL of them, one bucket at a
     * time, until there is room -- from then on a message signed before them is refused as too old, in every memory. True
     * when there is room; false when what is held was all signed in the present bucket or later (nothing older is left
     * to give up). Each step visits every signer's memory once; it runs only while the bound is held.
     */
    private boolean makeRoom(long now) {
        synchronized (roomLock) {
            long present = Math.floorDiv(now, bucketMs);
            while (signerHeld.sum() >= maxEntries) {
                long next = keep(now) + 1;
                if (next > present) return false;
                globalFloor = next;
                expireAll(now);
                note("MESSAGE SIGNATURE: the replay memory is full (msg_replay_memory_max=" + maxEntries + " over " + signers.size()
                        + " signers): messages signed before " + new java.util.Date(next * bucketMs) + " are now refused as too old ("
                        + (now - next * bucketMs) / 1000 + " s instead of the max age), from every signer", now);
            }
            return true;
        }
    }

    /** Drop, in every signer's memory, the buckets no message could repeat any more. */
    private void expireAll(long now) {
        long keep = keep(now);
        for (Stripe s : signers.values()) {
            synchronized (s) {
                if (s.gone || s.floor >= keep) continue;
                long before = s.size;
                s.expire(keep);
                if (s.size != before) signerHeld.add(s.size - before);
            }
        }
    }

    private void note(String line, long now) {
        if (log != null && now - lastLog > 60_000L) {
            lastLog = now;
            log.line(line);
        }
    }

    /**
     * Once a bucket's time: drop in every signer's memory what no message could repeat any more, and the memories that
     * are empty and were not written for a bucket's time (or not at all for {@code retainMs}).
     */
    private void sweep(long now) {
        if (!sweeping.compareAndSet(false, true)) return;
        try {
            lastSweep = now;
            long keep = keep(now);
            for (java.util.Iterator<java.util.Map.Entry<Long, Stripe>> it = signers.entrySet().iterator(); it.hasNext(); ) {
                Stripe s = it.next().getValue();
                synchronized (s) {
                    if (s.floor < keep) {
                        long before = s.size;
                        s.expire(keep);
                        if (s.size != before) signerHeld.add(s.size - before);
                    }
                    // written after this caller read the clock: in use (never "idle for less than no time", which
                    // dropped a signer's memory, and with it what it had accepted, under a caller with an older reading)
                    long idle = Math.max(0L, now - s.lastRecord);
                    if (idle <= retainMs && (s.size > 0 || idle <= bucketMs)) continue;
                    s.gone = true;
                    signerHeld.add(-s.size);
                }
                it.remove();
            }
        } finally {
            sweeping.set(false);
        }
    }

    /** Signers with a memory (diagnostics, tests). */
    int signers() { return signers.size(); }

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
        long n = signerHeld.sum();
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
        /** As one signer's memory: when it last recorded, and that it was dropped (its holder takes another). */
        long lastRecord;
        boolean gone;

        /** Is {@code d} held in the bucket of time {@code b}? */
        boolean holds(long d, long b) {
            int slot = (int) Math.floorMod(b, (long) idx.length);
            return idx[slot] == b && sets[slot] != null && sets[slot].contains(d);
        }

        Stripe(int ring) {
            idx = new long[ring];
            sets = new LongSet[ring];
            java.util.Arrays.fill(idx, Long.MIN_VALUE);
        }

        /** Drop every bucket before {@code keep}; from now on a signing time before it is refused as too old. */
        void expire(long keep) {
            for (int i = 0; i < idx.length; i++) {
                if (idx[i] != Long.MIN_VALUE && idx[i] < keep) {
                    if (sets[i] != null) size -= sets[i].size;
                    sets[i] = null;
                    idx[i] = Long.MIN_VALUE;
                }
            }
            if (keep > floor) floor = keep;
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

    /**
     * An open-addressing set of non-zero longs (linear probing, grows at 3/4 load). It starts at 4 slots: a signer that
     * sends a message or two in a bucket's time holds 48 bytes for it, not the 528 of a 64-slot table -- with one memory
     * per signer, 200,000 quiet signers would otherwise hold gigabytes.
     */
    static final class LongSet {
        private long[] t = new long[4];
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
