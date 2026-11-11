package io.cresco.agent.controller.communication;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Counts by kind, and says when a kind's log line is due: at its first occurrence, then at most once per interval --
 * each kind by itself, so a kind that happens all the time never hides one that happens once. A count costs one
 * {@link LongAdder} increment, one clock read and one volatile read; nothing is allocated, and the caller builds the
 * line only when it is due.
 */
final class KindTally {

    private final LongAdder[] counts;
    /** When each kind's line was last due (never 0 once it was); 0: never yet. */
    private final AtomicLongArray lineAt;
    private final long everyMs;
    private final LongSupplier clock;

    KindTally(int kinds, long everyMs) {
        this(kinds, everyMs, System::currentTimeMillis);
    }

    KindTally(int kinds, long everyMs, LongSupplier clock) {
        this.counts = new LongAdder[kinds];
        for (int i = 0; i < kinds; i++) counts[i] = new LongAdder();
        this.lineAt = new AtomicLongArray(kinds);
        this.everyMs = everyMs;
        this.clock = clock;
    }

    /** Count one of {@code kind}; true when its line is due now (to one caller only, when several count at once). */
    boolean count(int kind) {
        return add(kind, 1L);
    }

    /** Count {@code n} of {@code kind} at once; true when its line is due now. */
    boolean add(int kind, long n) {
        counts[kind].add(n);
        long now = clock.getAsLong();
        long last = lineAt.get(kind);
        // a clock that went back makes the line due, never silent for longer than the interval
        if (last != 0L && now >= last && now - last < everyMs) return false;
        return lineAt.compareAndSet(kind, last, now == 0L ? 1L : now);
    }

    long of(int kind) {
        return counts[kind].sum();
    }

    int kinds() {
        return counts.length;
    }
}
