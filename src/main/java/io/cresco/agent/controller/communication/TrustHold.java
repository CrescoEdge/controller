package io.cresco.agent.controller.communication;

import jakarta.jms.Message;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Messages an inbox holds because the node cannot judge them YET: its parent's trust data has not arrived (a node's first
 * moments after it joins: the trust bundle, the attestation and the revocations come on the first pong), or the signer's
 * controller is not in the trust data it holds (a region that joined a moment ago). Refusing them at once lost every
 * message that reached a node in that window -- 181 in the first 10 s of one storage core's start -- although the same
 * message verified a second later.
 *
 * <p>A held message is handed back to the inbox ({@code redeliver}) when the trust data changes, and once more when it
 * has been held {@code maxMs}: that last time the inbox does not hold it again, so it is accepted or refused as it then
 * stands. Nothing is ever dispatched unverified, and nothing is held for longer than {@code maxMs}.
 *
 * <p>Bounded: at most {@code maxCount} messages and {@code maxChars} of text; a message that does not fit is not held (its
 * inbox refuses it at once, as before). One daemon thread, started with the first message held.
 */
final class TrustHold {

    private static final class Held {
        final Message jms;
        final long since;
        final int chars;
        Held(Message jms, long since, int chars) { this.jms = jms; this.since = since; this.chars = chars; }
    }

    final long maxMs;
    private final int maxCount;
    private final long maxChars;
    private final Consumer<Message> redeliver;
    private final ArrayDeque<Held> held = new ArrayDeque<>();
    private long chars;
    private ScheduledExecutorService timer;
    private boolean closed;
    private final AtomicBoolean changed = new AtomicBoolean();
    /** On the redelivering thread: since when the message being redelivered is held (null: a first delivery). */
    private final ThreadLocal<Long> redelivering = new ThreadLocal<>();
    final AtomicLong heldTotal = new AtomicLong(), notHeld = new AtomicLong();

    TrustHold(long maxMs, int maxCount, long maxChars, Consumer<Message> redeliver) {
        this.maxMs = maxMs;
        this.maxCount = Math.max(1, maxCount);
        this.maxChars = Math.max(1L, maxChars);
        this.redeliver = redeliver;
    }

    /** Since when the message this thread is delivering has been held; null for a message off the wire. */
    Long since() { return redelivering.get(); }

    /** May the message this thread is delivering be held (again)? Not once it has been held {@code maxMs}. */
    boolean mayHold(long now) {
        Long since = redelivering.get();
        return since == null || now - since < maxMs;
    }

    /** Hold {@code jms} ({@code chars} of text); false when there is no room (or after {@link #close}). */
    boolean hold(Message jms, int chars, long now) {
        Long since = redelivering.get();
        synchronized (this) {
            if (closed || held.size() >= maxCount || this.chars + chars > maxChars) {
                notHeld.incrementAndGet();
                return false;
            }
            held.add(new Held(jms, since == null ? now : since, chars));
            this.chars += chars;
            if (since == null) heldTotal.incrementAndGet();
            if (timer == null) {
                timer = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "cresco-trust-hold");
                    t.setDaemon(true);
                    return t;
                });
                long tick = Math.max(20L, Math.min(250L, maxMs / 8));
                timer.scheduleWithFixedDelay(this::tick, tick, tick, TimeUnit.MILLISECONDS);
            }
        }
        return true;
    }

    /** The trust data this node verifies with changed: what is held is tried again (on the hold's thread, soon). */
    void trustChanged() { changed.set(true); }

    /** Messages held now (diagnostics, tests). */
    synchronized int size() { return held.size(); }

    private void tick() {
        try {
            boolean all = changed.getAndSet(false);
            long now = System.currentTimeMillis();
            List<Held> due = new ArrayList<>();
            synchronized (this) {
                for (java.util.Iterator<Held> it = held.iterator(); it.hasNext(); ) {
                    Held h = it.next();
                    if (all || now - h.since >= maxMs) {
                        it.remove();
                        chars -= h.chars;
                        due.add(h);
                    }
                }
            }
            for (Held h : due) {
                redelivering.set(h.since);
                try {
                    redeliver.accept(h.jms);
                } catch (RuntimeException ignore) {
                    // the inbox logs its own failures; one message never stops the rest
                } finally {
                    redelivering.remove();
                }
            }
        } catch (RuntimeException ignore) {
            // the timer goes on
        }
    }

    /** Stop; what is held is dropped (the inbox is closing). */
    void close() {
        ScheduledExecutorService t;
        synchronized (this) {
            closed = true;
            held.clear();
            chars = 0;
            t = timer;
            timer = null;
        }
        if (t != null) t.shutdownNow();
    }
}
