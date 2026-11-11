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
 * <p>Bounded: at most {@code maxCount} messages and {@code maxChars} of text (a message's body and the properties its
 * inbox counts with it), and at most an eighth of the messages from one sender (the node its broker says delivered it):
 * what is held is not verified yet, so one sender must not be able to take the room of the others. A message that does
 * not fit is not held (its inbox refuses it at once, as before). One daemon thread, started with the first message held.
 */
final class TrustHold {

    private static final class Held {
        final Message jms;
        final long since;
        final int chars;
        final String sender;
        Held(Message jms, long since, int chars, String sender) { this.jms = jms; this.since = since; this.chars = chars; this.sender = sender; }
    }

    /** One sender's share of the messages held. */
    static final int SENDER_DIVISOR = 8;

    final long maxMs;
    private final int maxCount;
    private final long maxChars;
    private final Consumer<Message> redeliver;
    private final ArrayDeque<Held> held = new ArrayDeque<>();
    private final java.util.HashMap<String, Integer> perSender = new java.util.HashMap<>();
    private final int perSenderMax;
    private volatile long lastChangeAt;
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
        this.perSenderMax = Math.max(1, this.maxCount / SENDER_DIVISOR);
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

    /** As {@link #hold(Message, int, long, String)} for a message whose sender is not known. */
    boolean hold(Message jms, int chars, long now) { return hold(jms, chars, now, null); }

    /**
     * Hold {@code jms} ({@code chars} of text) from {@code sender}; false when there is no room, in all or for that sender
     * (or after {@link #close}). {@code now}: the time its inbox read before it judged the message -- trust data that
     * changed since then is a reason to judge it again at once.
     */
    boolean hold(Message jms, int chars, long now, String sender) {
        Long since = redelivering.get();
        String from = sender == null ? "" : sender;
        synchronized (this) {
            int own = perSender.getOrDefault(from, 0);
            if (closed || held.size() >= maxCount || own >= perSenderMax || this.chars + chars > maxChars) {
                notHeld.incrementAndGet();
                return false;
            }
            held.add(new Held(jms, since == null ? now : since, chars, from));
            perSender.put(from, own + 1);
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
        // the trust data changed after this message was judged and before it was held: a tick that ran in between has
        // taken the change without seeing this message, which would then wait out its whole time
        if (lastChangeAt >= now) changed.set(true);
        return true;
    }

    /** The trust data this node verifies with changed: what is held is tried again (on the hold's thread, soon). */
    void trustChanged() {
        lastChangeAt = System.currentTimeMillis();
        changed.set(true);
    }

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
                        perSender.computeIfPresent(h.sender, (k, v) -> v <= 1 ? null : v - 1);
                        due.add(h);
                    }
                }
            }
            for (Held h : due) {
                redelivering.set(h.since);
                try {
                    redeliver.accept(h.jms);
                } catch (Throwable t) {
                    // the inbox logs its own failures; one message never stops the rest, nor the timer
                    passOnFatal(t);
                } finally {
                    redelivering.remove();
                }
            }
        } catch (Throwable t) {
            // the timer goes on: an exception that left this task would end it, and what is held would never be judged
            passOnFatal(t);
        }
    }

    /** A fatal JVM error (also a wrapped one) is not swallowed: the node halts on it. */
    private static void passOnFatal(Throwable t) {
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            if (c instanceof VirtualMachineError) throw (VirtualMachineError) c;
        }
    }

    /** Stop; what is held is dropped (the inbox is closing). Returns how many messages that was. */
    int close() {
        ScheduledExecutorService t;
        int dropped;
        synchronized (this) {
            closed = true;
            dropped = held.size();
            held.clear();
            perSender.clear();
            chars = 0;
            t = timer;
            timer = null;
        }
        if (t != null) t.shutdownNow();
        return dropped;
    }
}
