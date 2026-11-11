package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The messages a node was to send before it could: while it had no producer (its link to a parent being made anew), or
 * while it held no credential their recipient can verify (an agent before its controller's first pong). Until now the
 * first were dropped with one ERROR line ({@code AgentProducer is null. Message not sent}) and the second left with a
 * signature their recipient refused; the reply to a request, or a plugin's state report, was simply gone. Here they wait,
 * in the order they came, and are sent when the node can ({@link ActiveClient}) -- or, when the hold time of one ends
 * first, sent as it stands if there is a producer and counted as dropped if there is none.
 *
 * <p><b>Bounded.</b> At most {@value #MAX_MESSAGES} messages and {@value #MAX_CHARS} characters of parameters in all, each
 * for at most the hold time its owner gives. A message there is no room for is not taken: it is sent, or its sender told,
 * at once, as before.
 *
 * <p>Only a queue: its lock is held for a few field updates and never across a send, a log line or a count.
 */
final class SendHold {

    static final int MAX_MESSAGES = 1024;
    static final long MAX_CHARS = 8L << 20;

    private static final class Held {
        final MsgEvent msg;
        final long until;
        final long chars;
        Held(MsgEvent msg, long until, long chars) { this.msg = msg; this.until = until; this.chars = chars; }
    }

    private final ArrayDeque<Held> queue = new ArrayDeque<>();
    private long chars;
    private boolean closed;

    /** Take {@code m} until a producer exists, for {@code holdMs} at most; false when there is no room (or the node is stopping). */
    synchronized boolean offer(MsgEvent m, long now, long holdMs) {
        if (closed || holdMs <= 0 || queue.size() >= MAX_MESSAGES) return false;
        long c = size(m);
        if (chars + c > MAX_CHARS) return false;
        queue.addLast(new Held(m, now + holdMs, c));
        chars += c;
        return true;
    }

    /**
     * Remove and return, in the order they came, the messages whose time has passed and those {@code ready} says can go
     * now (null: only those whose time has passed). At most {@value #MAX_MESSAGES} entries are looked at; {@code ready}
     * is asked outside the lock.
     */
    List<MsgEvent> take(java.util.function.Predicate<MsgEvent> ready, long now) {
        Held[] held;
        synchronized (this) {
            if (queue.isEmpty()) return List.of();
            held = queue.toArray(new Held[0]);
        }
        java.util.Set<Held> go = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Held h : held) {
            if (now >= h.until || (ready != null && ready.test(h.msg))) go.add(h);
        }
        if (go.isEmpty()) return List.of();
        List<MsgEvent> out = new ArrayList<>(go.size());
        synchronized (this) {
            // what another thread took meanwhile is not taken twice: only what is still queued leaves here
            for (java.util.Iterator<Held> i = queue.iterator(); i.hasNext(); ) {
                Held h = i.next();
                if (go.contains(h)) {
                    i.remove();
                    chars -= h.chars;
                    out.add(h.msg);
                }
            }
        }
        return out;
    }

    /** When the first of the messages held runs out; 0 when none is held. */
    synchronized long nextDeadline() {
        long next = 0L;
        for (Held h : queue) if (next == 0L || h.until < next) next = h.until;
        return next;
    }

    synchronized int size() { return queue.size(); }

    /** Take nothing more, and give back what is held (the node stops). */
    synchronized List<MsgEvent> close() {
        closed = true;
        List<MsgEvent> left = new ArrayList<>(queue.size());
        for (Held h : queue) left.add(h.msg);
        queue.clear();
        chars = 0;
        return left;
    }

    /** The characters of {@code m}'s parameters (a String's length is a field read: this never looks at the text). */
    private static long size(MsgEvent m) {
        long n = 64;
        Map<String, String> p = m.getParams();
        if (p != null) {
            for (Map.Entry<String, String> e : p.entrySet()) {
                n += e.getKey().length() + (e.getValue() == null ? 0 : e.getValue().length());
            }
        }
        return n;
    }
}
