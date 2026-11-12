package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Messages a node was to send before it could, in the order they came: while it had no producer (its link to a
 * controller being made anew), or while it held no credential their recipient can verify (an agent before its
 * controller's first pong). Until now the first were dropped with one ERROR line ({@code AgentProducer is null. Message
 * not sent}) and the second left with a signature their recipient refused.
 *
 * <p><b>First in, first out, and nothing else.</b> A queue of one kind of message ({@link ActiveClient} keeps one for
 * what waits on the attestation and one for the rest): its owner takes the head when the head can go, and while anything
 * is in it every later message of that kind goes behind it. There is ONE time limit for the whole stretch, set when the
 * queue stops being empty ({@link #stretchUntil}), not one per message: a later message can never leave before an
 * earlier one because its own time came first.
 *
 * <p><b>Bounded.</b> At most {@value #MAX_MESSAGES} messages and {@value #MAX_CHARS} characters of parameters. A message
 * there is no room for is not taken.
 *
 * <p>Only a queue: its lock is held for a few field updates and never across a send, a log line or a count.
 */
final class SendHold {

    static final int MAX_MESSAGES = 1024;
    static final long MAX_CHARS = 8L << 20;

    /** A message held, and the controller this node had above it when it was held ({@code region_agent}; null: none). */
    static final class Held {
        final MsgEvent msg;
        final String uplink;
        final long chars;
        Held(MsgEvent msg, String uplink, long chars) { this.msg = msg; this.uplink = uplink; this.chars = chars; }
    }

    private final ArrayDeque<Held> queue = new ArrayDeque<>();
    private long chars;
    private boolean closed;
    /** When the present stretch ends ({@link System#nanoTime}); 0: the queue is empty. */
    private long stretchUntil;

    /**
     * Put {@code m} behind what is held. An empty queue begins a stretch that ends {@code holdMs} from now; a queue that
     * holds something keeps its end. False when there is no room (or the node is stopping).
     */
    synchronized boolean offer(MsgEvent m, String uplink, long holdMs) {
        if (closed || holdMs <= 0 || queue.size() >= MAX_MESSAGES) return false;
        long c = size(m);
        if (chars + c > MAX_CHARS) return false;
        if (queue.isEmpty()) {
            long until = System.nanoTime() + holdMs * 1_000_000L;
            stretchUntil = until == 0 ? 1 : until;
        }
        queue.addLast(new Held(m, uplink, c));
        chars += c;
        return true;
    }

    /** The message held longest; null when none is held. */
    synchronized Held peek() { return queue.peekFirst(); }

    /** Take {@code head} if it is still the message held longest. */
    synchronized boolean take(Held head) {
        if (queue.peekFirst() != head) return false;
        queue.pollFirst();
        chars -= head.chars;
        if (queue.isEmpty()) stretchUntil = 0;
        return true;
    }

    /** Has the present stretch run out? False for an empty queue. */
    synchronized boolean timeUp() { return stretchUntil != 0 && System.nanoTime() - stretchUntil >= 0; }

    /** Milliseconds until the present stretch ends (0: it has ended), or -1 for an empty queue. */
    synchronized long remainingMs() {
        if (stretchUntil == 0) return -1;
        return Math.max(0L, (stretchUntil - System.nanoTime() + 999_999L) / 1_000_000L);
    }

    /** The present stretch ends {@code holdMs} from now at the latest (what is held waits for something that comes sooner). */
    synchronized void endWithin(long holdMs) {
        if (stretchUntil == 0) return;
        long until = System.nanoTime() + Math.max(0L, holdMs) * 1_000_000L;
        if (until - stretchUntil < 0) stretchUntil = until == 0 ? 1 : until;
    }

    synchronized int size() { return queue.size(); }

    /** Take nothing more, and give back what is held (the node stops). */
    synchronized List<Held> close() {
        closed = true;
        List<Held> left = new ArrayList<>(queue);
        queue.clear();
        chars = 0;
        stretchUntil = 0;
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
