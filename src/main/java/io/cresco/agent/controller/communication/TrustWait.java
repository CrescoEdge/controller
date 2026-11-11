package io.cresco.agent.controller.communication;

/**
 * The first moments of a join, as an inbox sees them: while its node does not hold its parent's trust data (the trust
 * bundle, the parent's whole revocation list), the inbox holds what it cannot judge without it -- from the moment the node
 * is first seen not ready, for at most {@code maxMs}. The wait belongs to each stretch of not being ready, that is to
 * EVERY join (a region that joins or rejoins a global long after its inbox was made, an agent that moved to another
 * controller), not to the inbox's first seconds alone; and a parent that never answers does not hold a node back for
 * longer than {@code maxMs}: after that its messages are judged with what it holds.
 */
final class TrustWait {

    private final long maxMs;
    /** When the node was first seen not ready in the present stretch; 0: it was ready when last asked. */
    private volatile long since;

    TrustWait(long maxMs) { this.maxMs = Math.max(0L, maxMs); }

    /** Should the inbox hold now what it cannot judge? {@code ready}: the node holds its parent's trust data. */
    boolean waiting(boolean ready, long now) {
        if (ready) {
            if (since != 0L) since = 0L;
            return false;
        }
        long s = since;
        if (s == 0L || now < s) since = s = now == 0L ? 1L : now;
        return now - s < maxMs;
    }
}
