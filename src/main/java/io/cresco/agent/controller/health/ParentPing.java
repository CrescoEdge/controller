package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.MsgRouter;
import io.cresco.library.messaging.MsgEvent;

/**
 * What a child (an agent under its region controller, a region under its global) makes of the answer to its ping, and what
 * it holds while its parent REFUSES its pings.
 *
 * <p>A ping is answered by a pong only: the parent's ping handler ran, which it does for a ping it authenticated. Its
 * {@code 401 unauthenticated} answer (the ping did not verify there) is the ping itself coming back with a status: taken for
 * a pong, the child read its OWN ping as its parent's word -- its offer of the direct link as the parent's, its parent as
 * heard -- and looked healthy while its parent accepted nothing it sent. So the answers are told apart, and each consumer
 * of "a pong came" gets what it needs:
 * <ul>
 * <li><b>trust</b> (the parent is heard, its trust data, revocations and crypto policy are taken, the direct link comes
 *     back, the state resync, the parent's health, the round-trip time): a PONG only;</li>
 * <li><b>liveness</b> (is the parent's broker reachable: {@code link:parent}, which tears the link down and joins again
 *     when it is not): a PONG, and a REFUSAL too -- the parent answers, and joining it again in a loop cures nothing a
 *     refusal names. The link is then reported as WARN ({@link ParentLinkHealthCheck}), never OK, and one WARN line per
 *     {@value #LINE_EVERY_MS} ms says what the parent refuses and what cures it;</li>
 * <li>anything else that came back (a route error) and no answer at all: a miss, as before.</li>
 * </ul>
 * Every reply a watcher sees was authenticated by this node's inbox (or it would not have reached the call waiting for it).
 */
public final class ParentPing {

    /** The answer to a ping. */
    public enum Answer {
        /** Nothing came back in time, or something that is neither a pong nor the parent's refusal (a route error). */
        MISS,
        /** The parent's ping handler answered. */
        PONG,
        /** The parent answered that it does not accept the ping: {@code 401 unauthenticated}. */
        REFUSED
    }

    /** After its first line, a refusing parent is reported at most this often. */
    public static final long LINE_EVERY_MS = 60_000L;

    public static Answer answer(MsgEvent reply) {
        if (reply == null) return Answer.MISS;
        if ("401".equals(reply.getParam("status_code"))) return Answer.REFUSED;
        if (reply.getParam(MsgRouter.ROUTE_UNAVAILABLE) != null || reply.getParam(MsgRouter.ROUTE_REFUSED) != null) return Answer.MISS;
        // the parent's ping handler sets it (RegionalExecutor, GlobalExecutor); a ping that came back otherwise is no pong
        return "pong".equals(reply.getParam("action")) ? Answer.PONG : Answer.MISS;
    }

    private long since, lineAt;
    private int inARow, lines;
    private volatile String standing;

    /**
     * The parent refused a ping ({@code why}: its answer's {@code status_desc}). Returns the line to log at WARN when one
     * is due -- the first refusal at once, then one per {@value #LINE_EVERY_MS} ms -- else null.
     */
    public synchronized String refused(String parent, String why, long now) {
        if (inARow == 0) since = now;
        inARow++;
        String said = why == null || why.isEmpty() ? "401" : why;
        standing = "refuses this node's pings (" + said + "): " + inARow + " in a row";
        if (lines > 0 && now >= lineAt && now - lineAt < LINE_EVERY_MS) return null;
        lineAt = now;
        lines++;
        return "PARENT REFUSES THIS NODE: " + parent + " answered this node's ping with a refusal (" + said + "): " + inARow
                + " in a row, for " + (now - since) / 1000L + " s. The link is up and is NOT torn down; nothing that came with the"
                + " refusal is taken (the parent is not heard: no trust data, no attestation, no direct link), and what this node"
                + " sends is not accepted there. Cure: the parent's own log names the reason. When it is this node's credential --"
                + " it is not attested there, or its certificate is not the one the parent holds for its name -- restart this node"
                + " so that it joins again, or have the operator authorise its key at the parent (a rebind; a certificate of the"
                + " operator's PKI); when it is the time of its signatures, set this node's clock. (This line at most every "
                + LINE_EVERY_MS / 1000L + " s.)";
    }

    /** A pong: the parent accepts this node. Returns a line to log (INFO) when that ends a run of refusals, else null. */
    public synchronized String pong(String parent, long now) {
        if (inARow == 0) return null;
        String line = parent + " accepts this node's pings again, after " + inARow + " refusal(s) in " + (now - since) / 1000L + " s";
        inARow = 0;
        lines = 0;
        lineAt = 0L;
        standing = null;
        return line;
    }

    /** What the parent refuses, while its last answer was a refusal; null when it accepts this node (or has not answered). */
    public String standing() { return standing; }

    /** Refusals since the last pong; lines {@link #refused} handed out for them (diagnostics, tests). */
    public synchronized int refusedInARow() { return inARow; }
    public synchronized int lines() { return lines; }
}
