package io.cresco.agent.controller.netdiscovery;

import java.util.function.DoubleSupplier;

/**
 * When a node that is joining tries again, and the one line it logs meanwhile.
 *
 * <p>A node whose discovery was not answered slept 3 s and tried again, every node the same 3 s: a thousand nodes started
 * together came back together, every 3 s, for as long as their controller was behind. Here the wait depends on what
 * happened:
 * <ul>
 * <li>the responder is not what is behind -- nothing listens there ({@link TCPDiscoveryStatic.Outcome#UNREACHABLE}: the
 *     controller is not up yet), it answered and the exchange did not complete
 *     ({@link TCPDiscoveryStatic.Outcome#INCOMPLETE}: refused, not authentic), or discovery was answered and what follows
 *     failed (the broker connection, the registration: {@link #failedAfterDiscovery}): as often as before --
 *     {@code steadyMs} (retrywait, 3 s), spread over 75 % to 125 % of it so that nodes do not arrive in step. Nobody is
 *     being overloaded by this node's discovery, and a fabric forms as fast as before once its controller is up;</li>
 * <li>the responder is there and behind ({@link TCPDiscoveryStatic.Outcome#BUSY}: it said so;
 *     {@link TCPDiscoveryStatic.Outcome#NO_ANSWER}: the connection opened and nothing came back): a bounded exponential
 *     backoff with jitter -- {@code minMs}, doubled at each such failure in a row up to {@code maxMs}, each time a random
 *     50 % to 100 % of that; a responder that says it is busy and for how long is not asked again before that time (plus
 *     up to half as much again, so the nodes it sent away do not return together).</li>
 * </ul>
 * A success starts over. Not thread safe: one joining thread owns it.
 */
public final class JoinBackoff {

    public static final long DEFAULT_MIN_MS = 1_000;      // discovery_retry_min_ms
    public static final long DEFAULT_MAX_MS = 30_000;     // discovery_retry_max_ms

    private final long steadyMs, minMs, maxMs;
    private final DoubleSupplier random;
    private int attempts, failuresInARow;

    public JoinBackoff(long steadyMs, long minMs, long maxMs) {
        this(steadyMs, minMs, maxMs, () -> java.util.concurrent.ThreadLocalRandom.current().nextDouble());
    }

    /** @param random numbers in [0, 1) (tests pass a fixed one) */
    public JoinBackoff(long steadyMs, long minMs, long maxMs, DoubleSupplier random) {
        this.steadyMs = Math.max(1, steadyMs);
        this.minMs = Math.max(1, minMs);
        this.maxMs = Math.max(this.minMs, maxMs);
        this.random = random;
    }

    /** The attempt joined: the next failure starts from the shortest wait again. */
    public void succeeded() {
        attempts = 0;
        failuresInARow = 0;
    }

    /** Attempts that failed since the last success. */
    public int attempts() { return attempts; }

    /**
     * An attempt failed with {@code outcome}: how long to wait before the next one, in milliseconds.
     *
     * @param busyRetryMs what a busy responder asked for (not authenticated: never more than {@code maxMs} is taken from it); -1 none
     */
    public long failed(TCPDiscoveryStatic.Outcome outcome, long busyRetryMs) {
        attempts++;
        double r = Math.min(0.999999, Math.max(0.0, random.getAsDouble()));
        if (outcome != TCPDiscoveryStatic.Outcome.BUSY && outcome != TCPDiscoveryStatic.Outcome.NO_ANSWER) {
            failuresInARow = 0;
            return Math.max(1, Math.round(steadyMs * (0.75 + 0.5 * r)));
        }
        failuresInARow++;
        long step = minMs;
        for (int i = 1; i < failuresInARow && step < maxMs; i++) step = Math.min(maxMs, step * 2);
        long wait = Math.round(step * (0.5 + 0.5 * r));
        if (busyRetryMs > 0) wait = Math.max(wait, Math.round(Math.min(busyRetryMs, maxMs) * (1.0 + 0.5 * r)));
        return Math.max(1, Math.min(maxMs, wait));
    }

    /**
     * Discovery was answered and what follows it failed (the broker connection, the registration): the wait there always
     * was, spread. The responder is not behind, so nothing grows.
     */
    public long failedAfterDiscovery() {
        return failed(TCPDiscoveryStatic.Outcome.INCOMPLETE, -1);
    }

    /**
     * The one line a joining node logs for a failed attempt: who was asked, what happened and how long it took, which
     * attempt it was, and when the next one is.
     */
    public static String line(TCPDiscoveryStatic.Attempt a, String what, int attempt, long nextMs) {
        String who = a.host + ":" + a.port;
        String happened;
        switch (a.outcome) {
            case BUSY:
                happened = who + " is busy and asked this node to come back in " + a.busyRetryMs + " ms (said after " + a.elapsedMs + " ms)";
                break;
            case UNREACHABLE:
                happened = who + " cannot be reached (" + (a.detail == null ? "no connection" : a.detail) + ", after " + a.elapsedMs + " ms)";
                break;
            case INCOMPLETE:
                happened = who + " answered but the exchange did not complete (the reason is in the line before; " + a.elapsedMs + " ms)";
                break;
            default:
                happened = who + " did not answer in " + a.elapsedMs + " ms";
        }
        return "Discovery: " + what + ": " + happened + "; attempt " + attempt + ", trying again in " + (nextMs + 50) / 100 / 10.0 + " s";
    }
}
