package io.cresco.agent.controller.netmetrics;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-peer path choice produced by explicit latency probing (see RegionHealthWatcher.measurePeerRtt).
 * For each peer region the prober times the DIRECT path (1-hop region&lt;-&gt;region bridge) and the
 * VIA-GLOBAL path (2-hop, forced by a source-route waypoint) and records which is faster. The MsgRouter
 * reads {@link #chosenSrcroute} for peer-bound traffic: when via-G wins, it attaches that waypoint stack
 * so the flow is steered onto the faster multi-hop path instead of ActiveMQ's default shortest-hop link.
 *
 * A hysteresis band guards the selection so a near-tie or noisy sample cannot make the route flap.
 */
public class PathTable {

    /** The current direct-vs-via-global decision for one peer, plus the two measurements behind it. */
    public static final class Choice {
        public volatile double directRtt = -1, viaGRtt = -1;
        public volatile String viaGRoute; // source-route stack for the via-global path
        public volatile boolean viaG;     // true => prefer via-G over the direct link
        public volatile long ts;
    }

    private final ConcurrentHashMap<String, Choice> table = new ConcurrentHashMap<>();
    private final double hysteresisMs;

    /**
     * @param hysteresisMs the guard band, in milliseconds, by which one path must beat the other before
     *                     the selection changes; a wider band trades responsiveness for stability (no flap).
     */
    public PathTable(double hysteresisMs) { this.hysteresisMs = hysteresisMs; }

    /** Ingest a fresh probe result for {@code peer} and (re)select with anti-flap hysteresis. */
    public void update(String peer, double directRtt, double viaGRtt, String viaGRoute) {
        Choice c = table.computeIfAbsent(peer, k -> new Choice());
        c.directRtt = directRtt;
        c.viaGRtt = viaGRtt;
        c.viaGRoute = viaGRoute;
        c.ts = System.currentTimeMillis();
        boolean viaGValid = viaGRtt >= 0, directValid = directRtt >= 0;
        if (viaGValid && (!directValid || viaGRtt + hysteresisMs < directRtt)) {
            c.viaG = true;                 // via-G clearly faster
        } else if (directValid && (!viaGValid || directRtt + hysteresisMs < viaGRtt)) {
            c.viaG = false;                // direct clearly faster
        }
        // else: within the hysteresis band -> keep the prior choice (no flap)
    }

    /** @return true if the via-global path is currently the selected (faster) route to {@code peer}. */
    public boolean chosenIsViaG(String peer) {
        Choice c = table.get(peer);
        return c != null && c.viaG;
    }

    /** The source-route stack to attach for {@code peer}, or null to use the default direct route. */
    public String chosenSrcroute(String peer) {
        Choice c = table.get(peer);
        return (c != null && c.viaG) ? c.viaGRoute : null;
    }

    /** @return the raw {@link Choice} for {@code peer} (both measurements + selection), or null if unprobed. */
    public Choice get(String peer) { return table.get(peer); }
}
