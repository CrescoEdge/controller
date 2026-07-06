package io.cresco.agent.controller.netmetrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Phi-accrual failure detector (Hayashibara et al.). Instead of a binary alive/dead timeout it maintains,
 * per monitored node, a sliding window of heartbeat inter-arrival times and outputs a continuous suspicion
 * level phi = -log10(P(now - lastHeartbeat)), where P is the tail of the fitted (normal) inter-arrival
 * distribution. A caller takes graduated action on phi (warn / redirect / declare dead) rather than reacting
 * to one late heartbeat, which is exactly what a latency-heterogeneous, partition-prone mesh needs so a GC
 * pause or a transient jitter spike does not produce a false "lost" verdict.
 *
 * This replaces fixed-timeout aging for peer/parent liveness (see RegionHealthWatcher); combined with SWIM
 * indirect probing it also feeds the (quorum-committed) cross-domain LOST verdict.
 */
public final class PhiAccrualFailureDetector {

    /** Per-node heartbeat history + derived statistics. */
    private static final class History {
        final ConcurrentLinkedDeque<Long> intervals = new ConcurrentLinkedDeque<>();
        volatile long lastHeartbeatMs = -1;
        volatile double mean = 0, variance = 0;
    }

    private final ConcurrentHashMap<String, History> nodes = new ConcurrentHashMap<>();
    private final int windowSize;
    private final double minStdDevMs;      // floor on std-dev so a very regular stream still tolerates jitter
    private final long firstIntervalMs;    // seeded interval before any sample exists

    public PhiAccrualFailureDetector(int windowSize, double minStdDevMs, long firstIntervalMs) {
        this.windowSize = Math.max(3, windowSize);
        this.minStdDevMs = Math.max(1.0, minStdDevMs);
        this.firstIntervalMs = Math.max(1, firstIntervalMs);
    }

    public PhiAccrualFailureDetector() { this(100, 100.0, 5000L); }

    /** Record a heartbeat (successful ping/pong) from {@code node} at now. */
    public void heartbeat(String node) {
        History h = nodes.computeIfAbsent(node, k -> new History());
        long now = System.currentTimeMillis();
        synchronized (h) {
            if (h.lastHeartbeatMs > 0) {
                long interval = now - h.lastHeartbeatMs;
                h.intervals.addLast(interval);
                while (h.intervals.size() > windowSize) h.intervals.pollFirst();
            } else {
                // seed the window so phi is meaningful before the first measured interval
                for (int i = 0; i < 3; i++) h.intervals.addLast(firstIntervalMs);
            }
            h.lastHeartbeatMs = now;
            recompute(h);
        }
    }

    /**
     * Current suspicion level for {@code node}. 0 = just heard from it; grows without bound as silence
     * exceeds the expected inter-arrival distribution. phi=1 ~ 10% chance of a mistake, phi=2 ~ 1%, etc.
     * Returns 0 for an unknown node (never monitored → nothing to suspect yet).
     */
    public double phi(String node) {
        History h = nodes.get(node);
        if (h == null || h.lastHeartbeatMs < 0) return 0.0;
        long elapsed = System.currentTimeMillis() - h.lastHeartbeatMs;
        double stdDev = Math.max(minStdDevMs, Math.sqrt(h.variance));
        double y = (elapsed - h.mean) / stdDev;
        // logistic approximation of the Gaussian tail (Hayashibara), stable and cheap
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        double p;
        if (elapsed > h.mean) {
            p = e / (1.0 + e);
        } else {
            p = 1.0 - 1.0 / (1.0 + e);
        }
        p = Math.max(p, 1e-12);
        return -Math.log10(p);
    }

    /** True if {@code node}'s suspicion has crossed {@code threshold} (typical: 8–12 for a hard verdict). */
    public boolean isSuspect(String node, double threshold) { return phi(node) >= threshold; }

    /** Drop a node from monitoring (e.g. it left cleanly). */
    public void forget(String node) { nodes.remove(node); }

    public long lastHeartbeatMs(String node) {
        History h = nodes.get(node);
        return h == null ? -1 : h.lastHeartbeatMs;
    }

    private void recompute(History h) {
        int n = h.intervals.size();
        if (n == 0) { h.mean = firstIntervalMs; h.variance = minStdDevMs * minStdDevMs; return; }
        double sum = 0;
        for (long v : h.intervals) sum += v;
        double mean = sum / n;
        double varSum = 0;
        for (long v : h.intervals) { double d = v - mean; varSum += d * d; }
        h.mean = mean;
        h.variance = (n > 1) ? varSum / n : (minStdDevMs * minStdDevMs);
    }
}
