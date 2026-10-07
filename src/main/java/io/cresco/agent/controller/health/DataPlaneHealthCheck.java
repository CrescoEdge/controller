package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.ActiveClient;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Local check: this node's own messaging-plane connection (the JMS "fault" URI to its broker) is
 * up. A down connection returns TEMPORARILY_UNAVAILABLE — the executor's grace window promotes a
 * sustained outage to CRITICAL, so a transient reconnect never looks fatal.
 */
public class DataPlaneHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public DataPlaneHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            ActiveClient ac = ce.getActiveClient();
            if (ac == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "active client not ready");
            }
            if (!ac.isFaultURIActive()) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "messaging fault URI not active");
            }
            // isFaultURIActive() reports the CONTROL-plane connection. Since control moved to its
            // own sockets, a wedged DATAPLANE connection is invisible to it - this check would sit
            // green while every dataplane consumer/producer call blocked. Probe it separately.
            Object dps = ce.getDataPlaneService();
            if (!(dps instanceof io.cresco.agent.data.DataPlaneServiceImpl)) {
                return new Result(Result.Status.OK, "messaging plane active");
            }
            io.cresco.agent.data.DataPlaneServiceImpl dp = (io.cresco.agent.data.DataPlaneServiceImpl) dps;
            if (!dp.isDataPlaneConnectionHealthy()) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "dataplane broker connection unusable");
            }
            // #22: a session whose listeners stopped draining stays "connected"; its queues show it
            synchronized (progress) {
                return evaluate(dp.listenerQueues(), progress, System.currentTimeMillis(), dp.listenerStallMs());
            }
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "dataplane check error: " + t);
        }
    }

    /** listener id -> {last delivered sequence id, when it last moved or its queue was empty (ms)}. */
    private final Map<String, long[]> progress = new HashMap<>();

    /**
     * #22: OK with each session's queue depth; WARN naming every listener that has held queued messages without
     * taking one for {@code stallMs} (its last delivered sequence id did not move). A queue at the prefetch limit
     * that keeps draining is busy, not stalled. {@code progress} carries the state between checks.
     */
    static Result evaluate(Map<String, Object[]> queues, Map<String, long[]> progress, long now, long stallMs) {
        progress.keySet().retainAll(queues.keySet());
        Map<String, Integer> depths = new TreeMap<>();
        StringBuilder stalled = new StringBuilder();
        for (Map.Entry<String, Object[]> e : queues.entrySet()) {
            String session = (String) e.getValue()[0];
            int depth = ((Number) e.getValue()[1]).intValue();
            long seq = ((Number) e.getValue()[2]).longValue();
            depths.merge(session, depth, Integer::sum);
            long[] p = progress.get(e.getKey());
            if (p == null || p[0] != seq || depth == 0) progress.put(e.getKey(), p = new long[]{seq, now});
            if (depth > 0 && now - p[1] >= stallMs) {
                String id = e.getKey();
                stalled.append(stalled.length() == 0 ? "" : ", ").append(session).append(" listener ")
                        .append(id.length() > 8 ? id.substring(0, 8) : id).append(" (").append(depth).append(" queued, idle ")
                        .append((now - p[1]) / 1000).append(" s)");
            }
        }
        if (stalled.length() > 0)
            return new Result(Result.Status.WARN, "dataplane listeners not draining: " + stalled + "; queue depth " + depths);
        return new Result(Result.Status.OK, "messaging plane active; dataplane queue depth " + depths);
    }
}
