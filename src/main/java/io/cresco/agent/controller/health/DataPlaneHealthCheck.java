package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.ActiveClient;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

import java.util.Map;

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
            // #22: a session whose listeners stopped draining stays "connected"; its queue depth shows it
            return depthResult(dp.sessionQueueDepths(), dp.sessionQueueWarnDepth());
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "dataplane check error: " + t);
        }
    }

    /** OK with each session's queue depth in the message; WARN naming the sessions at or above {@code warn}. */
    static Result depthResult(Map<String, Integer> depths, int warn) {
        StringBuilder over = new StringBuilder();
        for (Map.Entry<String, Integer> e : depths.entrySet())
            if (e.getValue() >= warn) over.append(over.length() == 0 ? "" : ", ").append(e.getKey()).append('=').append(e.getValue());
        if (over.length() > 0)
            return new Result(Result.Status.WARN, "dataplane listeners not draining (queue depth >= " + warn + "): " + over
                    + "; all sessions " + depths);
        return new Result(Result.Status.OK, "messaging plane active; dataplane queue depth " + depths);
    }
}
