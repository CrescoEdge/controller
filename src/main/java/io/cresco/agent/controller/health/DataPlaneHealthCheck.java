package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.ActiveClient;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

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
            if (dps instanceof io.cresco.agent.data.DataPlaneServiceImpl
                    && !((io.cresco.agent.data.DataPlaneServiceImpl) dps).isDataPlaneConnectionHealthy()) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "dataplane broker connection unusable");
            }
            return new Result(Result.Status.OK, "messaging plane active");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "dataplane check error: " + t);
        }
    }
}
