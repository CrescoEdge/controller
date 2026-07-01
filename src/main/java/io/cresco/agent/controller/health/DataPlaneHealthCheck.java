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
            if (ac.isFaultURIActive()) {
                return new Result(Result.Status.OK, "messaging plane active");
            }
            return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "messaging fault URI not active");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "dataplane check error: " + t);
        }
    }
}
