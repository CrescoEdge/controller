package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.ActiveBroker;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Local check: the embedded ActiveMQ broker (regional/global controllers only). A plain agent has
 * no local broker, so this is a no-op OK there. Reads {@code ControllerEngine.getBroker().isHealthy()}
 * and the broker-manager flag; no messaging.
 */
public class BrokerHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public BrokerHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            if (ce.cstate == null || !ce.cstate.isRegionalController()) {
                return new Result(Result.Status.OK, "n/a (no local broker on this role)");
            }
            ActiveBroker broker = ce.getBroker();
            if (broker == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "broker not yet started");
            }
            if (!broker.isHealthy()) {
                return new Result(Result.Status.CRITICAL, "broker not started/healthy");
            }
            if (!ce.isActiveBrokerManagerActive()) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "broker manager not active");
            }
            return new Result(Result.Status.OK, "broker healthy");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "broker check error: " + t);
        }
    }
}
