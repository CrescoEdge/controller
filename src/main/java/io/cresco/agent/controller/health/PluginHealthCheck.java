package io.cresco.agent.controller.health;

import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

import java.util.Map;

/**
 * Local check: aggregate health of all plugins hosted on this node. Reads each plugin's
 * {@code status_code} from {@link PluginAdmin} and maps it via {@link StatusAdapter}; worst-wins.
 * A single sick plugin is intentionally a WARN/CRITICAL <em>health</em> signal only — per the
 * design it does not by itself drive a MINA state transition (see the HC-&gt;MINA bridge policy).
 */
public class PluginHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public PluginHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            PluginAdmin pa = ce.getPluginAdmin();
            if (pa == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "plugin admin not ready");
            }
            Map<String, Integer> codes = pa.getPluginStatusCodes();
            if (codes.isEmpty()) {
                return new Result(Result.Status.OK, "no plugins loaded");
            }
            Result.Status worst = Result.Status.OK;
            StringBuilder detail = new StringBuilder();
            for (Map.Entry<String, Integer> e : codes.entrySet()) {
                Result.Status s = StatusAdapter.fromPluginStatusCode(e.getValue());
                if (s.ordinal() > worst.ordinal()) {
                    worst = s;
                }
                if (detail.length() > 0) {
                    detail.append(", ");
                }
                detail.append(e.getKey()).append('=').append(e.getValue()).append('(').append(s).append(')');
            }
            return new Result(worst, codes.size() + " plugin(s): " + detail);
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "plugin check error: " + t);
        }
    }
}
