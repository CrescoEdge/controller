package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Local check: JVM heap pressure. WARN above {@code health_mem_warn_pct} (default 85),
 * CRITICAL above {@code health_mem_crit_pct} (default 95), measured as used/max heap. Pure JDK.
 */
public class MemoryHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public MemoryHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            int warnPct = ce.getPluginBuilder().getConfig().getIntegerParam("health_mem_warn_pct", 85);
            int critPct = ce.getPluginBuilder().getConfig().getIntegerParam("health_mem_crit_pct", 95);
            Runtime rt = Runtime.getRuntime();
            long max = rt.maxMemory();
            long used = rt.totalMemory() - rt.freeMemory();
            double pct = (max > 0) ? (100.0 * used / max) : 0.0;
            String msg = String.format("heap %.1f%% used (%dMB/%dMB)",
                    pct, used / (1024L * 1024L), max / (1024L * 1024L));
            if (pct >= critPct) {
                return new Result(Result.Status.CRITICAL, msg);
            }
            if (pct >= warnPct) {
                return new Result(Result.Status.WARN, msg);
            }
            return new Result(Result.Status.OK, msg);
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "memory check error: " + t);
        }
    }
}
