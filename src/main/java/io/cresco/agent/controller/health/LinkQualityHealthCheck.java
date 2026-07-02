package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netmetrics.LinkMetrics;
import io.cresco.agent.controller.netmetrics.LinkMetricsRegistry;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Parent-link QUALITY check (tag {@code link:quality}). Distinct from {@code link:parent} liveness
 * (up/down): this reads the continuous {@link LinkMetrics} maintained by the MEASUREMENT subsystem and
 * returns {@code WARN} when the parent edge is <em>degraded</em> — smoothed RTT, jitter, producer
 * send-latency, or broker backlog over configured thresholds. This is the clean separation the design
 * intends: health <em>consumes</em> measurements to emit a verdict; it never re-measures. Returns
 * {@code OK} when the link is healthy or has no samples yet, so it never flaps a quiet fabric.
 */
public class LinkQualityHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public LinkQualityHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            LinkMetricsRegistry reg = ce.getLinkMetricsRegistry();
            if (reg == null || ce.cstate == null) {
                return new Result(Result.Status.OK, "metrics not ready");
            }
            String path = LinkMetricsRegistry.parentLinkKey(ce);
            LinkMetrics lm = reg.get(path);
            if (lm == null || lm.getRttSampleCount() == 0) {
                return new Result(Result.Status.OK, "no samples yet for " + path);
            }

            double rttWarnMs = ce.getPluginBuilder().getConfig().getDoubleParam("link_quality_rtt_warn_ms", 50.0);
            double jitterWarnMs = ce.getPluginBuilder().getConfig().getDoubleParam("link_quality_jitter_warn_ms", 25.0);
            double sendLatWarnMs = ce.getPluginBuilder().getConfig().getDoubleParam("link_quality_sendlat_warn_ms", 25.0);
            long backlogWarn = ce.getPluginBuilder().getConfig().getLongParam("link_quality_backlog_warn", 1000L);

            StringBuilder why = new StringBuilder();
            if (lm.getRttHigh() > rttWarnMs) why.append(String.format("rttHi=%.1fms ", lm.getRttHigh()));
            if (lm.getJitter() > jitterWarnMs) why.append(String.format("jitter=%.1fms ", lm.getJitter()));
            if (lm.getSendLatencyEwma() > sendLatWarnMs) why.append(String.format("sendLat=%.1fms ", lm.getSendLatencyEwma()));
            if (lm.getPendingBacklog() > backlogWarn) why.append("backlog=").append(lm.getPendingBacklog()).append(' ');

            if (why.length() > 0) {
                return new Result(Result.Status.WARN, "parent link degraded [" + path + "]: " + why.toString().trim());
            }
            return new Result(Result.Status.OK,
                    String.format("parent link ok [%s] srtt=%.2fms jitter=%.2fms sendLat=%.2fms", path,
                            lm.getSrtt(), lm.getJitter(), lm.getSendLatencyEwma()));
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "link:quality check error: " + t);
        }
    }
}
