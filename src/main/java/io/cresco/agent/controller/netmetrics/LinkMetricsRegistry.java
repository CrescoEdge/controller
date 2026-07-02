package io.cresco.agent.controller.netmetrics;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.metrics.CMetric;
import io.cresco.library.metrics.MeasurementEngine;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mesh-wide registry of {@link LinkMetrics}, one per neighbor edge (keyed by {@code region_agent}
 * path). This is the network-link piece of the MEASUREMENT subsystem: every edge's values are
 * registered as Micrometer meters in the shared {@link MeasurementEngine} (the same registry
 * PerfControllerMonitor uses), so they surface through the normal metrics path — NOT as health.
 * (Health is a separate Felix HealthCheck that reads these; see LinkQualityHealthCheck.)
 *
 * Hot paths (ping task, producer send) update the in-memory EWMA cheaply; the tuner loop calls
 * {@link #publishAll()} to push the current values into Micrometer on its interval.
 */
public class LinkMetricsRegistry {

    private final ConcurrentHashMap<String, LinkMetrics> links = new ConcurrentHashMap<>();
    private final MeasurementEngine measurementEngine; // may be null (metrics still collected in-memory)

    public LinkMetricsRegistry(MeasurementEngine measurementEngine) {
        this.measurementEngine = measurementEngine;
    }

    public LinkMetrics forPath(String path) {
        if (path == null) path = "unknown";
        return links.computeIfAbsent(path, p -> {
            LinkMetrics lm = new LinkMetrics(p);
            registerMeters(p);
            return lm;
        });
    }

    public LinkMetrics get(String path) { return links.get(path); }
    public Collection<LinkMetrics> all() { return links.values(); }
    public int size() { return links.size(); }

    /**
     * The single, stable key for this node's parent (uplink) edge, used by the RTT harvest, the
     * dataplane send instrumentation, and the link:quality health check so they all agree.
     * {@code getRegionalControllerPath()} is only non-null on a regional controller, so for an agent
     * we key by the region it reports into.
     */
    public static String parentLinkKey(ControllerEngine ce) {
        try {
            if (ce != null && ce.cstate != null) {
                String p = ce.cstate.getRegionalControllerPath();
                if (p != null && !p.isEmpty()) return p;
                String region = ce.cstate.getRegion();
                if (region != null && !region.isEmpty()) return region + "_parent";
            }
        } catch (Exception ignore) { }
        return "parent";
    }

    // Micrometer meter name for a link metric: link.<sanitized-path>.<metric>
    private static String meter(String path, String metric) {
        return "link." + path.replaceAll("[^A-Za-z0-9]", "_") + "." + metric;
    }

    private void registerMeters(String path) {
        if (measurementEngine == null) return;
        try {
            measurementEngine.setGauge(meter(path, "rtt_ms"), "link smoothed RTT (ms)", "netlink", CMetric.MeasureClass.GAUGE_DOUBLE);
            measurementEngine.setGauge(meter(path, "jitter_ms"), "link RTT variation (ms)", "netlink", CMetric.MeasureClass.GAUGE_DOUBLE);
            measurementEngine.setGauge(meter(path, "tx_mbps"), "link tx throughput (MB/s)", "netlink", CMetric.MeasureClass.GAUGE_DOUBLE);
            measurementEngine.setGauge(meter(path, "rx_mbps"), "link rx throughput (MB/s)", "netlink", CMetric.MeasureClass.GAUGE_DOUBLE);
            measurementEngine.setGauge(meter(path, "sendlat_ms"), "producer send-latency EWMA (ms)", "netlink", CMetric.MeasureClass.GAUGE_DOUBLE);
            measurementEngine.setGauge(meter(path, "backlog"), "broker pending backlog", "netlink", CMetric.MeasureClass.GAUGE_LONG);
        } catch (Exception ignore) { /* metrics are best-effort */ }
    }

    /** Push the current in-memory values of every link into Micrometer. Called on the tuner interval. */
    public void publishAll() {
        if (measurementEngine == null) return;
        for (LinkMetrics lm : links.values()) {
            String p = lm.getPath();
            try {
                measurementEngine.updateDoubleGauge(meter(p, "rtt_ms"), Math.max(0, lm.getSrtt()));
                measurementEngine.updateDoubleGauge(meter(p, "jitter_ms"), lm.getJitter());
                measurementEngine.updateDoubleGauge(meter(p, "tx_mbps"), lm.getTxBytesPerSec() / 1e6);
                measurementEngine.updateDoubleGauge(meter(p, "rx_mbps"), lm.getRxBytesPerSec() / 1e6);
                measurementEngine.updateDoubleGauge(meter(p, "sendlat_ms"), lm.getSendLatencyEwma());
                measurementEngine.updateLongGauge(meter(p, "backlog"), lm.getPendingBacklog());
            } catch (Exception ignore) { }
        }
    }
}
