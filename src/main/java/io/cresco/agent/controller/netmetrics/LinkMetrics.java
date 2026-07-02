package io.cresco.agent.controller.netmetrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-edge (neighbor path) link-quality metrics for the Cresco mesh. Latency is measured actively
 * (the RTT of the health ping RPC we already pay for); throughput and saturation are measured
 * passively (byte rate + producer send-latency + broker backlog). All values are EWMA-smoothed and
 * lock-free so they can be updated from hot paths (the ping task, the producer send path) and read
 * from the {@link ConnectionAutoScaler} loop.
 *
 * Design ref: docs/link-metrics-design.md. This is the substrate the auto-scaler and (later) a
 * cost-aware router consume — one object per {@code region_agent} edge.
 */
public class LinkMetrics {

    private final String path;

    // --- Latency: TCP-style smoothed RTT (Jacobson/Karels), milliseconds ---
    private volatile double srtt = -1;   // -1 => no sample yet
    private volatile double rttvar = 0;
    private static final double ALPHA = 0.125; // 1/8  (srtt gain)
    private static final double BETA = 0.25;   // 1/4  (rttvar gain)
    private volatile long lastRttTs = 0;
    private final AtomicLong rttSamples = new AtomicLong();

    // --- Saturation: producer send() dwell time EWMA (ms). Rises when the broker is memory/flow
    //     -control pressured -> a direct downstream-congestion signal from the path we already run. ---
    private volatile double sendLatencyEwma = 0;
    private static final double SL_ALPHA = 0.2;

    // --- Throughput: windowed byte rate (LongAdder counters sampled by the scaler) ---
    private final LongAdder txBytes = new LongAdder();
    private final LongAdder rxBytes = new LongAdder();
    private volatile long lastRateTs = System.currentTimeMillis();
    private volatile long lastTxBytes = 0, lastRxBytes = 0;
    private volatile double txBytesPerSec = 0, rxBytesPerSec = 0;

    // --- Congestion + capacity ---
    private volatile long pendingBacklog = 0;       // best native congestion signal (JMX QueueSize)
    private volatile long linkSpeedCeilingBps = 0;  // OSHI NIC link speed, if known (capacity ceiling)

    public LinkMetrics(String path) { this.path = path; }

    public String getPath() { return path; }

    // ---- Latency ----
    public void recordRtt(double rttMs) {
        if (rttMs < 0) return;
        if (srtt < 0) {                       // first sample
            srtt = rttMs;
            rttvar = rttMs / 2.0;
        } else {
            rttvar = (1 - BETA) * rttvar + BETA * Math.abs(srtt - rttMs);
            srtt = (1 - ALPHA) * srtt + ALPHA * rttMs;
        }
        lastRttTs = System.currentTimeMillis();
        rttSamples.incrementAndGet();
    }
    public double getSrtt() { return srtt; }
    public double getJitter() { return rttvar; }
    /** TCP RTO-style upper bound (srtt + 4*rttvar): the path's realistic worst-case latency. */
    public double getRttHigh() { return srtt < 0 ? -1 : srtt + 4 * rttvar; }
    public long getRttSampleCount() { return rttSamples.get(); }
    public long getLastRttTs() { return lastRttTs; }

    // ---- Saturation ----
    public void recordSendLatency(double ms) {
        if (ms < 0) return;
        sendLatencyEwma = (sendLatencyEwma == 0) ? ms : (1 - SL_ALPHA) * sendLatencyEwma + SL_ALPHA * ms;
    }
    public double getSendLatencyEwma() { return sendLatencyEwma; }

    // ---- Throughput ----
    public void addTxBytes(long n) { if (n > 0) txBytes.add(n); }
    public void addRxBytes(long n) { if (n > 0) rxBytes.add(n); }

    /** Compute windowed tx/rx byte rates since the last sample. Called periodically by the scaler. */
    public void sampleRates() {
        long now = System.currentTimeMillis();
        long dt = now - lastRateTs;
        if (dt <= 0) return;
        long tx = txBytes.sum(), rx = rxBytes.sum();
        txBytesPerSec = (tx - lastTxBytes) * 1000.0 / dt;
        rxBytesPerSec = (rx - lastRxBytes) * 1000.0 / dt;
        lastTxBytes = tx; lastRxBytes = rx; lastRateTs = now;
    }
    public double getTxBytesPerSec() { return txBytesPerSec; }
    public double getRxBytesPerSec() { return rxBytesPerSec; }

    // ---- Congestion / capacity ----
    public void setPendingBacklog(long b) { this.pendingBacklog = b; }
    public long getPendingBacklog() { return pendingBacklog; }
    public void setLinkSpeedCeilingBps(long c) { this.linkSpeedCeilingBps = c; }
    public long getLinkSpeedCeilingBps() { return linkSpeedCeilingBps; }

    /** Fraction of the NIC ceiling the tx rate is using (0..1+), or -1 if the ceiling is unknown. */
    public double getUtilization() {
        if (linkSpeedCeilingBps <= 0) return -1;
        return (txBytesPerSec * 8.0) / linkSpeedCeilingBps;
    }

    /** Composite link cost for a future cost-aware router: latency + backpressure + inverse-throughput. */
    public double cost() {
        double lat = (srtt < 0) ? 1.0 : getRttHigh();
        double tput = Math.max(1.0, txBytesPerSec / 1e6);      // MB/s, floored
        return lat + sendLatencyEwma + pendingBacklog * 0.1 + 50.0 / tput;
    }

    @Override
    public String toString() {
        return String.format("Link[%s] srtt=%.2fms jitter=%.2fms rttHi=%.2fms tx=%.1fMB/s rx=%.1fMB/s sendLat=%.2fms backlog=%d samples=%d",
                path, srtt, rttvar, getRttHigh(), txBytesPerSec / 1e6, rxBytesPerSec / 1e6, sendLatencyEwma, pendingBacklog, rttSamples.get());
    }
}
