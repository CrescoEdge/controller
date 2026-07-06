package io.cresco.agent.controller.netmetrics;

import io.cresco.agent.controller.communication.BrokeredAgent;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Network-aware, automated tuner. Every cycle it reads the measured {@link LinkMetrics} for each edge
 * and adapts the whole I/O control surface — end-to-end and live:
 *
 *  - socket buffer size  <- bandwidth-delay product (throughput x RTT): fat/long links get deeper buffers
 *  - read/block size     <- achieved throughput: busy links use bigger blocks to amortize per-message cost
 *  - connections per link <- saturation (producer send-latency EWMA + broker backlog): scale out/in
 *
 * Buffer/block changes are written to the shared {@link NetTuningProfile} (read live by in-controller
 * I/O: dataplane + bridge) and broadcast to out-of-controller plugins (wsapi, stunnel) as a
 * {@code nettuning} CONFIG MsgEvent so the fabric moves together. Connection changes call the broker's
 * dynamic {@code addBridgeConnections}/{@code removeBridgeConnections}. Everything is bounded + on a
 * cooldown so it converges instead of oscillating. Off by default ({@code net_autotune=false}).
 */
public class AutoTuner implements Runnable {

    private final ControllerEngine ce;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final LinkMetricsRegistry registry;
    private final NetTuningProfile profile;

    private ScheduledExecutorService exec;

    private final boolean enabled;
    private final int intervalSec;
    private final double sendLatHighMs, sendLatLowMs;
    private final long backlogHigh;
    private final long cooldownMs;
    private final double bdpSafety;
    // Capacity ceiling for the uplink (bits/sec). OSHI/sysinfo can supply the real NIC link speed; for
    // now it's config-provided (0 = unknown -> utilization not computed). Used as a hint + BDP cap.
    private final long linkSpeedCeilingBps;
    private final ConcurrentHashMap<String, Long> lastConnAction = new ConcurrentHashMap<>();
    private long lastProfileVersionBroadcast = -1;

    public AutoTuner(ControllerEngine ce, LinkMetricsRegistry registry, NetTuningProfile profile) {
        this.ce = ce;
        this.plugin = ce.getPluginBuilder();
        this.logger = plugin.getLogger(AutoTuner.class.getName(), CLogger.Level.Info);
        this.registry = registry;
        this.profile = profile;
        this.enabled = plugin.getConfig().getBooleanParam("net_autotune", false);
        this.intervalSec = plugin.getConfig().getIntegerParam("net_autotune_interval_sec", 5);
        this.sendLatHighMs = plugin.getConfig().getDoubleParam("net_autotune_sendlat_high_ms", 20.0);
        this.sendLatLowMs = plugin.getConfig().getDoubleParam("net_autotune_sendlat_low_ms", 2.0);
        this.backlogHigh = plugin.getConfig().getLongParam("net_autotune_backlog_high", 500L);
        this.cooldownMs = plugin.getConfig().getLongParam("net_autotune_cooldown_ms", 15000L);
        this.bdpSafety = plugin.getConfig().getDoubleParam("net_autotune_bdp_safety", 3.0);
        this.linkSpeedCeilingBps = plugin.getConfig().getLongParam("net_link_speed_bps", 0L);
    }

    // The loop ALWAYS runs to publish measurements into Micrometer; only the ACTUATION (buffer/block
    // adaptation + connection scaling) is gated by net_autotune. Measurement != control.
    public void start() {
        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "net-autotuner");
            t.setDaemon(true);
            return t;
        });
        exec.scheduleAtFixedRate(this, intervalSec, intervalSec, TimeUnit.SECONDS);
        logger.info("Net metrics loop started (interval=" + intervalSec + "s); autotune actuation="
                + (enabled ? "ON" : "off") + ", profile=" + profile);
    }

    public void stop() { if (exec != null) exec.shutdownNow(); }

    @Override
    public void run() {
        try {
            // congestion + capacity: poll the broker's pending backlog and set the NIC ceiling onto the
            // uplink edge before assessing saturation. (Item 1: JMX backlog; Item 3: capacity ceiling.)
            try {
                LinkMetrics up = registry.forPath(LinkMetricsRegistry.parentLinkKey(ce));
                if (ce.getBroker() != null) up.setPendingBacklog(ce.getBroker().getBrokerPendingBacklog());
                if (linkSpeedCeilingBps > 0) up.setLinkSpeedCeilingBps(linkSpeedCeilingBps);
            } catch (Exception ignore) { }

            double peakTx = 0, peakRtt = 0;
            boolean anySaturated = false, allIdle = true;

            for (LinkMetrics lm : registry.all()) {
                lm.sampleRates();
                double tput = Math.max(lm.getTxBytesPerSec(), lm.getRxBytesPerSec());
                peakTx = Math.max(peakTx, tput);
                if (lm.getSrtt() > 0) peakRtt = Math.max(peakRtt, lm.getSrtt());

                boolean saturated = lm.getSendLatencyEwma() > sendLatHighMs || lm.getPendingBacklog() > backlogHigh;
                boolean idle = lm.getSendLatencyEwma() < sendLatLowMs && lm.getPendingBacklog() == 0;
                anySaturated |= saturated;
                allIdle &= idle;

                if (enabled) maybeScaleConnections(lm, saturated, idle);
            }

            // measurement: always publish to Micrometer
            registry.publishAll();
            if (plugin.getConfig().getBooleanParam("net_metrics_log", false)) {
                for (LinkMetrics lm : registry.all()) logger.info("NETLINK " + lm);
            }

            // share: push this node's link-state onto the dataplane + ingest peers' (pub/sub, not pull).
            if (ce.getRouteAdvertiser() != null) ce.getRouteAdvertiser().tick();
            // coordinator consensus: heartbeat, recompute leader, bump epoch on change (multi-global).
            if (ce.getCoordinatorConsensus() != null) ce.getCoordinatorConsensus().tick();

            // actuation: only when autotune is enabled
            if (enabled) {
                adaptBuffersAndBlocks(peakTx, peakRtt, anySaturated, allIdle);
                maybeBroadcastProfile();
            }
        } catch (Exception ex) {
            logger.error("AutoTuner.run error: " + ex.getMessage(), ex);
        }
    }

    // Buffers <- bandwidth-delay product; block size <- achieved throughput. Both clamped by the profile.
    private void adaptBuffersAndBlocks(double peakTxBps, double peakRttMs, boolean saturated, boolean idle) {
        boolean changed = false;
        if (peakTxBps > 0 && peakRttMs > 0) {
            // BDP (bytes) = throughput(bytes/s) * RTT(s); give the socket buffer a safety multiple of it.
            long bdp = (long) (peakTxBps * (peakRttMs / 1000.0) * bdpSafety);
            changed |= profile.setSocketBufferBytes((int) Math.min(Integer.MAX_VALUE, bdp));
        }
        // block/read size tracks throughput: <64MB/s -> 64KB, scaling up toward the max on fast links.
        if (peakTxBps > 0) {
            int block;
            double mbps = peakTxBps / 1e6;
            if (mbps < 64) block = 64 * 1024;
            else if (mbps < 256) block = 256 * 1024;
            else if (mbps < 1024) block = 1024 * 1024;
            else block = 4 * 1024 * 1024;
            changed |= profile.setReadChunkBytes(block);
            changed |= profile.setWriteHighWaterBytes(Math.max(block, profile.getWriteHighWaterBytes()));
        }
        if (changed) {
            logger.info("AutoTuner adapted buffers/blocks (peakTx=" + String.format("%.0f", peakTxBps / 1e6)
                    + "MB/s peakRtt=" + String.format("%.2f", peakRttMs) + "ms): " + profile);
        }
    }

    private void maybeScaleConnections(LinkMetrics lm, boolean saturated, boolean idle) {
        String host = hostForPath(lm.getPath());
        if (host == null) return;
        int cur;
        try { cur = ce.getBroker().getBridgeConnectionCount(host); } catch (Exception e) { return; }
        if (cur == 0) return; // no inter-broker bridge to this host (agent-client path scales via shards)

        long now = System.currentTimeMillis();
        if (now - lastConnAction.getOrDefault(host, 0L) < cooldownMs) return;

        if (saturated && cur < profile.getMaxConns()) {
            int n = ce.getBroker().addBridgeConnections(host, 1);
            profile.setConnectionsPerLink(n);
            lastConnAction.put(host, now);
            logger.info("AUTOSCALE UP " + lm.getPath() + " -> " + n + " bridge conns  (" + lm + ")");
        } else if (idle && cur > profile.getMinConns()) {
            int n = ce.getBroker().removeBridgeConnections(host, 1);
            profile.setConnectionsPerLink(n);
            lastConnAction.put(host, now);
            logger.info("AUTOSCALE DOWN " + lm.getPath() + " -> " + n + " bridge conns  (" + lm + ")");
        }
    }

    // Push the current tuning snapshot to local plugins (wsapi, stunnel) so buffer/block sizes move
    // fabric-wide. Only sends when the profile version actually advanced.
    private void maybeBroadcastProfile() {
        long v = profile.getVersion();
        if (v == lastProfileVersionBroadcast) return;
        lastProfileVersionBroadcast = v;
        try {
            Map<String, String> snap = profile.snapshot();
            int sent = 0;
            // target the local wsapi + stunnel plugins by id (a CONFIG to the agent wouldn't reach them)
            for (String pname : new String[]{"io.cresco.wsapi", "io.cresco.stunnel"}) {
                for (Map<String, String> pm : ce.getGDB().getPluginListMapByType("pluginname", pname)) {
                    if (plugin.getRegion().equals(pm.get("region")) && plugin.getAgent().equals(pm.get("agent"))) {
                        MsgEvent tuning = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG,
                                pm.get("region"), pm.get("agent"), pm.get("pluginid"));
                        tuning.setParam("action", "nettuning");
                        for (Map.Entry<String, String> e : snap.entrySet()) tuning.setParam(e.getKey(), e.getValue());
                        plugin.msgOut(tuning);
                        sent++;
                    }
                }
            }
            logger.info("AutoTuner broadcast tuning v" + v + " to " + sent + " local plugins: " + profile);
        } catch (Exception ex) {
            logger.debug("AutoTuner broadcast failed: " + ex.getMessage());
        }
    }

    private String hostForPath(String path) {
        try {
            BrokeredAgent ba = ce.getBrokeredAgents().get(path);
            return (ba != null) ? ba.getActiveAddress() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
