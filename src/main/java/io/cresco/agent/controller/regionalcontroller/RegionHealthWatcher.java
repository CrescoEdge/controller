package io.cresco.agent.controller.regionalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netdiscovery.TCPDiscoveryStatic;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionHealthWatcher {
    public Timer communicationsHealthTimer;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    public Timer regionalUpdateTimer;
    // New Timer for active pinging Global Controller
    public Timer activePingTimer;

    private RegionalExecutor regionalExecutor;
    private AtomicBoolean communicationsHealthTimerActive = new AtomicBoolean();
    private AtomicBoolean regionalUpdateTimerActive = new AtomicBoolean();
    // New AtomicBoolean for ping timer
    private AtomicBoolean activePingTimerActive = new AtomicBoolean();
    private AtomicBoolean disabled = new AtomicBoolean(false); // Keep this if used elsewhere, otherwise remove

    private long pingInterval; // Interval for active ping checks to Global
    private long pingTimeout; // Timeout for waiting for ping response from Global

    // In-tick ping retry: a single delayed pong (GC pause / load burst) must not even register as a
    // miss. The failure DECISION and the anti-flap tolerance that used to live here (a
    // consecutive-missed-ticks counter) now live in HC: ParentLinkHealthCheck + the executor grace
    // window decide when the global is really lost.
    private int pingRetries;      // extra attempts within one tick before recording a miss

    private Set<String> registeredPeers = new HashSet<>(); // Add this field

    // Health->state moved to HC: transport-only. Stamps the last successful pong from the global
    // (parent) controller; ParentLinkHealthCheck reads it and the HC->MINA bridge fires
    // globalControllerLost after the grace window.
    private volatile long lastGlobalPongTs;

    // Phi-accrual failure detection for peer regions (Phase A / W7). Successful cost-probes are
    // heartbeats; a rising phi triggers SWIM indirect probing before any "peer unreachable" conclusion,
    // so an asymmetric A<->B partition does not become a false verdict.
    private final io.cresco.agent.controller.netmetrics.PhiAccrualFailureDetector peerFD =
            new io.cresco.agent.controller.netmetrics.PhiAccrualFailureDetector();
    private double phiSuspect;   // suspicion at which we start SWIM indirect probing
    private double phiDead;      // suspicion at which (after SWIM fails) we log an unreachable conclusion
    private int swimK;           // number of indirect probers to fan out to


    public RegionHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(RegionHealthWatcher.class.getName(),CLogger.Level.Info);
        this.regionalExecutor = new RegionalExecutor(controllerEngine);

        this.phiSuspect = plugin.getConfig().getDoubleParam("failure_phi_suspect", 4.0);
        this.phiDead = plugin.getConfig().getDoubleParam("failure_phi_dead", 8.0);
        this.swimK = plugin.getConfig().getIntegerParam("failure_swim_k", 2);

        long watchDogIntervalDelay = plugin.getConfig().getLongParam("watchdog_interval_delay",5000L);
        long commWatchDogInterval = plugin.getConfig().getLongParam("comm_watchdog_interval",5000L); // Interval for checking local components
        long watchDogInterval = plugin.getConfig().getLongParam("watchdog_interval",15000L); // Interval for checking agent statuses in DB
        // watchdog_scan_multiplier scales the node-status scan cadence (x watchdog_interval).
        // Historically this shared the period_multiplier key with the staleness threshold in
        // DBInterfaceImpl but with a DIFFERENT default (3 vs 10), so setting the shared key
        // silently changed both. The legacy key is honored as a fallback for existing configs.
        long periodMultiplier = plugin.getConfig().getLongParam("watchdog_scan_multiplier",
                plugin.getConfig().getLongParam("period_multiplier",3L));

        // Use separate config params for ping interval and timeout, or derive from wdTimer
        this.pingInterval = plugin.getConfig().getLongParam("region_ping_interval", 5000L); // ping cadence (also caps link-failure detection latency)
        this.pingTimeout = plugin.getConfig().getLongParam("region_ping_timeout", 5000L); // Default 5 second timeout for ping response from Global
        this.pingRetries = plugin.getConfig().getIntegerParam("region_ping_retries", 2); // extra attempts within a tick

        logger.debug("RegionHealthWatcher Initializing");
        communicationsHealthTimer = new Timer("RegionCommHealthTimer", true); // Daemon thread
        // This timer checks local broker/discovery health and updates its own status in DB
        communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), watchDogIntervalDelay, commWatchDogInterval);

        regionalUpdateTimer = new Timer("RegionNodeStatusTimer", true); // Daemon thread
        // This timer checks the status of agents connected to this region via DB timestamps
        regionalUpdateTimer.scheduleAtFixedRate(new RegionalNodeStatusWatchDog(controllerEngine, logger), watchDogIntervalDelay * periodMultiplier, periodMultiplier * watchDogInterval);//remote

        // --- NEW: Schedule Active Ping Timer for Global Controller ---
        // Only schedule if this node might connect to a global controller
        if (controllerEngine.cstate.isRegionalController()) { // Check if it's configured or potentially acting as a region
            this.activePingTimer = new Timer("RegionActivePingTimer", true); // Daemon thread
            this.activePingTimer.scheduleAtFixedRate(new ActivePingTask(), pingInterval, pingInterval); // Start after first interval
            logger.info("Active Ping Timer (for Global) scheduled with interval: {} ms", pingInterval);
        }
        // --- END NEW ---

        this.lastGlobalPongTs = System.currentTimeMillis();
        logger.info("Initialized");
    }

    public long getLastGlobalPongTs() { return lastGlobalPongTs; }

    public long getPingIntervalMs() { return pingInterval; }

    public void shutdown() {

        try {
            if (communicationsHealthTimer != null) {
                communicationsHealthTimer.cancel();
                communicationsHealthTimer = null;
            }
            if (regionalUpdateTimer != null) {
                regionalUpdateTimer.cancel();
                regionalUpdateTimer = null;
            }
            // --- NEW: Cancel Ping Timer ---
            if (activePingTimer != null) {
                activePingTimer.cancel();
                activePingTimer = null;
            }
            // --- END NEW ---

            // Wait for timers to potentially finish current cycle if needed (using AtomicBooleans)
            // This might be overly cautious if timers are daemon threads
            /*
            while (regionalUpdateTimerActive.get()) {
                Thread.sleep(100); // Short sleep
            }
            while(communicationsHealthTimerActive.get()) {
                Thread.sleep(100); // Short sleep
            }
            while(activePingTimerActive.get()) {
                 Thread.sleep(100); // Short sleep
            }
            */

            logger.debug("Shutdown");
        } catch (Exception ex) {
            logger.error("Shutdown Error: {}", ex.getMessage(), ex);
        }
    }

    private void maintainPeerConnections() {
        logger.trace("Maintaining regional peer connections...");
        String regionalPeersStr = plugin.getConfig().getStringParam("regional_peers");

        if (regionalPeersStr != null && !regionalPeersStr.isEmpty()) {
            List<String> regionalPeers = new ArrayList<>(Arrays.asList(regionalPeersStr.split(",")));

            for (String peerAddress : regionalPeers) {
                peerAddress = peerAddress.trim();
                try {
                    // A peer entry may be "host" (discovered on this node's own netdiscoveryport — the
                    // multi-host default where every region uses 32005) OR "host:port" (required
                    // same-host, where regions run on distinct discovery ports to avoid bind clashes).
                    String peerHost = peerAddress;
                    int peerPort = plugin.getConfig().getIntegerParam("netdiscoveryport", 32005);
                    int colon = peerAddress.lastIndexOf(':');
                    if (colon > 0 && colon < peerAddress.length() - 1) {
                        try {
                            peerPort = Integer.parseInt(peerAddress.substring(colon + 1).trim());
                            peerHost = peerAddress.substring(0, colon).trim();
                        } catch (NumberFormatException nfe) {
                            logger.warn("regional_peers entry '{}' has a non-numeric port; using default {}", peerAddress, peerPort);
                        }
                    }
                    // Discover the peer first to get its proper agent path
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
                    List<DiscoveryNode> discovered = ds.discover(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("peer_discovery_timeout",5000), peerHost, peerPort, true); // 5 sec timeout

                    if (discovered != null && !discovered.isEmpty()) {
                        DiscoveryNode peerNode = discovered.get(0);
                        String peerAgentPath = peerNode.getDiscoveredPath();

                        // Use isPeerConnected, which checks the brokeredAgents map directly
                        logger.debug("Peer Discovered Path: {}", peerAgentPath);
                        if (!controllerEngine.getBroker().isPeerConnected(peerAgentPath)) {
                            logger.warn("Peer connection to {} ({}) is down or not established. Attempting to connect...", peerAddress, peerAgentPath);
                            controllerEngine.getIncomingCanidateBrokers().put(peerNode);
                            logger.info("Submitted connection candidate for peer {}", peerAddress);
                        } else {
                            logger.trace("Peer connection to {} ({}) is already active or pending.", peerAddress, peerAgentPath);
                            // (cost-probing of connected peers happens in probeConnectedRegionPeers(),
                            //  which covers configured AND inferred peers uniformly)
                        }
                    } else {
                        logger.error("Could not discover or connect to peer: {}", peerAddress);
                    }
                } catch (Exception e) {
                    logger.error("Error while maintaining peer connection to {}: {}", peerAddress, e.getMessage());
                }
            }
        }
    }

    /**
     * SELF-ORGANIZATION. Using the mesh-wide route state (RouteView, learned via dataplane push), try to
     * form a DIRECT bridge to every known region we are not already directly connected to. Each region
     * advertises the dialable addresses of all its data-plane NICs; we attempt discovery on each. Only an
     * address on a shared PHYSICAL link answers -- so we LEARN which regions we CAN reach directly (the
     * network is not assumed fully meshed) and connect exactly those. Forming the bridge makes that region
     * a peer; probeConnectedRegionPeers() then measures the new direct path and the cost selector ADOPTS it
     * only if it beats the existing multi-hop / via-global path. That is: infer a link between two already-
     * (transitively-)connected regions, connect it, and use it iff it is faster.
     */
    public void inferConnections() {
        try {
            if (!plugin.getConfig().getBooleanParam("net_infer_connections", true)) return;
            io.cresco.agent.controller.netmetrics.RouteView rv = controllerEngine.getRouteView();
            if (rv == null) return;
            String self = plugin.getRegion() + "_" + plugin.getAgent();
            int timeout = plugin.getConfig().getIntegerParam("infer_discovery_timeout", 1000);
            for (io.cresco.agent.controller.netmetrics.RouteView.NodeState ns : rv.fresh()) {
                if (ns.node == null || ns.node.equals(self)) continue;
                if (!"region".equals(ns.role)) continue;                                 // region<->region only
                if (controllerEngine.getBroker().isPeerConnected(ns.node)) continue;      // already bridged
                if (ns.addrs == null || ns.addrs.isEmpty()) continue;
                for (String addr : ns.addrs) {
                    int colon = addr.lastIndexOf(':');
                    if (colon <= 0) continue;
                    String host = addr.substring(0, colon);
                    int port;
                    try { port = Integer.parseInt(addr.substring(colon + 1).trim()); } catch (Exception e) { continue; }
                    try {
                        TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
                        List<DiscoveryNode> found = ds.discover(DiscoveryType.REGION, timeout, host, port, true);
                        if (found != null && !found.isEmpty()) {
                            controllerEngine.getIncomingCanidateBrokers().put(found.get(0));
                            logger.info("INFERRED CONNECTION: region {} directly reachable at {}:{} (learned "
                                    + "from shared route state) -> self-organizing a bridge", ns.node, host, port);
                            break; // one reachable address suffices
                        }
                    } catch (Exception ignore) { }
                }
            }
        } catch (Exception e) {
            logger.debug("inferConnections error: {}", e.getMessage());
        }
    }

    /**
     * Exercise + verify GRAPH routing to regions we have NO direct link to. For each known non-peer
     * region, log the Dijkstra-computed lowest-latency path and send one real (steered) ping so the
     * MsgRouter enforces that path; the destination's receiver-stamp independently confirms the hops.
     */
    public void verifyGraphRoutes() {
        try {
            if (!plugin.getConfig().getBooleanParam("net_cost_routing", false)) return;
            io.cresco.agent.controller.netmetrics.RouteView rv = controllerEngine.getRouteView();
            if (rv == null) return;
            String self = plugin.getRegion() + "_" + plugin.getAgent();
            for (io.cresco.agent.controller.netmetrics.RouteView.NodeState ns : rv.fresh()) {
                if (ns.node == null || ns.node.equals(self) || ns.region == null) continue;
                if (!"region".equals(ns.role)) continue;
                if (controllerEngine.getBroker().isPeerConnected(ns.node)) continue;   // peers handled elsewhere
                String peerAgent = ns.node.substring(Math.min(ns.region.length() + 1, ns.node.length()));
                if (peerAgent.isEmpty()) continue;
                String route = io.cresco.agent.controller.netmetrics.RouteComputer.computeSrcRoute(rv, self, ns.node);
                double rtt = probePath(ns.region, peerAgent, null,
                        plugin.getConfig().getIntegerParam("peer_ping_timeout", 5000), false, "graph-route-verify");
                logger.info("GRAPH-ROUTE to {}: computed lowest-latency path=[{}] steered-rtt={}ms",
                        ns.node, (route == null ? "(direct)" : route), String.format("%.1f", rtt));
            }
        } catch (Exception e) {
            logger.debug("verifyGraphRoutes error: {}", e.getMessage());
        }
    }

    /** Cost-probe every connected REGION peer (configured or inferred) and update its path selection. */
    public void probeConnectedRegionPeers() {
        try {
            String globalRegion = controllerEngine.cstate.getGlobalRegion();
            List<DiscoveryNode> peers = new ArrayList<>();
            for (io.cresco.agent.controller.communication.BrokeredAgent ba : controllerEngine.getBrokeredAgents().values()) {
                DiscoveryNode dn = ba.brokerNode;
                if (dn == null || dn.discovered_region == null || dn.discovered_agent == null) continue;
                if (dn.discovered_region.equals(plugin.getRegion())) continue;           // skip self
                if (globalRegion != null && dn.discovered_region.equals(globalRegion)) continue; // skip global uplink
                peers.add(dn);
                measurePeerRtt(dn, ba.getPath());
            }
            evaluatePeerFailures(peers);   // W7: phi-accrual + SWIM after this round of probes
        } catch (Exception e) {
            logger.debug("probeConnectedRegionPeers error: {}", e.getMessage());
        }
    }

    /**
     * Phi-accrual + SWIM (W7). For each connected peer whose suspicion (phi) has crossed the suspect
     * threshold, do NOT immediately conclude it is gone: ask up to {@code swimK} OTHER peers to probe it
     * indirectly. If any relay reaches it, the fault is localized to OUR link (asymmetric partition) and the
     * suspicion is suppressed. Only if the indirect probes ALSO fail and phi exceeds the dead threshold do we
     * log an "unreachable" conclusion — which Phase D turns into a quorum-committed LOST verdict.
     */
    private void evaluatePeerFailures(List<DiscoveryNode> peers) {
        for (DiscoveryNode suspect : peers) {
            String sRegion = suspect.discovered_region, sAgent = suspect.discovered_agent;
            double ph = peerFD.phi(sRegion);
            if (ph < phiSuspect) continue;  // healthy enough, no action
            // gather candidate relays: other connected peers (not the suspect)
            List<DiscoveryNode> relays = new ArrayList<>();
            for (DiscoveryNode p : peers) {
                if (!p.discovered_region.equals(sRegion) && !p.discovered_region.equals(plugin.getRegion())) relays.add(p);
            }
            boolean reachedIndirect = false;
            String viaRelay = null;
            int probed = 0;
            for (DiscoveryNode relay : relays) {
                if (probed >= swimK) break;
                probed++;
                if (indirectProbe(relay, sRegion, sAgent)) { reachedIndirect = true; viaRelay = relay.discovered_region; break; }
            }
            if (reachedIndirect) {
                logger.info(String.format("SWIM: peer %s suspected (phi=%.1f) but REACHABLE via relay %s -> "
                        + "suspicion SUPPRESSED (asymmetric partition on our direct link, not a peer failure).",
                        sRegion, ph, viaRelay));
            } else if (ph >= phiDead) {
                logger.warn(String.format("PEER-UNREACHABLE: %s phi=%.1f, SWIM indirect probe via %d relay(s) also "
                        + "failed -> peer is unreachable from the mesh (Phase D would commit a quorum LOST verdict).",
                        sRegion, ph, probed));
            } else {
                logger.info(String.format("SWIM: peer %s suspected (phi=%.1f), no relays available to confirm; "
                        + "holding verdict (phi<dead=%.1f).", sRegion, ph, phiDead));
            }
        }
    }

    /**
     * SWIM indirect probe: ask {@code relay} to ping {@code targetRegion/targetAgent} on our behalf and report
     * whether it is reachable. Returns true iff the relay confirms reachability.
     */
    private boolean indirectProbe(DiscoveryNode relay, String targetRegion, String targetAgent) {
        try {
            int timeout = plugin.getConfig().getIntegerParam("peer_ping_timeout", 5000);
            MsgEvent req = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, relay.discovered_region, relay.discovered_agent);
            if (req == null) return false;
            req.setParam("action", "indirectprobe");
            req.setParam("target_region", targetRegion);
            req.setParam("target_agent", targetAgent);
            req.setParam("no_cost_route", "1");
            MsgEvent resp = plugin.sendRPC(req, timeout);
            return resp != null && "true".equalsIgnoreCase(resp.getParam("reachable"));
        } catch (Exception e) {
            logger.debug("indirectProbe via {} failed: {}", relay.discovered_region, e.getMessage());
            return false;
        }
    }

    /**
     * Probe BOTH candidate paths to a connected peer region and select the faster by measured end-to-end
     * RTT (latency-dominant). Two timed pings via the shipped source-routing data plane:
     *   - DIRECT : addressed straight at the peer -> ActiveMQ takes the 1-hop region<->region bridge (link C).
     *   - VIA-G  : srcroute forces global as a waypoint -> the 2-hop path (this->global->peer).
     * The measured RTTs feed (a) the peer-edge LinkMetrics (direct) and (b) the PathTable, whose winning
     * srcroute the MsgRouter attaches to real peer-bound traffic. This is how Cresco routes around a slow
     * "short" link: if via-G is faster, its stack is chosen even though it has more broker hops.
     */
    private void measurePeerRtt(DiscoveryNode peerNode, String peerAgentPath) {
        try {
            io.cresco.agent.controller.netmetrics.LinkMetricsRegistry reg = controllerEngine.getLinkMetricsRegistry();
            if (reg == null || peerNode.discovered_region == null || peerNode.discovered_agent == null) return;
            String peerRegion = peerNode.discovered_region, peerAgent = peerNode.discovered_agent;
            String gRegion = controllerEngine.cstate.getGlobalRegion(), gAgent = controllerEngine.cstate.getGlobalAgent();
            int timeout = plugin.getConfig().getIntegerParam("peer_ping_timeout", 5000);

            // DIRECT probe (bypasses cost injection so it measures the RAW default path = the direct
            // 1-hop bridge / link C).
            double directRtt = probePath(peerRegion, peerAgent, null, timeout, true, "probe-DIRECT");
            if (directRtt >= 0) {
                reg.forPath(peerAgentPath).recordRtt(directRtt);
                peerFD.heartbeat(peerRegion);   // W7: successful reachability = a heartbeat for phi-accrual
            }

            // VIA-G probe: force global as an explicit source-route waypoint (also bypasses injection).
            double viaGRtt = -1;
            String viaGRoute = (gRegion != null && gAgent != null && !gRegion.equals(peerRegion))
                    ? gRegion + "," + gAgent + ";" + peerRegion + "," + peerAgent : null;
            if (viaGRoute != null) {
                viaGRtt = probePath(peerRegion, peerAgent, viaGRoute, timeout, true, "probe-VIAG");
            }

            // Select the faster path; record the choice for the MsgRouter to enforce.
            if (controllerEngine.getPathTable() != null) {
                controllerEngine.getPathTable().update(peerAgentPath, directRtt, viaGRtt, viaGRoute);
                boolean viaG = controllerEngine.getPathTable().chosenIsViaG(peerAgentPath);

                // ENFORCEMENT PROOF: a real-traffic ping that does NOT bypass injection. If cost routing
                // is enforcing the choice, MsgRouter steers this onto the chosen path -> its RTT tracks
                // the winner (via-G), not the raw default (direct). This is the end-to-end demonstration.
                double steeredRtt = probePath(peerRegion, peerAgent, null, timeout, false, "probe-STEERED-realtraffic");

                logger.info(String.format(
                        "PATH-PROBE to %s: direct=%.1fms via-G=%.1fms -> chose %s | real-traffic(steered)=%.1fms",
                        peerAgentPath, directRtt, viaGRtt, viaG ? "VIA-G" : "DIRECT", steeredRtt));
            }
        } catch (Exception e) {
            logger.debug("measurePeerRtt failed for {}: {}", peerAgentPath, e.getMessage());
        }
    }

    /**
     * Send one timed ping to {@code peerRegion/peerAgent}. If {@code srcroute} is non-null the probe is
     * source-routed over that waypoint stack (head becomes the forwardDst); otherwise it is addressed
     * directly. Returns the round-trip in ms, or -1 on miss.
     */
    private double probePath(String peerRegion, String peerAgent, String srcroute, int timeout, boolean bypassInject, String label) {
        try {
            MsgEvent probe = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, peerRegion, peerAgent);
            if (probe == null) return -1;
            probe.setParam("action", "ping");
            probe.setParam("desc", label);
            if (bypassInject) probe.setParam("no_cost_route", "1"); // measure the RAW path, unsteered
            if (srcroute != null) {
                // Head of the stack is the first waypoint; deliver there first, it pops+forwards onward.
                probe.setParam(io.cresco.agent.controller.communication.MsgRouter.SRCROUTE, srcroute);
                int comma = srcroute.indexOf(','), semi = srcroute.indexOf(';');
                String headRegion = srcroute.substring(0, comma);
                String headAgent = srcroute.substring(comma + 1, semi < 0 ? srcroute.length() : semi);
                probe.setForwardDst(headRegion, headAgent, null);
            }
            long t0 = System.nanoTime();
            MsgEvent resp = plugin.sendRPC(probe, timeout);
            return (resp != null) ? (System.nanoTime() - t0) / 1_000_000.0 : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    // sendRegionalMsg remains largely the same, handling incoming messages for the region
    public void sendRegionalMsg(MsgEvent incoming) {
        try {
            if (incoming.isGlobal()) {
                if(controllerEngine.cstate.isGlobalController()) {
                    // If this node IS the global controller, execute locally
                    regionalExecutor.sendGlobalMsg(incoming);
                } else {
                    // If this node is just a region, forward to the known global controller
                    regionalExecutor.remoteGlobalSend(incoming);
                }
            } else {
                // Handle messages destined for this region or its agents
                if (incoming.dstIsLocal(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID())) {
                    MsgEvent retMsg = null;
                    switch (incoming.getMsgType().toString().toUpperCase()) {
                        case "CONFIG":
                            retMsg = regionalExecutor.executeCONFIG(incoming);
                            break;
                        case "DISCOVER":
                            retMsg = regionalExecutor.executeDISCOVER(incoming);
                            break;
                        case "ERROR":
                            retMsg = regionalExecutor.executeERROR(incoming);
                            break;
                        case "EXEC":
                            retMsg = regionalExecutor.executeEXEC(incoming);
                            break;
                        case "INFO":
                            retMsg = regionalExecutor.executeINFO(incoming);
                            break;
                        case "WATCHDOG":
                            retMsg = regionalExecutor.executeWATCHDOG(incoming);
                            break;
                        case "KPI":
                            retMsg = regionalExecutor.executeKPI(incoming);
                            break;
                        default:
                            logger.error("UNKNOWN MESSAGE TYPE! {}", incoming.getParams());
                            break;
                    }

                    // Handle RPC response if necessary
                    if ((retMsg != null) && (retMsg.getParams().containsKey("is_rpc"))) {
                        retMsg.setReturn();
                        String callId = retMsg.getParam(("callId-" + plugin.getRegion() + "-" +
                                plugin.getAgent() + "-" + plugin.getPluginID()));
                        if (callId != null) {
                            plugin.receiveRPC(callId, retMsg);
                        } else {
                            plugin.msgOut(retMsg);
                        }
                    }
                } else {
                    logger.error("RegionalController received message not destined for it: {}", incoming.printHeader());
                }
            }
        } catch(Exception ex) {
            logger.error("sendRegionalMsg Error: {}", ex.getMessage(), ex);
        }
    }

    // Task to check health of local communication components (broker, discovery)
    class CommunicationHealthWatcherTask extends TimerTask {
        public void run() {
            boolean isHealthy = true;
            try {
                logger.trace("CommunicationHealthWatcherTask running...");

                // Update own controller status in DB (important for global controller visibility)
                MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                if (tick != null) {
                    tick.setParam("region_watchdog_update", controllerEngine.cstate.getRegion());
                    tick.setParam("agent_watchdog_update", controllerEngine.cstate.getAgent()); // Also update agent part
                    tick.setParam("mode", "REGION"); // Identify update source
                    controllerEngine.getGDB().nodeUpdate(tick);
                    logger.trace("Updated own watchdog status in DB.");
                    // ALSO send it to the global (LIVENESS tier, control-plane queue path). Despite
                    // this message being addressed to the global, it was only ever applied to the
                    // LOCAL DB, so the global's view of this region's liveness depended entirely on
                    // the dataplane stateUpdate — which bulk dataplane traffic can starve.
                    if (controllerEngine.cstate.isRegionalController() && !controllerEngine.cstate.isGlobalController()) {
                        plugin.msgOut(tick);
                    }
                } else {
                    logger.warn("CommunicationHealthWatcherTask: Failed to create watchdog message event!");
                }

                // Check local components only if this node is acting as a regional controller
                if (controllerEngine.cstate.isRegionalController()) {
                    if (!communicationsHealthTimerActive.compareAndSet(false, true)) {
                        logger.warn("CommunicationHealthWatcherTask already running, skipping cycle.");
                        return; // Already running
                    }
                    try {
                        // Check Discovery (if expected to be active)
                        // Note: Discovery might only run during initial phases, adjust check accordingly
                        /*
                        if (controllerEngine.isDiscoveryExpected() && !controllerEngine.isDiscoveryActive()) {
                            isHealthy = false;
                            logger.error("CommunicationHealthWatcherTask: Discovery shutdown detected!");
                        }
                        */

                        // Check Broker Manager
                        if (!(controllerEngine.isActiveBrokerManagerActive()) || !(controllerEngine.getActiveBrokerManagerThread().isAlive())) {
                            isHealthy = false;
                            logger.error("CommunicationHealthWatcherTask: Active Broker Manager shutdown detected!");
                        }

                        // Check Broker itself
                        if (controllerEngine.getBroker() == null || !controllerEngine.getBroker().isHealthy()) {
                            isHealthy = false;
                            logger.error("CommunicationHealthWatcherTask: Broker shutdown or unhealthy detected!");
                        }

                        // Local component health (broker/broker-manager) is now surfaced by the
                        // "broker" local HealthCheck, and the global connection by "link:parent".
                        // This task logs only; the HC->MINA bridge owns state transitions.
                        if (controllerEngine.cstate.getControllerState() == io.cresco.library.agent.ControllerMode.REGION_GLOBAL) {
                            String globalUri = controllerEngine.getActiveClient().getFaultTriggerURI();
                            if (globalUri == null || !controllerEngine.getActiveClient().isConnectionActive(globalUri)) {
                                logger.warn("CommunicationHealthWatcherTask: Global connection appears down (HC decides). URI: {}", globalUri);
                            }
                        }

                        if (!isHealthy) {
                            logger.warn("CommunicationHealthWatcherTask: local component unhealthy (HC decides recovery).");
                        } else {
                            logger.trace("CommunicationHealthWatcherTask: Local components appear healthy.");
                        }

                    } finally {
                        communicationsHealthTimerActive.set(false); // Release lock
                    }
                } // end isRegionalController check
            } catch (Exception ex) {
                if(communicationsHealthTimerActive.get()) {
                    communicationsHealthTimerActive.set(false); // Ensure lock release on exception
                }
                logger.error("CommunicationHealthWatcherTask Error: {}", ex.getMessage(), ex);
            }
        }
    }

    // Task to check status of connected agents via DB timestamps
    class RegionalNodeStatusWatchDog extends TimerTask {
        private ControllerEngine controllerEngine;
        private CLogger logger;
        private PluginBuilder plugin;

        public RegionalNodeStatusWatchDog(ControllerEngine controllerEngine, CLogger logger) {
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
            this.logger = logger;
        }

        public void run() {
            if (controllerEngine.cstate.isRegionalController()) { // Only run if node is regional controller

                // 1) keep CONFIGURED peer links up; 2) self-organize INFERRED links from learned route
                // state; 3) cost-probe EVERY connected region peer (configured or inferred) and select.
                controllerEngine.getRegionHealthWatcher().maintainPeerConnections();
                controllerEngine.getRegionHealthWatcher().inferConnections();
                controllerEngine.getRegionHealthWatcher().probeConnectedRegionPeers();
                controllerEngine.getRegionHealthWatcher().verifyGraphRoutes();

                if (!regionalUpdateTimerActive.compareAndSet(false, true)) {
                    logger.warn("RegionalNodeStatusWatchDog already running, skipping cycle.");
                    return; // Already running
                }
                try {
                    logger.debug("RegionalNodeStatusWatchDog running check...");
                    Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), null, null);

                    if (edgeStatus != null) {
                        for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {
                            // Skip checking self
                            if (!plugin.getAgent().equals(entry.getKey())) {
                                logger.trace("Checking Agent NodeID: {}, DB Status: {}", entry.getKey(), entry.getValue());
                                if (entry.getValue() == NodeStatusType.PENDINGSTALE) {
                                    logger.warn("Agent NodeID: {} is PENDINGSTALE. Setting to STALE.", entry.getKey());
                                    controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 40, "set STALE by regional controller health watcher");
                                } else if (entry.getValue() == NodeStatusType.STALE) {
                                    logger.error("Agent NodeID: {} is STALE. Setting to LOST.", entry.getKey());
                                    controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 50, "agent set LOST by regional controller health watcher");
                                    setPluginsLost(entry.getKey()); // Mark associated plugins as lost too
                                } else if (entry.getValue() == NodeStatusType.ERROR) {
                                    logger.error("Agent NodeID: {} is in ERROR state according to DB.", entry.getKey());
                                    // Consider further action for ERROR state if needed
                                }
                            }
                        }
                    } else {
                        logger.warn("RegionalNodeStatusWatchDog: getEdgeHealthStatus returned null.");
                    }
                    logger.debug("RegionalNodeStatusWatchDog check complete.");
                } catch (Exception ex) {
                    logger.error("RegionalNodeStatusWatchDog Error: {}", ex.getMessage(), ex);
                } finally {
                    regionalUpdateTimerActive.set(false); // Release lock
                }
            } // end isRegionalController check
        }

        // Helper to mark plugins as lost when an agent is lost
        private void setPluginsLost(String agentName) {
            try {
                Map<String, NodeStatusType> pluginStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), agentName, null);
                if (pluginStatus != null) {
                    for (Map.Entry<String, NodeStatusType> entry : pluginStatus.entrySet()) {
                        // Check if plugin is not already marked as LOST or something worse
                        if (entry.getValue() != NodeStatusType.LOST && entry.getValue() != NodeStatusType.ERROR) {
                            logger.error("Agent NodeID: {} lost. Setting Plugin NodeID: {} to LOST.", agentName, entry.getKey());
                            controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), agentName, entry.getKey(), 50, "plugin set LOST by regional controller health watcher due to agent loss");
                        }
                    }
                }
            } catch (Exception ex) {
                logger.error("setPluginsLost for agent [{}] Error: {}", agentName, ex.getMessage(), ex);
            }
        }
    }

    // --- NEW TASK for Actively Pinging Global Controller ---
    class ActivePingTask extends TimerTask {
        public void run() {
            // Only run if this region is connected to a global controller
            if (controllerEngine.cstate.getControllerState() == io.cresco.library.agent.ControllerMode.REGION_GLOBAL) {
                if (!activePingTimerActive.compareAndSet(false, true)) {
                    logger.warn("ActivePingTask (Global) already running, skipping cycle.");
                    return; // Exit if already running
                }
                String globalControllerPath = controllerEngine.cstate.getGlobalControllerPath();
                logger.debug("ActivePingTask: Sending PING to Global Controller [{}]", globalControllerPath);
                try {
                    MsgEvent pingRequest = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
                    if (pingRequest == null) {
                        logger.error("ActivePingTask: Failed to create global controller message event!");
                        activePingTimerActive.set(false);
                        return;
                    }
                    pingRequest.setParam("action", "ping");
                    pingRequest.setParam("desc", "region-ping-request"); // Identify the ping
                    // mesh health: advertise our rolled-up health (local + subtree) up to the global.
                    io.cresco.agent.controller.health.MeshHealthPing.advertiseChild(controllerEngine, pingRequest);

                    // Retry within this tick before counting a miss; a single delayed pong (GC pause,
                    // load burst) must never drop the global.
                    long pingT0 = System.nanoTime();
                    MsgEvent pingResponse = null;
                    for (int attempt = 0; attempt <= pingRetries && pingResponse == null; attempt++) {
                        if (attempt > 0) {
                            // small jittered backoff between in-tick retries
                            try { Thread.sleep(150L + (long) (Math.random() * 150L)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                        }
                        pingResponse = plugin.sendRPC(pingRequest, pingTimeout);
                    }

                    if (pingResponse == null) {
                        // Transport-only: record the miss. ParentLinkHealthCheck sees the stale
                        // lastGlobalPongTs and the HC->MINA bridge fires globalControllerLost after
                        // the grace window. No direct state transition here.
                        logger.warn("ActivePingTask: No PONG from Global [{}] within {}ms x{} attempts (miss recorded; HC decides).",
                                globalControllerPath, pingTimeout, pingRetries + 1);
                    } else {
                        // Healthy pong -> stamp liveness for ParentLinkHealthCheck.
                        lastGlobalPongTs = System.currentTimeMillis();
                        // measurement: harvest the region->global FEDERATION-edge RTT (this link rides a
                        // broker-to-broker bridge). Same free application-path timing as the agent harvest.
                        try {
                            io.cresco.agent.controller.netmetrics.LinkMetricsRegistry reg = controllerEngine.getLinkMetricsRegistry();
                            if (reg != null && globalControllerPath != null) {
                                reg.forPath(globalControllerPath).recordRtt((System.nanoTime() - pingT0) / 1_000_000.0);
                            }
                        } catch (Exception ignore) { }
                        // mesh health: record the global's advertised health carried back on the pong.
                        io.cresco.agent.controller.health.MeshHealthPing.recordParent(controllerEngine, pingResponse);
                        logger.debug("ActivePingTask: Received PONG from Global Controller [{}]. Connection healthy.", globalControllerPath);
                    }
                } catch (Exception ex) {
                    // Transport-only: log; HC owns the failure decision.
                    logger.warn("ActivePingTask (Global) Error (miss recorded; HC decides): {}", ex.getMessage());
                } finally {
                    activePingTimerActive.set(false); // Release lock
                }
            } else {
                logger.trace("ActivePingTask (Global): Skipping ping, controller is not in REGION_GLOBAL state.");
            }
        }
    }
    // --- END NEW TASK ---
}
