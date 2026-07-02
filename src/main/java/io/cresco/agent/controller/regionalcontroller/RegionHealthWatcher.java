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


    public RegionHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(RegionHealthWatcher.class.getName(),CLogger.Level.Info);
        this.regionalExecutor = new RegionalExecutor(controllerEngine);

        long watchDogIntervalDelay = plugin.getConfig().getLongParam("watchdog_interval_delay",5000L);
        long commWatchDogInterval = plugin.getConfig().getLongParam("comm_watchdog_interval",5000L); // Interval for checking local components
        long watchDogInterval = plugin.getConfig().getLongParam("watchdog_interval",15000L); // Interval for checking agent statuses in DB
        long periodMultiplier = plugin.getConfig().getLongParam("period_multiplier",3L);

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
                    // Discover the peer first to get its proper agent path
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
                    List<DiscoveryNode> discovered = ds.discover(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("peer_discovery_timeout",5000), peerAddress, true); // 5 sec timeout

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

                // Do something with peers
                controllerEngine.getRegionHealthWatcher().maintainPeerConnections();

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
