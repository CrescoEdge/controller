package io.cresco.agent.controller.agentcontroller;


import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

//job of the agentwatcher is to update region with changes

public class AgentHealthWatcher {

    public Timer communicationsHealthTimer;
    // New Timer for active pinging
    public Timer activePingTimer;


    private long startTS;
    // Removed wdMap as it wasn't used consistently
    // private Map<String,String> wdMap;

    private String agentExport = "";
    private String pluginExport = "";

    // Removed watchDogTimerString as wdTimer is used directly
    // private String watchDogTimerString;

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private int wdTimer; // Watchdog interval for sending updates
    private long pingInterval; // Interval for active ping checks
    private long pingTimeout; // Timeout for waiting for ping response

    // Health->state moved to HC: this watcher is now transport-only. It stamps the last successful
    // pong from the regional (parent) controller; ParentLinkHealthCheck reads it and the HC->MINA
    // bridge fires regionalControllerLost after the grace window (no single-missed-ping drop).
    private volatile long lastParentPongTs;

    private AtomicBoolean communicationsHealthTimerActive = new AtomicBoolean();
    // New AtomicBoolean for ping timer
    private AtomicBoolean activePingTimerActive = new AtomicBoolean();

    public AgentHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AgentHealthWatcher.class.getName(),CLogger.Level.Info);

        // Use watchdog_interval from config for consistency
        this.wdTimer = plugin.getConfig().getIntegerParam("watchdog_interval", 15000);
        // Use separate config params for ping interval and timeout, or derive from wdTimer
        this.pingInterval = plugin.getConfig().getLongParam("agent_ping_interval", 5000L); // ping cadence (also caps link-failure detection latency)
        this.pingTimeout = plugin.getConfig().getLongParam("agent_ping_timeout", 5000L); // Default 5 second timeout for ping response

        this.startTS = System.currentTimeMillis();
        // Initialise to creation time so a freshly-created watcher is not instantly "stale".
        this.lastParentPongTs = System.currentTimeMillis();
        this.communicationsHealthTimer = new Timer("AgentCommHealthTimer", true); // Daemon thread
        this.communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 5000, wdTimer);

        // --- NEW: Schedule Active Ping Timer ---
        if (!controllerEngine.cstate.isRegionalController()) { // Only ping if this is an agent connected to a region
            this.activePingTimer = new Timer("AgentActivePingTimer", true); // Daemon thread
            io.cresco.agent.controller.communication.RevocationSync.onParentLink(controllerEngine, false);   // a new watcher: a new join
            this.activePingTimer.scheduleAtFixedRate(new ActivePingTask(), pingInterval, pingInterval); // Start after first interval
            // the first ping goes at once when this agent is active under its region, not an interval later: the trust
            // bundle, this agent's attestation and the region's revocation list arrive on its pong
            this.activePingTimer.schedule(new ParentLinkTask(), 0L, PARENT_LINK_CHECK_MS);
            logger.info("Active Ping Timer scheduled with interval: {} ms", pingInterval);
        }
        // --- END NEW ---

        this.gson = new Gson();
        // Removed watchDogTimerString initialization
    }

    public long getLastParentPongTs() { return lastParentPongTs; }

    /** What this agent holds of a region controller that refuses its pings ({@link io.cresco.agent.controller.health.ParentPing}). */
    private final io.cresco.agent.controller.health.ParentPing parentPing = new io.cresco.agent.controller.health.ParentPing();

    public io.cresco.agent.controller.health.ParentPing parentPing() { return parentPing; }

    /**
     * The answer to a ping of this agent to its region controller ({@code pingResponse}: null when none came in time),
     * sent at {@code pingT0} (System.nanoTime). A PONG is the parent's word: liveness, the round-trip time, its health, and
     * everything {@link ControllerEngine#onParentPong} takes from it (the parent is heard; trust data, revocations, the
     * direct link, the state resync). Its REFUSAL (401) says only that its broker answers: the link is not declared lost,
     * nothing else advances -- what this agent sends its parent stays signed -- and one WARN line per interval says so.
     * Anything else is a miss. See {@link io.cresco.agent.controller.health.ParentPing}.
     */
    public void pingAnswered(MsgEvent pingResponse, long pingT0) {
        // parent path may be null during the brief AGENT_INIT window before it is populated;
        // show a readable placeholder instead of "[null]".
        String parentPath = controllerEngine.cstate.getRegionalControllerPath();
        String parent = "Regional Controller [" + (parentPath != null ? parentPath : "<pending-init>") + "]";
        long now = System.currentTimeMillis();
        switch (io.cresco.agent.controller.health.ParentPing.answer(pingResponse)) {
            case PONG: {
                // Healthy pong -> stamp liveness for ParentLinkHealthCheck.
                lastParentPongTs = now;
                String again = parentPing.pong(parent, now);
                if (again != null) logger.info(again);
                // measurement: harvest the round-trip time onto the parent link.
                try {
                    io.cresco.agent.controller.netmetrics.LinkMetricsRegistry reg = controllerEngine.getLinkMetricsRegistry();
                    if (reg != null) {
                        reg.forPath(io.cresco.agent.controller.netmetrics.LinkMetricsRegistry.parentLinkKey(controllerEngine))
                                .recordRtt((System.nanoTime() - pingT0) / 1_000_000.0);
                    }
                } catch (Exception ignore) { }
                // mesh health: record the region's advertised health carried back on the pong.
                io.cresco.agent.controller.health.MeshHealthPing.recordParent(controllerEngine, pingResponse);
                // the region holds this agent as not ACTIVE (e.g. LOST while we kept our session): send it
                // our full state now rather than on the next periodic full export
                controllerEngine.onParentPong(pingResponse);
                logger.debug("ActivePingTask: Received PONG from Regional Controller [{}]. Connection healthy.", controllerEngine.cstate.getRegionalControllerPath());
                break;
            }
            case REFUSED: {
                // the parent's broker answers: liveness only (link:parent reports WARN, not OK, and does not tear the link down)
                lastParentPongTs = now;
                // nothing of the answer is the parent's word (it is this agent's own ping with a status): the next ping
                // goes signed and offers no direct link, as after a ping that was not answered
                controllerEngine.msgSigning().parentPingUnanswered();
                String line = parentPing.refused(parent, pingResponse.getParam("status_desc"), now);
                if (line != null) logger.warn(line);
                break;
            }
            default:
                // Transport-only: record the miss. The failure DECISION now lives in HC —
                // ParentLinkHealthCheck sees the stale lastParentPongTs and the HC->MINA
                // bridge fires regionalControllerLost after the grace window. No direct state
                // transition here (that direct call was the single-missed-ping drop bug).
                // an unanswered ping: the region may have restarted and not bound this link yet, so the next goes signed
                controllerEngine.msgSigning().parentPingUnanswered();
                logger.warn("ActivePingTask: No PONG from {} within {}ms (miss recorded; HC decides).", parent, pingTimeout);
        }
    }

    public long getPingIntervalMs() { return pingInterval; }

    public void shutdown() {
        if (communicationsHealthTimer != null) {
            communicationsHealthTimer.cancel();
            communicationsHealthTimer = null; // Help GC
        }
        // --- NEW: Cancel Ping Timer ---
        if (activePingTimer != null) {
            activePingTimer.cancel();
            activePingTimer = null; // Help GC
            // this watcher goes with the agent's link to its region: the next join starts anew
            io.cresco.agent.controller.communication.RevocationSync.onParentLink(controllerEngine, false);
        }
        // --- END NEW ---
        logger.info("Shutdown AgentWatcher");
    }

    // Renamed from sendUpdate to sendWatchdogUpdate for clarity
    public void sendWatchdogUpdate() {

        // Only send updates if connected to a region (not standalone, not a region/global itself)
        if(!controllerEngine.cstate.isRegionalController()) {
            long runTime = System.currentTimeMillis() - startTS;
            // Removed wdMap logic

            MsgEvent le = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
            if (le == null) {
                logger.error("sendWatchdogUpdate: Failed to create regional controller message event!");
                return;
            }

            le.setParam("desc", "agent-watchdog-update"); // More specific description
            le.setParam("agent_watchdog_update", plugin.getAgent());
            le.setParam("agent_runtime", String.valueOf(runTime));
            le.setParam("agent_timestamp", String.valueOf(System.currentTimeMillis()));


            Map<String,List<Map<String,String>>> agentMap = new HashMap<>();
            List<Map<String,String>> agentList = new ArrayList<>();
            Map<String, String> agentNode = controllerEngine.getGDB().getANode(controllerEngine.cstate.getAgent());
            if (agentNode != null) {
                agentList.add(io.cresco.agent.db.ConfigRedaction.redactNode(agentNode));
                agentMap.put(controllerEngine.cstate.getRegion(), agentList);

                String tmpAgentExport = gson.toJson(agentMap);
                // Only send if changed
                if (!agentExport.equals(tmpAgentExport)) {
                    agentExport = tmpAgentExport;
                    le.setCompressedParam("agentconfigs", agentExport);
                    logger.trace("Sending updated agentconfigs");
                }
            } else {
                logger.warn("sendWatchdogUpdate: Could not retrieve agent node info for {}", controllerEngine.cstate.getAgent());
            }


            Map<String,List<Map<String,String>>> pluginMap = new HashMap<>();
            List<Map<String,String>> pluginList = new ArrayList<>();
            List<String> tmpPluginList = controllerEngine.getGDB().getNodeList(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent());
            if (tmpPluginList != null) {
                for (String pluginId : tmpPluginList) {
                    Map<String, String> pNode = controllerEngine.getGDB().getPNode(pluginId);
                    if (pNode != null) {
                        // secrets stay on this agent: the export goes upstream (ConfigRedaction)
                        pluginList.add(io.cresco.agent.db.ConfigRedaction.redactNode(pNode));
                    }
                }
                pluginMap.put(controllerEngine.cstate.getAgent(), pluginList);

                String tmpPluginExport = gson.toJson(pluginMap);
                // Only send if changed
                if (!pluginExport.equals(tmpPluginExport)) {
                    pluginExport = tmpPluginExport;
                    le.setCompressedParam("pluginconfigs", pluginExport);
                    logger.trace("Sending updated pluginconfigs");
                }
            } else {
                logger.warn("sendWatchdogUpdate: Could not retrieve plugin list for agent {}", controllerEngine.cstate.getAgent());
            }

            // ALWAYS send: this WATCHDOG is the agent's control-plane keep-alive (it carries
            // agent_watchdog_update, refreshing watchdog_ts on the regional controller). The old
            // params.size()>6 guard suppressed it whenever configs were unchanged, leaving the
            // 15s dataplane stateUpdate as the ONLY thing keeping this agent from going STALE.
            // Config exports above are still diff-gated, so the steady-state message stays small.
            logger.trace("Sending watchdog update to regional controller.");
            plugin.msgOut(le);

        } else {
            // If this node *is* a regional/global controller, just update its own DB timestamp
            logger.trace("Updating local watchdog timestamp for regional/global controller.");
            controllerEngine.getGDB().updateWatchDogTS(plugin.getRegion(), plugin.getAgent(), null);
        }
    }

    // Task to check underlying JMS connection via ActiveClient
    class CommunicationHealthWatcherTask extends io.cresco.agent.core.GuardedTimerTask {
        CommunicationHealthWatcherTask() { super(logger); }

        protected void tick() {
            // Renamed from sendUpdate to sendWatchdogUpdate
            sendWatchdogUpdate();

            try {
                if(controllerEngine.cstate.isActive() && !controllerEngine.cstate.isRegionalController()) { // Only check if active and *not* the region controller itself
                    if (!communicationsHealthTimerActive.compareAndSet(false, true)) {
                        logger.warn("CommunicationHealthWatcherTask already running, skipping cycle.");
                        return; // Exit if already running
                    }
                    try {
                        // Health of this connection is now surfaced by the "dataplane" local check and
                        // the "link:parent" check; this task no longer fires state transitions.
                        if (!controllerEngine.getActiveClient().isFaultURIActive()) {
                            logger.warn("CommunicationHealthWatcherTask: fault URI inactive (HC decides recovery).");
                        } else {
                            logger.trace("CommunicationHealthWatcherTask: Connection to Regional Controller appears active.");
                        }
                    } finally {
                        communicationsHealthTimerActive.set(false); // Release lock
                    }
                }
            } catch (Exception ex) {
                if(communicationsHealthTimerActive.get()) {
                    communicationsHealthTimerActive.set(false); // Ensure lock release on exception
                }
                logger.error("CommunicationHealthWatcherTask Error: {}", ex.getMessage(), ex);
            }
        }
    }

    /** How often the ping timer looks whether this agent just became active under its region (a field read). */
    static final long PARENT_LINK_CHECK_MS = 250L;

    /** Sees this agent join its region (active) and pings at once then; tells RevocationSync whether the link is up. */
    class ParentLinkTask extends io.cresco.agent.core.GuardedTimerTask {
        ParentLinkTask() { super(logger); }

        protected void tick() {
            boolean up = controllerEngine.cstate.isActive() && !controllerEngine.cstate.isRegionalController();
            io.cresco.agent.controller.communication.MsgSigning signing = controllerEngine.msgSigning();
            boolean joined = io.cresco.agent.controller.communication.RevocationSync.onParentLink(controllerEngine, up);
            // while the link is down, and for the first message of a join, nothing goes to the region unsigned: its pong
            // offers the direct link anew
            if (!up || joined) signing.parentLinkReset();
            // a join, or a message this agent holds because it cannot judge it yet: ping now, not at the next interval
            if (joined || (up && signing.takeTrustWanted(System.currentTimeMillis()))) new ActivePingTask().run();
        }
    }

    // --- NEW TASK for Active Pinging ---
    class ActivePingTask extends io.cresco.agent.core.GuardedTimerTask {
        ActivePingTask() { super(logger); }

        protected void tick() {
            // Only run if connected to a region and not the controller itself
            if (controllerEngine.cstate.isActive() && !controllerEngine.cstate.isRegionalController()) {
                if (!activePingTimerActive.compareAndSet(false, true)) {
                    logger.warn("ActivePingTask already running, skipping cycle.");
                    return; // Exit if already running
                }
                logger.debug("ActivePingTask: Sending PING to Regional Controller [{}]", controllerEngine.cstate.getRegionalControllerPath());
                try {
                    MsgEvent pingRequest = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.EXEC);
                    if (pingRequest == null) {
                        logger.error("ActivePingTask: Failed to create regional controller message event!");
                        activePingTimerActive.set(false);
                        return;
                    }
                    pingRequest.setParam("action", "ping");
                    pingRequest.setParam("desc", "agent-ping-request"); // Identify the ping
                    // mesh health: advertise our rolled-up health up to the region on the ping.
                    io.cresco.agent.controller.health.MeshHealthPing.advertiseChild(controllerEngine, pingRequest);
                    // the id of the crypto policy we hold: the pong carries the policy when the region's differs
                    io.cresco.agent.controller.statemachine.CryptoPolicySync.advertise(pingRequest);
                    // revocations: what we hold of the region's list (the pong brings it when it differs)
                    io.cresco.agent.controller.communication.RevocationSync.onChildPing(controllerEngine, pingRequest);
                    // message signatures: our certificate up, our attestation and the trust bundle down (D4)
                    controllerEngine.msgSigning().onChildPing(pingRequest);

                    // Send RPC and wait for response with timeout. This synchronous agent->region->agent
                    // round trip is a free, continuous RTT probe on the parent link (measurement subsystem);
                    // time it and feed LinkMetrics. It is end-to-end APPLICATION path latency (incl. broker
                    // enqueue/dequeue + queueing), which is what a cost-aware router actually wants.
                    long pingT0 = System.nanoTime();
                    MsgEvent pingResponse = plugin.sendRPC(pingRequest, pingTimeout);

                    // a pong, the parent's refusal, or a miss: each consumer of "a pong came" gets what is its own
                    pingAnswered(pingResponse, pingT0);
                } catch (Exception ex) {
                    // Transport-only: log; HC (ParentLinkHealthCheck + bridge) owns the failure decision.
                    logger.warn("ActivePingTask Error (miss recorded; HC decides): {}", ex.getMessage());
                } finally {
                    activePingTimerActive.set(false); // Release lock
                }
            } else {
                logger.trace("ActivePingTask: Skipping ping, controller is not in active AGENT state.");
            }
        }
    }
    // --- END NEW TASK ---
}
