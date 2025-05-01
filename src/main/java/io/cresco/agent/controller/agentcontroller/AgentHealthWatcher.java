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
        this.pingInterval = plugin.getConfig().getLongParam("agent_ping_interval", (long)wdTimer); // Ping as often as watchdog update by default
        this.pingTimeout = plugin.getConfig().getLongParam("agent_ping_timeout", 5000L); // Default 5 second timeout for ping response

        this.startTS = System.currentTimeMillis();
        this.communicationsHealthTimer = new Timer("AgentCommHealthTimer", true); // Daemon thread
        this.communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 5000, wdTimer);

        // --- NEW: Schedule Active Ping Timer ---
        if (!controllerEngine.cstate.isRegionalController()) { // Only ping if this is an agent connected to a region
            this.activePingTimer = new Timer("AgentActivePingTimer", true); // Daemon thread
            this.activePingTimer.scheduleAtFixedRate(new ActivePingTask(), pingInterval, pingInterval); // Start after first interval
            logger.info("Active Ping Timer scheduled with interval: {} ms", pingInterval);
        }
        // --- END NEW ---

        this.gson = new Gson();
        // Removed watchDogTimerString initialization
    }

    public void shutdown() {
        if (communicationsHealthTimer != null) {
            communicationsHealthTimer.cancel();
            communicationsHealthTimer = null; // Help GC
        }
        // --- NEW: Cancel Ping Timer ---
        if (activePingTimer != null) {
            activePingTimer.cancel();
            activePingTimer = null; // Help GC
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
                agentList.add(agentNode);
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
                        pluginList.add(pNode);
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

            // Send the message only if there are config changes or it's a basic watchdog
            if (le.paramsContains("agentconfigs") || le.paramsContains("pluginconfigs") || (le.getParams().size() > 6)) { // Check if more than basic params exist
                logger.trace("Sending watchdog update to regional controller.");
                plugin.msgOut(le);
            } else {
                logger.trace("No changes in agent/plugin configs, skipping watchdog send.");
            }

        } else {
            // If this node *is* a regional/global controller, just update its own DB timestamp
            logger.trace("Updating local watchdog timestamp for regional/global controller.");
            controllerEngine.getGDB().updateWatchDogTS(plugin.getRegion(), plugin.getAgent(), null);
        }
    }

    // Task to check underlying JMS connection via ActiveClient
    class CommunicationHealthWatcherTask extends TimerTask {
        public void run() {
            // Renamed from sendUpdate to sendWatchdogUpdate
            sendWatchdogUpdate();

            try {
                if(controllerEngine.cstate.isActive() && !controllerEngine.cstate.isRegionalController()) { // Only check if active and *not* the region controller itself
                    if (!communicationsHealthTimerActive.compareAndSet(false, true)) {
                        logger.warn("CommunicationHealthWatcherTask already running, skipping cycle.");
                        return; // Exit if already running
                    }
                    try {
                        if (!controllerEngine.getActiveClient().isFaultURIActive()) {
                            logger.error("CommunicationHealthWatcherTask: Connection to Regional Controller appears down (isFaultURIActive() == false). Triggering recovery.");
                            // Trigger state machine failure
                            controllerEngine.getControllerSM().regionalControllerLost("AgentHealthWatcher: CommunicationHealthWatcherTask detected inactive connection.");
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

    // --- NEW TASK for Active Pinging ---
    class ActivePingTask extends TimerTask {
        public void run() {
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

                    // Send RPC and wait for response with timeout
                    MsgEvent pingResponse = plugin.sendRPC(pingRequest, pingTimeout);

                    if (pingResponse == null) {
                        // Timeout or other RPC failure
                        logger.error("ActivePingTask: No PONG received from Regional Controller [{}] within timeout ({}ms). Triggering recovery.",
                                controllerEngine.cstate.getRegionalControllerPath(), pingTimeout);
                        controllerEngine.getControllerSM().regionalControllerLost("AgentHealthWatcher: ActivePingTask timeout.");
                    } else {
                        // We got a response, connection seems okay at application level
                        logger.debug("ActivePingTask: Received PONG from Regional Controller [{}]. Connection healthy.", controllerEngine.cstate.getRegionalControllerPath());
                        // Optional: Log latency = System.currentTimeMillis() - Long.parseLong(pingResponse.getParam("remote_ts"));
                    }
                } catch (Exception ex) {
                    logger.error("ActivePingTask Error: {}", ex.getMessage(), ex);
                    // Assume failure and trigger recovery on exception
                    controllerEngine.getControllerSM().regionalControllerLost("AgentHealthWatcher: ActivePingTask exception: " + ex.getMessage());
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
