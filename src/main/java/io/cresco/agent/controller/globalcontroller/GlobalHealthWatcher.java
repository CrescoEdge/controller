package io.cresco.agent.controller.globalcontroller;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class GlobalHealthWatcher {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    public Timer globalUpdateTimer;

    public GlobalHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(GlobalHealthWatcher.class.getName(),CLogger.Level.Info);

        logger.debug("GlobalHealthWatcher Initializing");

        long watchDogIntervalDelay = plugin.getConfig().getLongParam("watchdog_interval_delay",5000L);
        long watchDogInterval = plugin.getConfig().getLongParam("watchdog_interval",15000L);
        long periodMultiplier = plugin.getConfig().getLongParam("period_multiplier",3L);

        globalUpdateTimer = new Timer();
        globalUpdateTimer.scheduleAtFixedRate(new GlobalNodeStatusWatchDog(controllerEngine, logger),  periodMultiplier * watchDogIntervalDelay, periodMultiplier * watchDogInterval);//remote

        logger.info("Initialized");

    }

    public void shutdown() {
        globalUpdateTimer.cancel();
        logger.debug("Shutdown");
    }



    static class GlobalNodeStatusWatchDog extends TimerTask {
        private ControllerEngine controllerEngine;
        private CLogger logger;
        private PluginBuilder plugin;

        public GlobalNodeStatusWatchDog(ControllerEngine controllerEngine, CLogger logger) {

            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();

            this.logger = logger;
        }

        public void run() {
            if (controllerEngine.cstate.isGlobalController()) { //only run if node is regional controller
                logger.debug("GlobalNodeStatusWatchDog");

                Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(null, null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {

                    if (!plugin.getAgent().equals(entry.getKey())) {

                        logger.trace("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                        if (entry.getValue() == NodeStatusType.PENDINGSTALE) { //will include more items once nodes update correctly
                            logger.warn("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                            controllerEngine.getGDB().setNodeStatusCode(entry.getKey(), null, null, 40, "plugin set STALE by global controller health watcher");

                        } else if (entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                            logger.error("Region NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString() + " SETTING LOST");
                            controllerEngine.getGDB().setNodeStatusCode(entry.getKey(), null, null, 50, "plugin set LOST by global controller health watcher I0");
                            setAgentLost(entry.getKey());
                            //set plugins lost
                        } else if (entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly
                            logger.error("NODE IS IN ERROR");
                        }

                    }
                }
            }
        }

        private void setAgentLost(String regionName) {
            Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(regionName, null, null);

            for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {
                logger.error("Agent NodeID : " + regionName + " Agent NodeID : " + entry.getKey() + " SETTING LOST");
                controllerEngine.getGDB().setNodeStatusCode(regionName, null, null, 50, "agent set LOST by global controller health watcher I1");
                setPluginsLost(regionName, entry.getKey());
            }
        }

        private void setPluginsLost(String regionName, String agentName) {
            Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(regionName, agentName, null);

            for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {
                logger.error("Plugin NodeID : " + agentName + " Plugin NodeID : " + entry.getKey() + " SETTING LOST");
                controllerEngine.getGDB().setNodeStatusCode(regionName, agentName, entry.getKey(), 50, "plugin set LOST by global controller health watcher I2");
            }
        }

    }

}
