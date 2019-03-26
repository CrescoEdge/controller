package io.cresco.agent.controller.regionalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class RegionHealthWatcher {
    public Timer communicationsHealthTimer;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private long startTS;
    private int wdTimer;
    public Timer regionalUpdateTimer;
    private RegionalExecutor regionalExecutor;


    public RegionHealthWatcher(ControllerEngine controllerEngine) {
        //this.logger = new CLogger(RegionHealthWatcher.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(RegionHealthWatcher.class.getName(),CLogger.Level.Info);

        this.regionalExecutor = new RegionalExecutor(controllerEngine);

        //rce = new RegionalCommandExec(controllerEngine);
        
        logger.debug("Initializing");
        this.plugin = plugin;
        wdTimer = 1000;
        startTS = System.currentTimeMillis();
        communicationsHealthTimer = new Timer();
        communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 1000, wdTimer);
        regionalUpdateTimer = new Timer();
        regionalUpdateTimer.scheduleAtFixedRate(new RegionHealthWatcher.RegionalNodeStatusWatchDog(controllerEngine, logger), 15000, 15000);//remote
        logger.info("Initialized");

    }

    public void shutdown() {
        communicationsHealthTimer.cancel();
        regionalUpdateTimer.cancel();
        logger.debug("Shutdown");
    }

    public void sendRegionalMsg(MsgEvent incoming) {

        try {

            if (incoming.isGlobal()) {
                if(controllerEngine.cstate.isGlobalController()) {
                    regionalExecutor.sendGlobalMsg(incoming);
                } else {
                    regionalExecutor.remoteGlobalSend(incoming);
                }
            } else {

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
                        logger.error("UNKNOWN MESSAGE TYPE! " + incoming.getParams());
                        break;
                }


                if ((retMsg != null) && (retMsg.getParams().keySet().contains("is_rpc"))) {
                    retMsg.setReturn();

                    //pick up RPC from local agent
                    String callId = retMsg.getParam(("callId-" + plugin.getRegion() + "-" +
                            plugin.getAgent() + "-" + plugin.getPluginID()));
                    if (callId != null) {
                        plugin.receiveRPC(callId, retMsg);
                    } else {
                        plugin.msgOut(retMsg);
                    }

                }
            }
        }

        } catch(Exception ex) {
            logger.error("senRegionalMsg Error : " + ex.getMessage());
        }

    }

    class CommunicationHealthWatcherTask extends TimerTask {
        public void run() {
            boolean isHealthy = true;
            try {
                if (!controllerEngine.getActiveClient().isFaultURIActive()) {
                    isHealthy = false;
                    logger.info("Agent Consumer shutdown detected");
                }

                if (controllerEngine.cstate.isRegionalController()) {
                    if (!controllerEngine.isDiscoveryActive()) {
                        isHealthy = false;
                        logger.info("Discovery shutdown detected");

                    }
                    if (!(controllerEngine.isActiveBrokerManagerActive()) || !(controllerEngine.getActiveBrokerManagerThread().isAlive())) {
                        isHealthy = false;
                        logger.info("Active Broker Manager shutdown detected");
                    }
                    if (!controllerEngine.getBroker().isHealthy()) {
                        isHealthy = false;
                        logger.info("Broker shutdown detected");
                    }
                }

                if (!isHealthy) {
                    controllerEngine.cstate.setRegionFailed("Regional CommunicationHealthWatcherTask Unhealthy Region");
                    controllerEngine.removeGDBNode(plugin.getRegion(), plugin.getAgent(), null); //remove self from DB
                    logger.info("System has become unhealthy, rebooting services");
                    controllerEngine.setRestartOnShutdown(true);
                    controllerEngine.closeCommunications();
                }
            } catch (Exception ex) {
                logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

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
            if(controllerEngine.cstate.isRegionalController()) { //only run if node is regional controller
                logger.debug("RegionalNodeStatusWatchDog");

                Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {
                    logger.debug("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                    if(entry.getValue() == NodeStatusType.PENDINGSTALE) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        //mark node disabled
                        //controllerEngine.getGDB().setEdgeParam(entry.getKey(),"is_active",Boolean.FALSE.toString());
                        controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(),entry.getKey(),null,40,"set STALE by regional controller health watcher");

                    }
                    else if(entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        logger.error("Removing " + plugin.getRegion() + " " + entry.getKey());
                        controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(),entry.getKey(),null,50,"set LOST by regional controller health watcher");
                        //controllerEngine.getGDB().removeNode(region,agent,pluginId);
                    }
                    else if(entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly
                        /*
                        Map<String,String> nodeParams = controllerEngine.getGDB().getNodeParams(entry.getKey());
                        for (Map.Entry<String, String> entry2 : nodeParams.entrySet()) {
                            logger.error("Key = " + entry2.getKey() + ", Value = " + entry2.getValue());
                        }
                        String region = nodeParams.get("region");
                        String agent = nodeParams.get("agent");
                        String pluginId = nodeParams.get("agentcontroller");
                        logger.error("Problem with " + region + " " + agent + " " + pluginId);
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        */
                    } /* else {
                        logger.info("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        Map<String,String> nodeMap = controllerEngine.getGDB().gdb.getNodeParams(entry.getKey());
                        logger.info("Region : " + nodeMap.get("region_name") + " Agent : " + nodeMap.get("agent_name"));
                    } */

                }
            }
        }
    }

}
