package io.cresco.agent.controller.regionalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionHealthWatcher {
    public Timer communicationsHealthTimer;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private long startTS;
    private int wdTimer;
    public Timer regionalUpdateTimer;
    private RegionalExecutor regionalExecutor;
    private AtomicBoolean communicationsHealthTimerActive = new AtomicBoolean();
    private AtomicBoolean regionalUpdateTimerActive = new AtomicBoolean();
    private AtomicBoolean disabled = new AtomicBoolean(false);

    public RegionHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(RegionHealthWatcher.class.getName(),CLogger.Level.Info);
        this.regionalExecutor = new RegionalExecutor(controllerEngine);

        logger.debug("RegionHealthWatcher Initializing");
        this.plugin = plugin;
        wdTimer = 3000;
        startTS = System.currentTimeMillis();
        communicationsHealthTimer = new Timer();
        communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 5000, wdTimer);


        long periodMultiplier = plugin.getConfig().getLongParam("period_multiplier",3l);
        regionalUpdateTimer = new Timer();
        regionalUpdateTimer.scheduleAtFixedRate(new RegionalNodeStatusWatchDog(controllerEngine, logger), 5000 * periodMultiplier, 5000 * periodMultiplier);//remote

        logger.info("Initialized");
    }

    public void shutdown() {

        try {
            communicationsHealthTimer.cancel();
            regionalUpdateTimer.cancel();
            while (regionalUpdateTimerActive.get()) {
                Thread.sleep(1000);
            }
            while(communicationsHealthTimerActive.get()) {
                Thread.sleep(1000);
            }

            logger.debug("Shutdown");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
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


                if ((retMsg != null) && (retMsg.getParams().containsKey("is_rpc"))) {
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

                logger.trace("CommunicationHealthWatcherTask() Update Own Controller");

                MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                //tick.setParam("region_name",controllerEngine.cstate.getRegionalRegion());
                //tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                //tick.setParam("watchdogtimer", String.valueOf(wdTimer));
                tick.setParam("region_watchdog_update",controllerEngine.cstate.getRegion());
                tick.setParam("mode","REGION");
                controllerEngine.getGDB().nodeUpdate(tick);

                if(!communicationsHealthTimerActive.get()) {
                    communicationsHealthTimerActive.set(true);


                    if (controllerEngine.cstate.isRegionalController()) {
                        //disabled to allow discovery to be started after regional init
                        /*
                        if (!controllerEngine.isDiscoveryActive()) {
                            isHealthy = false;
                            logger.info("Discovery shutdown detected");

                        }
                         */
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
                        logger.info("System has become unhealthy, rebooting services");
                        //controllerEngine.cstate.setRegionFailed("Regional CommunicationHealthWatcherTask Unhealthy Region");
                        controllerEngine.getControllerSM().globalControllerLost("RegionHealthWatcher : !isHealthy");
                    }
                    communicationsHealthTimerActive.set(false);
                }

            } catch (Exception ex) {
                if(communicationsHealthTimerActive.get()) {
                    communicationsHealthTimerActive.set(false);
                }
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

            if(!regionalUpdateTimerActive.get()) {
                regionalUpdateTimerActive.set(true);

                if (controllerEngine.cstate.isRegionalController()) { //only run if node is regional controller
                    logger.debug("RegionalNodeStatusWatchDog");

                    Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), null, null);

                    for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {

                        if (!plugin.getAgent().equals(entry.getKey())) {

                            logger.trace("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                            if (entry.getValue() == NodeStatusType.PENDINGSTALE) { //will include more items once nodes update correctly
                                logger.warn("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                                controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 40, "set STALE by regional controller health watcher");

                            } else if (entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                                logger.error("Agent NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString() + " SETTING LOST");
                                controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 50, "agent set LOST by regional controller health watcher I0");
                                setPluginsLost(entry.getKey());
                                //set plugins lost
                            } else if (entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly
                                logger.error("NODE IS IN ERROR");
                            }

                        }
                    }
                }
                regionalUpdateTimerActive.set(false);
            }
        }

        private void setPluginsLost(String agentName) {
            Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), agentName, null);

            for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {
                logger.error("Agent NodeID : " + agentName + " Plugin NodeID : " + entry.getKey() + " SETTING LOST");
                controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), agentName, entry.getKey(), 50, "plugin set LOST by regional controller health watcher I1");
            }
        }

    }

}
