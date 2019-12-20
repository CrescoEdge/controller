package io.cresco.agent.controller.regionalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
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
        regionalUpdateTimer.scheduleAtFixedRate(new RegionHealthWatcher.RegionalNodeStatusWatchDog(controllerEngine, logger), 5000 * periodMultiplier, 5000 * periodMultiplier);//remote
        logger.info("Initialized");


        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof MapMessage) {

                        MapMessage mapMessage = (MapMessage)msg;
                        logger.error("REGIONAL HEALTH MESSAGE: INCOMING");

                        String pluginconfigs = mapMessage.getString("pluginconfigs");
                        logger.error(pluginconfigs);


                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
        //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT'");


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

                if(!communicationsHealthTimerActive.get()) {
                    communicationsHealthTimerActive.set(true);
                    if (!controllerEngine.getActiveClient().isFaultURIActive()) {
                        isHealthy = false;
                        logger.info("Agent Consumer shutdown detected");
                    }

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
                        controllerEngine.cstate.setRegionFailed("Regional CommunicationHealthWatcherTask Unhealthy Region");
                        controllerEngine.setRestartOnShutdown(true);
                        controllerEngine.closeCommunications();
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
            if (controllerEngine.cstate.isRegionalController()) { //only run if node is regional controller
                logger.debug("RegionalNodeStatusWatchDog");

                Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(plugin.getRegion(), null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {

                    if (!plugin.getAgent().equals(entry.getKey())) {

                        logger.trace("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                        if (entry.getValue() == NodeStatusType.PENDINGSTALE) { //will include more items once nodes update correctly
                            logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                            controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 40, "set STALE by regional controller health watcher");

                        } else if (entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                            logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                            controllerEngine.getGDB().setNodeStatusCode(plugin.getRegion(), entry.getKey(), null, 50, "set LOST by regional controller health watcher");

                        } else if (entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly

                        }

                    }
                }
            }
        }
    }

}
