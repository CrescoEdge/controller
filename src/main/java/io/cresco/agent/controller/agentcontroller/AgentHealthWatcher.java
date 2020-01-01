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

     private long startTS;
     private Map<String,String> wdMap;

     private String agentExport = "";
     private String pluginExport = "";

     private String watchDogTimerString;

     private ControllerEngine controllerEngine;
     private PluginBuilder plugin;
     private CLogger logger;
     private Gson gson;
     private int wdTimer;
     private AtomicBoolean communicationsHealthTimerActive = new AtomicBoolean();

    public AgentHealthWatcher(ControllerEngine controllerEngine) {
	  	this.controllerEngine = controllerEngine;
	  	this.plugin = controllerEngine.getPluginBuilder();
	  	this.logger = plugin.getLogger(AgentHealthWatcher.class.getName(),CLogger.Level.Info);

	  	this.wdTimer = 3000;
	  	this.startTS = System.currentTimeMillis();
	  	this.communicationsHealthTimer = new Timer();
	  	this.communicationsHealthTimer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 5000, wdTimer);

	  	this.gson = new Gson();
	  	this.watchDogTimerString = plugin.getConfig().getStringParam("watchdogtimer","5000");

      }

    public void shutdown() {
        communicationsHealthTimer.cancel();
        logger.info("Shutdown AgentWatcher");
    }

    public void sendUpdate() {

          if((!controllerEngine.cstate.isRegionalController()) || (!controllerEngine.cstate.isGlobalController()) ) {
              long runTime = System.currentTimeMillis() - startTS;
              wdMap.put("runtime", String.valueOf(runTime));
              wdMap.put("timestamp", String.valueOf(System.currentTimeMillis()));

              MsgEvent le = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.WATCHDOG);

              le.setParam("desc", "to-rc-agent");
              //le.setParam("region_name", plugin.getRegion());
              le.setParam("agent_watchdog_update", plugin.getAgent());


              Map<String,List<Map<String,String>>> agentMap = new HashMap<>();
              List<Map<String,String>> agentList = new ArrayList<>();
              agentList.add(controllerEngine.getGDB().getANode(controllerEngine.cstate.getAgent()));
              agentMap.put(controllerEngine.cstate.getRegion(),agentList);

              String tmpAgentExport = gson.toJson(agentMap);
              if (!agentExport.equals(tmpAgentExport)) {

                  agentExport = tmpAgentExport;
                  le.setCompressedParam("agentconfigs", agentExport);
              }

              Map<String,List<Map<String,String>>> pluginMap = new HashMap<>();
              List<Map<String,String>> pluginList = new ArrayList<>();
              List<String> tmpPluginList = controllerEngine.getGDB().getNodeList(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent());
              for(String pluginId : tmpPluginList) {
                  pluginList.add(controllerEngine.getGDB().getPNode(pluginId));
              }
              pluginMap.put(controllerEngine.cstate.getAgent(),pluginList);

              String tmpPluginExport = gson.toJson(pluginMap);
              if(!pluginExport.equals(tmpPluginExport)) {
                  pluginExport = tmpPluginExport;
                  le.setCompressedParam("pluginconfigs", pluginExport);
              }

              plugin.msgOut(le);

          } else {
              //need to update timer in DB
          }
      }

    class CommunicationHealthWatcherTask extends TimerTask {
        public void run() {
            try {

                if(controllerEngine.cstate.isActive()) {
                    if (!communicationsHealthTimerActive.get()) {
                        communicationsHealthTimerActive.set(true);
                        if (!controllerEngine.getActiveClient().isFaultURIActive()) {
                            logger.info("Agent Consumer shutdown detected");
                            controllerEngine.getControllerSM().regionalControllerLost("AgentHealthWatcher: Agent Consumer shutdown detected");
                        }
                        communicationsHealthTimerActive.set(false);
                    }
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

}
