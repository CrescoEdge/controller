package io.cresco.agent.controller.agentcontroller;


import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.*;


public class AgentHealthWatcher {

	  public Timer timer;
	  private long startTS;
	  private Map<String,String> wdMap;
	  private boolean isRegistered = false;
	  private boolean isRegistering = false;

	  private String agentExport = "";
	  private String pluginExport = "";

	  private String watchDogTimerString;

	  private ControllerEngine controllerEngine;
	  private PluginBuilder plugin;
	  private CLogger logger;
	  private Gson gson;

	  public AgentHealthWatcher(ControllerEngine controllerEngine) {
	  	this.controllerEngine = controllerEngine;
	  	this.plugin = controllerEngine.getPluginBuilder();
	  	this.logger = plugin.getLogger(AgentHealthWatcher.class.getName(),CLogger.Level.Info);

		  startTS = System.currentTimeMillis();
		  timer = new Timer();

		  gson = new Gson();

		  watchDogTimerString = plugin.getConfig().getStringParam("watchdogtimer","5000");


	      timer.scheduleAtFixedRate(new WatchDogTask(), 500, Long.parseLong(watchDogTimerString));
	      wdMap = new HashMap<>(); //for sending future WD messages

          if((controllerEngine.cstate.isActive()) && (plugin.isActive())) {
              //isRegistered = enable(true);
          }
      }

      public void shutdown(boolean unregister) {
          if(!controllerEngine.cstate.isRegionalController() && unregister) {

              /*
              MsgEvent disableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
              disableMsg.setParam("region_name",plugin.getRegion());
              disableMsg.setParam("agent_name",plugin.getAgent());
              disableMsg.setParam("desc","to-rc-agent");
			  disableMsg.setParam("action", "agent_disable");
              plugin.msgOut(disableMsg);
              */
			  //le.setParam("watchdogtimer", watchDogTimerString);
			  //AgentEngine.msgInQueue.add(le);
              //MsgEvent re = new RPCCall().call(le);
              //System.out.println("RPC DISABLE: " + re.getMsgBody() + " [" + re.getParams().toString() + "]");
          }
          timer.cancel();
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

	class WatchDogTask extends TimerTask {
	    public void run() 
	    {
	    	if(controllerEngine.cstate.isActive())
	    	{
                //sendUpdate();
	    	    /*
	    	    if((!isRegistered) && (controllerEngine.cstate.isActive())) {
	    	        if(!isRegistering) {
	    	            isRegistering = true;
                        //isRegistered = enable(true);
                        isRegistering = false;
                    }
                } else {
	    	        sendUpdate();
                }
                */

	    	}
	    }
      }
}
