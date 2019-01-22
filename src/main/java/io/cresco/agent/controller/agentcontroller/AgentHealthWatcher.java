package io.cresco.agent.controller.agentcontroller;


import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.net.InetAddress;
import java.util.*;


public class AgentHealthWatcher {

	  public Timer timer;
	  private long startTS;
	  private Map<String,String> wdMap;
	  private boolean isRegistered = false;
	  private boolean isRegistering = false;

	  private String jsonExport;
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
              isRegistered = enable(true);
          }
      }

      public boolean enable(boolean register) {
	      boolean isRegistered = false;

	      try {

	          while(!plugin.isActive()) {
	              Thread.sleep(1000);
              }

              MsgEvent enableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
              enableMsg.setParam("action", "agent_enable");
              enableMsg.setParam("watchdogtimer",watchDogTimerString);
              enableMsg.setParam("req-seq", UUID.randomUUID().toString());
              enableMsg.setParam("region_name",plugin.getRegion());
              enableMsg.setParam("agent_name",plugin.getAgent());
              enableMsg.setParam("desc","to-rc-agent");

              jsonExport = controllerEngine.getPluginAdmin().getPluginExport();
              enableMsg.setCompressedParam("pluginconfigs",jsonExport);


              Map<String,String> configParams = new HashMap<>();


              String platform = System.getenv("CRESCO_PLATFORM");
              if(platform == null) {
                  platform = plugin.getConfig().getStringParam("general", "platform");
                  if(platform == null) {
                      platform = "unknown";
                  }
              }

              configParams.put("platform", platform);
              //enableMsg.setParam("platform", platform);

              String environment = System.getenv("CRESCO_ENVIRONMENT");
              if(environment == null) {
                  environment = plugin.getConfig().getStringParam("general", "environment");
                  if(environment == null) {
                      try {
                          environment = System.getProperty("os.name");
                      } catch (Exception ex) {
                          environment = "unknown";
                      }
                  }
              }
              //enableMsg.setParam("environment", environment);
              configParams.put("environment", environment);

              //String location = System.getenv("CRESCO_LOCATION");
              String location = plugin.getConfig().getStringParam("location");
              if(location == null) {

                      try {
                          location = InetAddress.getLocalHost().getHostName();
                          if(location != null) {
                              logger.info("Location set: " + location);
                          }
                      } catch(Exception ex) {
                          logger.error("getLocalHost() Failed : " + ex.getMessage());
                      }

                if(location == null) {
                  try {

                      String osType = System.getProperty("os.name").toLowerCase();
                      if(osType.equals("windows")) {
                          location = System.getenv("COMPUTERNAME");
                      } else if(osType.equals("linux")) {
                          location = System.getenv("HOSTNAME");
                      }

                      if(location != null) {
                          logger.info("Location set env: " + location);
                      }

                  } catch(Exception exx) {
                      //do nothing
                      logger.error("Get System Env Failed : " + exx.getMessage());
                  }
                      }
              }
              if(location == null) {
                  location = "unknown";
              }
              //enableMsg.setParam("location", location);
              configParams.put("location", location);

              enableMsg.setParam("configparams",gson.toJson(configParams));

              MsgEvent re = plugin.sendRPC(enableMsg);
              if(re != null) {
                  logger.info("AgentHealthWatcher Started.");
                  isRegistered = true;
              } else {
                  logger.error("Could not confirm AgentHealthWatcher Reg!");
              }

          } catch(Exception ex) {
	          logger.error("Enable Failed !" + ex.getMessage());
          }

	      return isRegistered;
      }

      public void shutdown(boolean unregister) {
          if(!controllerEngine.cstate.isRegionalController() && unregister) {

              MsgEvent disableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
              disableMsg.setParam("region_name",plugin.getRegion());
              disableMsg.setParam("agent_name",plugin.getAgent());
              disableMsg.setParam("desc","to-rc-agent");
			  disableMsg.setParam("action", "agent_disable");
              plugin.msgOut(disableMsg);
			  //le.setParam("watchdogtimer", watchDogTimerString);
			  //AgentEngine.msgInQueue.add(le);
              //MsgEvent re = new RPCCall().call(le);
              //System.out.println("RPC DISABLE: " + re.getMsgBody() + " [" + re.getParams().toString() + "]");
          }
          timer.cancel();
      }

      public void sendUpdate() {
          long runTime = System.currentTimeMillis() - startTS;
          wdMap.put("runtime", String.valueOf(runTime));
          wdMap.put("timestamp", String.valueOf(System.currentTimeMillis()));

          MsgEvent le = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.WATCHDOG);

          le.setParam("desc","to-rc-agent");
          le.setParam("region_name",plugin.getRegion());
          le.setParam("agent_name",plugin.getAgent());


          String tmpJsonExport = controllerEngine.getPluginAdmin().getPluginExport();
          if(!jsonExport.equals(tmpJsonExport)) {

              jsonExport = tmpJsonExport;
              le.setCompressedParam("pluginconfigs", jsonExport);
          }

          plugin.msgOut(le);
      }

	class WatchDogTask extends TimerTask 
	{
	    public void run() 
	    {
	    	if(controllerEngine.cstate.isActive())
	    	{
	    	    if((!isRegistered) && (controllerEngine.cstate.isActive())) {
	    	        if(!isRegistering) {
	    	            isRegistering = true;
                        isRegistered = enable(true);
                        isRegistering = false;
                    }
                } else {
	    	        sendUpdate();
                }

	    	}
	    }
	  }
}
