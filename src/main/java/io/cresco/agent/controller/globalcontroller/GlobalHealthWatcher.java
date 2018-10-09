package io.cresco.agent.controller.globalcontroller;


import io.cresco.agent.controller.app.gPayload;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.db.NodeStatusType;
import io.cresco.agent.controller.globalscheduler.AppSchedulerEngine;
import io.cresco.agent.controller.globalscheduler.ResourceSchedulerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryClientIPv4;
import io.cresco.agent.controller.netdiscovery.DiscoveryClientIPv6;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netdiscovery.TCPDiscoveryStatic;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class GlobalHealthWatcher implements Runnable {
    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private Map<String,String> global_host_map;
    private Timer regionalUpdateTimer;
    private Long gCheckInterval;
    private String lastDBUpdate;

    public Boolean SchedulerActive;
    public Boolean AppSchedulerActive;

    public GlobalHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(GlobalHealthWatcher.class.getName(),CLogger.Level.Info);


        //this.logger = new CLogger(GlobalHealthWatcher.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
		//this.agentcontroller = agentcontroller;
        global_host_map = new HashMap<>();
        regionalUpdateTimer = new Timer();
        regionalUpdateTimer.scheduleAtFixedRate(new GlobalHealthWatcher.GlobalNodeStatusWatchDog(controllerEngine, logger), 500, 15000);//remote
        gCheckInterval = plugin.getConfig().getLongParam("watchdogtimer",5000L);
        SchedulerActive = false;
        AppSchedulerActive = false;
        controllerEngine.setResourceScheduleQueue(new LinkedBlockingQueue<MsgEvent>());
    }

	public void shutdown() {

        regionalUpdateTimer.cancel();

        SchedulerActive = false;
        AppSchedulerActive = false;


        if(!controllerEngine.cstate.isGlobalController()) {
            if (controllerEngine.isReachableAgent(controllerEngine.cstate.getGlobalControllerPath())) {
                MsgEvent le = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                /*
                MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, controllerEngine.cstate.getGlobalRegion(), controllerEngine.cstate.getGlobalAgent(), null, "disabled");

                le.setParam("src_region", plugin.getRegion());
                le.setParam("src_agent", plugin.getAgent());
                //le.setParam("src_plugin", controllerEngine.cstate.getControllerId());

                le.setParam("dst_region", controllerEngine.cstate.getGlobalRegion());
                le.setParam("dst_agent", controllerEngine.cstate.getGlobalAgent());
                //le.setParam("dst_plugin", controllerEngine.cstate.getControllerId());

                le.setParam("is_regional", Boolean.TRUE.toString());
                le.setParam("is_global", Boolean.TRUE.toString());
                */


                le.setParam("region_name", controllerEngine.cstate.getRegionalRegion());


                le.setParam("action", "region_disable");

                //TODO does this need to be added?
                //le.setParam("globalcmd", Boolean.TRUE.toString());

                le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                //MsgEvent re = new RPCCall().call(le);
                //MsgEvent re = agentcontroller.getRPC().call(le);
                //logger.info("RPC DISABLE: " + re.getMsgBody() + " [" + re.getParams().toString() + "]");
                plugin.msgOut(le);
            }
        }
	}

	public void run() {
		try {

            while(!controllerEngine.hasActiveProducter()) {
                logger.trace("GlobalHealthWatcher waiting on Active Producer.");
                Thread.sleep(2500);
            }

            logger.trace("Initial gCheck");

                gCheck(); //do initial check


                controllerEngine.setGlobalControllerManagerActive(true);

                while (controllerEngine.isGlobalControllerManagerActive()) {
                    Thread.sleep(gCheckInterval);
                    gCheck();
                    gNotify();
                }
                logger.info("Starting Global Shutdown");
                shutdown();

		} catch(Exception ex) {
			logger.error("globalwatcher run() " + ex.getMessage());
            logger.error(ex.getStackTrace().toString());
		}
	}

	private void gNotify() {
        logger.trace("gNotify Start");
        try {
            //if there is a remote global controller and it is reachable
            if(controllerEngine.cstate.isRegionalController()) {
                logger.debug("gNotify !Global Controller Message");
                //is the global controller reachable
                if(controllerEngine.isReachableAgent(controllerEngine.cstate.getGlobalControllerPath())) {

                    MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                    /*
                    MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick. 0");
                    tick.setParam("src_region", this.plugin.getRegion());
                    tick.setParam("src_agent", this.plugin.getAgent());

                    tick.setParam("dst_region",controllerEngine.cstate.getGlobalRegion());
                    tick.setParam("dst_agent",controllerEngine.cstate.getGlobalAgent());
                    */

                    tick.setParam("region_name",controllerEngine.cstate.getRegionalRegion());


                    tick.setParam("is_regional", Boolean.TRUE.toString());
                    tick.setParam("is_global", Boolean.TRUE.toString());

                    tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                    tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));

                    logger.trace("gNotify !Global Controller Message : " + tick.getParams().toString());

                    plugin.msgIn(tick);


                }
            }
            //TODO This might be required

            else if(controllerEngine.cstate.isGlobalController()) {
                //controllerEngine.getGDB().watchDogUpdate()
                logger.trace("gNotify Global Controller Message");

                MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                /*
                MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick. 1");
                tick.setParam("src_region", this.plugin.getRegion());
                tick.setParam("src_agent", this.plugin.getAgent());

                tick.setParam("dst_region",controllerEngine.cstate.getGlobalRegion());
                tick.setParam("dst_agent",controllerEngine.cstate.getGlobalAgent());

                tick.setParam("is_regional", Boolean.TRUE.toString());
                tick.setParam("is_global", Boolean.TRUE.toString());
                */

                tick.setParam("region_name",controllerEngine.cstate.getRegionalRegion());


                tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));
                //logger.error("CALLING FROM GLOBAL HEALTH: " + tick.getParams().toString());

                logger.trace("gNotify Global Controller Message : " + tick.getParams().toString());

                controllerEngine.getGDB().watchDogUpdate(tick);
            }

            else {
                logger.debug("gNotify : Why and I here!");
            }

        }
        catch(Exception ex) {
            logger.error("gNotify() " + ex.getMessage());
        }
        logger.trace("gNotify End");

    }

    private void gCheck() {
        logger.trace("gCheck Start");

        try{
            /*
            logger.trace("GlobalControllerManager is Active");
                controllerEngine.cstate.setGlobalSuccess("GlobalControllerManager is Active");
                logger.info("CSTATE : " + controllerEngine.cstate.getControllerState() + " Region:" + controllerEngine.cstate.getRegion() + " Agent:" + controllerEngine.cstate.getAgent());

             */

            //Static Remote Global Controller
            String global_controller_host = plugin.getConfig().getStringParam("global_controller_host",null);
            if(global_controller_host != null) {
                logger.trace("Starting Static Global Controller Check on " + global_controller_host);
                if(global_host_map.containsKey(global_controller_host)) {
                    if(controllerEngine.isReachableAgent(global_host_map.get(global_controller_host))) {
                        logger.trace("Static Global Controller Check " + global_controller_host + " Ok.");
                        //TODO Check if regionalDBexport() is needed
                        regionalDBexport();
                        return;
                    }
                    else {
                        logger.trace("Static Global Controller Check " + global_controller_host + " Failed.");
                        controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Static Global Host :" + global_controller_host + " not reachable.");
                        global_host_map.remove(global_controller_host);
                    }
                }
                String[] globalController = connectToGlobal(staticGlobalDiscovery());
                if(globalController == null) {
                    logger.error("Failed to connect to Global Controller Host :" + global_controller_host);
                    controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Static Global Host :" + global_controller_host + " failed to connect.");
                }
                else {
                    if(isGlobalWatchDogRegister(globalController[0],globalController[1])) {
                        controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0], globalController[1], "gCheck : Static Global Host :" + global_controller_host + " connected.");
                        logger.info("Static Global Controller Static Host: " + global_controller_host + " Connect with path: " + controllerEngine.cstate.getGlobalControllerPath());
                        global_host_map.put(global_controller_host, controllerEngine.cstate.getGlobalControllerPath());
                        //register with global controller
                        //sendGlobalWatchDogRegister();
                    } else {
                        controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Static Global Host : " + this.controllerEngine.cstate.getGlobalControllerPath() + " is not reachable.");
                    }
                }
            }
            else if(this.controllerEngine.cstate.isGlobalController()) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");
                if(controllerEngine.getAppScheduleQueue() == null) {
                    controllerEngine.setAppScheduleQueue(new LinkedBlockingQueue<gPayload>());
                    startGlobalSchedulers();
                }
            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(this.controllerEngine.cstate.isGlobalController()) {
                    //if(this.controllerEngine.cstate.getGlobalControllerPath() != null) {

                        if (controllerEngine.isReachableAgent(this.controllerEngine.cstate.getGlobalControllerPath())) {
                        logger.debug("Dynamic Global Path : " + this.controllerEngine.cstate.getGlobalControllerPath() + " reachable :" + controllerEngine.isReachableAgent(this.controllerEngine.cstate.getGlobalControllerPath()));
                        //TODO Check if regionalDBexport() is needed
                        //regionalDBexport();
                        return;
                    }
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host :" + this.controllerEngine.cstate.getGlobalControllerPath() + " is not reachable.");
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(!discoveryList.isEmpty()) {
                        String[] globalController = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalController == null) {
                            logger.error("Failed to connect to Global Controller Host : [unnown]");
                            controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host [unknown] failed to connect.");
                        }
                        else {
                            controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0],globalController[1], "gCheck : Dyanmic Global Host :" + globalController[0] + "_" + globalController[1] + " connected." );
                            logger.info("Static Global Controller Dynamic" + global_controller_host + " Connect with path: " + controllerEngine.cstate.getGlobalControllerPath());

                            //register with global controller
                            //todo does this need to exist?
                            //sendGlobalWatchDogRegister();
                        }
                    }
                    else {
                        //No global controller found, starting global services
                        logger.info("No Global Controller Found: Starting Global Services");
                        //start global stuff
                        //create globalscheduler queue
                        //agentcontroller.setResourceScheduleQueue(new LinkedBlockingQueue<MsgEvent>());
                        controllerEngine.setAppScheduleQueue(new LinkedBlockingQueue<gPayload>());
                        startGlobalSchedulers();
                        //end global start
                        this.controllerEngine.cstate.setGlobalSuccess("gCheck : Creating Global Host");
                        logger.info("Global: " + this.controllerEngine.cstate.getRegionalRegion() + " Agent: " + this.controllerEngine.cstate.getRegionalAgent());
                        //todo does this need to exist?
                        //sendGlobalWatchDogRegister();
                    }
                }
            }

        }
        catch(Exception ex) {
            logger.error("gCheck() " +ex.getMessage());
        }
        logger.trace("gCheck End");

    }

    private Boolean startGlobalSchedulers() {
        boolean isStarted = false;
        try {
            //Start Global Controller Services
            logger.info("Initialized");
            ResourceSchedulerEngine se = new ResourceSchedulerEngine(controllerEngine,this);
            Thread schedulerEngineThread = new Thread(se);
            schedulerEngineThread.start();

            AppSchedulerEngine ae = new AppSchedulerEngine(controllerEngine,this);
            Thread appSchedulerEngineThread = new Thread(ae);
            appSchedulerEngineThread.start();

            isStarted = true;
        }
        catch (Exception ex) {
            logger.error("startGlobalSchedulers() " + ex.getMessage());
        }
        return isStarted;
    }

    private String[] connectToGlobal(List<MsgEvent> discoveryList) {
        String[] globalController = null;
        MsgEvent cme = null;
        int cme_count = 0;

	    try {
	        for(MsgEvent ime : discoveryList) {
	            logger.trace("Global Discovery Response : " + ime.getParams().toString());
	            //determine least loaded
	            String ime_count_string = ime.getParam("agent_count");
	            if(ime_count_string != null) {
                    int ime_count = Integer.parseInt(ime_count_string);
	                if(cme == null) {
                        cme = ime;
                        cme_count = ime_count;
                    }
                    else {
                        if(ime_count < cme_count) {
                            cme = ime;
                            cme_count = ime_count;
                        }
                    }
                }

            }
            //if we have a canadate, check to see if we are already connected a regions
            if(cme != null) {
	            if((cme.getParam("dst_region") != null) && (cme.getParam("dst_agent")) !=null) {
                    String cGlobalPath = cme.getParam("dst_region") + "_" + (cme.getParam("dst_agent"));
                    if(!controllerEngine.isReachableAgent(cGlobalPath)) {
                        controllerEngine.getIncomingCanidateBrokers().add(cme);
                        //while
                        int timeout = 0;
                        while((!controllerEngine.isReachableAgent(cGlobalPath)) && (timeout < 10)) {
                            logger.trace("Trying to connect to Global Controller : " + cGlobalPath);
                            timeout++;
                            Thread.sleep(1000);
                        }
                        if(controllerEngine.isReachableAgent(cGlobalPath)) {
                            globalController = new String[2];
                            globalController[0] = cme.getParam("dst_region");
                            globalController[1] = cme.getParam("dst_agent");
                        }
                    }
                    else {
                        globalController = new String[2];
                        globalController[0] = cme.getParam("dst_region");
                        globalController[1] = cme.getParam("dst_agent");
                    }
                }

            }

        }
        catch(Exception ex) {
            logger.error("connectToGlobal()" + ex.getMessage());
        }

        return globalController;
    }

    private Boolean isGlobalWatchDogRegister(String globalRegion, String globalAgent) {
        boolean isGlobalReg = false;
        try {

            if(controllerEngine.isReachableAgent(globalRegion + "_" + globalAgent)) {

                MsgEvent le  = new MsgEvent(MsgEvent.Type.CONFIG, controllerEngine.cstate.getRegion(),controllerEngine.cstate.getAgent(),null,globalRegion,globalAgent,null,true,true);


                le.setParam("region_name",controllerEngine.cstate.getRegion());

                //le.setParam("dst_region", gPath[0]);
                le.setParam("is_active", Boolean.TRUE.toString());
                le.setParam("action", "region_enable");
                //le.setParam("globalcmd", Boolean.TRUE.toString());
                le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                //this should be RPC, but routing needs to be fixed route 16 -> 32 -> regionsend -> 16 -> 32 -> regionsend (goes to region, not rpc)
                le.setParam("source","sendGlobalWatchDogRegister()");
                MsgEvent re = plugin.sendRPC(le);
                if(re != null) {
                    isGlobalReg = true;
                }
            } else {
                logger.info("Candidate Global Controller not reachable!");
            }
        }
        catch(Exception ex) {
            logger.info("sendGlobalWatchDogRegister() " + ex.getMessage());
            ex.printStackTrace();
        }
        return isGlobalReg;
    }

    private void sendGlobalWatchDogRegister() {

	    try {
            //if(!controllerEngine.cstate.isGlobalController()) {
                //is the global controller reachable

            while(!plugin.isActive()) {
                Thread.sleep(1000);
            }

                if(controllerEngine.isReachableAgent(controllerEngine.cstate.getGlobalControllerPath())) {
                    MsgEvent le = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                    /*
                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "enabled");
                    le.setParam("src_region", plugin.getRegion());
                    le.setParam("src_agent", plugin.getAgent());
                    le.setParam("dst_region",controllerEngine.cstate.getGlobalRegion());
                    le.setParam("dst_agent",controllerEngine.cstate.getGlobalAgent());
                    le.setParam("is_regional", Boolean.TRUE.toString());
                    le.setParam("is_global", Boolean.TRUE.toString());
                    */

                    le.setParam("region_name",controllerEngine.cstate.getRegionalRegion());

                    //le.setParam("dst_region", gPath[0]);
                    le.setParam("is_active", Boolean.TRUE.toString());
                    le.setParam("action", "region_enable");
                    //le.setParam("globalcmd", Boolean.TRUE.toString());
                    le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                    //this should be RPC, but routing needs to be fixed route 16 -> 32 -> regionsend -> 16 -> 32 -> regionsend (goes to region, not rpc)
                    le.setParam("source","sendGlobalWatchDogRegister()");

                    plugin.msgOut(le);
                }
            //}
        }
        catch(Exception ex) {
	        logger.error("sendGlobalWatchDogRegister() " + ex.getMessage());
        }

    }

    private List<MsgEvent> dynamicGlobalDiscovery() {
        List<MsgEvent> discoveryList = null;
        try {
            discoveryList = new ArrayList<>();

            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dcv6 = new DiscoveryClientIPv6(controllerEngine);
                discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000));
            }
            DiscoveryClientIPv4 dcv4 = new DiscoveryClientIPv4(controllerEngine);
            discoveryList.addAll(dcv4.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));

            if (!discoveryList.isEmpty()) {
                for (MsgEvent ime : discoveryList) {
                    logger.debug("Global Controller Found: " + ime.getParams());
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return discoveryList;
    }

    private List<MsgEvent> staticGlobalDiscovery() {
        List<MsgEvent> discoveryList = null;
        try {
            discoveryList = new ArrayList<>();
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host",null));
                TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
                discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("global_controller_host",null)));
                logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                if (discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host",null) + " failed! - Restarting Global Discovery");
                } else {
                    //agentcontroller.getIncomingCanidateBrokers().add(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: Region: " + discoveryList.get(0).getParam("src_region") + " agent:" + discoveryList.get(0).getParam("src_agent"));
                }
        }
        catch(Exception ex) {
            logger.error("staticGlobalDiscovery() " + ex.getMessage());
        }
        return discoveryList;
    }

    private boolean doRegionalUpdate(String dbExport) {
        boolean doUpdate = false;
            if(lastDBUpdate == null) {
                doUpdate = true;
            } else {
                //if(!(lastDBUpdate.length() == dbExport.length())) {
                    //doUpdate = true;
                //} else {
                    if(!lastDBUpdate.equals(dbExport)) {
                        doUpdate = true;
                    }
                //}
            }
        return doUpdate;
    }

    public MsgEvent regionalDBexport() {
        MsgEvent me = null;
	    try {
            if(!this.controllerEngine.cstate.isGlobalController()) {
                //TODO Enable Export
                String dbexport = controllerEngine.getGDB().getDBExport();

                if (doRegionalUpdate(dbexport)) {

                    logger.info("Exporting Region DB to Global Controller");

                    lastDBUpdate = dbexport;
                    me = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                    me.setParam("action", "regionalimport");
                    /*
                    me = new MsgEvent(MsgEvent.Type.CONFIG, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "regionalimport");
                    me.setParam("action", "regionalimport");
                    me.setParam("src_region", this.plugin.getRegion());
                    me.setParam("src_agent", this.plugin.getAgent());

                    me.setParam("dst_region", controllerEngine.cstate.getGlobalRegion());
                    me.setParam("dst_agent", controllerEngine.cstate.getGlobalAgent());

                    me.setParam("is_regional", Boolean.TRUE.toString());
                    me.setParam("is_global", Boolean.TRUE.toString());
                    */

                    me.setParam("exportdata", dbexport);

                    //logger.error("*" + me.getCompressedParam("exportdata")  + "*");

                    /*
                    MsgEvent re = this.plugin.sendRPC(me);
                    if(re != null) {
                        logger.info("EXPORT RETURNED!");
                    } else {
                        logger.info("RE = NULL");
                    }
                    */
                    this.plugin.msgOut(me);
                }
            }

        }
        catch(Exception ex) {
            logger.error("regionalDBexport() " + ex.getMessage());
        }
        return me;
    }

    class GlobalNodeStatusWatchDog extends TimerTask {
        private ControllerEngine controllerEngine;
        private CLogger logger;
        private PluginBuilder plugin;
        public GlobalNodeStatusWatchDog(ControllerEngine controllerEngine, CLogger logger) {
            //this.agentcontroller = agentcontroller;
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
            this.logger = logger;

        }

        public void run() {
            if(controllerEngine.cstate.isGlobalController()) { //only run if node is a global controller
                logger.debug("GlobalNodeStatusWatchDog");
                //update own database

                /*
                Map<String, NodeStatusType> edgeHealthListStatus = controllerEngine.getGDB().getNodeStatus(null, null, null);
                for (Map.Entry<String, NodeStatusType> entry : edgeHealthListStatus.entrySet()) {
                    logger.info("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                }

                Map<String, NodeStatusType> nodeListStatus = controllerEngine.getGDB().getNodeStatus(null, null, null);


                for (Map.Entry<String, NodeStatusType> entry : nodeListStatus.entrySet()) {
*/
                Map<String, NodeStatusType> edgeHealthListStatus = controllerEngine.getGDB().getEdgeHealthStatus(null, null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeHealthListStatus.entrySet()) {

                logger.debug("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                    if(entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        //mark node disabled
                        controllerEngine.getGDB().setEdgeParam(entry.getKey(),"is_active",Boolean.FALSE.toString());
                    }
                    else if(entry.getValue() == NodeStatusType.LOST) { //will include more items once nodes update correctly
                        /*
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        //remove nodes
                        Map<String,String> nodeParams = controllerEngine.getGDB().gdb.getNodeParams(entry.getKey());
                        String region = nodeParams.get("region");
                        String agent = nodeParams.get("agent");
                        String pluginId = nodeParams.get("agentcontroller");
                        logger.error("Removing " + region + " " + agent + " " + pluginId);
                        controllerEngine.getGDB().removeNode(region,agent,pluginId);
                        */
                        Map<String,String> edgeParams = controllerEngine.getGDB().getEdgeParamsNoTx(entry.getKey());
                        //String nodeId = controllerEngine.getGDB().gdb.getNodeId(edgeMap.get("region"),null,null);
                        //Map<String,String> nodeParams = controllerEngine.getGDB().gdb.getNodeParams(entry.getKey());
                        String region = edgeParams.get("region");
                        String agent = edgeParams.get("agent");
                        String pluginId = edgeParams.get("agentcontroller");
                        logger.error("Removing " + region + " " + agent + " " + pluginId);
                        controllerEngine.getGDB().removeNode(region,agent,pluginId);
                    }
                    else if(entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                    } /*else {
                        logger.info("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        Map<String,String> nodeMap = controllerEngine.getGDB().gdb.getNodeParams(entry.getKey());
                        logger.info("Region : " + nodeMap.get("region_name") + " Agent : " + nodeMap.get("agent_name"));
                    }
                    */

                }
            }
            else {
                //send full export of region to global controller
                //Need to develop a better inconsistency method
                //logger.info("Regional Export");
                //TODO enable global export
                //regionalDBexport();

            }
        }
    }
}