package io.cresco.agent.controller.globalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.netdiscovery.DiscoveryClientIPv4;
import io.cresco.agent.controller.netdiscovery.DiscoveryClientIPv6;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netdiscovery.TCPDiscoveryStatic;
import io.cresco.agent.db.NodeStatusType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class GlobalHealthWatcher  {
    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private Map<String,String> global_host_map;
    private Timer communicationsHealthTimer;
    private Timer globalUpdateTimer;
    private Long gCheckInterval;
    private String lastDBUpdateRegions;
    private String lastDBUpdateAgents;
    private String lastDBUpdatePlugins;
    private long startTS;
    private int wdTimer;


    public Boolean SchedulerActive;
    public Boolean AppSchedulerActive;

    public GlobalHealthWatcher(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(GlobalHealthWatcher.class.getName(),CLogger.Level.Info);

        global_host_map = new HashMap<>();

        communicationsHealthTimer = new Timer();
        communicationsHealthTimer.scheduleAtFixedRate(new GlobalHealthWatcher.GlobalHealthWatchTask(controllerEngine, logger), 5000, 3000);

        globalUpdateTimer = new Timer();
        globalUpdateTimer.scheduleAtFixedRate(new GlobalHealthWatcher.GlobalNodeStatusWatchDog(controllerEngine, logger), 500, 15000);//remote
        gCheckInterval = plugin.getConfig().getLongParam("watchdogtimer",5000L);
        SchedulerActive = false;
        AppSchedulerActive = false;

        try {
            while (!controllerEngine.getActiveClient().hasActiveProducer()) {
                logger.trace("GlobalHealthWatcher waiting on Active Producer.");
                Thread.sleep(2500);
            }

            logger.trace("Initial gCheck");

            gCheck(); //do initial check

            controllerEngine.setGlobalControllerManagerActive(true);


        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }

	public void shutdown() {

        globalUpdateTimer.cancel();
        communicationsHealthTimer.cancel();

        SchedulerActive = false;
        AppSchedulerActive = false;

        //controllerEngine.getPerfControllerMonitor().removeKpiListener();


        if(!controllerEngine.cstate.isGlobalController()) {
            if (controllerEngine.isReachableAgent(controllerEngine.cstate.getGlobalControllerPath())) {
                MsgEvent le = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);

                le.setParam("region_name", controllerEngine.cstate.getRegionalRegion());
                le.setParam("action", "region_disable");

                le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                plugin.msgOut(le);
            }
        }
	}

	private void gNotify() {
        logger.trace("gNotify Start");
        try {
            //if there is a remote global controller and it is reachable
            if(controllerEngine.cstate.isRegionalController()) {
                logger.trace("gNotify !Global Controller Message");
                //is the global controller reachable
                if(controllerEngine.isReachableAgent(controllerEngine.cstate.getGlobalControllerPath())) {

                    MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                    tick.setParam("region_name",controllerEngine.cstate.getRegionalRegion());
                    tick.setParam("is_regional", Boolean.TRUE.toString());
                    tick.setParam("is_global", Boolean.TRUE.toString());

                    tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                    tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));

                    logger.trace("gNotify !Global Controller Message : " + tick.getParams().toString());

                    plugin.msgIn(tick);


                }
            }

            else if(controllerEngine.cstate.isGlobalController()) {
                logger.trace("gNotify Global Controller Message");

                MsgEvent tick = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
                tick.setParam("region_name",controllerEngine.cstate.getRegionalRegion());


                tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));

                logger.trace("gNotify Global Controller Message : " + tick.getParams().toString());

                tick.setParam("mode","REGION");
                controllerEngine.getGDB().nodeUpdate(tick);
            }

            else {
                logger.trace("gNotify : Why and I here!");
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
                    boolean isRegistered = controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0], globalController[1], "gCheck : Static Global Host :" + global_controller_host + " connected.");
                    if(isRegistered) {
                        //controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0], globalController[1], "gCheck : Static Global Host :" + global_controller_host + " connected.");
                        logger.info("Static Global Controller Static Host: " + global_controller_host + " Connect with path: " + controllerEngine.cstate.getGlobalControllerPath());
                        global_host_map.put(global_controller_host, controllerEngine.cstate.getGlobalControllerPath());

                        //register with global controller
                        //sendGlobalWatchDogRegister();
                    } else {
                        controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Static Global Host : " + this.controllerEngine.cstate.getGlobalControllerPath() + " is not reachable.");
                    }
                }
            }
            else if(plugin.getConfig().getBooleanParam("is_global",false)) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");

                //if not global controller start it
                if(!this.controllerEngine.cstate.isGlobalController()) {
                    this.controllerEngine.cstate.setGlobalSuccess("gCheck : Creating Global Host");
                    logger.info("Global: " + this.controllerEngine.cstate.getRegionalRegion() + " Agent: " + this.controllerEngine.cstate.getRegionalAgent());

                    if (controllerEngine.getAppScheduler() == null) {
                        startGlobalSchedulers();
                    }
                }


            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(this.controllerEngine.cstate.isGlobalController()) {
                    //if(this.controllerEngine.cstate.getGlobalControllerPath() != null) {

                        if (controllerEngine.isReachableAgent(this.controllerEngine.cstate.getGlobalControllerPath())) {
                        logger.trace("Dynamic Global Path : " + this.controllerEngine.cstate.getGlobalControllerPath() + " reachable :" + controllerEngine.isReachableAgent(this.controllerEngine.cstate.getGlobalControllerPath()));
                        return;
                    }
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host :" + this.controllerEngine.cstate.getGlobalControllerPath() + " is not reachable.");
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(discoveryList == null) {
                        discoveryList = new ArrayList<>();
                    }

                    if(!discoveryList.isEmpty()) {

                        /*
                        {dst_region=region-2742c227-5467-4ec2-b3fe-f1e727717884, dst_agent=agent-0bd88dd6-196f-4128-b600-be6b6fb7a2b6, discovery_type=GLOBAL, broadcast_ts=1576788694283, dst_port=32005, src_agent=tester, src_region=test, dst_ip=172.17.0.3, agent_count=2, validated_authenication=7b8a0bdd-8998-4e6d-be32-c64e20be3ccc,e01c630a-b3ec-48fb-a342-8ab67c5c4783,global, broadcast_latency=3, src_ip=172.17.0.2, src_port=49659}
                         */

                        String[] globalController = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalController == null) {
                            logger.error("Failed to connect to Global Controller Host : [unnown]");
                            controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host [unknown] failed to connect.");
                        }
                        else {

                            boolean isRegistered = controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0], globalController[1], "gCheck : Static Global Host :" + global_controller_host + " connected.");
                            if(isRegistered) {
                                //controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0], globalController[1], "gCheck : Static Global Host :" + global_controller_host + " connected.");
                                logger.info("Static Global Controller Static Host: " + globalController[2] + " Connect with path: " + controllerEngine.cstate.getGlobalControllerPath());
                                global_host_map.put(globalController[2], controllerEngine.cstate.getGlobalControllerPath());

                                //register with global controller
                                //sendGlobalWatchDogRegister();
                            } else {
                                controllerEngine.cstate.setRegionalGlobalFailed("gCheck : Static Global Host : " + this.controllerEngine.cstate.getGlobalControllerPath() + " is not reachable.");
                            }

                            //logger.info("Static Global Controller Dynamic " + globalController[2] + " Connect with path: " + controllerEngine.cstate.getGlobalControllerPath());
                            //controllerEngine.cstate.setRegionalGlobalSuccess(globalController[0],globalController[1], "gCheck : Dyanmic Global Host :" + globalController[0] + "_" + globalController[1] + " connected." );
                            //global_host_map.put(globalController[2], controllerEngine.cstate.getGlobalControllerPath());
                        }
                    }
                    else {
                        //No global controller found, starting global services
                        logger.info("No Global Controller Found: Starting Global Services");
                        //start global stuff
                        //create globalscheduler queue
                        startGlobalSchedulers();
                        //end global start
                        this.controllerEngine.cstate.setGlobalSuccess("gCheck : Creating Global Host");
                        logger.info("Global: " + this.controllerEngine.cstate.getRegionalRegion() + " Agent: " + this.controllerEngine.cstate.getRegionalAgent());
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
            logger.info("Initialized Global Application Scheduling");

            ResourceScheduler resourceScheduler = new ResourceScheduler(controllerEngine,this);
            controllerEngine.setResourceScheduler(resourceScheduler);

            AppScheduler appScheduler = new AppScheduler(controllerEngine,this);
            controllerEngine.setAppScheduler(appScheduler);

            //we also need to start collecting KPI information
            //controllerEngine.getPerfControllerMonitor().setKpiListener();

            isStarted = true;
        }
        catch (Exception ex) {
            logger.error("startGlobalSchedulers() " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());

        }
        return isStarted;
    }


    private String[] connectToGlobal(List<MsgEvent> discoveryList) {

        logger.debug("connecToGlobal()");

        String[] globalController = null;
        MsgEvent cme = null;
        int cme_count = 0;

	    try {
	        for(MsgEvent ime : discoveryList) {
	            logger.debug("Global Discovery Response : " + ime.getParams().toString());
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
                        logger.debug("cme NOT REACHABLE");
                        controllerEngine.getIncomingCanidateBrokers().add(cme);
                        logger.debug("cme submitted canidate broker");
                        //while
                        int timeout = 0;
                        while((!controllerEngine.isReachableAgent(cGlobalPath)) && (timeout < 10)) {
                            logger.info("Trying to connect to Global Controller : " + cGlobalPath);
                            timeout++;
                            Thread.sleep(1000);
                        }
                        if(controllerEngine.isReachableAgent(cGlobalPath)) {
                            globalController = new String[2];
                            globalController[0] = cme.getParam("dst_region");
                            globalController[1] = cme.getParam("dst_agent");
                            globalController[2] = cme.getParam("dst_ip");

                        }
                    }
                    else {
                        globalController = new String[2];
                        globalController[0] = cme.getParam("dst_region");
                        globalController[1] = cme.getParam("dst_agent");
                        globalController[2] = cme.getParam("dst_ip");
                    }
                }

            }

        }
        catch(Exception ex) {
            logger.error("connectToGlobal()" + ex.getMessage());
        }

        return globalController;
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
                logger.debug("staticGlobalDiscovery() Static Agent Connection count = " + discoveryList.size());
                if (discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host",null) + " failed! - Restarting Global Discovery");
                } else {
                    //agentcontroller.getIncomingCanidateBrokers().add(discoveryList.get(0)); //perhaps better way to do this
                    //controllerEngine.getIncomingCanidateBrokers().add(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: Region: " + discoveryList.get(0).getParam("src_region") + " agent:" + discoveryList.get(0).getParam("src_agent"));
                }
        }
        catch(Exception ex) {
            logger.error("staticGlobalDiscovery() " + ex.getMessage());
        }
        return discoveryList;
    }


    private Map<String,String> filterRegionalUpdate(Map<String,String> dbExport) {

        if(dbExport.containsKey("regionconfigs")) {

            if(lastDBUpdateRegions != null) {
                if (!dbExport.get("regionconfigs").equals(lastDBUpdateRegions)) {
                    lastDBUpdateRegions = dbExport.get("regionconfigs");
                } else {
                    dbExport.remove("regionconfigs");
                    logger.error("REMOVE REGION CONFIG");
                }
            } else {
                lastDBUpdateRegions = dbExport.get("regionconfigs");
            }
        }

        if(dbExport.containsKey("agentconfigs")) {

            if(lastDBUpdateAgents != null) {
                if (!dbExport.get("agentconfigs").equals(lastDBUpdateAgents)) {
                    lastDBUpdateAgents = dbExport.get("agentconfigs");
                } else {
                    dbExport.remove("agentconfigs");
                    logger.error("REMOVE AGENT CONFIG");
                }
            } else {
                lastDBUpdateAgents = dbExport.get("agentconfigs");
            }
        }

        if(dbExport.containsKey("pluginconfigs")) {

            if(lastDBUpdatePlugins != null) {
                if (!dbExport.get("pluginconfigs").equals(lastDBUpdatePlugins)) {
                    lastDBUpdatePlugins = dbExport.get("pluginconfigs");
                } else {
                    dbExport.remove("pluginconfigs");
                    logger.error("REMOVE PLUGIN CONFIG");
                }
            } else {
                lastDBUpdatePlugins = dbExport.get("pluginconfigs");
            }
        }

        return dbExport;
    }

    public MsgEvent regionalDBexport() {
        MsgEvent me = null;
	    try {
            if(!this.controllerEngine.cstate.isGlobalController()) {

                Map<String,String> dbexport = controllerEngine.getGDB().getDBExport(true,true,true,null,null,null);


                dbexport = filterRegionalUpdate(dbexport);

                logger.error("[" + dbexport + "]");

                logger.info("Exporting Region DB to Global Controller");

                    me = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                    me.setParam("action", "regionalimport");

                    me.setParam("region_watchdog_update", plugin.getRegion());

                    for (Map.Entry<String, String> entry : dbexport.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        me.setCompressedParam(key,value);
                    }

                    this.plugin.msgOut(me);
            }

        }
        catch(Exception ex) {
            logger.error("regionalDBexport() " + ex.getMessage());
            ex.printStackTrace();
        }
        return me;
    }

    class GlobalHealthWatchTask extends TimerTask {
        private ControllerEngine controllerEngine;
        private CLogger logger;
        private PluginBuilder plugin;
        public GlobalHealthWatchTask(ControllerEngine controllerEngine, CLogger logger) {
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
            this.logger = logger;

        }

        public void run() {
            try {


                //while (controllerEngine.isGlobalControllerManagerActive()) {
                //    Thread.sleep(gCheckInterval);
                    gCheck();
                    gNotify();
                //}
                //logger.info("Starting Global Shutdown");
                //shutdown();

            } catch(Exception ex) {
                logger.error("globalwatcher run() " + ex.getMessage());
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logger.error(errors.toString());
            }
        }

    }

    class GlobalNodeStatusWatchDog extends TimerTask {
        private ControllerEngine controllerEngine;
        private CLogger logger;
        private PluginBuilder plugin;
        public GlobalNodeStatusWatchDog(ControllerEngine controllerEngine, CLogger logger) {
            this.controllerEngine = controllerEngine;
            this.plugin = controllerEngine.getPluginBuilder();
            this.logger = logger;

        }

        public void run() {

            if (controllerEngine.cstate.isGlobalController()) { //only run if node is global controller
                logger.trace("GlobalNodeStatusWatchDog");

                Map<String, NodeStatusType> edgeStatus = controllerEngine.getGDB().getEdgeHealthStatus(null, null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeStatus.entrySet()) {

                    if (!plugin.getRegion().equals(entry.getKey())) {

                        logger.trace("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                        if (entry.getValue() == NodeStatusType.PENDINGSTALE) { //will include more items once nodes update correctly
                            logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                            controllerEngine.getGDB().setNodeStatusCode(entry.getKey(), null, null, 40, "set STALE by global controller health watcher");

                        } else if (entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                            logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                            controllerEngine.getGDB().setNodeStatusCode(entry.getKey(), null, null, 50, "set LOST by regional controller health watcher");

                        } else if (entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly

                        }

                    }
                }
            }
        }
    }
}