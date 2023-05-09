package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.communication.ActiveBroker;
import io.cresco.agent.controller.communication.ActiveBrokerManager;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.db.DBManager;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.measurement.PerfControllerMonitor;
import io.cresco.agent.controller.measurement.PerfMonitorNet;
import io.cresco.agent.controller.netdiscovery.*;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.data.DataPlanePersistantInstance;
import io.cresco.agent.data.DataPlaneServiceImpl;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.mina.statemachine.annotation.State;
import org.apache.mina.statemachine.annotation.Transition;
import org.apache.mina.statemachine.annotation.Transitions;
import org.apache.mina.statemachine.context.StateContext;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ControllerSMHandler {

    @State public static final String ROOT = "Root";
    @State(ROOT) public static final String EMPTY   = "Empty";

    @State(ROOT) public static final String PRE_INIT = "PRE_INIT";
    @State(ROOT) public static final String STANDALONE_INIT = "STANDALONE_INIT";
    @State(ROOT) public static final String STANDALONE = "STANDALONE";
    @State(ROOT) public static final String STANDALONE_FAILED = "STANDALONE_FAILED";
    @State(ROOT) public static final String STANDALONE_SHUTDOWN = "STANDALONE_SHUTDOWN";
    @State(ROOT) public static final String AGENT_INIT = "AGENT_INIT";
    @State(ROOT) public static final String AGENT = "AGENT";
    @State(ROOT) public static final String AGENT_FAILED = "AGENT_FAILED";
    @State(ROOT) public static final String AGENT_SHUTDOWN = "AGENT_SHUTDOWN";
    @State(ROOT) public static final String REGION_INIT = "REGION_INIT";
    @State(ROOT) public static final String REGION_FAILED = "REGION_FAILED";
    @State(ROOT) public static final String REGION = "REGION";
    @State(ROOT) public static final String REGION_GLOBAL_INIT = "REGION_GLOBAL_INIT";
    @State(ROOT) public static final String REGION_GLOBAL_FAILED = "REGION_GLOBAL_FAILED";
    @State(ROOT) public static final String REGION_GLOBAL = "REGION_GLOBAL";
    @State(ROOT) public static final String REGION_SHUTDOWN = "REGION_SHUTDOWN";
    @State(ROOT) public static final String GLOBAL_INIT = "GLOBAL_INIT";
    @State(ROOT) public static final String GLOBAL = "GLOBAL";
    @State(ROOT) public static final String GLOBAL_FAILED = "GLOBAL_FAILED";
    @State(ROOT) public static final String GLOBAL_SHUTDOWN = "GLOBAL_SHUTDOWN";


    private ControllerEngine controllerEngine;
    private ControllerStateImp cstate;
    private PluginBuilder plugin;
    private CLogger logger;
    private StateContext stateContext;

    private Timer stateUpdateTimer;
    private Collection<org.apache.mina.statemachine.State> stateCollection = null;

    private ControllerMode currentState = null;
    private String currentDesc = null;
    private String globalRegion = null;
    private String globalAgent = null;
    private String regionalRegion = null;
    private String regionalAgent = null;
    private String localRegion = null;
    private String localAgent = null;

    private AtomicBoolean forceShutdown = new AtomicBoolean(false);

    private DataPlaneServiceImpl dataPlaneService;

    public ControllerSMHandler(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ControllerSMHandler.class.getName(), CLogger.Level.Info);
        this.cstate = controllerEngine.cstate;
        this.stateUpdateTimer = new Timer();
        this.stateUpdateTimer.scheduleAtFixedRate(new stateUpdateTask(), 500, 5000l);
    }

    @Transition(on = "start", in = EMPTY)
    public void start(StateContext context) {
        if(stateContext == null) {
            stateContext = context;
        }
        stateInit();
    }

    @Transitions({
            @Transition(on = "globalControllerLost", in = REGION_GLOBAL_FAILED),
            @Transition(on = "globalControllerLost", in = REGION_GLOBAL)
    })
    public void globalControllerLost(StateContext context, String desc) {
        logger.error("GLOBAL CONTROLLER LOST : CURRENT STATE: " + context.getCurrentState().getId());

        if(context.getCurrentState().getId().equals(REGION_GLOBAL)) {
            controllerEngine.cstate.setRegionalGlobalFailed(desc);
            if(!isRegionalGlobalShutdown()) {
              logger.error("!isRegionalGlobalShutdown() Dirty Shutdown!");
            }
            stateInit();
            logger.info("RegionalGlobalFailed Recovered");
            //startup things
        } else {
            logger.error("!= REGIONAL_GLOBAL");
        }
    }

    @Transitions({
            @Transition(on = "regionalControllerLost", in = AGENT_FAILED),
            @Transition(on = "regionalControllerLost", in = AGENT)
    })
    public void regionalControllerLost(StateContext context) {
        logger.error("REGIONAL CONTROLLER LOST : CURRENT STATE: " + context.getCurrentState().getId());

        if(context.getCurrentState().getId().equals(AGENT)) {
            controllerEngine.cstate.setAgentFailed("AgentWatcher Failed");
            if(!isAgentShutdown()) {
                logger.error("!isAgentShutdown() Dirty Shutdown!");
            }
            stateInit();
            logger.info("AgentFailed Recovered");
            //startup things
        } else {
            logger.error("!= AGENT");
        }
    }

    @Transition(on = "stop", in = PRE_INIT, next = EMPTY)
    public void stopPre() {
        logger.error("STOP CALLED PreINIT");
    }

    @Transitions({
            @Transition(on = "stop", in = STANDALONE_INIT, next = EMPTY),
            @Transition(on = "stop", in = STANDALONE, next = EMPTY),
            @Transition(on = "stop", in = STANDALONE_FAILED, next = EMPTY)
    })
    public void stopStandalone() {
        logger.error("STOP CALLED STANDALONE");
        cstate.setStandaloneShutdown("Shutdown Called");
    }

    @Transitions({
            @Transition(on = "stop", in = AGENT_INIT, next = EMPTY),
            @Transition(on = "stop", in = AGENT, next = EMPTY),
            @Transition(on = "stop", in = AGENT_FAILED, next = EMPTY)
    })
    public void stopAgent() {

        //stop internal update timer
        stateUpdateTimer.cancel();

        //don't try to unregister if agent has failed
        if(cstate.getControllerState() != ControllerMode.AGENT_FAILED) {
            unregisterAgent(cstate.getRegion(), cstate.getAgent());
        }

        if(!isAgentShutdown()) {
            logger.error("!isAgentShutdown() Dirty Shutdown!");
        }

        cstate.setAgentShutdown("Shutdown Called");

    }

    @Transitions({
            @Transition(on = "stop", in = REGION_INIT, next = EMPTY),
            @Transition(on = "stop", in = REGION_FAILED, next = EMPTY),
            @Transition(on = "stop", in = REGION, next = EMPTY),
            @Transition(on = "stop", in = REGION_GLOBAL_INIT, next = EMPTY),
            @Transition(on = "stop", in = REGION_GLOBAL_FAILED, next = EMPTY),
            @Transition(on = "stop", in = REGION_GLOBAL, next = EMPTY)
    })
    public void stopRegion() {
        //stop internal update timer
        stateUpdateTimer.cancel();

        logger.error("STOP CALLED REGION");
        cstate.setRegionShutdown("Shutdown Called");
    }


    @Transitions({
            @Transition(on = "stop", in = GLOBAL_INIT, next = EMPTY),
            @Transition(on = "stop", in = GLOBAL, next = EMPTY),
            @Transition(on = "stop", in = GLOBAL_FAILED, next = EMPTY)
    })
    public void stopGlobal() {

        //stop internal update timer
        stateUpdateTimer.cancel();

        logger.debug("STOP CALLED GLOBAL");
        cstate.setGlobalShutdown("Shutdown Called");

        //stop net discovery
        logger.info("Shutdown discovery functions");
        stopNetDiscoveryEngine();

        logger.info("Dataplane service shutdown");
        dataPlaneService.shutdown();

        logger.info("Shutdown agent functions");
        isAgentShutdown();


        //DB should not be called after this

        //prevent regional from trying to restart broker
        logger.info("Shutting down Regional Health Watcher");
        if(controllerEngine.getRegionHealthWatcher() != null) {
            controllerEngine.getRegionHealthWatcher().shutdown();
            controllerEngine.setRegionHealthWatcher(null);
        }

        if(controllerEngine.getGlobalHealthWatcher() != null) {
            controllerEngine.getGlobalHealthWatcher().shutdown();
            controllerEngine.setGlobalHealthWatcher(null);
        }

        logger.info("Shutdown Active Broker Manager");
        controllerEngine.setActiveBrokerManagerActive(false);
        //Poison Pill Shutdown, send null class if blocking on input
        DiscoveryNode poisonDiscoveryNode = new DiscoveryNode(DiscoveryType.SHUTDOWN);
        try {
            controllerEngine.getIncomingCanidateBrokers().put(poisonDiscoveryNode);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while(controllerEngine.getActiveBrokerManagerThread().isAlive()) {
            logger.info("Waiting on Active Broker Manager Thread");
        }

        controllerEngine.getActiveClient().shutdown();

        logger.info("Shutting down Broker");
        if(controllerEngine.getBroker() != null) {
            controllerEngine.getBroker().stopBroker();
        }

        controllerEngine.setDBManagerActive(false);
            while(controllerEngine.getDBManagerThread().isAlive()) {
                logger.info("Waiting on DB Manager Shutdown");
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            /*
            if(controllerEngine.getGDB() != null) {
                controllerEngine.getGDB().shutdown();
            }
             */
            logger.debug("stopGlobal Complete");

    }

    public void shutdown() {
        forceShutdown.set(true);
    }


    private boolean stateInit() {
        boolean isStateInit = false;
        try {

            int configMode = getConfigMode();
            long retryWait = plugin.getConfig().getLongParam("retrywait", 3000l);

            //On startup ControllerMode.PRE_INIT is default, but SM is empty
            //If ControllerMode.PRE_INIT then we are on startup
            //else we are restarting so bypass full init
            if(cstate.getControllerState() == ControllerMode.PRE_INIT) {
                //Start with PRE_INIT
                stateContext.setCurrentState(getStateByEnum(ControllerMode.PRE_INIT)); //1
                cstate.setPreInit(); //2
                Map<String, String> preInitMap = preInit(); //2

                //STANDALONE_INIT
                stateContext.setCurrentState(getStateByEnum(ControllerMode.STANDALONE_INIT)); //1
                cstate.setStandaloneInit(preInitMap.get("region_id"), preInitMap.get("agent_id"), "stateInit()"); //2
                standAloneInit(); // 3
            }

            switch (configMode) {

                case 0:
                    logger.info("dynamic : 0");
                    break;

                case 1:
                    logger.info("is_standalone : 1");
                    isStateInit = isStandAlone();
                    break;

                case 2:
                    logger.info("is_agent : 2 : STATE : " + getCurrentState());

                    //setting certificate manager if does not exist
                    if(controllerEngine.getCertificateManager() == null) {
                        controllerEngine.setCertificateManager(new CertificateManager(controllerEngine));
                    }
                    //stay in loop until something happens
                    while((cstate.getControllerState() != ControllerMode.AGENT) && (!forceShutdown.get())) {

                        //try and discover REGIONS that will accept agents
                        List<DiscoveryNode> discoveryNodeList = nodeDiscovery(DiscoveryType.AGENT);
                        DiscoveryNode discoveryNode = getDiscoveryNode(discoveryNodeList);
                        if(discoveryNode != null) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.AGENT_INIT)); //1
                            cstate.setAgentInit(discoveryNode.discovered_region, cstate.getAgent(), "stateInit() : Case 2"); //2
                            //try and do a static connection, if successful set agent
                            isStateInit = isAgent(discoveryNode);
                        } else {
                            logger.error("No agents discovered.");
                        }
                        Thread.sleep(retryWait);
                    }
                    break;

                case 6:
                    logger.info("is_agent /w regional_controller_host : 6 : STATE : " + getCurrentState());

                    //setting certificate manager if does not exist
                    if(controllerEngine.getCertificateManager() == null) {
                        controllerEngine.setCertificateManager(new CertificateManager(controllerEngine));
                    }

                    while((cstate.getControllerState() != ControllerMode.AGENT) && (!forceShutdown.get())) {

                        //manually create the discoveryNode when host is specified
                        DiscoveryNode discoveryNode = null;
                        String discoveryIP = plugin.getConfig().getStringParam("regional_controller_host");
                        int discoveryPort = plugin.getConfig().getIntegerParam("regional_controller_port",32005);
                        try {
                            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
                            List<DiscoveryNode> certDiscovery = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), discoveryIP, discoveryPort, false);
                            discoveryNode = certDiscovery.get(0);
                        } catch (Exception ex) {
                            logger.error("Discovery Failed: " + ex.getMessage());
                            logger.debug(getStringFromError(ex));
                        }

                        if(discoveryNode != null) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.AGENT_INIT)); //1
                            cstate.setAgentInit(discoveryNode.discovered_region, cstate.getAgent(), "stateInit() : Case 2"); //2
                            //try and do a static connection, if successful set agent
                            isStateInit = isAgent(discoveryNode);
                        } else {
                            logger.error("No agents discovered.");
                        }
                        Thread.sleep(retryWait);
                    }
                    break;

                case 8:
                    logger.info("is_region : 8 cstate.getControllerState() == " + cstate.getControllerState());

                    //if this is startup bring up region, skip otherwise
                    if(cstate.getControllerState() == ControllerMode.STANDALONE_INIT) {
                        //first bring up regional services
                        while (cstate.getControllerState() != ControllerMode.REGION) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_INIT)); //1
                            cstate.setRegionInit(cstate.getRegion(), cstate.getAgent(), "regionInit() : Case 8"); //2
                            regionInit();
                        }
                    }
                    //next try and connect to global controller

                    while((cstate.getControllerState() != ControllerMode.REGION_GLOBAL) && (!forceShutdown.get())) {
                        List<DiscoveryNode> discoveryNodeList = nodeDiscovery(DiscoveryType.GLOBAL);
                        DiscoveryNode discoveryNode = getDiscoveryNode(discoveryNodeList);
                        if(discoveryNode != null) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_GLOBAL_INIT)); //1
                            cstate.setRegionGlobalInit("regionInit() : Case 8"); //2
                            isStateInit = isRegionGlobal(discoveryNode);
                        } else {
                            logger.error("No agents discovered.");
                        }
                        Thread.sleep(retryWait);
                    }
                    //discovery engine
                    if(!startNetDiscoveryEngine()) {
                        logger.error("Start Network Discovery Engine Failed!");
                    }
                    break;

                case 24:
                    logger.info("is_region /w global_controller_host : 24");

                    //if this is startup bring up region, skip otherwise
                    if(cstate.getControllerState() == ControllerMode.STANDALONE_INIT) {
                        //first bring up regional services
                        while (cstate.getControllerState() != ControllerMode.REGION) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_INIT)); //1
                            cstate.setRegionInit(cstate.getRegion(), cstate.getAgent(), "regionInit() : Case 24"); //2
                            regionInit();
                        }
                    }
                    //next try and connect to global controller

                    while((cstate.getControllerState() != ControllerMode.REGION_GLOBAL) && (!forceShutdown.get())) {

                        DiscoveryNode discoveryNode = exchangeKeyWithBroker(DiscoveryType.GLOBAL, plugin.getConfig().getStringParam("global_controller_host"), 32005);

                        if(discoveryNode != null) {
                            stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_GLOBAL_INIT)); //1
                            cstate.setRegionGlobalInit("regionInit() : Case 8"); //2
                            isStateInit = isRegionGlobal(discoveryNode);
                        } else {
                            logger.error("No agents discovered.");
                        }
                        Thread.sleep(retryWait);
                    }
                    //discovery engine
                    if(!startNetDiscoveryEngine()) {
                        logger.error("Start Network Discovery Engine Failed!");
                    }
                    break;

                case 32:
                    logger.info("is_global : 32");

                    //first bring up regional services
                    while(cstate.getControllerState() != ControllerMode.REGION) {
                        stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_INIT)); //1
                        cstate.setRegionInit(cstate.getRegion(), cstate.getAgent(), "regionInit() : Case 32"); //2
                        regionInit();
                    }

                    while(cstate.getControllerState() != ControllerMode.GLOBAL) {
                        stateContext.setCurrentState(getStateByEnum(ControllerMode.GLOBAL_INIT)); //1
                        cstate.setGlobalInit("globalInit() : Case 32"); //2
                        isStateInit = globalInit();
                    }

                    //discovery engine
                    if(!startNetDiscoveryEngine()) {
                        logger.error("Start Network Discovery Engine Failed!");
                    }

                    break;


                default:
                    //System.out.println("CONTROLLER ROUTE CASE " + routePath + " " + rm.getParams());
                    logger.error("UNKNOWN CONFIG MODE " + configMode);
                    break;
            }

            if(!forceShutdown.get()) {
            //enable this here to avoid nu,, perfControllerMonitor on global controller start
                if(plugin.getConfig().getBooleanParam("enable_controllermon",true)) {
                    //enable measurements
                    controllerEngine.setMeasurementEngine(new MeasurementEngine(plugin));
                    logger.info("MeasurementEngine initialized");

                    //send measurement info out
                    controllerEngine.setPerfControllerMonitor(new PerfControllerMonitor(controllerEngine));
                    //don't start this yet, otherwise agents will be listening for all KPIs
                    //perfControllerMonitor.setKpiListener();
                    logger.info("Performance Controller monitoring initialized");

                    //PerfMonitorNet perfMonitorNet = new PerfMonitorNet(controllerEngine);
                    controllerEngine.setPerfMonitorNet(new PerfMonitorNet(controllerEngine));
                    logger.info("Performance Network monitoring initialized");

                    /*
                    PerfMonitorNet perfMonitorNet = new PerfMonitorNet(this);
                    perfMonitorNet.start();
                    logger.info("Performance Network monitoring initialized");
                 */
                }


            }

        } catch (Exception ex) {
            logger.error("statInit() " + ex.getMessage());
            logger.error(getStringFromError(ex));

        }
        return isStateInit;
    }

    private Map<String,String> preInit() {

        Map<String,String> preInitMap = new HashMap<>();
        logger.debug("preInit Called");

        String tmpAgent = plugin.getConfig().getStringParam("agentname");
        String tmpRegion = plugin.getConfig().getStringParam("regionname");

        if(tmpAgent == null) {
            tmpAgent = cstate.getAgent();
            if(tmpAgent == null) {
                tmpAgent = "agent-" + java.util.UUID.randomUUID().toString();
            }
        }

        if(tmpRegion == null) {
            tmpRegion = cstate.getRegion();
            if(tmpRegion == null) {
                tmpRegion = "region-" + java.util.UUID.randomUUID().toString();
            }
        }
        preInitMap.put("region_id",tmpRegion);
        preInitMap.put("agent_id",tmpAgent);
        return preInitMap;
    }

    //prepare for standalone init
    public void standAloneInit() {
        logger.debug("standAloneInit Called");
        //here is where is starts
        //cstate.setStandaloneInit(tmpRegion, tmpAgent,"Core Init");

    }

    //do isStandAlone
    public boolean isStandAlone() {
        logger.debug("isStandAlone Called");
        //here is where is starts
        //cstate.setStandaloneInit(tmpRegion, tmpAgent,"Core Init");
        stateContext.setCurrentState(getStateByEnum(ControllerMode.STANDALONE)); //1
        cstate.setStandaloneSuccess(cstate.getRegion(),cstate.getAgent(),"isStandAlone()");
        return true;
    }

    private boolean isAgent(DiscoveryNode discoveryNode) {
        logger.debug("isAgent Called");

        //connect to a specific regional controller
        boolean isInit = false;
        try {

            logger.debug("trying host: " + discoveryNode.discovered_ip + " port:" + discoveryNode.discovered_port);

            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
            List<DiscoveryNode> certDiscovery = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), discoveryNode.discovered_ip, discoveryNode.discovered_port, true);

            if(certDiscovery.size() == 1) {
                discoveryNode = certDiscovery.get(0);

                String cbrokerAddress = discoveryNode.discovered_ip;
                InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                if (remoteAddress instanceof Inet6Address) {
                    cbrokerAddress = "[" + cbrokerAddress + "]";
                }

                logger.debug("Broker Address: " + cbrokerAddress);
                //this.brokerAddressAgent = cbrokerAddress;

                logger.info("AgentPath=" + cstate.getAgentPath());

                if(initIOChannels(cbrokerAddress)) {

                    logger.info("initIOChannels Success");
                    //agent name not set on core init
                    stateContext.setCurrentState(getStateByEnum(ControllerMode.AGENT)); //1
                    cstate.setAgentSuccess(discoveryNode.discovered_region, discoveryNode.discovered_agent, "Agent() Dynamic Regional Host: " + cbrokerAddress + " connected."); //2
                    if(registerAgent(discoveryNode.discovered_region,cstate.getAgent())) {
                        controllerEngine.setAgentHealthWatcher(new AgentHealthWatcher(controllerEngine));
                        isInit = true;
                    }
                }

                if(!isInit) {
                    stateContext.setCurrentState(getStateByEnum(ControllerMode.AGENT_FAILED)); //1
                    cstate.setAgentFailed("Agent() Dynamic Regional Host: " + cbrokerAddress + " failed.");
                    logger.error("initIOChannels Failed");
                }

            } else {
                logger.error("isAgent() certDiscovery.size() == " + certDiscovery.size() + " should == 1");
            }



        } catch (Exception ex) {
            logger.error("isAgent Error " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return isInit;
    }

    private boolean isAgentShutdown() {

        boolean isShutdown = false;
        try {

            if(controllerEngine.getPluginAdmin() != null){
                controllerEngine.getPluginAdmin().clearDataPlaneLogger();
            } else {
                logger.error("controllerEngine.getPluginAdmin() == null");
            }

            if(controllerEngine.getPerfControllerMonitor() != null) {
                controllerEngine.getPerfControllerMonitor().shutdown();
                controllerEngine.setPerfControllerMonitor(null);
            } else {
                logger.error("controllerEngine.getPerfControllerMonitor() == null");
            }

            if(controllerEngine.getAgentHealthWatcher() != null) {
                controllerEngine.getAgentHealthWatcher().shutdown();
                controllerEngine.setAgentHealthWatcher(null);
            } else {
                logger.error("controllerEngine.getAgentHealthWatcher() == null");
            }

            if(controllerEngine.getActiveClient() != null) {
                controllerEngine.getActiveClient().shutdown();
            } else {
                logger.error("controllerEngine.getActiveClient() == null");
            }

            isShutdown = true;
        } catch (Exception ex) {
            logger.error("isAgentShutdown() Error : " + ex.getMessage());
            logger.error(getStringFromError(ex));
        }
        return isShutdown;
    }

    private boolean regionInit() {
        logger.debug("regionInit Called");
        boolean isInit = false;
        try {
            //cstate.setRegionInit(cstate.getRegion(),cstate.getAgent(),"initRegion() TS :" + System.currentTimeMillis());
            //logger.info("Generated regionid=" + cstate.getRegion());

            //certificate manager for broker connections
            controllerEngine.setCertificateManager(new CertificateManager(controllerEngine));
            //incoming queue for canidate brokers
            controllerEngine.setIncomingCanidateBrokers(new LinkedBlockingQueue<>());
            //to store brokered agents
            controllerEngine.setBrokeredAgents(new ConcurrentHashMap<>());

            logger.info("AgentPath=" + cstate.getAgentPath());

            logger.debug("Broker starting");

            controllerEngine.setBroker(new ActiveBroker(controllerEngine, cstate.getAgentPath()));

            //broker manager
            logger.info("Starting Broker Manager");
            controllerEngine.setActiveBrokerManagerThread(new Thread(new ActiveBrokerManager(controllerEngine)));
            controllerEngine.getActiveBrokerManagerThread().start();

            while (!controllerEngine.isActiveBrokerManagerActive()) {
                logger.info("Waiting on active manager active");
                Thread.sleep(1000);
            }
            logger.info("ActiveBrokerManager Started..");

            String brokerAddress = null;
            if (plugin.isIPv6()) { //set broker address for consumers and producers
                brokerAddress = "[::1]";
            } else {
                brokerAddress = "localhost";
            }

            //DB manager only used for regional and global
            logger.debug("Starting DB Manager");
            logger.debug("Starting Broker Manager");

            controllerEngine.setDBManagerThread(new Thread(new DBManager(controllerEngine, controllerEngine.getGDB().importQueue)));
            controllerEngine.getDBManagerThread().start();

            //started by DBInterfaceImpl
            while (!controllerEngine.isDBManagerActive()) {
                logger.info("isDBManagerActive() = false");
                Thread.sleep(1000);
            }

            if(initIOChannels(brokerAddress)) {
                logger.debug("initIOChannels Success");
                stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION)); //1
                cstate.setRegionSuccess(cstate.getRegion(),cstate.getAgent(), "isRegion() Success");
                isInit = true;
            } else {
                logger.error("initIOChannels Failed");
                stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_FAILED)); //1
                cstate.setRegionFailed("isRegion() initIOChannels Failed");
            }

        } catch (Exception ex) {
            logger.error("isRegion() Error " + ex.getMessage());
            stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_FAILED)); //1
            this.cstate.setRegionFailed("isRegion() Error " + ex.getMessage());
        }

        return isInit;
    }

    private boolean isRegionGlobal(DiscoveryNode discoveryNode) {

        boolean isInit = false;
        logger.debug("isRegionGlobal Called");

        try {
            if(!controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath())) {
                logger.info(discoveryNode.getDiscoveredPath() + " NOT REACHABLE");
                controllerEngine.getIncomingCanidateBrokers().add(discoveryNode);
                logger.info(discoveryNode.getDiscoveredPath() + " submitted canidate broker ip " + discoveryNode.discovered_ip);

                int timeout = 0;
                while ((!controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath())) && (timeout < 10)) {
                    logger.info("Trying to connect to Global Controller : " + discoveryNode.getDiscoveredPath());
                    timeout++;
                    Thread.sleep(1000);
                }
            } else {
                logger.error("HOW IS THIS THING DISCOVERABLE? ");
                logger.error("getDiscoveredPath() " + discoveryNode.getDiscoveredPath());
                logger.error("is Reachable: " + controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath()));
            }

            if(controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath())) {
                isInit = true;
                logger.debug("Regional Global Success");
                //set this must be enabled to let global message go through, it should be on the region side
                controllerEngine.setRegionHealthWatcher(new RegionHealthWatcher(controllerEngine));

                stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_GLOBAL)); //1
                cstate.setRegionalGlobalSuccess(discoveryNode.discovered_region, discoveryNode.discovered_agent, "isRegionGlobal() Success");
            } else {
                logger.debug("Regional Global Failure");
                stateContext.setCurrentState(getStateByEnum(ControllerMode.REGION_GLOBAL_FAILED)); //1
                cstate.setRegionalGlobalFailed("isRegionGlobal() Failed");
            }

        }
        catch(Exception ex) {
            logger.error("connectToGlobal() Error " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return isInit;
    }

    private boolean isRegionalGlobalShutdown() {

        boolean isShutdown = false;
        try {
            stopNetDiscoveryEngine();
            controllerEngine.getRegionHealthWatcher().shutdown();
            controllerEngine.setRegionHealthWatcher(null);
            //controllerEngine.getActiveClient().shutdown();

            unregisterRegion(cstate.getRegion(),cstate.getGlobalRegion());

            isShutdown = true;
        } catch (Exception ex) {
            logger.error("isRegionalGlobalShutdown() Error : " + ex.getMessage());
        }
        return isShutdown;
    }

    private boolean globalInit() {
        //don't discover anything
        boolean isInit = false;
        try {


            if(cstate.isRegionalController()) {

                if(startGlobalSchedulers()) {

                    this.controllerEngine.cstate.setGlobalSuccess("gCheck : Creating Global Host");
                    logger.info("isGlobal: " + this.controllerEngine.cstate.getRegionalRegion() + " Agent: " + this.controllerEngine.cstate.getRegionalAgent());

                    controllerEngine.setRegionHealthWatcher(new RegionHealthWatcher(controllerEngine));
                    controllerEngine.setGlobalHealthWatcher(new GlobalHealthWatcher(controllerEngine));

                    isInit = true;
                    stateContext.setCurrentState(getStateByEnum(ControllerMode.GLOBAL)); //1
                    cstate.setGlobalSuccess("globalSuccess() : Case 32"); //2

                    //measurementEngine.initGlobalMetrics();
                } else {
                    logger.error("globalInit() Unable to start GlobalSchedulers");
                }

            } else {
                logger.error("initGlobal Error : Must be Regional Controller First!");
            }

        } catch (Exception ex) {
            logger.error("initGlobal() Error " + ex.getMessage());
            logger.error(getStringFromError(ex));
        }
        return isInit;
    }

    private boolean startGlobalSchedulers() {
        boolean isStarted = false;
        try {
            //Start Global Controller Services
            logger.info("Initialized Global Application Scheduling");

            ResourceScheduler resourceScheduler = new ResourceScheduler(controllerEngine);
            controllerEngine.setResourceScheduler(resourceScheduler);

            AppScheduler appScheduler = new AppScheduler(controllerEngine);
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

    private DiscoveryNode exchangeKeyWithBroker(DiscoveryType discoveryType, String discoveryIp, int discoveryPort) {

        DiscoveryNode discoveryNode = null;

        try {
            int discoveryTimeOut = 10000;
            if(discoveryType == DiscoveryType.AGENT) {
                discoveryTimeOut = plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000);
            } else if(discoveryType == DiscoveryType.REGION) {
                discoveryTimeOut = plugin.getConfig().getIntegerParam("discovery_static_region_timeout", 10000);
            } else if(discoveryType == DiscoveryType.GLOBAL) {
                discoveryTimeOut = plugin.getConfig().getIntegerParam("discovery_static_global_timeout", 10000);
            }

            logger.debug("trying host: " + discoveryIp + " port:" + discoveryPort);
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
            List<DiscoveryNode> certDiscovery = ds.discover(discoveryType, discoveryTimeOut , discoveryIp, discoveryPort, true);

            if(certDiscovery.size() == 1) {
                discoveryNode = certDiscovery.get(0);

                //set brackets on ipv6
                InetAddress remoteAddress = InetAddress.getByName(discoveryNode.discovered_ip);
                if (remoteAddress instanceof Inet6Address) {
                    discoveryNode.discovered_ip = "[" + discoveryNode.discovered_ip + "]";
                }

            } else {
                logger.error("CERT DISCOVERY != 1 : Size: " + certDiscovery.size());
                return null;
            }

        } catch (Exception ex) {
            logger.error("exchangeKeyWithBroker() " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return discoveryNode;
    }

    private  boolean initIOChannels(String brokerAddress) {
        boolean isInit = false;
        try {

            boolean consumerAgentConnected = false; //loop to catch expections on JMX connect of consumer
            int consumerAgentConnectCount = 0;
            while(!consumerAgentConnected && (consumerAgentConnectCount < 10)) {
                try {
                    //consumer agent
                    int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port",32010);

                    String URI = null;
                    if(isLocalBroker(brokerAddress)) {
                        URI = "vm://localhost";
                        //controllerEngine.getActiveClient().initActiveAgentConsumer(cstate.getAgentPath(), URI);
                        //dataPlaneService = new DataPlaneServiceImpl(controllerEngine,URI);
                        //controllerEngine.setDataPlaneService(dataPlaneService);
                    } else {
                        URI = "failover:(nio+ssl://" + brokerAddress + ":" + discoveryPort + "?verifyHostName=false)?maxReconnectAttempts=5&initialReconnectDelay=" + plugin.getConfig().getStringParam("failover_reconnect_delay","5000") + "&useExponentialBackOff=false";
                        //controllerEngine.getActiveClient().initActiveAgentConsumer(cstate.getAgentPath(), URI);
                        //controllerEngine.getActiveClient().initActiveAgentConsumer(cstate.getAgentPath(), "nio+ssl://" + brokerAddress + ":" + discoveryPort + "?verifyHostName=false");
                        //dataPlaneService = new DataPlaneServiceImpl(controllerEngine,URI);
                        //dataPlaneService = new DataPlaneServiceImpl(controllerEngine,"nio+ssl://" + brokerAddress + ":" + discoveryPort + "?verifyHostName=false");
                        //controllerEngine.setDataPlaneService(dataPlaneService);
                    }

                    if(URI != null) {
                        controllerEngine.getActiveClient().initActiveAgentConsumer(cstate.getAgentPath(), URI);
                        //check to see if there is an dataPlaneService, if so reset connections
                        if(controllerEngine.getDataPlaneService() == null) {
                            dataPlaneService = new DataPlaneServiceImpl(controllerEngine, URI);
                            controllerEngine.setDataPlaneService(dataPlaneService);
                        } else {
                            controllerEngine.getDataPlaneService().updateConnections(URI);
                        }

                    }

                    while (!controllerEngine.getActiveClient().isFaultURIActive()) {
                        logger.info("Waiting on Agent Consumer Startup.");
                        Thread.sleep(1000);
                    }

                    consumerAgentConnected = true;
                    logger.info("Agent ConsumerThread Started..");
                } catch (JMSException jmx) {
                    logger.error("Agent ConsumerThread JMX " + jmx.getMessage());
                    StringWriter errors = new StringWriter();
                    jmx.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
                catch (Exception ex) {
                    logger.error("Agent ConsumerThread " + ex.getMessage());
                    StringWriter errors = new StringWriter();
                    ex.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
                consumerAgentConnectCount++;
            }
            int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port",32010);
            if(isLocalBroker(brokerAddress)) {
                controllerEngine.getActiveClient().initActiveAgentProducer("vm://" + brokerAddress + ":" + discoveryPort);
            } else {
                controllerEngine.getActiveClient().initActiveAgentProducer("failover:(nio+ssl://" + brokerAddress + ":" + discoveryPort + "?verifyHostName=false)?maxReconnectAttempts=5&initialReconnectDelay=" + plugin.getConfig().getStringParam("failover_reconnect_delay","5000") + "&useExponentialBackOff=false");
                //controllerEngine.getActiveClient().initActiveAgentProducer("nio+ssl://" + brokerAddress + ":" + discoveryPort + "?verifyHostName=false");
            }
            logger.info("Agent ProducerThread Started..");
            isInit = true;
        } catch (Exception ex) {
            logger.error("initIOChannels() Error " + ex.getMessage());
        }
        return isInit;
    }

    public boolean isLocalBroker(String brokerAddress) {

        if(brokerAddress != null) {
            return (brokerAddress.equals("[::1]")) || ((brokerAddress.equals("localhost")));
        }
        return false;
    }

    private  List<DiscoveryNode> nodeDiscovery(DiscoveryType discoveryType) {

        logger.info("Starting Dynamic Node Discovery Type: " + discoveryType.name());

        List<DiscoveryNode> discoveryNodeList = null;

        try {
            discoveryNodeList = new ArrayList<>();

            //List<DiscoveryNode> discoveryNodeList = new ArrayList<>();

            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(controllerEngine);
                logger.info("IPv6: " + discoveryType.name() + " Search ...");
                //discoveryNodeList.addAll(dc.getDiscoveryResponse(discoveryType, plugin.getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 2000)));
                logger.info("IPv6: count = {} " + discoveryNodeList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(controllerEngine);
            logger.debug("IPv4: " + discoveryType.name() + " Search ...");
            discoveryNodeList.addAll(dc.getDiscoveryResponse(discoveryType, plugin.getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 2000)));
            logger.debug("IPv4: count = {} " + discoveryNodeList.size());

        } catch (Exception ex) {
            logger.error("nodeDiscovery() Error " + ex.getMessage());
        }

        return discoveryNodeList;
    }

    public boolean startNetDiscoveryEngine() {
        boolean isStarted = false;
        try {
            if(!controllerEngine.isDiscoveryActive()) {
                //discovery engine

                if(controllerEngine.getDiscoveryUDPEngineThread() == null) {
                    logger.info("Starting DiscoveryUDPEngine");
                    controllerEngine.setDiscoveryUDPEngineThread(new Thread(new UDPDiscoveryEngine(controllerEngine)));
                    controllerEngine.getDiscoveryUDPEngineThread().start();
                }

                if(controllerEngine.getDiscoveryTCPEngineThread() == null) {
                    logger.info("Starting DiscoveryTCPEngine");
                    controllerEngine.setDiscoveryTCPEngineThread(new Thread(new TCPDiscoveryEngine(controllerEngine)));
                    controllerEngine.getDiscoveryTCPEngineThread().start();
                }

                while (!controllerEngine.isUDPDiscoveryActive() && !controllerEngine.isTCPDiscoveryActive()) {
                    Thread.sleep(1000);
                }

                controllerEngine.setDiscoveryActive(true);
            }
            isStarted = true;
        } catch(Exception ex) {
            logger.error("startNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStarted;
    }

    public boolean stopNetDiscoveryEngine() {
        boolean isStopped = false;
        try {

            controllerEngine.setDiscoveryActive(false);

            controllerEngine.setUDPDiscoveryActive(false);
            if (controllerEngine.getDiscoveryUDPEngineThread() != null) {
                logger.info("UDP Discovery Engine shutting down");
                UDPDiscoveryEngine.shutdown();
                controllerEngine.getDiscoveryUDPEngineThread().join();
                controllerEngine.setDiscoveryUDPEngineThread(null);
            }

            controllerEngine.setTCPDiscoveryActive(false);
            if (controllerEngine.getDiscoveryTCPEngineThread() != null) {
                logger.info("TCP Discovery Engine shutting down");
                TCPDiscoveryEngine.shutdown();
                controllerEngine.getDiscoveryTCPEngineThread().join();
                controllerEngine.setDiscoveryTCPEngineThread(null);
            }

            isStopped = true;

        } catch(Exception ex) {
            logger.error("stopNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStopped;
    }


    private DiscoveryNode getDiscoveryNode(List<DiscoveryNode> discoveryNodeList) {

        DiscoveryNode discoveryNode = null;

        logger.debug("getDiscoveryNode discoveryNodeList Size = " + discoveryNodeList.size() );

        while(discoveryNodeList.size() != 0) {

            int agent_count = Integer.MAX_VALUE;
            DiscoveryNode discoveryNodeCanidate = null;

            //pick lowest number canidate
            for (DiscoveryNode tmpDiscoveryNode : discoveryNodeList) {
                if (tmpDiscoveryNode.discovered_agent_count < agent_count) {
                    discoveryNodeCanidate = tmpDiscoveryNode;
                }
            }
            if(discoveryNodeCanidate != null) {
                logger.info("Found canidate: " + discoveryNodeCanidate.discovered_ip);
            } else {
                logger.info("No canidate found ");
            }

            //see if we can exchange keys with the canidate host
            discoveryNode = exchangeKeyWithBroker(discoveryNodeCanidate.discovery_type, discoveryNodeCanidate.discovered_ip, discoveryNodeCanidate.discovered_port);
            if(discoveryNode != null) {
                logger.debug("Found node: " + discoveryNode.discovered_ip);
                break;
            } else {
                discoveryNodeList.remove(discoveryNodeCanidate);
            }

        }
        return discoveryNode;
    }

    public String getCurrentState() {
        return stateContext.getCurrentState().getId();
    }

    private String compareStrings(String s1, String s2) {

        String diff = "1";
        if((s1 != null) && (s2 != null)) {
            if(s1.equals(s2)) {
                diff = "0";
            }
        }
        return diff;
    }

    private String getNameModeString(String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        return compareStrings(globalRegion,this.globalRegion) + compareStrings(globalAgent,this.globalAgent) +
                compareStrings(regionalRegion,this.regionalRegion) + compareStrings(regionalAgent,this.regionalAgent) +
                compareStrings(localRegion,this.localRegion) + compareStrings(localAgent,this.localAgent);

    }

    private int getConfigMode() {
        return Integer.parseInt(getConfigModeString(), 2);
    }

    private String getConfigModeString() {

        String stateIdSubString = "000000";

        try {
            //is Standalone
            String iS = "0";

            if (plugin.getConfig().getBooleanParam("is_standalone", false)) {
                iS = "1";
            }

            //isAgent
            String iA = "0";

            if (plugin.getConfig().getBooleanParam("is_agent", false)) {
                iA = "1";
            }

            //is agent has host
            String iAh = "0";
            if (plugin.getConfig().getStringParam("regional_controller_host") != null) {
                iAh = "1";
            }

            //isRegion
            String iR = "0";
            if (plugin.getConfig().getBooleanParam("is_region", false)) {
                iR = "1";
            }

            //is region has host
            String iRh = "0";
            if (plugin.getConfig().getStringParam("global_controller_host") != null) {
                iRh = "1";
            }

            //isRegion
            String iG = "0";
            if (plugin.getConfig().getBooleanParam("is_global", false)) {
                iG = "1";
            }

            stateIdSubString = iG + iRh + iR + iAh + iA + iS;
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return stateIdSubString;

    }

    private String getControllerModeString(ControllerMode controllerMode) {

        String stateSubString = "00000000000000000000";

        if(controllerMode != null) {
            String iPi = "0";
            if (controllerMode == ControllerMode.PRE_INIT) {
                iPi = "1";
            }

            String iSi = "0";
            if (controllerMode == ControllerMode.STANDALONE_INIT) {
                iSi = "1";
            }

            String iS = "0";
            if (controllerMode == ControllerMode.STANDALONE) {
                iS = "1";
            }

            String iSf = "0";
            if (controllerMode == ControllerMode.STANDALONE_FAILED) {
                iSf = "1";
            }

            String iSs = "0";
            if (controllerMode == ControllerMode.STANDALONE_SHUTDOWN) {
                iSs = "1";
            }

            String iAi = "0";
            if (controllerMode == ControllerMode.AGENT_INIT) {
                iAi = "1";
            }

            String iAA = "0";
            if (controllerMode == ControllerMode.AGENT) {
                iAA = "1";
            }

            String iAf = "0";
            if (controllerMode == ControllerMode.AGENT_FAILED) {
                iAf = "1";
            }

            String iAs = "0";
            if (controllerMode == ControllerMode.AGENT_SHUTDOWN) {
                iAs = "1";
            }

            String iRi = "0";
            if (controllerMode == ControllerMode.REGION_INIT) {
                iRi = "1";
            }

            String iRf = "0";
            if (controllerMode == ControllerMode.REGION_FAILED) {
                iRf = "1";
            }

            String iRR = "0";
            if (controllerMode == ControllerMode.REGION) {
                iRR = "1";
            }

            String iRGi = "0";
            if (controllerMode == ControllerMode.REGION_GLOBAL_INIT) {
                iRGi = "1";
            }

            String iRGf = "0";
            if (controllerMode == ControllerMode.REGION_GLOBAL_FAILED) {
                iRGf = "1";
            }

            String iRG = "0";
            if (controllerMode == ControllerMode.REGION_GLOBAL) {
                iRG = "1";
            }

            String iRs = "0";
            if (controllerMode == ControllerMode.REGION_SHUTDOWN) {
                iRs = "1";
            }

            String iGi = "0";
            if (controllerMode == ControllerMode.GLOBAL_INIT) {
                iGi = "1";
            }

            String iGG = "0";
            if (controllerMode == ControllerMode.GLOBAL) {
                iGG = "1";
            }

            String iGf = "0";
            if (controllerMode == ControllerMode.GLOBAL_FAILED) {
                iGf = "1";
            }

            String iGs = "0";
            if (controllerMode == ControllerMode.GLOBAL_SHUTDOWN) {
                iGs = "1";
            }

            stateSubString = iGs + iGf + iGG + iGi + iRs + iRG + iRGf + iRGi + iRR + iRf + iRi + iAs + iAf + iAA + iAi + iSs + iSf + iS + iSi + iPi;
        }

        logger.debug("stateSub: " + stateSubString);
        return stateSubString;
    }

    public void setStateCollection(Collection<org.apache.mina.statemachine.State> stateCollection) {
        this.stateCollection = stateCollection;
    }


    private org.apache.mina.statemachine.State getStateByString(String stateId) {
        org.apache.mina.statemachine.State state = null;

        for(org.apache.mina.statemachine.State tmpState : stateCollection) {
            if(tmpState.getId().equals(stateId)) {
                state = tmpState;
                break;
            }
        }

        return state;
    }

    private org.apache.mina.statemachine.State getStateByEnum(ControllerMode controllerMode) {
        org.apache.mina.statemachine.State state = null;

        for(org.apache.mina.statemachine.State tmpState : stateCollection) {
            if(tmpState.getId().equals(controllerMode.name())) {
                state = tmpState;
                break;
            }
        }

        return state;
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    class stateUpdateTask extends TimerTask {
        public void run() {

                if (controllerEngine.cstate.isActive()) {

                    switch (plugin.getAgentService().getAgentState().getControllerState()) {

                        case STANDALONE:
                            break;
                        case AGENT:

                            try {
                                Map<String, String> exportMap = controllerEngine.getGDB().getDBExport(false, true, true, plugin.getRegion(), plugin.getAgent(), null);

                                MapMessage updateMap = plugin.getAgentService().getDataPlaneService().createMapMessage();
                                updateMap.setString("agentconfigs", exportMap.get("agentconfigs"));
                                updateMap.setString("pluginconfigs", exportMap.get("pluginconfigs"));

                                updateMap.setStringProperty("update_mode", "AGENT");
                                updateMap.setStringProperty("region_id", plugin.getRegion());
                                updateMap.setStringProperty("agent_id", plugin.getAgent());

                                //logger.error("SENDING AGENT UPDATE!!!");
                                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, updateMap, DeliveryMode.PERSISTENT, 8, 0);

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }

                            break;

                        case REGION_GLOBAL:

                            try {
                                Map<String, String> exportMap = controllerEngine.getGDB().getDBExport(true, true, true, plugin.getRegion(), null, null);

                                MapMessage updateMap = plugin.getAgentService().getDataPlaneService().createMapMessage();
                                updateMap.setString("regionconfigs", exportMap.get("regionconfigs"));
                                updateMap.setString("agentconfigs", exportMap.get("agentconfigs"));
                                updateMap.setString("pluginconfigs", exportMap.get("pluginconfigs"));

                                updateMap.setStringProperty("update_mode", "REGION");
                                updateMap.setStringProperty("region_id", plugin.getRegion());

                                //logger.error("SENDING REGIONAL UPDATE!!!");
                                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, updateMap, DeliveryMode.PERSISTENT, 8, 0);

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }

                            break;
                        case GLOBAL:
                            break;

                        default:
                            logger.error("stateUpdateTask() INVALID MODE : " + plugin.getAgentService().getAgentState().getControllerState());
                            break;
                    }

                }
        }
    }

    //persistence functions

    private boolean registerRegion(String localRegion, String globalRegion) {
        boolean isRegistered = false;

        try {

            MsgEvent enableMsg = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
            enableMsg.setParam("action", "region_enable");
            enableMsg.setParam("req-seq", UUID.randomUUID().toString());
            enableMsg.setParam("region_name", localRegion);
            enableMsg.setParam("desc", "to-gc-region");
            enableMsg.setParam("mode","REGION");
            //todo fix export
            /*
            Map<String, String> exportMap = dbe.getDBExport(true, true, false, plugin.getRegion(), plugin.getAgent(), null);

            enableMsg.setCompressedParam("regionconfigs",exportMap.get("regionconfigs"));
            enableMsg.setCompressedParam("agentconfigs",exportMap.get("agentconfigs"));

             */
            MsgEvent re = plugin.sendRPC(enableMsg);

            if (re != null) {

                if (re.paramsContains("is_registered")) {

                    isRegistered = Boolean.parseBoolean(re.getParam("is_registered"));

                }
            }

            if(isRegistered) {
                logger.info("Region: " + localRegion + " registered with Global: " + globalRegion);
            } else {
                logger.error("Region: " + localRegion + " failed to register with Global: " + globalRegion + "!");
            }

        } catch (Exception ex) {
            logger.error("Exception during Agent: " + localRegion + " registration with Region: " + globalRegion + "! " + ex.getMessage());
        }

        return isRegistered;
    }

    private boolean unregisterRegion(String localRegion, String globalRegion) {
        boolean isRegistered = false;

        try {

            MsgEvent disableMsg = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
            disableMsg.setParam("unregister_region_id",plugin.getRegion());
            disableMsg.setParam("desc","to-gc-region");
            disableMsg.setParam("action", "region_disable");



            MsgEvent re = plugin.sendRPC(disableMsg,3000);

            if (re != null) {

                if (re.paramsContains("is_unregistered")) {

                    isRegistered = Boolean.parseBoolean(re.getParam("is_unregistered"));

                }
            }

            if (isRegistered) {
                logger.info("Region: " + localRegion + " unregistered from Global: " + globalRegion);
                isRegistered = true;
            } else {
                logger.error("Region: " + localRegion + " failed to unregister with Global: " + globalRegion + "!");
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Exception during Agent: " + localRegion + " unregistration with Global: " + globalRegion + "! " + ex.getMessage());
        }

        return isRegistered;
    }

    private boolean registerAgent(String localRegion, String localAgent) {
        boolean isRegistered = false;

        try {

            MsgEvent enableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
            //MsgEvent enableMsg = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG,regionalRegion,regionalAgent);
            enableMsg.setParam("action", "agent_enable");
            enableMsg.setParam("req-seq", UUID.randomUUID().toString());
            enableMsg.setParam("region_name", localRegion);
            enableMsg.setParam("agent_name", localAgent);
            enableMsg.setParam("desc", "to-rc-agent");
            enableMsg.setParam("mode","AGENT");

            Map<String, String> exportMap = controllerEngine.getGDB().getDBExport(false, true, true, plugin.getRegion(), plugin.getAgent(), null);
            enableMsg.setCompressedParam("agentconfigs",exportMap.get("agentconfigs"));

            logger.debug("registerAgent() SENDING MESSAGE: " + enableMsg.printHeader() + " " + enableMsg.getParams());

            MsgEvent re = plugin.sendRPC(enableMsg);

            if (re != null) {

                if (re.paramsContains("is_registered")) {

                    isRegistered = Boolean.parseBoolean(re.getParam("is_registered"));
                    logger.debug("ISREG: " + isRegistered);

                } else {
                    logger.error("RETURN DOES NOT CONTAIN IS REGISTERED");
                    logger.error("[" + re.printHeader() + "]");
                    logger.error("[" + re.getParams() + "]");
                }
            } else {
                logger.error("registerAgent : RETURN = NULL");
            }

            if (isRegistered) {
                logger.info("Agent: " + localAgent + " registered with Region: " + localRegion);

            } else {
                logger.error("Agent: " + localAgent + " failed to register with Region: " + localRegion + "!");
            }

        } catch (Exception ex) {
            logger.error("Exception during Agent: " + localAgent + " registration with Region: " + localRegion + "! " + ex.getMessage());
        }

        return isRegistered;
    }

    private boolean unregisterAgent(String localRegion, String localAgent) {
        boolean isRegistered = false;

        try {

            if(controllerEngine.getActiveClient().isFaultURIActive()) {
                MsgEvent disableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
                disableMsg.setParam("unregister_region_id", plugin.getRegion());
                disableMsg.setParam("unregister_agent_id", plugin.getAgent());
                disableMsg.setParam("desc", "to-rc-agent");
                disableMsg.setParam("action", "agent_disable");

                MsgEvent re = plugin.sendRPC(disableMsg, 2000);

                if (re != null) {
                    logger.info("Agent: " + localAgent + " unregistered from Region: " + localRegion);
                    isRegistered = true;
                } else {
                    logger.error("Agent: " + localAgent + " failed to unregister with Region: " + localRegion + "!");
                }
            } else {
                logger.error("Agent: " + localAgent + " failed to unregister with Region: " + localRegion + "! isFaultURIActive()");
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Exception during Agent: " + localAgent + " registration with Region: " + localRegion + "! " + ex.getMessage());
        }

        return isRegistered;
    }

}

