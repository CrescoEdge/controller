package io.cresco.agent.controller.core;

import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.communication.*;
import io.cresco.agent.controller.db.DBManager;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.measurement.MeasurementEngine;
import io.cresco.agent.controller.measurement.PerfControllerMonitor;
import io.cresco.agent.controller.netdiscovery.*;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.data.DataPlaneServiceImpl;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.JMSException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ControllerEngine {


    private PluginBuilder plugin;
    public ControllerState cstate;
    private CLogger logger;

    //manager for all certificates
    private CertificateManager certificateManager;
    private ConcurrentHashMap<String, BrokeredAgent> brokeredAgents;
    private BlockingQueue<MsgEvent> incomingCanidateBrokers;
    private BlockingQueue<MsgEvent> outgoingMessages;
    private Map<String, Long> discoveryMap;


    public AtomicInteger responds = new AtomicInteger(0);

    private boolean ActiveBrokerManagerActive = false;
    private boolean clientDiscoveryActiveIPv4 = false;
    private boolean clientDiscoveryActiveIPv6 = false;
    private boolean DiscoveryActive = false;
    private boolean UDPDiscoveryActive = false;
    private boolean TCPDiscoveryActive = false;
    private boolean DBManagerActive = false;
    private boolean GlobalControllerManagerActive = false;
    private boolean restartOnShutdown = false;

    private String brokerAddressAgent;
    public String brokerUserNameAgent;
    public String brokerPasswordAgent;

    private PerfControllerMonitor perfControllerMonitor;
    private ActiveClient activeClient;
    private DataPlaneService dataPlaneService;
    private ActiveBroker broker;
    private DBInterfaceImpl gdb;
    private AgentHealthWatcher agentHealthWatcher;
    private RegionHealthWatcher regionHealthWatcher;
    private ExecutorService msgInProcessQueue;
    private PluginAdmin pluginAdmin;
    private AgentExecutor executor;
    private MeasurementEngine measurementEngine;
    private MsgRouter msgRouter;

    private AppScheduler appScheduler;
    private ResourceScheduler resourceScheduler;

    private Thread activeBrokerManagerThread;
    private Thread globalControllerManagerThread;
    private Thread discoveryUDPEngineThread;
    private Thread discoveryTCPEngineThread;
    private Thread DBManagerThread;


    public ControllerEngine(ControllerState controllerState, PluginBuilder pluginBuilder, PluginAdmin pluginAdmin, DBInterfaceImpl gdb){

        this.plugin = pluginBuilder;
        this.cstate = controllerState;
        this.logger = pluginBuilder.getLogger(ControllerEngine.class.getName(), CLogger.Level.Info);
        this.msgRouter = new MsgRouter(this);
        this.executor = new AgentExecutor(this);
        this.plugin.setExecutor(this.executor);
        this.pluginAdmin = pluginAdmin;
        this.gdb = gdb;

        this.activeClient = new ActiveClient(this);

        //this.msgInProcessQueue = Executors.newFixedThreadPool(100);
        this.msgInProcessQueue = Executors.newCachedThreadPool();
        //this.msgInProcessQueue = Executors.newSingleThreadExecutor();

        //will wait until active then load plugins
        StaticPluginLoader staticPluginLoader = new StaticPluginLoader(this);
        new Thread(staticPluginLoader).start();

    }

    //setup persistance and populate config
    public Boolean coreInit() {
        boolean isCoreInit = false;
        try {
            //set agent name
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

            cstate.setStandaloneInit(tmpRegion, tmpAgent,"Core Init");

            isCoreInit = true;
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return isCoreInit;
    }

    //primary init
    public Boolean commInit() {

        boolean isRegionalController = false;
        boolean isGlobalController = false;

        boolean isCommInit = true;

        logger.info("Initializing services");

        try {


            this.brokeredAgents = new ConcurrentHashMap<>();
            //this.brokeredAgents = getMeasurementEngine().\
            this.incomingCanidateBrokers = new LinkedBlockingQueue<>();
            this.outgoingMessages = new LinkedBlockingQueue<>();
            this.brokerAddressAgent = null;

            List<MsgEvent> discoveryList = null;

            if(plugin.getConfig().getBooleanParam("is_agent",false)) {

                if(plugin.getConfig().getStringParam("regional_controller_host") == null) {

                    discoveryList = initAgentDiscovery();
                    while((discoveryList == null) && cstate.getControllerState().equals(ControllerState.Mode.AGENT_INIT)) {
                        discoveryList = initAgentDiscovery();
                    }
                    isRegionalController = false;
                    isGlobalController = false;
                } else {
                    //agent with static region
                    discoveryList = initAgentStatic();

                    while((discoveryList == null) && (cstate.getControllerState().equals(ControllerState.Mode.AGENT_INIT) || cstate.getControllerState().equals(ControllerState.Mode.STANDALONE_INIT))) {
                        discoveryList = initAgentStatic();
                        Thread.sleep(1000);
                    }
                    isRegionalController = false;
                    isGlobalController = false;
                }
            } else if(plugin.getConfig().getBooleanParam("is_region",false)) {
                isRegionalController = true;
                isGlobalController = false;

            } else if(plugin.getConfig().getBooleanParam("is_global",false)) {
                //by pass all discovery
                isRegionalController = true;
                isGlobalController = true;
            } else {
                //allow promotion of agent to region if agent connection fails
                discoveryList = initAgentDiscovery();
                if(discoveryList != null) {
                    isRegionalController = false;
                    isGlobalController = false;
                } else {
                    discoveryList = initGlobalDiscovery();
                    if(discoveryList != null) {
                        isRegionalController = true;
                        isGlobalController = false;
                    } else {
                        isRegionalController = true;
                        isGlobalController = true;
                    }
                }
            }

            //if a regional controller setup a broker and attach consumer and producer
            if(isRegionalController) {

                if(initRegion()) {
                    //connect to other regions
                    if(plugin.getConfig().getBooleanParam("regional_discovery",false)) {
                        initRegionToRegion();
                    }
                } else {
                    logger.error("Unable to init Region!");
                    return false;
                }
            } else { //not a region, try and connect to one.
                if(discoveryList == null) {
                 return false;
                } else {
                    if (!initAgent(discoveryList)) {
                        logger.error("Unable to init agent!");
                        return false;
                    }
                }
            }


            this.agentHealthWatcher = new AgentHealthWatcher(this);
            //Setup Regional Watcher
            this.regionHealthWatcher = new RegionHealthWatcher(this);

            //enable this here to avoid nu,, perfControllerMonitor on global controller start
            if(plugin.getConfig().getBooleanParam("enable_controllermon",true)) {
                //enable measurements
                this.measurementEngine = new MeasurementEngine(this);
                logger.info("MeasurementEngine initialized");

                //send measurement info out
                perfControllerMonitor = new PerfControllerMonitor(this);
                //don't start this yet, otherwise agents will be listening for all KPIs
                //perfControllerMonitor.setKpiListener();
                logger.info("Performance Controller monitoring initialized");
            }

            if(isRegionalController){

                //don't enable discovery until regional init is complete
                logger.info("Discovery Engine ");

                //discovery engine
                if(!startNetDiscoveryEngine()) {
                    logger.error("Start Network Discovery Engine Failed!");
                }

                //Setup Global is Needed
                initGlobal();
            }

            //populate controller-specific metrics
            //measurementEngine.initControllerMetrics();

            /*
            PerfControllerMonitor perfControllerMonitor = new PerfControllerMonitor(this);
            perfControllerMonitor.start();
            logger.info("Performance Controller monitoring initialized");


            PerfSysMonitor perfSysMonitor = new PerfSysMonitor(this);
            perfSysMonitor.start();
            logger.info("Performance System monitoring initialized");

            PerfMonitorNet perfMonitorNet = new PerfMonitorNet(this);
            perfMonitorNet.start();
            logger.info("Performance Network monitoring initialized");
            */

            /*
            logger.info("Starting Network Discovery Engine...");
            if(!startNetDiscoveryEngine()) {
                logger.error("Start Network Discovery Engine Failed!");
            }
            */

            logger.info("CSTATE : " + cstate.getControllerState() + " Region:" + cstate.getRegion() + " Agent:" + cstate.getAgent());

        } catch (Exception e) {

            e.printStackTrace();

            logger.error("commInit " + e.getMessage());
            logger.error(getStringFromError(e));
        }
        return isCommInit;
    }

    //Mode Discovery

    private  List<MsgEvent> initAgentDiscovery() {
        //continue regional discovery until regional controller is found
        logger.info("Starting Dynamic Agent Discovery...");
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
            try {
                discoveryList = new ArrayList<>();

                if (plugin.isIPv6()) {
                    DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                    logger.debug("Broker Search (IPv6)...");
                    discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 2000)));
                    logger.debug("IPv6 Broker count = {}" + discoveryList.size());
                }
                DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
                logger.debug("Broker Search (IPv4)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 2000)));
                logger.debug("Broker count = {}" + discoveryList.size());
                if (discoveryList.isEmpty()) {
                    discoveryList = null;
                }
            } catch (Exception ex) {
                logger.error("initAgentDiscovery() Error " + ex.getMessage());
                discoveryList = null;
            }

        return discoveryList;
    }

    private Boolean initAgent(List<MsgEvent> discoveryList) {
        //connect to a specific regional controller
        boolean isInit = false;
        try {
            if(plugin.getConfig().getStringParam("regional_controller_host") != null) {
                while(!isInit) {

                    String tmpRegion = discoveryList.get(0).getParam("dst_region");

                    //Agent Name now set on core init
                    certificateManager = new CertificateManager(this);

                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
                    List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("regional_controller_host"), true);

                    while(certDiscovery.isEmpty()) {
                        Thread.sleep(1000);
                        logger.error("Retry Cert Discovery Host: " + plugin.getConfig().getStringParam("regional_controller_host"));
                        certDiscovery = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("regional_controller_host"), true);
                    }

                    String cbrokerAddress = certDiscovery.get(0).getParam("dst_ip");
                    String cbrokerValidatedAuthenication = certDiscovery.get(0).getParam("validated_authenication");
                    String cRegion = certDiscovery.get(0).getParam("dst_region");
                    String cAgent = certDiscovery.get(0).getParam("dst_agent");

                    if ((cbrokerAddress != null) && (cbrokerValidatedAuthenication != null)) {

                        if((tmpRegion.equals(cRegion)) && (plugin.getConfig().getStringParam("regional_controller_host").equals(cbrokerAddress))) {

                            tmpRegion = certDiscovery.get(0).getParam("dst_region");
                            cstate.setAgentInit(tmpRegion,cstate.getAgent(),"initAgent() Static Regional Host: " + plugin.getConfig().getStringParam("regional_controller_host") + "TS : " + System.currentTimeMillis());

                            String[]tmpAuth = cbrokerValidatedAuthenication.split(",");
                            this.brokerUserNameAgent = tmpAuth[0];
                            this.brokerPasswordAgent = tmpAuth[1];

                            //set broker ip
                            InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                            if (remoteAddress instanceof Inet6Address) {
                                cbrokerAddress = "[" + cbrokerAddress + "]";
                            }

                            this.brokerAddressAgent = cbrokerAddress;


                            if(initIOChannels()) {
                                logger.debug("initIOChannels Success");
                                //agent name not set on core init
                                this.cstate.setAgentSuccess(cRegion,cAgent,"AgentSuccess() Static Regional Host: " + plugin.getConfig().getStringParam("regional_controller_host") + " connected.");
                                isInit = true;
                            } else {
                                this.cstate.setAgentFailed(cRegion,cAgent,"AgentSuccess() Static Regional Host: " + plugin.getConfig().getStringParam("regional_controller_host") + " failed.");
                                logger.error("initIOChannels Failed");
                            }

                            isInit = true;
                            logger.info("Broker IP: " + cbrokerAddress);
                            logger.info("Region: " + cstate.getRegion());
                            logger.info("Agent: " + cstate.getAgent());

                        }
                    }
                }
            }
            //do discovery
            else {

                while(!isInit || discoveryList.isEmpty()) {

                    //determine least loaded broker
                    //need to use additional metrics to determine best fit broker
                    String pcbrokerAddress = null;
                    String pcbrokerValidatedAuthenication = null;

                    String pcRegion = null;

                    int brokerCount = -1;
                    for (MsgEvent bm : discoveryList) {

                        int tmpBrokerCount = Integer.parseInt(bm.getParam("agent_count"));
                        if (brokerCount < tmpBrokerCount) {
                            logger.trace("commInit {}" + bm.getParams().toString());
                            pcbrokerAddress = bm.getParam("dst_ip");
                            pcbrokerValidatedAuthenication = bm.getParam("validated_authenication");
                            pcRegion = bm.getParam("dst_region");
                        }
                    }

                    if ((pcbrokerAddress != null) && (pcbrokerValidatedAuthenication != null)) {

                        //agent name now set on core init
                        cstate.setAgentInit(pcRegion,cstate.getAgent(),"initAgent() : Dynamic Discovery");

                        certificateManager = new CertificateManager(this);

                        TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
                        List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), pcbrokerAddress, true);

                        logger.info("Message: " + certDiscovery.get(0).getParams().toString());
                        String cbrokerAddress = certDiscovery.get(0).getParam("dst_ip");
                        String cbrokerValidatedAuthenication = certDiscovery.get(0).getParam("validated_authenication");
                        String cRegion = certDiscovery.get(0).getParam("dst_region");
                        String cAgent = certDiscovery.get(0).getParam("dst_agent");


                        if ((cbrokerAddress != null) && (cbrokerValidatedAuthenication != null)) {


                            //UDPDiscoveryStatic ds = new UDPDiscoveryStatic(this);
                            //discoveryList.addAll(ds.discover(DiscoveryType.AGENT, agentcontroller.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), agentcontroller.getConfig().getStringParam("regional_controller_host")));

                            //List<MsgEvent> certDiscovery =

                            //set agent broker auth
                            String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
                            this.brokerUserNameAgent = tmpAuth[0];
                            this.brokerPasswordAgent = tmpAuth[1];

                            //set broker ip
                            InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                            if (remoteAddress instanceof Inet6Address) {
                                cbrokerAddress = "[" + cbrokerAddress + "]";
                            }

                            //tmpAgent = plugin.getConfig().getStringParam("agentname", "agent-" + java.util.UUID.randomUUID().toString());

                            this.brokerAddressAgent = cbrokerAddress;

                            logger.info("Assigned regionid=" + cstate.getRegion());
                            logger.debug("AgentPath=" + cstate.getAgentPath());

                            if(initIOChannels()) {
                                logger.debug("initIOChannels Success");
                                //agent name not set on core init
                                this.cstate.setAgentSuccess(cRegion, cstate.getAgent(), "Agent() Dynamic Regional Host: " + cbrokerAddress + " connected.");
                                isInit = true;
                            } else {
                                this.cstate.setAgentFailed(cRegion, cstate.getAgent(), "Agent() Dynamic Regional Host: " + cbrokerAddress + " failed.");
                                logger.error("initIOChannels Failed");
                            }
                        }
                    }

                    /*
                    if (this.plugin.getConfig().getBooleanParam("enable_clientnetdiscovery", true)) {
                        //discovery engine
                        if (!startNetDiscoveryEngine()) {
                            logger.error("Start Network Discovery Engine Failed!");
                        }
                    }
                    */

                    //hold no loop
                    if(!isInit || discoveryList.isEmpty()) {

                        logger.error("isInit Status: " + isInit + " discoveryList.isEmpty() == true");
                        Thread.sleep(1000);
                    }

                }
            }
        } catch (Exception ex) {
            logger.error("initAgent() Error " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return isInit;
    }

    private List<MsgEvent> initAgentStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;

            try {
                discoveryList = new ArrayList<>();
                logger.info("Static Agent Connection to Regional Controller : " + plugin.getConfig().getStringParam("regional_controller_host"));

                TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);

                List<MsgEvent> discoveryListTmp = ds.discover(DiscoveryType.AGENT, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("regional_controller_host"));

                if(discoveryListTmp != null) {
                    discoveryList.addAll(discoveryListTmp);

                    logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                    if (discoveryList.size() == 0) {
                        logger.info("Static Agent Connection to Regional Controller : " + plugin.getConfig().getStringParam("regional_controller_host") + " failed! - Restarting Discovery!");
                        discoveryList = null;
                    }

                } else {
                    discoveryList = null;
                }

            } catch (Exception ex) {
                logger.error("initAgentStatic() Error " + ex.getMessage());
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logger.error(errors.toString());
                discoveryList = null;
            }

        return discoveryList;
    }

    private  List<MsgEvent> initRegionDiscovery() {
        //continue regional discovery until regional controller is found
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_ipv6_region_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_ipv4_region_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initRegionDiscovery() Error " + ex.getMessage());
            discoveryList = null;
        }
        return discoveryList;
    }

    private  Boolean initRegionToRegion() {
        //continue regional discovery until regional controller is found
        boolean isInit = false;
        try {
            List<MsgEvent> discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_ipv6_region_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_ipv4_region_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());


            if (!discoveryList.isEmpty()) {

                for (MsgEvent ime : discoveryList) {
                    this.incomingCanidateBrokers.add(ime);
                    logger.debug("Regional Controller Found: " + ime.getParams());
                }
            }

        } catch (Exception ex) {
            logger.error("initRegionToRegion() Error " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return isInit;
    }

    private  Boolean initIOChannels() {
        boolean isInit = false;
        try {
            boolean consumerAgentConnected = false; //loop to catch expections on JMX connect of consumer
            int consumerAgentConnectCount = 0;
            while(!consumerAgentConnected && (consumerAgentConnectCount < 10)) {
                try {
                    //consumer agent
                    int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port",32010);
                    if(isLocalBroker()) {
                        activeClient.initActiveAgentConsumer(cstate.getAgentPath(), "vm://localhost");
                        dataPlaneService = new DataPlaneServiceImpl(this,"vm://localhost");
                    } else {
                        activeClient.initActiveAgentConsumer(cstate.getAgentPath(), "nio+ssl://" + this.brokerAddressAgent + ":" + discoveryPort + "?verifyHostName=false");
                        dataPlaneService = new DataPlaneServiceImpl(this,"nio+ssl://" + this.brokerAddressAgent + ":" + discoveryPort + "?verifyHostName=false");
                    }

                    while (!activeClient.isFaultURIActive()) {
                        logger.info("Waiting on Agent Consumer Startup.");
                        Thread.sleep(1000);
                    }

                    consumerAgentConnected = true;
                    logger.debug("Agent ConsumerThread Started..");
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
            if(isLocalBroker()) {
                this.activeClient.initActiveAgentProducer("vm://" + this.brokerAddressAgent + ":" + discoveryPort);
            } else {
                this.activeClient.initActiveAgentProducer("nio+ssl://" + this.brokerAddressAgent + ":" + discoveryPort + "?verifyHostName=false");
            }
            logger.debug("Agent ProducerThread Started..");
            isInit = true;
        } catch (Exception ex) {
            logger.error("initIOChannels() Error " + ex.getMessage());
        }
        return isInit;
    }

    private List<MsgEvent> initRegionStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            logger.info("Static Region Connection to Regional Controller : " + plugin.getConfig().getStringParam("regional_controller_host"));
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
            discoveryList.addAll(ds.discover(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout",10000), plugin.getConfig().getStringParam("regional_controller_host")));
            logger.debug("Static Agent Connection count = {}" + discoveryList.size());
            if(discoveryList.size() == 0) {
                logger.info("Static Region Connection to Regional Controller : " + plugin.getConfig().getStringParam("regional_controller_host") + " failed! - Restarting Discovery!");
            }
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initRegionStatic() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private  List<MsgEvent> initGlobalDiscovery() {
        //continue regional discovery until regional controller is found
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initGlobalDiscovery() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private List<MsgEvent> initGlobalStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host"));
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
            discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_global_timeout",10000), plugin.getConfig().getStringParam("global_controller_host")));
            logger.debug("Static Agent Connection count = {}" + discoveryList.size());
            if(discoveryList.size() == 0) {
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host") + " failed! - Restarting Discovery!");
            }
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initGlobalStatic() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private Boolean initGlobal() {
        //don't discover anything
        boolean isInit = false;
        try {


            if(cstate.isRegionalController()) {

                //do global discovery here
                this.globalControllerManagerThread = new Thread(new GlobalHealthWatcher(this));
                this.globalControllerManagerThread.start();

                while (!this.GlobalControllerManagerActive) {
                    Thread.sleep(1000);
                    logger.trace("Wait loop for Global Controller");
                }
                isInit = true;
                //measurementEngine.initGlobalMetrics();
            } else {
                logger.error("initGlobal Error : Must be Regional Controller First!");
            }

        } catch (Exception ex) {
            logger.error("initGlobal() Error " + ex.getMessage());
            logger.error(getStringFromError(ex));
        }
        return isInit;
    }

    private Boolean initRegion() {
        boolean isInit = false;
        try {
            cstate.setRegionInit(cstate.getRegion(),cstate.getAgent(),"initRegion() TS :" + System.currentTimeMillis());
            logger.debug("Generated regionid=" + cstate.getRegion());

            certificateManager = new CertificateManager(this);

            logger.debug("AgentPath=" + cstate.getAgentPath());
            //Start controller services

            //logger.debug("IPv6 UDPDiscoveryEngine Started..");

            logger.debug("Broker starting");
            if((plugin.getConfig().getStringParam("broker_username") != null) && (plugin.getConfig().getStringParam("broker_password") != null)) {
                brokerUserNameAgent = plugin.getConfig().getStringParam("broker_username");
                brokerPasswordAgent = plugin.getConfig().getStringParam("broker_password");
            }
            else {
                brokerUserNameAgent = java.util.UUID.randomUUID().toString();
                brokerPasswordAgent = java.util.UUID.randomUUID().toString();
            }
            this.broker = new ActiveBroker(this, cstate.getAgentPath(),brokerUserNameAgent,brokerPasswordAgent);

            //broker manager
            logger.debug("Starting Broker Manager");
            this.activeBrokerManagerThread = new Thread(new ActiveBrokerManager(this));
            this.activeBrokerManagerThread.start();
                /*synchronized (activeBrokerManagerThread) {
					activeBrokerManagerThread.wait();
				}*/
            while (!this.ActiveBrokerManagerActive) {
                Thread.sleep(1000);
            }
            logger.debug("ActiveBrokerManager Started..");

            if (plugin.isIPv6()) { //set broker address for consumers and producers
                this.brokerAddressAgent = "[::1]";
            } else {
                this.brokerAddressAgent = "localhost";
            }

            //DB manager only used for regional and global
            logger.debug("Starting DB Manager");
            logger.debug("Starting Broker Manager");
            this.DBManagerThread = new Thread(new DBManager(this, this.gdb.importQueue));
            this.DBManagerThread.start();

            //started by DBInterfaceImpl
            while (!this.DBManagerActive) {
                Thread.sleep(1000);
            }

            this.discoveryMap = new ConcurrentHashMap<>(); //discovery map

            if(initIOChannels()) {
                logger.debug("initIOChannels Success");
                cstate.setRegionSuccess(plugin.getRegion(),plugin.getAgent(), "initRegion() Success");
                isInit = true;
            } else {
                logger.error("initIOChannels Failed");
                cstate.setRegionFailed("initRegion() initIOChannels Failed");
            }

        } catch (Exception ex) {
            logger.error("initRegion() Error " + ex.getMessage());
            this.cstate.setRegionFailed("initRegion() Error " + ex.getMessage());
        }

        return isInit;
    }

    //helper functions
    public CertificateManager getCertificateManager() {
        return certificateManager;
    }

    public PluginBuilder getPluginBuilder() {return  plugin; }

    public ConcurrentHashMap<String, BrokeredAgent> getBrokeredAgents() {
        return brokeredAgents;
    }
    public void setBrokeredAgents(ConcurrentHashMap<String, BrokeredAgent> brokeredAgents) {
        this.brokeredAgents = brokeredAgents;
    }
    public boolean isActiveBrokerManagerActive() {
        return ActiveBrokerManagerActive;
    }
    public void setActiveBrokerManagerActive(boolean activeBrokerManagerActive) {
        ActiveBrokerManagerActive = activeBrokerManagerActive;
    }
    public BlockingQueue<MsgEvent> getIncomingCanidateBrokers() {
        return incomingCanidateBrokers;
    }
    public void setIncomingCanidateBrokers(BlockingQueue<MsgEvent> incomingCanidateBrokers) {
        this.incomingCanidateBrokers = incomingCanidateBrokers;
    }
    public ActiveBroker getBroker() {
        return broker;
    }
    public void setBroker(ActiveBroker broker) {
        this.broker = broker;
    }
    public boolean isLocal(String checkAddress) {
        boolean isLocal = false;
        if (checkAddress.contains("%")) {
            String[] checkScope = checkAddress.split("%");
            checkAddress = checkScope[0];
        }
        List<String> localAddressList = localAddresses();
        for (String localAddress : localAddressList) {
            if (localAddress.contains(checkAddress)) {
                isLocal = true;
            }
        }
        return isLocal;
    }
    public List<String> localAddresses() {
        List<String> localAddressList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> inter = NetworkInterface.getNetworkInterfaces();
            while (inter.hasMoreElements()) {
                NetworkInterface networkInter = inter.nextElement();
                for (InterfaceAddress interfaceAddress : networkInter.getInterfaceAddresses()) {
                    String localAddress = interfaceAddress.getAddress().getHostAddress();
                    if (localAddress.contains("%")) {
                        String[] localScope = localAddress.split("%");
                        localAddress = localScope[0];
                    }
                    if (!localAddressList.contains(localAddress)) {
                        localAddressList.add(localAddress);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("localAddresses Error: {}", ex.getMessage());
        }
        return localAddressList;
    }
    public boolean isIPv6() {
        boolean isIPv6 = false;
        try {


            if (plugin.getConfig().getStringParam("isIPv6") != null) {
                isIPv6 = plugin.getConfig().getBooleanParam("isIPv6", false);
            }
            else {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = interfaces.nextElement();
                    if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                        continue; // Don't want to broadcast to the loopback interface
                    }
                    if (networkInterface.supportsMulticast()) {
                        for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                            if ((interfaceAddress.getAddress() instanceof Inet6Address)) {
                                isIPv6 = true;
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("isIPv6 Error: {}", ex.getMessage());
        }
        return isIPv6;
    }

    public boolean isReachableAgent(String remoteAgentPath) {
        boolean isReachableAgent = false;
        if (this.cstate.isRegionalController()) {
            try {
                ActiveMQDestination[] er = this.broker.getBrokerDestinations();
                for (ActiveMQDestination des : er) {

                    if (des.isQueue()) {
                        String testPath = des.getPhysicalName();

                        logger.trace("isReachable isQueue: physical = " + testPath + " qualified = " + des.getQualifiedName());
                        if (testPath.equals(remoteAgentPath)) {
                            isReachableAgent = true;
                        }
                    }
                }

                er = this.broker.getRegionalBrokerDestinations();
                for (ActiveMQDestination des : er) {
                    //for(String despaths : des.getDestinationPaths()) {
                    //    logger.info("isReachable destPaths: " + despaths);
                    //}

                    if (des.isQueue()) {
                        String testPath = des.getPhysicalName();
                        logger.trace("Regional isReachable isQueue: physical = " + testPath + " qualified = " + des.getQualifiedName());
                        if (testPath.equals(remoteAgentPath)) {
                            isReachableAgent = true;
                        }
                    }
                }

            } catch (Exception ex) {
                logger.error("isReachableAgent Error: {}", ex.getMessage());
            }
        } else {
            isReachableAgent = true; //send all messages to regional controller if not broker
        }
        return isReachableAgent;
    }

    public List<String> reachableAgents() {
        List<String> rAgents = null;
        try {
            rAgents = new ArrayList<>();
            if (this.cstate.isRegionalController()) {
                ActiveMQDestination[] er = this.broker.getBrokerDestinations();
                for (ActiveMQDestination des : er) {
                    if (des.isQueue()) {
                        rAgents.add(des.getPhysicalName());
                    }
                }
            } else {
                rAgents.add(cstate.getRegion()); //just return regional controller
            }
        } catch (Exception ex) {
            logger.error("isReachableAgent Error: {}", ex.getMessage());
        }
        return rAgents;
    }

    public boolean isClientDiscoveryActiveIPv4() {
        return clientDiscoveryActiveIPv4;
    }
    public void setClientDiscoveryActiveIPv4(boolean clientDiscoveryActiveIPv4) {
        this.clientDiscoveryActiveIPv4 = clientDiscoveryActiveIPv4;
    }
    public boolean isClientDiscoveryActiveIPv6() {
        return clientDiscoveryActiveIPv6;
    }
    public void setClientDiscoveryActiveIPv6(boolean clientDiscoveryActiveIPv6) {
        this.clientDiscoveryActiveIPv6 = clientDiscoveryActiveIPv6;
    }

    public boolean isUDPDiscoveryActive() {
        return UDPDiscoveryActive;
    }
    public void setTCPDiscoveryActive(boolean discoveryActive) {
        TCPDiscoveryActive = discoveryActive;
    }
    public boolean isTCPDiscoveryActive() {
        return TCPDiscoveryActive;
    }
    public void setUDPDiscoveryActive(boolean discoveryActive) {
        UDPDiscoveryActive = discoveryActive;
    }
    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
    public AppScheduler getAppScheduler() {
        return appScheduler;
    }
    public void setAppScheduler(AppScheduler appScheduler) {
        this.appScheduler = appScheduler;
    }
    public ResourceScheduler getResourceScheduler() {
        return resourceScheduler;
    }
    public void setResourceScheduler(ResourceScheduler resourceScheduler) {
        this.resourceScheduler = resourceScheduler;
    }

    public DBInterfaceImpl getGDB() {
        return gdb;
    }
    public void setGDB(DBInterfaceImpl gdb) {
        this.gdb = gdb;
    }

    public boolean isDBManagerActive() {
        return DBManagerActive;
    }
    public void setDBManagerActive(boolean DBManagerActive) {
        this.DBManagerActive = DBManagerActive;
    }

    public boolean isGlobalControllerManagerActive() {
        return GlobalControllerManagerActive;
    }
    public void setGlobalControllerManagerActive(boolean activeBrokerManagerActive) {
        GlobalControllerManagerActive = activeBrokerManagerActive;
    }

    public Map<String, Long> getDiscoveryMap() {
        return discoveryMap;
    }

    public boolean isDiscoveryActive() {
        return DiscoveryActive;
    }

    public ActiveClient getActiveClient() { return activeClient; }

    public Thread getActiveBrokerManagerThread() {
        return activeBrokerManagerThread;
    }

    public void setActiveBrokerManagerThread(Thread activeBrokerManagerThread) {
        this.activeBrokerManagerThread = activeBrokerManagerThread;
    }

    public void removeGDBNode(String region, String agent, String pluginID) {
        if (this.gdb != null)
            this.gdb.removeNode(region, agent, pluginID);
    }

    public DataPlaneService getDataPlaneService() { return  dataPlaneService; }

    public PerfControllerMonitor getPerfControllerMonitor() { return  perfControllerMonitor; }

    public void setRestartOnShutdown(boolean restartOnShutdown) {
        this.restartOnShutdown = restartOnShutdown;
    }

    public void closeCommunications() {
        try {
            if (this.restartOnShutdown)
                logger.info("Tearing down services");
            else
                logger.info("Shutting down");

            this.DiscoveryActive = false;

            if(this.perfControllerMonitor != null) {
                this.perfControllerMonitor.stop();
            }

            if(this.measurementEngine != null) {
                this.measurementEngine.shutdown();
            }

            if(!stopNetDiscoveryEngine()) {
                logger.error("Failed to stop Network Discovery Engine");
            }

            this.GlobalControllerManagerActive = false;
            if (this.globalControllerManagerThread!= null) {
                logger.trace("Global HealthWatcher shutting down");
                this.regionHealthWatcher.communicationsHealthTimer.cancel();
                this.regionHealthWatcher.regionalUpdateTimer.cancel();
                this.globalControllerManagerThread.join();
                this.globalControllerManagerThread = null;
                logger.info("Global HealthWatcher shutting down");

            }

            if (this.regionHealthWatcher != null) {
                logger.trace("Region HealthWatcher shutting down");
                //in case its not canceled as part of global
                this.regionHealthWatcher.communicationsHealthTimer.cancel();
                this.regionHealthWatcher.regionalUpdateTimer.cancel();
                this.regionHealthWatcher = null;
                logger.info("Region HealthWatcher shutting down");

            }

            if(this.agentHealthWatcher != null) {
                logger.trace("Agent HealthWatcher shutting down");
                this.agentHealthWatcher.shutdown(true);
                this.agentHealthWatcher = null;
                logger.info("Agent HealthWatcher shutting down");
            }

            if(this.activeClient != null) {
                this.activeClient.shutdown();
            }


            this.ActiveBrokerManagerActive = false;

            //this.getIncomingCanidateBrokers().offer(null);
            if (this.activeBrokerManagerThread != null) {
                logger.trace("Active Broker Manager shutting down");
                this.activeBrokerManagerThread.interrupt();
                this.activeBrokerManagerThread.join();
                this.activeBrokerManagerThread = null;
                logger.info("Active Broker Manager shutting down");
            }


            if (this.broker != null) {
                logger.trace("Broker shutting down");
                this.broker.stopBroker();
                this.broker = null;
                logger.info("Broker shutting down");

            }

            //disable
            this.DBManagerActive = false;
            logger.info("DB shutting down");


            if (this.restartOnShutdown) {
                while(!commInit()) {
                    Thread.sleep(1000);
                }
                this.restartOnShutdown = false;
                logger.info("Communications Reset Complete");
            } else {
                logger.info("Communications Shutdown Complete");
            }

        } catch (Exception ex) {
            logger.error("shutdown {}", ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

    }

    public boolean stopNetDiscoveryEngine() {
        boolean isStopped = false;
        try {
            if (this.discoveryUDPEngineThread != null) {
                logger.trace("UDP Discovery Engine shutting down");
                UDPDiscoveryEngine.shutdown();
                this.discoveryUDPEngineThread.join();
                this.discoveryUDPEngineThread = null;
                this.DiscoveryActive = false;
            }


            if (this.discoveryTCPEngineThread != null) {
                logger.trace("TCP Discovery Engine shutting down");
                TCPDiscoveryEngine.shutdown();
                this.discoveryTCPEngineThread.join();
                this.discoveryTCPEngineThread = null;
                this.DiscoveryActive = false;
            }

            isStopped = true;
        } catch(Exception ex) {
            logger.error("stopNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStopped;
    }

    public boolean startNetDiscoveryEngine() {
        boolean isStarted = false;
        try {
            if(!this.DiscoveryActive) {
                //discovery engine
                this.discoveryUDPEngineThread = new Thread(new UDPDiscoveryEngine(this));
                this.discoveryUDPEngineThread.start();

                this.discoveryTCPEngineThread = new Thread(new TCPDiscoveryEngine(this));
                this.discoveryTCPEngineThread.start();

                while (!this.UDPDiscoveryActive && !this.TCPDiscoveryActive) {
                    Thread.sleep(1000);
                }

                this.DiscoveryActive = true;
            }
            isStarted = true;
        } catch(Exception ex) {
            logger.error("startNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStarted;
    }

    public boolean isLocalBroker() {

        if(this.brokerAddressAgent != null) {
            return (this.brokerAddressAgent.equals("[::1]")) || ((this.brokerAddressAgent.equals("localhost")));
        }
        return false;
    }

    public RegionHealthWatcher getRegionHealthWatcher() {return this.regionHealthWatcher;}

    public void msgIn(MsgEvent msg) {

        try {
            while (!getActiveClient().isFaultURIActive()) {
                Thread.sleep(1000);
                logger.error("STUCK IN CONNECTION FAULT!!!");
                logger.error("[" + msg.getParams() + "]");
            }
            msgRouter.route(msg);
        } catch (Exception ex) {
        logger.error(ex.getMessage());
        ex.printStackTrace();
        }


    }

    public void msgInThreaded(MsgEvent msg) {
        msgInProcessQueue.submit(new MsgEventRunner(this, msg));
    }

    public PluginAdmin getPluginAdmin() { return pluginAdmin; }

    public MeasurementEngine getMeasurementEngine() {
        return measurementEngine;
    }
}
