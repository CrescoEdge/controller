package io.cresco.agent.controller.core;

import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.communication.*;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.measurement.PerfControllerMonitor;
import io.cresco.agent.controller.netdiscovery.*;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.controller.statemachine.ControllerSM;
import io.cresco.agent.controller.statemachine.ControllerSMHandler;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.mina.statemachine.StateMachine;
import org.apache.mina.statemachine.StateMachineFactory;
import org.apache.mina.statemachine.StateMachineProxyBuilder;
import org.apache.mina.statemachine.annotation.Transition;

import java.net.Inet6Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ControllerEngine {

    private PluginBuilder plugin;
    public ControllerStateImp cstate;
    private CLogger logger;

    //manager for all certificates
    private CertificateManager certificateManager;
    private ConcurrentHashMap<String, BrokeredAgent> brokeredAgents;
    private BlockingQueue<DiscoveryNode> incomingCanidateBrokers;

    public AtomicInteger responds = new AtomicInteger(0);

    private AtomicBoolean ActiveBrokerManagerActive = new AtomicBoolean(false);
    private boolean clientDiscoveryActiveIPv4 = false;
    private boolean clientDiscoveryActiveIPv6 = false;
    private boolean DiscoveryActive = false;
    private boolean UDPDiscoveryActive = false;
    private boolean TCPDiscoveryActive = false;
    private boolean DBManagerActive = false;

    private PerfControllerMonitor perfControllerMonitor;
    private ActiveClient activeClient;
    private DataPlaneService dataPlaneService;
    private ActiveBroker broker;
    private DBInterfaceImpl gdb;
    private AgentHealthWatcher agentHealthWatcher;
    private RegionHealthWatcher regionHealthWatcher;
    private GlobalHealthWatcher globalHealthWatcher;
    private ExecutorService msgInProcessQueue;
    private PluginAdmin pluginAdmin;
    private AgentExecutor executor;
    private MeasurementEngine measurementEngine;
    private MsgRouter msgRouter;
    private ControllerSM controllerSM;

    private AppScheduler appScheduler;
    private ResourceScheduler resourceScheduler;

    private Thread activeBrokerManagerThread;
    //private Thread globalControllerManagerThread;
    private Thread discoveryUDPEngineThread;
    private Thread discoveryTCPEngineThread;
    private Thread DBManagerThread;

    private ControllerSMHandler controllerSMHandler;


    public ControllerEngine(ControllerStateImp cstate, PluginBuilder pluginBuilder, PluginAdmin pluginAdmin, DBInterfaceImpl gdb){

        this.plugin = pluginBuilder;
        this.cstate = cstate;
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

    public boolean start() {
        boolean isStarted = false;
        try {

            controllerSMHandler = new ControllerSMHandler(this);
            StateMachine sm = StateMachineFactory.getInstance(Transition.class).create(ControllerSMHandler.EMPTY, controllerSMHandler);
            //provide the state list back to the handler

            controllerSMHandler.setStateCollection(sm.getStates());
            //start the state machine
            controllerSM = new StateMachineProxyBuilder().setClassLoader(ControllerSM.class.getClassLoader()).create(ControllerSM.class, sm);

            controllerSM.start();

            isStarted = true;
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return isStarted;
    }

    public boolean stop() {
        boolean isStopped = false;
        try {

            logger.info("Stopping all plugins.");
            pluginAdmin.stopAllPlugins();

            logger.info("Setting controller to stopped state");
            if(controllerSM != null) {
                //if in STANDALONE_INIT startup was interupted in the middle of a state transition, must force interupt
                controllerSMHandler.shutdown();

                controllerSM.stop();
            }
            if(plugin != null) {
                plugin.setIsActive(false);
            }

            isStopped = true;
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return isStopped;
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
                break;
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

    //getters and setters

    //helper functions

    public ControllerSM getControllerSM() {
        return this.controllerSM;
    }

    public CertificateManager getCertificateManager() {
        return certificateManager;
    }
    public void setCertificateManager(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    public PluginBuilder getPluginBuilder() {return  plugin; }

    public ConcurrentHashMap<String, BrokeredAgent> getBrokeredAgents() {
        return brokeredAgents;
    }
    public void setBrokeredAgents(ConcurrentHashMap<String, BrokeredAgent> brokeredAgents) {
        this.brokeredAgents = brokeredAgents;
    }
    public boolean isActiveBrokerManagerActive() {

        return ActiveBrokerManagerActive.get();

    }
    public void setActiveBrokerManagerActive(boolean activeBrokerManagerActiveState) {
        ActiveBrokerManagerActive.set(activeBrokerManagerActiveState);
    }
    public BlockingQueue<DiscoveryNode> getIncomingCanidateBrokers() {
        return incomingCanidateBrokers;
    }
    public void setIncomingCanidateBrokers(BlockingQueue<DiscoveryNode> incomingCanidateBrokers) {
        this.incomingCanidateBrokers = incomingCanidateBrokers;
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

    public void setDBManagerThread(Thread DBManagerThread) {
        this.DBManagerThread = DBManagerThread;
    }

    public Thread getDBManagerThread() {
        return DBManagerThread;
    }

    public boolean isDiscoveryActive() {
        return this.DiscoveryActive;
    }

    public void setDiscoveryActive(boolean DiscoveryActive) {
        this.DiscoveryActive = DiscoveryActive;
    }

    public ActiveClient getActiveClient() { return activeClient; }

    public Thread getActiveBrokerManagerThread() {
        return activeBrokerManagerThread;
    }

    public void setActiveBrokerManagerThread(Thread activeBrokerManagerThread) {
        this.activeBrokerManagerThread = activeBrokerManagerThread;
    }

    public DataPlaneService getDataPlaneService() { return  this.dataPlaneService; }

    public void setDataPlaneService(DataPlaneService dataPlaneService) {
        this.dataPlaneService = dataPlaneService;
    }

    public PerfControllerMonitor getPerfControllerMonitor() { return  perfControllerMonitor; }

    public void setPerfControllerMonitor(PerfControllerMonitor perfControllerMonitor) {
        this.perfControllerMonitor = perfControllerMonitor;
    }

    public Thread getDiscoveryUDPEngineThread() {
        return discoveryUDPEngineThread;
    }
    public void setDiscoveryUDPEngineThread(Thread discoveryUDPEngineThread) {
        this.discoveryUDPEngineThread = discoveryUDPEngineThread;
    }
    public Thread getDiscoveryTCPEngineThread() {
        return discoveryTCPEngineThread;
    }
    public void setDiscoveryTCPEngineThread(Thread discoveryTCPEngineThread) {
        this.discoveryTCPEngineThread = discoveryTCPEngineThread;
    }

    public AgentHealthWatcher getAgentHealthWatcher() {return this.agentHealthWatcher;}
    public void setAgentHealthWatcher(AgentHealthWatcher agentHealthWatcher) {
        this.agentHealthWatcher = agentHealthWatcher;
    }

    public RegionHealthWatcher getRegionHealthWatcher() {return this.regionHealthWatcher;}
    public void setRegionHealthWatcher(RegionHealthWatcher regionHealthWatcher) {
        this.regionHealthWatcher = regionHealthWatcher;
    }

    public GlobalHealthWatcher getGlobalHealthWatcher() {return this.globalHealthWatcher;}
    public void setGlobalHealthWatcher(GlobalHealthWatcher globalHealthWatcher) {
        this.globalHealthWatcher = globalHealthWatcher;
    }

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
        return this.measurementEngine;
    }
    public void setMeasurementEngine(MeasurementEngine measurementEngine) {
        this.measurementEngine = measurementEngine;
    }


}
