package io.cresco.agent.controller.core;

import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.communication.*;
import io.cresco.agent.controller.health.CrescoHealthExecutor;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.measurement.PerfControllerMonitor;
import io.cresco.agent.controller.measurement.PerfMonitorNet;
import io.cresco.agent.controller.netdiscovery.*;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.controller.statemachine.ControllerSM;
import io.cresco.agent.controller.statemachine.ControllerSMHandler;
import io.cresco.agent.controller.statemachine.StateResync;
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

    private PerfControllerMonitor perfControllerMonitor;
    private PerfMonitorNet perfMonitorNet;
    private ActiveClient activeClient;
    private DataPlaneService dataPlaneService;
    private ActiveBroker broker;
    private DBInterfaceImpl gdb;
    private AgentHealthWatcher agentHealthWatcher;
    private RegionHealthWatcher regionHealthWatcher;
    private GlobalHealthWatcher globalHealthWatcher;
    // The regional controller's message executor. One per engine, created on first use and never replaced:
    // the RegionHealthWatcher (timers) is nulled and rebuilt across state-machine re-inits, but the
    // router must reach the regional executor throughout (see MsgRouter.forwardToLocalRegionalController).
    private volatile io.cresco.agent.controller.regionalcontroller.RegionalExecutor regionalExecutor;
    private final Object regionalExecutorLock = new Object();
    private ExecutorService msgInProcessQueue;
    private PluginAdmin pluginAdmin;
    private AgentExecutor executor;
    private MeasurementEngine measurementEngine;
    // Network link measurement + automated tuning (measurement subsystem + controller control loop).
    private io.cresco.agent.controller.netmetrics.LinkMetricsRegistry linkMetricsRegistry;
    private io.cresco.agent.controller.netmetrics.NetTuningProfile netTuningProfile;
    private io.cresco.agent.controller.netmetrics.AutoTuner autoTuner;
    private MsgRouter msgRouter;
    private ControllerSM controllerSM;

    private AppScheduler appScheduler;
    private ResourceScheduler resourceScheduler;

    private Thread activeBrokerManagerThread;
    //private Thread globalControllerManagerThread;
    private Thread discoveryUDPEngineThread;
    private Thread discoveryTCPEngineThread;

    private ControllerSMHandler controllerSMHandler;
    private volatile StateResync stateResync;

    private CrescoHealthExecutor healthExecutor;

    private io.cresco.agent.controller.health.MeshHealth meshHealth;


    public ControllerEngine(ControllerStateImp cstate, PluginBuilder pluginBuilder, PluginAdmin pluginAdmin, DBInterfaceImpl gdb){

        this.plugin = pluginBuilder;
        this.cstate = cstate;
        if (cstate != null) cstate.setRegionControllerCheck(this::speaksForRegion);
        this.logger = pluginBuilder.getLogger(ControllerEngine.class.getName(), CLogger.Level.Info);
        this.msgCounts = new io.cresco.agent.controller.communication.MsgCounts(
                pluginBuilder.getLogger(io.cresco.agent.controller.communication.MsgCounts.class.getName(), CLogger.Level.Info));
        this.routeGate = new io.cresco.agent.controller.communication.RouteGate(pluginBuilder.getConfig());
        CLogger routeLog = pluginBuilder.getLogger(io.cresco.agent.controller.communication.RouteGate.class.getName(), CLogger.Level.Info);
        this.routeGate.setWarn(routeLog::warn);
        CLogger expiryLog = pluginBuilder.getLogger(io.cresco.agent.controller.communication.ExpiredMessages.class.getName(), CLogger.Level.Info);
        this.expiredMessages.setWarn(expiryLog::warn);
        this.msgRouter = new MsgRouter(this);
        this.executor = new AgentExecutor(this);
        this.plugin.setExecutor(this.executor);
        // who may call each agent-tier action (docs/caller-authorization.md section 3); undeclared actions fail closed
        io.cresco.agent.controller.communication.ControllerActionRoles.agentTier(this.plugin.getAuthorizer());
        this.pluginAdmin = pluginAdmin;
        this.gdb = gdb;

        this.activeClient = new ActiveClient(this);

        //bounded pool: a cached (unbounded) pool means thread-per-message under broker churn,
        //which exhausts threads/FDs on constrained edge hosts; CallerRuns pushes backpressure
        //onto the delivery thread instead of dropping or growing without limit
        int msgInMaxThreads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        ThreadPoolExecutor msgInPool = new ThreadPoolExecutor(msgInMaxThreads, msgInMaxThreads,
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(4096),
                new ThreadPoolExecutor.CallerRunsPolicy());
        msgInPool.allowCoreThreadTimeOut(true);
        this.msgInProcessQueue = msgInPool;

        //will wait until active then load plugins
        StaticPluginLoader staticPluginLoader = new StaticPluginLoader(this);
        new Thread(staticPluginLoader).start();

    }

    private final io.cresco.agent.controller.communication.MsgCounts msgCounts;

    /**
     * What became of the control-plane messages this node had in hand (originated, relayed, delivered, held, dropped by
     * cause); with {@code msgSigning().refusedCount()}, every message is accounted for. In the metric inventory as
     * {@code msg.*}.
     */
    public io.cresco.agent.controller.communication.MsgCounts msgCounts() { return msgCounts; }

    private volatile io.cresco.agent.controller.communication.PeerIdentity.Expected joiningGlobal, lastGlobal;
    private volatile boolean knewAGlobal;

    /**
     * This region begins to join the global {@code region/agent} (found in discovery; its bridge is about to be built).
     * From now this node has known a global ({@link #knewAGlobal}), and its broker takes a bridge connection that verifies
     * as that controller for its parent's ({@link #parentWhileNotJoined}).
     */
    public void joiningGlobal(String region, String agent) {
        if (region == null || agent == null) return;
        this.joiningGlobal = new io.cresco.agent.controller.communication.PeerIdentity.Expected(region, agent);
        this.knewAGlobal = true;
    }

    /**
     * The join attempt begun with {@link #joiningGlobal} ended. {@code joined}: the controller state names the global now,
     * and it is remembered as the one joined last. Not joined: this region is joining nobody.
     */
    public void joinEnded(boolean joined) {
        io.cresco.agent.controller.communication.PeerIdentity.Expected g = joiningGlobal;
        if (joined && g != null) lastGlobal = g;
        joiningGlobal = null;
    }

    /**
     * The global of this region while its controller state names none: the one it is joining now, else the one it joined
     * last (and lost: it is trying to join it again); null when neither. The broker grants it nothing by name -- only a
     * bridge connection whose certificate verified as that controller is its parent's (CrescoAuthorizationBroker.isParent).
     */
    public io.cresco.agent.controller.communication.PeerIdentity.Expected parentWhileNotJoined() {
        io.cresco.agent.controller.communication.PeerIdentity.Expected g = joiningGlobal;
        return g != null ? g : lastGlobal;
    }

    /** Has this node joined a global, or begun to join one, since it started? Never false again once true. */
    public boolean knewAGlobal() { return knewAGlobal; }

    private final io.cresco.agent.controller.communication.ExpiredMessages expiredMessages = new io.cresco.agent.controller.communication.ExpiredMessages();

    /** The messages this node's broker let expire: counted (msg.expired) and said, whichever broker the node runs now. */
    public io.cresco.agent.controller.communication.ExpiredMessages expiredMessages() { return expiredMessages; }

    private final io.cresco.agent.controller.communication.RouteGate routeGate;

    /**
     * What this node's broker can reach, and the RPC requests held for a route that is being made anew: a request for a
     * node the broker has no way to is answered {@code route unavailable} instead of waiting in a queue (MsgRouter).
     */
    public io.cresco.agent.controller.communication.RouteGate routeGate() { return routeGate; }

    /**
     * Send {@code msg} on toward its forward destination, as the router does: an RPC request this node's broker has no
     * route for is held or answered (false: it was not sent, and has been accounted for). For the senders beside the
     * router (the regional executor's send to the global).
     */
    public boolean routeKnown(MsgEvent msg, String hop, boolean offWire) {
        return msgRouter.routeKnown(msg, hop, offWire);
    }

    /** Set when {@link #start} refused this node's security configuration (it must not run at all). */
    private volatile boolean refusedByConfiguration;

    public boolean isRefusedByConfiguration() { return refusedByConfiguration; }

    public boolean start() {
        boolean isStarted = false;
        try {
            // secure by default: inconsistent security settings stop the node here, before anything listens, every
            // problem named; a pre-provisioned node whose identity cannot be used does not start at all (never a
            // generated or enrolled fallback)
            java.util.List<String> refusals = new java.util.ArrayList<>();
            String provisioning = io.cresco.agent.controller.communication.CertificateManager.provisioningRefusal(plugin.getConfig());
            if (provisioning != null) refusals.add("the pre-provisioned identity cannot be used: " + provisioning);
            refusals.addAll(io.cresco.agent.controller.communication.SecuritySettings.startupRefusals(plugin.getConfig()));
            // production runs in FIPS 140-3 approved-only mode: checked here, never assumed (providers, the module's
            // approved-only mode and self-tests, a non-approved algorithm refused, an approved one's known answer)
            if (io.cresco.agent.controller.communication.SecuritySettings.production(plugin.getConfig())) {
                String fips = io.cresco.agent.controller.communication.FipsMode.refusal();
                if (fips != null) {
                    refusals.add("security_profile=production runs in FIPS 140-3 approved-only mode, and this JVM is not in it: " + fips
                            + " (start the node with the agent jar, whose crypto boot sets it up; docs/production-deployment.md, section 6)");
                } else {
                    logger.info("FIPS 140-3 approved-only mode verified (self-tests passed, MD5 refused): " + io.cresco.agent.controller.communication.FipsMode.summary());
                }
            }
            if (!refusals.isEmpty()) {
                for (String r : refusals) logger.error("REFUSING TO START: " + r);
                refusedByConfiguration = true;
                return false;
            }
            for (String w : io.cresco.agent.controller.communication.SecuritySettings.bootWarnings(plugin.getConfig(), System.currentTimeMillis())) {
                // an open upgrade window admits unauthenticated legacy nodes: said as loudly as an error, at every boot
                if (w.startsWith("UPGRADE WINDOW OPEN")) logger.error("WARNING: " + w);
                else logger.warn("WARNING: " + w);
            }
            for (String n : io.cresco.agent.controller.communication.SecuritySettings.bootNotices(plugin.getConfig())) logger.info(n);

            controllerSMHandler = new ControllerSMHandler(this);
            StateMachine sm = StateMachineFactory.getInstance(Transition.class).create(ControllerSMHandler.EMPTY, controllerSMHandler);
            //provide the state list back to the handler

            controllerSMHandler.setStateCollection(sm.getStates());
            //start the state machine
            controllerSM = new StateMachineProxyBuilder().setClassLoader(ControllerSM.class.getClassLoader()).create(ControllerSM.class, sm);

            controllerSM.start();

            // Start the health executor: discovers HealthCheck services and runs them with grace/
            // sticky/cache. Guarded so a missing Felix HC api bundle cannot block controller start.
            try {
                this.meshHealth = new io.cresco.agent.controller.health.MeshHealth();
                this.healthExecutor = new CrescoHealthExecutor(plugin.getBundleContext(), plugin);
                this.healthExecutor.start();
                io.cresco.agent.controller.health.LocalHealthChecks.register(plugin.getBundleContext(), this);
                io.cresco.agent.controller.health.LinkHealthChecks.register(plugin.getBundleContext(), this);
                // mesh rollup: children advertise health on the ping, this node rolls it up (subtree check).
                io.cresco.agent.controller.health.MeshHealthChecks.register(plugin.getBundleContext(), this);
                // HC->MINA bridge: a grace-protected link:parent CRITICAL fires the MINA loss event.
                this.healthExecutor.addListener(new io.cresco.agent.controller.health.HealthMinaBridge(this));
            } catch (Throwable t) {
                logger.error("Health executor failed to start: {}", String.valueOf(t));
            }

            schedulePluginDataSweep();
            isStarted = true;
        } catch (Exception ex) {
            logger.error("ControllerEngine.start ", ex);
        }
        return isStarted;
    }

    private volatile java.util.concurrent.ScheduledExecutorService pluginDataSweeper;

    /**
     * plugin_data_retention_days (default 0: never): once a day, delete the data of plugins no longer configured here
     * after that many days without a change (PluginAdmin.sweepStalePluginData). Never on a stop.
     */
    private void schedulePluginDataSweep() {
        long days = plugin.getConfig().getLongParam("plugin_data_retention_days", 0L);
        if (days <= 0 || pluginAdmin == null) return;
        java.util.concurrent.ScheduledExecutorService s = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cresco-plugin-data-sweep");
            t.setDaemon(true);
            return t;
        });
        s.scheduleWithFixedDelay(() -> {
            try { pluginAdmin.sweepStalePluginData(days); } catch (Exception ex) { logger.error("plugin-data sweep: " + ex.getMessage()); }
        }, 1, 24, TimeUnit.HOURS);
        pluginDataSweeper = s;
    }

    public boolean stop() {
        java.util.concurrent.ScheduledExecutorService sweeper = pluginDataSweeper;
        if (sweeper != null) sweeper.shutdownNow();
        boolean isStopped = false;
        try {
            // first, while this node can still send and sign: the requests it holds for a route are answered (nothing will
            // send them on), and none is held from now
            routeGate.stop();

            if (healthExecutor != null) {
                healthExecutor.shutdown();
            }

            logger.info("Stopping all plugins.");
            pluginAdmin.stopAllPlugins();
            // release the ConfigurationAdmin/CoreState service trackers
            pluginAdmin.close();

            logger.info("Setting controller to stopped state");
            if(controllerSM != null) {
                //if in STANDALONE_INIT startup was interupted in the middle of a state transition, must force interupt
                controllerSMHandler.shutdown();

                controllerSM.stop();
            }

            if(gdb != null) {
                gdb.shutdown();
            }

            if (msgSigning != null) msgSigning.close();
            if (activeClient != null) activeClient.stopHolding();     // what waited for a producer is counted, its timer stopped

            if(plugin != null) {
                plugin.setIsActive(false);
            }

            try {
                msgInProcessQueue.shutdown();
                if (!msgInProcessQueue.awaitTermination(5, TimeUnit.SECONDS)) {
                    notRouted(msgInProcessQueue.shutdownNow());
                }
            } catch (InterruptedException ie) {
                notRouted(msgInProcessQueue.shutdownNow());
                Thread.currentThread().interrupt();
            }

            //disable logging
            //pluginAdmin.setRootLogLevel(CLogger.Level.None);

            isStopped = true;
        } catch (Exception ex) {
            logger.error("ControllerEngine.stop ", ex);
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

    /*
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

     */
    /**
     * Whether messages for {@code remoteAgentPath} currently have somewhere to go from this node.
     *
     * <p>On a regional controller this means a LIVE route on the local broker: a queue named after the path
     * that has a consumer -- the remote node's own inbox, or the network bridge's demand subscription for a
     * node behind a bridge (the global, a peer region). The destination merely EXISTING is not enough: it
     * outlives the bridge that created it (and our own sends to the path re-create it), so after a global
     * restart the old check answered "reachable" for a dead bridge, {@code isRegionGlobal} skipped submitting
     * the global as a broker candidate, no bridge was rebuilt, and the region never re-attached (every PING
     * unanswered, the global aged it to LOST).
     */
    public boolean isReachableAgent(String remoteAgentPath) {
        if (!this.cstate.isRegionalController()) {
            return true; //send all messages to regional controller if not broker
        }
        try {
            if (this.broker == null) return false;
            // the queue this node's messages for that agent go to: with tenant namespacing, T.<this node's tenant>.<path>
            // (AgentProducer qualifies by the message's tenant); without it the bare path
            String queue = io.cresco.agent.controller.communication.SecuritySettings.tenantNamespacing(plugin.getConfig())
                    ? io.cresco.library.security.TenantNamespace.qualify(tenant(), remoteAgentPath) : remoteAgentPath;
            return this.broker.hasLiveQueueRoute(queue);
        } catch (Exception ex) {
            logger.error("isReachableAgent Error: {}", ex.getMessage());
            return false;
        }
    }

    /**
     * Whether a state export published now reaches the parent's state-update listener. A regional controller
     * under a global publishes on its OWN broker and depends on the data bridge having the global's
     * subscription; an agent publishes straight onto its parent's broker, where the listener lives, so for it
     * (and when this cannot be determined) the answer is true.
     */
    public boolean isParentExportRouteLive() {
        try {
            if (cstate.getControllerState() != io.cresco.library.agent.ControllerMode.REGION_GLOBAL || broker == null) {
                return true;
            }
            Object dps = plugin.getAgentService().getDataPlaneService();
            if (!(dps instanceof io.cresco.agent.data.DataPlaneServiceImpl)) {
                return true;
            }
            String topic = ((io.cresco.agent.data.DataPlaneServiceImpl) dps).getTopicPhysicalName(io.cresco.library.data.TopicType.AGENT);
            return broker.hasNetworkSubscription(topic);
        } catch (Exception ex) {
            logger.error("isParentExportRouteLive Error: {}", ex.getMessage());
            return true;
        }
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
    /**
     * Re-enroll with this node's regional controller: one authenticated discovery exchange with the controller it joined,
     * which issues a fresh (short-lived) leaf for the same key. False when there is none to ask or it did not succeed.
     */
    public boolean reEnroll() {
        io.cresco.agent.controller.statemachine.ControllerSMHandler h = controllerSMHandler;
        return h != null && h.reEnroll();
    }

    public void setCertificateManager(CertificateManager certificateManager) {
        CertificateManager previous = this.certificateManager;
        this.certificateManager = certificateManager;
        if (previous != null && previous != certificateManager) previous.close();   // its renewal check; the registry is shared
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

    public void setPerfMonitorNet(PerfMonitorNet perfMonitorNet) {
        this.perfMonitorNet = perfMonitorNet;
    }
    public PerfMonitorNet getPerfMonitorNet() { return  perfMonitorNet; }


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

    /**
     * Ask the state machine to export this node's (diff-gated) config upstream now rather than on the
     * next watchdog tick; used after an explicit plugin add/remove. No-op before start / when there is
     * no parent (GLOBAL/REGION/STANDALONE share or own the DB the inventory is read from).
     */
    public void requestStateUpdate() {
        ControllerSMHandler handler = controllerSMHandler;
        if (handler != null) {
            handler.requestStateUpdate();
        }
    }

    /**
     * A pong from this node's parent: when it asks for our full state (the parent holds us as not ACTIVE, see
     * StateResync), request one full state export -- at most once per state_resync_min_interval_ms.
     */
    public void onParentPong(MsgEvent pong) {
        // a policy change at the global reaches this node on its parent's pong (CryptoPolicySync)
        io.cresco.agent.controller.statemachine.CryptoPolicySync.adopt(pong, cstate,
                "parent " + pong.getSrcRegion() + "/" + pong.getSrcAgent(), logger);
        // the parent's clock: a skew past the certificate backdate locks one side out of the other's certificates (P4)
        try {
            String ts = pong.getParam("remote_ts");
            if (ts != null && getCertificateManager() != null) {
                getCertificateManager().noteClock(Long.parseLong(ts), "parent " + pong.getSrcRegion() + "/" + pong.getSrcAgent());
            }
        } catch (Exception ignore) { }
        boolean region = cstate.isRegionalController();
        String parentRegion = region ? cstate.getGlobalRegion() : cstate.getRegionalRegion();
        String parentAgent = region ? cstate.getGlobalAgent() : cstate.getRegionalAgent();
        boolean readyBefore = trustReady();
        // revocations first: the parent's signed list, verified as the controller this node joined (the bundle that comes
        // with it is then judged with the list as it now stands)
        io.cresco.agent.controller.communication.RevocationSync.onParentPong(this, pong, parentRegion, parentAgent);
        // message signatures: this node's attestation (an agent) and the parent's trust bundle
        msgSigning().onParentPong(pong, parentRegion, parentAgent);
        // the last part of the parent's revocation list came with this pong: what the inbox held for it is judged now
        if (!readyBefore && trustReady()) msgSigning().trustDataArrived();
        StateResync resync = stateResync;
        if (resync == null) {
            resync = initStateResync();
        }
        if (!resync.shouldResync(pong, System.currentTimeMillis())) {
            return;
        }
        ControllerSMHandler handler = controllerSMHandler;
        if (handler != null) {
            handler.requestFullStateUpdate("parent " + pong.getSrcRegion() + "_" + pong.getSrcAgent()
                    + " holds this node as status " + pong.getParam(StateResync.STATUS_PARAM));
        }
    }

    private synchronized StateResync initStateResync() {
        if (stateResync == null) {
            stateResync = new StateResync(plugin.getConfig().getLongParam("state_resync_min_interval_ms", 10000L));
        }
        return stateResync;
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

    /** This node's regional-controller executor: created on first use, never null, never replaced. */
    public io.cresco.agent.controller.regionalcontroller.RegionalExecutor getRegionalExecutor() {
        io.cresco.agent.controller.regionalcontroller.RegionalExecutor re = regionalExecutor;
        if (re == null) {
            synchronized (regionalExecutorLock) {
                re = regionalExecutor;
                if (re == null) {
                    re = new io.cresco.agent.controller.regionalcontroller.RegionalExecutor(this);
                    regionalExecutor = re;
                }
            }
        }
        return re;
    }
    public void setGlobalHealthWatcher(GlobalHealthWatcher globalHealthWatcher) {
        this.globalHealthWatcher = globalHealthWatcher;
    }

    // A message that arrives while the parent-broker link is down (fault URI inactive) used to
    // spin here FOREVER, one ERROR line per second per waiting thread ("STUCK IN CONNECTION
    // FAULT"), pinning every plugin thread that tried to talk during a parent loss. Bounded now:
    // hold the message up to msgin_fault_wait_ms for the link to come back (a normal re-init
    // completes well inside that), then drop it with ONE error line -- an RPC caller sees a
    // timeout and retries, exactly as for any other transient loss. Messages addressed to this
    // agent itself never need the parent link and are routed immediately.
    public void msgIn(MsgEvent msg) {
        msgIn(msg, false);
    }

    /**
     * As {@link #msgIn(MsgEvent)}; {@code offWire}: this node took {@code msg} off its inbox (another node originated it),
     * so when it goes on toward another node it is counted as relayed.
     */
    public void msgIn(MsgEvent msg, boolean offWire) {

        boolean routed = false;             // handed to the router, which accounts for it from there
        try {
            if (!getActiveClient().isFaultURIActive() && !isLocalDestination(msg)) {
                long waitMs = plugin.getConfig().getLongParam("msgin_fault_wait_ms", 20000L);
                long deadline = System.currentTimeMillis() + waitMs;
                boolean logged = false;
                while (!getActiveClient().isFaultURIActive()) {
                    if (System.currentTimeMillis() >= deadline) {
                        logger.error("msgIn: parent link in connection fault for " + waitMs + "ms, dropping message [" + io.cresco.agent.controller.communication.ParamLog.describe(msg) + "]");
                        msgCounts.dropped(io.cresco.agent.controller.communication.MsgCounts.Drop.PARENT_LINK_DOWN, msg);
                        return;
                    }
                    if (!logged) {
                        logged = true;
                        logger.warn("msgIn: parent link in connection fault, holding message up to " + waitMs + "ms [type=" + msg.getMsgType() + " dst=" + msg.getDstRegion() + "_" + msg.getDstAgent() + "]");
                    }
                    Thread.sleep(250);
                }
            }
            routed = true;
            msgRouter.route(msg, offWire);
        } catch (InterruptedException gaveUp) {
            // The sender stopped waiting for the parent link (its own timeout cancelled the send, or the node is
            // stopping). That is the link's fault, not a routing error: one WARN line, no stack trace, the cause the
            // message would have had at the end of the wait, and the thread's interrupt kept for the caller.
            Thread.currentThread().interrupt();
            logger.warn("msgIn: parent link in connection fault, the sender stopped waiting; dropping message [" + io.cresco.agent.controller.communication.ParamLog.describe(msg) + "]");
            if (!routed) msgCounts.dropped(io.cresco.agent.controller.communication.MsgCounts.Drop.PARENT_LINK_DOWN, msg);
        } catch (Exception ex) {
        logger.error("ControllerEngine.msgIn ", ex);
            // it never reached the router (the check before it failed)
            if (!routed) msgCounts.dropped(io.cresco.agent.controller.communication.MsgCounts.Drop.ROUTE_ERROR, msg);
        }
    }

    private boolean isLocalDestination(MsgEvent msg) {
        try {
            return cstate != null && msg.getDstRegion() != null && msg.getDstAgent() != null
                    && msg.getDstRegion().equals(cstate.getRegion()) && msg.getDstAgent().equals(cstate.getAgent());
        } catch (Exception ex) {
            return false;
        }
    }

    /** Route a message this node's inbox took off the wire, on the routing pool. */
    public void msgInThreaded(MsgEvent msg) {
        // a pool that was shut down takes nothing and says nothing (its CallerRunsPolicy discards then): counted here
        if (msgInProcessQueue.isShutdown()) {
            msgCounts.dropped(io.cresco.agent.controller.communication.MsgCounts.Drop.NODE_STOPPING, msg);
            return;
        }
        msgInProcessQueue.submit(new MsgEventRunner(this, msg));
    }

    /** Messages still queued for routing when this node stopped: they are not routed. */
    private void notRouted(java.util.List<Runnable> queued) {
        if (queued != null && !queued.isEmpty()) {
            msgCounts.dropped(io.cresco.agent.controller.communication.MsgCounts.Drop.NODE_STOPPING, queued.size(), "queued for routing");
        }
    }

    // ------------------------------------------------------------------ verified callers (docs/caller-authorization.md)

    private volatile io.cresco.library.security.Caller selfCaller;
    private volatile String selfCallerKey;
    private volatile long lastStampProblemLog;

    /** {@code broker_security_enabled}: callers are verified (otherwise every caller is {@code anonymous}). */
    public boolean securityOn() {
        return io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(plugin.getConfig());
    }

    /** This node's tenant. */
    public String tenant() {
        return io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
    }

    /**
     * This controller as a caller: {@code controller:<tenant>/<region>/<agent>} with {@code infra}; {@code anonymous} when
     * security is off; null while the node has no (valid) name yet -- its messages then go out unstamped and are refused.
     */
    public io.cresco.library.security.Caller selfCaller() {
        if (!securityOn()) return io.cresco.library.security.Caller.anonymous();
        String region = cstate == null ? null : cstate.getRegion();
        String agent = cstate == null ? null : cstate.getAgent();
        String tenant = tenant();
        String key = tenant + "/" + region + "/" + agent;
        io.cresco.library.security.Caller c = selfCaller;
        if (c != null && key.equals(selfCallerKey)) return c;
        try {
            c = io.cresco.library.security.Caller.controller(tenant, region, agent);
        } catch (IllegalArgumentException invalid) {
            stampProblem("this node's name " + key + " is not a valid caller (" + invalid.getMessage() + "): its messages go unstamped");
            return null;
        }
        selfCaller = c;
        selfCallerKey = key;
        return c;
    }

    /** A plugin of this node as a caller ({@code plugin:<tenant>/<region>/<agent>/<pluginid>}); see {@link #selfCaller}. */
    public io.cresco.library.security.Caller pluginCaller(String pluginId) {
        if (!securityOn()) return io.cresco.library.security.Caller.anonymous();
        try {
            return io.cresco.library.security.Caller.plugin(tenant(), cstate.getRegion(), cstate.getAgent(), pluginId);
        } catch (RuntimeException invalid) {
            stampProblem("plugin id " + pluginId + " is not a valid caller (" + invalid.getMessage() + "): its messages go unstamped");
            return null;
        }
    }

    /** Strip whatever {@code msg} says about its caller and stamp this controller (with this node's region alias, if any). */
    public void stampSelf(MsgEvent msg) {
        stampAs(msg, selfCaller(), regionAlias());
    }

    /**
     * A stamping point: whatever {@code msg} says about its caller is replaced by {@code caller} (null: it goes unstamped;
     * {@code alias}: the former region a node caller also answers at), and the message is this node's from now on -- counted
     * as originated, given the namespace it travels in, and signed when it leaves the node.
     */
    public void stampAs(MsgEvent msg, io.cresco.library.security.Caller caller, String alias) {
        Travel cfg = travel();
        // on a superuser tenant's node: whose word the message carried before this node's replaces it -- the verified
        // caller of the request it answers (read as it stands: nothing is parsed)
        String answered = cfg.superuserNode && msg != null && msg.getParams() != null
                ? msg.getParam(io.cresco.library.security.Caller.PARAM_TENANT) : null;
        io.cresco.agent.controller.communication.CallerStamps.stamp(msg, caller, alias);
        originated(msg, caller, answered, cfg);
    }

    private volatile io.cresco.agent.controller.communication.MsgSigning msgSigning;

    /** End-to-end MsgEvent signatures (D4): this node signs what it originates and checks what it dispatches. */
    public io.cresco.agent.controller.communication.MsgSigning msgSigning() {
        io.cresco.agent.controller.communication.MsgSigning s = msgSigning;
        if (s == null) {
            synchronized (this) {
                if (msgSigning == null) msgSigning = new io.cresco.agent.controller.communication.MsgSigning(this);
                s = msgSigning;
            }
        }
        return s;
    }

    /** The message signing state once made (null before its first use): what a revocation flushes. */
    public io.cresco.agent.controller.communication.MsgSigning msgSigningIfCreated() {
        return msgSigning;
    }

    /**
     * D4: before {@code msg} is dispatched on this node (to its controller, its agent executor or a plugin), a message off the
     * wire this node did not verify on arrival -- it was not addressed here, or was only to be carried on -- is verified now
     * (MsgSigning.verifyIfDeferred), whatever re-addressed it on the way. False when it is refused: an RPC request is
     * answered 401, anything else is dropped.
     */
    public boolean admitDeferred(MsgEvent msg) {
        io.cresco.agent.controller.communication.MsgSigning s = msgSigning();
        io.cresco.agent.controller.communication.MsgSigning.Verdict v = s.verifyIfDeferred(msg, legacyNodesAdmitted());
        if (v == null) return true;
        if (v.ok()) return admitArrival(msg, v.caller);        // verified only now: the tenant rule of an arrival, now too
        s.answerRefused(msg, v);                               // the 401; it names a session this node does not hold
        return false;
    }

    /**
     * Does this node hold what it needs to judge the signers of the messages it receives? A node with a parent (an agent's
     * region controller, a region's global) does once it has taken that parent's first pong, which brings the trust
     * bundle, an agent's attestation and the parent's revocations -- and, when that list is longer than one pong carries,
     * the pong with its last part (a key the parent revoked is otherwise accepted here until it arrives); before, it can
     * verify nobody but that parent. A global, a node with no parent joined, and a node without message signing are
     * always ready.
     */
    public boolean trustReady() {
        io.cresco.agent.core.ControllerStateImp cs = cstate;
        if (cs == null || !securityOn() || !msgSigning().required() || cs.isGlobalController()) return true;
        boolean region = cs.isRegionalController();
        String pr = region ? cs.getGlobalRegion() : cs.getRegionalRegion(), pa = region ? cs.getGlobalAgent() : cs.getRegionalAgent();
        return pr == null || pa == null || (msgSigning().heardParent(pr, pa)
                && !io.cresco.agent.controller.communication.RevocationSync.awaitingParentList(this));
    }

    /** A message this node originates was just stamped: sign it when it leaves the node (MsgSigning). */
    public void markForSigning(MsgEvent msg) {
        originated(msg, io.cresco.library.security.Caller.fromMsgEvent(msg), null, travel());
    }

    /** What deciding a message's namespace reads from the configuration, read once a second at most (not per message). */
    private static final class Travel {
        final boolean on, superuserNode;
        final String tenant;
        final java.util.Set<String> superusers;
        final long at;
        Travel(boolean on, String tenant, java.util.Set<String> superusers, long at) {
            this.on = on; this.tenant = tenant; this.superusers = superusers; this.at = at;
            this.superuserNode = on && tenant != null && superusers.contains(tenant);
        }
    }

    private volatile Travel travel;

    private Travel travel() {
        Travel t = travel;
        long now = System.currentTimeMillis();
        if (t == null || now - t.at >= 1000L || now < t.at) {
            travel = t = new Travel(securityOn() && io.cresco.agent.controller.communication.SecuritySettings.tenantNamespacing(plugin.getConfig()),
                    tenant(), io.cresco.agent.controller.communication.SecuritySettings.superuserTenants(plugin.getConfig()), now);
        }
        return t;
    }

    /**
     * {@code msg} was just stamped as {@code caller}'s: it is this node's from now on (every stamping point passes here).
     * The namespace it travels in ({@code cresco_tenant}) is set before it is signed: its caller's tenant; for a superuser
     * tenant's caller the tenant of the caller it answers ({@code answeredTenant}), else the stamp it carries, else the
     * tenant of the node it is addressed to (MsgRouter.travelTenant). Nothing without security or tenant namespacing, or
     * for a message with no caller.
     */
    private void originated(MsgEvent msg, io.cresco.library.security.Caller caller, String answeredTenant, Travel cfg) {
        msgCounts.originated();
        if (cfg.on && msg != null && msg.getParams() != null && caller != null && !caller.isAnonymous()) {
            String carried = msg.getParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM);
            String t = io.cresco.agent.controller.communication.MsgRouter.travelTenant(caller, answeredTenant, carried, cfg.tenant, cfg.superusers);
            if (t == null) t = destinationTenant(msg, cfg);
            if (t != null && !t.equals(carried)) msg.setParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM, t);
        }
        msgSigning().markForSigning(msg, cfg.tenant);
    }

    /**
     * The tenant whose namespace reaches the node {@code msg} is addressed to, as this node knows it first hand: its own for
     * its own region; the tenant of that region's controllers that completed discovery with this node
     * (PeerTrustRegistry.tenantOfRegion); else the tenant bound here to that very node's name (a node this node bound
     * itself, at its former region too). When none of these says -- the region is unknown here, or controllers of several
     * tenants are recorded for it -- the message goes in this node's own namespace: counted and logged
     * (MsgCounts.tenantUnresolved), because a tenant's leaf does not read it there.
     */
    private String destinationTenant(MsgEvent msg, Travel cfg) {
        String region = msg.getDstRegion();
        if (region == null || region.isEmpty() || (cstate != null && region.equals(cstate.getRegion()))) return cfg.tenant;
        CertificateManager cm = getCertificateManager();
        io.cresco.agent.controller.communication.PeerTrustRegistry peers = cm == null ? null : cm.getPeerTrust();
        String t = peers == null ? null : peers.tenantOfRegion(region);
        if (t == null && peers != null && msg.getDstAgent() != null) t = peers.boundTenant(region, msg.getDstAgent());
        if (t != null) return t;
        msgCounts.tenantUnresolved(region, peers == null ? null : peers.tenantsRecordedFor(region));
        return cfg.tenant;
    }

    /**
     * Is the verified caller of {@code msg} of a tenant that is not a superuser's, asking for a node that this node does
     * not know to be of the caller's own tenant? Such a caller is told nothing about that node's state by how a request
     * for it fails (MsgRouter: one answer and one timing whether the node is gone, was never there, or cannot be told).
     * The node's tenant as this node knows it first hand: for its own region the tenant bound to that node's name here,
     * else its own; for another region the tenant of that region's controllers that completed discovery here, else the
     * tenant bound to the node's name. False without security or tenant namespacing, and for a message with no caller.
     */
    public boolean foreignTenantCaller(MsgEvent msg) {
        if (msg == null || msg.getParams() == null) return false;
        Travel cfg = travel();
        if (!cfg.on) return false;
        io.cresco.library.security.Caller caller = io.cresco.library.security.Caller.fromMsgEvent(msg);
        if (caller == null || caller.isAnonymous() || caller.getTenant() == null) return false;
        if (cfg.superusers != null && cfg.superusers.contains(caller.getTenant())) return false;
        String region = msg.getDstRegion(), agent = msg.getDstAgent();
        CertificateManager cm = getCertificateManager();
        io.cresco.agent.controller.communication.PeerTrustRegistry peers = cm == null ? null : cm.getPeerTrust();
        String t;
        if (region == null || region.isEmpty() || (cstate != null && region.equals(cstate.getRegion()))) {
            t = peers == null || agent == null || cstate == null ? null : peers.boundTenant(cstate.getRegion(), agent);
            if (t == null) t = cfg.tenant;
        } else {
            t = peers == null ? null : peers.tenantOfRegion(region);
            if (t == null && peers != null && agent != null) t = peers.boundTenant(region, agent);
        }
        return t == null || !t.equals(caller.getTenant());
    }

    /**
     * A message off the wire, verified as {@code caller}'s, is about to be dispatched on this node (to its controller, a
     * plugin, or as the answer to a call it made). The tenant rule a relay applies before it sends (MsgRouter.tenantRefusal)
     * holds here too: the namespace it travels in is its caller's tenant's, unless its caller is of a superuser tenant --
     * otherwise a tenant's controller could hand a superuser tenant's node a request stamped with ANOTHER tenant, and the
     * answer, which kept that stamp, went into that tenant's namespace. And an RPC request of another tenant's caller must
     * name the node that verified as its sender (MsgRouter.senderRefusal). False: refused -- counted as dropped under its
     * cause, logged, never answered.
     */
    public boolean admitArrival(MsgEvent msg, io.cresco.library.security.Caller caller) {
        if (msg == null || msg.getParams() == null || caller == null) return true;
        Travel cfg = travel();
        if (!cfg.on) return true;
        io.cresco.agent.controller.communication.MsgCounts.Drop cause = io.cresco.agent.controller.communication.MsgCounts.Drop.CROSS_TENANT;
        String why = io.cresco.agent.controller.communication.MsgRouter.tenantRefusal(caller,
                msg.getParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM), cfg.superusers);
        if (why == null) {
            cause = io.cresco.agent.controller.communication.MsgCounts.Drop.SENDER_UNBOUND;
            why = io.cresco.agent.controller.communication.MsgRouter.senderRefusal(msg, caller,
                    io.cresco.agent.controller.communication.MsgSigning.speakerOf(msg), cfg.tenant, cfg.superusers);
        }
        if (why == null) return true;
        msgCounts.dropped(cause, msg);
        long now = System.currentTimeMillis();
        if (now - lastArrivalRefusedLog > 10_000L) {
            lastArrivalRefusedLog = now;
            logger.warn("arrival: refused (" + why + "); not dispatched, not answered");
        }
        return false;
    }

    private volatile long lastArrivalRefusedLog;

    /**
     * Unsigned messages from nodes without signing (stamped {@code agent:} by their broker) are admitted only during an
     * operator's upgrade window (see UpgradeWindow); outside it, never.
     */
    public boolean legacyNodesAdmitted() {
        // read on hot paths (every inbound message, every broker write): the window is re-evaluated at most once a second
        long now = System.currentTimeMillis();
        if (now - windowCheckedAt >= 1000L || now < windowCheckedAt) {
            // the window was decided when the configuration was first read (it never opens later by itself); it closes on time
            windowOpen = io.cresco.agent.controller.communication.SecuritySettings.compatOpen(plugin.getConfig(), now);
            windowCheckedAt = now;
            if (windowOpen && now - windowRemindedAt >= 3_600_000L) {
                windowRemindedAt = now;
                logger.error("UPGRADE WINDOW OPEN until " + io.cresco.agent.controller.communication.SecuritySettings.compatOpenUntil(plugin.getConfig(), now)
                        + ": nodes not yet upgraded are admitted unauthenticated to their own membership (logged hourly while it is open)");
            }
        }
        return windowOpen;
    }

    private volatile long windowRemindedAt = Long.MIN_VALUE / 2;

    private volatile long windowCheckedAt = Long.MIN_VALUE / 2;
    private volatile boolean windowOpen;

    private final io.cresco.agent.controller.communication.LegacyNodes legacyNodes = new io.cresco.agent.controller.communication.LegacyNodes();

    /** The nodes of this region not upgraded yet, as heard during the upgrade window (their raw inboxes). */
    public io.cresco.agent.controller.communication.LegacyNodes legacyNodes() {
        return legacyNodes;
    }

    /**
     * This node's former region ({@code region_alias}, GaiaKeep gfs#26), stamped on what this node originates so a call made
     * at its former address accepts its replies; null without one. A node's own value is declared only in-JVM: the broker a
     * node is attached to replaces it with what its region recorded from the node's own registration.
     */
    public String regionAlias() {
        if (regionAliasRead) return regionAlias;
        try {
            if (cstate == null || cstate.getControllerState() == null) return null;     // not decided yet: read again later
            regionAlias = io.cresco.agent.controller.communication.RegionAlias.of(plugin, cstate.isGlobalController());
            regionAliasRead = true;
            return regionAlias;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private volatile String regionAlias;
    private volatile boolean regionAliasRead;

    private volatile boolean warnedRegionControllerUnverifiable;

    /**
     * At the global: does {@code c} speak for its region -- is it this node, or the controller of {@code c}'s region that
     * joined this global (a bridge peer, or bound here as a joining controller, in the peer trust registry)? An ordinary
     * agent's stamp names its region too; it never speaks for it (review F3). With {@code peer_identity_mode=off} nothing
     * records who joined: then a controller stamp of the region is taken (logged once).
     */
    public boolean speaksForRegion(io.cresco.library.security.Caller c) {
        if (c == null || c.getKind() != io.cresco.library.security.Caller.Kind.CONTROLLER) return false;
        if (cstate != null && c.getRegion().equals(cstate.getRegion()) && c.getAgent().equals(cstate.getAgent())
                && c.getTenant().equals(tenant())) return true;
        io.cresco.agent.controller.communication.CertificateManager cm = certificateManager;
        io.cresco.agent.controller.communication.PeerTrustRegistry reg = cm == null ? null : cm.getPeerTrust();
        if (reg != null && reg.isJoinedController(c.getTenant(), c.getRegion(), c.getAgent())) return true;
        if (cm != null && cm.peerIdentityMode() == io.cresco.agent.controller.communication.PeerIdentity.Mode.OFF) {
            if (!warnedRegionControllerUnverifiable) {
                warnedRegionControllerUnverifiable = true;
                logger.warn("peer_identity_mode=off: the global cannot tell a region's controller from its agents; registry"
                        + " updates are bound to the region named in the caller's stamp only (logged once)");
            }
            return true;
        }
        return false;
    }

    /**
     * At the global: did the controller {@code tenant/region/agent} join this node -- a recorded bridge peer, or a name
     * bound here as a joining controller, in the peer trust registry ({@link #speaksForRegion} asks the same of a
     * caller)? False when nothing records it (no registry yet, or {@code peer_identity_mode=off}): what is not known to
     * have joined is not below this node. Lock-free; no I/O.
     */
    public boolean joinedAsController(String tenant, String region, String agent) {
        if (tenant == null || region == null || agent == null) return false;
        io.cresco.agent.controller.communication.CertificateManager cm = certificateManager;
        io.cresco.agent.controller.communication.PeerTrustRegistry reg = cm == null ? null : cm.getPeerTrust();
        return reg != null && reg.isJoinedController(tenant, region, agent);
    }

    private volatile io.cresco.agent.controller.communication.DirectionAuthority authority;

    /**
     * Where the speaker of a message stands relative to this node, and what a call from the wrong place meets
     * ({@code authority_direction}): asked by every tier's authorization and by every plugin's (through the AgentService).
     */
    public io.cresco.agent.controller.communication.DirectionAuthority authority() {
        io.cresco.agent.controller.communication.DirectionAuthority a = authority;
        if (a == null) {
            synchronized (this) {
                if (authority == null) authority = new io.cresco.agent.controller.communication.DirectionAuthority(this);
                a = authority;
            }
        }
        return a;
    }

    private void stampProblem(String what) {
        long now = System.currentTimeMillis();
        if (now - lastStampProblemLog > 60_000L) {
            lastStampProblemLog = now;
            logger.error(what);
        }
    }

    public PluginAdmin getPluginAdmin() { return pluginAdmin; }

    public CrescoHealthExecutor getHealthExecutor() { return healthExecutor; }
    public AgentExecutor getAgentExecutor() { return executor; }

    public io.cresco.agent.controller.health.MeshHealth getMeshHealth() { return meshHealth; }

    public MeasurementEngine getMeasurementEngine() {
        return this.measurementEngine;
    }
    public io.cresco.agent.controller.netmetrics.LinkMetricsRegistry getLinkMetricsRegistry() { return this.linkMetricsRegistry; }
    public void setLinkMetricsRegistry(io.cresco.agent.controller.netmetrics.LinkMetricsRegistry r) { this.linkMetricsRegistry = r; }
    public io.cresco.agent.controller.netmetrics.NetTuningProfile getNetTuningProfile() { return this.netTuningProfile; }
    public void setNetTuningProfile(io.cresco.agent.controller.netmetrics.NetTuningProfile p) { this.netTuningProfile = p; }
    public io.cresco.agent.controller.netmetrics.AutoTuner getAutoTuner() { return this.autoTuner; }
    public void setAutoTuner(io.cresco.agent.controller.netmetrics.AutoTuner t) { this.autoTuner = t; }
    private io.cresco.agent.controller.netmetrics.RouteView routeView;
    private io.cresco.agent.controller.netmetrics.RouteAdvertiser routeAdvertiser;
    public io.cresco.agent.controller.netmetrics.RouteView getRouteView() { return this.routeView; }
    public void setRouteView(io.cresco.agent.controller.netmetrics.RouteView v) { this.routeView = v; }
    public io.cresco.agent.controller.netmetrics.RouteAdvertiser getRouteAdvertiser() { return this.routeAdvertiser; }
    public void setRouteAdvertiser(io.cresco.agent.controller.netmetrics.RouteAdvertiser a) { this.routeAdvertiser = a; }
    private io.cresco.agent.controller.netmetrics.PathTable pathTable;
    public io.cresco.agent.controller.netmetrics.PathTable getPathTable() { return this.pathTable; }
    public void setPathTable(io.cresco.agent.controller.netmetrics.PathTable t) { this.pathTable = t; }
    private io.cresco.agent.controller.netmetrics.CoordinatorRegistry coordinatorRegistry;
    public io.cresco.agent.controller.netmetrics.CoordinatorRegistry getCoordinatorRegistry() { return this.coordinatorRegistry; }
    public void setCoordinatorRegistry(io.cresco.agent.controller.netmetrics.CoordinatorRegistry c) { this.coordinatorRegistry = c; }
    private io.cresco.agent.controller.globalscheduler.CoordinatorConsensus coordinatorConsensus;
    public io.cresco.agent.controller.globalscheduler.CoordinatorConsensus getCoordinatorConsensus() { return this.coordinatorConsensus; }
    public void setCoordinatorConsensus(io.cresco.agent.controller.globalscheduler.CoordinatorConsensus c) { this.coordinatorConsensus = c; }

    public void setMeasurementEngine(MeasurementEngine measurementEngine) {
        this.measurementEngine = measurementEngine;
    }


}
