package io.cresco.agent.core;

import com.google.gson.Gson;
import io.cresco.agent.db.DBEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.*;
import java.util.Map;

public class ControllerStatePersistance {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe;
    private Gson gson;

    // the registry listeners (the state exports of the nodes below this controller): one per role, its id kept, removed
    // when the role is left (syncRegistryListeners)
    private String regionalListener = null;
    private String globalListener = null;

    /** At the global: does a REGION state update's writer speak for its region (ControllerEngine.speaksForRegion)? */
    private volatile java.util.function.Predicate<io.cresco.library.security.Caller> regionController;

    void setRegionControllerCheck(java.util.function.Predicate<io.cresco.library.security.Caller> check) {
        this.regionController = check;
    }

    public ControllerStatePersistance(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(ControllerStatePersistance.class.getName(),CLogger.Level.Info);
        this.dbe = dbe;
        this.gson = new Gson();
    }

    public boolean setControllerState(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {
        boolean saved = saveControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
        syncRegistryListeners(currentMode);
        return saved;
    }

    /**
     * The registry listeners the mode calls for, each held once. They were added on every entry to GLOBAL and
     * REGION_GLOBAL and never removed (the id kept was always null): a global held two of each from boot and applied every
     * region export twice -- an older export could then be applied after a newer one, and its reconcile deleted rows the
     * newer one added --, a region gained one per global rejoin, and a region with no global took none of its agents'
     * exports. A subscription the dataplane could not make leaves the id null and is tried again on the next state change.
     */
    private synchronized void syncRegistryListeners(ControllerMode mode) {
        try {
            if (takesAgentExports(mode)) {
                if (regionalListener == null) regionalListener = registerRegionalListener();
            } else if (regionalListener != null) {
                String id = regionalListener;
                regionalListener = null;
                removeRegistryListener(id);
            }
            if (takesRegionExports(mode)) {
                if (globalListener == null) globalListener = registerGlobalListener();
            } else if (globalListener != null) {
                String id = globalListener;
                globalListener = null;
                removeRegistryListener(id);
            }
        } catch (Exception ex) {
            logger.error("syncRegistryListeners() " + mode, ex);
        }
    }

    private boolean saveControllerState(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        //logger.error(currentMode.name() + " " + currentDesc + " " + globalRegion + " " + globalAgent + " " + regionalRegion + " " + regionalAgent + " " + localRegion + " " + localAgent);
        // how we we save to the db
        switch (currentMode) {
            case PRE_INIT:
                return preInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_INIT:
                return standAloneInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE:
                return standAloneSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_SHUTDOWN:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT_INIT:
                return agentInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT:
                return agentSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT_SHUTDOWN:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION:
                return regionSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_SHUTDOWN:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_FAILED:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL_FAILED:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL:
                return regionGlobalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL_INIT:
                // I think this is the right thing to do
                return true;
            case GLOBAL:
                return globalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL_SHUTDOWN:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);

            // Transient failure states (a MINA loss event immediately re-inits). Record the
            // transition for audit and accept it, rather than logging "INVALID MODE" and returning
            // false. The in-memory MINA transition proceeds regardless of this return value.
            case STANDALONE_FAILED:
            case AGENT_FAILED:
            case GLOBAL_FAILED:
                return recordTransientState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);

            default:
                logger.warn("setControllerState() INVALID MODE : " + currentMode.name() + " DESC: " + currentDesc);
                break;
        }
        return false;
    }

    /**
     * Record a transient state transition (a _FAILED state that immediately re-inits) as a cstate
     * event for audit, and accept it. These states used to log "NOT IMPLEMENTED"/"INVALID MODE" and
     * return false — noisy, and the DB never saw the transition. The MINA transition proceeds
     * regardless of this return value; recording keeps the audit trail complete.
     */
    private boolean recordTransientState(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {
        try {
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            return true;
        } catch (Exception ex) {
            logger.error("recordTransientState({}) : {}", currentMode.name(), ex.getMessage());
            return false;
        }
    }

    public boolean preInit(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent){

        boolean returnState = false;
        try {
            //pull CSTATE to see if current region and agent match past region and agent
            Map<String, String> stateMap = dbe.getCSTATE(null);
            if (stateMap != null) {
                String previousRegion = stateMap.get("local_region");
                String previousAgent = stateMap.get("local_agent");

                //clean up plugins that should not persist
                dbe.purgeTransientPNodes(previousRegion, previousAgent);

                //if name has changed we need to change assoications
                if (!(previousRegion.equals(localRegion) && previousAgent.equals(localAgent))) {

                    dbe.reassoicateANodes(previousRegion, previousAgent, localRegion, localAgent);
                    dbe.reassoicatePNodes(previousAgent, localAgent);
                }
                returnState = true;
            }
        } catch (Exception ex) {
            logger.error("preInit()", ex);
        }
        return returnState;
    }

    public boolean standAloneInit(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {

            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }

            //check if agent exist, if not add it, if so update it
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }
            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;
        } catch (Exception ex) {
            logger.error("standAloneInit()", ex);
        }
        return returnState;
    }

    public boolean standAloneSuccess(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {

            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }

            //check if agent exist, if not add it, if so update it
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }
            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;

        } catch (Exception ex) {
            logger.error("standAloneSuccess()", ex);
        }
        return returnState;
    }

    public boolean agentInit(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }

            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;

        } catch (Exception ex) {
            logger.error("agentInit()", ex);
        }
        return returnState;
    }

    public boolean agentSuccess(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }

            //send information to remote
            //if(registerAgent(regionalRegion, regionalAgent, localRegion, localAgent)) {
                //add event
                dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                returnState = true;
            //}

        } catch (Exception ex) {
            logger.error("agentSuccess()", ex);
        }
        return returnState;
    }

    public boolean regionInit(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 3, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }

            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;

        } catch (Exception ex) {
            logger.error("regionInit()", ex);
        }
        return returnState;
    }

    public boolean regionSuccess(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }

            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;

        } catch (Exception ex) {
            logger.error("regionSuccess()", ex);
        }
        return returnState;
    }

    public boolean regionGlobalSuccess(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }


            //if(registerRegion(localRegion, globalRegion)) {
                //add event
                dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                returnState = true;
            //}

        } catch (Exception ex) {
            logger.error("regionGlobalSuccess()", ex);
        }
        return returnState;
    }

    public boolean globalSuccess(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        boolean returnState = false;
        try {
            if (!dbe.nodeExist(localRegion, null, null)) {
                dbe.addRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            } else {
                dbe.updateRNode(localRegion, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), "pending");
            }
            if (!dbe.nodeExist(null, localAgent, null)) {
                dbe.addANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            } else {
                dbe.updateANode(localAgent, 10, currentDesc, plugin.getConfig().getIntegerParam("watchdog_period",5000), System.currentTimeMillis(), gson.toJson(plugin.getConfig().getConfigMap()));
            }

            if(!dbe.assoicateANodetoRNodeExist(localRegion, localAgent)) {
                dbe.assoicateANodetoRNode(localRegion, localAgent);
            }

            //add event
            dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            returnState = true;

        } catch (Exception ex) {
            logger.error("globalSuccess()", ex);
        }
        return returnState;
    }


    public Map<String,String> getStateMap() {

        Map<String,String> stateMap = dbe.getCSTATE(null);
        return stateMap;
    }


    /**
     * A state update on the dataplane is made only by the node it reports (its writer, stamped by that node's controller
     * and checked by the broker): RegistryBinding. Insecure mode checks nothing.
     */
    private String writerRefusal(Message msg, boolean atGlobal, String regionId, String agentId, String regionconfigs,
                                 String agentconfigs, String pluginconfigs) {
        if (!io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(plugin.getConfig())) return null;
        io.cresco.library.security.Caller writer = io.cresco.agent.controller.communication.CallerStamps.fromJmsProperties(msg);
        return io.cresco.agent.controller.communication.RegistryBinding.dataplaneRefusal(writer, atGlobal,
                dbe::regionOfAgentQuiet, regionController,
                io.cresco.agent.controller.communication.SecuritySettings.compatOpen(plugin.getConfig(), System.currentTimeMillis()),
                regionId, agentId, regionconfigs, agentconfigs, pluginconfigs);
    }

    private volatile long lastRefusedUpdateLog;

    private void refusedUpdate(String mode, String why) {
        long now = System.currentTimeMillis();
        if (now - lastRefusedUpdateLog > 10_000L) {
            lastRefusedUpdateLog = now;
            logger.warn("REFUSED a " + mode + " state update on the dataplane: " + why);
        }
    }

    /** A region controller (with or without a global, joining one, having lost one, or the global itself) takes its agents' exports. */
    static boolean takesAgentExports(ControllerMode mode) {
        switch (mode) {
            case REGION:
            case REGION_GLOBAL_INIT:
            case REGION_GLOBAL_FAILED:
            case REGION_GLOBAL:
            case GLOBAL_INIT:
            case GLOBAL:
                return true;
            default:
                return false;
        }
    }

    /** Only the global takes the regions' exports. */
    static boolean takesRegionExports(ControllerMode mode) {
        return mode == ControllerMode.GLOBAL;
    }

    /** Subscribe {@code ml} to the state updates {@code selector} picks; the listener's id, or null when not subscribed. */
    String addRegistryListener(MessageListener ml, String selector) {
        return plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT, ml, selector);
    }

    void removeRegistryListener(String listenerId) {
        plugin.getAgentService().getDataPlaneService().removeMessageListener(listenerId);
    }

    public String registerRegionalListener() {
        String lid = null;
        try {

            try {

                MessageListener ml = new MessageListener() {
                    public void onMessage(Message msg) {
                        try {

                            if (msg instanceof MapMessage) {

                                MapMessage mapMessage = (MapMessage)msg;
                                //logger.error("REGIONAL HEALTH MESSAGE: INCOMING");
                                String why = writerRefusal(msg, false, null, mapMessage.getStringProperty("agent_id"),
                                        null, mapMessage.getString("agentconfigs"), mapMessage.getString("pluginconfigs"));
                                if (why != null) { refusedUpdate("AGENT", why); return; }
                                String agentconfigs = mapMessage.getString("agentconfigs");
                                if (io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(plugin.getConfig())) {
                                    io.cresco.library.security.Caller writer = io.cresco.agent.controller.communication.CallerStamps.fromJmsProperties(msg);
                                    if (writer != null) agentconfigs = io.cresco.agent.controller.communication.RegistryBinding.withVerifiedTenant(agentconfigs, writer.getTenant());
                                }
                                dbe.nodeUpdateStatus("AGENT",null, mapMessage.getStringProperty("agent_id"), null, null, agentconfigs, mapMessage.getString("pluginconfigs"));

                            }
                        } catch(Exception ex) {

                            logger.error("registerRegionalListener() onMessage", ex);
                        }
                    }
                };

                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
                lid = addRegistryListener(ml, "update_mode = 'AGENT' AND region_id = '" + plugin.getRegion() + "'");
                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT'");

            } catch (Exception e) {
                logger.error("registerRegionalListener() addMessageListener", e);
            }

        } catch (Exception ex) {
            logger.error("registerRegionalListener()", ex);
        }
        return lid;
    }

    public String registerGlobalListener() {
        String lid = null;
        try {

            try {

                MessageListener ml = new MessageListener() {
                    public void onMessage(Message msg) {
                        try {

                            if (msg instanceof MapMessage) {

                                MapMessage mapMessage = (MapMessage)msg;
                                //logger.error("GLOBAL HEALTH MESSAGE: INCOMING " + mapMessage.getStringProperty("region_id"));
                                String why = writerRefusal(msg, true, mapMessage.getStringProperty("region_id"), null,
                                        mapMessage.getString("regionconfigs"), mapMessage.getString("agentconfigs"), mapMessage.getString("pluginconfigs"));
                                if (why != null) { refusedUpdate("REGION", why); return; }
                                dbe.nodeUpdateStatus("REGION", mapMessage.getStringProperty("region_id"),null, null, mapMessage.getString("regionconfigs"),mapMessage.getString("agentconfigs"), mapMessage.getString("pluginconfigs"));

                            }
                        } catch(Exception ex) {

                            logger.error("registerGlobalListener() onMessage", ex);
                        }
                    }
                };

                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
                lid = addRegistryListener(ml, "update_mode = 'REGION'");
                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT'");

            } catch (Exception e) {
                logger.error("registerGlobalListener() addMessageListener", e);
            }

        } catch (Exception ex) {
            logger.error("registerGlobalListener()", ex);
        }
        return lid;
    }


}


    /*
    public enum Mode {
		PRE_INIT,

		STANDALONE_INIT,
		STANDALONE,

		AGENT_INIT,
		AGENT,

		REGION_INIT,
		REGION_FAILED,
		REGION_GLOBAL_INIT,
		REGION_GLOBAL_FAILED,
		REGION_GLOBAL,
		GLOBAL,

		Mode() {

		}
	}
     */


