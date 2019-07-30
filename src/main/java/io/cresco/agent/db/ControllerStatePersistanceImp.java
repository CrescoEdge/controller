package io.cresco.agent.db;

import com.google.gson.Gson;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.agent.ControllerStatePersistance;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class ControllerStatePersistanceImp implements ControllerStatePersistance {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe;
    private Gson gson;
    private Timer stateUpdateTimer;

    String regionalListener = null;
    String globalListener = null;

    public ControllerStatePersistanceImp(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(ControllerStatePersistanceImp.class.getName(),CLogger.Level.Info);
        this.dbe = dbe;
        this.gson = new Gson();
        this.stateUpdateTimer = new Timer();
        this.stateUpdateTimer.scheduleAtFixedRate(new stateUpdateTask(), 500, 5000l);

    }

    public boolean setControllerState(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        //logger.error(currentMode.name() + " " + currentDesc + " " + globalRegion + " " + globalAgent + " " + regionalRegion + " " + regionalAgent + " " + localRegion + " " + localAgent);

        switch (currentMode) {
            case PRE_INIT:
                return preInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_INIT:
                return standAloneInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE:
                return standAloneSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_SHUTDOWN:
                logger.error("STANDALONE_SHUTDOWN: NOT IMPLEMENTED");
                break;
            case AGENT_INIT:
                return agentInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT:
                return agentSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT_SHUTDOWN:
                return unregisterAgent(localRegion, localAgent);
            case REGION_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION:
                return regionSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_SHUTDOWN:
                return unregisterRegion(localRegion,globalRegion);
            case REGION_FAILED:
                logger.error("REGION_FAILED: NOT IMPLEMENTED");
                break;
            case REGION_GLOBAL_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL_FAILED:
                logger.error("GLOBAL_FAILED: NOT IMPLEMENTED");
                break;
            case REGION_GLOBAL:
                return regionGlobalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL:
                return globalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL_SHUTDOWN:
                logger.error("GLOBAL_SHUTDOWN: NOT IMPLEMENTED");
                break;

            default:
                logger.error("INVALID MODE : " + currentMode.name());
                break;
        }
        return false;
    }

    public boolean preInit(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent){

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
            logger.error("preInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean standAloneInit(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            logger.error("standAloneInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean standAloneSuccess(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            logger.error("standAloneSuccess()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean agentInit(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            logger.error("agentInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }


    public boolean agentSuccess(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            if(registerAgent(regionalRegion, regionalAgent, localRegion, localAgent)) {
                //add event
                dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                returnState = true;
            }

        } catch (Exception ex) {
            logger.error("agentSuccess()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean regionInit(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean regionSuccess(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean regionGlobalSuccess(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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

            if(registerRegion(localRegion, globalRegion)) {
                //add event
                dbe.addCStateEvent(System.currentTimeMillis(), currentMode.name(), currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                regionalListener = registerRegionalListener();
                returnState = true;
            }

        } catch (Exception ex) {
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public boolean globalSuccess(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

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
            regionalListener = registerRegionalListener();
            globalListener = registerGlobalListener();
            returnState = true;

        } catch (Exception ex) {
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return returnState;
    }

    public Map<String,String> getStateMap() {

        Map<String,String> stateMap = dbe.getCSTATE(null);
        return stateMap;
    }

    public boolean registerRegion(String localRegion, String globalRegion) {
        boolean isRegistered = false;

        try {

            MsgEvent enableMsg = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
            enableMsg.setParam("action", "region_enable");
            enableMsg.setParam("req-seq", UUID.randomUUID().toString());
            enableMsg.setParam("region_name", localRegion);
            enableMsg.setParam("desc", "to-gc-region");
            enableMsg.setParam("mode","REGION");

            Map<String, String> exportMap = dbe.getDBExport(true, true, false, plugin.getRegion(), plugin.getAgent(), null);

            enableMsg.setCompressedParam("regionconfigs",exportMap.get("regionconfigs"));
            enableMsg.setCompressedParam("agentconfigs",exportMap.get("agentconfigs"));

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


    public boolean unregisterRegion(String localRegion, String globalRegion) {
        boolean isRegistered = false;

        try {

            MsgEvent disableMsg = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
            disableMsg.setParam("unregister_region_id",plugin.getRegion());
            disableMsg.setParam("desc","to-gc-region");
            disableMsg.setParam("action", "region_disable");



            MsgEvent re = plugin.sendRPC(disableMsg,3000);

            if (re != null) {

                if (re.paramsContains("is_registered")) {

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


    public boolean registerAgent(String regionalRegion, String regionalAgent, String localRegion, String localAgent) {
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

                Map<String, String> exportMap = dbe.getDBExport(false, true, true, plugin.getRegion(), plugin.getAgent(), null);

                enableMsg.setCompressedParam("agentconfigs",exportMap.get("agentconfigs"));

                //logger.error("SENDING MESSAGE: " + enableMsg.printHeader() + " " + enableMsg.getParams());

                MsgEvent re = plugin.sendRPC(enableMsg);

                if (re != null) {

                    if (re.paramsContains("is_registered")) {

                        isRegistered = Boolean.parseBoolean(re.getParam("is_registered"));
                        logger.error("ISREG: " + isRegistered);

                    } else {
                        logger.error("RETURN DOES NOT CONTAIN IS REGISTERED");
                    }
                } else {
                    logger.error("RETURN = NULL");
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

    public boolean unregisterAgent(String localRegion, String localAgent) {
        boolean isRegistered = false;

        try {

            MsgEvent disableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
            disableMsg.setParam("unregister_region_id",plugin.getRegion());
            disableMsg.setParam("unregister_agent_id",plugin.getAgent());
            disableMsg.setParam("desc","to-rc-agent");
            disableMsg.setParam("action", "agent_disable");

            MsgEvent re = plugin.sendRPC(disableMsg, 2000);

            if (re != null) {
                logger.info("Agent: " + localAgent + " unregistered from Region: " + localRegion);
                isRegistered = true;
            } else {
                logger.error("Agent: " + localAgent + " failed to unregister with Region: " + localRegion + "!");
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Exception during Agent: " + localAgent + " registration with Region: " + localRegion + "! " + ex.getMessage());
        }

        return isRegistered;
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
                                dbe.nodeUpdateStatus("AGENT",null, mapMessage.getStringProperty("agent_id"), null, null,mapMessage.getString("agentconfigs"), mapMessage.getString("pluginconfigs"));

                            }
                        } catch(Exception ex) {

                            ex.printStackTrace();
                        }
                    }
                };

                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
                plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT' AND region_id = '" + plugin.getRegion() + "'");
                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT'");

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
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
                                dbe.nodeUpdateStatus("REGION", mapMessage.getStringProperty("region_id"),null, null, mapMessage.getString("regionconfigs"),mapMessage.getString("agentconfigs"), mapMessage.getString("pluginconfigs"));

                            }
                        } catch(Exception ex) {

                            ex.printStackTrace();
                        }
                    }
                };

                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
                plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'REGION'");
                //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"update_mode = 'AGENT'");

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return lid;
    }

    class stateUpdateTask extends TimerTask {
        public void run() {
            if (plugin != null) {
                if (plugin.isActive()) {

                    switch (plugin.getAgentService().getAgentState().getControllerState()) {

                        case STANDALONE:
                            break;
                        case AGENT:

                            try {
                                Map<String, String> exportMap = dbe.getDBExport(false, true, true, plugin.getRegion(), plugin.getAgent(), null);

                                MapMessage updateMap = plugin.getAgentService().getDataPlaneService().createMapMessage();
                                updateMap.setString("agentconfigs", exportMap.get("agentconfigs"));
                                updateMap.setString("pluginconfigs", exportMap.get("pluginconfigs"));

                                updateMap.setStringProperty("update_mode", "AGENT");
                                updateMap.setStringProperty("region_id", plugin.getRegion());
                                updateMap.setStringProperty("agent_id", plugin.getAgent());

                                //logger.error("SENDING AGENT UPDATE!!!");
                                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, updateMap);

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }

                            break;
                        case REGION_GLOBAL:

                            try {
                                Map<String, String> exportMap = dbe.getDBExport(true, true, true, plugin.getRegion(), null, null);

                                MapMessage updateMap = plugin.getAgentService().getDataPlaneService().createMapMessage();
                                updateMap.setString("regionconfigs", exportMap.get("regionconfigs"));
                                updateMap.setString("agentconfigs", exportMap.get("agentconfigs"));
                                updateMap.setString("pluginconfigs", exportMap.get("pluginconfigs"));

                                updateMap.setStringProperty("update_mode", "REGION");
                                updateMap.setStringProperty("region_id", plugin.getRegion());

                                //logger.error("SENDING REGIONAL UPDATE!!!");
                                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, updateMap);

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }

                            break;
                        case GLOBAL:
                            break;

                        default:
                            logger.error("INVALID MODE : " + plugin.getAgentService().getAgentState().getControllerState());
                            break;
                    }

                }
            }
        }
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


