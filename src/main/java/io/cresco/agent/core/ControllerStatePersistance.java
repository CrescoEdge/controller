package io.cresco.agent.core;

import com.google.gson.Gson;
import io.cresco.agent.db.DBEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class ControllerStatePersistance {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe;
    private Gson gson;

    private String regionalListener = null;
    private String globalListener = null;

    public ControllerStatePersistance(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(ControllerStatePersistance.class.getName(),CLogger.Level.Info);
        this.dbe = dbe;
        this.gson = new Gson();
    }

    public boolean setControllerState(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        //logger.error(currentMode.name() + " " + currentDesc + " " + globalRegion + " " + globalAgent + " " + regionalRegion + " " + regionalAgent + " " + localRegion + " " + localAgent);

        switch (currentMode) {
            case PRE_INIT:
                return preInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_INIT:
                return standAloneInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE:
                return standAloneSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_SHUTDOWN:
                logger.warn("STANDALONE_SHUTDOWN: NOT IMPLEMENTED : " + currentDesc);
                break;
            case AGENT_INIT:
                return agentInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT:
                return agentSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT_SHUTDOWN:
                logger.warn("AGENT_SHUTDOWN: NOT IMPLEMENTED : " + currentDesc);
                //return unregisterAgent(localRegion, localAgent);
            case REGION_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION:
                return regionSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_SHUTDOWN:
                logger.warn("REGION_SHUTDOWN: NOT IMPLEMENTED : " + currentDesc);
                //return unregisterRegion(localRegion,globalRegion);
            case REGION_FAILED:
                logger.error("REGION_FAILED: NOT IMPLEMENTED : " + currentDesc);
                break;
            case REGION_GLOBAL_INIT:
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL_FAILED:
                logger.error("REGION_GLOBAL_FAILED: NOT IMPLEMENTED : " + currentDesc);
                break;
            case REGION_GLOBAL:
                return regionGlobalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL:
                return globalSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case GLOBAL_SHUTDOWN:
                logger.warn("GLOBAL_SHUTDOWN: NOT IMPLEMENTED : " + currentDesc);
                break;

            default:
                logger.warn("setControllerState() INVALID MODE : " + currentMode.name() + " DESC: " + currentDesc);
                break;
        }
        return false;
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
            logger.error("preInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("standAloneInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("standAloneSuccess()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("agentInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("agentSuccess()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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
                regionalListener = registerRegionalListener();
                returnState = true;
            //}

        } catch (Exception ex) {
            logger.error("regionInit()");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
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


