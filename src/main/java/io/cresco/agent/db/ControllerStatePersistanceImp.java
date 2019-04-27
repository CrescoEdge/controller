package io.cresco.agent.db;

import com.google.gson.Gson;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.agent.ControllerStatePersistance;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.*;

public class ControllerStatePersistanceImp implements ControllerStatePersistance {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe;
    private Gson gson;


    public ControllerStatePersistanceImp(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(ControllerStatePersistanceImp.class.getName(),CLogger.Level.Info);
        this.dbe = dbe;
        this.gson = new Gson();

    }

    public boolean setControllerState(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        //logger.error(currentMode.name() + " " + currentDesc + " " + globalRegion + " " + globalAgent + " " + regionalRegion + " " + regionalAgent + " " + localRegion + " " + localAgent);

        switch (currentMode) {
            case PRE_INIT:
                return preInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE_INIT:
                //STANDALONE_INIT Core Init null null null null null agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                return standAloneInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case STANDALONE:
                return standAloneSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT_INIT:
                return agentInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case AGENT:
                return agentSuccess(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_INIT:
                //REGION_INIT initRegion() TS :1553784233245 null null null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6
                // agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_FAILED:
                break;
            case REGION_GLOBAL_INIT:
                //REGION_GLOBAL_INIT initRegion() : Success null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                // region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL_FAILED:
                //REGION_GLOBAL_FAILED gCheck : Dynamic Global Host :null_null is not reachable. null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6
                // agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540 region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
            case REGION_GLOBAL:
                break;
            case GLOBAL:
                //GLOBAL gCheck : Creating Global Host null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                // region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                return regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                //logger.error("LINKING REGION: " + localRegion + " AGENT: " + localAgent);

                //dbe.assoicateANodetoRNode(localRegion, localAgent);
                //logger.error("LINKED REGION: " + localRegion + " AGENT: " + localAgent);

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
            if(registerAgent(localRegion, localAgent)) {
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

    public Map<String,String> getStateMap() {

        Map<String,String> stateMap = dbe.getCSTATE(null);
        /*
        if(stateMap != null) {
            if (stateMap.containsKey("local_agent") && stateMap.containsKey("local_region")) {
                stateMap.put("configparams", dbe.getNodeConfigParams(null, stateMap.get("local_agent"), null));
            }
        }
        */
        return stateMap;
    }

    public boolean registerAgent(String localRegion, String localAgent) {
        boolean isRegistered = false;

            try {

                MsgEvent enableMsg = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.CONFIG);
                enableMsg.setParam("action", "agent_enable");
                enableMsg.setParam("req-seq", UUID.randomUUID().toString());
                enableMsg.setParam("region_name", localRegion);
                enableMsg.setParam("agent_name", localAgent);
                enableMsg.setParam("desc", "to-rc-agent");

                Map<String,List<Map<String,String>>> agentMap = new HashMap<>();
                List<Map<String,String>> agentList = new ArrayList<>();
                agentList.add(dbe.getANode(localAgent));
                agentMap.put(localRegion,agentList);

                enableMsg.setCompressedParam("agentconfigs",gson.toJson(agentMap));

                Map<String,List<Map<String,String>>> pluginMap = new HashMap<>();
                List<Map<String,String>> pluginList = new ArrayList<>();
                List<String> tmpPluginList = dbe.getNodeList(localRegion, localAgent);
                for(String pluginId : tmpPluginList) {
                    pluginList.add(dbe.getPNode(pluginId));
                }
                pluginMap.put(localAgent,pluginList);

                enableMsg.setCompressedParam("pluginconfigs", gson.toJson(pluginMap));

                MsgEvent re = plugin.sendRPC(enableMsg);

                if (re != null) {
                    logger.info("Agent: " + localAgent + " registered with Region: " + localRegion);
                    isRegistered = true;
                } else {
                    logger.error("Agent: " + localAgent + " failed to register with Region: " + localRegion + "!");
                }

            } catch (Exception ex) {
                logger.error("Exception during Agent: " + localAgent + " registration with Region: " + localRegion + "! " + ex.getMessage());
            }

            return isRegistered;
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


