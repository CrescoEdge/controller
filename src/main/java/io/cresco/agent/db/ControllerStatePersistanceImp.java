package io.cresco.agent.db;

import com.google.gson.Gson;
import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.agent.ControllerStatePersistance;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.MapMessage;
import javax.jms.TextMessage;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.*;

public class ControllerStatePersistanceImp implements ControllerStatePersistance {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe;
    private Gson gson;
    private Timer stateUpdateTimer;

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
                logger.error("REGION_SHUTDOWN: NOT IMPLEMENTED");
                break;
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

    /*
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
    */

    public boolean registerRegion(String localRegion, String globalRegion) {
        boolean isRegistered = false;

        try {

            MsgEvent enableMsg = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
            enableMsg.setParam("action", "region_enable");
            enableMsg.setParam("req-seq", UUID.randomUUID().toString());
            enableMsg.setParam("region_name", localRegion);
            enableMsg.setParam("desc", "to-gc-region");

            Map<String, String> exportMap = dbe.getDBExport(true, false, false, plugin.getRegion(), null, null);

            /*
            Map<String,List<Map<String,String>>> regionMap = new HashMap<>();
            List<Map<String,String>> regionList = new ArrayList<>();
            regionList.add(dbe.getRNode(localRegion));
            regionMap.put(localRegion,regionList);
            */

            enableMsg.setCompressedParam("regionconfigs",exportMap.get("regionconfigs"));

            MsgEvent re = plugin.sendRPC(enableMsg);

            if (re != null) {

                if (re.paramsContains("is_registered")) {

                    isRegistered = Boolean.parseBoolean(re.getParam("is_registered"));

                }
            }

            if(isRegistered) {
                logger.info("Region: " + localRegion + " registered with Region: " + globalRegion);
            } else {
                logger.error("Region: " + localRegion + " failed to register with Region: " + globalRegion + "!");
            }

        } catch (Exception ex) {
            logger.error("Exception during Agent: " + localRegion + " registration with Region: " + globalRegion + "! " + ex.getMessage());
        }

        return isRegistered;
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

                Map<String, String> exportMap = dbe.getDBExport(false, true, false, plugin.getRegion(), plugin.getAgent(), null);

                /*
                Map<String,List<Map<String,String>>> agentMap = new HashMap<>();
                List<Map<String,String>> agentList = new ArrayList<>();
                agentList.add(dbe.getANode(localAgent));
                agentMap.put(localRegion,agentList);
                */
                enableMsg.setCompressedParam("agentconfigs",exportMap.get("agentconfigs"));


                MsgEvent re = plugin.sendRPC(enableMsg);

                if (re != null) {

                    if (re.paramsContains("is_registered")) {

                        isRegistered = Boolean.parseBoolean(re.getParam("is_registered"));

                    }
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

            MsgEvent re = plugin.sendRPC(disableMsg);

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

                                //updateMap.setStringProperty("update_mode", plugin.getAgentService().getAgentState().getControllerState().toString());
                                updateMap.setStringProperty("update_mode", "AGENT");
                                updateMap.setStringProperty("region_id", plugin.getRegion());
                                updateMap.setStringProperty("agent_id", plugin.getAgent());

                                //plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, updateMap);

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }

                            break;
                        case REGION_GLOBAL:
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


