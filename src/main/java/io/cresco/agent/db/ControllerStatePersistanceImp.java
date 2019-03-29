package io.cresco.agent.db;

import com.google.gson.Gson;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.agent.ControllerStatePersistance;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;

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

    public void setControllerState(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {

        logger.error(currentMode.name() + " " + currentDesc + " " + globalRegion + " " + globalAgent + " " + regionalRegion + " " + regionalAgent + " " + localRegion + " " + localAgent);

        switch (currentMode) {
            case PRE_INIT:
                break;
            case STANDALONE_INIT:
                //STANDALONE_INIT Core Init null null null null null agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                standAloneINIT(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                break;
            case STANDALONE:
                break;
            case AGENT_INIT:
                break;
            case AGENT:
                break;
            case REGION_INIT:
                //REGION_INIT initRegion() TS :1553784233245 null null null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6
                // agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                break;
            case REGION_FAILED:
                break;
            case REGION_GLOBAL_INIT:
                //REGION_GLOBAL_INIT initRegion() : Success null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                // region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                break;
            case REGION_GLOBAL_FAILED:
                //REGION_GLOBAL_FAILED gCheck : Dynamic Global Host :null_null is not reachable. null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6
                // agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540 region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                break;
            case REGION_GLOBAL:
                break;
            case GLOBAL:
                //GLOBAL gCheck : Creating Global Host null null region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                // region-07581fcc-bfb9-48f8-a2da-165583fb65c6 agent-b612f075-0f3b-4ba6-ba75-1d08bd24b540
                regionInit(currentMode,currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
                logger.error("LINKING REGION: " + localRegion + " AGENT: " + localAgent);
                dbe.assoicateANodetoRNode(localRegion, localAgent);
                logger.error("LINKED REGION: " + localRegion + " AGENT: " + localAgent);
                break;

            default:
                logger.error("INVALID MODE : " + currentMode.name());
                break;
        }


    }

    public void standAloneINIT(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {
        //check if agent exist, if not add it, if so update it
        if(!dbe.nodeExist(null,localAgent,null)) {
            dbe.addANode(localAgent, 0, "Core Init", 0, 0, gson.toJson(plugin.getConfig().getConfigMap()));
        } else {
            dbe.updateANode(localAgent, 0, "Core Init", 0, 0, gson.toJson(plugin.getConfig().getConfigMap()));
        }
        //add event
        dbe.addCStateEvent(System.currentTimeMillis(),currentMode.name(),currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
    }

    public void regionInit(ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent) {
        if(!dbe.nodeExist(localRegion,null,null)) {
            dbe.addRNode(localRegion, 0, "Core Init", 0, 0, "pending");
        } else {
            dbe.updateRNode(localRegion, 0, "Core Init", 0, 0, "pending");
        }
        if(!dbe.nodeExist(null,localAgent,null)) {
            dbe.addANode(localAgent, 0, "Core Init", 0, 0, gson.toJson(plugin.getConfig().getConfigMap()));
        } else {
            dbe.updateANode(localAgent, 0, "Core Init", 0, 0, gson.toJson(plugin.getConfig().getConfigMap()));
        }
        //add event
        dbe.addCStateEvent(System.currentTimeMillis(),currentMode.name(),currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
    }

    public Map<String,String> getStateMap() {
        return new HashMap<>();
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



}