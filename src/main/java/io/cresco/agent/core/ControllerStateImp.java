package io.cresco.agent.core;


import io.cresco.agent.db.DBEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import java.util.Map;

public class ControllerStateImp implements ControllerState {

	private PluginBuilder plugin;
	private CLogger logger;

	private ControllerMode currentMode = ControllerMode.PRE_INIT;
	private String localRegion;
	private String localAgent;
	private String currentDesc;
	private String globalAgent;
	private String globalRegion;
	private String regionalAgent;
	private String regionalRegion;

	private ControllerStatePersistance controllerStatePersistance;

	public ControllerStateImp(PluginBuilder plugin, DBEngine dbe) {
		this.plugin = plugin;
		this.logger = plugin.getLogger(ControllerStatePersistance.class.getName(), CLogger.Level.Info);

		//keeps things recorded in the database
		this.controllerStatePersistance = new ControllerStatePersistance(plugin,dbe);
	}

	public boolean isActive() {
		return ((currentMode == ControllerMode.STANDALONE) || (currentMode == ControllerMode.AGENT) || (currentMode == ControllerMode.GLOBAL) || (currentMode == ControllerMode.REGION_GLOBAL));
	}

	public boolean isFailed() {
		return ((currentMode == ControllerMode.AGENT_FAILED) || (currentMode == ControllerMode.GLOBAL_FAILED) || (currentMode == ControllerMode.REGION_GLOBAL) || (currentMode == ControllerMode.REGION_GLOBAL_FAILED) || (currentMode == ControllerMode.STANDALONE_FAILED));
	}

	public synchronized ControllerMode getControllerState() {
			return currentMode;
	}

	public synchronized String getCurrentDesc() {
		return  currentDesc;
	}

	public synchronized boolean isRegionalController() {
		boolean isRC = false;

			if ((currentMode.toString().startsWith("REGION")) || isGlobalController()) {
				isRC = true;
			}
		return isRC;
	}

	public synchronized boolean isGlobalController() {
		boolean isGC = false;

			if (currentMode.toString().startsWith("GLOBAL")) {
				isGC = true;
			}
		return isGC;
	}

	public synchronized String getRegion() { return localRegion; }

	public synchronized String getAgent() { return localAgent; }

	public synchronized String getGlobalAgent() {
		return globalAgent;
	}

	public synchronized String getGlobalRegion() {
		return globalRegion;
	}

	public synchronized String getRegionalAgent() {
		return regionalAgent;
	}

	public synchronized String getRegionalRegion() {
		return regionalRegion;
	}

	public synchronized String getGlobalControllerPath() {
		if(isRegionalController()) {
			return globalRegion + "_" + globalAgent;
		} else {
			return null;
		}
	}

	public synchronized String getRegionalControllerPath() {
		if(isRegionalController()) {
			return regionalRegion + "_" + regionalAgent;
		} else {
			return null;
		}
	}

	public synchronized String getAgentPath() {
		return localRegion + "_" + localAgent;
	}

	public boolean setPreInit() {

		//pull last known agent and region name from the database
		Map<String, String> lastCState = controllerStatePersistance.getStateMap();

			currentMode = ControllerMode.PRE_INIT;
			currentDesc = "Initial State";
			if(lastCState != null) {
				localAgent = lastCState.get("local_agent");
				localRegion = lastCState.get("local_region");
			} else {
				localAgent = null;
				localRegion = null;
			}
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.AGENT_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)){

				currentMode = ControllerMode.AGENT_SHUTDOWN;
				currentDesc = desc;

			return true;
		} else {
			return false;
		}
	}

	public boolean setAgentFailed(String desc) {
			currentMode = ControllerMode.AGENT_FAILED;
			currentDesc = desc;
			this.globalAgent = null;
			this.globalRegion = null;
			this.regionalRegion = null;
			this.regionalAgent = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentSuccess(String regionalRegion, String regionalAgent, String desc) {
			currentMode = ControllerMode.AGENT;
			currentDesc = desc;
			this.globalAgent = null;
			this.globalRegion = null;
			this.regionalRegion = regionalRegion;
			this.regionalAgent = regionalAgent;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentInit(String regionName, String agentName, String desc) {
			currentMode = ControllerMode.AGENT_INIT;
			currentDesc = desc;
			this.localAgent = agentName;
			this.localRegion = regionName;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);

	}

	public boolean setRegionInit(String regionName, String agentName, String desc) {
			currentMode = ControllerMode.REGION_INIT;
			currentDesc = desc;
			localRegion = regionName;
			localAgent = agentName;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionSuccess(String regionName, String agentName, String desc) {
			currentMode = ControllerMode.REGION;
			currentDesc = desc;
			localRegion = regionName;
			localAgent = agentName;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionGlobalInit(String desc) {
			currentMode = ControllerMode.REGION_GLOBAL_INIT;
			currentDesc = desc;
			this.globalAgent = null;
			this.globalRegion = null;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionFailed(String desc) {
			currentMode = ControllerMode.REGION_FAILED;
			currentDesc = desc;
			this.globalAgent = null;
			this.globalRegion = null;
			this.regionalAgent = null;
			this.regionalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalSuccess(String desc) {
			currentMode = ControllerMode.GLOBAL;
			currentDesc = desc;
			this.globalRegion = localRegion;
			this.globalAgent = localAgent;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalInit(String desc) {
			currentMode = ControllerMode.GLOBAL_INIT;
			currentDesc = desc;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalFailed(String desc) {
			currentMode = ControllerMode.GLOBAL_FAILED;
			currentDesc = desc;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;
			this.regionalAgent = localAgent;
			this.regionalRegion = localRegion;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.GLOBAL_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)) {
				currentMode = ControllerMode.GLOBAL_SHUTDOWN;
				currentDesc = desc;

			return true;
		} else {
			return false;
		}
	}

	public boolean setRegionalGlobalSuccess(String globalRegion, String globalAgent, String desc) {
			currentMode = ControllerMode.REGION_GLOBAL;
			currentDesc = desc;
			this.globalRegion = globalRegion;
			this.globalAgent = globalAgent;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.REGION_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)) {
				currentMode = ControllerMode.REGION_SHUTDOWN;
				currentDesc = desc;

			return true;
		} else {
			return false;
		}
	}

	public boolean setRegionalGlobalFailed(String desc) {
			currentMode = ControllerMode.REGION_GLOBAL_FAILED;
			currentDesc = desc;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneInit(String region, String agent, String desc) {
			currentMode = ControllerMode.STANDALONE_INIT;
			currentDesc = desc;
			localAgent = agent;
			localRegion = region;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneSuccess(String region, String agent, String desc) {
			currentMode = ControllerMode.STANDALONE;
			currentDesc = desc;
			localAgent = agent;
			localRegion = region;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneFailed(String region, String agent, String desc) {
			currentMode = ControllerMode.STANDALONE_FAILED;
			currentDesc = desc;
			localAgent = agent;
			localRegion = null;
			regionalRegion = null;
			regionalAgent = null;
			globalAgent = null;
			globalRegion = null;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneShutdown(String desc) {
			currentMode = ControllerMode.STANDALONE_SHUTDOWN;
			currentDesc = desc;

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

}