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

	/**
	 * This node as it stands (name, role, parent), for the message paths: one immutable value, replaced whole by every
	 * method below that changes the mode, the node's name or a parent's -- inside the same monitor as the change, so no
	 * reader ever sees half of one. Read with no lock ({@link #standing()}): the getters above enter this object's
	 * monitor on every call, which a per-message path must not.
	 */
	private volatile io.cresco.agent.controller.communication.SpeakerPosition.Standing standing =
			io.cresco.agent.controller.communication.SpeakerPosition.Standing.NONE;

	/** This node as it stands now. A volatile read: no lock, nothing allocated. Never null. */
	public io.cresco.agent.controller.communication.SpeakerPosition.Standing standing() {
		return standing;
	}

	/**
	 * Publish the standing from the fields as they are now. Called with this object's monitor held, as the last step of
	 * every change of {@code currentMode}, {@code localRegion}/{@code localAgent}, {@code regionalRegion}/{@code
	 * regionalAgent} or {@code globalRegion}/{@code globalAgent}: a method that changes one of them and does not call
	 * this leaves the message paths on the state before (ControllerStandingTest holds every setter to it).
	 */
	private void publishStanding() {
		String mode = currentMode.name();
		boolean global = mode.startsWith("GLOBAL");
		boolean regional = global || mode.startsWith("REGION");
		standing = new io.cresco.agent.controller.communication.SpeakerPosition.Standing(localRegion, localAgent, regional, global,
				regional ? globalRegion : regionalRegion, regional ? globalAgent : regionalAgent);
	}

	public ControllerStateImp(PluginBuilder plugin, DBEngine dbe) {
		this.plugin = plugin;
		this.logger = plugin.getLogger(ControllerStatePersistance.class.getName(), CLogger.Level.Info);

		//keeps things recorded in the database
		this.controllerStatePersistance = new ControllerStatePersistance(plugin,dbe);
	}

	// The active (serving) modes. Was missing REGION (a region-only controller reported not-active).
	/** At the global, who speaks for a region in its dataplane state updates (review F3; set by the ControllerEngine). */
	public void setRegionControllerCheck(java.util.function.Predicate<io.cresco.library.security.Caller> check) {
		controllerStatePersistance.setRegionControllerCheck(check);
	}

	public boolean isActive() {
		return ((currentMode == ControllerMode.STANDALONE)
				|| (currentMode == ControllerMode.AGENT)
				|| (currentMode == ControllerMode.REGION)
				|| (currentMode == ControllerMode.REGION_GLOBAL)
				|| (currentMode == ControllerMode.GLOBAL));
	}

	/**
	 * The node's dataplane may carry traffic. An active mode, and also a regional controller that lost its global
	 * (REGION_GLOBAL_FAILED) or is joining it again (REGION_GLOBAL_INIT): its own broker, its sessions on it and its
	 * agents keep running through a global restart, so its dataplane (a regional wsapi gateway's streams, route state)
	 * must not stall for the global's outage (GaiaKeep gfs#26: a global restart blocked every send of the region's
	 * controller for dataplane_session_wait_sec, then dropped it).
	 */
	public boolean isDataPlaneUsable() {
		ControllerMode m = getControllerState();
		return isActive() || m == ControllerMode.REGION_GLOBAL_FAILED || m == ControllerMode.REGION_GLOBAL_INIT;
	}

	// The failed modes. Was buggy: it listed REGION_GLOBAL (the healthy active state) so a
	// region+global reported isActive() && isFailed() at once; and it omitted REGION_FAILED.
	public boolean isFailed() {
		return ((currentMode == ControllerMode.STANDALONE_FAILED)
				|| (currentMode == ControllerMode.AGENT_FAILED)
				|| (currentMode == ControllerMode.REGION_FAILED)
				|| (currentMode == ControllerMode.REGION_GLOBAL_FAILED)
				|| (currentMode == ControllerMode.GLOBAL_FAILED));
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

			synchronized (this) {
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
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.AGENT_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)){

				synchronized (this) {
					currentMode = ControllerMode.AGENT_SHUTDOWN;
					currentDesc = desc;
					publishStanding();
				}

			return true;
		} else {
			return false;
		}
	}

	public boolean setAgentFailed(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.AGENT_FAILED;
				currentDesc = desc;
				this.globalAgent = null;
				this.globalRegion = null;
				this.regionalRegion = null;
				this.regionalAgent = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentSuccess(String regionalRegion, String regionalAgent, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.AGENT;
				currentDesc = desc;
				this.globalAgent = null;
				this.globalRegion = null;
				this.regionalRegion = regionalRegion;
				this.regionalAgent = regionalAgent;
				publishStanding();
			}

		// below a parent: the global's crypto policy governs; local crypto policy keys are ignored
		io.cresco.agent.controller.statemachine.CryptoPolicySync.warnIfLocalPolicyIgnored(plugin, logger);
        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setAgentInit(String regionName, String agentName, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.AGENT_INIT;
				currentDesc = desc;
				this.localAgent = agentName;
				this.localRegion = regionName;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);

	}

	public boolean setRegionInit(String regionName, String agentName, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION_INIT;
				currentDesc = desc;
				localRegion = regionName;
				localAgent = agentName;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionSuccess(String regionName, String agentName, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION;
				currentDesc = desc;
				localRegion = regionName;
				localAgent = agentName;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionGlobalInit(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION_GLOBAL_INIT;
				currentDesc = desc;
				this.globalAgent = null;
				this.globalRegion = null;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionFailed(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION_FAILED;
				currentDesc = desc;
				this.globalAgent = null;
				this.globalRegion = null;
				this.regionalAgent = null;
				this.regionalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalSuccess(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.GLOBAL;
				currentDesc = desc;
				this.globalRegion = localRegion;
				this.globalAgent = localAgent;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				publishStanding();
			}

		// the top of the fabric: its config sets the fabric-wide crypto policy (CryptoPolicySync)
		io.cresco.agent.controller.statemachine.CryptoPolicySync.becomeAuthority(plugin, logger, localRegion + "/" + localAgent);
        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalInit(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.GLOBAL_INIT;
				currentDesc = desc;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				publishStanding();
			}

		// the top of the fabric: its config sets the fabric-wide crypto policy (CryptoPolicySync)
		io.cresco.agent.controller.statemachine.CryptoPolicySync.becomeAuthority(plugin, logger, localRegion + "/" + localAgent);
        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalFailed(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.GLOBAL_FAILED;
				currentDesc = desc;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				this.regionalAgent = localAgent;
				this.regionalRegion = localRegion;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setGlobalShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.GLOBAL_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)) {
				synchronized (this) {
					currentMode = ControllerMode.GLOBAL_SHUTDOWN;
					currentDesc = desc;
					publishStanding();
				}

			return true;
		} else {
			return false;
		}
	}

	public boolean setRegionalGlobalSuccess(String globalRegion, String globalAgent, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION_GLOBAL;
				currentDesc = desc;
				this.globalRegion = globalRegion;
				this.globalAgent = globalAgent;
				publishStanding();
			}

		// below a parent: the global's crypto policy governs; local crypto policy keys are ignored
		io.cresco.agent.controller.statemachine.CryptoPolicySync.warnIfLocalPolicyIgnored(plugin, logger);
        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setRegionShutdown(String desc) {

		if(controllerStatePersistance.setControllerState(ControllerMode.REGION_SHUTDOWN, desc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent)) {
				synchronized (this) {
					currentMode = ControllerMode.REGION_SHUTDOWN;
					currentDesc = desc;
					publishStanding();
				}

			return true;
		} else {
			return false;
		}
	}

	public boolean setRegionalGlobalFailed(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.REGION_GLOBAL_FAILED;
				currentDesc = desc;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneInit(String region, String agent, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.STANDALONE_INIT;
				currentDesc = desc;
				localAgent = agent;
				localRegion = region;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneSuccess(String region, String agent, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.STANDALONE;
				currentDesc = desc;
				localAgent = agent;
				localRegion = region;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

		// the top of the fabric: its config sets the fabric-wide crypto policy (CryptoPolicySync)
		io.cresco.agent.controller.statemachine.CryptoPolicySync.becomeAuthority(plugin, logger, localRegion + "/" + localAgent);
        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneFailed(String region, String agent, String desc) {
			synchronized (this) {
				currentMode = ControllerMode.STANDALONE_FAILED;
				currentDesc = desc;
				localAgent = agent;
				localRegion = null;
				regionalRegion = null;
				regionalAgent = null;
				globalAgent = null;
				globalRegion = null;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

	public boolean setStandaloneShutdown(String desc) {
			synchronized (this) {
				currentMode = ControllerMode.STANDALONE_SHUTDOWN;
				currentDesc = desc;
				publishStanding();
			}

        return controllerStatePersistance.setControllerState(currentMode, currentDesc, globalRegion, globalAgent, regionalRegion, regionalAgent, localRegion, localAgent);
	}

}