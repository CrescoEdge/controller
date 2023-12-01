package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {
	public Map<String,BrokerStatusType> addressMap;
	private BrokerStatusType brokerStatus;

	public DiscoveryNode brokerNode;
	public String URI;

	public BrokerMonitor bm;
	private PluginBuilder plugin;
	private CLogger logger;
	private ControllerEngine controllerEngine;

	public BrokeredAgent(ControllerEngine controllerEngine, DiscoveryNode brokerNode) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(BrokeredAgent.class.getName(),CLogger.Level.Info);
		this.brokerNode = brokerNode;
		logger.debug("Initializing: " + brokerNode.getDiscoveredPath() + " address: " + brokerNode.discovered_ip);
		this.bm = new BrokerMonitor(controllerEngine, brokerNode.getDiscoveredPath());
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<>();
		this.addressMap.put(brokerNode.discovered_ip, BrokerStatusType.INIT);
	}

	public String getActiveAddress() {
		return brokerNode.discovered_ip;
	}

	public void setActiveAddress(String aciveAddress) {
		this.brokerNode.discovered_ip = aciveAddress;
	}

	public String getPath() {
		return this.brokerNode.getDiscoveredPath();
	}


	public BrokerStatusType getBrokerStatus() {
		return this.brokerStatus;
	}

	public void setBrokerStatus(BrokerStatusType brokerStatus) {

		switch (brokerStatus) {

			case INIT:
				logger.debug("BROKER STATUS = INIT");
				this.brokerStatus = BrokerStatusType.INIT;
				break;
			case STARTING:
				logger.debug("BROKER STATUS = STARTING");
				this.brokerStatus = BrokerStatusType.STARTING;
				addressMap.put(brokerNode.discovered_ip, BrokerStatusType.STARTING);
				setStarting();
				break;
			case ACTIVE:
				logger.debug("BROKER STATUS = ACTIVE");
				this.brokerStatus = BrokerStatusType.ACTIVE;
				addressMap.put(brokerNode.discovered_ip, BrokerStatusType.ACTIVE);
				break;
			case STOPPED:
				logger.debug("BROKER STATUS = STOPPED");
				setStopped();
				this.brokerStatus = BrokerStatusType.STOPPED;
				logger.debug("setStop : Broker STOP");
				break;
			case FAILED:
				logger.debug("BROKER STATUS = FAILED");
				this.brokerStatus = BrokerStatusType.FAILED;
				setStopped();
				checkIfGlobal();
				break;

			default:
				logger.debug("BROKER STATUS = " + brokerStatus);
				break;
		}

	}

	private void setStarting() {
		if(bm.MonitorActive) {
			bm.shutdown();
			logger.error("bm.MonitorActive : shutting down.. activeAddress: " + brokerNode.discovered_ip);
		}
		bm = new BrokerMonitor(controllerEngine, brokerNode.getDiscoveredPath());
		new Thread(bm).start();
		while(!bm.MonitorActive) {
			try {
				Thread.sleep(1000);
				logger.debug("waiting on monitor active ");
			} catch (Exception e) {
				logger.error("setStarting {}", e.getMessage());
			}
		}
	}

	private void setStopped() {
		logger.error("CALLING FROM BROKERED AGENT");
		if(bm.MonitorActive) {
			bm.shutdown();
		}
		while(bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				logger.error("setStop {}", e.getMessage());
			}
		}
	}

	private void checkIfGlobal() {

		if(controllerEngine.cstate.getControllerState() == ControllerMode.REGION_GLOBAL) {
			if (controllerEngine.cstate.getGlobalControllerPath().equals(getPath())) {
				logger.info("Global Controller Path Lost: " + getPath() + " alerting ControllerSM");
				controllerEngine.getControllerSM().globalControllerLost("BrokeredAgent: " + getPath() + " lost");
			}
		}

	}

}