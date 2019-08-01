package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {
	public Map<String,BrokerStatusType> addressMap;
	public BrokerStatusType brokerStatus;
	public String activeAddress;
	public String agentPath;

    public String URI;
    public String brokerUsername;
    public String brokerPassword;

	public BrokerMonitor bm;
	private PluginBuilder plugin;
	private CLogger logger;
	private ControllerEngine controllerEngine;

	public BrokeredAgent(ControllerEngine controllerEngine, String agentPath, String activeAddress, String brokerUsername, String brokerPassword) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(BrokeredAgent.class.getName(),CLogger.Level.Info);
		logger.debug("Initializing: " + agentPath + " address: " + activeAddress);
		this.plugin = plugin;
		this.bm = new BrokerMonitor(controllerEngine, agentPath);
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
        this.brokerUsername = brokerUsername;
        this.brokerPassword = brokerPassword;
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<>();
		this.addressMap.put(activeAddress, BrokerStatusType.INIT);
	}

	public void setStop() {
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
		brokerStatus = BrokerStatusType.STOPPED;
		logger.debug("setStop : Broker STOP");
	}

	public void setStarting() {
		brokerStatus = BrokerStatusType.STARTING;
		addressMap.put(activeAddress, BrokerStatusType.STARTING);
		if(bm.MonitorActive) {
			bm.shutdown();
            logger.error("bm.MonitorActive : shutting down.. activeAddress: " + activeAddress);
		}
		bm = new BrokerMonitor(controllerEngine, agentPath);
		new Thread(bm).start();
		while(!bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				logger.error("setStarting {}", e.getMessage());
			}
		}
	}

	public void setActive() {
		brokerStatus = BrokerStatusType.ACTIVE;
		addressMap.put(activeAddress, BrokerStatusType.ACTIVE);
	}
}