package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

public class ActiveBrokerManager implements Runnable  {
	private PluginBuilder plugin;
	private CLogger logger;
	private Timer timer;
	private ControllerEngine controllerEngine;

	public ActiveBrokerManager(ControllerEngine controllerEngine) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveBrokerManager.class.getName(),CLogger.Level.Info);

		logger.debug("Active Broker Manger initialized");
		this.plugin = plugin;
		timer = new Timer();
		timer.scheduleAtFixedRate(new BrokerWatchDog(logger), 500, 5000);//remote
	}
	  
	public void shutdown() {
		logger.debug("Active Broker Manager shutdown initialized");
	}

	public void addBroker(String agentPath) {
		BrokeredAgent ba = controllerEngine.getBrokeredAgents().get(agentPath);
		//logger.error("addBroker: agentPath = " + agentPath + " status 0 = " + ba.brokerStatus.toString());
		if(ba.getBrokerStatus() == BrokerStatusType.INIT) {
			//Fire up new thread.
			ba.setBrokerStatus(BrokerStatusType.STARTING);
		}
		/*
		logger.error("addBroker: agentPath = " + agentPath + " status 1 = " + ba.brokerStatus.toString());
		if(ba.brokerStatus == BrokerStatusType.STARTING) {
			ba.setActive();
		}
		logger.error("addBroker: agentPath = " + agentPath + " status 2 = " + ba.brokerStatus.toString());
		*/
	}

	public void run() {
		logger.info("Initialized");
		controllerEngine.setActiveBrokerManagerActive(true);
		while(controllerEngine.isActiveBrokerManagerActive()) {
			try {
				DiscoveryNode discoveryNode = controllerEngine.getIncomingCanidateBrokers().take();

				if(discoveryNode != null) {

					//Poison Pill Shutdown, send null class if blocking on input
					if (discoveryNode.discovery_type != DiscoveryType.SHUTDOWN) {
						//String agentIP = cb.getParam("dst_ip");

						if ((!controllerEngine.isLocal(discoveryNode.discovered_ip)) || (discoveryNode.discovered_ip.equals("127.0.0.1")) || (discoveryNode.discovered_ip.equals("localhost"))) { //ignore local responses


							boolean addBroker = false;
							//String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
							logger.info("Trying to connect to: " + discoveryNode.getDiscoveredPath());
							//logger.trace(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;

							BrokeredAgent ba = null;
							if (controllerEngine.getBrokeredAgents().containsKey(discoveryNode.getDiscoveredPath())) {
								logger.error("brokered agents contains key for " + discoveryNode.getDiscoveredPath());
								ba = controllerEngine.getBrokeredAgents().get(discoveryNode.getDiscoveredPath());
								//add ip to possible list
								if (!ba.addressMap.containsKey(discoveryNode.discovered_ip)) {
									ba.addressMap.put(discoveryNode.discovered_ip, BrokerStatusType.INIT);
								}
								//reset status if needed
								if ((ba.getBrokerStatus() == BrokerStatusType.FAILED) || (ba.getBrokerStatus() == BrokerStatusType.STOPPED)) {
									ba.setActiveAddress(discoveryNode.discovered_ip);
									ba.setBrokerStatus(BrokerStatusType.INIT);
									addBroker = true;
									logger.info("BA EXIST ADDING agentPath: " + discoveryNode.discovered_ip + " remote_ip: " + discoveryNode.discovered_ip);
								}
								logger.info("BA EXIST ADDING agentPath: " + discoveryNode.getDiscoveredPath() + " remote_ip: " + discoveryNode.discovered_ip);

							} else {

								logger.error("brokered agents does not contains key for " + discoveryNode.getDiscoveredPath());

								ba = new BrokeredAgent(controllerEngine, discoveryNode);
								controllerEngine.getBrokeredAgents().put(discoveryNode.getDiscoveredPath(), ba);
								addBroker = true;
								logger.trace("BA NEW ADDING agentPath: " + discoveryNode.getDiscoveredPath() + " remote_ip: " + discoveryNode.getDiscoveredPath());
								addBroker = true;
							}
							//try and connect
							if (addBroker && !controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath())) {
								addBroker(discoveryNode.getDiscoveredPath());
								int count = 0;

								logger.trace("Waiting on Broker : " + discoveryNode.getDiscoveredPath() + " remote_ip: " + discoveryNode.discovered_ip + " count:" + count);
								logger.trace("Status : " + ba.getBrokerStatus().toString() + " URI : " + ba.URI + " Address : " + ba.getActiveAddress());
								logger.trace("isReachable : " + controllerEngine.isReachableAgent(discoveryNode.getDiscoveredPath()));
								Thread.sleep(1000);
								count++;

							} else {
								logger.info("Not Adding Broker : " + discoveryNode.getDiscoveredPath() + " remote_ip: " + discoveryNode.discovered_ip);
							}
						} else {
							logger.error("LOCAL DISCOVERY FOR BROKER");
						}
					}
				}
			}
			catch (Exception ex) {
				logger.error("Run {}", ex.getMessage());
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				ex.printStackTrace(pw);
				logger.error(sw.toString());
			}
		}
		timer.cancel();

		//stop monitoring all the brokers
		for (Map.Entry<String, BrokeredAgent> entry : controllerEngine.getBrokeredAgents().entrySet()) {
			String key = entry.getKey();
			BrokeredAgent ba = entry.getValue();
			logger.error("Stopping Brokered Agent [" + key + "]");
			ba.setBrokerStatus(BrokerStatusType.STOPPED);
		}


		logger.debug("Broker Manager has shutdown");
	}

	class BrokerWatchDog extends TimerTask {
		//private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);
        private CLogger logger;

        public BrokerWatchDog(CLogger logger) {
            this.logger = logger;
        }
		public void run() {

		    for (Entry<String, BrokeredAgent> entry : controllerEngine.getBrokeredAgents().entrySet()) {
				//logger.trace(entry.getKey() + "/" + entry.getValue());
				BrokeredAgent ba = entry.getValue();
				if(ba.getBrokerStatus() == BrokerStatusType.FAILED) {
					if (!controllerEngine.cstate.getGlobalControllerPath().equals(ba.getPath())) {
						logger.info("Controller Path Lost: " + ba.getPath() + " removing path");
						controllerEngine.getBrokeredAgents().remove(entry.getKey());//remove agent
					}
				}
                logger.trace("Brokered Agents: " + ba.getPath());
			}
		}
	}
}