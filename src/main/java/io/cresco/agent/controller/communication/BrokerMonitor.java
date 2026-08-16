package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import java.net.Inet6Address;
import java.net.InetAddress;

class BrokerMonitor implements Runnable {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private String agentPath;
	private NetworkConnector bridge;
	private String monitoredHost;

	public volatile boolean MonitorActive;

	public BrokerMonitor(ControllerEngine controllerEngine, String agentPath) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(BrokerMonitor.class.getName(),CLogger.Level.Info);

		this.agentPath = agentPath;
	}

	public void shutdown() {
		stopBridge(); //kill bridge
		MonitorActive = false;
	}

	public boolean connectToBroker(String brokerAddress, String agentPath) {
	    logger.trace("connectToBroker() BrokerAddress: " + brokerAddress);
		boolean isConnected = false;
		try {
			if((InetAddress.getByName(brokerAddress) instanceof Inet6Address)) {
				brokerAddress = "[" + brokerAddress + "]";
			}
			this.monitoredHost = brokerAddress;
			bridge = controllerEngine.getBroker().AddNetworkConnector(brokerAddress);
			bridge.start();


			logger.info("Starting Bridge: " + bridge.getBrokerName() + " brokerAddress: " + brokerAddress);
			int connect_count = 0;

			while((connect_count++ < 10) && !bridge.isStarted()) {
				Thread.sleep(1000);
                logger.trace("Wating on Bridge to Start: " + bridge.getBrokerName());
			}
            logger.debug("Bridge \nisStarted: " + bridge.isStarted() + " \nbrokerName: " + bridge.getBrokerName() + " \nname: " + bridge.getName());

			//
            //Send a message

			/*
            List<ActiveMQDestination> dest = bridge.getDynamicallyIncludedDestinations();
            //dest.addAll(bridge.getDurableDestinations());
            for(ActiveMQDestination ades : dest) {
                logger.trace("MQDEST: " + ades.getPhysicalName() + " " + ades.getQualifiedName() + " " + ades.isQueue());
            }

            Set<ActiveMQDestination> dests = bridge.getDurableDestinations();
            for(ActiveMQDestination ades : dests) {
                logger.trace("MQDESTS: " + ades.getPhysicalName() + " " + ades.getQualifiedName() + " " + ades.isQueue());
            }
			*/

            if (connect_count >= 10 && !bridge.isStarted()) {
				throw new Exception("Failed to start bridge after 10 attempts. Aborting.");
			}

            connect_count = 0;
			while((connect_count++ < 5) && !isConnected) {
                logger.debug("ActiveBridge Count: " + bridge.activeBridges().size() + " isStarted:" + bridge.isStarted() + " isStopped: " + bridge.isStopped());

                for(NetworkBridge b : bridge.activeBridges()) {
                    String remoteBroker = b.getRemoteBrokerName();

                    logger.debug("RemoteBroker: " + b.getRemoteBrokerName() + " Remote Address: " + b.getRemoteAddress() + " Local Address: " + b.getLocalAddress() + " Local Name: " + b.getLocalBrokerName() + " Remote ID: " + b.getRemoteBrokerId() );
					if(remoteBroker != null) {
                        logger.debug("RemoteBroker: " + remoteBroker + " agentPath: " + agentPath);
                        if(remoteBroker.equals(agentPath)) {
	    					isConnected = true;
	    				}
					}
				}
				Thread.sleep(1000);
			}

        } catch(Exception ex) {
			logger.error(getClass().getName() + " connectToBroker Error " + ex.toString(), ex);
		}
		return isConnected;
	}
	  
	public void stopBridge() {
		logger.trace("Stopping Bridge : " + agentPath);
		try {
			controllerEngine.getBroker().removeNetworkConnector(bridge);
		} catch (Exception e) {
			logger.error("stopBridge {}", e.getMessage());
		}
		//controllerEngine.getBrokeredAgents().get(agentPath).setBrokerStatus(BrokerStatusType.FAILED);
	}
	  
	public void run() {
		try {
		    /*
            while(this.agentcontroller.getBrokeredAgents().get(agentPath).brokerStatus == BrokerStatusType.STARTING) {
                logger.trace("Waiting on agentpath: " + agentPath + " brokerstatus: " + this.agentcontroller.getBrokeredAgents().get(agentPath).brokerStatus.toString());
                Thread.sleep(1000);
            }
		    */
            /*
			String brokerAddress = this.agentcontroller.getBrokeredAgents().get(agentPath).activeAddress;
			if (connectToBroker(brokerAddress)) { //connect to broker
				MonitorActive = true;
				this.agentcontroller.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.ACTIVE;
			}
            */

            String brokerAddress = controllerEngine.getBrokeredAgents().get(agentPath).getActiveAddress();

            logger.trace("Connecting to brokerAddress: " + brokerAddress);

            if (connectToBroker(brokerAddress, agentPath)) { //connect to broker
                MonitorActive = true;
                //this.agentcontroller.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.ACTIVE;
				controllerEngine.getBrokeredAgents().get(agentPath).setBrokerStatus(BrokerStatusType.ACTIVE);
                logger.trace("Connected to brokerAddress: " + brokerAddress);

			}

            // Liveness across the WHOLE connector group: with the control/data bridge split,
            // connector 0 (control) staying up while a data connector dies would silently
            // blackhole ALL topic forwarding — monitoring only the primary missed that entirely.
            // A member counts once it has connected at least once (everConnected), so a freshly
            // added AutoTuner connector that has not yet established does not flap the group.
            java.util.Set<String> everConnected = new java.util.HashSet<>();
            while (MonitorActive) {
                boolean primaryAlive = false;
                boolean memberLost = false;
                for (NetworkConnector nc : controllerEngine.getBroker().getBridgeGroup(monitoredHost)) {
                    boolean alive = !nc.activeBridges().isEmpty();
                    if (nc == bridge && alive) {
                        primaryAlive = true;
                    }
                    if (alive) {
                        everConnected.add(nc.getName());
                    } else if (everConnected.contains(nc.getName())) {
                        logger.warn("Bridge connector " + nc.getName() + " to " + agentPath + " lost its bridge; failing the group for rebuild.");
                        memberLost = true;
                    }
                }
                MonitorActive = primaryAlive && !memberLost;
				Thread.sleep(5000);
			}

			logger.trace("agentpath: " + agentPath + " is being shutdown");

            if(controllerEngine.getBrokeredAgents().containsKey(agentPath)) {
                controllerEngine.getBrokeredAgents().get(agentPath).setBrokerStatus(BrokerStatusType.FAILED);
            }

			shutdown();
		} catch(Exception ex) {
			logger.error("BrokerMonitor.run Run {}", ex.getMessage(), ex);
		}
	}
}