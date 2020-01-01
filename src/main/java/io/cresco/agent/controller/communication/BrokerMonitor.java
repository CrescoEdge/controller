package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet6Address;
import java.net.InetAddress;

class BrokerMonitor implements Runnable {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private String agentPath;
	private NetworkConnector bridge;

	public boolean MonitorActive;

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
	    logger.trace("BrokerAddress: " + brokerAddress);
		boolean isConnected = false;
		try {
			if((InetAddress.getByName(brokerAddress) instanceof Inet6Address)) {
				brokerAddress = "[" + brokerAddress + "]";
			}
			bridge = controllerEngine.getBroker().AddNetworkConnector(brokerAddress);
			bridge.start();
            logger.trace("Starting Bridge: " + bridge.getBrokerName() + " brokerAddress: " + brokerAddress);
			int connect_count = 0;

			while((connect_count++ < 10) && !bridge.isStarted()) {
				Thread.sleep(1000);
                logger.trace("Wating on Bridge to Start: " + bridge.getBrokerName());
			}
            logger.trace("Bridge.isStarted: " + bridge.isStarted() + " brokerName: " + bridge.getBrokerName() + " name: " + bridge.getName());

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
			while((connect_count++ < 10) && !isConnected) {
                logger.trace("ActiveBridge Count: " + bridge.activeBridges().size());

                for(NetworkBridge b : bridge.activeBridges()) {
                    String remoteBroker = b.getRemoteBrokerName();

                    logger.trace("RemoteBroker: " + b.getRemoteBrokerName() + " Remote Address: " + b.getRemoteAddress() + " Local Address: " + b.getLocalAddress() + " Local Name: " + b.getLocalBrokerName() + " Remote ID: " + b.getRemoteBrokerId() );
					if(remoteBroker != null) {
                        logger.trace("RemoteBroker: " + remoteBroker + " agentPath: " + agentPath);
                        if(remoteBroker.equals(agentPath)) {
	    					isConnected = true;
	    				}
					}
					Thread.sleep(1000);
				}
			}

        } catch(Exception ex) {
			logger.error(getClass().getName() + " connectToBroker Error " + ex.toString());
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			logger.error(sw.toString()); // stack trace as a string
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
		controllerEngine.getBrokeredAgents().get(agentPath).setBrokerStatus(BrokerStatusType.FAILED);
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

            while (MonitorActive) {
				MonitorActive = false;
				for (NetworkBridge b : bridge.activeBridges()) {
				    logger.trace("Check Broker Name: " + b.getRemoteBrokerName() + " for agentPath: " + agentPath);
					logger.trace("found bridge[" + b + "] to " + b.getRemoteBrokerName() + " on broker :" + b.getLocalBrokerName());

					//agentcontroller.sendAPMessage(MsgEvent);
                    //if (b.getRemoteBrokerName().equals(agentPath)) {
					    MonitorActive = true;
					//}

                }
				Thread.sleep(5000);
			}
			logger.trace("agentpath: " + agentPath + " is being shutdown");
			shutdown();
		} catch(Exception ex) {
			logger.error("Run {}", ex.getMessage());
			StringWriter errors = new StringWriter();
			ex.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}
}