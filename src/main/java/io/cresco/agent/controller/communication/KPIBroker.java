package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ServiceStopper;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KPIBroker {
	private CLogger logger;
	private TransportConnector connector;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;

	public BrokerService broker;

	public KPIBroker(ControllerEngine controllerEngine, String kpiProtocol, String kpiPort, String brokerName, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(KPIBroker.class.getName(),CLogger.Level.Info);

		//this.logger = new CLogger(ActiveBroker.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(),CLogger.Level.Info);
		logger.info("Initialized");
		try {
			if(portAvailable(32011)) {

				PolicyEntry entry = new PolicyEntry();
				entry.setGcInactiveDestinations(true);
				entry.setInactiveTimeoutBeforeGC(15000);


				PolicyMap map = new PolicyMap();
				map.setDefaultEntry(entry);

				broker = new BrokerService();
				broker.setUseShutdownHook(false);
				broker.setPersistent(false);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(2500);
				broker.setDestinationPolicy(map);

				/*
				broker.setUseJmx(true);
				broker.getManagementContext().setConnectorPort(2099);
				broker.getManagementContext().setCreateConnector(true);
                */

				//authorizationPlugin = new CrescoAuthorizationPlugin();
				//authenticationPlugin = new CrescoAuthenticationPlugin();
				//broker.setPlugins(new BrokerPlugin[]{authorizationPlugin,authenticationPlugin});

				connector = new TransportConnector();
				if (plugin.isIPv6())
					connector.setUri(new URI(kpiProtocol + "://[::]:" + kpiPort));
				else
					connector.setUri(new URI(kpiProtocol + "://0.0.0.0:" + kpiPort));

                /*
                connector.setUpdateClusterClients(true);
                connector.setRebalanceClusterClients(true);
                connector.setUpdateClusterClientsOnRemove(true);
                */

				broker.addConnector(connector);

				broker.start();


				while(!broker.isStarted()) {
					Thread.sleep(1000);
				}
				//addUser(brokerUserNameAgent,brokerPasswordAgent,"agent");
				//addPolicy(">", "agent");


			} else {
				logger.trace("Constructor : portAvailable(32011) == false");
			}
		} catch(Exception ex) {
			//ex.printStackTrace();
			logger.error("Init {}" + ex.getMessage());
		}
	}
/*
	public void addUser(String username, String password, String groups) {
		authenticationPlugin.addUser(username, password, groups);
	}

	public void removeUser(String username) {
		authenticationPlugin.removeUser(username);
	}

	public void addPolicy(String channelName, String groupName) {
		try {
			authorizationPlugin.addEntry(channelName, groupName);
		} catch (Exception e) {
			logger.error("addPolicy : {}", e.getMessage());
		}
	}

	public void removePolicy(String channelName) {
		authorizationPlugin.removeEntry(channelName);
	}
*/
	public boolean isHealthy() {
		boolean isHealthy = false;
		try  {
			if(broker.isStarted()) {
				isHealthy = true;
			}
		} catch (Exception e) {
			logger.error("isHealthy {}", e.getMessage());
		}
		return isHealthy;
	}

	public void stopBroker() {
		try {
			connector.stop();
			ServiceStopper stopper = new ServiceStopper();
			//broker.getManagementContext().stop();
			broker.stopAllConnectors(stopper);
			broker.stop();

			while(!broker.isStopped()) {
				Thread.sleep(1000);
			}
			logger.debug("Broker has shutdown");
		} catch (Exception e) {
			logger.error("stopBroker {}", e.getMessage());
		}

	}

	public boolean removeNetworkConnector(NetworkConnector bridge) {
		boolean isRemoved = false;
		try {
			bridge.stop();
			while(!bridge.isStopped()) {
				Thread.sleep(1000);
			}
			broker.removeNetworkConnector(bridge);
			isRemoved = true;
		}
		catch(Exception ex) {
			logger.error("removeNetworkConnector {}", ex.getMessage());
		}
		return isRemoved;

	}

	public NetworkConnector AddNetworkConnectorURI(String URI, String brokerUserName, String brokerPassword) {
		NetworkConnector bridge = null;
		try {
			logger.trace("URI: " + URI + " brokerUserName: " + brokerUserName + " brokerPassword: " + brokerPassword);
			bridge = broker.addNetworkConnector(new URI(URI));
			//RandomString rs = new RandomString(5);
			bridge.setUserName(brokerUserName);
			bridge.setPassword(brokerPassword);
			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);
			bridge.setDynamicOnly(true);
			bridge.setPrefetchSize(1);

		} catch(Exception ex) {
			logger.error("AddNetworkConnector {}", ex.getMessage());
		}
		return bridge;
	}

	public List<ActiveMQDestination> getDest(String agentPath) {
		List<ActiveMQDestination> dstList = new ArrayList<>();
		ActiveMQDestination dst = ActiveMQDestination.createDestination("queue://" + agentPath, ActiveMQDestination.QUEUE_TYPE);
		dstList.add(dst);
		return dstList;
	}

	public NetworkConnector AddNetworkConnector(String URI, String brokerUserName, String brokerPassword, String agentPath) {
		NetworkConnector bridge = null;
		try {
			logger.trace("URI: static:tcp://" + URI + ":32011" + " brokerUserName: " + brokerUserName + " brokerPassword: " + brokerPassword);
			bridge = broker.addNetworkConnector(new URI("static:tcp://" + URI + ":32011?useKeepAlive=true&keepAlive=true"));


			bridge.setUserName(brokerUserName);
			bridge.setPassword(brokerPassword);
			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);


		} catch(Exception ex) {
			logger.error("AddNetworkConnector {}", ex.getMessage());
		}
		return bridge;
	}

	public void AddTransportConnector(String URI) {
		try {
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));

			this.broker.addConnector(connector);
			this.broker.startTransportConnector(connector);
		} catch(Exception ex) {
			logger.error("AddTransportConnector {}", ex.getMessage());
		}
	}

	public boolean portAvailable(int port) {
		if (port < 0 || port > 65535) {
			throw new IllegalArgumentException("Invalid start port: " + port);
		}

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
			logger.error("portAvailable {}", e.getMessage());
		} finally  {
			if (ds != null)  {
				ds.close();
			}

			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e)  {
					/* should not be thrown */
					logger.error("portAvailable : finally {}", e.getMessage());
				}
			}
		}
		return false;
	}
}