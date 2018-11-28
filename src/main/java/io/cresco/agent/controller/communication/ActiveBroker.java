package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ServiceStopper;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class ActiveBroker {
	private CLogger logger;
	private TransportConnector connector;
	//private CrescoAuthenticationPlugin authenticationPlugin;
	//private CrescoAuthorizationPlugin authorizationPlugin;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	public SslBrokerService broker;

	public ActiveBroker(ControllerEngine controllerEngine, String brokerName, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveBroker.class.getName(),CLogger.Level.Info);


		try {

			int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port",32010);

			if(portAvailable(discoveryPort)) {


				/*
				SystemUsage systemUsage = new SystemUsage();
				systemUsage.setSendFailIfNoSpace(true);

				MemoryUsage memoryUsage = new MemoryUsage();
				memoryUsage.setUsage(10000);

				StoreUsage storeUsage = new StoreUsage();
				storeUsage.setLimit(1000000000);

				systemUsage.setMemoryUsage(memoryUsage);
				systemUsage.setStoreUsage(storeUsage);
				*/

				PolicyEntry entry = new PolicyEntry();
		        entry.setGcInactiveDestinations(true);
		        entry.setInactiveTimeoutBeforeGC(15000);

				ManagementContext mc = new ManagementContext();
				mc.setSuppressMBean("endpoint=dynamicProducer,endpoint=Consumer");

		        /*

				entry.setProducerFlowControl(true);
				entry.setQueue(">");
				entry.setMemoryLimit(1000000000);
				entry.setTopic(">");
				entry.setAllConsumersExclusiveByDefault(true);
				entry.setAdvisoryWhenFull(true);
				*/

		        /*
		        <beans
  <amq:broker useJmx="false" persistent="false">

    <amq:sslContext>
      <amq:sslContext
            keyStore="broker.ks" keyStorePassword="password"
            trustStore="client.ks" trustStorePassword="password"/>
    </amq:sslContext>

    <amq:transportConnectors>
      <amq:transportConnector uri="ssl://localhost:61616" />
    </amq:transportConnectors>

  </amq:broker>
</beans>
		         */

				SslContext sslContextBroker = new SslContext();
				SSLContext sslContext = sslContextBroker.getSSLContext();
				//SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
				//SSLContext sslContext = SSLContext.getInstance("TLS");
				//SSLContext sslContext = SSLContext.getInstance("Default");
				sslContext.init(controllerEngine.getCertificateManager().getKeyManagers(), controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());
				sslContextBroker.setSSLContext(sslContext);
				logger.info("Initialized SSL Context");

				PolicyMap map = new PolicyMap();
		        map.setDefaultEntry(entry);

				broker = new SslBrokerService();
				broker.setUseShutdownHook(true);
				broker.setPersistent(false);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(2500);
				broker.setDestinationPolicy(map);
				broker.setManagementContext(mc);
				broker.setSslContext(sslContextBroker);
				broker.setPopulateJMSXUserID(true);
				broker.setUseAuthenticatedPrincipalForJMSXUserID(true);


				LoggingBrokerPlugin lbp = new LoggingBrokerPlugin();
				lbp.setLogAll(false);
				lbp.setLogConnectionEvents(false);
				lbp.setLogConsumerEvents(false);
				lbp.setLogProducerEvents(false);
				lbp.setLogInternalEvents(false);
				lbp.setLogSessionEvents(false);
				lbp.setLogTransactionEvents(false);
				lbp.setPerDestinationLogger(false);


				//broker.setPlugins(new BrokerPlugin[]{lbp});
				//LoggingBrokerPlugin
				//LoggingBrokerPlugin
				/*
				broker.setUseJmx(true);
				broker.getManagementContext().setConnectorPort(2099);
				broker.getManagementContext().setCreateConnector(true);
                */

				//authorizationPlugin = new CrescoAuthorizationPlugin();
				//authenticationPlugin = new CrescoAuthenticationPlugin();
				//broker.setPlugins(new BrokerPlugin[]{authorizationPlugin,authenticationPlugin});
				//<amq:transportConnector uri="ssl://localhost:61616" />

				connector = new TransportConnector();



				if (plugin.isIPv6())
					//connector.setUri(new URI("ssl://[::]:"+ discoveryPort + "?transport.verifyHostName=false"));
					connector.setUri(new URI("ssl://[::]:"+ discoveryPort));

				else
					//connector.setUri(new URI("ssl://0.0.0.0:"+ discoveryPort + "?transport.verifyHostName=false"));
					connector.setUri(new URI("ssl://0.0.0.0:"+ discoveryPort));


                /*
                connector.setUpdateClusterClients(true);
                connector.setRebalanceClusterClients(true);
                connector.setUpdateClusterClientsOnRemove(true);
                */

				broker.addConnector(connector);

				logger.info("Starting Broker");

				broker.start();

				while(!broker.isStarted()) {
			    	Thread.sleep(1000);
                }
				//addUser(brokerUserNameAgent,brokerPasswordAgent,"agent");
				//addPolicy(">", "agent");


			} else {
				//todo Figure out some way to run more than one agent per instance if needed
				logger.error("Constructor : portAvailable("+ discoveryPort +") == false");
				logger.error("Shutting down!");
				System.exit(0);
			}
		} catch(Exception ex) {
			//ex.printStackTrace();
			logger.error("Init {}" + ex.getMessage());
		}
	}

	public void updateTrustManager() {
		try {
			broker.getSslContext().getSSLContext().init(controllerEngine.getCertificateManager().getKeyManagers(), controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());

		} catch(Exception ex) {
			System.out.println("updateTrustManager() : Error " + ex.getMessage());
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
			ServiceStopper stopper = new ServiceStopper();
			//broker.getManagementContext().stop();
			for(TransportConnector tc : broker.getTransportConnectors()) {
				tc.stop();

			}
			connector.stop();
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
			int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port",32010);
			logger.info("Added Network Connector to Broker URI: static:ssl://" + URI + ":" + discoveryPort);
			logger.trace("URI: static:ssl://" + URI + ":" + discoveryPort + " brokerUserName: " + brokerUserName + " brokerPassword: " + brokerPassword);
			//bridge = broker.addNetworkConnector(new URI("static:ssl://" + URI + ":"+ discoveryPort + "?transport.verifyHostName=false"));
			bridge = broker.addNetworkConnector(new URI("static:ssl://" + URI + ":"+ discoveryPort));

			bridge.setUserName(brokerUserName);
            bridge.setPassword(brokerPassword);
			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);
			updateTrustManager();

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