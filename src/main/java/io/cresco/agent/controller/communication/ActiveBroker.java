package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.*;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.PrefetchRatePendingMessageLimitStrategy;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.io.FileUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;

public class ActiveBroker {
	private CLogger logger;
	private TransportConnector connector;
	//private CrescoAuthenticationPlugin authenticationPlugin;
	//private CrescoAuthorizationPlugin authorizationPlugin;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private SslBrokerService broker;
	private final String transport;
	private String verifyTransport = "";

	public ActiveBroker(ControllerEngine controllerEngine, String brokerName) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveBroker.class.getName(),CLogger.Level.Info);
		transport = plugin.getConfig().getStringParam("activemq_transport", "nio+ssl");
		if(transport.contains("ssl")) {
			verifyTransport = "?verifyHostName=false";
		}

		try {

			boolean enable_broker_transport = plugin.getConfig().getBooleanParam("enable_broker_transport", true);
			boolean enable_dynamic_broker_port = plugin.getConfig().getBooleanParam("enable_dynamic_broker_port", true);

			int brokerPort = getBrokerPort();

			boolean isPortAvailable = false;
			//check if transport is enabled, otherwise bypass
			if(enable_broker_transport) {

				if(enable_dynamic_broker_port) {
					while (!portAvailable(brokerPort)) {
						brokerPort++;
					}
				}

				isPortAvailable = portAvailable(brokerPort);
			} else {
				isPortAvailable = true;
			}

			if(isPortAvailable) {

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

				entry.setQueue(">");
				//enable prioritization of messages in queues
				entry.setPrioritizedMessages(true);
				entry.setProducerFlowControl(true);


				entry.setTopic(">");
				//enable prioritization of messages in queues
				entry.setPrioritizedMessages(true);
				entry.setProducerFlowControl(true);
				//configure prefetch rate ratio to prevent exhaustion of resources from slow consumers
				int topicPrefetchLimit = plugin.getConfig().getIntegerParam("topic_prefetch_limit",100);
				entry.setTopicPrefetch(topicPrefetchLimit);
				//configure prefetch rate ratio to prevent exhaustion of resources from slow consumers
				PrefetchRatePendingMessageLimitStrategy preFetchRate = new PrefetchRatePendingMessageLimitStrategy();
				preFetchRate.setMultiplier(plugin.getConfig().getDoubleParam("prefetch_rate_multiplier",2.5));
				entry.setPendingMessageLimitStrategy(preFetchRate);
				//configure exclusive consumers
				boolean allConsumersExclusive = plugin.getConfig().getBooleanParam("all_consumers_exclusive",true);
				entry.setAllConsumersExclusiveByDefault(allConsumersExclusive);

				//entry.setProducerFlowControl(true);
				//entry.setOptimizedDispatch(true);
				//entry.setProducerFlowControl(true);
				//entry.setAdvisoryWhenFull(true);
				//ManagementContext mc = new ManagementContext();
				//mc.setSuppressMBean("endpoint=dynamicProducer,endpoint=Consumer");

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

		        //String jarPath = ControllerEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();


				//File jarLocation = new File(ControllerEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
				//String parentDirName = jarLocation.getParent(); // to get the parent dir name

				///Users/vcbumg2/IdeaProjects/agent/target/agent-1.0-SNAPSHOT.jar!

				//String agentJar = Paths.get(ControllerEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).toFile().getParent();
				//String dataDir = agentJar.substring(0,agentJar.lastIndexOf("/")) + "/cresco-data/";

				String cresco_data_location = System.getProperty("cresco_data_location");
				if(cresco_data_location != null) {
					Path path = Paths.get(cresco_data_location, "activemq-data");
					FileUtils.deleteDirectory(Paths.get(path.toAbsolutePath().normalize().toString()).toFile());
					System.setProperty("org.apache.activemq.default.directory.prefix", cresco_data_location + System.getProperty("file.separator"));
				} else {
					FileUtils.deleteDirectory(Paths.get("cresco-data/activemq-data").toFile());
					System.setProperty("org.apache.activemq.default.directory.prefix", "cresco-data/");
				}



				broker = new SslBrokerService();
				//broker.setUseShutdownHook(true);
				broker.setUseShutdownHook(false);
				broker.setPersistent(true);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(2500);
				broker.setDestinationPolicy(map);
				//broker.setManagementContext(mc);
				broker.setSslContext(sslContextBroker);

				broker.setPopulateJMSXUserID(true);
				//broker.setUseJmx(false);

				broker.setUseAuthenticatedPrincipalForJMSXUserID(true);

				//broker.getTempDataStore().setDirectory(Paths.get("cresco.data").toFile());
				/*
				By default, ActiveMQ uses a dedicated thread per destination. If there are large numbers of Destinations there will be a large number of threads and
				their associated memory resource usage. ActiveMQ can be configured to use a thread pool through the use of the system property
				 */
				//Performance greatly suffered under load
				//broker.setDedicatedTaskRunner(true);

				/*
				LoggingBrokerPlugin lbp = new LoggingBrokerPlugin();
				lbp.setLogAll(false);
				lbp.setLogConnectionEvents(false);
				lbp.setLogConsumerEvents(false);
				lbp.setLogProducerEvents(false);
				lbp.setLogInternalEvents(false);
				lbp.setLogSessionEvents(false);
				lbp.setLogTransactionEvents(false);
				lbp.setPerDestinationLogger(false);
				 */


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

				if(enable_broker_transport) {

					logger.info("Broker transport " + transport + " on port: " + brokerPort);
					connector = new TransportConnector();

					//try for connector
					connector.setUpdateClusterClients(true);
					connector.setUpdateClusterClientsOnRemove(true);

					if (plugin.isIPv6())
						connector.setUri(new URI(transport + "://[::]:" + brokerPort + "?daemon=true"));

					else
						connector.setUri(new URI(transport + "://0.0.0.0:" + brokerPort + "?daemon=true"));

					broker.addConnector(connector);

				}


				logger.info("Starting Broker");

				broker.start();

				while(!broker.isStarted()) {
			    	Thread.sleep(1000);
                }
				//addUser(brokerUserNameAgent,brokerPasswordAgent,"agent");
				//addPolicy(">", "agent");


			} else {
				//In the future we might need to figure out some way to run more than one agent per instance if needed
				logger.error("Constructor : portAvailable("+ brokerPort +") == false");
				logger.error("Shutting down!");
				System.exit(0);
			}
		} catch(Exception ex) {
			//ex.printStackTrace();
			logger.error("Init {}" + ex.getMessage());
		}
	}

	public int getBrokerPort() {
		return plugin.getConfig().getIntegerParam("broker_port",32010);
	}

	public ActiveMQDestination[] getBrokerDestinations() {
		ActiveMQDestination[] destinations = null;
		try {
			destinations = broker.getBroker().getDestinations();
		} catch (Exception ex) {
			logger.error("getDestinations() " + ex.getMessage());
		}

		return destinations;
	}

	public ActiveMQDestination[] getRegionalBrokerDestinations() {
		ActiveMQDestination[] destinations = null;
		try {
			destinations = broker.getRegionBroker().getDestinations();
		} catch (Exception ex) {
			logger.error("getDestinations() " + ex.getMessage());
		}

		return destinations;
	}

	public void updateTrustManager() {
		try {
			broker.getSslContext().getSSLContext().init(controllerEngine.getCertificateManager().getKeyManagers(), controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());

		} catch(Exception ex) {
			logger.error("updateTrustManager() : Error " + ex.getMessage());
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

			/*
			ServiceStopper stopper = new ServiceStopper();

			//broker.getManagementContext().stop();
			logger.error("Stopping transport connectors");
			for(TransportConnector tc : broker.getTransportConnectors()) {
				logger.error("Stopping " + tc.getName() );
				tc.stop();
			}
			logger.error("Stopping remaining connectors");
			broker.stopAllConnectors(stopper);
			 */

			/*
			for(Connection connection : broker.getRegionBroker().getClients()) {
				connection.stop();
			}

			for(Connection connection : broker.getBroker().getClients()) {
				connection.stop();
			}

			 */


			/*
			broker.getRegionBroker().getScheduler().shutdown();
			broker.getRegionBroker().stop();
			while(!broker.getRegionBroker().isStopped()) {
				logger.error("Waiting until Regional Broker Stop");
			}

			 */

			//broker.getBroker().stop();
			//broker.getBroker().getScheduler().shutdown();

			//broker.getScheduler().shutdown();
			broker.getRegionBroker().getScheduler().shutdown();
			broker.getBroker().getScheduler().shutdown();
			broker.getRegionBroker().stop();
			broker.getBroker().stop();
            broker.stop();
			broker.waitUntilStopped();
			while(!broker.isStopped()) {
				logger.error("Waiting until Broker Stop");
			}

			logger.debug("Broker Stopped: " + broker.isStopped());

			/*
			broker.getScheduler().shutdown();
			while(!broker.getScheduler().isStopped()) {
				logger.error("Waiting until Broker Scheduler is Stop");
			}

			broker.getRegionBroker().getScheduler().shutdown();
			while(!broker.getRegionBroker().getScheduler().isStopped()) {
				logger.error("Waiting until Regional Broker Scheduler is Stop");
			}
			 */


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

	public NetworkConnector AddNetworkConnector(String hostname) {
		NetworkConnector bridge = null;
		try {

			int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port_remote",32010);

			URI uri = new URI("static:(" + transport +"://" + hostname + ":"+ discoveryPort + verifyTransport + ")?maxReconnectAttempts=" + plugin.getConfig().getStringParam("max_reconnect_attempts","5") + "&initialReconnectDelay=" + plugin.getConfig().getStringParam("failover_reconnect_delay","5000") + "&useExponentialBackOff=" + plugin.getConfig().getStringParam("use_exponential_backOff","false"));

			logger.debug("Connector URI: " + uri);

			bridge = broker.addNetworkConnector(uri);

			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);
			updateTrustManager();


		} catch(Exception ex) {
			logger.error("NetworkConnector AddNetworkConnector: {}", ex.getMessage());
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			logger.error(sw.toString());
		}
		return bridge;
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