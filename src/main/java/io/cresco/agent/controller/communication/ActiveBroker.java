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
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.commons.io.FileUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;

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
                entry.setMemoryLimit(64 * 1024 * 1024); // 64 MB memory limit per destination
                // Dispatch cache: keeps recent messages in memory for fast dispatch instead of always
                // reading from the store. Cresco keeps this OFF by design (default false): benchmarking
                // showed it gives NO throughput gain on the single-node vm:// dataplane/control paths
                // (identical MB/s cache on vs off), while an in-memory cache adds memory pressure under
                // the flow-control-off + generous-systemUsage policy used here. Set activemq_use_cache=true
                // only if a future persistent/multi-node workload measurably benefits.
                boolean useCache = plugin.getConfig().getBooleanParam("activemq_use_cache", false);

                // Queue
				entry.setQueue(">");
				//enable prioritization of messages in queues
				entry.setPrioritizedMessages(true);
				// QoS: producer flow control OFF so a backed-up destination never BLOCKS the producer
				// (blocking would stall the liveness ping queued behind bulk, and priority does NOT
				// bypass producer-side flow control). Priority now governs dispatch order; the
				// pending-message-limit strategy below bounds NON-persistent memory (telemetry);
				// persistent traffic (liveness/control/bulk) pages to the store.
				entry.setProducerFlowControl(false);
                entry.setUseCache(useCache);
                entry.setPrioritizedMessages(true);
                //entry.setExpireMessagesPeriod(0);

                int queuePrefetchLimit = plugin.getConfig().getIntegerParam("queue_prefetch_limit",100);
                entry.setTopicPrefetch(queuePrefetchLimit);

                PrefetchRatePendingMessageLimitStrategy queuePreFetchRate = new PrefetchRatePendingMessageLimitStrategy();
                queuePreFetchRate.setMultiplier(plugin.getConfig().getDoubleParam("prefetch_rate_multiplier",2.5));
                entry.setPendingMessageLimitStrategy(queuePreFetchRate);
                //configure exclusive consumers
                boolean queueAllConsumersExclusive = plugin.getConfig().getBooleanParam("queue_all_consumers_exclusive",true);
                entry.setAllConsumersExclusiveByDefault(queueAllConsumersExclusive);

                // Topics
                entry.setTopic(">");
				//enable prioritization of messages in queues
				entry.setPrioritizedMessages(true);
				// QoS: see queue note above -- flow control OFF, priority governs, non-persistent bounded by eviction.
				entry.setProducerFlowControl(false);
                entry.setUseCache(useCache);
                entry.setPrioritizedMessages(true);
                //entry.setExpireMessagesPeriod(0);

				//configure prefetch rate ratio to prevent exhaustion of resources from slow consumers
				int topicPrefetchLimit = plugin.getConfig().getIntegerParam("topic_prefetch_limit",100);
				entry.setTopicPrefetch(topicPrefetchLimit);
				//configure prefetch rate ratio to prevent exhaustion of resources from slow consumers
				PrefetchRatePendingMessageLimitStrategy topicPreFetchRate = new PrefetchRatePendingMessageLimitStrategy();
				topicPreFetchRate.setMultiplier(plugin.getConfig().getDoubleParam("prefetch_rate_multiplier",2.5));
				entry.setPendingMessageLimitStrategy(topicPreFetchRate);
				//configure exclusive consumers
				boolean topicAllConsumersExclusive = plugin.getConfig().getBooleanParam("topic_all_consumers_exclusive",true);
				entry.setAllConsumersExclusiveByDefault(topicAllConsumersExclusive);

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

				// KahaDB with CONFIGURABLE journal disk syncs. Cresco uses persistence as a flow-control
				// mechanism, not for crash durability -- so disk syncs default OFF, removing the per-write
				// fsync latency that was the persistent-bulk throughput bottleneck (~50x on 256KB bulk in
				// broker-bench). Set activemq_journal_disk_syncs=true to restore fsync-per-write durability.
				KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
				String amqDataPrefix = (cresco_data_location != null ? cresco_data_location : "cresco-data");
				kahaDB.setDirectory(Paths.get(amqDataPrefix, "activemq-data", "kahadb").toFile());
				kahaDB.setEnableJournalDiskSyncs(plugin.getConfig().getBooleanParam("activemq_journal_disk_syncs", false));
				broker.setPersistenceAdapter(kahaDB);

				// Scaling: default task runner uses a THREAD PER DESTINATION -> thread blowup at large
				// agent counts. false = shared pool. Configurable in case a deployment prefers dedicated.
				broker.setDedicatedTaskRunner(plugin.getConfig().getBooleanParam("activemq_dedicated_task_runner", false));

				// Generous broker-wide usage so aggregate pressure from many agents/telemetry never
				// starves the control plane. With producer flow control OFF, these are soft ceilings:
				// non-persistent spills via the pending-message-limit eviction, persistent pages to store.
				SystemUsage systemUsage = new SystemUsage();
				MemoryUsage memoryUsage = new MemoryUsage();
				memoryUsage.setLimit(plugin.getConfig().getLongParam("broker_memory_limit", 256L * 1024 * 1024)); // 256 MB
				StoreUsage storeUsage = new StoreUsage();
				storeUsage.setLimit(plugin.getConfig().getLongParam("broker_store_limit", 8L * 1024 * 1024 * 1024)); // 8 GB
				TempUsage tempUsage = new TempUsage();
				tempUsage.setLimit(plugin.getConfig().getLongParam("broker_temp_limit", 4L * 1024 * 1024 * 1024)); // 4 GB
				systemUsage.setMemoryUsage(memoryUsage);
				systemUsage.setStoreUsage(storeUsage);
				systemUsage.setTempUsage(tempUsage);
				broker.setSystemUsage(systemUsage);
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

					// Throughput: default TCP socket buffers (~64KB) throttle large inter-node binary
					// (measured 44 -> 1300+ MB/s in broker-bench). Match the client's large buffers.
					// This is a SPEED knob, not backpressure -- TCP window + prefetch + usage limits still
					// bound a fast producer to a slow consumer. Configurable for slow-edge deployments.
					String txOpts = "?daemon=true"
							+ "&socketBufferSize=" + plugin.getConfig().getIntegerParam("activemq_socket_buffer_size", 2 * 1024 * 1024)
							+ "&wireFormat.maxFrameSize=" + plugin.getConfig().getLongParam("activemq_max_frame_size", 128L * 1024 * 1024);

					if (plugin.isIPv6())
						connector.setUri(new URI(transport + "://[::]:" + brokerPort + txOpts));

					else
						connector.setUri(new URI(transport + "://0.0.0.0:" + brokerPort + txOpts));

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
            int messageTTL = plugin.getConfig().getIntegerParam("broker_message_ttl",5);


            URI uri = new URI("static:(" + transport +"://" + hostname + ":"+ discoveryPort + verifyTransport + ")?maxReconnectAttempts=" + plugin.getConfig().getStringParam("max_reconnect_attempts","5") + "&initialReconnectDelay=" + plugin.getConfig().getStringParam("failover_reconnect_delay","5000") + "&useExponentialBackOff=" + plugin.getConfig().getStringParam("use_exponential_backOff","false"));

			logger.debug("Connector URI: " + uri);

			bridge = broker.addNetworkConnector(uri);

			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);
            bridge.setPrefetchSize(100);
            bridge.setNetworkTTL(messageTTL);
            bridge.setDecreaseNetworkConsumerPriority(false);
            bridge.setConduitSubscriptions(false);
            updateTrustManager();

		} catch(Exception ex) {
			logger.error("NetworkConnector AddNetworkConnector: {}", ex.getMessage(), ex);
		}
		return bridge;
	}

    public List<NetworkConnector> getNetworkConnectors() {
        return broker.getNetworkConnectors();
    }

    public List<Map<String,String>> getBridgedRegions() {
        List<Map<String, String>> returnBridgedRegions = new ArrayList<>();

        try {
            // Process destinations from the main broker
            ActiveMQDestination[] destinations = getBrokerDestinations();
            if (destinations != null) {
                for (ActiveMQDestination destination : destinations) {
                    if (destination.isQueue()) {

                        if (!broker.getBroker().getDestinationMap().get(destination).getConsumers().isEmpty()) {
                            String physicalName = destination.getPhysicalName();
                            String[] pathParts = physicalName.split("_");
                            if (pathParts.length == 2) {
                                Map<String, String> regionMap = new HashMap<>();
                                regionMap.put("region_id", pathParts[0]);
                                regionMap.put("agent_id", pathParts[1]);
                                returnBridgedRegions.add(regionMap);
                            }
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("Error during broker connection reconciliation: " + ex.getMessage());
        }
        return returnBridgedRegions;
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

    public boolean isPeerConnected(String agentPath) {
        return controllerEngine.getBrokeredAgents().containsKey(agentPath);
    }
}