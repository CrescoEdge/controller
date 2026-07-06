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
	private final String brokerName;

	// Parallel inter-broker bridge connections, keyed by remote host. A single duplex network
	// connector funnels all cross-node traffic through one TLS socket (one core) -> the multi-node
	// throughput cliff. Multiple connectors spread load across sockets/cores. Tracked per host so
	// they can be added/removed dynamically at runtime. broker_bridge_connections sets the default.
	private final Map<String, List<NetworkConnector>> bridgeGroups = new HashMap<>();

	public ActiveBroker(ControllerEngine controllerEngine, String brokerName) {
		this.controllerEngine = controllerEngine;
		this.brokerName = brokerName;
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


				// Every broker/destination tuning lever below is configurable; the defaults preserve
				// the shipped behavior. Nothing hardcoded -- each knob is overridable via plugin config.
				boolean producerFlowControl = plugin.getConfig().getBooleanParam("activemq_producer_flow_control", false);
				boolean prioritizedMessages = plugin.getConfig().getBooleanParam("activemq_prioritized_messages", true);
				long destinationMemoryLimit = plugin.getConfig().getLongParam("activemq_destination_memory_limit", 256L * 1024 * 1024);
				boolean gcInactiveDestinations = plugin.getConfig().getBooleanParam("activemq_gc_inactive_destinations", true);
				int inactiveTimeoutBeforeGC = plugin.getConfig().getIntegerParam("activemq_inactive_timeout_before_gc", 15000);

				PolicyEntry entry = new PolicyEntry();
		        entry.setGcInactiveDestinations(gcInactiveDestinations);
		        entry.setInactiveTimeoutBeforeGC(inactiveTimeoutBeforeGC);
                entry.setMemoryLimit(destinationMemoryLimit); // configurable per-destination memory limit
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
				entry.setProducerFlowControl(producerFlowControl);
                entry.setUseCache(useCache);
                entry.setPrioritizedMessages(prioritizedMessages);
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
				entry.setProducerFlowControl(producerFlowControl);
                entry.setUseCache(useCache);
                entry.setPrioritizedMessages(prioritizedMessages);
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
				broker.setPersistent(plugin.getConfig().getBooleanParam("activemq_persistent", true));
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(plugin.getConfig().getIntegerParam("activemq_destination_purge_period", 2500));
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
				// Non-persistent buffering headroom. The old 256MB fixed default filled under a few
				// concurrent large-message dataplane streams -> the pending-message eviction throttle
				// collapsed aggregate throughput (measured ~16 MB/s at 4x256KB streams vs ~470 with
				// headroom). Default to half the JVM max heap (floor 512MB) so concurrent streams don't
				// hit the eviction cliff; override with broker_memory_limit. Priority QoS still governs
				// dispatch order under pressure, so control traffic is not starved.
				long defaultBrokerMem = Math.max(512L * 1024 * 1024, Runtime.getRuntime().maxMemory() / 2);
				memoryUsage.setLimit(plugin.getConfig().getLongParam("broker_memory_limit", defaultBrokerMem));
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

				// Tenant isolation: install the Cresco authorization broker plugin so every consumer/
				// producer/send is checked against per-connection identity (see CrescoAuthorizationBroker
				// + TenantPolicy). OFF by default -> broker behaves exactly as before; enable per-region
				// with broker_security_enabled. Must be set before broker.start().
				if (plugin.getConfig().getBooleanParam("broker_security_enabled", false)) {
					broker.setPlugins(new org.apache.activemq.broker.BrokerPlugin[]{
							new CrescoAuthorizationBroker(plugin)
					});
					logger.info("Cresco broker security ENABLED — tenant authorization plugin installed");
				}
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

					// Mutual TLS: require every network client to present a certificate the broker's trust
					// managers validate, so identity is cryptographically bound (not a self-asserted
					// username). The CrescoAuthorizationBroker then derives the principal from the cert DN.
					// Gated separately from broker_security_enabled so authz can run without mTLS if wanted.
					if (plugin.getConfig().getBooleanParam("broker_require_client_auth", false)) {
						txOpts += "&needClientAuth=true";
						logger.info("Broker mutual-TLS ENABLED — clients must present a trusted certificate (needClientAuth)");
					}

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

	// Total pending (undispatched) message count across this broker's topics — the cheapest, highest-
	// signal native congestion indicator. Read in-process from ActiveMQ's DestinationViewMBean via the
	// platform MBeanServer (JMX is on by default; only the remote connector is disabled). A growing
	// backlog = a saturated downstream path; the AutoTuner + link:quality health check consume it.
	public long getBrokerPendingBacklog() {
		long total = 0;
		try {
			javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer();
			javax.management.ObjectName q = new javax.management.ObjectName(
					"org.apache.activemq:type=Broker,brokerName=" + brokerName + ",destinationType=Topic,destinationName=*");
			java.util.Set<javax.management.ObjectName> names = mbs.queryNames(q, null);
			for (javax.management.ObjectName on : names) {
				Object qs = mbs.getAttribute(on, "QueueSize");
				if (qs instanceof Number) total += ((Number) qs).longValue();
			}
			if (plugin.getConfig().getBooleanParam("net_metrics_log", false)) {
				logger.info("backlog JMX query: matched " + names.size() + " topic MBeans, total QueueSize=" + total);
			}
		} catch (Exception ex) {
			if (plugin.getConfig().getBooleanParam("net_metrics_log", false)) {
				logger.info("backlog JMX query FAILED: " + ex);
			}
			// JMX unavailable or broker not up yet -> report 0 (no signal)
		}
		return total;
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


	// Remove a bridge connector AND its parallel siblings (the whole per-host group). BrokerMonitor
	// holds only the primary; stopping it must tear down every parallel connector to that host.
	public boolean removeNetworkConnector(NetworkConnector bridge) {
		boolean isRemoved = false;
		try {
			List<NetworkConnector> toRemove;
			synchronized (bridgeGroups) {
				String host = null;
				for (Map.Entry<String, List<NetworkConnector>> e : bridgeGroups.entrySet()) {
					if (e.getValue().contains(bridge)) { host = e.getKey(); break; }
				}
				toRemove = (host != null) ? bridgeGroups.remove(host)
						: new ArrayList<>(java.util.Collections.singletonList(bridge));
			}
			for (NetworkConnector nc : toRemove) {
				try {
					nc.stop();
					int wait = 0;
					while(!nc.isStopped() && wait++ < 10) { Thread.sleep(1000); }
					broker.removeNetworkConnector(nc);
				} catch (Exception e) {
					logger.error("removeNetworkConnector member {}", e.getMessage());
				}
			}
			isRemoved = true;
		}
		catch(Exception ex) {
			logger.error("removeNetworkConnector {}", ex.getMessage());
		}
		return isRemoved;

	}

	// Create the bridge group to a remote host. Count comes from broker_bridge_connections (default 1,
	// = shipped single-connector behavior). Returns the PRIMARY connector (index 0) un-started so the
	// existing BrokerMonitor connect path starts+monitors it; any extra connectors are started here.
	public NetworkConnector AddNetworkConnector(String hostname) {
		int count = Math.max(1, plugin.getConfig().getIntegerParam("broker_bridge_connections", 1));
		// When the dataplane is sharded, use exactly one connector per shard so each shard forwards
		// over its own TLS socket (the destination filter in buildConnector binds connector i to
		// global.event.i). Without that 1:1 binding, only one connector actually forwards -> no gain.
		int shards = Math.max(1, plugin.getConfig().getIntegerParam("dataplane_shards", 1));
		if (shards > 1) count = shards;
		List<NetworkConnector> group = addBridgeConnectors(hostname, count, false);
		return group.isEmpty() ? null : group.get(0);
	}

	// Build one duplex connector to hostname. When sharding is on, PARTITION destinations so each
	// connector forwards exactly one shard-topic: connector i owns global.event.i; connector 0 also
	// carries everything else (control queues, advisories, unsharded topics) by excluding the other
	// shards. This gives one TLS socket per shard -> real cross-node parallelism, no duplicates.
	private NetworkConnector buildConnector(String hostname, int index) throws Exception {
		int discoveryPort = plugin.getConfig().getIntegerParam("discovery_port_remote",32010);
		int messageTTL = plugin.getConfig().getIntegerParam("broker_message_ttl",5);
		URI uri = new URI("static:(" + transport +"://" + hostname + ":"+ discoveryPort + verifyTransport + ")?maxReconnectAttempts=" + plugin.getConfig().getStringParam("max_reconnect_attempts","5") + "&initialReconnectDelay=" + plugin.getConfig().getStringParam("failover_reconnect_delay","5000") + "&useExponentialBackOff=" + plugin.getConfig().getStringParam("use_exponential_backOff","false"));
		NetworkConnector bridge = broker.addNetworkConnector(uri);
		bridge.setName("cresco-bridge-" + hostname + "-" + index + "-" + java.util.UUID.randomUUID());
		bridge.setDuplex(true);
		bridge.setPrefetchSize(plugin.getConfig().getIntegerParam("broker_bridge_prefetch", 100));
		bridge.setNetworkTTL(messageTTL);
		// Decrease a bridged consumer's priority by hop count so ActiveMQ's demand-forwarding prefers the
		// FEWEST-broker-hop path to a destination. Without this, a redundant mesh (e.g. R1 reachable both
		// directly over link C and via the global) forwards over an arbitrary bridge -- empirically the
		// multi-hop via-global path -- so the "short" direct link is never actually taken and there is
		// nothing for cost-aware routing to route around. With it on, the direct 1-hop bridge is ActiveMQ's
		// default; Cresco's source-route/cost selector then deliberately overrides that to the faster
		// multi-hop path when the direct link is the slow one. Configurable; default now on.
		bridge.setDecreaseNetworkConsumerPriority(
				plugin.getConfig().getBooleanParam("broker_bridge_decrease_consumer_priority", true));
		bridge.setConduitSubscriptions(false);

		int shards = Math.max(1, plugin.getConfig().getIntegerParam("dataplane_shards", 1));
		String shardBase = plugin.getConfig().getStringParam("dataplane_shard_topic", "global.event");
		if (shards > 1) {
			if (index == 0) {
				// connector 0 forwards everything EXCEPT the shards owned by the other connectors
				List<ActiveMQDestination> excluded = new ArrayList<>();
				for (int s = 1; s < shards; s++) {
					excluded.add(new org.apache.activemq.command.ActiveMQTopic(shardBase + "." + s));
				}
				bridge.setExcludedDestinations(excluded);
			} else if (index < shards) {
				// connector i forwards ONLY its shard topic
				List<ActiveMQDestination> mine = new ArrayList<>();
				mine.add(new org.apache.activemq.command.ActiveMQTopic(shardBase + "." + index));
				bridge.setDynamicallyIncludedDestinations(mine);
			}
		}
		return bridge;
	}

	// Add `count` connectors to a host's group. startAll=true starts every new connector (runtime add);
	// startAll=false leaves the very first primary un-started for BrokerMonitor, starting the rest.
	private List<NetworkConnector> addBridgeConnectors(String hostname, int count, boolean startAll) {
		synchronized (bridgeGroups) {
			List<NetworkConnector> group = bridgeGroups.computeIfAbsent(hostname, k -> new ArrayList<>());
			try {
				int startIndex = group.size();
				for (int i = 0; i < count; i++) {
					int idx = startIndex + i;
					NetworkConnector bridge = buildConnector(hostname, idx);
					group.add(bridge);
					if (startAll || idx > 0) {
						bridge.start();
					}
				}
				updateTrustManager();
			} catch (Exception ex) {
				logger.error("NetworkConnector addBridgeConnectors: {}", ex.getMessage(), ex);
			}
			return group;
		}
	}

	// --- Dynamic runtime control of bridge parallelism ---

	// Add `count` more parallel connectors to an already-bridged host, live. Returns the new group size.
	public int addBridgeConnections(String hostname, int count) {
		List<NetworkConnector> group = addBridgeConnectors(hostname, Math.max(0, count), true);
		logger.info("addBridgeConnections: host=" + hostname + " added=" + count + " total=" + group.size());
		return group.size();
	}

	// Remove up to `count` parallel connectors from a host (never drops below 1). Returns new group size.
	public int removeBridgeConnections(String hostname, int count) {
		synchronized (bridgeGroups) {
			List<NetworkConnector> group = bridgeGroups.get(hostname);
			if (group == null || group.isEmpty()) return 0;
			int removable = Math.max(0, Math.min(count, group.size() - 1)); // keep >=1
			for (int i = 0; i < removable; i++) {
				NetworkConnector nc = group.remove(group.size() - 1);
				try {
					nc.stop();
					int wait = 0;
					while (!nc.isStopped() && wait++ < 10) { Thread.sleep(1000); }
					broker.removeNetworkConnector(nc);
				} catch (Exception e) {
					logger.error("removeBridgeConnections member {}", e.getMessage());
				}
			}
			logger.info("removeBridgeConnections: host=" + hostname + " removed=" + removable + " total=" + group.size());
			return group.size();
		}
	}

	// Current parallel-connector count for a host (0 if not bridged).
	public int getBridgeConnectionCount(String hostname) {
		synchronized (bridgeGroups) {
			List<NetworkConnector> group = bridgeGroups.get(hostname);
			return group == null ? 0 : group.size();
		}
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