package io.cresco.agent.data;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.communication.MsgQoS;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.FileObject;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class DataPlaneServiceImpl implements DataPlaneService {
	private PluginBuilder plugin;
	private CLogger logger;
	private ControllerEngine controllerEngine;

	private CEPEngine cepEngine;

    private ActiveMQSession activeMQSession;

    private Destination agentTopic;
    private Destination regionTopic;
    private Destination globalTopic;
    private String agentTopicName = "agent.event";
    private String regionTopicName = "region.event";
    private String globalTopicName = "global.event";

    private MessageProducer agentProducer;
    private MessageProducer regionProducer;
    private MessageProducer globalProducer;

    private Map<String, MessageConsumer> messageConsumerMap;
    private Map<String,DataPlanePersistantInstance> messageConfigMap;
    private final AtomicBoolean lockMessage = new AtomicBoolean();

    // Dataplane sharding: split a topic type into N shard-topics (global.event.0..N-1) so ActiveMQ
    // per-destination demand-forwarding can spread cross-node traffic across parallel bridge
    // connectors. N=1 (default) => original single-topic behavior, zero change. Producers/destinations
    // for shard topics are cached by physical topic name.
    private int dataPlaneShards = 1;
    // Per-shard DEDICATED broker connection/session (parallel agent->region sockets). Each shard's
    // producers/consumers ride shardSessions[shard] so shard traffic spreads across sockets/cores
    // instead of funneling through the single pooled session. Enabled with dataplane_parallel_connections.
    private boolean parallelConnections = false;
    private final Map<Integer, ActiveMQSession> shardSessions = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Destination> shardDestMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, MessageProducer> shardProducerMap = Collections.synchronizedMap(new HashMap<>());

    private String URI;

    // Tenant namespacing for the dataplane (the second controlled channel besides MsgEvent). When on, every
    // dataplane topic is qualified T.<tenant>.<topic> so a tenant's agent/region/global streams are isolated
    // per tenant at every broker; same-tenant cross-region flow still works via demand-forwarding, while a
    // cross-tenant subscribe is denied by the broker ACL. Default off -> raw topic names, unchanged.
    private boolean tenantNamespacing = false;
    private String localTenant = "default";

    private Path journalPath;

    private Type typeOfListFileObject;
    private Gson gson;

	public DataPlaneServiceImpl(ControllerEngine controllerEngine, String URI)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DataPlaneServiceImpl.class.getName(),CLogger.Level.Info);

        this.cepEngine = new CEPEngine(plugin);


		messageConsumerMap = Collections.synchronizedMap(new HashMap<>());
        messageConfigMap = Collections.synchronizedMap(new HashMap<>());

		this.URI = URI;
        typeOfListFileObject = new TypeToken<List<FileObject>>() { }.getType();

        gson = new Gson();

        // Dataplane shard count (default 1 = unsharded, original behavior). Must match across all
        // agents in the fabric so publisher/subscriber derive the same shard topic from a shared key.
        tenantNamespacing = plugin.getConfig().getBooleanParam("tenant_namespacing", false);
        localTenant = plugin.getConfig().getStringParam("tenant_id", "default");

        dataPlaneShards = Math.max(1, plugin.getConfig().getIntegerParam("dataplane_shards", 1));
        // Give each shard its own dedicated broker connection (parallel sockets) rather than
        // multiplexing all shards over the single pooled session. Default on when sharding is enabled.
        parallelConnections = plugin.getConfig().getBooleanParam("dataplane_parallel_connections", dataPlaneShards > 1);
        if (dataPlaneShards > 1) {
            logger.info("Dataplane sharding enabled: " + dataPlaneShards + " shards per topic type, "
                    + "parallel_connections=" + parallelConnections);
        }

        agentTopic = getDestination(TopicType.AGENT);
        regionTopic = getDestination(TopicType.REGION);
        globalTopic = getDestination(TopicType.GLOBAL);


        String inputStreamName = "input1";
        String outputStreamName = "output1";

        String inputRecordSchemaString = "{\"type\":\"record\",\"name\":\"Ticker\",\"fields\":[{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"urn\",\"type\":\"string\"},{\"name\":\"metric\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}]}";

        String outputStreamAttributesString = "source string, avgValue double";

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from " + inputStreamName + "#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into " + outputStreamName + "; ";


        try {
            String journalDirPath = null;

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {
                Path path = Paths.get(cresco_data_location, "dp-journal");
                journalDirPath = plugin.getConfig().getStringParam("dp-journal_dir", path.toAbsolutePath().normalize().toString());
            } else {
                journalDirPath = plugin.getConfig().getStringParam("dp-journal_dir", FileSystems.getDefault().getPath("cresco-data/dp-journal").toAbsolutePath().toString());
            }

            journalPath = Paths.get(journalDirPath);
            //remove old files if they exist from the journal
            if(journalPath.toFile().exists()) {

                try (Stream<Path> journalWalk = Files.walk(journalPath)) {
                            journalWalk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);

                } catch (IOException e) {
                    logger.error("DataPlaneServiceImpl constructor journal cleanup failed", e);
                }

            }
            Files.createDirectories(journalPath);
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl constructor journal init failed", ex);
        }

    }

    public void shutdown() {
	    try {
	        if(cepEngine != null) cepEngine.shutdown();

            List<String> listeners = null;
            synchronized (lockMessage) {
                listeners = new ArrayList<>(messageConsumerMap.keySet());
            }
            for (String listener : listeners) {
                logger.info("Removing listener: " + listener);
                removeMessageListener(listener);
            }


        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.shutdown error", ex);
        }
    }

    public boolean isFaultURIActive() {
        return controllerEngine.getActiveClient().isFaultURIActive();
    }

    private Destination getDestination(TopicType topicType) {
        Destination destination = null;
	    try {

	        ActiveMQSession activeMQSession = getSession();
	        String topicName = getTopicName(topicType);

	        if((activeMQSession != null) && (topicName != null)) {
	            destination = activeMQSession.createTopic(topicName);
            }


        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.getDestination error", ex);
        }
	    return destination;
    }

    /**
     * Liveness of the DATAPLANE's own broker connection. Since control-plane traffic moved to its
     * own sockets, nothing else exercises this connection, so it can wedge silently - and
     * isFaultURIActive() now reports the CONTROL connection, not this one.
     * @return false only when the dataplane connection exists and is unusable.
     */
    public boolean isDataPlaneConnectionHealthy() {
        try {
            ActiveMQSession s = activeMQSession;
            if (s == null) return true;          // never used yet - nothing to call broken
            if (s.isClosed()) return false;
            org.apache.activemq.ActiveMQConnection c =
                    (org.apache.activemq.ActiveMQConnection) s.getConnection();
            return c != null && c.isStarted() && !c.isClosed() && !c.isTransportFailed();
        } catch (Exception ex) {
            return false;
        }
    }

    private ActiveMQSession getSession() {
	    try {

	        // BOUNDED. This used to park forever, so any caller (e.g. a stunnel DST session
	        // attaching its listener) hung indefinitely with no error and leaked its thread.
	        int waited = 0;
	        int maxWait = plugin.getConfig().getIntegerParam("dataplane_session_wait_sec", 30);
	        while (!controllerEngine.getActiveClient().isFaultURIActive()) {
                if (waited++ >= maxWait) {
                    logger.error("getSession: messaging plane not active after " + maxWait
                            + "s - returning null instead of blocking the caller forever");
                    return null;
                }
                Thread.sleep(1000);
            }

	        // A failover connection whose transport has failed still reads as open/started, but
	        // every synchronous call on it (createConsumer/createProducer) blocks. Drop it so the
	        // pooled connection is rebuilt - the dataplane no longer shares the control plane's
	        // socket, so nothing else would notice it had gone bad.
	        if (activeMQSession != null) {
	            try {
	                org.apache.activemq.ActiveMQConnection c =
	                        (org.apache.activemq.ActiveMQConnection) activeMQSession.getConnection();
	                if (c != null && (c.isTransportFailed() || c.isClosed())) {
	                    logger.warn("getSession: dataplane connection transport failed/closed - dropping for rebuild");
	                    controllerEngine.getActiveClient().handleConnectionFailure(URI);
	                    activeMQSession = null;
	                }
	            } catch (Exception ignore) { }
	        }

	        if(activeMQSession == null) {
                activeMQSession = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
	        }

                
            if(activeMQSession.isClosed()) {
                activeMQSession = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
                logger.error("getsession: activeMQSession.isClosed()");
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.getSession error", ex);
        }

	    return activeMQSession;
    }
    private MessageConsumer getConsumer(Destination destination) {
	    return getConsumer(destination,null);
    }
    private MessageConsumer getConsumer(Destination destination, String selectorString) {
	    MessageConsumer messageConsumer = null;
	    try {

            ActiveMQSession activeMQSession = getSession();

            if((activeMQSession != null) && (destination != null)) {

                if(selectorString == null) {
                    messageConsumer = activeMQSession.createConsumer(destination);
                } else {
                    messageConsumer = activeMQSession.createConsumer(destination, selectorString);
                }
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.getConsumer error", ex);
        }
	    return  messageConsumer;
    }

    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString) {
        return addMessageListener(topicType, messageListener, selectorString, true);
    }

    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString, boolean persistant) {
        return addMessageListener(topicType, messageListener, selectorString, persistant, null);
    }

	public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString, Boolean persistant, String listenerId) {
	    try {

            MessageConsumer consumer = null;

            switch (topicType) {
                case AGENT:
                    if(selectorString == null) {
                        consumer = getConsumer(agentTopic);
                    } else {
                        consumer = getConsumer(agentTopic, selectorString);
                    }
                    break;
                case REGION:
                    if(selectorString == null) {
                        consumer = getConsumer(regionTopic);
                    } else {
                        consumer = getConsumer(regionTopic, selectorString);
                    }
                    break;
                case GLOBAL:
                    if(selectorString == null) {
                        consumer = getConsumer(globalTopic);
                    } else {
                        consumer = getConsumer(globalTopic, selectorString);
                    }
                    break;
            }
            if(consumer != null) {
                consumer.setMessageListener(messageListener);

                if(listenerId == null) {
                    listenerId = UUID.randomUUID().toString();
                }
                synchronized (lockMessage) {
                    messageConsumerMap.put(listenerId, consumer);
                    //keep persistent in memory
                    if(persistant) {
                        messageConfigMap.put(listenerId, new DataPlanePersistantInstance(topicType, messageListener, selectorString, listenerId));
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.addMessageListener error", ex);
        }
        return listenerId;
    }

    // --- Measurement: record producer send-latency + tx bytes onto the uplink (the local broker
    //     connection = the parent link for an agent). Send dwell time rises under broker flow-control
    //     pressure -> a direct saturation signal the AutoTuner consumes. ---
    private void recordUplinkSend(long startNanos, Message message) {
        try {
            io.cresco.agent.controller.netmetrics.LinkMetricsRegistry reg = controllerEngine.getLinkMetricsRegistry();
            if (reg == null) return;
            io.cresco.agent.controller.netmetrics.LinkMetrics lm =
                    reg.forPath(io.cresco.agent.controller.netmetrics.LinkMetricsRegistry.parentLinkKey(controllerEngine));
            lm.recordSendLatency((System.nanoTime() - startNanos) / 1_000_000.0);
            // tx bytes: a JMS int property is readable even on a just-sent (write-mode) message, unlike
            // getBodyLength(); the wsapi/stunnel producers stamp "dp_bytes" with the payload length.
            try {
                if (message.propertyExists("dp_bytes")) lm.addTxBytes(message.getIntProperty("dp_bytes"));
            } catch (Exception ignore) { }
        } catch (Exception ignore) { }
    }

    // --- Dataplane sharding ---

    @Override
    public int getDataPlaneShardCount() { return dataPlaneShards; }

    // Physical topic name for (type, shard): base name when unsharded, base.<shard> when sharded.
    private String shardedTopicName(TopicType topicType, int shard) {
        String base = getTopicName(topicType);
        if (dataPlaneShards <= 1) return base;
        return base + "." + Math.floorMod(shard, dataPlaneShards);
    }

    // A shard's session: its own dedicated broker connection when parallelConnections is on, else the
    // shared pooled session. This is the agent->region parallelism: shard i rides its own socket/core.
    private ActiveMQSession getShardSession(int shard) {
        if (!parallelConnections) return getSession();
        Integer key = Math.floorMod(shard, dataPlaneShards);
        ActiveMQSession s = shardSessions.get(key);
        try {
            if (s == null || s.isClosed()) {
                synchronized (shardSessions) {
                    s = shardSessions.get(key);
                    if (s == null || s.isClosed()) {
                        // a closed session's dedicated connection may still be open — close it or
                        // every shard rebuild leaks a socket
                        if (s != null) {
                            try {
                                org.apache.activemq.ActiveMQConnection oldConn =
                                        (org.apache.activemq.ActiveMQConnection) s.getConnection();
                                if (oldConn != null && !oldConn.isClosed()) oldConn.close();
                            } catch (Exception ignore) { }
                        }
                        while (!controllerEngine.getActiveClient().isFaultURIActive()) { Thread.sleep(1000); }
                        s = controllerEngine.getActiveClient().createDedicatedSession(URI, false, Session.AUTO_ACKNOWLEDGE);
                        if (s != null) {
                            shardSessions.put(key, s);
                            logger.info("Opened dedicated dataplane connection for shard " + key);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("getShardSession(" + shard + ") error", ex);
        }
        return s;
    }

    private Destination getShardDestination(String topicName, ActiveMQSession session) {
        return shardDestMap.computeIfAbsent(topicName, tn -> {
            try {
                return (session != null) ? session.createTopic(tn) : null;
            } catch (Exception ex) {
                logger.error("getShardDestination(" + tn + ") error", ex);
                return null;
            }
        });
    }

    private MessageProducer getShardProducer(String topicName, int shard) {
        MessageProducer p = shardProducerMap.get(topicName);
        if (p != null) return p;
        try {
            ActiveMQSession s = getShardSession(shard);
            Destination d = getShardDestination(topicName, s);
            if (s != null && d != null) {
                synchronized (shardProducerMap) {
                    p = shardProducerMap.get(topicName);
                    if (p == null) {
                        p = s.createProducer(d);
                        p.setTimeToLive(300000L);
                        p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        shardProducerMap.put(topicName, p);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("getShardProducer(" + topicName + ") error", ex);
        }
        return p;
    }

    @Override
    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString, int shard) {
        if (dataPlaneShards <= 1) {
            return addMessageListener(topicType, messageListener, selectorString);
        }
        String listenerId = null;
        try {
            String topicName = shardedTopicName(topicType, shard);
            ActiveMQSession s = getShardSession(shard);
            Destination dest = getShardDestination(topicName, s);
            MessageConsumer consumer = (s != null && dest != null)
                    ? ((selectorString == null) ? s.createConsumer(dest) : s.createConsumer(dest, selectorString))
                    : null;
            if (consumer != null) {
                consumer.setMessageListener(messageListener);
                listenerId = UUID.randomUUID().toString();
                synchronized (lockMessage) {
                    messageConsumerMap.put(listenerId, consumer);
                }
            }
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.addMessageListener(shard) error", ex);
        }
        return listenerId;
    }

    @Override
    public boolean sendMessage(TopicType topicType, Message message, int deliveryMode, int priority, int timeToLive, int shard) {
        if (dataPlaneShards <= 1) {
            return sendMessage(topicType, message, deliveryMode, priority, timeToLive);
        }
        try {
            while(!controllerEngine.cstate.isActive()) {
                Thread.sleep(1000);
            }
            String topicName = shardedTopicName(topicType, shard);
            MessageProducer producer = getShardProducer(topicName, shard);
            if (producer != null) {
                long t0 = System.nanoTime();
                producer.send(message, deliveryMode, priority, timeToLive);
                recordUplinkSend(t0, message);
                return true;
            }
            return false;
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.sendMessage(shard) error", ex);
            return false;
        }
    }

    public void updateConnections(String URI)  {

        //set new URI
        logger.error("Restoring DataPlane");
        this.URI = URI;

        //reset activeMQSession
        try {
            if(activeMQSession != null){
                if(!activeMQSession.isClosed()) {
                    activeMQSession.close();
                }
                activeMQSession = null;
            }

            if(agentProducer != null){
                agentProducer.close();
                agentProducer = null;
            }

            if(regionProducer != null){
                regionProducer.close();
                regionProducer = null;
            }

            if(globalProducer != null){
                globalProducer.close();
                globalProducer = null;

            }

            // drop sharded producer/destination caches and close the dedicated shard connections
            synchronized (shardProducerMap) {
                for (MessageProducer p : shardProducerMap.values()) {
                    try { if (p != null) p.close(); } catch (Exception ignore) {}
                }
                shardProducerMap.clear();
            }
            shardDestMap.clear();
            synchronized (shardSessions) {
                for (ActiveMQSession ss : shardSessions.values()) {
                    try {
                        if (ss != null) {
                            jakarta.jms.Connection c = ss.getConnection();
                            ss.close();
                            if (c != null) c.close();
                        }
                    } catch (Exception ignore) {}
                }
                shardSessions.clear();
            }

        } catch (Exception ex) {
            logger.error("updateConnections(): reset activeMQSession ");
            logger.error(ex.getMessage());
            logger.error("DataPlaneServiceImpl.updateConnections reset activeMQSession error", ex);
        }

        //clean existing listeners
        synchronized (lockMessage) {
            //get all keys
            List<String> listenerIds = new ArrayList<>(messageConsumerMap.keySet());
            for(String listenerId : listenerIds) {
                try {
                    logger.error("Removing old listenerId: " + listenerId);
                    //messageConsumerMap.get(listenerId).setMessageListener(null);
                    //messageConsumerMap.get(listenerId).close();
                    messageConsumerMap.remove(listenerId);

                } catch (Exception ex) {
                    logger.error("resetMessageListener() : remove existing messageConsumerMap");
                    logger.error(ex.getMessage());
                    logger.error("DataPlaneServiceImpl.updateConnections remove messageConsumerMap error", ex);
                }
            }
        }
        //add consumers back
        Map<String, DataPlanePersistantInstance> saveMessageConfigMap = null;
        synchronized (lockMessage) {
            saveMessageConfigMap = new HashMap<>(messageConfigMap);
            messageConfigMap.clear();
        }

        for (DataPlanePersistantInstance dataPlanePersistantInstance : saveMessageConfigMap.values()) {
            logger.info("Restoring listenerId: " + dataPlanePersistantInstance.getListenerId());
            addMessageListener(dataPlanePersistantInstance.getTopicType(),dataPlanePersistantInstance.getMessageListener(),dataPlanePersistantInstance.getSelectorString(),true,dataPlanePersistantInstance.getListenerId());
        }

    }

    public void removeMessageListener(String listenerId) {
	    try {
	        synchronized (lockMessage) {
                MessageConsumer consumer = messageConsumerMap.get(listenerId);
                if (consumer != null) {
                    try {
                        logger.trace("removeMessageListener: closing listener : " + listenerId);
                        logger.trace("removeMessageListener: message selector : " + consumer.getMessageSelector());

                        consumer.close();

                        messageConsumerMap.remove(listenerId);
                    } catch (JMSException e) {
                        logger.error("Failed to close message listener [{}]", listenerId);
                    }
                } else {
                    logger.error("removeMessageListener close called on unknown listener_id: " + listenerId);
                }
            }
        } catch (Exception e) {
	        logger.error("removeMessageListener('{}'): {}", listenerId, e.getMessage());
            logger.error("DataPlaneServiceImpl.removeMessageListener error", e);
        }
    }


    public boolean sendMessage(TopicType topicType, Message message) {
        return sendMessage(MsgEvent.Type.INFO, topicType, message, DeliveryMode.NON_PERSISTENT, 0, 0);
    }

    @Override
    public boolean sendMessage(MsgEvent.Type msgEventType, TopicType topicType, Message message) {
        //priority and delivery model will be adjusted in downstream function baed on MsgEvent type
        return sendMessage(msgEventType, topicType, message, DeliveryMode.NON_PERSISTENT, 0, 0);
    }

    public boolean sendMessage(TopicType topicType, Message message, int deliveryMode, int priority, int timeToLive) {
        return sendMessage(MsgEvent.Type.INFO, topicType, message, deliveryMode, priority, timeToLive);
    }
    private boolean sendMessage(MsgEvent.Type msgEventType, TopicType topicType, Message message, int deliveryMode, int priority, int timeToLive) {
        try {

            /*
			Default (JMSPriority == 4)
			High (JMSPriority > 4 && <= 9)
			Low (JMSPriority > 0 && < 4)
             */
            //JMSPriority > 4 is reserved for CONFIG, WATCHDOG, and EXEC MsgEvents
            //Reduce requested priority to 4 (default) if needed

            if(priority > 4) {
                priority = 4;
            }

            /*
			CONFIG,
        	DISCOVER,
        	ERROR,
        	EXEC,
        	GC,
        	INFO,
        	KPI,
        	LOG,
        	WATCHDOG;
			 */

			/*
			Default (JMSPriority == 4)
			High (JMSPriority > 4 && <= 9)
			Low (JMSPriority > 0 && < 4)
			 */

            // QoS: the dataplane rides the same priority hierarchy as the MsgEvent fabric
            // (LIVENESS > CONTROL > TELEMETRY > BULK). Control-plane messages are forced to their
            // tier (high priority + persistent); the bulk data stream keeps the caller-supplied
            // low priority + non-persistent so it can never starve control. The broker topic policy
            // (prioritizedMessages + producer-flow-control OFF) makes this effective without blocking.
            MsgQoS.Tier tier = MsgQoS.classify(msgEventType);
            if (tier.isControlPlane()) {
                priority = tier.priority;
                deliveryMode = tier.deliveryMode;
            }

            while(!controllerEngine.cstate.isActive()) {
                Thread.sleep(1000);
                logger.debug("!controllerEngine.cstate.isActive() SLEEPING 1s");
            }

            switch (topicType) {
                case AGENT:
                    if(agentProducer == null) {
                        agentProducer = getMessageProducer(topicType);
                        if(agentProducer == null) {
                            return false;
                        }
                    }
                    //if has header, send blob
                    Object inputObject = message.getObjectProperty("blob_data_stream");
                    if(inputObject != null) {
                        InputStream inputStream = (InputStream) inputObject;
                        ActiveMQSession activeMQSession = getSession();
                        if(activeMQSession != null) {
                            BlobMessage blobMessage = activeMQSession.createBlobMessage(inputStream);
                            agentProducer.send(blobMessage, deliveryMode, priority, timeToLive);
                        }
                        if (inputStream != null) {
                            inputStream.close();
                        }
                    } else {
                        if(agentProducer != null) {
                            agentProducer.send(message, deliveryMode, priority, timeToLive);
                        }
                    }
                    break;
                case REGION:
                    if(regionProducer == null) {
                        regionProducer = getMessageProducer(topicType);
                    }
                    if(regionProducer != null) {
                        regionProducer.send(message, deliveryMode, priority, timeToLive);
                    }
                    break;
                case GLOBAL:
                    if(globalProducer == null) {
                        globalProducer = getMessageProducer(topicType);
                    }
                    if(globalProducer != null) {
                        long gt0 = System.nanoTime();
                        globalProducer.send(message, deliveryMode, priority, timeToLive);
                        recordUplinkSend(gt0, message);
                    }
                    break;
            }

            return true;
        } catch (JMSException jmse) {
            logger.error("DataPlaneServiceImpl.sendMessage JMSException", jmse);
            return false;
        }
        catch (Exception ex) {
            logger.error(ex.getMessage());
            logger.error("DataPlaneServiceImpl.sendMessage error", ex);
            return false;
        }

    }

    private String getTopicName(TopicType topicType) {

        String topicNameString = null;
        switch (topicType) {
            case AGENT:
                topicNameString = agentTopicName;
                break;
            case REGION:
                topicNameString = regionTopicName;
                break;
            case GLOBAL:
                topicNameString = globalTopicName;
                break;
        }
        if (tenantNamespacing && topicNameString != null) {
            topicNameString = io.cresco.library.security.TenantNamespace.qualify(localTenant, topicNameString);
        }
        return topicNameString;
    }

    private MessageProducer getMessageProducer(TopicType topicType) {

        MessageProducer messageProducer = null;

        ActiveMQSession activeMQSession = getSession();

        //logger.info("Disabling AsyncBatch");
        //activeMQSession.setAsyncDispatch(false);
        //activeMQSession.setSessionAsyncDispatch(false);

        if(activeMQSession != null) {

            try {
                switch (topicType) {
                    case AGENT:
                        messageProducer = activeMQSession.createProducer(agentTopic);
                        break;
                    case REGION:
                        messageProducer = activeMQSession.createProducer(regionTopic);
                        break;
                    case GLOBAL:
                        messageProducer = activeMQSession.createProducer(globalTopic);
                        break;
                }

                if(messageProducer != null) {
                    messageProducer.setTimeToLive(300000L);
                    messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                }

            } catch (Exception ex) {
                logger.error("DataPlaneServiceImpl.getMessageProducer error", ex);
            }
        } else {
            logger.error("getMessageProducer: activeMQSession != null");
        }

        return messageProducer;
    }

    public BytesMessage createBytesMessage() {
        BytesMessage bytesMessage = null;
	    try {
	        ActiveMQSession activeMQSession = getSession();
	        if(activeMQSession != null) {
                bytesMessage = activeMQSession.createBytesMessage();
            }

        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createBytesMessage error", ex);
        }
        return bytesMessage;
	}

    public MapMessage createMapMessage() {
        MapMessage mapMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                mapMessage = activeMQSession.createMapMessage();
            }

        } catch (Exception ex){
            logger.error("DataPlaneServiceImpl.createMapMessage error", ex);
        }
	    return mapMessage;
    }

    public Message createMessage() {
	    Message message = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                message = activeMQSession.createMessage();
            }

        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createMessage error", ex);
        }
        return message;
    }

    public Message createMessage(InputStream inputStream) {
        BlobMessage message = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                message = activeMQSession.createBlobMessage(inputStream);
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.createMessage(InputStream) error", ex);
        }
        return message;
    }

    public InputStream getInputMessageStream(Message message) {
        InputStream inputStream = null;
        try {
            BlobMessage blobMessage = (BlobMessage) message;
            inputStream = blobMessage.getInputStream();

        } catch (Exception ex) {
                logger.error("DataPlaneServiceImpl.getInputMessageStream error", ex);
            }
        return inputStream;
    }

    public ObjectMessage createObjectMessage() {
	    ObjectMessage objectMessage = null;

	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                objectMessage = activeMQSession.createObjectMessage();
            }
        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createObjectMessage error", ex);
        }
	    return objectMessage;
    }

    //blobs are not part of JMX, they are part of ActiveMQ, which is not in the core lib
    //for now we must use i/o streams

    public BlobMessage createBlobMessage(URL url) {
	    BlobMessage blobMessage = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(url);
            }
        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createBlobMessage(URL) error", ex);
        }
	    return blobMessage;
    }

    public BlobMessage createBlobMessage(File file) {
        BlobMessage blobMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(file);
            }
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.createBlobMessage(File) error", ex);
        }
        return blobMessage;
    }

    public BlobMessage createBlobMessage(InputStream inputStream) {
        BlobMessage blobMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(inputStream);
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.createBlobMessage(InputStream) error", ex);
        }
        return blobMessage;

    }


    public StreamMessage createStreamMessage() {
	    StreamMessage streamMessage = null;
	    try{
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                streamMessage = activeMQSession.createStreamMessage();
            }

        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createStreamMessage error", ex);
        }
        return streamMessage;
    }

    public TextMessage createTextMessage() {
	    TextMessage textMessage = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                textMessage = activeMQSession.createTextMessage();
            }

        } catch (Exception ex) {
	        logger.error("DataPlaneServiceImpl.createTextMessage error", ex);
        }
        return textMessage;
    }



    // CEP runs in-process in the controller (Siddhi embedded via the library). The Siddhi extraction
    // to a standalone cep plugin was reverted: the plugin builds but Felix SCR would not activate its
    // component with Siddhi's full runtime tail embedded. Keeping the proven in-process engine.
    public String createCEP(String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {
        String cepId = UUID.randomUUID().toString();
        if(cepEngine.createCEP(cepId,inputStreamName,inputStreamDefinition,outputStreamName,outputStreamDefinition,queryString)) {
            return cepId;
        } else {
            return null;
        }
    }

    public void inputCEP(String streamName, String jsonPayload) {

	    try {
            TextMessage tickle = createTextMessage();
            tickle.setText(jsonPayload);
            tickle.setStringProperty("stream_name", streamName);

            // Feed on the GLOBAL sharded dataplane (same topic/shard CEPInstance now listens on).
            sendMessage(TopicType.GLOBAL, tickle, jakarta.jms.DeliveryMode.NON_PERSISTENT, 0, 0, shardFor(streamName));

        } catch (Exception ex) {
	        logger.error("inputCEP Error: " + ex.getMessage());
        }

    }

    public boolean removeCEP(String cepId) {
        return cepEngine.removeCEP(cepId);
    }

    /** Active CEP query count — for the controller's central metrics + cep HealthCheck. */
    public int getActiveCEPCount() {
        return (cepEngine != null) ? cepEngine.getActiveCount() : 0;
    }

    /** Whether the embedded CEP (Siddhi) engine is initialized and ready. */
    public boolean isCEPReady() {
        return (cepEngine != null) && cepEngine.isReady();
    }

    public Path getJournalPath() {
	    return journalPath;
	}

    public List<FileObject> createFileObjects(List<String> fileList) {
        List<FileObject> fileObjects = null;

        try {

            fileObjects = new ArrayList<>();


            for(String filePath : fileList) {
                fileObjects.add(createFileObject(filePath));
            }


        }catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.createFileObjects error", ex);
        }

        return fileObjects;
    }

    public FileObject createFileObject(String fileName) {
        FileObject fileObject = null;
        try {

            File inFile = new File(fileName);
            if(inFile.exists()) {
                String dataName = UUID.randomUUID().toString();
                String fileMD5Hash = getMD5(fileName);

                Map<String, String> dataMap = splitFile(dataName, fileName);
                fileObject = new FileObject(inFile.getName(),fileMD5Hash,dataMap,dataName);
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.createFileObject error", ex);
        }
        return fileObject;
    }

    public Map<String,String> splitFile(String dataName, String fileName)  {

        Map<String,String> filePartNames = null;
        try {

            File f = new File(fileName);

            FileInputStream fis = new FileInputStream(f);

            filePartNames = streamToSplitFile(dataName, fis);

            if (fis != null) {
                fis.close();
            }
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.splitFile error", ex);
        }

        return filePartNames;
    }

    public Map<String,String> streamToSplitFile(String dataName, InputStream is)  {

        Map<String,String> filePartNames = null;
        try {


            filePartNames = new HashMap<>();

            int partCounter = 0;//I like to name parts from 001, 002, 003, ...
            //you can change it to 0 if you want 000, 001, ...

            int sizeOfFiles = 1024 * 1024 * 5;// 50MB
            byte[] buffer = new byte[sizeOfFiles];


            //String fileName = UUID.randomUUID().toString();

            //try-with-resources to ensure closing stream
            try (BufferedInputStream bis = new BufferedInputStream(is)) {

                int bytesAmount = 0;
                while ((bytesAmount = bis.read(buffer)) > 0) {
                    //write each chunk of data into separate file with different number in name
                    //String filePartName = String.format("%s.%03d", fileName, partCounter++);

                    String filePartName = dataName + "." + partCounter;
                    //MessageDigest m= MessageDigest.getInstance("MD5");
                    //m.update(buffer);
                    //String md5Hash = new BigInteger(1,m.digest()).toString(16);

                    partCounter++;

                    Path filePath = Paths.get(journalPath.toAbsolutePath() + System.getProperty("file.separator") + dataName);
                    Files.createDirectories(filePath);

                    File newFile = new File(filePath.toAbsolutePath().toString(), filePartName);
                    try (FileOutputStream out = new FileOutputStream(newFile)) {
                        out.write(buffer, 0, bytesAmount);
                    }

                    String md5Hash = getMD5(newFile.getAbsolutePath());
                    filePartNames.put(filePartName, md5Hash);
                }
            }
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.streamToSplitFile error", ex);
        }
        return filePartNames;
    }

    public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
	    Path returnFilePath = null;

	    try {

            MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC,remoteRegion,remoteAgent);
            me.setParam("action","getfileinfo");
            me.setParam("filepath",remoteFilePath);

            MsgEvent re  = plugin.sendRPC(me);

            if(re == null) {
                logger.error("downloadRemoteFile re message == null");
            } else {

                if(re.paramsContains("md5") && re.paramsContains("size")) {

                    String rmd5 = re.getParam("md5");
                    long fileSize = Long.parseLong(re.getParam("size"));

                    int sizeOfFilePart = 1024 * 1024 * 5;// 5MB

                    if(fileSize <= sizeOfFilePart) {
                        //send request for file directly

                        Path filePath = Paths.get(localFilePath);

                        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile())) {

                            MsgEvent dme = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, remoteRegion, remoteAgent);
                            dme.setParam("action", "getfiledata");
                            dme.setParam("filepath", remoteFilePath);
                            dme.setParam("skiplength", "0");
                            dme.setParam("partsize", String.valueOf(fileSize));

                            MsgEvent rdme = plugin.sendRPC(dme);
                            fileOutputStream.write(rdme.getDataParam("payload"));
                        }

                    } else { //we need to break up the file

                        long fileDataRemaining = fileSize;
                        long skipLength = 0;

                        Path filePath = Paths.get(localFilePath);

                        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile())) {

                            while (fileDataRemaining != 0) {
                                //loop through writing files

                                if (sizeOfFilePart >= fileDataRemaining) {
                                    sizeOfFilePart = (int) fileDataRemaining;
                                }

                                //System.out.println("Size of Data Part " + sizeOfFilePart + " Data Remaining = " + fileDataRemaining + " skipLength " + skipLength);
                                MsgEvent dme = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, remoteRegion, remoteAgent);
                                dme.setParam("action", "getfiledata");
                                dme.setParam("filepath", remoteFilePath);
                                dme.setParam("skiplength", String.valueOf(skipLength));
                                dme.setParam("partsize", String.valueOf(sizeOfFilePart));

                                MsgEvent rdme = plugin.sendRPC(dme);
                                fileOutputStream.write(rdme.getDataParam("payload"));

                                fileDataRemaining = fileDataRemaining - sizeOfFilePart;
                                skipLength += sizeOfFilePart;

                            }
                        }
                    }

                    //check if file is correct
                    String lmd5 = plugin.getMD5(localFilePath);
                    if(lmd5.equals(rmd5)) {
                        returnFilePath = Paths.get(localFilePath);
                    }

                }

            }

        } catch (Exception ex) {
	        logger.error("downloadRemoteFile() error", ex);
        }
	    return returnFilePath;
    }


    public String getMD5(String filePath) {
        String hashString = null;
        try {
            //Get file input stream for reading the file content
            try (FileInputStream fis = new FileInputStream(filePath)) {

                MessageDigest digest = MessageDigest.getInstance("MD5");

                //Create byte array to read data in chunks
                byte[] byteArray = new byte[1024];
                int bytesCount = 0;

                //Read file data and update in message digest
                while ((bytesCount = fis.read(byteArray)) != -1) {
                    digest.update(byteArray, 0, bytesCount);
                }

                //close the stream; We don't need it now.
                fis.close();

                //Get the hash's bytes
                byte[] bytes = digest.digest();

                //This bytes[] has bytes in decimal format;
                //Convert it to hexadecimal format
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < bytes.length; i++) {
                    sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
                }

                hashString = sb.toString();
            }

        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.getMD5 error", ex);
        }
        //return complete hash
        return hashString;
    }

    public void mergeFiles(List<File> files, File into, boolean deleteParts) {

        try {

            try (FileOutputStream fos = new FileOutputStream(into);
                 BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
                for (File f : files) {
                    Files.copy(f.toPath(), mergingStream);
                    if (deleteParts) {
                        f.delete();
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("DataPlaneServiceImpl.mergeFiles error", ex);
        }
    }

    public List<FileObject> getFileObjectsFromString(String fileObjectsString){

        return gson.fromJson(fileObjectsString,typeOfListFileObject);
    }

    public String generateFileObjectsString(List<FileObject> fileObjects){
	    return gson.toJson(fileObjects);
    }

}



