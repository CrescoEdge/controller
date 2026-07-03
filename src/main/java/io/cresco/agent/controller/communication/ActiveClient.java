package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import jakarta.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap; // Import ConcurrentHashMap

public class ActiveClient {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    // Use ConcurrentHashMap for better thread safety with less contention than synchronizedMap
    private Map<String, ActiveMQSslConnectionFactory> connectionFactoryMap;
    private Map<String, ActiveMQConnection> connectionMap;

    private AgentConsumer agentConsumer;
    private AgentProducer agentProducer;

    private String faultTriggerURI;
    private final int MAX_CONNECTION_START_ATTEMPTS = 5;
    private final long CONNECTION_START_RETRY_DELAY_MS = 500;


    public ActiveClient(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ActiveClient.class.getName(), CLogger.Level.Info);
        //agentProducer is set via initActiveAgentProducer now
        connectionFactoryMap = new ConcurrentHashMap<>();
        connectionMap = new ConcurrentHashMap<>();
    }


    /**
     * Drop cached connection factories and connections so subsequent connections are rebuilt with the
     * CURRENT key/trust managers. Needed after a regional-CA enrollment swaps this node's identity and
     * adds the region CA to the truststore: the factories snapshot their SSL material at creation, so
     * without this the client keeps presenting/validating with the pre-enrollment certificates.
     */
    public void refreshTrust() {
        try {
            // Only NETWORK (non-vm) connections carry TLS material that changes at enrollment. The local
            // vm://localhost connection is the agent's own in-JVM broker consumer/producer (no TLS) and
            // MUST NOT be torn down — closing it drops the RPC reply consumer and the node goes deaf.
            int refreshed = 0;
            for (String uri : new java.util.ArrayList<>(connectionMap.keySet())) {
                if (uri != null && !uri.startsWith("vm://")) {
                    ActiveMQConnection c = connectionMap.remove(uri);
                    if (c != null) { try { c.close(); } catch (Exception ignore) {} }
                    refreshed++;
                }
            }
            for (String uri : new java.util.ArrayList<>(connectionFactoryMap.keySet())) {
                if (uri != null && !uri.startsWith("vm://")) {
                    connectionFactoryMap.remove(uri);
                }
            }
            logger.info("ActiveClient trust refreshed — " + refreshed + " network connection(s) rebuilt with current certificates; vm:// preserved");
        } catch (Exception ex) {
            logger.error("refreshTrust error: " + ex.getMessage());
        }
    }

    private void setFaultTriggerURI(String faultTriggerURI) {
        this.faultTriggerURI = faultTriggerURI;
    }

    public String getFaultTriggerURI() {
        return faultTriggerURI;
    }

    public boolean isFaultURIActive() {
        boolean isActive = false;
        try {
            if (faultTriggerURI != null) {
                isActive = isConnectionActive(faultTriggerURI);
            } else {
                logger.trace("isFaultURIActive: faultTriggerURI is null, assuming inactive.");
                isActive = false; // No URI to check, assume inactive or handle as per application logic
            }
        } catch (Exception ex) {
            logger.error("isFaultURIActive Exception: {}", ex.getMessage(), ex);
            isActive = false; // Assume inactive on error
        }
        return isActive;
    }

    public ActiveMQSession createSession(String URI, boolean transacted, int acknowledgeMode) {
        ActiveMQSession activeMQSession = null;
        logger.debug("Attempting to create session for URI [{}]", URI);
        try {
            ActiveMQConnection activeMQConnection = getConnection(URI);

            if (activeMQConnection != null && activeMQConnection.isStarted()) {
                activeMQSession = (ActiveMQSession) activeMQConnection.createSession(transacted, acknowledgeMode);
                logger.info("Successfully created session for URI [{}]", URI);
            } else {
                logger.error("Failed to create session for URI [{}]: Connection is null or not started.", URI);
                if(activeMQConnection != null) {
                    logger.error("Connection details: isStarted={}, isClosed={}, isClosing={}",
                            activeMQConnection.isStarted(), activeMQConnection.isClosed(), activeMQConnection.isClosing());
                }
            }
        } catch (JMSException jmse) {
            logger.error("JMSException creating session for URI [{}]: {}", URI, jmse.getMessage(), jmse);
            // Trigger failure handling as session creation failed, implies connection issue
            handleConnectionFailure(URI);
        } catch (Exception ex) {
            logger.error("General Exception creating session for URI [{}]: {}", URI, ex.getMessage(), ex);
            // Potentially trigger failure handling here as well if appropriate
            handleConnectionFailure(URI);
        }
        return activeMQSession;
    }

    // Create a session on a DEDICATED, non-pooled connection to URI (its own TCP socket), separate
    // from the single pooled control-plane connection. Used to give each parallel dataplane shard its
    // own connection/core instead of multiplexing everything over one socket (the agent->region
    // throughput funnel). The caller owns the lifecycle and closes it via the session's connection.
    public ActiveMQSession createDedicatedSession(String URI, boolean transacted, int acknowledgeMode) {
        try {
            ActiveMQSslConnectionFactory factory = connectionFactoryMap.computeIfAbsent(URI, key -> {
                logger.info("createDedicatedSession: initializing factory for URI [{}]", key);
                return initConnectionFactory(key);
            });
            if (factory == null) {
                logger.error("createDedicatedSession: null factory for URI [{}]", URI);
                return null;
            }
            ActiveMQConnection conn = (ActiveMQConnection) factory.createConnection();
            conn.setExceptionListener(new ConnectionExceptionListener(URI));
            conn.start();
            int attempts = 0;
            while (!conn.isStarted() && attempts < MAX_CONNECTION_START_ATTEMPTS) {
                Thread.sleep(CONNECTION_START_RETRY_DELAY_MS);
                attempts++;
            }
            if (!conn.isStarted()) {
                logger.error("createDedicatedSession: connection to [{}] failed to start", URI);
                try { conn.close(); } catch (JMSException ignore) {}
                return null;
            }
            return (ActiveMQSession) conn.createSession(transacted, acknowledgeMode);
        } catch (Exception ex) {
            logger.error("createDedicatedSession error for URI [{}]: {}", URI, ex.getMessage(), ex);
            return null;
        }
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    private ActiveMQConnection getConnection(String URI) {
        ActiveMQConnection activeMQConnection = connectionMap.get(URI);

        // Verify the existing connection if found
        if (activeMQConnection != null) {
            try {
                if (activeMQConnection.isClosed() || activeMQConnection.isClosing() || !activeMQConnection.isStarted()) {
                    logger.warn("Existing connection for URI [{}] found but is invalid (closed/closing/not started). Will attempt to remove and recreate.", URI);
                    // Clean up the invalid connection from the map
                    connectionMap.remove(URI, activeMQConnection); // Only remove if it's the same instance
                    try {
                        activeMQConnection.close(); // Attempt to close it fully
                    } catch (JMSException e) {
                        logger.warn("JMSException while closing invalid connection for URI [{}]: {}", URI, e.getMessage());
                    }
                    activeMQConnection = null; // Nullify to force recreation
                } else {
                    logger.trace("Reusing existing active connection for URI [{}]", URI);
                    return activeMQConnection; // Return valid, existing connection
                }
            } catch (Exception e) {
                logger.warn("Exception while checking existing connection for URI [{}]: {}. Will attempt to remove and recreate.", URI, e.getMessage());
                connectionMap.remove(URI, activeMQConnection);
                try { activeMQConnection.close(); } catch (JMSException closeEx) { /* ignore */ }
                activeMQConnection = null;
            }
        }

        // If no valid connection exists, create a new one
        logger.info("No valid connection found for URI [{}]. Attempting to create new connection.", URI);

        // Thread-safe creation of connection factory
        ActiveMQSslConnectionFactory activeMQSslConnectionFactory = connectionFactoryMap.computeIfAbsent(URI, key -> {
            logger.info("No existing factory for URI [{}]. Initializing new connection factory.", key);
            ActiveMQSslConnectionFactory newFactory = initConnectionFactory(key);
            if (newFactory == null) {
                logger.error("Failed to initialize new connection factory for URI [{}].", key);
            }
            return newFactory;
        });


        if (activeMQSslConnectionFactory == null) {
            logger.error("Cannot create connection for URI [{}]: Connection factory is null.", URI);
            handleConnectionFailure(URI); // Signal failure
            return null;
        }

        try {
            ActiveMQConnection newConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
            logger.info("Created new connection object for URI [{}]", URI);

            newConnection.setExceptionListener(new ConnectionExceptionListener(URI));
            logger.info("Attached ExceptionListener to new connection for URI [{}]", URI);

            newConnection.start();
            int startAttempts = 0;
            while (!newConnection.isStarted() && startAttempts < MAX_CONNECTION_START_ATTEMPTS) {
                logger.warn("Waiting for connection to URI [{}] to start (Attempt {}/{})", URI, startAttempts + 1, MAX_CONNECTION_START_ATTEMPTS);
                Thread.sleep(CONNECTION_START_RETRY_DELAY_MS);
                startAttempts++;
            }

            if (newConnection.isStarted()) {
                logger.info("Connection to URI [{}] started successfully.", URI);
                // Put in map, potentially replacing an old/invalid one if another thread also tried
                ActiveMQConnection oldConn = connectionMap.put(URI, newConnection);
                if (oldConn != null && oldConn != newConnection) { // If there was a different old connection
                    logger.warn("Replaced an existing connection in map for URI [{}] during new connection setup.", URI);
                    try { oldConn.close(); } catch (JMSException e) { /* ignore */ }
                }
                return newConnection;
            } else {
                logger.error("Connection to URI [{}] failed to start after {} attempts.", URI, startAttempts);
                try { newConnection.close(); } catch (JMSException e) { logger.warn("Error closing failed connection attempt for URI [{}]: {}", URI, e.getMessage()); }
                handleConnectionFailure(URI); // Signal failure
                return null;
            }
        } catch (JMSException jmse) {
            logger.error("JMSException during connection creation or start for URI [{}]: {}", URI, jmse.getMessage(), jmse);
            handleConnectionFailure(URI); // Signal failure
            return null;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for connection to URI [{}] to start.", URI, ie);
            handleConnectionFailure(URI); // Signal failure
            return null;
        } catch (Exception ex) {
            logger.error("General Exception in getConnection (creation phase) for URI [{}]: {}", URI, ex.getMessage(), ex);
            handleConnectionFailure(URI); // Signal failure
            return null;
        }
    }


    private ActiveMQSslConnectionFactory initConnectionFactory(String URI) {
        ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;
        try {
            logger.debug("Initializing ConnectionFactory for URI: {}", URI);
            activeMQSslConnectionFactory = new ActiveMQSslConnectionFactory(URI);
            if (URI.startsWith("vm://")) {
                activeMQSslConnectionFactory.setObjectMessageSerializationDefered(true);
            }
            activeMQSslConnectionFactory.setKeyAndTrustManagers(
                    controllerEngine.getCertificateManager().getKeyManagers(),
                    controllerEngine.getCertificateManager().getTrustManagers(),
                    new SecureRandom()
            );

            // Tenant identity on the connection. When broker security is enabled, a NETWORK client
            // (agent -> regional broker; not the local vm:// path) asserts its identity as
            // "tenant|region|agent" via the JMS username so the broker's CrescoAuthorizationBroker can
            // authorize it. OFF by default -> anonymous connections, exactly as before. (Cryptographic
            // binding of this identity is the mutual-TLS/cert-DN path; see distributed-identity design.)
            PluginBuilder pb = controllerEngine.getPluginBuilder();
            if (pb.getConfig().getBooleanParam("broker_security_enabled", false) && !URI.startsWith("vm://")) {
                String tenant = pb.getConfig().getStringParam("tenant_id", "default");
                String username = tenant + "|" + controllerEngine.cstate.getRegion() + "|" + controllerEngine.cstate.getAgent();
                activeMQSslConnectionFactory.setUserName(username);
                activeMQSslConnectionFactory.setPassword(pb.getConfig().getStringParam("broker_security_secret", "cresco"));
                logger.info("Broker security: asserting identity [{}] on connection to [{}]", username, URI);
            }
            logger.info("ConnectionFactory initialized successfully for URI: {}", URI);
        } catch (Exception ex) {
            logger.error("initConnectionFactory Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
            // Return null on failure
        }
        return activeMQSslConnectionFactory;
    }

    public synchronized void handleConnectionFailure(String uri) {
        logger.error("Handling connection failure for URI [{}]", uri);
        boolean connectionRemoved = false;
        boolean factoryRemoved = false;

        ActiveMQConnection failedConnection = connectionMap.remove(uri);
        if (failedConnection != null) {
            connectionRemoved = true;
            try {
                logger.warn("Closing failed connection object for URI [{}] due to failure.", uri);
                failedConnection.setExceptionListener(null);
                failedConnection.close();
            } catch (JMSException e) {
                logger.warn("Exception while closing failed connection for URI [{}]: {}", uri, e.getMessage());
            }
            logger.info("Removed connection from map for URI [{}] after failure.", uri);
        } else {
            logger.info("No active connection found in map for URI [{}] during failure handling (already removed or never added).", uri);
        }

        if (connectionFactoryMap.remove(uri) != null) {
            factoryRemoved = true;
            logger.info("Removed connection factory from map for URI [{}] after failure.", uri);
        }

        if ((connectionRemoved || factoryRemoved)) {
            if (agentProducer != null) {
                logger.info("Notifying AgentProducer about connection failure for URI [{}]", uri);
                agentProducer.invalidateWorkersForURI(uri);
            } else {
                logger.warn("AgentProducer is null in ActiveClient. Cannot notify about failure for URI [{}]", uri);
            }
        }
    }

    public boolean isConnectionActive(String uri) {
        ActiveMQConnection connection = connectionMap.get(uri);
        if (connection != null) {
            try {
                return connection.isStarted() && !connection.isClosing() && !connection.isClosed();
            } catch (Exception e) {
                logger.warn("Exception while checking status of connection for URI [{}]: {}. Assuming inactive.", uri, e.getMessage());
                return false;
            }
        }
        return false;
    }

    private class ConnectionExceptionListener implements ExceptionListener {
        private final String listenerUri;

        public ConnectionExceptionListener(String uri) {
            this.listenerUri = uri;
        }

        @Override
        public void onException(JMSException exception) {
            logger.error("JMS ExceptionListener triggered for URI [{}]: type [{}], message [{}]",
                    listenerUri, exception.getClass().getName(), exception.getMessage());
            if (exception.getLinkedException() != null) {
                logger.error("--> Linked Exception: type [{}], message [{}]",
                        exception.getLinkedException().getClass().getName(), exception.getLinkedException().getMessage());
            }
            handleConnectionFailure(listenerUri);
        }
    }

    public boolean initActiveAgentConsumer(String RXQueueName, String URI) {
        boolean isInit = false;
        try {
            logger.info("Initializing Agent Consumer for Queue [{}] on URI [{}]", RXQueueName, URI);
            if (this.agentConsumer != null) {
                logger.warn("Existing Agent Consumer found. Shutting it down before creating a new one.");
                this.agentConsumer.shutdown();
            }
            this.agentConsumer = new AgentConsumer(controllerEngine, RXQueueName, URI);
            isInit = true; // Assume success if constructor doesn't throw
            logger.info("Agent Consumer initialized successfully for Queue [{}] on URI [{}]", RXQueueName, URI);
            setFaultTriggerURI(URI);
            logger.debug("Set Fault Trigger URI to [{}]", URI);

        } catch (JMSException jmse) {
            logger.error("JMSException initializing AgentConsumer for Queue [{}] on URI [{}]: {}", RXQueueName, URI, jmse.getMessage(), jmse);
        } catch (Exception ex) {
            logger.error("Exception initializing AgentConsumer for Queue [{}] on URI [{}]: {}", RXQueueName, URI, ex.getMessage(), ex);
        }
        return isInit;
    }

    public boolean initActiveAgentProducer(String URI) {
        boolean isInit = false;
        try {
            logger.info("Initializing Agent Producer for URI [{}]", URI);
            if (this.agentProducer != null) {
                logger.warn("Existing Agent Producer found. Shutting it down before creating a new one.");
                this.agentProducer.shutdown();
            }
            this.agentProducer = new AgentProducer(controllerEngine, URI, this);
            isInit = true; // Assume success if constructor doesn't throw
            logger.info("Agent Producer initialized successfully for URI [{}]", URI);
        } catch (Exception ex) {
            logger.error("initActiveAgentProducer Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
        }
        return isInit;
    }

    public boolean hasActiveProducer() {
        return agentProducer != null;
    }

    public void sendAPMessage(MsgEvent msg) {
        if (hasActiveProducer()) {
            agentProducer.sendMessage(msg);
        } else {
            logger.error("sendAPMessage called but AgentProducer is null. Message not sent: {}", msg.getParams());
        }
    }

    public void shutdown() {
        logger.info("ActiveClient shutting down...");

        if (this.agentProducer != null) {
            logger.info("Shutting down AgentProducer...");
            this.agentProducer.shutdown();
            this.agentProducer = null;
        }

        if (this.agentConsumer != null) {
            logger.info("Shutting down AgentConsumer...");
            this.agentConsumer.shutdown();
            this.agentConsumer = null;
        }

        logger.info("Closing all active JMS connections...");
        // Create a new list from keys to avoid ConcurrentModificationException if handleConnectionFailure modifies the map
        List<String> urisToClose = new ArrayList<>(connectionMap.keySet());
        for (String uri : urisToClose) {
            ActiveMQConnection connection = connectionMap.remove(uri);
            if (connection != null) {
                try {
                    logger.debug("Closing connection for URI [{}]", uri);
                    connection.setExceptionListener(null); // Remove listener before closing
                    connection.close();
                } catch (JMSException e) {
                    logger.warn("Exception closing connection for URI [{}] during shutdown: {}", uri, e.getMessage());
                }
            }
        }
        logger.info("Cleared connection map ({} entries).", urisToClose.size());

        int factoryCount = connectionFactoryMap.size();
        connectionFactoryMap.clear();
        logger.info("Cleared connection factory map ({} entries).", factoryCount);

        logger.info("ActiveClient shutdown complete.");
    }

    public boolean sendMessage(MsgEvent sm) {
        if (agentProducer != null) {
            return agentProducer.sendMessage(sm);
        } else {
            logger.error("sendMessage called directly on ActiveClient, but AgentProducer is null. Message not sent: {}", sm.getParams());
            return false;
        }
    }
}