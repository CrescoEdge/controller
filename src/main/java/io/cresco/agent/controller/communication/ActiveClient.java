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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ActiveClient {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    private Map<String, ActiveMQSslConnectionFactory> connectionFactoryMap;
    private AtomicBoolean lockFactoryMap = new AtomicBoolean();

    private Map<String, ActiveMQConnection> connectionMap;
    private AtomicBoolean lockConnectionMap = new AtomicBoolean();

    private AgentConsumer agentConsumer;
    private AgentProducer agentProducer; // Reference to AgentProducer needed for notifications

    private String faultTriggerURI;

    // Modified Constructor to accept AgentProducer
    public ActiveClient(ControllerEngine controllerEngine){
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ActiveClient.class.getName(), CLogger.Level.Info);
        this.agentProducer = agentProducer; // Store the reference
        connectionFactoryMap = Collections.synchronizedMap(new HashMap<>());
        connectionMap = Collections.synchronizedMap(new HashMap<>());
    }


    private void setFaultTriggerURI(String faultTriggerURI) {
        this.faultTriggerURI = faultTriggerURI;
    }

    private String getFaultTriggerURI() {
        return faultTriggerURI;
    }

    public boolean isFaultURIActive() {
        boolean isActive = false;
        try {
            if(faultTriggerURI != null) {
                // Use the new isConnectionActive method for a more robust check
                isActive = isConnectionActive(faultTriggerURI);
            } else {
                // If no specific fault URI is set, perhaps assume active or check a default?
                // For now, return false if no faultTriggerURI is set.
                isActive = false;
            }
        } catch (Exception ex) {
            logger.error("isFaultURIActive Exception: {}", ex.getMessage(), ex);
            isActive = false; // Assume inactive on error
        }
        return isActive;
    }

    public ActiveMQSession createSession(String URI, boolean transacted, int acknowledgeMode) {
        ActiveMQSession activeMQSession = null;
        try {
            // Get a potentially new or existing connection using the modified getConnection
            ActiveMQConnection activeMQConnection = getConnection(URI);

            if(activeMQConnection != null) {
                // Create session using the obtained connection
                activeMQSession = (ActiveMQSession)activeMQConnection.createSession(transacted, acknowledgeMode);
                logger.debug("Created new session for URI [{}]", URI);
            } else {
                logger.error("Failed to get valid connection for URI [{}] in createSession", URI);
            }

        } catch (Exception ex) {
            logger.error("createSession Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
        }
        return activeMQSession;
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    // Modified getConnection to add ExceptionListener
    private ActiveMQConnection getConnection(String URI) {
        ActiveMQConnection activeMQConnection = null;
        boolean connectionExisted = false; // Flag to check if we're reusing or creating

        try {
            synchronized (lockConnectionMap) {
                if (connectionMap.containsKey(URI)) {
                    activeMQConnection = connectionMap.get(URI);
                    // Verify the existing connection is still valid
                    if (activeMQConnection == null || activeMQConnection.isClosed() || activeMQConnection.isClosing() || !activeMQConnection.isStarted()) {
                        logger.warn("Existing connection for URI [{}] found but is invalid. Cleaning up.", URI);
                        if(activeMQConnection != null) {
                            try {
                                activeMQConnection.close(); // Attempt cleanup
                            } catch (JMSException e) { /* Ignore close error */ }
                        }
                        connectionMap.remove(URI); // Remove invalid entry
                        activeMQConnection = null; // Force recreation
                    } else {
                        connectionExisted = true; // We are reusing a valid, existing connection
                        logger.trace("Reusing existing active connection for URI [{}]", URI);
                    }
                }
            } // End synchronized block for connectionMap read

            // If no valid connection exists, create a new one
            if (activeMQConnection == null) {
                logger.info("No valid connection found for URI [{}]. Attempting to create new connection.", URI);
                ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;

                // Get or create the connection factory
                synchronized (lockFactoryMap) {
                    if (!connectionFactoryMap.containsKey(URI)) {
                        activeMQSslConnectionFactory = initConnectionFactory(URI);
                        if (activeMQSslConnectionFactory != null) {
                            connectionFactoryMap.put(URI, activeMQSslConnectionFactory);
                            logger.info("Created and cached new connection factory for URI [{}]", URI);
                        } else {
                            logger.error("Failed to initialize connection factory for URI [{}]", URI);
                        }
                    } else {
                        activeMQSslConnectionFactory = connectionFactoryMap.get(URI);
                        logger.trace("Reusing existing connection factory for URI [{}]", URI);
                    }
                } // End synchronized block for factoryMap

                // Create connection using the factory
                if (activeMQSslConnectionFactory != null) {
                    try {
                        activeMQConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
                        logger.info("Created new connection object for URI [{}]", URI);

                        // --- MODIFICATION: Attach ExceptionListener ---
                        activeMQConnection.setExceptionListener(new ConnectionExceptionListener(URI));
                        logger.info("Attached ExceptionListener to new connection for URI [{}]", URI);
                        // --- END MODIFICATION ---

                        activeMQConnection.start(); // Start the connection
                        // Wait briefly for the connection to start - adjust timeout as needed
                        int startAttempts = 0;
                        while (!activeMQConnection.isStarted() && startAttempts < 5) {
                            logger.warn("Waiting for connection to URI [{}] to start...", URI);
                            Thread.sleep(500);
                            startAttempts++;
                        }

                        if (activeMQConnection.isStarted()) {
                            logger.info("Connection to URI [{}] started successfully.", URI);
                            synchronized (lockConnectionMap) {
                                connectionMap.put(URI, activeMQConnection); // Add to map only if started
                            }
                        } else {
                            logger.error("Connection to URI [{}] failed to start after {} attempts.", URI, startAttempts);
                            try { activeMQConnection.close(); } catch (JMSException e) { /* Ignore */ } // Cleanup failed connection
                            activeMQConnection = null;
                        }
                    } catch (JMSException jmse) {
                        logger.error("JMSException during connection creation or start for URI [{}]: {}", URI, jmse.getMessage(), jmse);
                        if(activeMQConnection != null) { try { activeMQConnection.close(); } catch (JMSException e) { /* Ignore */ } }
                        activeMQConnection = null;
                        // Also trigger failure handling as creating the connection failed
                        handleConnectionFailure(URI);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("General Exception in getConnection for URI [{}]: {}", URI, ex.getMessage(), ex);
            if(activeMQConnection != null) { try { activeMQConnection.close(); } catch (JMSException e) { /* Ignore */ } }
            activeMQConnection = null; // Ensure null is returned on error
            // Trigger failure handling on general exception too
            handleConnectionFailure(URI);
        }
        return activeMQConnection;
    }


    private ActiveMQSslConnectionFactory initConnectionFactory(String URI) {
        ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;
        try {
            logger.debug("Initializing ConnectionFactory for URI: {}", URI);
            activeMQSslConnectionFactory = new ActiveMQSslConnectionFactory(URI);
            // Apply necessary configurations
            if(URI.startsWith("vm://")) {
                activeMQSslConnectionFactory.setObjectMessageSerializationDefered(true);
            }
            // Set Key and Trust Managers for SSL/TLS
            activeMQSslConnectionFactory.setKeyAndTrustManagers(
                    controllerEngine.getCertificateManager().getKeyManagers(),
                    controllerEngine.getCertificateManager().getTrustManagers(),
                    new SecureRandom()
            );
            logger.info("ConnectionFactory initialized successfully for URI: {}", URI);
        } catch(Exception ex) {
            logger.error("initConnectionFactory Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
        }
        return activeMQSslConnectionFactory;
    }

    // --- NEW METHOD: Handle connection failure notification ---
    // Made public and synchronized for external access and thread safety
    public synchronized void handleConnectionFailure(String uri) {
        logger.error("Handling connection failure for URI [{}]", uri);
        boolean connectionRemoved = false;
        boolean factoryRemoved = false;

        synchronized (lockConnectionMap) {
            if (connectionMap.containsKey(uri)) {
                ActiveMQConnection failedConnection = connectionMap.remove(uri); // Remove first
                connectionRemoved = true;
                if (failedConnection != null) {
                    try {
                        logger.warn("Closing failed connection object for URI [{}]", uri);
                        failedConnection.setExceptionListener(null); // Avoid listener recursion
                        failedConnection.close();
                    } catch (JMSException e) {
                        logger.warn("Exception while closing failed connection for URI [{}]: {}", uri, e.getMessage());
                    }
                }
                logger.info("Removed connection from map for URI [{}]", uri);
            } else {
                logger.info("No active connection found in map for URI [{}] during failure handling.", uri);
            }
        }

        // Optionally remove factory to force recreation
        synchronized (lockFactoryMap) {
            if(connectionFactoryMap.containsKey(uri)) {
                connectionFactoryMap.remove(uri);
                factoryRemoved = true;
                logger.info("Removed connection factory from map for URI [{}]", uri);
            }
        }

        // Notify AgentProducer only if a connection or factory was actually removed
        if ((connectionRemoved || factoryRemoved) && agentProducer != null) {
            logger.info("Notifying AgentProducer about connection failure for URI [{}]", uri);
            agentProducer.invalidateWorkersForURI(uri);
        } else if (agentProducer == null) {
            logger.error("AgentProducer reference is null in ActiveClient. Cannot notify about failure for URI [{}]", uri);
        }
    }

    // --- NEW METHOD: Check connection status ---
    // Made public for external use (e.g., by AgentProducer)
    public boolean isConnectionActive(String uri) {
        synchronized (lockConnectionMap) {
            if (connectionMap.containsKey(uri)) {
                ActiveMQConnection connection = connectionMap.get(uri);
                // Check if started and not closing/closed
                // Added null check for safety
                return connection != null && connection.isStarted() && !connection.isClosing() && !connection.isClosed();
            }
            return false; // Not in map means not active
        }
    }


    // --- NEW INNER CLASS: ExceptionListener ---
    private class ConnectionExceptionListener implements ExceptionListener {
        private final String listenerUri;

        public ConnectionExceptionListener(String uri) {
            this.listenerUri = uri;
        }

        @Override
        public void onException(JMSException exception) {
            logger.error("JMS ExceptionListener triggered for URI [{}]: type [{}], message [{}]",
                    listenerUri, exception.getClass().getName(), exception.getMessage());
            // Log linked exception if it exists
            if (exception.getLinkedException() != null) {
                logger.error("--> Linked Exception: type [{}], message [{}]",
                        exception.getLinkedException().getClass().getName(), exception.getLinkedException().getMessage());
            }
            // Optionally log stack trace for debugging
            // logger.error("Full Stack Trace for JMSException on URI [{}]:", listenerUri, exception);

            // Trigger the cleanup and notification process in ActiveClient
            // Ensure this doesn't cause infinite loops if handleConnectionFailure itself throws JMSException
            handleConnectionFailure(listenerUri);
        }
    }

    // --- Methods related to Consumer/Producer Init ---
    public boolean initActiveAgentConsumer(String RXQueueName, String URI) {
        boolean isInit = false;
        try {
            logger.info("Initializing Agent Consumer for Queue [{}] on URI [{}]", RXQueueName, URI);
            // Ensure previous consumer is shut down if exists
            if (this.agentConsumer != null) {
                logger.warn("Existing Agent Consumer found. Shutting it down before creating a new one.");
                this.agentConsumer.shutdown();
            }
            this.agentConsumer = new AgentConsumer(controllerEngine, RXQueueName, URI);
            isInit = true;
            logger.info("Agent Consumer initialized successfully for Queue [{}] on URI [{}]", RXQueueName, URI);
            // Set this URI as the one to monitor for faults affecting the primary consumer
            setFaultTriggerURI(URI);
            logger.debug("Set Fault Trigger URI to [{}]", URI);

        } catch(Exception ex) {
            logger.error("initActiveAgentConsumer Exception for Queue [{}] on URI [{}]: {}", RXQueueName, URI, ex.getMessage(), ex);
        }
        return isInit;
    }

    public boolean initActiveAgentProducer(String URI) {
        boolean isInit = false;
        try {
            logger.info("Initializing Agent Producer for URI [{}]", URI);
            // Ensure previous producer is shut down if exists
            if (this.agentProducer != null) {
                logger.warn("Existing Agent Producer found. Shutting it down before creating a new one.");
                this.agentProducer.shutdown();
            }
            // Pass 'this' ActiveClient instance to the AgentProducer constructor
            this.agentProducer = new AgentProducer(controllerEngine, URI, this);
            isInit = true;
            logger.info("Agent Producer initialized successfully for URI [{}]", URI);
        } catch(Exception ex) {
            logger.error("initActiveAgentProducer Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
        }
        return isInit;
    }

    // --- Other existing methods (hasActiveProducer, sendAPMessage, shutdown, sendMessage) ---
    public boolean hasActiveProducer() {
        // Check if agentProducer instance exists
        return agentProducer != null;
    }

    public void sendAPMessage(MsgEvent msg) {
        // Delegate sending to the AgentProducer instance
        if(hasActiveProducer()) {
            agentProducer.sendMessage(msg);
        } else {
            logger.error("sendAPMessage called but AgentProducer is null. Message not sent: {}", msg.getParams());
        }
    }

    public void shutdown() {
        logger.info("ActiveClient shutting down...");

        // Shutdown producer first
        if(this.agentProducer != null) {
            logger.info("Shutting down AgentProducer...");
            this.agentProducer.shutdown();
            this.agentProducer = null; // Release reference
        } else {
            logger.info("AgentProducer already null during shutdown.");
        }

        // Shutdown consumer
        if (this.agentConsumer != null) {
            logger.info("Shutting down AgentConsumer...");
            this.agentConsumer.shutdown();
            this.agentConsumer = null; // Release reference
        } else {
            logger.info("AgentConsumer already null during shutdown.");
        }

        // Close all connections
        logger.info("Closing all active JMS connections...");
        List<String> urisToClose = new ArrayList<>();
        synchronized (lockConnectionMap) {
            urisToClose.addAll(connectionMap.keySet()); // Copy keys to avoid ConcurrentModificationException
        }

        for (String uri : urisToClose) {
            synchronized (lockConnectionMap) {
                if (connectionMap.containsKey(uri)) {
                    ActiveMQConnection connection = connectionMap.remove(uri); // Remove from map
                    if (connection != null) {
                        try {
                            logger.debug("Closing connection for URI [{}]", uri);
                            connection.close();
                        } catch (JMSException e) {
                            logger.warn("Exception closing connection for URI [{}] during shutdown: {}", uri, e.getMessage());
                        }
                    }
                }
            }
        }
        logger.info("Cleared connection map ({} entries).", urisToClose.size());


        // Clear factories (optional, but good practice)
        synchronized (lockFactoryMap) {
            int factoryCount = connectionFactoryMap.size();
            connectionFactoryMap.clear();
            logger.info("Cleared connection factory map ({} entries).", factoryCount);
        }

        logger.info("ActiveClient shutdown complete.");
    }

    // Keep this method if direct sending from ActiveClient is needed,
    // but prefer sending via AgentProducer (sendAPMessage)
    public boolean sendMessage(MsgEvent sm) {
        if (agentProducer != null) {
            return agentProducer.sendMessage(sm);
        } else {
            logger.error("sendMessage called directly on ActiveClient, but AgentProducer is null. Message not sent: {}", sm.getParams());
            return false;
        }
    }
}



