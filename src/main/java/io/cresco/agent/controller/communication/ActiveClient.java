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
import java.util.Collections;
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
    // Dedicated (non-pooled) connections handed out by createDedicatedSession. Tracked with their
    // URI ONLY so refreshTrust/shutdown can close them (they are otherwise invisible to both, which
    // left shard/control sockets running on pre-enrollment TLS material). Owners rebuild lazily.
    // NOTE: a plain class, not a record — the ClassIndex annotation processor (transitive, Siddhi)
    // fails compilation on record components ("Internal error: Unknown element"), which is why
    // nothing else in this tree uses records either.
    private static final class DedicatedConn {
        private final String uri;
        private final ActiveMQConnection conn;
        DedicatedConn(String uri, ActiveMQConnection conn) { this.uri = uri; this.conn = conn; }
        String uri() { return uri; }
        ActiveMQConnection conn() { return conn; }
    }
    private final List<DedicatedConn> dedicatedConnections = Collections.synchronizedList(new ArrayList<>());

    // ---- dead-transport teardown ----------------------------------------------------------
    // A failover transport that is mid-reconnect parks EVERY JMS close() (consumer, producer,
    // session, connection) inside FailoverTransport.oneway() until its reconnect attempts are
    // exhausted (~20s with maxReconnectAttempts=5 / 5s delay), and the closes run serially. On a
    // parent-broker loss that stretched the agent's re-init (isAgentShutdown -> ActiveClient
    // .shutdown) from milliseconds to minutes, leaving the agent parked in "STUCK IN CONNECTION
    // FAULT" long after the parent was back (W-GFS-6). Disposing a DEAD transport first makes
    // every parked oneway() throw immediately, so the teardown is instant; a healthy transport is
    // left alone and closes gracefully (RemoveInfo/ShutdownInfo still reach the broker).
    static boolean isTransportDead(ActiveMQConnection c) {
        if (c == null) return true;
        try {
            if (c.isTransportFailed()) return true;
            org.apache.activemq.transport.Transport t = c.getTransport();
            org.apache.activemq.transport.failover.FailoverTransport ft =
                    (t != null) ? t.narrow(org.apache.activemq.transport.failover.FailoverTransport.class) : null;
            return ft != null && !ft.isConnected();
        } catch (Exception ex) {
            return true;
        }
    }

    /** Dispose the transport of a DEAD connection so parked closes/sends fail fast. @return true if disposed. */
    static boolean disposeIfDead(ActiveMQConnection c) {
        if (c == null || !isTransportDead(c)) return false;
        try {
            org.apache.activemq.transport.Transport t = c.getTransport();
            if (t != null) t.stop();
            return true;
        } catch (Exception ignore) {
            return false;
        }
    }

    /** Close a connection without ever parking on a dead transport (graceful when healthy). */
    static void closeConnectionFast(ActiveMQConnection c) {
        if (c == null) return;
        disposeIfDead(c);
        try { c.setExceptionListener(null); } catch (Exception ignore) { }
        try { if (!c.isClosed()) c.close(); } catch (Exception ignore) { }
    }

    // volatile: the state-machine thread swaps these on a re-init while timer and plugin threads send through them
    private volatile AgentConsumer agentConsumer;
    private volatile AgentProducer agentProducer;

    private String faultTriggerURI;
    private final int MAX_CONNECTION_START_ATTEMPTS = 5;
    private final long CONNECTION_START_RETRY_DELAY_MS = 500;


    // The peer each network broker URI is meant to reach (from the discovery that chose it). Every TLS connection
    // to the URI is bound to it (PeerIdentity): a broker whose certificate names somebody else is refused.
    private final Map<String, PeerIdentity.Expected> expectedPeers = new ConcurrentHashMap<>();

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
            // Dedicated connections (control-plane sender/consumer, dataplane shards) carry the same
            // TLS material; close the NETWORK ones so their owners lazily rebuild with the refreshed
            // identity. vm:// dedicated connections (controllers to their own broker) hold no TLS and
            // their owners cache sessions/producers — closing them would wedge, exactly like the
            // pooled vm:// case above.
            int dedicated = 0;
            synchronized (dedicatedConnections) {
                java.util.Iterator<DedicatedConn> it = dedicatedConnections.iterator();
                while (it.hasNext()) {
                    DedicatedConn dc = it.next();
                    if (dc.uri() != null && dc.uri().startsWith("vm://")) {
                        continue;
                    }
                    ActiveMQConnection c = dc.conn();
                    if (c != null && !c.isClosed()) {
                        try { c.setExceptionListener(null); c.close(); dedicated++; } catch (Exception ignore) { }
                    }
                    it.remove();
                }
            }
            logger.info("ActiveClient trust refreshed — " + refreshed + " pooled + " + dedicated + " dedicated network connection(s) rebuilt with current certificates; vm:// preserved");
        } catch (Exception ex) {
            logger.error("refreshTrust error: " + ex.getMessage());
        }
    }

    /**
     * Record the peer a network broker URI is meant to reach (the region/agent discovery returned for that address).
     * Must be called before the first connection to the URI; with {@code peer_identity_mode=enforce} a URI with no
     * recorded peer is refused. Re-recording a different peer drops the URI's cached factory so new connections
     * bind to the new peer.
     */
    public void expectPeer(String uri, String region, String agent) {
        if (uri == null || uri.startsWith("vm://")) return;
        PeerIdentity.Expected e = new PeerIdentity.Expected(region, agent);
        PeerIdentity.Expected prev = expectedPeers.put(uri, e);
        if (prev != null && !prev.equals(e)) {
            connectionFactoryMap.remove(uri);
            logger.info("Broker URI [{}] now expects peer {} (was {}); its connection factory will be rebuilt", BrokerTransport.forActiveMQ(uri, null), e, prev);
        }
    }

    /** The peer recorded for {@code uri} by {@link #expectPeer}, or null. */
    public PeerIdentity.Expected expectedPeer(String uri) {
        return uri == null ? null : expectedPeers.get(uri);
    }

    /**
     * B-1: Confirm a remote broker's TLS endpoint is actually TRUST-ready before we hand its URI to the
     * ActiveMQ failover transport. On a rapid same-host bring-up the agent can reach the connect step a
     * beat before the regional broker's certificate has been installed in our truststore (discovery cert
     * exchange still settling) or before the broker's TLS transport is fully up. The failover transport's
     * first handshake then fails with "PKIX path building failed: unable to find valid certification path"
     * and dumps full reconnect stack traces before the mesh self-heals — false-alarm log noise, not a real
     * fault. This probe performs a bounded, QUIET TLS handshake with our CURRENT key/trust managers (the
     * same material the real connection uses, including the binding of the broker's certificate to the identity
     * this node meant to reach, see {@link PeerIdentity}; TLS hostname verification is off because brokers are
     * dialled by address and dynamic port), so the failover transport only ever touches a ready endpoint. An
     * identity refusal ends the probe at once: waiting cannot fix it, and the real connection logs the refusal.
     *
     * Bounded and non-blocking-forever: if the endpoint is not confirmed ready within maxWaitMs we log one
     * WARN and return false; the caller proceeds anyway and the failover transport retries exactly as
     * before — so this can only ADD a quiet grace window, never make startup worse. In the common
     * (non-raced) case the first probe handshakes in milliseconds and returns immediately.
     *
     * @return true if a clean TLS handshake completed within the window; false if we gave up (caller proceeds).
     */
    public boolean waitForBrokerTlsReady(String host, int port, long maxWaitMs) {
        return waitForBrokerTlsReady(host, port, maxWaitMs, null);
    }

    /** As {@link #waitForBrokerTlsReady(String, int, long)}, binding the probe to {@code expected}. */
    public boolean waitForBrokerTlsReady(String host, int port, long maxWaitMs, PeerIdentity.Expected expected) {
        String probeHost = (host == null) ? null : host.replace("[", "").replace("]", "");
        if (probeHost == null || probeHost.isEmpty()) {
            logger.warn("waitForBrokerTlsReady: no host to probe — skipping");
            return false;
        }
        logger.info("Probing broker TLS readiness [{}:{}] before first connect (up to {}ms) to avoid a startup PKIX race", probeHost, port, maxWaitMs);
        long deadline = System.currentTimeMillis() + maxWaitMs;
        int attempt = 0;
        String lastError = null;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try {
                javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
                CertificateManager cm = controllerEngine.getCertificateManager();
                ctx.init(cm.getKeyManagers(),
                        expected != null ? cm.clientTrustManagers(expected, "readiness probe " + probeHost + ":" + port) : cm.getTrustManagers(),
                        new SecureRandom());
                try (javax.net.ssl.SSLSocket socket =
                             (javax.net.ssl.SSLSocket) ctx.getSocketFactory().createSocket()) {
                    socket.connect(new java.net.InetSocketAddress(probeHost, port), 3000);
                    socket.setSoTimeout(3000);
                    socket.startHandshake(); // throws until TCP is up AND our truststore trusts the broker cert
                }
                if (attempt > 1) {
                    logger.info("Broker TLS endpoint [{}:{}] became trust-ready after {} probe attempt(s)", probeHost, port, attempt);
                }
                return true;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                if (PeerIdentityTrustManager.isIdentityRefusal(ex)) {
                    logger.error("Broker TLS endpoint [{}:{}] presented a certificate that is not {}: not waiting for it (see the PEER IDENTITY MISMATCH line)",
                            probeHost, port, expected);
                    return false;
                }
                logger.debug("Broker TLS endpoint [{}:{}] not trust-ready yet (probe {}): {}", probeHost, port, attempt, lastError);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        logger.warn("Broker TLS endpoint [{}:{}] not confirmed trust-ready within {}ms ({} probe attempt(s)) — proceeding; failover transport will retry. Last: {}",
                probeHost, port, maxWaitMs, attempt, lastError);
        return false;
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
                // The fault URI tracks the AgentConsumer's link to the parent broker. With the
                // consumer on a DEDICATED connection there may be no pooled connection for this
                // URI at all (waiters like DataPlaneServiceImpl.getSession spun forever on the
                // map lookup during startup) — ask the consumer for its actual connection state.
                AgentConsumer consumer = agentConsumer;
                if (consumer != null) {
                    isActive = consumer.isConnectionActive();
                } else {
                    isActive = isConnectionActive(faultTriggerURI);
                }
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

    /**
     * Whether a control-plane owner (ControlPlaneSender, the AgentConsumer inbox) gets its own
     * connection to {@code uri} instead of the pooled one the dataplane uses. On a network URI the
     * owner's flag decides (default true: its own socket). On vm:// (an agent on its own broker,
     * e.g. the global controller) {@code controlplane_dedicated_vm} decides (default true): the
     * pooled vm:// connection is shared with the dataplane, so without it control and bulk share
     * one transport and one session-dispatch path (OUT-81). Owners open the dedicated connection
     * with quietFailure=true, so recovery is theirs either way.
     */
    static boolean dedicatedControlConnection(io.cresco.library.plugin.Config config, String uri, String networkFlag) {
        if (uri != null && uri.startsWith("vm")) {
            return config.getBooleanParam("controlplane_dedicated_vm", true);
        }
        return config.getBooleanParam(networkFlag, true);
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
        return createDedicatedSession(URI, transacted, acknowledgeMode, false);
    }

    /**
     * @param quietFailure true = a transport failure on THIS connection only logs and prunes it from
     *        the registry; it does NOT invoke handleConnectionFailure, which would tear down the
     *        healthy pooled connection and invalidate every producer worker. Used by owners with
     *        their own recovery (ControlPlaneSender rebuilds lazily on send failure; AgentConsumer
     *        failure surfaces through isFaultURIActive -> the fault state machine re-inits).
     */
    public ActiveMQSession createDedicatedSession(String URI, boolean transacted, int acknowledgeMode, boolean quietFailure) {
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
            if (quietFailure) {
                conn.setExceptionListener(e -> {
                    logger.warn("Dedicated connection to [{}] failed (owner recovers): {}", URI, e.getMessage());
                    synchronized (dedicatedConnections) {
                        dedicatedConnections.removeIf(dc -> dc.conn() == conn);
                    }
                    closeConnectionFast(conn);
                });
            } else {
                conn.setExceptionListener(new ConnectionExceptionListener(URI));
            }
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
            synchronized (dedicatedConnections) {
                // prune dead entries: closed AND transport-failed (a failover-exhausted connection
                // never reads as closed but is permanently dead — it would pin sockets forever)
                java.util.Iterator<DedicatedConn> it = dedicatedConnections.iterator();
                while (it.hasNext()) {
                    DedicatedConn dc = it.next();
                    ActiveMQConnection c = dc.conn();
                    if (c == null || c.isClosed() || c.isTransportFailed()) {
                        closeConnectionFast(c);
                        it.remove();
                    }
                }
                dedicatedConnections.add(new DedicatedConn(URI, conn));
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
            // the connection map stays keyed by URI; ActiveMQ gets it without any secret-looking parameter
            activeMQSslConnectionFactory = new ActiveMQSslConnectionFactory(BrokerTransport.forActiveMQ(URI, logger));
            if (URI.startsWith("vm://")) {
                activeMQSslConnectionFactory.setObjectMessageSerializationDefered(true);
                // mark this controller's own in-JVM connections: for recipient-bound dataplane streams the broker treats
                // them (and so every plugin and wsapi client using them) as this controller's identity, not infrastructure
                String tenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(controllerEngine.getPluginBuilder().getConfig());
                activeMQSslConnectionFactory.setUserName(CrescoAuthorizationBroker.LOCAL_CONTROLLER_PREFIX
                        + tenant + "|" + controllerEngine.cstate.getRegion() + "|" + controllerEngine.cstate.getAgent());
            } else {
                // vm:// is in-JVM and never does TLS: only network URIs need the key/trust managers
                // the broker's certificate must name the peer this URI was opened to (PeerIdentity); a URI with no
                // recorded peer gets a null expectation, which peer_identity_mode=enforce refuses (fail closed)
                PeerIdentity.Expected expected = expectedPeers.get(URI);
                if (expected == null && controllerEngine.getCertificateManager().peerIdentityMode() != PeerIdentity.Mode.OFF) {
                    logger.warn("No expected peer identity recorded for broker URI [{}]; its TLS connections will be refused under {}=enforce",
                            BrokerTransport.forActiveMQ(URI, null), PeerIdentity.MODE_KEY);
                }
                activeMQSslConnectionFactory.setKeyAndTrustManagers(
                        controllerEngine.getCertificateManager().getKeyManagers(),
                        controllerEngine.getCertificateManager().clientTrustManagers(expected, "broker connection " + endpointOf(URI)),
                        new SecureRandom()
                );
            }

            // Tenant identity on the connection. When broker security is enabled, a NETWORK client
            // (agent -> regional broker; not the local vm:// path) asserts its identity as
            // "tenant|region|agent" via the JMS username so the broker's CrescoAuthorizationBroker can
            // authorize it. OFF by default -> anonymous connections, exactly as before. (Cryptographic
            // binding of this identity is the mutual-TLS/cert-DN path; see distributed-identity design.)
            PluginBuilder pb = controllerEngine.getPluginBuilder();
            if (SecuritySettings.brokerSecurity(pb.getConfig()) && !URI.startsWith("vm://")) {
                String tenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(pb.getConfig());
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

    /** host:port of the first transport in a (failover:/static:) broker URI, for logs. */
    static String endpointOf(String uri) {
        if (uri == null) return "?";
        int s = uri.indexOf("://");
        if (s < 0) return uri;
        int e = s + 3;
        while (e < uri.length() && "?,)".indexOf(uri.charAt(e)) < 0) e++;
        return uri.substring(s + 3, e);
    }

    public synchronized void handleConnectionFailure(String uri) {
        logger.error("Handling connection failure for URI [{}]", uri);
        boolean connectionRemoved = false;
        boolean factoryRemoved = false;

        ActiveMQConnection failedConnection = connectionMap.remove(uri);
        if (failedConnection != null) {
            connectionRemoved = true;
            logger.warn("Closing failed connection object for URI [{}] due to failure.", uri);
            closeConnectionFast(failedConnection);
            logger.info("Removed connection from map for URI [{}] after failure.", uri);
        } else {
            logger.info("No active connection found in map for URI [{}] during failure handling (already removed or never added).", uri);
        }

        if (connectionFactoryMap.remove(uri) != null) {
            factoryRemoved = true;
            logger.info("Removed connection factory from map for URI [{}] after failure.", uri);
        }

        if ((connectionRemoved || factoryRemoved)) {
            AgentProducer producer = agentProducer;
            if (producer != null) {
                logger.info("Notifying AgentProducer about connection failure for URI [{}]", uri);
                producer.invalidateWorkersForURI(uri);
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
            AgentConsumer old = this.agentConsumer;
            if (old != null) {
                logger.warn("Existing Agent Consumer found. Shutting it down before creating a new one.");
                // Null BEFORE constructing: if the constructor throws, a stale field pointing at the
                // shut-down consumer would make isFaultURIActive() false FOREVER (its dedicated
                // connection never restarts) and permanently hang initIOChannels' wait loop.
                // (Unlike the producer, the old consumer must go first: two consumers would split the inbox.)
                this.agentConsumer = null;
                old.shutdown();
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

    /**
     * controller#26: build the new producer first, then swap it in, then shut the old one down. Shutting the old
     * one down first left a window where every send landed on its closed ControlPlaneSender ("ControlPlaneSender:
     * closed"), and a constructor that threw left that shut-down producer installed for good. On a failure the
     * field is cleared instead, so senders see "AgentProducer is null" and the caller's re-init retries.
     */
    public boolean initActiveAgentProducer(String URI) {
        boolean isInit = false;
        AgentProducer old = this.agentProducer;
        try {
            logger.info("Initializing Agent Producer for URI [{}]", URI);
            this.agentProducer = new AgentProducer(controllerEngine, URI, this);
            isInit = true; // Assume success if constructor doesn't throw
            logger.info("Agent Producer initialized successfully for URI [{}]", URI);
            if (nearWaiting.get() > 0 || farWaiting.get() > 0) onHoldTimer(this::drain);   // what waited for a producer goes now
        } catch (Exception ex) {
            logger.error("initActiveAgentProducer Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
            this.agentProducer = null;
        }
        if (old != null) {
            logger.warn("Shutting down the previous Agent Producer.");
            old.shutdown();
        }
        return isInit;
    }

    public boolean hasActiveProducer() {
        return agentProducer != null;
    }

    /** The current producer (tests). */
    AgentProducer agentProducer() { return agentProducer; }

    /**
     * Hand {@code msg} to the producer. False when it was not sent: counted as dropped, here when there is no producer
     * (the link is being made anew), by the producer when its send failed.
     */
    /** The queue {@code msg} would be sent to ({@link AgentProducer#queueOf}); null without a producer or a destination. */
    public String queueOf(MsgEvent msg) {
        AgentProducer p = agentProducer;
        return p == null ? null : p.queueOf(msg);
    }

    public boolean sendAPMessage(MsgEvent msg) {
        return send(msg, "sendAPMessage called but");
    }

    // ---- sends this node cannot make yet (SendHold): no producer, or no credential their recipient can verify

    /**
     * Two queues, each first in, first out: what an agent would sign for a node beyond its controller (it waits for the
     * attestation too: {@link MsgSigning#needsAttestation}), and everything else. Two, because what waits for the
     * attestation must not hold up the ping to the controller that brings it; within each, whatever goes to one
     * destination is in one queue, so its order is kept. {@code *Waiting}: the messages of that kind held, or being sent
     * by the timer thread at this moment -- while it is not 0, a later message of that kind goes behind them.
     */
    private final SendHold nearHold = new SendHold(), farHold = new SendHold();
    private final java.util.concurrent.atomic.AtomicInteger nearWaiting = new java.util.concurrent.atomic.AtomicInteger(),
            farWaiting = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong sendsHeld = new java.util.concurrent.atomic.AtomicLong(),
            heldSent = new java.util.concurrent.atomic.AtomicLong(), heldNotSent = new java.util.concurrent.atomic.AtomicLong();
    private final Object holdTimerLock = new Object();
    private java.util.concurrent.ScheduledThreadPoolExecutor holdTimer;
    /** When the timer next looks at what is held ({@link System#nanoTime}; 0: nothing is scheduled); guarded by {@link #holdTimerLock}. */
    private long holdWakeAt;

    /** Messages held so far; of those, sent when the node could, and not sent (counted as dropped under their cause). */
    public long sendsHeldCount() { return sendsHeld.get(); }
    public long heldSentCount() { return heldSent.get(); }
    public long heldNotSentCount() { return heldNotSent.get(); }
    /** Messages waiting now (tests). */
    int sendsHeldNow() { return nearHold.size() + farHold.size(); }

    /** How long a stretch without a producer lasts at most: as long as a message waits for the parent link anywhere else ({@code msgin_fault_wait_ms}). */
    private long sendHoldMs() {
        return Math.max(0L, plugin.getConfig().getLongParam("msgin_fault_wait_ms", 20000L));
    }

    /**
     * How long a stretch without the attestation lasts at most: as long as the inbox holds what it cannot judge yet
     * ({@code msg_signing_hold_ms}, 5 s; 0: not at all).
     */
    private long attestationHoldMs() {
        return Math.max(0L, Math.min(60_000L, plugin.getConfig().getLongParam("msg_signing_hold_ms", 5_000L)));
    }

    /**
     * Send {@code m}, or hold it until this node can. Nearly always it can: a producer, nothing held, and a node that is
     * not waiting for its attestation -- three field reads, then the producer. Otherwise:
     * <ul>
     * <li><b>No producer</b> (the link to a controller is being made anew; the producer goes when the controller is lost
     *     and comes back with the next one). An RPC request is not held: false, and the router answers its caller with an
     *     error at once ({@code route unavailable}), which is better than a request that leaves after its caller stopped
     *     waiting. Anything else -- the reply to a request this node took, a one-way message -- is held and sent when the
     *     producer exists.</li>
     * <li><b>No attestation yet</b> ({@link MsgSigning#attestationPending}): what an agent would sign for a node beyond
     *     its controller, which cannot verify its certificate, is held for the attestation (the controller's first pong,
     *     some tens of milliseconds later) -- an RPC request too: it leaves in time to be answered, where it would have
     *     been refused. What goes to its controller is not.</li>
     * <li><b>Something of its kind is held, or is being sent by the timer.</b> It goes behind it: first in, first out. A
     *     stretch has ONE end, set when its first message is held ({@code msgin_fault_wait_ms} without a producer,
     *     {@code msg_signing_hold_ms} without the attestation): then everything held is sent as it stands when there is a
     *     producer, and counted as dropped ({@code no_sender}) when there is none.</li>
     * <li><b>No room</b> (each queue: {@value SendHold#MAX_MESSAGES} messages, {@value SendHold#MAX_CHARS} characters).
     *     Nothing of its kind held: it is sent now, as before there was a hold (or, with no producer, not sent). Behind
     *     held messages: not sent ({@code hold_full}) -- sending it would overtake them.</li>
     * </ul>
     * No thread waits here: the sender returns at once, and one timer thread (made at the first held message, gone when
     * idle) sends or drops what is held, in order.
     */
    private boolean send(MsgEvent m, String where) {
        AgentProducer p = agentProducer;
        if (p != null && nearWaiting.get() == 0 && farWaiting.get() == 0) {
            MsgSigning signing = controllerEngine.msgSigning();
            if (!signing.attestationPending() || !signing.needsAttestation(m)) return p.sendMessage(m);
            if (!hold(farHold, farWaiting, m, attestationHoldMs(), "it is not attested yet")) return p.sendMessage(m);
            // attested between the look and the hold: nothing waits for a pong that has come
            if (!signing.attestationPending()) onHoldTimer(this::drain);
            return true;
        }
        if (m == null || m.getParams() == null) return p != null ? p.sendMessage(m) : notSent(m, where);
        MsgSigning signing = controllerEngine.msgSigning();
        boolean far = signing.needsAttestation(m);
        SendHold queue = far ? farHold : nearHold;
        java.util.concurrent.atomic.AtomicInteger waiting = far ? farWaiting : nearWaiting;
        if (p == null) {
            if (MsgRouter.isRpcRequest(m)) return notSent(m, where);
            if (hold(queue, waiting, m, sendHoldMs(), "it has no producer")) {
                // a producer came between the look at the field and the hold: nothing waits for the next one
                if (agentProducer != null) onHoldTimer(this::drain);
                return true;
            }
            return notSent(m, where);
        }
        if (waiting.get() == 0) {
            // nothing of its kind is held or on its way out: it does not wait for the other kind
            if (!far || !signing.attestationPending()) return p.sendMessage(m);
            if (!hold(queue, waiting, m, attestationHoldMs(), "it is not attested yet")) return p.sendMessage(m);
            if (!signing.attestationPending()) onHoldTimer(this::drain);
            return true;
        }
        if (hold(queue, waiting, m, far ? attestationHoldMs() : sendHoldMs(), "it goes behind what is held")) {
            onHoldTimer(this::drain);
            return true;
        }
        // behind held messages, and no room: sent now it would overtake them
        logger.error("Message not sent: no room behind the messages this node holds to be sent first: {}",
                io.cresco.agent.controller.communication.ParamLog.describe(m));
        controllerEngine.msgCounts().dropped(MsgCounts.Drop.HOLD_FULL, m);
        return false;
    }

    private boolean notSent(MsgEvent m, String where) {
        logger.error(where + " AgentProducer is null. Message not sent: {}", io.cresco.agent.controller.communication.ParamLog.describe(m));
        controllerEngine.msgCounts().dropped(MsgCounts.Drop.NO_SENDER, m);
        return false;
    }

    /** The controller above this node now ({@code region_agent}: an agent's region controller, a region's global), or null. */
    private String uplink() {
        io.cresco.agent.core.ControllerStateImp cs = controllerEngine.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        return st == null || st.parentRegion == null || st.parentAgent == null ? null : st.parentRegion + "_" + st.parentAgent;
    }

    /** Put {@code m} behind what {@code queue} holds; false: no room (or the node is stopping). */
    private boolean hold(SendHold queue, java.util.concurrent.atomic.AtomicInteger waiting, MsgEvent m, long holdMs, String why) {
        waiting.incrementAndGet();                           // first: from here on a later message of its kind goes behind it
        if (!queue.offer(m, uplink(), holdMs)) {
            waiting.decrementAndGet();
            return false;
        }
        sendsHeld.incrementAndGet();
        if (queue.size() == 1) {
            logger.info("Holding what this node cannot send yet ({}; first: {}): at most {} messages, {} ms for all of them; then"
                    + " they are sent, or counted as dropped", why, io.cresco.agent.controller.communication.ParamLog.describe(m),
                    SendHold.MAX_MESSAGES, queue.remainingMs());
        }
        wakeIn(queue.remainingMs());
        return true;
    }

    private java.util.concurrent.ScheduledThreadPoolExecutor holdTimer() {
        synchronized (holdTimerLock) {
            if (holdTimer == null) {
                java.util.concurrent.ScheduledThreadPoolExecutor t = new java.util.concurrent.ScheduledThreadPoolExecutor(1, r -> {
                    Thread th = new Thread(r, "cresco-send-hold");
                    th.setDaemon(true);
                    return th;
                });
                t.setKeepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS);
                t.allowCoreThreadTimeOut(true);
                t.setRemoveOnCancelPolicy(true);
                holdTimer = t;
            }
            return holdTimer;
        }
    }

    private void onHoldTimer(Runnable r) {
        try {
            holdTimer().execute(r);
        } catch (java.util.concurrent.RejectedExecutionException stopping) {
            // the node is stopping: stopHolding counts what is held
        }
    }

    /** The timer looks at what is held in {@code ms} at the latest (one wake is scheduled at a time: the earliest asked for). */
    private void wakeIn(long ms) {
        if (ms < 0) return;
        long at = System.nanoTime() + ms * 1_000_000L;
        synchronized (holdTimerLock) {
            if (holdWakeAt != 0 && holdWakeAt - at <= 0) return;
            holdWakeAt = at == 0 ? 1 : at;
        }
        try {
            holdTimer().schedule(() -> {
                synchronized (holdTimerLock) { holdWakeAt = 0; }
                drain();
            }, ms + 1, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException stopping) {
            // the node is stopping: stopHolding counts what is held
        }
    }

    /** This node took an attestation (MsgSigning): what waited for it goes now. */
    void attested() {
        if (farWaiting.get() > 0) onHoldTimer(this::drain);
    }

    /**
     * The timer thread, and only it: send what is held and can go, oldest first, each queue by itself; then come back when
     * the stretch of what is left ends. It runs when a message is held, when the producer or the attestation comes, and
     * when a stretch ends -- so nothing held waits for an event that has already happened.
     */
    private void drain() {
        try {
            drain(nearHold, nearWaiting, false);
            drain(farHold, farWaiting, true);
        } finally {
            long a = nearHold.remainingMs(), b = farHold.remainingMs();
            if (a >= 0 || b >= 0) wakeIn(a < 0 ? b : b < 0 ? a : Math.min(a, b));
        }
    }

    private void drain(SendHold queue, java.util.concurrent.atomic.AtomicInteger waiting, boolean far) {
        int sent = 0, lost = 0;
        MsgEvent firstLost = null;
        boolean asTheyStand = false;
        for (;;) {
            SendHold.Held head = queue.peek();
            if (head == null) break;
            AgentProducer p = agentProducer;
            boolean timeUp = queue.timeUp();
            if (p == null) {
                if (!timeUp) break;                              // it, and all behind it, wait for a producer
                if (!queue.take(head)) continue;
                // the stretch ended and no producer came: not sent
                dropHeld(head.msg, MsgCounts.Drop.NO_SENDER, "no producer came in time");
                waiting.decrementAndGet();
                lost++;
                if (firstLost == null) firstLost = head.msg;
                continue;
            }
            if (far && !timeUp && controllerEngine.msgSigning().attestationPending()) {
                // a producer, no attestation: it, and all behind it, wait for the attestation -- from now on for the time
                // an attestation is waited for, whatever the stretch was begun for
                queue.endWithin(attestationHoldMs());
                break;
            }
            if (!queue.take(head)) continue;
            try {
                String up = uplink();
                if (head.uplink != null && up != null && !head.uplink.equals(up) && head.uplink.equals(head.msg.getForwardDst())) {
                    // held for the controller this node had; it has another now. Its destination was chosen for the former
                    // one (a registration, a report, a function of "the controller above"): it is not sent to it from here
                    dropHeld(head.msg, MsgCounts.Drop.PARENT_CHANGED, "this node joined another controller meanwhile");
                    lost++;
                    if (firstLost == null) firstLost = head.msg;
                } else if (p.sendMessage(head.msg)) {
                    sent++;
                    asTheyStand |= timeUp && far;
                } else {
                    // counted by the producer (send_failed); it was told "held" when it was taken, so what a failed send
                    // owes its sender is settled here
                    settle(head.msg, "the broker did not take it");
                }
            } catch (RuntimeException e) {
                controllerEngine.msgCounts().dropped(MsgCounts.Drop.SEND_FAILED, head.msg);
                logger.error("A held message was not sent ({}): {}", e.toString(), io.cresco.agent.controller.communication.ParamLog.describe(head.msg));
            } finally {
                waiting.decrementAndGet();                       // after its send returned: nothing of its kind overtook it
            }
        }
        if (sent > 0) {
            heldSent.addAndGet(sent);
            logger.info("Sent {} message(s) that were held until this node could send them{}", sent,
                    asTheyStand ? " (the attestation did not come in time: sent as they stood)" : "");
        }
        if (lost > 0) {
            heldNotSent.addAndGet(lost);
            logger.error("NOT SENT: {} held message(s) (first: {})", lost, io.cresco.agent.controller.communication.ParamLog.describe(firstLost));
        }
    }

    /** A held message ends here, not sent: counted once, under {@code cause}; then what its sender is owed. */
    private void dropHeld(MsgEvent m, MsgCounts.Drop cause, String why) {
        controllerEngine.msgCounts().dropped(cause, m);
        settle(m, why);
    }

    /**
     * A held message was not sent after its sender was told "held". A message this node only carried on was counted as
     * relayed then: that is taken back, so that it ends in one count. An RPC request's caller is answered with the
     * router's error, so that it is not left to time out (a liveness ping is not: any answer reads as a pong).
     */
    private void settle(MsgEvent m, String why) {
        try {
            if (m.getParam(MsgSigning.PENDING) == null && !ownSource(m)) controllerEngine.msgCounts().notRelayed();
            if (MsgRouter.isRpcRequest(m) && !"ping".equals(m.getParam("action"))) {
                MsgRouter.answerWithError(plugin, m, MsgRouter.ROUTE_UNAVAILABLE, "HOLD", "route unavailable at " + plugin.getRegion()
                        + "_" + plugin.getAgent() + ": " + why);
            }
        } catch (RuntimeException e) {
            logger.warn("could not settle a held message that was not sent: {}", e.toString());
        }
    }

    /** Does {@code m} name this node as its source (with security off nothing else says this node originated it)? */
    private boolean ownSource(MsgEvent m) {
        io.cresco.agent.core.ControllerStateImp cs = controllerEngine.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        return st != null && st.region != null && st.region.equals(m.getSrcRegion()) && st.agent != null && st.agent.equals(m.getSrcAgent());
    }

    /** The node stops: nothing more is held, and what is held is counted as dropped. */
    public void stopHolding() {
        int left = 0;
        for (SendHold queue : new SendHold[]{nearHold, farHold}) {
            for (SendHold.Held h : queue.close()) {
                left++;
                controllerEngine.msgCounts().dropped(MsgCounts.Drop.NO_SENDER, h.msg);
                if (h.msg.getParam(MsgSigning.PENDING) == null && !ownSource(h.msg)) controllerEngine.msgCounts().notRelayed();
            }
        }
        nearWaiting.set(0);
        farWaiting.set(0);
        if (left > 0) heldNotSent.addAndGet(left);
        synchronized (holdTimerLock) {
            if (holdTimer != null) holdTimer.shutdownNow();
        }
    }

    public void shutdown() {
        logger.info("ActiveClient shutting down...");

        // Pre-pass: dispose every DEAD transport (dedicated + pooled) BEFORE tearing down the
        // producer/consumer, so none of the closes below can park on a reconnecting failover
        // transport. This is what keeps a parent-loss re-init at sub-second instead of minutes.
        int disposed = 0;
        synchronized (dedicatedConnections) {
            for (DedicatedConn dc : dedicatedConnections) {
                if (disposeIfDead(dc.conn())) disposed++;
            }
        }
        for (ActiveMQConnection c : new ArrayList<>(connectionMap.values())) {
            if (disposeIfDead(c)) disposed++;
        }
        if (disposed > 0) {
            logger.warn("ActiveClient shutdown: disposed {} dead transport(s) up front so teardown cannot park on reconnect", disposed);
        }

        // clear each field before shutting its object down: a send arriving meanwhile sees "AgentProducer is null"
        // rather than a closed sender (controller#26)
        AgentProducer producer = this.agentProducer;
        this.agentProducer = null;
        if (producer != null) {
            logger.info("Shutting down AgentProducer...");
            producer.shutdown();
        }

        AgentConsumer consumer = this.agentConsumer;
        this.agentConsumer = null;
        if (consumer != null) {
            logger.info("Shutting down AgentConsumer...");
            consumer.shutdown();
        }

        logger.info("Closing all active JMS connections...");
        synchronized (dedicatedConnections) {
            for (DedicatedConn dc : dedicatedConnections) {
                closeConnectionFast(dc.conn());
            }
            dedicatedConnections.clear();
        }
        // Create a new list from keys to avoid ConcurrentModificationException if handleConnectionFailure modifies the map
        List<String> urisToClose = new ArrayList<>(connectionMap.keySet());
        for (String uri : urisToClose) {
            ActiveMQConnection connection = connectionMap.remove(uri);
            if (connection != null) {
                logger.debug("Closing connection for URI [{}]", uri);
                closeConnectionFast(connection);
            }
        }
        logger.info("Cleared connection map ({} entries).", urisToClose.size());

        int factoryCount = connectionFactoryMap.size();
        connectionFactoryMap.clear();
        logger.info("Cleared connection factory map ({} entries).", factoryCount);

        logger.info("ActiveClient shutdown complete.");
    }

    /** As {@link #sendAPMessage}: false when {@code sm} was not sent, which is then counted as dropped (once). */
    public boolean sendMessage(MsgEvent sm) {
        return send(sm, "sendMessage called directly on ActiveClient, but");
    }
}