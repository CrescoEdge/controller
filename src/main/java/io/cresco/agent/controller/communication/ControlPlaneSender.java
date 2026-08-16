package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dedicated, shared sender for CONTROL-PLANE traffic (liveness + control) to any peer. It runs on
 * its OWN JMS session — isolated from the per-destination telemetry producers ({@link ActiveProducerWorker})
 * and the bulk data path ({@link ActiveProducerWorkerData}) — so a flood of telemetry or bulk can
 * never serialize behind, block, or evict the liveness ping.
 *
 * Messages are sent PERSISTENT (immune to the broker's pending-message eviction) with the QoS
 * priority from {@link MsgQoS} (liveness 9 &gt; control 7), so the broker dispatches agent-contact
 * traffic ahead of everything else. One anonymous producer serves all destinations; control-plane
 * volume is low, so a single shared session is both sufficient and scale-friendly for large fabrics.
 */
class ControlPlaneSender {

    private final ControllerEngine controllerEngine;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final String baseURI;
    private final Gson gson = new Gson();
    private final long ttl;
    private final Object lock = new Object();

    private ActiveMQSession session;
    private MessageProducer producer;
    private final boolean dedicatedConnection;
    private final Map<String, Destination> destCache = new ConcurrentHashMap<>();

    ControlPlaneSender(ControllerEngine controllerEngine, String baseURI) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ControlPlaneSender.class.getName(), CLogger.Level.Info);
        this.baseURI = baseURI;
        this.ttl = plugin.getConfig().getLongParam("controlplane_ttl", 300000L);
        // Transport isolation: a dedicated SESSION on the pooled connection still shares ONE TCP
        // socket with the dataplane, so 256KB bulk frames delay the liveness ping at the wire
        // (FIFO OpenWire marshal + TCP backpressure) no matter the JMS priority. Give control its
        // own socket. vm:// is in-JVM (no socket) — pooled is fine there.
        this.dedicatedConnection = !baseURI.startsWith("vm")
                && plugin.getConfig().getBooleanParam("controlplane_dedicated_connection", true);
    }

    private void ensureOpen() throws JMSException {
        if (session != null && !session.isClosed() && producer != null) return;
        synchronized (lock) {
            if (session != null && !session.isClosed() && producer != null) return;
            closeQuietly();
            session = dedicatedConnection
                    ? controllerEngine.getActiveClient().createDedicatedSession(baseURI, false, Session.AUTO_ACKNOWLEDGE, true)
                    : controllerEngine.getActiveClient().createSession(baseURI, false, Session.AUTO_ACKNOWLEDGE);
            if (session == null) throw new JMSException("ControlPlaneSender: null session for " + baseURI);
            producer = session.createProducer(null); // anonymous; destination chosen per send
            destCache.clear();
            logger.info("ControlPlaneSender session (re)initialized for {}{}", baseURI,
                    dedicatedConnection ? " (dedicated control-plane connection)" : "");
        }
    }

    private Destination dest(String queueName) throws JMSException {
        Destination d = destCache.get(queueName);
        if (d == null) {
            d = session.createQueue(queueName);
            destCache.put(queueName, d);
        }
        return d;
    }

    /** Send a control-plane (liveness/control) message to the given peer queue. Thread-safe. */
    boolean send(String dstQueueName, MsgEvent sm, MsgQoS.Tier tier) {
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                ensureOpen();
                synchronized (lock) {
                    TextMessage tm = session.createTextMessage(gson.toJson(sm));
                    producer.send(dest(dstQueueName), tm, tier.deliveryMode, tier.priority, ttl);
                }
                return true;
            } catch (JMSException e) {
                logger.warn("ControlPlaneSender send to [{}] failed (attempt {}/2): {}", dstQueueName, attempt, e.getMessage());
                closeQuietly();
            } catch (Exception e) {
                logger.error("ControlPlaneSender unexpected error to [{}]: {}", dstQueueName, e.getMessage(), e);
                closeQuietly();
            }
        }
        return false;
    }

    private void closeQuietly() {
        synchronized (lock) {
            try { if (producer != null) producer.close(); } catch (Exception ignore) { }
            // On a dedicated connection WE own the socket: close it too or every rebuild leaks one.
            // Never close the connection of a pooled session (shared). Session and connection are
            // closed in SEPARATE trys so a throwing session.close() cannot strand the socket.
            org.apache.activemq.ActiveMQConnection conn = null;
            try {
                if (session != null && dedicatedConnection) {
                    conn = (org.apache.activemq.ActiveMQConnection) session.getConnection();
                }
            } catch (Exception ignore) { }
            try { if (session != null && !session.isClosed()) session.close(); } catch (Exception ignore) { }
            try { if (conn != null && !conn.isClosed()) conn.close(); } catch (Exception ignore) { }
            producer = null;
            session = null;
            destCache.clear();
        }
    }

    void shutdown() {
        closeQuietly();
    }
}
