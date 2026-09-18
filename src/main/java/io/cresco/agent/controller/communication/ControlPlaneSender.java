package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
 *
 * <p><b>Liveness under a parent loss.</b> While the parent broker is down, (re)connecting the
 * dedicated connection parks the connecting thread inside the failover transport for up to
 * maxReconnectAttempts x delay (~20s). The sender lock is therefore never waited on unboundedly:
 * senders queue behind a (re)connect for at most {@code controlplane_lock_wait_ms} and then fail
 * that send, and {@link #shutdown()} never waits behind senders at all — it flags the sender closed
 * (every waiting/arriving send fails fast, a connect in flight releases what it built on return)
 * and tears the resources down out of band. Before this, dozens of plugin senders each holding the
 * lock ~20s starved shutdown for minutes, which is what parked agents in "STUCK IN CONNECTION
 * FAULT" after a global-controller restart (W-GFS-6).
 */
class ControlPlaneSender {

    private final ControllerEngine controllerEngine;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final String baseURI;
    private final Gson gson = new Gson();
    private final long ttl;
    private final long lockWaitMs;
    // fair: a sender that has waited longest (or shutdown) goes next; no thread can be starved
    private final ReentrantLock lock = new ReentrantLock(true);
    private volatile boolean closed = false;

    private volatile ActiveMQSession session;
    private volatile MessageProducer producer;
    private volatile ActiveMQConnection conn; // the dedicated connection we own (null when pooled)
    private final boolean dedicatedConnection;
    private final Map<String, Destination> destCache = new ConcurrentHashMap<>();

    ControlPlaneSender(ControllerEngine controllerEngine, String baseURI) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ControlPlaneSender.class.getName(), CLogger.Level.Info);
        this.baseURI = baseURI;
        this.ttl = plugin.getConfig().getLongParam("controlplane_ttl", 300000L);
        this.lockWaitMs = plugin.getConfig().getLongParam("controlplane_lock_wait_ms", 5000L);
        // Transport isolation: a dedicated SESSION on the pooled connection still shares ONE TCP
        // socket with the dataplane, so 256KB bulk frames delay the liveness ping at the wire
        // (FIFO OpenWire marshal + TCP backpressure) no matter the JMS priority. Give control its
        // own socket. vm:// is in-JVM (no socket) — pooled is fine there.
        this.dedicatedConnection = !baseURI.startsWith("vm")
                && plugin.getConfig().getBooleanParam("controlplane_dedicated_connection", true);
    }

    private boolean isOpen() {
        ActiveMQSession s = session;
        return s != null && !s.isClosed() && producer != null;
    }

    /** Bounded lock acquisition: never park a sender (or shutdown) behind a stalled (re)connect. */
    private void acquire() throws JMSException {
        try {
            if (lock.tryLock(lockWaitMs, TimeUnit.MILLISECONDS)) return;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new JMSException("ControlPlaneSender: interrupted waiting for the sender lock");
        }
        throw new JMSException("ControlPlaneSender: sender lock busy for " + lockWaitMs + "ms (transport (re)connect or send in progress)");
    }

    private void ensureOpen() throws JMSException {
        if (closed) throw new JMSException("ControlPlaneSender: closed");
        if (isOpen()) return;
        acquire();
        try {
            if (closed) throw new JMSException("ControlPlaneSender: closed");
            if (isOpen()) return;
            closeResources();
            ActiveMQSession s = dedicatedConnection
                    ? controllerEngine.getActiveClient().createDedicatedSession(baseURI, false, Session.AUTO_ACKNOWLEDGE, true)
                    : controllerEngine.getActiveClient().createSession(baseURI, false, Session.AUTO_ACKNOWLEDGE);
            if (s == null) throw new JMSException("ControlPlaneSender: null session for " + baseURI);
            ActiveMQConnection c = null;
            if (dedicatedConnection) {
                try { c = (ActiveMQConnection) s.getConnection(); } catch (Exception ignore) { }
            }
            if (closed) {
                // shutdown raced this (re)connect: release what we just built and fail the send
                try { s.close(); } catch (Exception ignore) { }
                ActiveClient.closeConnectionFast(c);
                throw new JMSException("ControlPlaneSender: closed during (re)connect");
            }
            // Persistent sends are synchronous (await broker receipt), and send() must hold the
            // lock (JMS sessions are not thread-safe) - so a wedged transport would otherwise hold
            // the lock for the whole stall. Bound it: a timed-out send throws, the session is
            // rebuilt, callers see a clean failure. Only on the dedicated connection (we own it;
            // never mutate the shared pooled one).
            if (c != null) {
                try {
                    c.setSendTimeout(plugin.getConfig().getIntegerParam("controlplane_send_timeout_ms", 15000));
                } catch (Exception ex) {
                    logger.warn("ControlPlaneSender: unable to set send timeout: {}", ex.getMessage());
                }
            }
            MessageProducer p = s.createProducer(null); // anonymous; destination chosen per send
            destCache.clear();
            conn = c;
            session = s;
            producer = p;
            logger.info("ControlPlaneSender session (re)initialized for {}{}", baseURI,
                    dedicatedConnection ? " (dedicated control-plane connection)" : "");
        } finally {
            lock.unlock();
        }
    }

    private Destination dest(ActiveMQSession s, String queueName) throws JMSException {
        Destination d = destCache.get(queueName);
        if (d == null) {
            d = s.createQueue(queueName);
            destCache.put(queueName, d);
        }
        return d;
    }

    /** Send a control-plane (liveness/control) message to the given peer queue. Thread-safe. */
    boolean send(String dstQueueName, MsgEvent sm, MsgQoS.Tier tier) {
        for (int attempt = 1; attempt <= 2; attempt++) {
            if (closed) {
                logger.warn("ControlPlaneSender send to [{}] refused: sender closed (parent link re-init in progress)", dstQueueName);
                return false;
            }
            ActiveMQSession used = null;
            try {
                ensureOpen();
                acquire();
                try {
                    used = session;
                    MessageProducer p = producer;
                    if (closed || used == null || p == null) throw new JMSException("ControlPlaneSender: closed");
                    // the send inside the lock is bounded by the connection sendTimeout set in
                    // ensureOpen, so a dead transport fails this send instead of parking senders
                    TextMessage tm = used.createTextMessage(gson.toJson(sm));
                    p.send(dest(used, dstQueueName), tm, tier.deliveryMode, tier.priority, ttl);
                } finally {
                    lock.unlock();
                }
                return true;
            } catch (JMSException e) {
                logger.warn("ControlPlaneSender send to [{}] failed (attempt {}/2): {}", dstQueueName, attempt, e.getMessage());
                closeIfCurrent(used);
            } catch (Exception e) {
                logger.error("ControlPlaneSender unexpected error to [{}]: {}", dstQueueName, e.getMessage(), e);
                closeIfCurrent(used);
            }
        }
        return false;
    }

    /** Rebuild only the session THIS send failed on; never tear down one another sender just rebuilt. */
    private void closeIfCurrent(ActiveMQSession used) {
        if (closed || used == null || used != session) return;
        closeQuietly();
    }

    /** Bounded: takes the lock if it can within the wait, closes regardless (we own these resources). */
    private void closeQuietly() {
        boolean locked = false;
        try {
            locked = lock.tryLock(Math.min(lockWaitMs, 500L), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        try {
            closeResources();
        } finally {
            if (locked) lock.unlock();
        }
    }

    private void closeResources() {
        MessageProducer p = producer;
        ActiveMQSession s = session;
        ActiveMQConnection c = conn;
        producer = null;
        session = null;
        conn = null;
        destCache.clear();
        // On the dedicated connection WE own the socket: dispose a dead transport first so none of
        // these closes can park on a reconnecting failover transport, then close in SEPARATE trys
        // so a throwing close cannot strand the socket. Never close a pooled session's connection.
        if (c != null) ActiveClient.disposeIfDead(c);
        try { if (p != null) p.close(); } catch (Exception ignore) { }
        try { if (s != null && !s.isClosed()) s.close(); } catch (Exception ignore) { }
        ActiveClient.closeConnectionFast(c);
    }

    /**
     * Never waits behind senders: flags closed (every waiting/arriving send fails fast; a connect
     * in flight releases what it built on return), then tears the resources down out of band.
     */
    void shutdown() {
        closed = true;
        closeQuietly();
    }
}
