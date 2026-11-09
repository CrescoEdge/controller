package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.usage.MemoryUsage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dataplane backpressure across broker bridges, in order and without a deadlock.
 *
 * <p>A topic message a bridge delivers waits HERE, in line, while the receiving topic's memory is full: the bridge's
 * data connectors send synchronously (ActiveBroker.buildConnector), so the sending broker acknowledges a message only
 * once this broker has taken it, the bridge's prefetch is its credit window, and a bridge that waits backs up into the
 * sending broker's topic, whose flow control then slows the writers there. ActiveMQ's own flow control would instead
 * PARK such a message and take the next one when room appeared, out of order (measured: 0, 32, 1, 2, 38, 65, 3 ...);
 * so for bridged messages it is turned off and this wait replaces it.
 *
 * <p>Two brokers whose topics are full of traffic for each other (each one's bridge subscription waiting for the other to
 * take) would wait on each other forever. A wait therefore ends once the topic has taken NOTHING for
 * {@code broker_bridge_flow_control_wait_ms} (default 10 s); the bridge then delivers without waiting until the topic has
 * room again ("overflow": the receiving broker keeps the excess pending, in memory and then the temp store), which breaks
 * the cycle. A subscriber that is merely slow keeps taking messages, so it never overflows the bridge. Nothing is
 * discarded either way.
 *
 * <p>Only bridged topic messages: what a client sends is held by the topic's own flow control on its producer-only
 * connection (in order), and queues (the control plane) never fill.
 */
public class DataplaneBridgeInbound implements BrokerPlugin {

    /** Waits that timed out and let a bridge deliver past a full topic (a cycle broken, or a stuck subscriber), since start. */
    public static final AtomicLong OVERFLOWS = new AtomicLong();

    private final CLogger logger;
    private final long waitMs;

    public DataplaneBridgeInbound(PluginBuilder plugin) {
        this(plugin.getLogger(DataplaneBridgeInbound.class.getName(), CLogger.Level.Info),
                plugin.getConfig().getLongParam("broker_bridge_flow_control_wait_ms", 10_000L));
    }

    DataplaneBridgeInbound(CLogger logger, long waitMs) {
        this.logger = logger;
        this.waitMs = Math.max(100L, waitMs);
    }

    @Override
    public Broker installPlugin(Broker next) {
        if (logger != null) {
            logger.info("dataplane backpressure: a bridged topic message waits in line while its topic is full (at most "
                    + waitMs + " ms, then the bridge overflows instead of deadlocking)");
        }
        return new Filter(next);
    }

    /** A bridged (network) topic message that is not an advisory: it waits in line here, not in ActiveMQ's flow control. */
    static boolean bridgedTopicMessage(ConnectionContext ctx, Message m) {
        if (ctx == null || !ctx.isNetworkConnection() || m == null) return false;
        ActiveMQDestination d = m.getDestination();
        return d != null && d.isTopic() && !AdvisorySupport.isAdvisoryTopic(d);
    }

    /**
     * Wait while {@code d}'s memory is full and its subscribers are taking messages; false once it has stayed full for
     * {@code waitMs} with nothing taken at all (a cycle of brokers waiting on each other, or every subscriber stuck). A
     * subscriber whose prefetch window alone exceeds the topic's memory limit (100 x 4 MiB frames > 256 MiB) keeps the
     * topic "full" while it reads, so the measure is progress, not room.
     */
    private boolean awaitRoomOrStall(Destination d, MemoryUsage mu) throws InterruptedException {
        long taken = d.getDestinationStatistics().getDequeues().getCount();
        long since = System.currentTimeMillis();
        while (!mu.waitForSpace(100)) {
            long now = d.getDestinationStatistics().getDequeues().getCount();
            if (now != taken) {
                taken = now;
                since = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - since >= waitMs) {
                return false;
            }
        }
        return true;
    }

    final class Filter extends BrokerFilter {
        /** Bridge connection + topic that timed out a wait and delivers without waiting until the topic has room. */
        private final Map<String, Long> overflowing = new ConcurrentHashMap<>();

        Filter(Broker next) { super(next); }

        @Override
        public void send(ProducerBrokerExchange exchange, Message message) throws Exception {
            ConnectionContext ctx = exchange.getConnectionContext();
            if (!bridgedTopicMessage(ctx, message)) {
                super.send(exchange, message);
                return;
            }
            awaitRoom(ctx, message.getDestination());
            // a bridge connection's commands are handled one at a time (its in-JVM pipe's one task), so turning its
            // context's flow control off for this send and back on is not seen by any other send
            boolean pfc = ctx.isProducerFlowControl();
            ctx.setProducerFlowControl(false);
            try {
                super.send(exchange, message);
            } finally {
                ctx.setProducerFlowControl(pfc);
            }
        }

        private void awaitRoom(ConnectionContext ctx, ActiveMQDestination dest) throws InterruptedException {
            for (Destination d : getDestinations(dest)) {
                MemoryUsage mu = d.getMemoryUsage();
                String key = ctx.getConnectionId() + "|" + dest.getQualifiedName();
                if (mu == null || !mu.isFull()) {
                    if (overflowing.remove(key) != null && logger != null) {
                        logger.info("bridge " + ctx.getConnectionId() + " waits again for room on " + dest.getPhysicalName());
                    }
                    continue;
                }
                if (overflowing.containsKey(key)) continue;
                if (!awaitRoomOrStall(d, mu)) {
                    overflowing.put(key, System.currentTimeMillis());
                    long n = OVERFLOWS.incrementAndGet();
                    if (logger != null) {
                        logger.warn("bridge " + ctx.getConnectionId() + ": " + dest.getPhysicalName() + " stayed full with nothing taken for "
                                + waitMs + " ms; delivering without waiting until it has room (kept pending here, nothing discarded;"
                                + " overflows total=" + n + ")");
                    }
                }
            }
        }
    }
}
