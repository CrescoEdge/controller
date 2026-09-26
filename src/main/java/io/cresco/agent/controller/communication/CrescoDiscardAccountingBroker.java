package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Makes the broker's own message loss visible. A non-durable topic subscriber (every dataplane consumer)
 * that falls more than its pending-message limit behind (prefetch x prefetch_rate_multiplier; 100 x 2.5 =
 * 250 messages by default) has its oldest pending messages DISCARDED by ActiveMQ, which logs that only at
 * DEBUG on its own logger. Cresco does not surface ActiveMQ's logs, so that loss was silent. This filter
 * counts every discard ({@link #DISCARDED}, the Micrometer counter dataplane.broker.discarded) and every
 * slow-consumer episode ({@link #SLOW_CONSUMERS}, dataplane.broker.slow.consumers), and logs them through the
 * Cresco logger, rate-limited: the first of a burst at once, then at most one summary per interval.
 *
 * <p>ActiveMQ reaches the broker chain for these events only when the destination policy asks for the
 * advisory (advisoryForDiscardingMessages / advisoryForSlowConsumers); ActiveBroker sets both on the topic
 * policies. The events are not passed on down the chain, so no advisory messages are produced.
 */
public class CrescoDiscardAccountingBroker implements BrokerPlugin {

    /** Messages the broker discarded for slow topic subscribers since this JVM started. */
    public static final AtomicLong DISCARDED = new AtomicLong();
    /** Slow-consumer episodes (a subscriber with more than its prefetch pending) since this JVM started. */
    public static final AtomicLong SLOW_CONSUMERS = new AtomicLong();

    private final CLogger logger;
    private final long logIntervalMs;
    private final AtomicLong lastDiscardLogMs = new AtomicLong();
    private final AtomicLong discardedAtLastLog = new AtomicLong();
    private final AtomicLong lastSlowLogMs = new AtomicLong();
    private final AtomicLong slowAtLastLog = new AtomicLong();
    // what the last event concerned; described only when a line is logged (a discard burst can be thousands)
    private volatile Subscription lastDiscardSub;
    private volatile String lastDiscardDest = "?";
    private volatile Subscription lastSlowSub;
    private volatile String lastSlowDest = "?";
    private volatile java.util.concurrent.ScheduledExecutorService flusher;

    public CrescoDiscardAccountingBroker(PluginBuilder plugin) {
        this.logger = plugin.getLogger(CrescoDiscardAccountingBroker.class.getName(), CLogger.Level.Info);
        this.logIntervalMs = Math.max(100L, plugin.getConfig().getLongParam("broker_discard_log_interval_ms", 5000L));
    }

    @Override
    public Broker installPlugin(Broker next) { return new AccountingFilter(next); }

    private void logDiscards(long total) {
        long since = total - discardedAtLastLog.getAndSet(total);
        if (since > 0) logger.warn("broker DISCARDED " + since + " message(s) for slow subscriber(s), last on " + lastDiscardDest
                + " for " + describe(lastDiscardSub) + " (past its pending-message limit); dataplane.broker.discarded total=" + total);
    }

    private void logSlow(long total) {
        long since = total - slowAtLastLog.getAndSet(total);
        if (since > 0) logger.info(since + " slow-subscriber episode(s) (more than prefetch pending, no loss until the pending-message limit), last "
                + "on " + lastSlowDest + ": " + describe(lastSlowSub) + "; dataplane.broker.slow.consumers total=" + total);
    }

    /** Logs whatever arrived after the last line of a burst, so the tail of a burst is never unlogged. */
    private void flush(boolean force) {
        try {
            if (DISCARDED.get() != discardedAtLastLog.get() && (force || due(lastDiscardLogMs))) logDiscards(DISCARDED.get());
            if (SLOW_CONSUMERS.get() != slowAtLastLog.get() && (force || due(lastSlowLogMs))) logSlow(SLOW_CONSUMERS.get());
        } catch (Throwable ignore) { }
    }

    private static String describe(Subscription sub) {
        if (sub == null) return "unknown subscriber";
        try {
            String remote = (sub.getContext() != null && sub.getContext().getConnection() != null)
                    ? sub.getContext().getConnection().getRemoteAddress() : null;
            return sub.getConsumerInfo().getConsumerId() + (remote != null ? " (" + remote + ")" : "")
                    + " prefetch=" + sub.getPrefetchSize();
        } catch (Exception e) {
            return String.valueOf(sub);
        }
    }

    /** True when a line may be logged now: the first event after a quiet interval, then one per interval. */
    private boolean due(AtomicLong last) {
        long now = System.currentTimeMillis();
        long prev = last.get();
        return (now - prev >= logIntervalMs) && last.compareAndSet(prev, now);
    }

    private final class AccountingFilter extends BrokerFilter {
        AccountingFilter(Broker next) { super(next); }

        @Override
        public void start() throws Exception {
            super.start();
            java.util.concurrent.ScheduledExecutorService f = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cresco-broker-discard-log");
                t.setDaemon(true);
                return t;
            });
            f.scheduleWithFixedDelay(() -> flush(false), logIntervalMs, logIntervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            flusher = f;
        }

        @Override
        public void stop() throws Exception {
            java.util.concurrent.ScheduledExecutorService f = flusher;
            flusher = null;
            if (f != null) f.shutdownNow();
            flush(true);
            super.stop();
        }

        @Override
        public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
            long total = DISCARDED.incrementAndGet();
            lastDiscardSub = sub;
            try {
                lastDiscardDest = messageReference.getMessage().getDestination().getPhysicalName();
            } catch (Throwable ignore) { }
            if (due(lastDiscardLogMs)) logDiscards(total);   // the first of a burst at once; the flusher logs the rest
            // not forwarded: the advisory flag only exists to route the event here
        }

        @Override
        public void slowConsumer(ConnectionContext context, Destination destination, Subscription subs) {
            long total = SLOW_CONSUMERS.incrementAndGet();
            lastSlowSub = subs;
            if (destination != null) lastSlowDest = destination.getName();
            if (due(lastSlowLogMs)) logSlow(total);
        }
    }
}
