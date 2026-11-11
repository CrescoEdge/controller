package io.cresco.agent.controller.communication;

import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * The messages this node's broker let expire: counted, and said.
 *
 * <p>A message that expires in a broker ended on no node: nothing counted it and nothing logged it. That was rare while
 * every message lived five minutes. An RPC request now lives as long as its caller waits plus the clock margin
 * ({@link RpcExpiry}), and its expiry is a time of day by its SENDER's clock: a sender whose clock is behind this node's
 * by more than that life has every such request expire the moment it arrives, and without this class nobody would know.
 *
 * <p>The broker tells a plugin of every message it expires. Each is counted ({@code msg.expired}; those that had already
 * expired when they arrived also as {@code msg.expired.on_arrival}) and reported at WARN at most every
 * {@value #LINE_EVERY_MS} ms: the destination, how long the message was to live, and either how long it waited here or --
 * expired on arrival -- by how much, which is how far the sender's clock is behind or how long the message was on its
 * way. A consumer whose client library discards a message as expired by ITS clock tells the broker so; that is counted as
 * {@code msg.expired.by_consumer} and reported the same way (the consumer's clock is ahead, or the message waited in its
 * prefetch).
 *
 * <p>One per controller: the counts outlive a broker the node makes anew. A count is a {@link LongAdder} increment.
 */
public final class ExpiredMessages {

    static final long LINE_EVERY_MS = 10_000L;

    private final LongAdder expired = new LongAdder(), onArrival = new LongAdder(), byConsumer = new LongAdder();
    private volatile long lastLine, lastConsumerLine;
    private volatile Consumer<String> warn = line -> { };

    /** Where the lines go (the controller's logger, at WARN). */
    public void setWarn(Consumer<String> warn) {
        this.warn = warn == null ? line -> { } : warn;
    }

    /** Messages this node's broker expired, so far. */
    public long expiredCount() { return expired.sum(); }

    /** Of those, the messages that had already expired when they reached this broker. */
    public long onArrivalCount() { return onArrival.sum(); }

    /** Messages a consumer of this broker discarded as expired by its own clock, so far. */
    public long byConsumerCount() { return byConsumer.sum(); }

    /** The plugin through which the broker reports its expiries. */
    public BrokerPlugin plugin() {
        return next -> new BrokerFilter(next) {
            @Override
            public void messageExpired(ConnectionContext context, MessageReference reference, Subscription subscription) {
                try {
                    Message m = reference == null ? null : reference.getMessage();
                    if (m != null) expired(m, System.currentTimeMillis());
                } catch (Throwable ignore) {
                    // an account of the loss must never add to it
                }
                super.messageExpired(context, reference, subscription);
            }

            @Override
            public void acknowledge(ConsumerBrokerExchange exchange, MessageAck ack) throws Exception {
                if (ack != null && ack.isExpiredAck()) {
                    try {
                        discardedByConsumer(ack, System.currentTimeMillis());
                    } catch (Throwable ignore) { }
                }
                super.acknowledge(exchange, ack);
            }
        };
    }

    /** The broker expired {@code m} at {@code now}. */
    void expired(Message m, long now) {
        expired.increment();
        long expiry = m.getExpiration(), in = m.getBrokerInTime(), stamped = m.getTimestamp();
        boolean arrivedExpired = expiry > 0 && in > 0 && expiry <= in;
        if (arrivedExpired) onArrival.increment();
        if (now - lastLine <= LINE_EVERY_MS) return;
        lastLine = now;
        String to = m.getDestination() == null ? "?" : m.getDestination().getPhysicalName();
        String life = stamped > 0 && expiry > stamped ? (expiry - stamped) + " ms" : "unknown";
        if (arrivedExpired) {
            long late = in - expiry, age = stamped > 0 ? in - stamped : -1;
            warn.accept("MESSAGE EXPIRED on arrival: a message for '" + to + "' had expired " + late + " ms before it reached this node's"
                    + " broker (its life: " + life + (age >= 0 ? "; stamped " + age + " ms before it arrived, by this node's clock" : "")
                    + "): the sender's clock is behind this node's by about " + Math.round((age >= 0 ? age : late) / 1000.0)
                    + " s, or the message waited that long on its way (" + expired.sum() + " expired so far, " + onArrival.sum()
                    + " of them on arrival; logged at most every 10 s)");
        } else {
            warn.accept("MESSAGE EXPIRED: a message for '" + to + "' was not consumed within its life of " + life
                    + (in > 0 ? " (it waited " + (now - in) + " ms at this node's broker)" : "") + " (" + expired.sum() + " expired so far, "
                    + onArrival.sum() + " of them on arrival; logged at most every 10 s)");
        }
    }

    private void discardedByConsumer(MessageAck ack, long now) {
        byConsumer.add(Math.max(1, ack.getMessageCount()));
        if (now - lastConsumerLine <= LINE_EVERY_MS) return;
        lastConsumerLine = now;
        String to = ack.getDestination() == null ? "?" : ack.getDestination().getPhysicalName();
        warn.accept("MESSAGE EXPIRED at its consumer: a consumer of '" + to + "' discarded a message as expired by its own clock (its clock"
                + " is ahead of the sender's by more than the message's life, or the message waited in the consumer's buffer; "
                + byConsumer.sum() + " so far; logged at most every 10 s)");
    }
}
