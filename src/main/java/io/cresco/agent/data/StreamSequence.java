package io.cresco.agent.data;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data loss on the dataplane is never silent. A slow topic subscriber does not lose messages (dataplane backpressure:
 * the topic's writers are slowed instead, see ActiveBroker); a loss that cannot be avoided -- a node or a link that dies
 * with messages on its way, a subscriber cut off, or dataplane_backpressure=false, where the broker discards the oldest
 * messages of a subscriber past its pending limit (counted at the broker, {@code DiscardAccounting}) -- is visible to
 * the subscriber that lost messages:
 *
 * <ul>
 *   <li>the writer's controller numbers every message of a stream -- one carrying {@code stream_name} -- per
 *       (writer, {@code stream_name}, {@code type}) in {@link #SEQ} (1, 2, 3, ...), so a subscriber selecting by stream
 *       and type sees consecutive numbers;</li>
 *   <li>the subscriber's controller checks them ({@link #detecting}) and, when numbers are missing, counts them in
 *       {@code cresco.dataplane.gap} and marks the first message after the gap with {@link #GAP} = how many are
 *       missing. A writer that restarts begins again at 1.</li>
 * </ul>
 * Messages without {@code stream_name} carry no number. See docs/caller-authorization.md section 8.
 */
public final class StreamSequence {

    /** JMS property: the message's number within its stream. */
    public static final String SEQ = "cresco_seq";
    /** JMS property on the first message after a gap: how many messages of the stream this subscriber did not get. */
    public static final String GAP = "cresco_gap";

    private static final int MAX_STREAMS = 10_000;

    private final Map<String, AtomicLong> next = lru();

    private static <V> Map<String, V> lru() {
        return new LinkedHashMap<String, V>(256, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, V> e) { return size() > MAX_STREAMS; }
        };
    }

    private static String key(Message m) throws JMSException {
        String stream = m.getStringProperty("stream_name");
        if (stream == null) return null;
        return m.getStringProperty(io.cresco.agent.controller.communication.CallerStamps.P_CALLER) + "|" + stream + "|"
                + m.getStringProperty("type");
    }

    /** Number {@code m} within its stream (after its writer was stamped); nothing for a message outside a stream. */
    public void number(Message m) throws JMSException {
        String k = key(m);
        if (k == null) return;
        AtomicLong n;
        synchronized (next) {
            n = next.computeIfAbsent(k, x -> new AtomicLong());
        }
        m.setLongProperty(SEQ, n.incrementAndGet());
    }

    /** Messages found missing so far by every detecting listener of this node. */
    public static final AtomicLong GAPS = new AtomicLong();

    /** {@code listener}, checking the stream numbers of what it receives (once: a detecting listener is returned as is). */
    public static MessageListener detecting(MessageListener listener) {
        if (listener == null || listener instanceof Detector) return listener;
        return new Detector(listener);
    }

    /** The listener {@code l} wraps (a gap-detecting one), or {@code l} itself. */
    static MessageListener unwrap(MessageListener l) {
        return (l instanceof Detector) ? ((Detector) l).delegate : l;
    }

    /** Per subscription: the last number seen of each stream. */
    static final class Detector implements MessageListener {
        private final MessageListener delegate;
        private final Map<String, Long> last = lru();

        Detector(MessageListener delegate) { this.delegate = delegate; }

        @Override
        public void onMessage(Message m) {
            try {
                if (m.propertyExists(SEQ)) {
                    String k = key(m);
                    long seq = m.getLongProperty(SEQ);
                    if (k != null) {
                        Long prev;
                        synchronized (last) {
                            prev = last.put(k, seq);
                        }
                        // the first message seen (a subscriber may join mid-stream) and a restarted writer (back to
                        // a lower number) start the count again; only numbers skipped after one seen are missing
                        long missing = (prev == null || seq <= prev) ? 0 : seq - prev - 1;
                        if (missing > 0) {
                            GAPS.addAndGet(missing);
                            markGap(m, missing);
                        }
                    }
                }
            } catch (Exception ignore) {
                // numbering is best effort: the message is delivered whatever its properties say
            }
            delegate.onMessage(m);
        }

        @Override public String toString() { return "gap-detecting " + delegate; }

        /**
         * Write {@link #GAP} onto a RECEIVED message without touching its body. Not through the JMS setters: on an
         * ActiveMQBytesMessage (every binary dataplane frame) setObjectProperty first moves the body into the message's
         * write buffer, so its reader then saw an empty body -- the first frame after every gap reached the subscriber
         * with no payload (wsapi sent a 0-byte frame). The property map of the message is set directly instead.
         */
        static void markGap(Message m, long missing) throws JMSException {
            if (m instanceof org.apache.activemq.command.ActiveMQMessage) {
                try {
                    ((org.apache.activemq.command.ActiveMQMessage) m).setProperty(GAP, missing);
                    return;
                } catch (java.io.IOException e) {
                    throw new JMSException("cannot mark the gap: " + e.getMessage());
                }
            }
            m.setLongProperty(GAP, missing);
        }
    }
}
