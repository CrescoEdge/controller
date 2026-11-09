package io.cresco.agent.data;

import jakarta.jms.Message;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Dataplane loss is never silent: a subscriber that lost stream messages sees how many (StreamSequence). */
class StreamSequenceTest {

    private static ActiveMQTextMessage line(String writer, String stream, String type) throws Exception {
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setStringProperty("cresco_caller", writer);
        m.setStringProperty("stream_name", stream);
        m.setStringProperty("type", type);
        return m;
    }

    @Test
    void aSubscriberThatLosesMessagesSeesTheGap() throws Exception {
        StreamSequence seq = new StreamSequence();
        List<Message> sent = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ActiveMQTextMessage m = line("plugin:t1/r/a/p", "s1", "output");
            seq.number(m);
            sent.add(m);
        }
        assertEquals(1, sent.get(0).getLongProperty(StreamSequence.SEQ));
        assertEquals(10, sent.get(9).getLongProperty(StreamSequence.SEQ));
        List<Message> got = new ArrayList<>();
        jakarta.jms.MessageListener l = StreamSequence.detecting(got::add);
        assertSame(l, StreamSequence.detecting(l), "a listener is wrapped once");
        long before = StreamSequence.GAPS.get();
        // the broker discarded messages 4..7 for this slow subscriber
        for (int i : new int[]{0, 1, 2, 7, 8, 9}) l.onMessage(sent.get(i));
        assertEquals(6, got.size(), "every message that arrived is delivered");
        assertEquals(4, got.get(3).getLongProperty(StreamSequence.GAP), "the first message after the gap says how many are missing");
        assertFalse(got.get(4).propertyExists(StreamSequence.GAP));
        assertEquals(before + 4, StreamSequence.GAPS.get());
    }

    @Test
    void streamsAreNumberedPerWriterStreamAndTypeAndALateJoinerSeesNoGap() throws Exception {
        StreamSequence seq = new StreamSequence();
        ActiveMQTextMessage out1 = line("w", "s1", "output"), err1 = line("w", "s1", "error"), other = line("v", "s1", "output");
        seq.number(out1);
        seq.number(err1);
        seq.number(other);
        assertEquals(1, err1.getLongProperty(StreamSequence.SEQ), "stdout and stderr are numbered apart");
        assertEquals(1, other.getLongProperty(StreamSequence.SEQ), "each writer numbers its own");
        ActiveMQTextMessage plain = new ActiveMQTextMessage();
        seq.number(plain);
        assertFalse(plain.propertyExists(StreamSequence.SEQ), "a message outside a stream carries no number");

        List<Message> got = new ArrayList<>();
        jakarta.jms.MessageListener l = StreamSequence.detecting(got::add);
        long before = StreamSequence.GAPS.get();
        ActiveMQTextMessage late = line("w", "s9", "output");
        late.setLongProperty(StreamSequence.SEQ, 500);
        l.onMessage(late);
        assertFalse(got.get(0).propertyExists(StreamSequence.GAP), "joining mid-stream is no loss");
        ActiveMQTextMessage restarted = line("w", "s9", "output");
        restarted.setLongProperty(StreamSequence.SEQ, 1);
        l.onMessage(restarted);
        assertFalse(got.get(1).propertyExists(StreamSequence.GAP), "a restarted writer starts again");
        assertEquals(before, StreamSequence.GAPS.get());
    }

    /**
     * The first binary frame after a gap reached the subscriber EMPTY: the gap was written with the JMS setter, which on an
     * ActiveMQBytesMessage moves the body into the message's write buffer first, so its reader saw zero bytes (wsapi sent
     * a 0-byte frame). The mark must leave the body as it came.
     */
    @Test
    void theGapMarkLeavesABinaryFramesBodyIntact() throws Exception {
        StreamSequence seq = new StreamSequence();
        List<ActiveMQBytesMessage> sent = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ActiveMQBytesMessage m = new ActiveMQBytesMessage();
            m.setStringProperty("cresco_caller", "api:t/bob");
            m.setStringProperty("stream_name", "u:bob:s");
            seq.number(m);
            m.writeBytes(new byte[]{(byte) i, 1, 2, 3, 4, 5, 6, 7});
            sent.add(m);
        }
        List<Message> got = new ArrayList<>();
        jakarta.jms.MessageListener l = StreamSequence.detecting(got::add);
        for (int i : new int[]{0, 3}) {                       // 1 and 2 lost on the way
            ActiveMQBytesMessage received = (ActiveMQBytesMessage) sent.get(i).copy();
            received.setReadOnlyBody(true);
            received.setReadOnlyProperties(true);             // as a consumer hands it to its listener
            l.onMessage(received);
        }
        ActiveMQBytesMessage after = (ActiveMQBytesMessage) got.get(1);
        assertEquals(2, after.getLongProperty(StreamSequence.GAP));
        assertEquals(8, after.getBodyLength(), "the body survived the gap mark");
        byte[] body = new byte[8];
        assertEquals(8, after.readBytes(body));
        assertEquals(3, body[0]);
        assertThrows(jakarta.jms.MessageNotWriteableException.class, () -> after.setStringProperty("x", "y"),
                "its properties stay read-only for the listener");
    }
}
