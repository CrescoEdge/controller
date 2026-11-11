package io.cresco.agent.controller.communication;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.cresco.agent.controller.communication.BridgedFabric.send;
import static io.cresco.agent.controller.communication.BridgedFabric.stampOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * What becomes of a message that expires in a broker, on a real broker with the policy ActiveBroker sets
 * ({@link BridgedFabric}). Before: with no dead-letter strategy every PERSISTENT message that expired -- liveness and
 * control are persistent -- was moved to the queue ActiveMQ.DLQ with its expiry cleared; nothing consumes that queue, so
 * it grew until the store was full (5 of 5 expired persistent messages were there). And an expired message ended on no
 * node: nothing counted or logged it.
 */
class ExpiredMessagesTest {

    private final BridgedFabric f = new BridgedFabric();

    @BeforeAll
    static void pki() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) BridgedFabric.ca("t1", r);
    }

    @AfterEach
    void stop() {
        f.close();
    }

    @Test
    void anExpiredMessageIsDiscardedCountedAndSaidNotKeptInTheDeadLetterQueue() throws Exception {
        BrokerService broker = f.startAlone("rgA", null);
        ExpiredMessages expired = f.expired.get("rgA");
        Session a = f.node("rgA");
        for (int i = 0; i < 5; i++) send(a, a.createQueue("t1.rgA_zz"), "p" + i, stampOf("rgA"), DeliveryMode.PERSISTENT, 300);
        for (int i = 0; i < 5; i++) send(a, a.createQueue("t1.rgA_zz"), "n" + i, stampOf("rgA"), DeliveryMode.NON_PERSISTENT, 300);
        BridgedFabric.await(() -> expired.expiredCount() >= 10, 15_000, "the broker's expiries were not counted: " + expired.expiredCount() + " of 10");
        assertEquals(10, expired.expiredCount());
        assertEquals(0, expired.onArrivalCount(), "they expired waiting, not on arrival");
        assertEquals(0, f.depth("rgA", "t1.rgA_zz"));
        assertEquals(0, ActiveBroker.deadLetterDepth(broker), "expired persistent messages were kept in ActiveMQ.DLQ, which nothing consumes");
        List<String> said = f.warnings.get("rgA");
        assertEquals(1, said.size(), "one line, then at most one every 10 s: " + said);
        assertTrue(said.get(0).startsWith("MESSAGE EXPIRED: a message for 't1.rgA_zz' was not consumed within its life of 300 ms"), said.get(0));
        // a consumer that comes afterwards is given nothing
        MessageConsumer late = a.createConsumer(a.createQueue("t1.rgA_zz"));
        assertNull(late.receive(500));
    }

    @Test
    void whatAConsumerRefusesGoesToTheDeadLetterQueueWithAnExpiryAndIsInTheGauge() throws Exception {
        SharedDeadLetterStrategy s = ActiveBroker.deadLetters(5);
        assertFalse(s.isProcessExpired(), "an expired message is not a dead letter");
        assertFalse(s.isProcessNonPersistent());
        assertEquals(60_000L, s.getExpiration(), "what lands there expires, after a minute at least");
        assertEquals(3_600_000L, ActiveBroker.deadLetters(ActiveBroker.DLQ_EXPIRY_MS).getExpiration());

        BrokerService broker = f.startAlone("rgA", null);
        Session a = f.node("rgA");
        send(a, a.createQueue("t1.rgA_yy"), "poison", stampOf("rgA"), DeliveryMode.PERSISTENT, 0);
        // a consumer that gives the message back until the broker gives up on it (no redelivery allowed)
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://rgA_rc?create=false");
        cf.getRedeliveryPolicy().setMaximumRedeliveries(0);
        Connection c = cf.createConnection();
        f.connections.add(c);
        c.start();
        Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer refuser = tx.createConsumer(tx.createQueue("t1.rgA_yy"));
        Message m = refuser.receive(5000);
        assertNotNull(m);
        long before = System.currentTimeMillis();
        tx.rollback();
        BridgedFabric.await(() -> ActiveBroker.deadLetterDepth(broker) == 1, 10_000, "the refused message is not in the dead-letter queue (the gauge reads "
                + ActiveBroker.deadLetterDepth(broker) + ")");
        Destination dlq = broker.getDestination(new ActiveMQQueue(ActiveBroker.DLQ));
        org.apache.activemq.command.Message[] held = dlq.browse();
        assertEquals(1, held.length);
        long expiry = held[0].getExpiration();
        assertTrue(expiry >= before + 3_600_000L - 1000 && expiry <= System.currentTimeMillis() + 3_600_000L,
                "a dead letter has no expiry (" + expiry + "): the queue nobody reads grows for ever");
        assertEquals(0, f.expired.get("rgA").expiredCount());
        assertTrue(c instanceof ActiveMQConnection);
    }

    private static ActiveMQTextMessage message(String queue, long stamped, long expiry, long arrived) {
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setDestination(new ActiveMQQueue(queue));
        m.setTimestamp(stamped);
        m.setExpiration(expiry);
        m.setBrokerInTime(arrived);
        return m;
    }

    @Test
    void aMessageThatHadExpiredWhenItArrivedSaysSoAndByHowMuch() {
        // a sender whose clock is three minutes behind sends a ping of a 5 s timeout: it lives 65 s by the sender's clock and
        // has been dead for 115 s when it reaches a broker whose clock is right. Before, nothing said so.
        ExpiredMessages expired = new ExpiredMessages();
        List<String> said = new ArrayList<>();
        expired.setWarn(said::add);
        long now = 10_000_000L, senderNow = now - 180_000L;
        expired.expired(message("T.t1.r2_rc2", senderNow, senderNow + 65_000L, now), now);
        assertEquals(1, expired.expiredCount());
        assertEquals(1, expired.onArrivalCount());
        assertEquals(1, said.size());
        String line = said.get(0);
        assertTrue(line.startsWith("MESSAGE EXPIRED on arrival: a message for 'T.t1.r2_rc2' had expired 115000 ms before it reached this node's broker"), line);
        assertTrue(line.contains("its life: 65000 ms"), line);
        assertTrue(line.contains("the sender's clock is behind this node's by about 180 s, or the message waited that long on its way"), line);
        // one that waited here is told apart, and counted without a second line inside 10 s
        expired.expired(message("T.t1.r2_rc2", now, now + 65_000L, now), now + 66_000L);
        assertEquals(2, expired.expiredCount());
        assertEquals(1, expired.onArrivalCount());
        assertEquals(2, said.size());
        assertTrue(said.get(1).startsWith("MESSAGE EXPIRED: a message for 'T.t1.r2_rc2' was not consumed within its life of 65000 ms (it waited 66000 ms"), said.get(1));
        expired.expired(message("T.t1.r2_rc2", now, now + 65_000L, now), now + 67_000L);
        assertEquals(3, expired.expiredCount());
        assertEquals(2, said.size(), "logged at most every 10 s");
    }
}
