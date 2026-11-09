package io.cresco.agent.controller.communication;

import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Dataplane backpressure at the broker, with Cresco's topic policy (producer flow control, a memory limit per topic) and
 * its slow-subscriber choice: a subscriber that cannot keep up slows the writer and loses nothing, in order; the old
 * discarding limit lost frames; and two bridged brokers full of traffic for each other do not wait on each other forever.
 */
class DataplaneBackpressureBrokerTest {

    static final int FRAME = 64 * 1024;
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<ActiveMQConnection> connections = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (ActiveMQConnection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) { b.stop(); b.waitUntilStopped(); }
    }

    /** A broker with Cresco's dataplane topic policy (ActiveBroker): flow control, {@code memoryLimit} per topic. */
    private BrokerService broker(String name, boolean backpressure, long memoryLimit, boolean bridgeInbound) throws Exception {
        PolicyEntry topics = new PolicyEntry();
        topics.setTopic(">");
        topics.setProducerFlowControl(true);
        topics.setMemoryLimit(memoryLimit);
        topics.setTopicPrefetch(10);
        topics.setAdvisoryForDiscardingMessages(true);
        ActiveBroker.slowSubscriberPolicy(topics, backpressure, 2.5);
        PolicyMap map = new PolicyMap();
        map.put(new ActiveMQTopic(">"), topics);
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setDestinationPolicy(map);
        b.setTmpDataDirectory(java.nio.file.Files.createTempDirectory("bp-" + name).toFile());
        if (bridgeInbound) b.setPlugins(new BrokerPlugin[]{new DataplaneBridgeInbound(null, 1000)});
        brokers.add(b);
        return b;
    }

    private ActiveMQConnection connect(String url) throws Exception {
        ActiveMQConnection c = (ActiveMQConnection) new ActiveMQConnectionFactory(url).createConnection();
        c.start();
        connections.add(c);
        return c;
    }

    /** A subscriber that takes {@code napMs} per message and records the numbers it got. */
    static final class Slow {
        final List<Integer> got = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch all;
        Slow(ActiveMQConnection c, String selector, int expected, long napMs) throws Exception {
            all = new CountDownLatch(expected);
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createTopic("global.event"), selector);
            mc.setMessageListener(m -> {
                try {
                    if (napMs > 0) Thread.sleep(napMs);
                    got.add(m.getIntProperty("n"));
                } catch (Exception ignore) { }
                all.countDown();
            });
        }
    }

    /** Send {@code n} frames tagged {@code dir} as fast as the broker takes them; returns the milliseconds it took. */
    static long send(ActiveMQConnection c, String dir, int n) throws Exception {
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = s.createProducer(s.createTopic("global.event"));
        p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[FRAME]);
            m.setIntProperty("n", i);
            m.setStringProperty("dir", dir);
            p.send(m);
        }
        return (System.nanoTime() - t0) / 1_000_000;
    }

    static List<Integer> upTo(int n) {
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(i);
        return l;
    }

    @Test
    void aSlowSubscriberSlowsTheWriterAndLosesNothing() throws Exception {
        BrokerService b = broker("bp1", true, 1024 * 1024, false);
        b.start();
        b.waitUntilStarted();
        // the subscriber on a connection of its own; the writer on a producer-only lane, in the sending thread (async=false)
        Slow slow = new Slow(connect("vm://bp1?create=false"), "dir = 'x'", 300, 2);
        long ms = send(connect("vm://bp1?create=false&async=false"), "x", 300);
        assertTrue(slow.all.await(60, TimeUnit.SECONDS), "every frame arrives: " + slow.got.size());
        assertEquals(upTo(300), slow.got, "in order");
        // 300 x 2 ms at the subscriber; 1 MiB of topic memory holds 16 frames, so the writer waited for most of it
        assertTrue(ms >= 300, "the writer was slowed to its subscriber (" + ms + " ms)");
    }

    @Test
    void theOldDiscardingLimitLostFramesOfASlowSubscriber() throws Exception {
        BrokerService b = broker("bp2", false, 64L * 1024 * 1024, false);
        b.start();
        b.waitUntilStarted();
        Slow slow = new Slow(connect("vm://bp2?create=false"), "dir = 'x'", 300, 2);
        long ms = send(connect("vm://bp2?create=false&async=false"), "x", 300);
        slow.all.await(5, TimeUnit.SECONDS);
        Thread.sleep(1000);
        assertTrue(slow.got.size() < 300, "prefetch 10 x 2.5 pending: the rest was discarded (" + slow.got.size() + " of 300)");
        assertTrue(ms < 300, "and the writer never waited (" + ms + " ms)");
    }

    /** Brokers a and z (Cresco's topic policy, bridged inbound waiting in line) and a duplex bridge a -> z like Cresco's. */
    private BrokerService[] bridged(String a, String z) throws Exception {
        BrokerService ba = broker(a, true, 512 * 1024, true);
        BrokerService bz = broker(z, true, 512 * 1024, true);
        TransportConnector zc = bz.addConnector("tcp://127.0.0.1:0");
        bz.start();
        bz.waitUntilStarted();
        NetworkConnector nc = ba.addNetworkConnector("static:(" + zc.getConnectUri() + ")");
        nc.setDuplex(true);
        nc.setAlwaysSyncSend(true);                                   // as Cresco's data connectors (ActiveBroker)
        nc.setConduitSubscriptions(false);
        nc.setPrefetchSize(64);
        ba.start();
        ba.waitUntilStarted();
        return new BrokerService[]{ba, bz};
    }

    /** Wait until each broker's dataplane topic has {@code n} consumers (its own and the bridge's demand). */
    private static void demand(int n, BrokerService... bs) throws Exception {
        long until = System.currentTimeMillis() + 20000;
        for (BrokerService b : bs) {
            while (System.currentTimeMillis() < until && (b.getDestination(new ActiveMQTopic("global.event")) == null
                    || b.getDestination(new ActiveMQTopic("global.event")).getConsumers().size() < n)) Thread.sleep(100);
        }
    }

    @Test
    void aSlowSubscriberBehindABridgeSlowsTheWriterOnTheOtherBroker() throws Exception {
        BrokerService[] b = bridged("bpb", "bpy");
        Slow atY = new Slow(connect("vm://bpy?create=false"), "dir = 'by'", 300, 2);
        demand(1, b[0]);
        long ms = send(connect("vm://bpb?create=false&async=false"), "by", 300);
        assertTrue(atY.all.await(60, TimeUnit.SECONDS), "every frame crossed the bridge: " + atY.got.size());
        assertEquals(upTo(300), atY.got, "in order");
        assertTrue(ms >= 300, "the writer on the other broker was slowed to the subscriber (" + ms + " ms)");
        assertEquals(0, DataplaneBridgeInbound.OVERFLOWS.get() - overflowsAtStart,
                "a slow subscriber never overflows the bridge, though its prefetch (10 x 64 KiB) alone exceeds the topic's 512 KiB");
    }

    private final long overflowsAtStart = DataplaneBridgeInbound.OVERFLOWS.get();

    @Test
    void twoBridgedBrokersFullOfEachOthersTrafficDoNotWaitOnEachOtherForever() throws Exception {
        BrokerService[] b = bridged("bpa", "bpz");
        // slow subscribers on both sides for the other side's stream, so each topic fills with traffic for the other
        Slow atZ = new Slow(connect("vm://bpz?create=false"), "dir = 'az'", 200, 3);
        Slow atA = new Slow(connect("vm://bpa?create=false"), "dir = 'za'", 200, 3);
        demand(2, b[0], b[1]);
        AtomicLong failures = new AtomicLong();
        Thread toZ = new Thread(() -> { try { send(connect("vm://bpa?create=false&async=false"), "az", 200); } catch (Exception e) { failures.incrementAndGet(); } });
        Thread toA = new Thread(() -> { try { send(connect("vm://bpz?create=false&async=false"), "za", 200); } catch (Exception e) { failures.incrementAndGet(); } });
        toZ.start();
        toA.start();
        assertTrue(atZ.all.await(90, TimeUnit.SECONDS), "a -> z delivered " + atZ.got.size() + " of 200");
        assertTrue(atA.all.await(90, TimeUnit.SECONDS), "z -> a delivered " + atA.got.size() + " of 200");
        toZ.join(10000);
        toA.join(10000);
        assertEquals(0, failures.get());
        assertEquals(upTo(200), atZ.got, "in order");
        assertEquals(upTo(200), atA.got, "in order");
    }

    @Test
    void onlyBridgedTopicMessagesPassTheReceivingTopicsFlowControl() {
        org.apache.activemq.broker.ConnectionContext bridge = new org.apache.activemq.broker.ConnectionContext();
        bridge.setNetworkConnection(true);
        org.apache.activemq.broker.ConnectionContext client = new org.apache.activemq.broker.ConnectionContext();
        org.apache.activemq.command.ActiveMQBytesMessage topicMsg = new org.apache.activemq.command.ActiveMQBytesMessage();
        topicMsg.setDestination(new ActiveMQTopic("global.event"));
        org.apache.activemq.command.ActiveMQBytesMessage queueMsg = new org.apache.activemq.command.ActiveMQBytesMessage();
        queueMsg.setDestination(new org.apache.activemq.command.ActiveMQQueue("T.default.r_a"));
        org.apache.activemq.command.ActiveMQMessage advisory = new org.apache.activemq.command.ActiveMQMessage();
        advisory.setDestination(new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.global.event"));
        assertTrue(DataplaneBridgeInbound.bridgedTopicMessage(bridge, topicMsg));
        assertFalse(DataplaneBridgeInbound.bridgedTopicMessage(client, topicMsg), "a client's send is held: that is the backpressure");
        assertFalse(DataplaneBridgeInbound.bridgedTopicMessage(bridge, queueMsg), "queues are the control plane");
        assertFalse(DataplaneBridgeInbound.bridgedTopicMessage(bridge, advisory));
    }
}
