package io.cresco.agent.controller.communication;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Region re-attach after a global restart: {@code ControllerEngine.isReachableAgent} (via
 * {@link ActiveBroker#hasLiveQueueRoute}) must report the global reachable only while a bridge actually
 * routes to it.
 *
 * <p>Before the fix it reported "reachable" whenever a queue named after the global EXISTED on the region's
 * broker. That destination outlives the duplex bridge that created it (and the region's own PINGs to the
 * global re-create it), so after the global restarted the region's re-join ({@code isRegionGlobal}) found the
 * dead route "reachable", never submitted the global as a broker candidate, never rebuilt the bridge, and the
 * global aged the region to LOST. Proven here on two real embedded brokers joined by the same kind of duplex
 * network bridge the region builds to its global.
 */
class LiveQueueRouteTest {

    private static final String GLOBAL_INBOX = "global-region_global-controller";

    private BrokerService global;
    private BrokerService region;
    private Connection globalConsumerConn;
    private Connection regionConn;

    private static BrokerService broker(String name, boolean listen) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        if (listen) {
            b.addConnector("tcp://127.0.0.1:0");
        }
        b.start();
        b.waitUntilStarted();
        return b;
    }

    private static String tcp(BrokerService b) throws Exception {
        TransportConnector c = b.getTransportConnectors().get(0);
        return c.getPublishableConnectString();
    }

    /** The global's own inbox consumer (the AgentConsumer on its control queue). */
    private Connection consumeInbox(BrokerService b) throws Exception {
        Connection c = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false").createConnection();
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = s.createConsumer(new ActiveMQQueue(GLOBAL_INBOX));
        assertNotNull(consumer);
        return c;
    }

    /** The region's duplex bridge to the global (ActiveBroker.AddNetworkConnector builds the same kind). */
    private NetworkConnector bridge(String globalTcp) throws Exception {
        NetworkConnector nc = region.addNetworkConnector("static:(" + globalTcp + ")?maxReconnectDelay=500");
        nc.setName("bridge-" + System.nanoTime());
        nc.setDuplex(true);
        nc.start();
        return nc;
    }

    private static boolean await(BooleanSupplier cond, long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(50);
        }
        return cond.getAsBoolean();
    }

    private boolean destinationExists() {
        try {
            return region.getRegionBroker().getDestinationMap().containsKey(new ActiveMQQueue(GLOBAL_INBOX));
        } catch (Exception e) {
            return false;
        }
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : new Connection[]{globalConsumerConn, regionConn}) {
            try { if (c != null) c.close(); } catch (Exception ignored) { }
        }
        for (BrokerService b : new BrokerService[]{region, global}) {
            if (b != null) {
                b.stop();
                b.waitUntilStopped();
            }
        }
    }

    @Test
    void dead_bridge_destination_is_not_a_route_and_a_rebuilt_bridge_is() throws Exception {
        region = broker("region-broker", false);
        assertFalse(ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX), "no bridge yet");

        global = broker("global-broker", true);
        globalConsumerConn = consumeInbox(global);
        NetworkConnector first = bridge(tcp(global));
        assertTrue(await(() -> ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX), 15_000),
                "bridge up: the global's inbox demand reaches the region broker");

        // the region keeps PINGing its global's inbox (RegionHealthWatcher) through its own broker
        regionConn = new ActiveMQConnectionFactory("vm://region-broker?create=false").createConnection();
        regionConn.start();
        Session rs = regionConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer ping = rs.createProducer(new ActiveMQQueue(GLOBAL_INBOX));

        // global restart: its broker goes away; the region's bridge loses its remote end
        globalConsumerConn.close();
        globalConsumerConn = null;
        global.stop();
        global.waitUntilStopped();
        ping.send(rs.createTextMessage("PING"));

        assertTrue(await(() -> !ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX), 15_000),
                "a dead bridge is not a route");
        assertTrue(destinationExists(),
                "the destination outlives the bridge -- what the old existence check took for 'reachable'");

        // the region rebuilds a bridge to the restarted global (new instance, here on a new port)
        region.removeNetworkConnector(first);
        first.stop();
        global = broker("global-broker", true);
        globalConsumerConn = consumeInbox(global);
        bridge(tcp(global));
        assertTrue(await(() -> ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX), 15_000),
                "rebuilt bridge: reachable again");
    }

    @Test
    void local_inbox_with_a_consumer_is_a_route_without_any_bridge() throws Exception {
        region = broker("region-broker", false);
        assertFalse(ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX));
        assertFalse(ActiveBroker.hasLiveQueueRoute(region, null));
        assertFalse(ActiveBroker.hasLiveQueueRoute(null, GLOBAL_INBOX));
        globalConsumerConn = consumeInbox(region);
        assertTrue(await(() -> ActiveBroker.hasLiveQueueRoute(region, GLOBAL_INBOX), 5_000));
        assertFalse(ActiveBroker.hasLiveQueueRoute(region, "some-region_some-agent"));
    }

    @Test
    void a_topic_route_to_the_global_needs_the_bridges_network_subscription() throws Exception {
        String topic = "agent.event";
        region = broker("region-broker", false);
        // the region's own listener on the topic (registerRegionalListener) is local: not a route to the global
        regionConn = new ActiveMQConnectionFactory("vm://region-broker?create=false").createConnection();
        regionConn.start();
        regionConn.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new org.apache.activemq.command.ActiveMQTopic(topic));
        assertFalse(ActiveBroker.hasNetworkSubscription(region, topic));

        // the global's state-update listener, reached over the region's duplex bridge
        global = broker("global-broker", true);
        globalConsumerConn = new ActiveMQConnectionFactory("vm://global-broker?create=false").createConnection();
        globalConsumerConn.start();
        globalConsumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createConsumer(new org.apache.activemq.command.ActiveMQTopic(topic), "update_mode = 'REGION'");
        bridge(tcp(global));
        assertTrue(await(() -> ActiveBroker.hasNetworkSubscription(region, topic), 15_000),
                "bridge demand for the global's listener");
        assertFalse(ActiveBroker.hasNetworkSubscription(region, "other.topic"));

        globalConsumerConn.close();
        globalConsumerConn = null;
        global.stop();
        global.waitUntilStopped();
        assertTrue(await(() -> !ActiveBroker.hasNetworkSubscription(region, topic), 15_000), "gone with the bridge");
    }
}
