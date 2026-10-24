package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * gfs#26: an agent moved from the global's region into its site's region keeps answering at its former address
 * (region_alias). Unit rules, then the routing on three real embedded brokers (the global and two regions, each
 * region bridged to the global and to the other region, as ActiveBroker builds them): a message for the former
 * address reaches the moved agent from its own region without leaving it, from another region, and from the global's
 * broker; and with the global's broker gone, from its own region and the other region over their direct bridge.
 */
class RegionAliasTest {

    @Test
    void anAliasIsAnotherRegionNeverTheGlobalsOrItsOwn() {
        assertEquals("gaiakeep", RegionAlias.of("gaiakeep", "sitea"));
        assertEquals("gaiakeep", RegionAlias.of(" gaiakeep ", "sitea"));
        assertNull(RegionAlias.of(null, "sitea"));
        assertNull(RegionAlias.of("", "sitea"));
        assertNull(RegionAlias.of("sitea", "sitea"), "its own region is no alias");
        assertNull(RegionAlias.of("a_b", "sitea"), "an alias cannot hold the queue separator");
        assertNull(RegionAlias.of("T.x", "sitea"), "nor a tenant-namespace dot");
    }

    @Test
    void theAliasQueueIsTheInboxWithTheFormerRegion() {
        assertEquals("gaiakeep_sitea-core", RegionAlias.aliasQueue("sitea_sitea-core", "gaiakeep", "sitea", "sitea-core"));
        // tenant-qualified (tenant_namespacing) and wildcard inboxes keep their prefix
        assertEquals("T.t1.gaiakeep_sitea-core", RegionAlias.aliasQueue("T.t1.sitea_sitea-core", "gaiakeep", "sitea", "sitea-core"));
        assertEquals("T.*.gaiakeep_sitea-core", RegionAlias.aliasQueue("T.*.sitea_sitea-core", "gaiakeep", "sitea", "sitea-core"));
        assertNull(RegionAlias.aliasQueue("other_x", "gaiakeep", "sitea", "sitea-core"));
        assertNull(RegionAlias.aliasQueue("sitea_sitea-core", null, "sitea", "sitea-core"));
    }

    @Test
    void aMessageForTheFormerAddressIsForThisNodeAndKeepsItsPlugin() {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "siteb", "siteb-core", "gk-core-siteb", "gaiakeep", "sitea-core", "gk-core-sitea", true, true);
        assertTrue(RegionAlias.normalize(m, "gaiakeep", "sitea", "sitea-core"));
        assertEquals("sitea", m.getDstRegion());
        assertEquals("sitea-core", m.getDstAgent());
        assertEquals("gk-core-sitea", m.getDstPlugin());
        assertEquals("siteb", m.getSrcRegion(), "the sender is untouched: the reply goes back to where it runs");

        MsgEvent other = new MsgEvent(MsgEvent.Type.EXEC, "siteb", "siteb-core", null, "gaiakeep", "sitec-core", null, true, true);
        assertFalse(RegionAlias.normalize(other, "gaiakeep", "sitea", "sitea-core"), "another agent's former address");
        assertEquals("gaiakeep", other.getDstRegion());
        MsgEvent own = new MsgEvent(MsgEvent.Type.EXEC, "siteb", "siteb-core", null, "sitea", "sitea-core", null, true, true);
        assertFalse(RegionAlias.normalize(own, "gaiakeep", "sitea", "sitea-core"));
        assertFalse(RegionAlias.normalize(other, null, "sitea", "sitea-core"), "no alias, nothing changes");
    }

    // ------------------------------------------------------------------ three brokers
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> conns = new ArrayList<>();

    private BrokerService broker(String name) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.addConnector("tcp://127.0.0.1:0");
        b.start();
        b.waitUntilStarted();
        brokers.add(b);
        return b;
    }

    /** A duplex bridge from {@code from} to {@code to}, as ActiveBroker.buildConnector makes it. */
    private static void bridge(BrokerService from, BrokerService to) throws Exception {
        NetworkConnector nc = from.addNetworkConnector("static:(" + to.getTransportConnectors().get(0).getPublishableConnectString()
                + ")?maxReconnectDelay=500");
        nc.setName(from.getBrokerName() + "-" + to.getBrokerName());
        nc.setDuplex(true);
        nc.setNetworkTTL(5);
        nc.setDecreaseNetworkConsumerPriority(true);
        nc.setConduitSubscriptions(false);
        nc.start();
    }

    private Session session(BrokerService b) throws Exception {
        Connection c = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false").createConnection();
        c.start();
        conns.add(c);
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static String send(Session s, String queue, String body) throws Exception {
        MessageProducer p = s.createProducer(new ActiveMQQueue(queue));
        p.send(s.createTextMessage(body));
        p.close();
        return body;
    }

    private static String receive(MessageConsumer c, long ms) throws Exception {
        Message m = c.receive(ms);
        return m == null ? null : ((TextMessage) m).getText();
    }

    @AfterEach
    void stop() {
        for (Connection c : conns) {
            try { c.close(); } catch (Exception ignored) { }
        }
        for (BrokerService b : brokers) {
            try { b.stop(); b.waitUntilStopped(); } catch (Exception ignored) { }
        }
    }

    @Test
    void theFormerAddressIsReachedFromEveryRegionAndWithoutTheGlobal() throws Exception {
        BrokerService global = broker("g-broker"), sitea = broker("a-broker"), siteb = broker("b-broker");
        bridge(sitea, global);
        bridge(siteb, global);
        bridge(sitea, siteb);           // regional_peers / the inferred region-to-region bridge

        // the moved agent sitea-core: its inbox and its former address's inbox, both on its regional broker
        String alias = RegionAlias.aliasQueue("sitea_sitea-core", "gaiakeep", "sitea", "sitea-core");
        Session agent = session(sitea);
        MessageConsumer inbox = agent.createConsumer(new ActiveMQQueue(alias));
        Thread.sleep(2000);             // demand reaches the other brokers over the bridges

        Session local = session(sitea), other = session(siteb), atGlobal = session(global);
        assertEquals(send(local, alias, "from sitea-store"), receive(inbox, 10_000), "inside the region");
        assertEquals(send(other, alias, "from siteb-core"), receive(inbox, 10_000), "from another region");
        assertEquals(send(atGlobal, alias, "from the gateway"), receive(inbox, 10_000), "from the global's broker (wsapi)");

        // the global restarts: its broker is gone; the region and the region-to-region bridge carry on
        global.stop();
        global.waitUntilStopped();
        assertEquals(send(local, alias, "global down, inside"), receive(inbox, 10_000));
        assertEquals(send(other, alias, "global down, siteb"), receive(inbox, 10_000));
    }
}
