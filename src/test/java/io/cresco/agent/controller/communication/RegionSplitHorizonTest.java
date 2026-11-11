package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region's broker is no relay between bridges (split horizon), over real duplex bridges between secured brokers.
 *
 * <p>Regions bridged to each other as well as to the global (a mesh) had every topic message arrive once over each path
 * of bridges, and a queue message take the way over another region whenever its own bridge was away. The broker at the
 * end strips the caller from what a region's bridge brings for another region's node, so those copies arrived without a
 * caller (fabric matrix, cell mesh: 150 to 300 stripped stamps a run; after a restart of the global the regions' watchdogs
 * went over another region and were refused as unsigned). Each test fails on a broker without the rule.
 *
 * <p>The fabric: a global rgG/gc and the regions rgA, rgB, rgC (controller rc, one node each), every broker with the
 * Cresco authorization, the regions with rgG/gc as their parent.
 */
class RegionSplitHorizonTest {

    private static final String[] REGIONS = {"rgG", "rgA", "rgB", "rgC"};
    private static final Map<String, RegionCA> cas = new HashMap<>();
    private static final Map<String, TlsTestSupport.Id> controllers = new HashMap<>(), nodes = new HashMap<>();
    private static X509Certificate[] anchors;

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private final Map<String, CrescoAuthorizationBroker> authz = new HashMap<>();
    private final Map<String, BrokerService> byRegion = new HashMap<>();

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        List<X509Certificate> roots = new ArrayList<>();
        for (String r : REGIONS) {
            RegionCA ca = RegionCA.generate("t1", r, 2048, 1);
            cas.put(r, ca);
            roots.add(ca.caCert());
            controllers.put(r, TlsTestSupport.issued(ca, "t1", r, controllerOf(r)));
            nodes.put(r, TlsTestSupport.issued(ca, "t1", r, "n1"));
        }
        anchors = roots.toArray(new X509Certificate[0]);
    }

    private static String controllerOf(String region) { return "rgG".equals(region) ? "gc" : "rc"; }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) try { brokers.get(i).stop(); } catch (Exception ignore) { }
    }

    /** The secured broker of {@code region}'s controller; {@code relay}: broker_bridge_transit (null: by its role). */
    private BrokerService start(String region, Boolean relay) throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        for (String r : REGIONS) {
            assertNull(peers.trustCa(cas.get(r).caCert(), r, "t1", "test"));
            if (!r.equals(region)) peers.allowBridgePeer(r, controllerOf(r));
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        if ("rgG".equals(region)) cfg.put("is_global", "true");
        if (relay != null) cfg.put(CrescoAuthorizationBroker.TRANSIT_KEY, relay.toString());
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("horizon").toString(), cfg);
        CrescoAuthorizationBroker a = new CrescoAuthorizationBroker(pb, peers);
        if (!"rgG".equals(region)) a.setParent(() -> new PeerIdentity.Expected("rgG", "gc"));
        BrokerService b = TlsTestSupport.broker(region + "_" + controllerOf(region), controllers.get(region), true, new BrokerPlugin[]{a}, a, anchors);
        brokers.add(b);
        // as ActiveBroker: a destination policy, with which a subscription takes a message once however often it arrives
        // (the copy of a region's message that the global relays to a region bridged to its sender as well)
        org.apache.activemq.broker.region.policy.PolicyMap policy = new org.apache.activemq.broker.region.policy.PolicyMap();
        policy.setDefaultEntry(new org.apache.activemq.broker.region.policy.PolicyEntry());
        b.setDestinationPolicy(policy);
        authz.put(region, a);
        byRegion.put(region, b);
        return b;
    }

    /** A duplex bridge that {@code from}'s broker opens to {@code to}'s, as ActiveBroker does (the connector is registered). */
    private NetworkConnector bridge(String from, String to) throws Exception {
        BrokerService f = byRegion.get(from), t = byRegion.get(to);
        NetworkConnector nc = f.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(t)
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("cresco-bridge-" + from + "-" + to + "-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setNetworkTTL(5);                                   // broker_message_ttl, as ActiveBroker sets it
        nc.setConduitSubscriptions(false);
        nc.setDecreaseNetworkConsumerPriority(true);
        nc.setSslContext(new SslContext(controllers.get(from).keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom()));
        authz.get(from).registerOutboundBridge(nc.getName(), new PeerIdentity.Expected(to, controllerOf(to)));
        nc.start();
        return nc;
    }

    /** The node n1 of {@code region}, connected to its region's broker with its certificate. */
    private Session node(String region) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(byRegion.get(region))
                + "?verifyHostName=false");
        f.setKeyAndTrustManagers(nodes.get(region).keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static String stampOf(String region) { return "controller:t1/" + region + "/n1"; }

    /** Send {@code text} as {@code caller} (a node's own stamp when it is that node's). */
    private static void send(Session s, jakarta.jms.Destination to, String text, String caller) throws Exception {
        TextMessage m = s.createTextMessage(text);
        m.setStringProperty("cresco_caller", caller);
        m.setStringProperty("cresco_caller_tenant", "t1");
        m.setStringProperty("cresco_caller_roles", "infra");
        s.createProducer(to).send(m);
    }

    private long stripped() {
        long n = 0;
        for (CrescoAuthorizationBroker a : authz.values()) n += a.forgedStampCount();
        return n;
    }

    /** Everything {@code c} is given within {@code quietMs} of the last message: text -> the caller it carried. */
    private static Map<String, String> drain(MessageConsumer c, int expected, long quietMs, List<String> all) throws Exception {
        Map<String, String> got = new HashMap<>();
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            Message m = c.receive(got.size() >= expected ? quietMs : 5000L);
            if (m == null) break;
            String text = ((TextMessage) m).getText();
            all.add(text);
            got.put(text, m.getStringProperty("cresco_caller"));
        }
        return got;
    }

    @Test
    void inAMeshEveryTopicMessageArrivesWithItsCallerAndNoBridgeBringsAnotherRegionsStamp() throws Exception {
        for (String r : REGIONS) start(r, null);
        for (String r : new String[]{"rgA", "rgB", "rgC"}) bridge(r, "rgG");
        bridge("rgA", "rgB");
        bridge("rgA", "rgC");
        bridge("rgB", "rgC");
        Map<String, MessageConsumer> listeners = new HashMap<>();
        for (String r : REGIONS) {
            Session s = node(r);
            listeners.put(r, s.createConsumer(s.createTopic("t1.global.event")));
        }
        Thread.sleep(3000);                                                      // every bridge has its demand
        Session a = node("rgA");
        int sent = 20;
        for (int i = 0; i < sent; i++) send(a, a.createTopic("t1.global.event"), "advert-" + i, stampOf("rgA"));
        for (String r : REGIONS) {
            List<String> all = new ArrayList<>();
            Map<String, String> got = drain(listeners.get(r), sent, 1500L, all);
            assertEquals(sent, got.size(), "the listener of " + r + " was not given every message");
            assertEquals(sent, all.size(), "the listener of " + r + " was given a message twice");
            Set<String> callers = new HashSet<>(got.values());
            assertEquals(Set.of(stampOf("rgA")), callers, "at " + r + " a message arrived without its sender's stamp");
        }
        assertEquals(0, stripped(), "a bridge brought a stamp of another region's node (a second hop through a region)");
        assertEquals(0, authz.get("rgG").transitWithheldCount(), "the global relays");
        assertTrue(authz.get("rgB").transitWithheldCount() > 0 && authz.get("rgC").transitWithheldCount() > 0,
                "a region handed on what a bridge brought");
    }

    @Test
    void aQueueMessageWaitsForItsOwnBridgeInsteadOfCrossingAnotherRegion() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB"}) start(r, null);
        bridge("rgB", "rgG");                                                    // rgA's bridge to the global is not back yet
        bridge("rgA", "rgB");
        Session g = node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(3000);                                                      // the demand has gone as far as it goes
        assertTrue(byRegion.get("rgA").getDestination(new ActiveMQQueue("t1.rgG_gc")).getConsumers().isEmpty(),
                "rgB told rgA that it is a way to the global's inbox");
        Session a = node("rgA");
        send(a, a.createQueue("t1.rgG_gc"), "watchdog", stampOf("rgA"));
        assertNull(inbox.receive(2500), "rgA's message for the global went over rgB's bridge");
        bridge("rgA", "rgG");
        Message got = inbox.receive(10_000);
        assertNotNull(got, "the message did not arrive once rgA's own bridge was there");
        assertEquals(stampOf("rgA"), got.getStringProperty("cresco_caller"));
        assertEquals(0, stripped());
    }

    @Test
    void theGlobalStillRelaysBetweenItsRegions() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB"}) start(r, null);
        bridge("rgA", "rgG");
        bridge("rgB", "rgG");
        Session b = node("rgB");
        MessageConsumer inbox = b.createConsumer(b.createQueue("t1.rgB_n1"));
        MessageConsumer listener = node("rgB").createConsumer(b.createTopic("t1.global.event"));
        Thread.sleep(3000);
        Session a = node("rgA");
        send(a, a.createQueue("t1.rgB_n1"), "request", stampOf("rgA"));
        send(a, a.createTopic("t1.global.event"), "advert", stampOf("rgA"));
        Message q = inbox.receive(8000), t = listener.receive(8000);
        assertNotNull(q, "a queue message of rgA did not reach rgB over the global");
        assertNotNull(t, "a topic message of rgA did not reach rgB over the global");
        assertEquals(stampOf("rgA"), q.getStringProperty("cresco_caller"), "the global is rgB's parent: it carries the fabric");
        assertEquals(stampOf("rgA"), t.getStringProperty("cresco_caller"));
        assertEquals(0, stripped());
        assertTrue(authz.get("rgG").relaysBetweenBridges());
        assertFalse(authz.get("rgA").relaysBetweenBridges());
    }

    @Test
    void whatARelayingRegionBringsForAnotherRegionsNodeIsStillStripped() throws Exception {
        // the rule that strips is untouched: a region told to relay (as every region did) brings rgA's stamp to the global
        // over its own bridge, and the global strips it -- a region speaks for its own nodes only
        start("rgG", null);
        start("rgA", null);
        start("rgB", Boolean.TRUE);
        bridge("rgB", "rgG");
        bridge("rgA", "rgB");
        Session g = node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(3000);
        Session a = node("rgA");
        send(a, a.createQueue("t1.rgG_gc"), "watchdog", stampOf("rgA"));
        Message got = inbox.receive(8000);
        assertNotNull(got, "a relaying region did not relay");
        assertEquals("", got.getStringProperty("cresco_caller"), "rgB's bridge brought a stamp of rgA's node and the global kept it");
        assertEquals(1, authz.get("rgG").outOfScopeStampCount());
    }

    @Test
    void aStampARegionForgesForAnotherRegionsNodeIsStillStripped() throws Exception {
        // a region whose broker runs no Cresco authorization (a compromised one would not) lets its node declare anything
        start("rgG", null);
        BrokerService rogue = TlsTestSupport.broker("rgB_rc", controllers.get("rgB"), false, null, null, anchors);
        brokers.add(rogue);
        NetworkConnector nc = rogue.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(byRegion.get("rgG"))
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("cresco-bridge-rogue-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(new SslContext(controllers.get("rgB").keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom()));
        nc.start();
        byRegion.put("rgB", rogue);
        Session g = node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(2500);
        Session b = node("rgB");
        send(b, b.createQueue("t1.rgG_gc"), "forged", stampOf("rgA"));
        send(b, b.createQueue("t1.rgG_gc"), "own", stampOf("rgB"));
        Map<String, String> got = drain(inbox, 2, 500L, new ArrayList<>());
        assertEquals("", got.get("forged"), "rgB's bridge spoke for a node of rgA");
        assertEquals(stampOf("rgB"), got.get("own"), "a region's own node is carried");
    }
}
