package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a broker bridge's traffic may go, over real duplex bridges. A bridge was exempt from tenant policy in both
 * directions: a region's controller (or whoever holds its key) could bridge to a broker, subscribe another tenant's
 * destinations there and write into them. A bridge now carries its peer's own: a region its tenant's traffic and what is
 * addressed to its region; this node's parent (its global) and a superuser tenant's controller the fabric.
 *
 * <p>The fabric: a global rgG/gc of the superuser tenant; region rgA of tenant t1 (the bridging region; its side runs no
 * Cresco authorization, as a compromised region's broker would not); tenant t2's nodes at the global.
 */
class BridgeTenantScopeTest {

    private static RegionCA caG, caA, caB, caP;
    private static TlsTestSupport.Id global, regionA, a1, b1, parentT1;
    private static X509Certificate[] anchors;

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        caG = RegionCA.generate("cresco-system", "rgG", 2048, 1);
        caA = RegionCA.generate("t1", "rgA", 2048, 1);
        caB = RegionCA.generate("t2", "rgB", 2048, 1);
        caP = RegionCA.generate("t1", "rgP", 2048, 1);
        global = TlsTestSupport.issued(caG, "cresco-system", "rgG", "gc");
        regionA = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        a1 = TlsTestSupport.issued(caA, "t1", "rgA", "a1");
        b1 = TlsTestSupport.issued(caB, "t2", "rgB", "b1");
        parentT1 = TlsTestSupport.issued(caP, "t1", "rgP", "pc");
        anchors = new X509Certificate[]{caG.caCert(), caA.caCert(), caB.caCert(), caP.caCert()};
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) brokers.get(i).stop();
    }

    private static PeerTrustRegistry registry() {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caG.caCert(), "rgG", "cresco-system", "test"));
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t2", "test"));
        assertNull(peers.trustCa(caP.caCert(), "rgP", "t1", "test"));
        return peers;
    }

    private static CrescoAuthorizationBroker authz(PeerTrustRegistry peers, String tenant) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", tenant);
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("btscope").toString(), cfg);
        return new CrescoAuthorizationBroker(pb, peers);
    }

    private BrokerService secured(String name, TlsTestSupport.Id id, CrescoAuthorizationBroker authz) throws Exception {
        BrokerService b = TlsTestSupport.broker(name, id, true, new BrokerPlugin[]{authz}, authz, anchors);
        brokers.add(b);
        return b;
    }

    private BrokerService plain(String name, TlsTestSupport.Id id) throws Exception {
        BrokerService b = TlsTestSupport.broker(name, id, false, null, null, anchors);
        brokers.add(b);
        return b;
    }

    /** A network client of {@code b} with the certificate of {@code id}. */
    private Session client(BrokerService b, TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /** This JVM's own connection to {@code b} (what the broker's controller holds). */
    private Session local(BrokerService b) throws Exception {
        Connection c = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false").createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private void bridge(BrokerService from, TlsTestSupport.Id as, BrokerService to) throws Exception {
        NetworkConnector nc = from.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(to)
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("cresco-bridge-test-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(new SslContext(as.keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom()));
        nc.start();
        Thread.sleep(2000);
    }

    private static void send(Session s, boolean topic, String name, int n) throws Exception {
        var p = s.createProducer(topic ? s.createTopic(name) : s.createQueue(name));
        for (int i = 0; i < n; i++) p.send(s.createTextMessage(name + "#" + i));
    }

    private static int drain(MessageConsumer c, long firstWaitMs) throws Exception {
        int n = 0;
        for (Message m = c.receive(firstWaitMs); m != null; m = c.receive(500)) n++;
        return n;
    }

    /** The global: the superuser tenant's, region rgA's controller joined it (discovery). */
    private CrescoAuthorizationBroker gAuthz;

    private BrokerService globalWithRegionA() throws Exception {
        PeerTrustRegistry peers = registry();
        peers.allowBridgePeer("rgA", "rc");
        gAuthz = authz(peers, "cresco-system");
        return secured("rgG_gc", global, gAuthz);
    }

    /** Region rgA (tenant t1) demands tenant t2's inbox and dataplane at the global: nothing of tenant t2 leaves over its bridge. */
    @Test
    void aRegionsBridgeIsGivenItsOwnTenantsAndRegionsTrafficOnly() throws Exception {
        BrokerService g = globalWithRegionA();
        BrokerService region = plain("rgA_rc", regionA);
        bridge(region, regionA, g);
        Session atRegion = local(region);
        MessageConsumer t2Inbox = atRegion.createConsumer(atRegion.createQueue("T.t2.rgB_b1"));
        MessageConsumer t2Wild = atRegion.createConsumer(atRegion.createQueue("T.t2.>"));
        MessageConsumer t2Data = atRegion.createConsumer(atRegion.createTopic("T.t2.global.event"));
        MessageConsumer ownInbox = atRegion.createConsumer(atRegion.createQueue("T.t1.rgA_a1"));
        MessageConsumer ownTenantOtherRegion = atRegion.createConsumer(atRegion.createQueue("T.t1.rgC_c1"));
        MessageConsumer ownData = atRegion.createConsumer(atRegion.createTopic("T.t1.global.event"));
        MessageConsumer ownRegionFromOperator = atRegion.createConsumer(atRegion.createQueue("T.cresco-system.rgA_rc"));
        Thread.sleep(2000);                                                          // demand crosses the bridge
        Session t2 = client(g, b1), t1 = client(g, a1), operator = local(g);
        send(t2, false, "T.t2.rgB_b1", 3);
        send(t2, false, "T.t2.other", 3);
        send(t2, true, "T.t2.global.event", 3);
        send(t1, false, "T.t1.rgA_a1", 3);
        send(t1, false, "T.t1.rgC_c1", 3);
        send(t1, true, "T.t1.global.event", 3);
        send(operator, false, "T.cresco-system.rgA_rc", 3);
        assertEquals(3, drain(ownInbox, 8000), "the region's own tenant's inbox traffic did not cross its bridge");
        assertEquals(3, drain(ownTenantOtherRegion, 4000), "its tenant's traffic for another region (transit) did not cross");
        assertEquals(3, drain(ownData, 4000), "its tenant's dataplane did not cross");
        assertEquals(3, drain(ownRegionFromOperator, 4000), "what the operator's node sent to an inbox of its region did not cross");
        int stolen = drain(t2Inbox, 3000) + drain(t2Wild, 500), stolenData = drain(t2Data, 500);
        System.out.println("region rgA (t1) over its bridge to the global: tenant t2's inbox messages received=" + stolen
                + ", dataplane messages=" + stolenData + "; withheld at the global=" + gAuthz.bridgeWithheldCount());
        assertEquals(0, stolen, "tenant t1's region read tenant t2's inboxes over its bridge");
        assertEquals(0, stolenData, "tenant t1's region read tenant t2's dataplane over its bridge");
        assertTrue(gAuthz.bridgeWithheldCount() > 0);
        // what was withheld is still there for whom it was meant
        assertEquals(3, drain(client(g, b1).createConsumer(t2.createQueue("T.t2.rgB_b1")), 5000), "tenant t2's inbox lost its messages");
    }

    /** Region rgA (tenant t1) writes into tenant t2's inbox and dataplane at the global over its bridge: discarded there. */
    @Test
    void aRegionsBridgeBringsInItsOwnTenantsTrafficOnly() throws Exception {
        BrokerService g = globalWithRegionA();
        BrokerService region = plain("rgA_rc", regionA);
        bridge(region, regionA, g);
        Session t2 = client(g, b1), t1 = client(g, a1), operator = local(g);
        MessageConsumer t2Inbox = t2.createConsumer(t2.createQueue("T.t2.rgB_b1"));
        MessageConsumer t2Data = t2.createConsumer(t2.createTopic("T.t2.global.event"));
        MessageConsumer t1Inbox = t1.createConsumer(t1.createQueue("T.t1.rgA_a1"));
        MessageConsumer t1Data = t1.createConsumer(t1.createTopic("T.t1.global.event"));
        MessageConsumer operatorInbox = operator.createConsumer(operator.createQueue("T.*.rgG_gc"));   // a controller's inbox
        Thread.sleep(2000);                                                          // demand crosses the bridge
        Session atRegion = local(region);
        send(atRegion, false, "T.t2.rgB_b1", 3);
        send(atRegion, true, "T.t2.global.event", 3);
        send(atRegion, false, "T.t1.rgA_a1", 3);
        send(atRegion, true, "T.t1.global.event", 3);
        send(atRegion, false, "T.t1.rgG_gc", 3);
        send(atRegion, false, "T.cresco-system.rgG_gc", 3);                           // its answers to the operator's global
        assertEquals(3, drain(t1Inbox, 8000), "the region's own tenant's traffic did not cross its bridge");
        assertEquals(3, drain(t1Data, 4000), "its tenant's dataplane did not cross");
        assertEquals(6, drain(operatorInbox, 4000), "what the region sent to the global's inbox did not cross");
        int injected = drain(t2Inbox, 3000), injectedData = drain(t2Data, 500);
        System.out.println("region rgA (t1) over its bridge to the global: messages written into tenant t2's inbox=" + injected
                + ", into its dataplane=" + injectedData + "; discarded at the global=" + gAuthz.bridgeDiscardedCount());
        assertEquals(0, injected, "tenant t1's region wrote into tenant t2's inbox over its bridge");
        assertEquals(0, injectedData, "tenant t1's region wrote into tenant t2's dataplane over its bridge");
        assertTrue(gAuthz.bridgeDiscardedCount() >= 6);
    }

    /**
     * At a region: the bridge to its parent carries the fabric (another tenant's message addressed to the region's
     * controller arrives); the same bridge to a controller that is not its parent carries that controller's tenant only.
     */
    @Test
    void theParentsBridgeCarriesTheFabricAndAPeersOnlyItsOwn() throws Exception {
        for (boolean isParent : new boolean[]{true, false}) {
            BrokerService upstream = plain("rgP_pc", parentT1);                      // a t1 controller (no superuser tenant)
            PeerTrustRegistry peers = registry();
            peers.allowBridgePeer("rgP", "pc");                                      // the region discovered it
            CrescoAuthorizationBroker rAuthz = authz(peers, "t1");
            if (isParent) rAuthz.setParent(() -> new PeerIdentity.Expected("rgP", "pc"));
            BrokerService region = secured("rgA_rc", regionA, rAuthz);
            bridge(region, regionA, upstream);
            Session controller = local(region);
            MessageConsumer inbox = controller.createConsumer(controller.createQueue("T.*.rgA_rc"));
            Thread.sleep(2000);
            Session up = local(upstream);
            send(up, false, "T.t1.rgA_rc", 2);
            send(up, false, "T.t2.rgA_rc", 2);                                       // relayed by the upstream for a node of tenant t2
            int got = drain(inbox, 8000);
            System.out.println("bridge to " + (isParent ? "the parent" : "a peer controller") + ": " + got + " of 4 messages arrived");
            assertEquals(isParent ? 4 : 2, got, isParent ? "the parent's bridge did not carry the fabric below it"
                    : "a peer controller's bridge carried another tenant's traffic");
            stop();
            brokers.clear();
            connections.clear();
        }
    }

    /** A controller that did not complete discovery with this node is no bridge peer; its bridge is refused. */
    @Test
    void aControllerThatDidNotJoinThisNodeBridgesNothing() throws Exception {
        PeerTrustRegistry peers = registry();                                        // rgA/rc verifies here, but never joined
        gAuthz = authz(peers, "cresco-system");
        BrokerService g = secured("rgG_gc", global, gAuthz);
        BrokerService region = plain("rgA_rc", regionA);
        bridge(region, regionA, g);
        Session atRegion = local(region);
        MessageConsumer own = atRegion.createConsumer(atRegion.createQueue("T.t1.rgA_a1"));
        Thread.sleep(1500);
        send(client(g, a1), false, "T.t1.rgA_a1", 2);
        assertEquals(0, drain(own, 3000), "a controller that never joined this node bridged into it");
    }
}
