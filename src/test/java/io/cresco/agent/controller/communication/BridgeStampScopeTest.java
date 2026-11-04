package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a broker bridge carries (review Phase 3 F2), over a real duplex bridge region rgB -> global rgA/gc: the global keeps
 * only the stamps of rgB's own nodes (and users of its tenant) from rgB's bridge; the region keeps any node's stamp of
 * the global's fabric from its own (outbound) bridge to its global, recognised by the connector it opened.
 */
class BridgeStampScopeTest {

    private static RegionCA ca, caB;
    private static TlsTestSupport.Id global, a1, regionCtl, s2;

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private BrokerService g;
    private CrescoAuthorizationBroker gAuthz;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        caB = RegionCA.generate("t1", "rgB", 2048, 1);
        global = TlsTestSupport.issued(ca, "t1", "rgA", "gc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        regionCtl = TlsTestSupport.issued(caB, "t1", "rgB", "rc");
        s2 = TlsTestSupport.issued(caB, "t1", "rgB", "s2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        peers.allowBridgePeer("rgB", "rc");
        gAuthz = authz(peers);
        g = TlsTestSupport.broker("rgA_gc", global, true, new BrokerPlugin[]{gAuthz}, gAuthz, ca.caCert(), caB.caCert());
        brokers.add(g);
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) brokers.get(i).stop();
    }

    private static CrescoAuthorizationBroker authz(PeerTrustRegistry peers) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("bscope").toString(), cfg);
        return new CrescoAuthorizationBroker(pb, peers);
    }

    private Session client(BrokerService b, TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caB.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private NetworkConnector bridge(BrokerService local, String name) throws Exception {
        return bridge(local, name, g);
    }

    private NetworkConnector bridge(BrokerService local, String name, BrokerService to) throws Exception {
        NetworkConnector nc = local.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(to)
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName(name);
        nc.setDuplex(true);
        nc.setSslContext(new SslContext(regionCtl.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caB.caCert()), new SecureRandom()));
        nc.start();
        return nc;
    }

    private static void sendAs(Session s, String queue, String caller, String tenant, String roles) throws Exception {
        TextMessage m = s.createTextMessage("x");
        m.setStringProperty("cresco_caller", caller);
        m.setStringProperty("cresco_caller_tenant", tenant);
        m.setStringProperty("cresco_caller_roles", roles);
        s.createProducer(s.createQueue(queue)).send(m);
    }

    /** At the global: what does a1 receive when the region side declares {@code caller}? */
    private String overRegionBridge(BrokerService region, String queue, String caller, String tenant, String roles) throws Exception {
        MessageConsumer c = client(g, a1).createConsumer(client(g, a1).createQueue(queue));
        Thread.sleep(1500);                                                      // demand crosses the bridge
        sendAs(client(region, s2), queue, caller, tenant, roles);
        Message got = c.receive(8000);
        assertNotNull(got, "nothing crossed the bridge");
        return got.getStringProperty("cresco_caller");
    }

    @Test
    void theGlobalKeepsOnlyTheBridgingRegionsOwnNodes() throws Exception {
        // the region side runs no Cresco authorization (a compromised region's broker would not): the global decides alone
        BrokerService region = TlsTestSupport.broker("rgB_rc", regionCtl, false, null, null, ca.caCert(), caB.caCert());
        brokers.add(region);
        bridge(region, "test-bridge-" + System.nanoTime());
        Thread.sleep(2000);
        assertEquals("controller:t1/rgB/s9", overRegionBridge(region, "t1.q1", "controller:t1/rgB/s9", "t1", "infra"),
                "a node of the bridging region is carried");
        assertEquals("", overRegionBridge(region, "t1.q2", "controller:t1/rgC/x", "t1", "infra"),
                "a node of another region crossed region rgB's bridge");
        assertEquals("", overRegionBridge(region, "t1.q3", "api:cresco-system/root", "cresco-system", "admin"),
                "a superuser admin crossed a t1 region's bridge");
        assertEquals("", overRegionBridge(region, "t1.q4", "controller:t1/rgB/s9", "t1", "infra,admin"),
                "a controller stamp with extra roles crossed");
        assertEquals("api:t1/u", overRegionBridge(region, "t1.q5", "api:t1/u", "t1", "user"), "a user of its tenant is carried");
        assertTrue(gAuthz.outOfScopeStampCount() >= 3);
    }

    /**
     * The region rgB opens a duplex bridge to {@code to} (the global rgA/gc; with {@code secured=false} a global that runs no
     * Cresco authorization, i.e. one that stamps whatever its clients declare); what stamp reaches s2 at the region?
     */
    private String overGlobalBridge(boolean secured, boolean parentSet, String caller, String tenant, String roles) throws Exception {
        BrokerService to = g;
        if (!secured) {
            to = TlsTestSupport.broker("rgA_gc", global, false, null, null, ca.caCert(), caB.caCert());
            brokers.add(to);
        }
        PeerTrustRegistry rPeers = new PeerTrustRegistry();
        assertNull(rPeers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        assertNull(rPeers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        rPeers.allowBridgePeer("rgA", "gc");                                    // the region discovered its global
        CrescoAuthorizationBroker rAuthz = authz(rPeers);
        if (parentSet) rAuthz.setParent(() -> new PeerIdentity.Expected("rgA", "gc"));
        String name = "cresco-bridge-127.0.0.1-0-0-mixed-" + System.nanoTime();
        rAuthz.registerOutboundBridge(name, new PeerIdentity.Expected("rgA", "gc"));
        BrokerService region = TlsTestSupport.broker("rgB_rc", regionCtl, true, new BrokerPlugin[]{rAuthz}, rAuthz, ca.caCert(), caB.caCert());
        brokers.add(region);
        bridge(region, name, to);
        Thread.sleep(2000);
        Session rs = client(region, s2);
        MessageConsumer c = rs.createConsumer(rs.createQueue("t1.back"));
        Thread.sleep(1500);
        sendAs(client(to, a1), "t1.back", caller, tenant, roles);
        Message got = c.receive(8000);
        assertNotNull(got, "nothing crossed the bridge back");
        return got.getStringProperty("cresco_caller");
    }

    @Test
    void theRegionKeepsItsGlobalsFabricFromItsGlobalsBridge() throws Exception {
        assertEquals("controller:t1/rgA/a1", overGlobalBridge(true, true, "controller:t1/rgA/a1", "t1", "infra"));
    }

    @Test
    void theGlobalsSubtreeIsTheFabricBelowIt() throws Exception {
        assertEquals("controller:t1/rgZ/z1", overGlobalBridge(false, true, "controller:t1/rgZ/z1", "t1", "infra"),
                "a node of another region below the global, carried by the global");
    }

    @Test
    void aBridgeThatIsNotThisNodesParentCarriesOnlyItsRegion() throws Exception {
        assertEquals("", overGlobalBridge(false, false, "controller:t1/rgZ/z1", "t1", "infra"));
    }

    @Test
    void evenTheParentCarriesNoOtherTenant() throws Exception {
        assertEquals("", overGlobalBridge(false, true, "api:cresco-system/root", "cresco-system", "admin"));
    }

    @Test
    void theScopeRules() {
        Set<String> su = Set.of("cresco-system");
        CrescoIdentity rB = CrescoIdentity.of("t1", "rgB", "rc", null), suR = CrescoIdentity.of("cresco-system", "rgB", "rc", null);
        assertNull(CrescoAuthorizationBroker.bridgeCarryRefusal(rB, false, Caller.plugin("t1", "rgB", "a3", "p1"), su));
        assertNotNull(CrescoAuthorizationBroker.bridgeCarryRefusal(rB, false, Caller.plugin("t1", "rgA", "a3", "p1"), su));
        assertNull(CrescoAuthorizationBroker.bridgeCarryRefusal(rB, true, Caller.plugin("t1", "rgA", "a3", "p1"), su), "the parent's subtree");
        assertNotNull(CrescoAuthorizationBroker.bridgeCarryRefusal(rB, true, Caller.plugin("t2", "rgA", "a3", "p1"), su), "another tenant, even from the parent");
        assertNull(CrescoAuthorizationBroker.bridgeCarryRefusal(suR, false, Caller.controller("t2", "rgB", "a3"), su), "a superuser region carries any tenant");
        assertNotNull(CrescoAuthorizationBroker.bridgeCarryRefusal(suR, false, Caller.controller("t2", "rgZ", "a3"), su), "but only in its subtree");
        assertNotNull(CrescoAuthorizationBroker.bridgeCarryRefusal(rB, true, Caller.anonymous(), su));
    }
}
