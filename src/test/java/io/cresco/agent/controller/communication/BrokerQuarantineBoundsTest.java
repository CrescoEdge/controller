package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** Round-4 review L2, H2 (durable subscriptions) and M3 (the live-connection sweep). */
class BrokerQuarantineBoundsTest {

    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private BrokerService broker(RegionCA ca, CrescoAuthorizationBroker authz, String tls) throws Exception {
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(ca, "t1", "rgA", "rc"), tls,
                new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
        return b;
    }

    private Connection connect(BrokerService b, RegionCA ca, TlsTestSupport.Id id, String clientId) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id == null ? null : id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        if (clientId != null) c.setClientID(clientId);
        connections.add(c);
        c.start();
        return c;
    }

    private static PluginBuilder plugin(Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("broker_require_client_auth", "false");
        cfg.put("tenant_namespacing", "true");
        cfg.putAll(extra);
        return TestAgentService.plugin(Files.createTempDirectory("bq").toString(), cfg);
    }

    /** L2: a quarantined connection holds a bounded number of consumers, and quarantined connections are bounded too. */
    @Test
    void aQuarantinedConnectionIsBounded() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_quarantine_max_consumers", "3",
                "broker_quarantine_max_connections", "2")), peers);
        BrokerService b = broker(ca, authz, "?wantClientAuth=true");
        Connection anon = connect(b, ca, null, null);
        Session s = anon.createSession(false, Session.AUTO_ACKNOWLEDGE);
        int made = 0;
        String refusal = null;
        try {
            for (int i = 0; i < 10; i++) { s.createConsumer(s.createQueue("q" + i)); made++; }
        } catch (Exception e) {
            refusal = e.getMessage();
        }
        System.out.println("quarantined connection: " + made + " consumers made, then: " + refusal);
        assertNotNull(refusal, "a quarantined connection created consumers without bound");
        assertTrue(made < 3, "more consumers than the bound (the client's own advisory consumer counts): " + made);
        connect(b, ca, null, null);
        assertThrows(Exception.class, () -> connect(b, ca, null, null), "more quarantined connections than the bound");
        assertEquals(2, authz.quarantinedConnectionCount());
    }

    /**
     * A connection the broker refuses to add (here: its client id is in use) holds nothing afterwards: the broker never
     * removes a connection it did not add, so neither a live-connection record nor a quarantine slot may stay behind.
     */
    @Test
    void aConnectionThatIsNotAddedHoldsNothing() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_quarantine_max_connections", "2")), peers);
        BrokerService b = broker(ca, authz, "?wantClientAuth=true");
        TlsTestSupport.Id a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        connect(b, ca, a1, "one-client");
        assertEquals(1, authz.liveVerifiedCount());
        for (int i = 0; i < 5; i++) assertThrows(Exception.class, () -> connect(b, ca, a1, "one-client"), "a client id in use was accepted");
        assertEquals(1, authz.liveVerifiedCount(), "every refused connection of a verified agent left its context with the broker");
        connect(b, ca, null, "anon-client");
        for (int i = 0; i < 5; i++) assertThrows(Exception.class, () -> connect(b, ca, null, "anon-client"), "a client id in use was accepted");
        assertEquals(1, authz.quarantinedConnectionCount(), "every refused connection kept a quarantine slot");
        connect(b, ca, null, "anon-2");
        assertEquals(2, authz.quarantinedConnectionCount());
    }

    /** L2: quarantined connections cannot create destinations without bound over time. */
    @Test
    void quarantinedConnectionsCreateABoundedNumberOfDestinations() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_quarantine_max_destinations", "3")), peers);
        BrokerService b = broker(ca, authz, "?wantClientAuth=true");
        int created = 0;
        for (int i = 0; i < 6; i++) {
            Connection anon = connect(b, ca, null, null);
            try {
                anon.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new org.apache.activemq.command.ActiveMQQueue("junk" + i));
                created++;
            } catch (Exception refused) {
                // bounded
            }
            anon.close();
        }
        System.out.println("destinations quarantined connections could create: " + created);
        assertEquals(3, created);
    }

    /** H2: a durable subscription can be removed only by the connection whose client id names it. */
    @Test
    void anotherConnectionCannotRemoveADurableSubscription() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_require_client_auth", "true")), peers);
        BrokerService b = broker(ca, authz, "?needClientAuth=true");
        Connection owner = connect(b, ca, TlsTestSupport.issued(ca, "t1", "rgA", "a1"), "owner-client");
        Session os = owner.createSession(false, Session.AUTO_ACKNOWLEDGE);
        os.createDurableSubscriber(os.createTopic("T.t1.feed"), "keep").close();
        Connection other = connect(b, ca, TlsTestSupport.issued(ca, "t1", "rgA", "a2"), "other-client");
        org.apache.activemq.command.RemoveSubscriptionInfo rsi = new org.apache.activemq.command.RemoveSubscriptionInfo();
        rsi.setClientId("owner-client");
        rsi.setSubscriptionName("keep");
        rsi.setConnectionId(((org.apache.activemq.ActiveMQConnection) other).getConnectionInfo().getConnectionId());
        assertThrows(Exception.class, () -> ((org.apache.activemq.ActiveMQConnection) other).syncSendPacket(rsi),
                "another connection removed a durable subscription it does not own");
        os.unsubscribe("keep");                                        // its owner may
    }

    /** M3: the periodic sweep refreshes every verified connection that is up, even an idle one. */
    @Test
    void theSweepRefreshesAnIdleConnectedPeer() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(peers.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_require_client_auth", "true")), peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(ca, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, a1.chain[1]);
        brokers.add(b);
        connect(b, ca, a1, null);
        assertEquals(1, authz.liveVerifiedCount());
        peers.backdateBinding("rgA", "a1", 30L * 24 * 3600 * 1000);
        authz.refreshLiveIdentities();
        assertTrue(System.currentTimeMillis() - peers.lastSeen("rgA", "a1") < 60_000L, "an idle but connected peer was not refreshed");
    }

    /** D2a: with tenant namespacing, the un-namespaced fabric-wide global.event is not shared; the tenant's own topic is. */
    @Test
    void withNamespacingATenantCannotUseTheUnNamespacedGlobalEvent() throws Exception {
        assertEquals("agent.event,region.event", CrescoAuthorizationBroker.defaultSharedDestinations(plugin(Map.of()).getConfig()));
        assertEquals("agent.event,region.event,global.event",
                CrescoAuthorizationBroker.defaultSharedDestinations(plugin(Map.of("tenant_namespacing", "false")).getConfig()));
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(plugin(Map.of("broker_require_client_auth", "true")), peers);
        BrokerService b = broker(ca, authz, "?needClientAuth=true");
        Session s = connect(b, ca, TlsTestSupport.issued(ca, "t1", "rgA", "a1"), null).createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(Exception.class, () -> s.createConsumer(s.createTopic("global.event")), "a tenant read the fabric-wide global.event");
        assertThrows(Exception.class, () -> s.createProducer(s.createTopic("global.event")), "a tenant wrote the fabric-wide global.event");
        s.createConsumer(s.createTopic("T.t1.global.event"));                    // its own dataplane
    }
}
