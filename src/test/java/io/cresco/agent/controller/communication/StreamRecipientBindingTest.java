package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OUT-28 "who may subscribe to which stream": a dataplane message bound to a recipient ({@code cresco_to}) reaches
 * only that recipient's connection, a subscription asking for someone else's stream is refused, and a client cannot
 * escape the broker's authorization by claiming to be a broker bridge. Real brokers, real mutual TLS (identities from
 * client certificates), the broker wired as ActiveBroker wires it under broker_security_enabled.
 */
class StreamRecipientBindingTest {

    private static final String TOPIC = "global.event";
    private static RegionCA ca, caB;                                // region rgA's CA and region rgB's CA
    private static TlsTestSupport.Id global, a1, a2, sender, regionCtl, strayAgent;

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private PeerTrustRegistry peers;
    private BrokerService g;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        caB = RegionCA.generate("t1", "rgB", 2048, 1);
        global = TlsTestSupport.issued(ca, "t1", "rgA", "gc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");        // the stream's recipient
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");        // another agent of the same tenant and region
        sender = TlsTestSupport.issued(ca, "t1", "rgA", "s1");
        regionCtl = TlsTestSupport.issued(caB, "t1", "rgB", "rc"); // a region controller that discovered the global
        strayAgent = TlsTestSupport.issued(ca, "t1", "rgA", "a9"); // a leaf agent with a valid certificate
    }

    @BeforeEach
    void start() throws Exception {
        peers = new PeerTrustRegistry();
        // what discovery records: each region's CA, scoped to its region, and rgB/rc as a bridge peer
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        peers.allowBridgePeer("rgB", "rc");
        g = securedBroker("rgA_gc", global);
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) brokers.get(i).stop();
    }

    /** A broker as ActiveBroker builds it with broker_security_enabled=true and broker_require_client_auth=true. */
    private BrokerService securedBroker(String name, TlsTestSupport.Id id) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");                                   // the controller's tenant, as on a real node
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker(name, id, true, new BrokerPlugin[]{authz}, authz, ca.caCert(), caB.caCert());
        brokers.add(b);
        return b;
    }

    private Session client(BrokerService b, TlsTestSupport.Id id) throws Exception {
        return client(b, id, null);
    }

    private Session client(BrokerService b, TlsTestSupport.Id id, String assertedUser) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caB.caCert()), new SecureRandom());
        if (assertedUser != null) f.setUserName(assertedUser);
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static void send(Session s, String text, String to, String toTenant) throws Exception {
        MessageProducer p = s.createProducer(s.createTopic(TOPIC));
        TextMessage m = s.createTextMessage(text);
        m.setStringProperty("stream_name", "gkt-1");
        if (to != null) m.setStringProperty(StreamRecipientPolicy.TO, to);
        if (toTenant != null) m.setStringProperty(StreamRecipientPolicy.TO_TENANT, toTenant);
        p.send(m);
    }

    private static List<String> drain(MessageConsumer c, long firstWaitMs) throws Exception {
        List<String> got = new ArrayList<>();
        Message m = c.receive(firstWaitMs);
        while (m != null) {
            got.add(((TextMessage) m).getText());
            m = c.receive(500);
        }
        return got;
    }

    @Test
    void theRecipientReceivesItsStream() throws Exception {
        Session rx = client(g, a1);
        MessageConsumer c = rx.createConsumer(rx.createTopic(TOPIC), StreamRecipientPolicy.TO + "='rgA_a1'");
        send(client(g, sender), "for-a1", "rgA_a1", null);
        TextMessage m = (TextMessage) c.receive(5000);
        assertNotNull(m, "the intended recipient did not receive its stream");
        assertEquals("for-a1", m.getText());
    }

    @Test
    void anotherIdentityCannotSubscribeToTheStream() throws Exception {
        Session spy = client(g, a2);
        Topic t = spy.createTopic(TOPIC);
        JMSException e = assertThrows(JMSException.class, () -> spy.createConsumer(t, StreamRecipientPolicy.TO + "='rgA_a1'"),
                "a2 subscribed to the stream bound to a1");
        assertTrue(String.valueOf(e.getMessage()).contains("rgA_a1"), e.getMessage());
    }

    @Test
    void aBroadSubscriptionSeesOnlyMessagesNotBoundToSomeoneElse() throws Exception {
        Session spy = client(g, a2);
        MessageConsumer wide = spy.createConsumer(spy.createTopic(TOPIC));            // no selector at all
        Session rx = client(g, a1);
        MessageConsumer mine = rx.createConsumer(rx.createTopic(TOPIC));
        Session tx = client(g, sender);
        send(tx, "for-a1", "rgA_a1", null);
        send(tx, "for-everyone", null, null);

        assertEquals(List.of("for-everyone"), drain(wide, 3000), "a non-recipient received a recipient-bound message");
        assertEquals(List.of("for-a1", "for-everyone"), drain(mine, 3000));
    }

    @Test
    void aStreamBoundToAnotherTenantIsWithheldFromTheSameAgentName() throws Exception {
        Session rx = client(g, a1);                                                   // a1 is tenant t1
        MessageConsumer c = rx.createConsumer(rx.createTopic(TOPIC));
        send(client(g, sender), "for-t2-a1", "rgA_a1", "t2");
        assertNull(c.receive(2000), "a1 of tenant t1 received a stream bound to tenant t2");
    }

    @Test
    void claimingToBeABridgeAfterConnectingEarnsNoExemption() throws Exception {
        Session spy = client(g, a2);
        ActiveMQConnection conn = (ActiveMQConnection) connections.get(connections.size() - 1);
        BrokerInfo claim = new BrokerInfo();
        claim.setBrokerId(new BrokerId("fake-" + System.nanoTime()));
        claim.setBrokerName("rgA_a2");
        claim.setNetworkConnection(true);
        conn.syncSendPacket(claim);                                                   // any client can send this
        Thread.sleep(200);
        // a2 may not read a1's inbox (same-region peer inbox: write only)
        JMSException e = assertThrows(JMSException.class, () -> spy.createConsumer(spy.createQueue("rgA_a1")),
                "a client became exempt from authorization by claiming to be a broker bridge");
        assertTrue(String.valueOf(e.getMessage()).contains("denied"), e.getMessage());
    }

    @Test
    void aCertificateNamingNoIdentityCannotBorrowAnAssertedOne() throws Exception {
        java.security.KeyPair kp = TlsTestSupport.rsa();
        TlsTestSupport.Id anon = new TlsTestSupport.Id(kp, ca.issueLeaf(kp.getPublic(),
                io.cresco.library.security.CrescoIdentity.of(null, null, "nobody", null), 1));   // CN=nobody, valid CA
        Session s = client(g, anon, "t1|rgB|rc");                                          // asserts a bridge peer's name
        assertThrows(JMSException.class, () -> s.createConsumer(s.createQueue("rgB_rc")),
                "a certificate without a Cresco identity was given the identity its username asserted");
        ActiveMQConnection conn = (ActiveMQConnection) connections.get(connections.size() - 1);
        BrokerInfo claim = new BrokerInfo();
        claim.setBrokerId(new BrokerId("fake-" + System.nanoTime()));
        claim.setBrokerName("rgB_rc");
        claim.setNetworkConnection(true);
        conn.syncSendPacket(claim);
        Thread.sleep(200);
        assertThrows(JMSException.class, () -> s.createConsumer(s.createQueue("rgA_a1")),
                "an identity-less certificate plus a bridge claim earned the bridge exemption");
    }

    private Session vm(BrokerService b, String user) throws Exception {
        org.apache.activemq.ActiveMQConnectionFactory f = new org.apache.activemq.ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false");
        if (user != null) f.setUserName(user);
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    void theControllersOwnVmConnectionIsItsIdentityForRecipientBoundStreams() throws Exception {
        // what ActiveClient opens on a controller: every plugin on it, wsapi and its clients included, uses this connection
        Session local = vm(g, CrescoAuthorizationBroker.LOCAL_CONTROLLER_PREFIX + "t1|rgA|gc");
        Topic t = local.createTopic(TOPIC);
        JMSException e = assertThrows(JMSException.class, () -> local.createConsumer(t, StreamRecipientPolicy.TO + "='rgA_a1'"),
                "a client of the controller's wsapi subscribed to another agent's stream");
        assertTrue(String.valueOf(e.getMessage()).contains("rgA_a1"), e.getMessage());
        MessageConsumer wide = local.createConsumer(t);
        Session unmarked = vm(g, null);                       // in-JVM, but neither the controller nor a bridge's local side
        MessageConsumer other = unmarked.createConsumer(unmarked.createTopic(TOPIC));
        Session tx = client(g, sender);
        send(tx, "for-a1", "rgA_a1", null);
        send(tx, "for-gc", "rgA_gc", null);
        assertEquals(List.of("for-gc"), drain(wide, 3000), "the controller's own connection received a stream bound to another agent");
        assertEquals(List.of(), drain(other, 2000), "an in-JVM connection that is neither the controller nor a bridge received"
                + " recipient-bound messages (bridges forwarding is covered by aBridgeFromADiscoveredController...)");
    }

    @Test
    void peerIdentityModeOffIsTheOldNameOnlyBehaviour() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put(PeerIdentity.MODE_KEY, "off");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, new PeerTrustRegistry());
        TlsTestSupport.Id forged = TlsTestSupport.selfRooted("anyone", "t1", "rgA", "a1");
        BrokerService b = TlsTestSupport.broker("off", global, true, new BrokerPlugin[]{authz}, authz, ca.caCert(), forged.chain[1]);
        brokers.add(b);
        Session spy = client(b, forged);
        MessageConsumer c = spy.createConsumer(spy.createTopic(TOPIC));
        send(client(b, sender), "for-a1", "rgA_a1", null);
        assertNotNull(c.receive(3000), "with peer_identity_mode=off the certificate's name alone is the identity, as before this change");
    }

    @Test
    void aBridgeClaimWithoutACertificateIsNotExemptUnlessTheMigrationFlagSaysSo() throws Exception {
        for (boolean allow : new boolean[]{false, true}) {
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("broker_security_enabled", "true");
            cfg.put("broker_allow_unverified_bridges", String.valueOf(allow));
            PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz").toString(), cfg);
            CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
            BrokerService b = TlsTestSupport.broker("rgA_gc" + allow, global, false, new BrokerPlugin[]{authz}, authz, ca.caCert());
            brokers.add(b);
            ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
            f.setKeyAndTrustManagers(null, TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());   // no client certificate
            ActiveMQConnection conn = (ActiveMQConnection) f.createConnection();
            connections.add(conn);
            conn.start();
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            BrokerInfo claim = new BrokerInfo();
            claim.setBrokerId(new BrokerId("fake-" + System.nanoTime()));
            claim.setBrokerName("rgB_rc");
            claim.setNetworkConnection(true);
            conn.syncSendPacket(claim);
            Thread.sleep(200);
            // without the migration flag the claim earns nothing: no exemption, no identity (it reads nothing)
            MessageConsumer inbox = s.createConsumer(s.createQueue("rgA_a1"));
            ActiveMQConnection local = (ActiveMQConnection) new org.apache.activemq.ActiveMQConnectionFactory("vm://rgA_gc" + allow + "?create=false").createConnection();
            connections.add(local);
            local.start();
            Session ls = local.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ls.createProducer(ls.createQueue("rgA_a1")).send(ls.createTextMessage("for-a1"));
            boolean read = inbox.receive(1500) != null;
            assertEquals(allow, read, "a certificate-less bridge claim with broker_allow_unverified_bridges=" + allow);
        }
    }

    @Test
    void clientCertificatesAreRequiredByDefaultWhenBrokerSecurityIsOn() throws Exception {
        java.util.function.Function<Map<String, Object>, Boolean> req = m -> ActiveBroker.requireClientAuth(
                TestAgentService.plugin(System.getProperty("java.io.tmpdir"), m).getConfig());
        assertFalse(req.apply(Map.of()));
        assertTrue(req.apply(Map.of("broker_security_enabled", "true")));
        assertFalse(req.apply(Map.of("broker_security_enabled", "true", "broker_require_client_auth", "false")));
    }

    /** A bridge from {@code local} into the global, presenting {@code id}, as ActiveBroker builds one. */
    private NetworkConnector bridge(BrokerService local, TlsTestSupport.Id id) throws Exception {
        NetworkConnector nc = local.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(g)
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("test-bridge-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(new SslContext(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caB.caCert()), new SecureRandom()));
        nc.start();
        return nc;
    }

    /** Did {@code accepting} admit a broker-bridge connection (one that passed addConnection) within {@code ms}? */
    private static boolean bridgeAdmitted(BrokerService accepting, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < end) {
            for (org.apache.activemq.broker.Connection c : accepting.getRegionBroker().getClients()) {
                if (c.isNetworkConnection()) return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    @Test
    void aBridgeFromALeafAgentIsRefused() throws Exception {
        // the stray side runs no Cresco authorization (an attacker's broker would not): the global must refuse alone
        BrokerService stray = TlsTestSupport.broker("rgA_a9", strayAgent, false, null, null, ca.caCert(), caB.caCert());
        brokers.add(stray);
        bridge(stray, strayAgent);
        assertTrue(!bridgeAdmitted(g, 4000), "the global accepted a broker bridge from a leaf agent's certificate");
    }

    @Test
    void aBridgeFromADiscoveredControllerCarriesTheStreamToItsRecipientOnly() throws Exception {
        // the region discovered the global too (its GLOBAL discovery exchange records it): the duplex bridge's reverse
        // half arrives on the region's broker carrying the global's certificate
        peers.allowBridgePeer("rgA", "gc");
        BrokerService region = securedBroker("rgB_rc", regionCtl);
        bridge(region, regionCtl);
        assertTrue(bridgeAdmitted(g, 10000), "the bridge from the discovered region controller did not come up");

        Session rx = client(g, a1);
        MessageConsumer mine = rx.createConsumer(rx.createTopic(TOPIC));
        Session spy = client(g, a2);
        MessageConsumer wide = spy.createConsumer(spy.createTopic(TOPIC));
        Thread.sleep(1500);                                                           // demand crosses the bridge

        TlsTestSupport.Id remoteSender = TlsTestSupport.issued(caB, "t1", "rgB", "s2");
        send(client(region, remoteSender), "for-a1-via-bridge", "rgA_a1", null);
        assertEquals(List.of("for-a1-via-bridge"), drain(mine, 8000), "the recipient did not receive its stream across the bridge");
        assertTrue(drain(wide, 1000).isEmpty(), "a non-recipient received the stream across the bridge");
    }
}
