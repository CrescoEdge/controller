package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryCrypto;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.security.CrescoIdentity;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Adversarial review of fix/peer-identity. Each test asserts the SECURE outcome; a failure is a finding. */
class AdversarialPeerIdentityReviewTest {

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) brokers.get(i).stop();
    }

    // ---------- helpers ----------

    /** A self-signed joiner certificate whose subject attributes are exactly these UTF8 strings. */
    private static X509Certificate[] joinerCert(String tenant, String region, String agent) throws Exception {
        KeyPair kp = TlsTestSupport.rsa();
        X500NameBuilder nb = new X500NameBuilder(BCStyle.INSTANCE);
        if (agent != null) nb.addRDN(BCStyle.CN, new DERUTF8String(agent));
        if (region != null) nb.addRDN(BCStyle.OU, new DERUTF8String(region));
        if (tenant != null) nb.addRDN(BCStyle.O, new DERUTF8String(tenant));
        X500Name n = nb.build();
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(n, new BigInteger(64, new SecureRandom()),
                new Date(now - 60_000L), new Date(now + 86_400_000L), n, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        X509Certificate c = new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(kp.getPrivate())));
        return new X509Certificate[]{c};
    }

    private static CrescoIdentity issue(ControllerEngine issuer, X509Certificate[] joiner, DiscoveryType type, X509Certificate[][] out) {
        CertificateManager cm = issuer.getCertificateManager();
        String json = cm.issueEnrollment(cm.getJsonFromCerts(joiner), type);
        if (json == null) return null;
        X509Certificate[] chain = cm.getCertsfromJson(json);
        if (out != null) out[0] = chain;
        return CrescoIdentity.fromCertificate(chain[0]);
    }

    // ---------- 1. regional CA: the "never issue our own identity / region" guards are canonicalization-bypassable ----------

    @Test
    void regionalCaIssuesItsOwnIdentityToAnAgentClaimingASpacePaddedName() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        X509Certificate[][] out = new X509Certificate[1][];
        CrescoIdentity got = issue(regionCtl, joinerCert("t1", "whatever", " rc"), DiscoveryType.AGENT, out);
        // secure outcome: refused, or at least not the controller's own identity
        assertTrue(got == null || !("rgA".equals(got.getRegion()) && "rc".equals(got.getAgent())),
                "the regional CA issued ITS OWN identity rgA/rc to a joining agent (claimed CN=' rc'): issued " + got
                        + " -> PeerIdentity accepts it for a connection to rgA/rc: "
                        + (out[0] == null ? "-" : PeerIdentity.verify(new PeerIdentity.Expected("rgA", "rc"), out[0], null).ok));
    }

    @Test
    void regionalCaIssuesItsOwnIdentityToAnAgentClaimingAHexEncodedName() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        // "#0c027263" = DER UTF8String "rc"; CrescoIdentity.toX500Name does not escape '#', BouncyCastle decodes it
        CrescoIdentity got = issue(regionCtl, joinerCert("t1", "whatever", "#0c027263"), DiscoveryType.AGENT, null);
        assertTrue(got == null || !("rgA".equals(got.getRegion()) && "rc".equals(got.getAgent())),
                "the regional CA issued ITS OWN identity rgA/rc to a joining agent (claimed CN='#0c027263'): issued " + got);
    }

    @Test
    void globalCaGivesAJoiningRegionTheGlobalsOwnIdentity() throws Exception {
        ControllerEngine global = TlsTestSupport.engine("glob", "gc", Map.of("security_regional_ca", "true"));
        CrescoIdentity got = issue(global, joinerCert("t1", " glob", "gc"), DiscoveryType.GLOBAL, null);
        assertTrue(got == null || !("glob".equals(got.getRegion())),
                "the CA gave a joining GLOBAL-type controller its own region name (claimed OU=' glob'): issued " + got);
    }

    @Test
    void regionalCaIssuesAnExistingAgentsIdentityToASecondKey() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        X509Certificate[][] first = new X509Certificate[1][], second = new X509Certificate[1][];
        CrescoIdentity a = issue(regionCtl, joinerCert("t1", "x", "a1"), DiscoveryType.AGENT, first);
        CrescoIdentity b = issue(regionCtl, joinerCert("t1", "y", "a1"), DiscoveryType.AGENT, second);
        assertNotNull(a);
        assertFalse(b != null && b.getAgentPath().equals(a.getAgentPath())
                        && !first[0][0].getPublicKey().equals(second[0][0].getPublicKey()),
                "the regional CA issued identity " + a.getAgentPath() + " to two different keys (no uniqueness check)");
    }

    // ---------- 2. per-node (legacy) trust: broker-side identity is name-only, any trusted peer root can mint ----------

    private static final String TOPIC = "global.event";

    private BrokerService securedBroker(String name, TlsTestSupport.Id id, PeerTrustRegistry peers, X509Certificate... anchors) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker(name, id, true, new BrokerPlugin[]{authz}, authz, anchors);
        brokers.add(b);
        return b;
    }

    private Session client(BrokerService b, TlsTestSupport.Id id, X509Certificate trustBroker) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(trustBroker), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    void perNodeTrustAPeerRootMintsADiscoveredControllerAndEarnsTheBridgeExemption() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "glob", 2048, 1);
        TlsTestSupport.Id global = TlsTestSupport.issued(ca, "t1", "glob", "gc");
        // an agent of the global's region that did a certified discovery exchange: under per-node trust the global
        // added its self-generated root to the truststore. It keeps that root key and mints the region controller.
        TlsTestSupport.Id forged = TlsTestSupport.selfRooted("agent-evil", "t1", "rgr", "rc");
        PeerTrustRegistry peers = new PeerTrustRegistry();
        peers.allowBridgePeer("rgr", "rc");                  // the real region controller did GLOBAL discovery
        BrokerService g = securedBroker("glob_gc", global, peers, ca.caCert(), forged.chain[1]);

        Session s = client(g, forged, ca.caCert());
        boolean pre; try { s.createConsumer(s.createQueue("glob_a1")); pre = true; } catch (Exception e) { pre = false; }
        assertFalse(pre, "control: without the bridge claim the forged identity is NOT allowed to read glob_a1");
        ActiveMQConnection conn = (ActiveMQConnection) connections.get(connections.size() - 1);
        BrokerInfo claim = new BrokerInfo();
        claim.setBrokerId(new BrokerId("fake-" + System.nanoTime()));
        claim.setBrokerName("rgr_rc");
        claim.setNetworkConnection(true);
        conn.syncSendPacket(claim);
        Thread.sleep(200);
        boolean read;
        try { s.createConsumer(s.createQueue("glob_a1")); read = true; } catch (Exception e) { read = false; }
        assertFalse(read, "a leaf agent's self-rooted certificate naming the region controller earned the bridge exemption"
                + " and opened a consumer on another agent's inbox glob_a1");
    }

    @Test
    void perNodeTrustAPeerRootMintsAnotherAgentAndReceivesItsStream() throws Exception {
        RegionCA ca = RegionCA.generate("t1", "glob", 2048, 1);
        TlsTestSupport.Id global = TlsTestSupport.issued(ca, "t1", "glob", "gc");
        TlsTestSupport.Id sender = TlsTestSupport.issued(ca, "t1", "glob", "s1");
        TlsTestSupport.Id forgedA1 = TlsTestSupport.selfRooted("agent-evil", "t1", "glob", "a1");
        BrokerService g = securedBroker("glob_gc", global, new PeerTrustRegistry(), ca.caCert(), forgedA1.chain[1]);

        Session spy = client(g, forgedA1, ca.caCert());
        MessageConsumer c = spy.createConsumer(spy.createTopic(TOPIC));
        Session tx = client(g, sender, ca.caCert());
        MessageProducer p = tx.createProducer(tx.createTopic(TOPIC));
        TextMessage m = tx.createTextMessage("for-a1");
        m.setStringProperty(StreamRecipientPolicy.TO, "glob_a1");
        p.send(m);
        Message got = c.receive(3000);
        assertNull(got, "a peer minting CN=a1 under its own trusted root received a1's recipient-bound stream");
    }

    @Test
    void regionalCaSecondEnrolleeOfAnAgentNameReceivesThatAgentsStream() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        CertificateManager cm = regionCtl.getCertificateManager();
        cm.ensureRegionCA();
        X509Certificate[] own = cm.getPublicCertificate();                      // [leaf rgA/rc, regionCA, root]
        // attacker enrolls as "a1" (an agent name that exists or will exist in the region) with its own key
        KeyPair kp = TlsTestSupport.rsa();
        X509Certificate[] self = joinerCert("t1", "x", "a1");
        // re-sign self with kp so the issued chain matches a key we hold
        X500Name n = new X500NameBuilder(BCStyle.INSTANCE).addRDN(BCStyle.CN, new DERUTF8String("a1")).build();
        long now = System.currentTimeMillis();
        X509Certificate mine = new JcaX509CertificateConverter().getCertificate(new JcaX509v3CertificateBuilder(n,
                BigInteger.ONE, new Date(now - 60_000L), new Date(now + 86_400_000L), n, kp.getPublic())
                .build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(kp.getPrivate())));
        String json = cm.issueEnrollment(cm.getJsonFromCerts(new X509Certificate[]{mine}), DiscoveryType.AGENT);
        assertNotNull(json);
        X509Certificate[] issued = cm.getCertsfromJson(json);
        TlsTestSupport.Id attacker = new TlsTestSupport.Id(kp, issued);
        assertEquals("rgA_a1", CrescoIdentity.fromCertificate(issued[0]).getAgentPath());

        RegionCA other = RegionCA.generate("t1", "rgA", 2048, 1);                 // only to give the broker a cert
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(other, "t1", "rgA", "rc");
        TlsTestSupport.Id sender = TlsTestSupport.issued(other, "t1", "rgA", "s1");
        BrokerService g = securedBroker("rgA_rc", brokerId, new PeerTrustRegistry(), issued[1], other.caCert());
        Session spy = client(g, attacker, other.caCert());
        MessageConsumer c = spy.createConsumer(spy.createTopic(TOPIC));
        Session tx = client(g, sender, other.caCert());
        TextMessage m = tx.createTextMessage("for-a1");
        m.setStringProperty(StreamRecipientPolicy.TO, "rgA_a1");
        tx.createProducer(tx.createTopic(TOPIC)).send(m);
        assertNull(c.receive(3000), "a second enrollee of the name a1 received rgA_a1's recipient-bound stream");
        assertNotNull(own);
    }

    // ---------- 3. recipient path ambiguity ----------

    @Test
    void recipientPathIsAmbiguousWhenNamesContainTheSeparator() {
        // stream meant for region "rgA_x", agent "a1"; consumer is region "rgA", agent "x_a1"
        TenantPolicy.Decision d = StreamRecipientPolicy.mayReceive(CrescoIdentity.of("t1", "rgA", "x_a1", null), "rgA_x_a1", null);
        assertFalse(d.allowed, "agent rgA/x_a1 is accepted as the recipient of rgA_x/a1's stream (path collision)");
    }

    // ---------- 4. the discovery proof every new binding is learned over ----------

    @Test
    void discoveryValidatorIsADeterministicReplayableToken() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        DiscoveryCrypto dc = new DiscoveryCrypto(e);
        String a = dc.encrypt("DISCOVERY_MESSAGE_VERIFIED", "secret-global");
        String b = dc.encrypt("DISCOVERY_MESSAGE_VERIFIED", "secret-global");
        assertNotEquals(a, b, "the discovery 'authentication' is AES/ECB of a constant: one observed token "
                + a + " replays forever without the secret");
    }

    // ---------- 5. forget() on a failed duplicate ConnectionInfo clears the state of the still-live one ----------

    @Test
    void aDuplicateConnectionInfoErasesTheCertificateMarkAndRevivesTheUsernameFallback() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id global = TlsTestSupport.issued(ca, "t1", "rgA", "gc");
        TlsTestSupport.Id sender = TlsTestSupport.issued(ca, "t1", "rgA", "s1");
        KeyPair kp = TlsTestSupport.rsa();
        TlsTestSupport.Id anon = new TlsTestSupport.Id(kp, ca.issueLeaf(kp.getPublic(), CrescoIdentity.of(null, null, "nobody", null), 1));
        BrokerService g = securedBroker("rgA_gc", global, new PeerTrustRegistry(), ca.caCert());

        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(g) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(anon.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        f.setUserName("t1|rgA|a1");                                         // asserts the victim
        ActiveMQConnection conn = (ActiveMQConnection) f.createConnection();
        connections.add(conn);
        conn.start();
        Session spy = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c = spy.createConsumer(spy.createTopic(TOPIC));     // shared topic, no selector: allowed

        Session tx = client(g, sender, ca.caCert());
        TextMessage m0 = tx.createTextMessage("before");
        m0.setStringProperty(StreamRecipientPolicy.TO, "rgA_a1");
        tx.createProducer(tx.createTopic(TOPIC)).send(m0);
        assertNull(c.receive(1000), "control: the identity-less certificate is not a1");

        try { conn.syncSendPacket(conn.getConnectionInfo()); } catch (Exception expected) { }   // same id + clientId again
        TextMessage m1 = tx.createTextMessage("after");
        m1.setStringProperty(StreamRecipientPolicy.TO, "rgA_a1");
        tx.createProducer(tx.createTopic(TOPIC)).send(m1);
        assertNull(c.receive(1500), "after a duplicate ConnectionInfo, the identity-less certificate was treated as the"
                + " username it asserted (t1|rgA|a1) and received a1's recipient-bound stream");
    }
}
