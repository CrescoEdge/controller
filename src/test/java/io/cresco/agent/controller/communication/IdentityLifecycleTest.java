package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2 identity lifecycle: revocation (operator, signed lists distributed parent/child, refused at TLS, at the broker and
 * on live connections), expiry refused at authorization, short-lived region-issued leaves, and renewal (round-4 M4).
 */
class IdentityLifecycleTest {

    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("lifecycle").resolve("security").resolve("peer-trust.json");
    }

    @Test
    void aRevokedIdentityIsRefusedEverywhereAndStaysRevoked() throws Exception {
        TlsTestSupport.provider();
        Path f = newFile();
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        assertNotNull(r.verifiedIdentity(a1.chain));
        List<String> fired = new ArrayList<>();
        r.setRevokedListener(fired::add);
        assertNull(r.revoke("rgA/a1", "test"));
        String fp = PeerTrustRegistry.fingerprint(a1.kp.getPublic().getEncoded());
        assertEquals(List.of(fp), fired, "the revocation was not announced");
        assertNull(r.verifiedIdentity(a1.chain), "a revoked identity verifies at the broker");
        assertNull(r.boundByKey(a1.chain), "a revoked key is trusted by its binding at TLS");
        assertNotNull(r.refusedKey(a1.chain), "a revoked key is not refused at TLS");
        assertFalse(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok, "a revoked key was bound again");
        assertFalse(r.bind("t1", "rgA", "other", a1.kp.getPublic(), "discovery").ok, "a revoked key was bound to another name");
        PeerTrustRegistry reboot = PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> { });
        assertTrue(reboot.isRevokedKey(fp), "a revocation did not survive a restart");
        assertNull(reboot.verifiedIdentity(a1.chain));
        assertNotNull(r.revoke("rgA/nobody", "test"), "revoking an unbound name succeeded silently");
    }

    @Test
    void theOperatorRevokesByFileOrSettingWithoutARestartOfAnythingElse() throws Exception {
        TlsTestSupport.provider();
        Path f = newFile();
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1"), a2 = TlsTestSupport.selfRooted("a2", "t1", "rgA", "a2");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> { });
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgA", "a2", a2.kp.getPublic(), "discovery").ok);
        Files.writeString(f.resolveSibling("peer-trust.json.revoke"), "# stolen laptop\nrgA/a1\n");
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.revokeIdentities = List.of(PeerTrustRegistry.fingerprint(a2.kp.getPublic().getEncoded()));
        PeerTrustRegistry reboot = PeerTrustRegistry.open(f, o, m -> { });
        assertNull(reboot.verifiedIdentity(a1.chain), "the operator file's revocation was not applied");
        assertNull(reboot.verifiedIdentity(a2.chain), "peer_identity_revoke was not applied");
    }

    @Test
    void anExpiredCertificateIsRefusedAtAuthorization() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry r = new PeerTrustRegistry();
        assertNull(r.trustCa(ca.caCert(), "rgA", "t1", "test"));
        java.security.KeyPair kp = TlsTestSupport.rsa();
        X509Certificate[] expired = ca.enrollFor(kp.getPublic(), "t1", "rgA", "a9", -60_000L);   // expired a minute ago
        String[] why = new String[1];
        assertNull(r.verifiedIdentity(expired, why), "an expired certificate verified");
        assertTrue(why[0].contains("not valid now"), why[0]);
        X509Certificate[] fresh = ca.enrollFor(kp.getPublic(), "t1", "rgA", "a9", 3_600_000L);
        assertEquals(CrescoIdentity.of("t1", "rgA", "a9", null), r.verifiedIdentity(fresh));
        assertTrue(fresh[0].getNotAfter().getTime() - System.currentTimeMillis() <= 3_600_000L, "the leaf lives longer than asked");
    }

    @Test
    void aConnectionHoldingARevokedKeyIsClosedAndCannotComeBack() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        TlsTestSupport.Id a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        assertTrue(peers.bind("t1", "rgA", "a1", a1.kp.getPublic(), "enrolled").ok);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("rv").toString(), cfg), peers);
        peers.setRevokedListener(authz::closeConnectionsOfKey);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(ca, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(a1.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
        connections.add(c);
        c.start();
        c.createSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE).createConsumer(new org.apache.activemq.command.ActiveMQQueue("rgA_a1"));
        assertNull(peers.revoke("rgA/a1", "test"));
        long end = System.currentTimeMillis() + 5000;
        while (!c.isTransportFailed() && !c.isClosed() && System.currentTimeMillis() < end) Thread.sleep(50);
        assertTrue(c.isTransportFailed() || c.isClosed(), "the connection of a revoked key stayed up");
        // and it cannot come back: the broker no longer identifies the key, so it is quarantined (and TLS refuses it)
        ActiveMQConnection again = (ActiveMQConnection) f.createConnection();
        connections.add(again);
        boolean usable;
        try {
            again.start();
            again.createSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE).createConsumer(new org.apache.activemq.command.ActiveMQQueue("rgA_a1"));
            usable = true;
        } catch (Exception e) {
            usable = false;
        }
        assertFalse(usable, "a revoked key reconnected and subscribed to its inbox");
    }

    // ------------------------------------------------------------------ signed revocation lists

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static void bindAsController(ControllerEngine at, ControllerEngine who) {
        X509Certificate leaf = who.getCertificateManager().getPublicCertificate()[0];
        assertNull(at.getCertificateManager().getPeerTrust().pin(who.cstate.getRegion(), who.cstate.getAgent(), leaf,
                PeerTrustRegistry.ROLE_CONTROLLER, false));
    }

    @Test
    void aRegionsSignedListRevokesOnlyItsOwnNamesAtTheGlobalAndTheGlobalsListReachesTheRegion() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), region = node("rgB", "rc", true);
        bindAsController(global, region);
        bindAsController(region, global);
        PeerTrustRegistry atGlobal = global.getCertificateManager().getPeerTrust(), atRegion = region.getCertificateManager().getPeerTrust();
        TlsTestSupport.Id b1 = TlsTestSupport.selfRooted("b1", "t1", "rgB", "b1"), g1 = TlsTestSupport.selfRooted("g1", "t1", "rgG", "g1");
        assertTrue(atGlobal.bind("t1", "rgB", "b1", b1.kp.getPublic(), "test").ok);
        assertTrue(atGlobal.bind("t1", "rgG", "g1", g1.kp.getPublic(), "test").ok);
        assertTrue(atRegion.bind("t1", "rgB", "b1", b1.kp.getPublic(), "discovery").ok);
        assertTrue(atRegion.bind("t1", "rgG", "g1", g1.kp.getPublic(), "test").ok);
        // the region revokes its agent b1 -- and (maliciously) the global's own g1 too
        assertNull(atRegion.revoke("rgB/b1", "operator"));
        assertNull(atRegion.revoke("rgG/g1", "operator"));
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc", null, "rgB", "rc", null, true, true);
        ping.setParam("action", "ping");
        RevocationSync.onChildPing(region, ping);
        assertNotNull(ping.getParam(RevocationSync.CHILD_LIST), "the region did not send its list up");
        RevocationSync.onPingAtParent(global, ping, true);
        assertNull(atGlobal.verifiedIdentity(b1.chain), "the region's revocation of its own agent was not applied at the global");
        assertNotNull(atGlobal.verifiedIdentity(g1.chain), "a region revoked a name of another region at the global");
        assertNotNull(ping.getParam(RevocationSync.ACK), "the global did not acknowledge the region's list");
        RevocationSync.onParentPong(region, ping, "rgG", "gc");
        // the merge is recorded off the ping path; only what is durable is served down
        atGlobal.awaitRevocations();
        // the pong carries the global's list down only when the region holds less of it
        MsgEvent ping2 = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc", null, "rgB", "rc", null, true, true);
        RevocationSync.onChildPing(region, ping2);
        assertNull(ping2.getParam(RevocationSync.CHILD_LIST), "an acknowledged list was sent again");
        RevocationSync.onPingAtParent(global, ping2, true);
        assertNotNull(ping2.getParam(RevocationSync.PARENT_LIST), "the global's list did not come down");
        RevocationSync.onParentPong(region, ping2, "rgG", "gc");
        MsgEvent ping3 = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc", null, "rgB", "rc", null, true, true);
        RevocationSync.onChildPing(region, ping3);
        RevocationSync.onPingAtParent(global, ping3, true);
        assertNull(ping3.getParam(RevocationSync.PARENT_LIST), "a list the region holds was sent again");
    }

    @Test
    void aListFromAnyoneButTheExpectedPeerOrWithABadSignatureIsRefused() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), region = node("rgB", "rc", true), impostor = node("rgB", "rc", true);
        bindAsController(global, region);
        TlsTestSupport.Id b1 = TlsTestSupport.selfRooted("b1", "t1", "rgB", "b1");
        assertTrue(global.getCertificateManager().getPeerTrust().bind("t1", "rgB", "b1", b1.kp.getPublic(), "test").ok);
        assertTrue(impostor.getCertificateManager().getPeerTrust().bind("t1", "rgB", "b1", b1.kp.getPublic(), "test").ok);
        assertNull(impostor.getCertificateManager().getPeerTrust().revoke("rgB/b1", "impostor"));
        String forged = RevocationSync.signOwn(impostor);                 // signed with a key not bound to rgB/rc at the global
        String[] why = new String[1];
        assertNull(RevocationSync.verify(global, forged, "rgB", "rc", why), "a list signed by an unbound key was accepted");
        assertNotNull(global.getCertificateManager().getPeerTrust().verifiedIdentity(b1.chain));
        // a genuine list whose content was changed after signing
        assertTrue(region.getCertificateManager().getPeerTrust().bind("t1", "rgB", "b1", b1.kp.getPublic(), "test").ok);
        assertNull(region.getCertificateManager().getPeerTrust().revoke("rgB/b1", "operator"));
        String genuine = RevocationSync.signOwn(region);
        assertNotNull(RevocationSync.verify(global, genuine, "rgB", "rc", why), why[0]);
        String altered = genuine.replace("\"issued\":", "\"issued\":1");
        assertNull(RevocationSync.verify(global, altered, "rgB", "rc", why), "an altered list verified");
        assertNull(RevocationSync.verify(global, genuine, "rgC", "rc", why), "a list was accepted for another issuer");
    }

    // ------------------------------------------------------------------ short-lived leaves and renewal

    @Test
    void aRegionCaIssuesShortLivedLeaves() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true", "identity_leaf_validity_hours", "24"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        region.getCertificateManager().ensureRegionCA();
        X509Certificate own = region.getCertificateManager().getPublicCertificate()[0];
        long left = own.getNotAfter().getTime() - System.currentTimeMillis();
        assertTrue(left <= 24 * 3600_000L && left > 23 * 3600_000L, "the controller's own leaf is not short-lived: " + own.getNotAfter());
        assertEquals(24 * 3600_000L, region.getCertificateManager().leafValidityMs());
    }

    /** M4: a region CA renewed with the same key keeps every leaf it issued valid, and its recorded scope. */
    @Test
    void aRenewedRegionCaKeepsItsKeyAndItsLeaves() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id leaf = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        RegionCA renewed = ca.renewed(2048, 3);
        assertEquals(ca.caCert().getPublicKey(), renewed.caCert().getPublicKey());
        assertEquals(ca.caCert().getSubjectX500Principal(), renewed.caCert().getSubjectX500Principal());
        assertTrue(renewed.caCert().getNotAfter().after(ca.caCert().getNotAfter()));
        leaf.chain[0].verify(renewed.caCert().getPublicKey());
        assertNull(peers.trustCa(renewed.caCert(), "rgA", "t1", "test"), "the renewed CA is not the recorded one");
        TlsTestSupport.Id fresh = TlsTestSupport.issued(renewed, "t1", "rgA", "a2");
        assertEquals(CrescoIdentity.of("t1", "rgA", "a2", null), peers.verifiedIdentity(fresh.chain));
    }

    /**
     * M4: renewal is due when a third of the lifetime (or the floor, but never more than half the lifetime) is left, or the
     * certificate expired. (Before the review of 2026-11-04 the 30-day floor applied to any lifetime, so a short-lived leaf
     * was due at birth; an 11-day certificate is now due at half-life, not with 10 days left.)
     */
    @Test
    void renewalIsDueAtAThirdOfTheLifetime() {
        long now = System.currentTimeMillis(), day = 24L * 3600 * 1000;
        assertFalse(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - day, now + 900 * day)}, now, 30 * day));
        assertTrue(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - 800 * day, now + 300 * day)}, now, 30 * day));
        assertFalse(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - day, now + 10 * day)}, now, 30 * day),
                "an 11-day certificate with 10 days left is not due (the floor is capped at half the lifetime)");
        assertTrue(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - 6 * day, now + 5 * day)}, now, 30 * day));
        assertTrue(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - 900 * day, now - day)}, now, 30 * day));
        assertTrue(CertificateManager.chainNeedsRenewal(new X509Certificate[]{fake(now - 3 * day, now + day)}, now, 0), "a short-lived leaf");
    }

    private static X509Certificate fake(long notBefore, long notAfter) {
        try {
            TlsTestSupport.provider();
            java.security.KeyPair kp = TlsTestSupport.rsa();
            org.bouncycastle.asn1.x500.X500Name n = new org.bouncycastle.asn1.x500.X500Name("CN=x");
            return new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(
                    new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(n, java.math.BigInteger.ONE, new Date(notBefore), new Date(notAfter), n, kp.getPublic())
                            .build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(kp.getPrivate())));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
