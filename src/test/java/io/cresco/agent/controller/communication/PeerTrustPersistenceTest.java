package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Identity bindings, CA scopes and bridge peers survive a restart with integrity; one key per name; a CA vouches only for
 * its own region; the node's own key and region CA persist.
 */
class PeerTrustPersistenceTest {

    private static Path file() throws Exception {
        return Files.createTempDirectory("ptr").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f, List<String> rebind) {
        return PeerTrustRegistry.open(f, rebind, List.of(), 30, m -> { });
    }

    @Test
    void aBoundNameRefusesASecondKeyAfterARestartUnlessTheOperatorRebindsIt() throws Exception {
        Path f = file();
        KeyPair k1 = TlsTestSupport.rsa(), k2 = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(f, List.of());
        assertTrue(r.bind("t1", "rgA", "a1", k1.getPublic(), "test").ok);
        PeerTrustRegistry restarted = open(f, List.of());
        assertTrue(restarted.bind("t1", "rgA", "a1", k1.getPublic(), "test").ok, "the same key is still welcome");
        assertFalse(restarted.bind("t1", "rgA", "a1", k2.getPublic(), "test").ok, "a second key took a bound name after a restart");
        assertFalse(restarted.bind("t2", "rgA", "a1", k1.getPublic(), "test").ok, "a bound name moved to another tenant");
        PeerTrustRegistry rebinding = open(f, List.of("rgA/a1"));
        assertTrue(rebinding.bind("t1", "rgA", "a1", k2.getPublic(), "test").ok, "the operator's rebind was not honoured");
        assertFalse(open(f, List.of()).bind("t1", "rgA", "a1", k1.getPublic(), "test").ok, "the old key came back after a rebind");
    }

    @Test
    void aTamperedRegistryIsSetAsideAndNothingNewIsBound() throws Exception {
        Path f = file();
        PeerTrustRegistry r = open(f, List.of());
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "test").ok);
        String json = Files.readString(f);
        Files.writeString(f, json.replace("rgA/a1", "rgA/a2"));                      // edit without the MAC key
        PeerTrustRegistry after = open(f, List.of());
        assertTrue(after.isSealed());
        assertFalse(after.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "test").ok);
        try (var s = Files.list(f.getParent())) {
            assertTrue(s.anyMatch(p -> p.getFileName().toString().startsWith("peer-trust.json.rejected-")));
        }
    }

    @Test
    void bridgePeersPersistAndCanBeRevokedOrPruned() throws Exception {
        Path f = file();
        CrescoIdentity rc = CrescoIdentity.of("t1", "rgB", "rc", null), rd = CrescoIdentity.of("t1", "rgD", "rc", null);
        PeerTrustRegistry r = open(f, List.of());
        r.allowBridgePeer("rgB", "rc");
        r.allowBridgePeer("rgC", "rc");
        r.allowBridgePeer("rgD", "rc");
        assertTrue(open(f, List.of()).isBridgePeer(rc), "a global restart forgot its regions");
        r.backdateBridgePeer("rgD", "rc", 31L * 24 * 3600 * 1000);
        PeerTrustRegistry revoking = PeerTrustRegistry.open(f, List.of(), List.of("rgC/rc"), 30, m -> { });
        assertFalse(revoking.isBridgePeer(CrescoIdentity.of("t1", "rgC", "rc", null)), "a revoked bridge peer was kept");
        assertFalse(revoking.isBridgePeer(rd), "a bridge peer unseen for 31 days was kept");
        assertTrue(revoking.isBridgePeer(rc));
        revoking.allowBridgePeer("rgC", "rc");
        assertFalse(revoking.isBridgePeer(CrescoIdentity.of("t1", "rgC", "rc", null)), "a revoked bridge peer came back");
    }

    @Test
    void aCaVouchesOnlyForItsOwnRegionAndTenant() throws Exception {
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry r = new PeerTrustRegistry();
        assertNull(r.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertNotNull(r.trustCa(caA.caCert(), "rgB", "t1", "test"), "a CA was recorded for a region it does not name");
        assertNotNull(r.trustCa(RegionCA.generate("t1", "rgA", 2048, 1).caCert(), "rgA", "t1", "test"), "a second CA took region rgA");
        assertNotNull(r.verifiedIdentity(TlsTestSupport.issued(caA, "t1", "rgA", "a1").chain));
        assertNull(r.verifiedIdentity(TlsTestSupport.issued(caA, "t1", "rgB", "rc").chain), "rgA's CA vouched for a name in rgB");
        assertNull(r.verifiedIdentity(TlsTestSupport.issued(caA, "t2", "rgA", "a1").chain), "rgA's CA vouched for another tenant");
        assertNull(r.verifiedIdentity(TlsTestSupport.selfRooted("x", "t1", "rgA", "a1").chain), "a self-rooted chain was trusted by name");
    }

    @Test
    void theNodeKeyAndRegionCaSurviveARestartAndSoDoTheCasNameBindings() throws Exception {
        Path dir = Files.createTempDirectory("node");
        Map<String, Object> cfg = Map.of("security_regional_ca", "true", "node_identity_dir", dir.toString());
        ControllerEngine first = TlsTestSupport.engine("rgA", "rc", cfg);
        first.getCertificateManager().ensureRegionCA();
        X509Certificate ca1 = first.getCertificateManager().getPublicCertificate()[1];
        java.security.PublicKey key1 = first.getCertificateManager().getPublicCertificate()[0].getPublicKey();
        ControllerEngine joinerA = TlsTestSupport.engine("rgA", "a1", null);
        assertNotNull(first.getCertificateManager().issueEnrollment(
                joinerA.getCertificateManager().getJsonFromCerts(joinerA.getCertificateManager().getPublicCertificate()), DiscoveryType.AGENT));

        ControllerEngine restarted = TlsTestSupport.engine("rgA", "rc", cfg);           // same identity directory
        restarted.getCertificateManager().ensureRegionCA();
        assertEquals(key1, restarted.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "the node key changed across a restart");
        assertEquals(ca1, restarted.getCertificateManager().getPublicCertificate()[1], "the region CA changed across a restart");
        ControllerEngine otherKeyA1 = TlsTestSupport.engine("rgA", "a1", null);
        assertNull(restarted.getCertificateManager().issueEnrollment(
                otherKeyA1.getCertificateManager().getJsonFromCerts(otherKeyA1.getCertificateManager().getPublicCertificate()), DiscoveryType.AGENT),
                "after a restart the CA issued rgA/a1 to a second key");
        assertNotNull(restarted.getCertificateManager().issueEnrollment(
                joinerA.getCertificateManager().getJsonFromCerts(joinerA.getCertificateManager().getPublicCertificate()), DiscoveryType.AGENT),
                "the original key of rgA/a1 was refused after a restart");
    }
}
