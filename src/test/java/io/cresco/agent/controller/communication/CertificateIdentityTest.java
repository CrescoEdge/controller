package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The identity a node's certificate names must be the identity the fabric knows it by, or binding connections to
 * identities (PeerIdentity, recipient-bound streams, the broker's per-identity rules) misfires.
 */
class CertificateIdentityTest {

    private static CrescoIdentity leafOf(ControllerEngine e) {
        return CrescoIdentity.fromCertificate(e.getCertificateManager().getPublicCertificate()[0]);
    }

    @Test
    void anAgentThatJoinedAnotherRegionReissuesItsLeafForThatRegion() throws Exception {
        ControllerEngine agent = TlsTestSupport.engine("region-start-uuid", "a1", null);
        assertEquals("region-start-uuid", leafOf(agent).getRegion());
        ((TlsTestSupport.State) agent.cstate).region = "rgA";             // cstate.setAgentInit(discovered_region, ...)
        assertTrue(agent.getCertificateManager().ensureLeafIdentity());
        CrescoIdentity now = leafOf(agent);
        assertEquals("rgA", now.getRegion());
        assertEquals("a1", now.getAgent());
        assertEquals("t1", now.getTenant());
        assertFalse(agent.getCertificateManager().ensureLeafIdentity(), "an up-to-date leaf must not be re-issued");
    }

    @Test
    void theLeafIsReissuedWithTheSameKey() throws Exception {
        ControllerEngine agent = TlsTestSupport.engine("region-start-uuid", "a1", null);
        java.security.PublicKey before = agent.getCertificateManager().getPublicCertificate()[0].getPublicKey();
        ((TlsTestSupport.State) agent.cstate).region = "rgA";
        assertTrue(agent.getCertificateManager().ensureLeafIdentity());
        assertEquals(before, agent.getCertificateManager().getPublicCertificate()[0].getPublicKey(),
                "re-issuing the leaf for a new name changed the key the node's identity bindings follow");
    }

    @Test
    void aRegionCaHolderNeverReissuesItsOwnLeaf() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        regionCtl.getCertificateManager().ensureRegionCA();
        ((TlsTestSupport.State) regionCtl.cstate).region = "rgZ";
        assertFalse(regionCtl.getCertificateManager().ensureLeafIdentity());
        assertEquals("rgA", leafOf(regionCtl).getRegion());
    }

    @Test
    void namesOutsideTheGrammarAreNeverIssued() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        for (String bad : new String[]{" a1", "a1 ", "#0c026131", "a_1", "a1,OU=x", "a\u0000", ""}) {
            ControllerEngine joiner = TlsTestSupport.engine("rgA", "a1", null);
            java.security.KeyPair kp = TlsTestSupport.rsa();
            X509Certificate self = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(
                    new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(
                            new org.bouncycastle.asn1.x500.X500NameBuilder(org.bouncycastle.asn1.x500.style.BCStyle.INSTANCE)
                                    .addRDN(org.bouncycastle.asn1.x500.style.BCStyle.CN, new org.bouncycastle.asn1.DERUTF8String(bad))
                                    .addRDN(org.bouncycastle.asn1.x500.style.BCStyle.OU, new org.bouncycastle.asn1.DERUTF8String("rgA"))
                                    .addRDN(org.bouncycastle.asn1.x500.style.BCStyle.O, new org.bouncycastle.asn1.DERUTF8String("t1")).build(),
                            java.math.BigInteger.ONE, new java.util.Date(System.currentTimeMillis() - 60_000L),
                            new java.util.Date(System.currentTimeMillis() + 86_400_000L),
                            new org.bouncycastle.asn1.x500.X500NameBuilder(org.bouncycastle.asn1.x500.style.BCStyle.INSTANCE)
                                    .addRDN(org.bouncycastle.asn1.x500.style.BCStyle.CN, new org.bouncycastle.asn1.DERUTF8String(bad)).build(),
                            kp.getPublic()).build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA384withRSA")
                            .setProvider("BC").build(kp.getPrivate())));
            assertNull(regionCtl.getCertificateManager().issueEnrollment(
                    regionCtl.getCertificateManager().getJsonFromCerts(new X509Certificate[]{self}), DiscoveryType.AGENT),
                    "issued a certificate for the agent name '" + bad + "'");
            assertNotNull(joiner);
        }
    }

    private static String issue(ControllerEngine issuer, ControllerEngine joiner, DiscoveryType type) {
        CertificateManager cm = joiner.getCertificateManager();
        return issuer.getCertificateManager().issueEnrollment(cm.getJsonFromCerts(cm.getPublicCertificate()), type);
    }

    @Test
    void theRegionalCaStampsItsOwnRegionOnAnAgentAndRefusesItsOwnIdentity() throws Exception {
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));

        ControllerEngine wanderer = TlsTestSupport.engine("region-start-uuid", "a1", null);
        String issued = issue(regionCtl, wanderer, DiscoveryType.AGENT);
        assertNotNull(issued);
        X509Certificate[] chain = regionCtl.getCertificateManager().getCertsfromJson(issued);
        CrescoIdentity id = CrescoIdentity.fromCertificate(chain[0]);
        assertEquals("rgA", id.getRegion(), "an agent belongs to the region that enrolled it");
        assertEquals("a1", id.getAgent());

        ControllerEngine impostor = TlsTestSupport.engine("rgA", "rc", null);       // claims the controller's own name
        assertNull(issue(regionCtl, impostor, DiscoveryType.AGENT), "the CA issued its own identity to a joiner");

        ControllerEngine sameRegion = TlsTestSupport.engine("rgA", "rc2", null);    // a "region" taking our region name
        assertNull(issue(regionCtl, sameRegion, DiscoveryType.REGION));

        // a joining controller is not re-issued: the two region CAs record each other, each scoped to its own region
        ControllerEngine peerRegion = TlsTestSupport.engine("rgB", "rc", Map.of("security_regional_ca", "true"));
        peerRegion.getCertificateManager().ensureRegionCA();
        String peer = issue(regionCtl, peerRegion, DiscoveryType.GLOBAL);
        assertNotNull(peer);
        X509Certificate[] returned = regionCtl.getCertificateManager().getCertsfromJson(peer);
        assertEquals("rgA", PeerTrustRegistry.caScopeOf(returned[0])[0], "the CA returned to a federating controller is ours");
        assertTrue(peerRegion.getCertificateManager().installEnrollment(peer, "rgA"));
        assertEquals("rgB", leafOf(peerRegion).getRegion(), "the federating controller kept its own identity");
        // a joining controller without its own region CA is refused
        assertNull(issue(regionCtl, TlsTestSupport.engine("rgC", "rc", null), DiscoveryType.GLOBAL));
    }

    @Test
    void certificatesAreValidForPeersWhoseClocksAreBehind() throws Exception {
        ControllerEngine node = TlsTestSupport.engine("rgA", "a1", null);
        ControllerEngine regionCtl = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        regionCtl.getCertificateManager().ensureRegionCA();
        X509Certificate[] perNode = node.getCertificateManager().getPublicCertificate();
        X509Certificate[] caIssued = regionCtl.getCertificateManager().getPublicCertificate();
        for (long behind : new long[]{2_000L, 5 * 60_000L}) {
            java.util.Date peerClock = new java.util.Date(System.currentTimeMillis() - behind);
            for (X509Certificate c : perNode) c.checkValidity(peerClock);
            for (X509Certificate c : caIssued) c.checkValidity(peerClock);
        }
    }

    @Test
    void anAgentExchangeMayNotInstallACa() throws Exception {
        ControllerEngine rogue = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        rogue.getCertificateManager().ensureRegionCA();
        X509Certificate[] own = rogue.getCertificateManager().getPublicCertificate();      // [leaf, regionCA, root]
        String caBundle = rogue.getCertificateManager().getJsonFromCerts(new X509Certificate[]{own[1], own[2]});
        ControllerEngine joiner = TlsTestSupport.engine("rgA", "a1", Map.of("security_regional_ca", "true"));
        assertFalse(joiner.getCertificateManager().installEnrollment(caBundle, "rgA", DiscoveryType.AGENT),
                "an AGENT exchange installed the responder's CA as a federated peer");
        assertTrue(joiner.getCertificateManager().installEnrollment(caBundle, "rgA", DiscoveryType.REGION));
    }
}
