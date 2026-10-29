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
    void underTheRegionalCaTheIssuerStampsTheIdentityInstead() throws Exception {
        ControllerEngine agent = TlsTestSupport.engine("region-start-uuid", "a1", Map.of("security_regional_ca", "true"));
        ((TlsTestSupport.State) agent.cstate).region = "rgA";
        assertFalse(agent.getCertificateManager().ensureLeafIdentity());
        assertEquals("region-start-uuid", leafOf(agent).getRegion());
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

        ControllerEngine peerRegion = TlsTestSupport.engine("rgB", "rc", null);
        String peer = issue(regionCtl, peerRegion, DiscoveryType.GLOBAL);
        assertNotNull(peer);
        assertEquals("rgB", CrescoIdentity.fromCertificate(regionCtl.getCertificateManager().getCertsfromJson(peer)[0]).getRegion());
    }
}
