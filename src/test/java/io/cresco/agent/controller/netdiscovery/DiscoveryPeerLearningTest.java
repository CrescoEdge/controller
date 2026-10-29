package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What a certified discovery exchange teaches a node about its peer (pinned key, bridge peer). */
class DiscoveryPeerLearningTest {

    private static final String SECRET = "test-secret-not-real";

    private static DiscoveryNode certifiedResponse(DiscoveryType type, ControllerEngine responder, DiscoveryCrypto crypto) {
        DiscoveryNode n = new DiscoveryNode(type);
        String certs = responder.getCertificateManager().getJsonFromCerts(responder.getCertificateManager().getPublicCertificate());
        n.setCertified("127.0.0.1", 32005, "127.0.0.1", 40000, responder.cstate.getRegion(), responder.cstate.getAgent(), 0,
                crypto.encrypt("DISCOVERY_MESSAGE_VALIDATED", SECRET), certs);
        return n;
    }

    @Test
    void theClientPinsTheResponderAndRecordsControllersAsBridgePeers() throws Exception {
        ControllerEngine me = TlsTestSupport.engine("rgB", "rc", Map.of("discovery_secret_global", SECRET, "discovery_secret_agent", SECRET));
        ControllerEngine global = TlsTestSupport.engine("rgG", "gc", null);
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", null);
        DiscoveryProcessor dp = new DiscoveryProcessor(me);
        DiscoveryCrypto crypto = new DiscoveryCrypto(me);
        PeerTrustRegistry reg = me.getCertificateManager().getPeerTrust();

        assertTrue(dp.setCertTrust(certifiedResponse(DiscoveryType.GLOBAL, global, crypto)));
        assertNotNull(reg.pinnedKey(new PeerIdentity.Expected("rgG", "gc")));
        assertTrue(reg.isBridgePeer(CrescoIdentity.of("t1", "rgG", "gc", null)));

        assertTrue(dp.setCertTrust(certifiedResponse(DiscoveryType.AGENT, region, crypto)));
        assertNotNull(reg.pinnedKey(new PeerIdentity.Expected("rgA", "rc")));
        assertFalse(reg.isBridgePeer(CrescoIdentity.of("t1", "rgA", "rc", null)), "an agent-secret exchange must not make a bridge peer");
    }

    @Test
    void theResponderRecordsARegionJoinerAsABridgePeerButNotAnAgent() throws Exception {
        ControllerEngine me = TlsTestSupport.engine("rgG", "gc", Map.of("discovery_secret_region", SECRET, "discovery_secret_agent", SECRET));
        ((TlsTestSupport.State) me.cstate).regional = true;
        DiscoveryProcessor dp = new DiscoveryProcessor(me);
        DiscoveryCrypto crypto = new DiscoveryCrypto(me);
        PeerTrustRegistry reg = me.getCertificateManager().getPeerTrust();

        ControllerEngine regionCtl = TlsTestSupport.engine("rgB", "rc", null);
        DiscoveryNode join = new DiscoveryNode(DiscoveryType.REGION);
        join.setCertify(crypto.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET),
                regionCtl.getCertificateManager().getJsonFromCerts(regionCtl.getCertificateManager().getPublicCertificate()), "rgB", "rc");
        assertNotNull(dp.processIncomingBroadCastDiscovery(join, "127.0.0.1", 32005, "127.0.0.1", 40001));
        assertTrue(reg.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));

        ControllerEngine agent = TlsTestSupport.engine("rgG", "a1", null);
        DiscoveryNode agentJoin = new DiscoveryNode(DiscoveryType.AGENT);
        agentJoin.setCertify(crypto.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET),
                agent.getCertificateManager().getJsonFromCerts(agent.getCertificateManager().getPublicCertificate()), "rgG", "a1");
        assertNotNull(dp.processIncomingBroadCastDiscovery(agentJoin, "127.0.0.1", 32005, "127.0.0.1", 40002));
        assertFalse(reg.isBridgePeer(CrescoIdentity.of("t1", "rgG", "a1", null)));
        assertNull(reg.pinnedKey(new PeerIdentity.Expected("rgX", "nobody")));
    }
}
