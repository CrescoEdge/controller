package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-2 review N1/P-b under the regional CA: controller_trust_anchors are checked on the RESPONDER's chain (a validated
 * path from its leaf to the anchor), and the identity it issues the joiner must come from a CA on that path. A responder
 * that copies the anchored controller's public chain cannot enroll joiners under a CA of its own.
 */
class AnchoredRegionalCaJoinTest {

    private static final String AGENT_SECRET = "agent-secret-every-agent-has";
    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", AGENT_SECRET);
        m.put("security_regional_ca", "true");
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "client");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    @Test
    void anImpostorCopyingTheAnchoredChainCannotEnrollUnderItsOwnCa() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true);
        rc.getCertificateManager().ensureRegionCA();
        X509Certificate[] real = rc.getCertificateManager().getPublicCertificate();
        String anchor = io.cresco.agent.controller.communication.PeerIdentity.spkiFingerprint(real[1]);   // the region CA's key
        ControllerEngine a1 = node("rgA", "a1", false, "controller_trust_anchors", anchor);
        assertNotNull(join(a1, rc), "the anchored region's own controller was refused");
        // an agent-secret holder with a CA of its own for rgA presents the real controller's (public) chain
        ControllerEngine impostor = node("rgA", "rc", true);
        impostor.getCertificateManager().ensureRegionCA();
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("chain");
        f.setAccessible(true);
        f.set(impostor.getCertificateManager(), real.clone());
        ControllerEngine a2 = node("rgA", "a2", false, "controller_trust_anchors", anchor);
        assertNull(join(a2, impostor), "a responder that copied the anchored chain enrolled a joiner under its own CA");
        assertNotEquals(impostor.getCertificateManager().getPublicCertificate()[1], a2.getCertificateManager().getPublicCertificate().length > 1
                ? a2.getCertificateManager().getPublicCertificate()[1] : null);
    }
}
