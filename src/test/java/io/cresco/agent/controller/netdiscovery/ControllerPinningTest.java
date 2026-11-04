package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2, item 7: holding the shared agent secret does not let anyone pose as a controller. A joiner pins the controller
 * it joined (and, under the regional CA, that region's CA); an agent-secret holder answering as that controller later is
 * refused. controller_trust_anchors closes the first join too.
 */
class ControllerPinningTest {

    private static final String AGENT_SECRET = "agent-secret-every-agent-has";
    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", AGENT_SECRET);
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
    void anAgentSecretHolderCannotPoseAsTheJoinedController() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        assertNotNull(join(a1, rc), "precondition: a1 joins the real controller");
        // another agent of the fabric (it holds only the agent secret) answers a1's discovery as rgA/rc with its own key
        ControllerEngine impostor = node("rgA", "rc", true);
        assertNull(join(a1, impostor), "an agent-secret holder posed as the controller a1 joined");
        assertNotNull(join(a1, rc), "the real controller is still accepted");
    }

    @Test
    void underTheRegionalCaTheJoinedRegionsCaIsPinned() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, "security_regional_ca", "true");
        rc.getCertificateManager().ensureRegionCA();
        ControllerEngine a1 = node("rgA", "a1", false, "security_regional_ca", "true");
        assertNotNull(join(a1, rc), "precondition: a1 enrolls under the real region CA");
        ControllerEngine impostor = node("rgA", "rc", true, "security_regional_ca", "true");
        impostor.getCertificateManager().ensureRegionCA();                     // its own CA for region rgA
        assertNull(join(a1, impostor), "an agent-secret holder with its own CA for rgA re-enrolled a1 under it");
        assertNotNull(join(a1, rc), "re-enrollment with the real region CA still works");
    }

    @Test
    void controllerTrustAnchorsCloseTheFirstJoin() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), impostor = node("rgA", "rc", true);
        String anchor = io.cresco.agent.controller.communication.PeerIdentity.spkiFingerprint(rc.getCertificateManager().getPublicCertificate()[0]);
        ControllerEngine a1 = node("rgA", "a1", false, "controller_trust_anchors", anchor);
        assertNull(join(a1, impostor), "a first join to a controller outside controller_trust_anchors succeeded");
        assertNotNull(join(a1, rc), "the anchored controller was refused");
    }
}
