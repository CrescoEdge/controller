package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review, round 2 (controller ee48daa), 2026-11-04: controller_trust_anchors. Each test asserts the
 * SECURE outcome; a failure is a finding.
 *
 * <p>CertificateManager.controllerAnchorRefusal / controllerAnchorVouches accept a presented chain when ANY certificate in
 * it (or its key) is listed -- without checking that the leaf chains to it. A controller's certificates are public (it
 * presents them in every TLS handshake and discovery exchange). An agent-secret holder answering discovery as the controller
 * presents [its own leaf naming that controller, the real controller's listed certificate].
 */
class ReviewIdentity2DiscoveryTest {

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

    /** The impostor presents [its own leaf, the real controller's leaf] (it holds only the agent secret and its own key). */
    private static void appendRealCertificate(ControllerEngine impostor, ControllerEngine real) throws Exception {
        X509Certificate own = impostor.getCertificateManager().getPublicCertificate()[0];
        X509Certificate listed = real.getCertificateManager().getPublicCertificate()[0];
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("chain");
        f.setAccessible(true);
        f.set(impostor.getCertificateManager(), new X509Certificate[]{own, listed});
    }

    @Test
    void anAppendedAnchorCertificateDoesNotPassTheFirstJoin() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), impostor = node("rgA", "rc", true);
        String anchor = PeerIdentity.spkiFingerprint(rc.getCertificateManager().getPublicCertificate()[0]);
        ControllerEngine a1 = node("rgA", "a1", false, "controller_trust_anchors", anchor);
        appendRealCertificate(impostor, rc);
        DiscoveryNode n = join(a1, impostor);
        System.out.println("anchors, first join: the impostor presenting [its leaf, the anchored controller's leaf] joined: " + (n != null)
                + "; a1 pinned rgA/rc to the impostor's key: " + impostorPinned(a1, impostor));
        assertNull(n, "controller_trust_anchors accepted an impostor that only APPENDED the anchored controller's certificate");
    }

    @Test
    void anAppendedAnchorCertificateDoesNotRekeyTheJoinedController() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), impostor = node("rgA", "rc", true);
        String anchor = PeerIdentity.spkiFingerprint(rc.getCertificateManager().getPublicCertificate()[0]);
        ControllerEngine a1 = node("rgA", "a1", false, "controller_trust_anchors", anchor);
        assertNotNull(join(a1, rc), "precondition: a1 joins the anchored controller");
        appendRealCertificate(impostor, rc);
        DiscoveryNode hijack = join(a1, impostor);
        DiscoveryNode real = join(a1, rc);
        System.out.println("anchors, re-key: impostor followed as a 'vouched' re-key: " + (hijack != null) + "; a1 still accepts the real"
                + " controller: " + (real != null) + "; pinned to the impostor: " + impostorPinned(a1, impostor));
        assertNull(hijack, "an agent-secret holder re-keyed the controller a1 joined to its own key by appending the real controller's"
                + " (public) anchored certificate -- and the real controller's key is now refused for the name");
    }

    private static boolean impostorPinned(ControllerEngine at, ControllerEngine impostor) {
        String pinned = at.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgA", "rc"));
        return pinned != null && pinned.equals(PeerIdentity.spkiFingerprint(impostor.getCertificateManager().getPublicCertificate()[0]));
    }
}
