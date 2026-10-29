package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** What a joining controller accepts from the controller that answers it (third review, findings 1 and 2). */
class FederationAnswerTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine controller(String region, String agent, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", "agent-secret");
        m.put("discovery_secret_region", "region-secret");
        m.put("discovery_secret_global", "global-secret");
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        ((TlsTestSupport.State) e.cstate).regional = true;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    /** A GLOBAL answer from {@code responder}'s name whose result carries {@code rcert}, correctly MAC'd. */
    private static DiscoveryNode answer(ControllerEngine victim, ControllerEngine responder, String rcert) {
        DiscoveryProcessor c = new DiscoveryProcessor(victim), s = new DiscoveryProcessor(responder);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "victim");
        assertNotNull(c.clientProof(hello, copy(ch), "responder"));
        DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
        r.step = "result"; r.type = hello.type; r.ok = true; r.rcert = rcert;
        X509Certificate cl = victim.getCertificateManager().getCertsfromJson(hello.ccert)[0];
        X509Certificate sl = victim.getCertificateManager().getCertsfromJson(ch.scert)[0];
        byte[] k = DiscoveryAuth.key("global-secret", "handshake|" + hello.type);
        r.mac = DiscoveryAuth.mac(k, "S2", DiscoveryAuth.transcript(hello, ch, cl, sl), DiscoveryAuth.resultDigest(r));
        return c.clientResult(hello, copy(ch), copy(r), "10.6.6.6", 32005);
    }

    private static boolean pkixTrusts(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkServerTrusted(chain, "RSA"); return true; }
        catch (Exception ex) { return false; }
    }

    @Test
    void aRefusedFederationAnswerRecordsAndTrustsNothing() throws Exception {
        ControllerEngine victim = controller("rgA", "rc", "security_regional_ca", "true");
        victim.getCertificateManager().ensureRegionCA();
        ControllerEngine responder = controller("rgZ", "gz");
        RegionCA evil = RegionCA.generate("cresco-system", "rgZ", 2048, 1);
        assertNull(answer(victim, responder, victim.getCertificateManager().getJsonFromCerts(evil.caChain())));
        assertEquals("none", victim.getCertificateManager().getPeerTrust().caState("rgZ"), "a refused answer's CA was recorded");
        TlsTestSupport.Id minted = TlsTestSupport.issued(evil, "cresco-system", "rgZ", "x");
        assertFalse(pkixTrusts(victim, minted.chain), "a refused answer's CA was added to the truststore");
        // the same answer for an ordinary tenant is accepted
        RegionCA ok = RegionCA.generate("t1", "rgZ", 2048, 1);
        assertNotNull(answer(victim, responder, victim.getCertificateManager().getJsonFromCerts(ok.caChain())));
        assertNotEquals("none", victim.getCertificateManager().getPeerTrust().caState("rgZ"));
    }

    @Test
    void theRulesForAFederatedCa() throws Exception {
        ControllerEngine victim = controller("rgA", "rc", "security_regional_ca", "true", "security_federated_tenants", "t1,t2");
        var cm = victim.getCertificateManager();
        assertNull(cm.federatedCaRefusal(RegionCA.generate("t1", "rgB", 2048, 1).caCert(), "rgB"));
        assertNotNull(cm.federatedCaRefusal(RegionCA.generate("t1", "rgA", 2048, 1).caCert(), "rgA"), "own region");
        assertNotNull(cm.federatedCaRefusal(RegionCA.generate("cresco-system", "rgB", 2048, 1).caCert(), "rgB"), "superuser tenant");
        assertNotNull(cm.federatedCaRefusal(RegionCA.generate("t9", "rgB", 2048, 1).caCert(), "rgB"), "tenant not federated");
        assertNotNull(cm.federatedCaRefusal(RegionCA.generate("t1", "rgB", 2048, 1).caCert(), "rgC"), "not the answering region");
        assertNotNull(cm.federatedCaRefusal(TlsTestSupport.issued(RegionCA.generate("t1", "rgB", 2048, 1), "t1", "rgB", "leaf").chain[0], "rgB"),
                "not a CA");
    }

    /** Per-node trust: a client exchange refused at binding leaves the impostor's chain untrusted for TLS. */
    @Test
    void aRefusedClientExchangeLeavesNoTrustAnchor() throws Exception {
        ControllerEngine victim = controller("rgA", "rc");
        ControllerEngine rgB = controller("rgB", "rcB");
        DiscoveryProcessor cb = new DiscoveryProcessor(rgB), sv = new DiscoveryProcessor(victim);
        DiscoveryAuth.Msg h1 = cb.clientHello(DiscoveryType.REGION);
        DiscoveryAuth.Msg c1 = sv.serverChallenge(copy(h1), "rgB");
        assertTrue(sv.serverResult(copy(h1), c1, copy(cb.clientProof(h1, copy(c1), "victim")), "rgB").ok);
        ControllerEngine impostor = controller("rgB", "rcB");
        DiscoveryProcessor cv = new DiscoveryProcessor(victim), si = new DiscoveryProcessor(impostor);
        DiscoveryAuth.Msg h2 = cv.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg c2 = si.serverChallenge(copy(h2), "victim");
        DiscoveryAuth.Msg r2 = si.serverResult(copy(h2), c2, copy(cv.clientProof(h2, copy(c2), "impostor")), "victim");
        assertNull(cv.clientResult(h2, copy(c2), copy(r2), "10.6.6.6", 32005), "the impostor's answer was accepted");
        assertFalse(pkixTrusts(victim, impostor.getCertificateManager().getPublicCertificate()),
                "a refused responder's chain was added to the truststore");
    }
}
