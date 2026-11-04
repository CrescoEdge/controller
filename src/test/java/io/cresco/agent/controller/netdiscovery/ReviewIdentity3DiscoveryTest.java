package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.communication.PkiTestSupport;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review, round 3 (hardening-identity 7ba5cd7), 2026-11-04: discovery under pre-provisioned
 * identities. Each test asserts the SECURE outcome; a failure is a finding.
 */
class ReviewIdentity3DiscoveryTest {

    private static final long DAY = 24L * 3600 * 1000;
    private static final Gson GSON = new Gson();

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String region, String agent) throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("rvid3-" + region);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("is_region", "true");
        cfg.put("discovery_secret_global", "the-global-secret-every-region-holds");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = true;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned(), "precondition: " + region + "/" + agent + " is pre-provisioned");
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static DiscoveryNode exchange(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        if (hello == null) return null;
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "client");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    /**
     * Under pre-provisioned identities nothing is pinned, and the PKI has one controller role: nothing says which controller
     * is THE GLOBAL. Every region holds discovery_secret_global (it needs it to join) and any regional controller answers a
     * GLOBAL discovery exchange (getDiscoverySecret/serverChallenge check only isRegionalController). So region rgA, looking
     * for its global (multicast, or a re-discovery after a global loss), accepts region rgB's controller as its global -- an
     * honest one by accident, or a compromised one on purpose -- and it then has PARENT authority over rgA: its revocation
     * lists apply to every name there (the real global's key included, permanently), its crypto policy is adopted, and rgA's
     * upstream traffic goes to it. (controller_trust_anchors, listing the global's key, closes it if an operator sets it.)
     */
    @Test
    void anotherRegionsControllerIsNotAcceptedAsTheGlobal() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgA = PkiTestSupport.region(root, "t1", "rgA"), rgB = PkiTestSupport.region(root, "t1", "rgB");
        Path pki = Files.createTempDirectory("rvid3-pki");
        Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert, rgA.cert, rgB.cert);
        long from = System.currentTimeMillis() - 60_000;
        PkiTestSupport.Node a = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "rc", true, from, 30 * DAY), pki.resolve("rgA_rc"));
        PkiTestSupport.Node b = PkiTestSupport.write(PkiTestSupport.node(rgB, "t1", "rgB", "rc", true, from, 30 * DAY), pki.resolve("rgB_rc"));
        ControllerEngine regionA = provisioned(a, anchors, "rgA", "rc"), regionB = provisioned(b, anchors, "rgB", "rc");
        DiscoveryNode asGlobal = exchange(regionA, regionB, DiscoveryType.GLOBAL);
        System.out.println("global: region rgA's GLOBAL discovery answered by region rgB's controller -> "
                + (asGlobal == null ? "refused" : "ACCEPTED as its global " + asGlobal.discovered_region + "/" + asGlobal.discovered_agent));
        assertNull(asGlobal, "a region accepted another region's controller as its GLOBAL controller (nothing in the PKI or the exchange"
                + " says which controller is the global, and nothing is pinned)");
    }
}
