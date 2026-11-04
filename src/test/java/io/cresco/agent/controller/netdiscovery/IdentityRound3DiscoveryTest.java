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

/** Round-3 fix P4: the global is a role of the PKI (cresco:role:global); only it answers GLOBAL discovery and is accepted. */
class IdentityRound3DiscoveryTest {

    private static final long DAY = 24L * 3600 * 1000;
    private static final Gson GSON = new Gson();

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String region, String agent) throws Exception {
        return provisioned(n, anchors, region, agent, false);
    }

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String region, String agent, boolean global) throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("rvid3-" + region);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put(global ? "is_global" : "is_region", "true");
        cfg.put("discovery_secret_global", "the-global-secret-every-region-holds");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = true;
        st.global = global;
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

    @Test
    void theRealGlobalAnswersAndIsAcceptedAndARegionCertificateCannotBeTheGlobal() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgA = PkiTestSupport.region(root, "t1", "rgA");
        Path pki = Files.createTempDirectory("id3d-pki");
        Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        long from = System.currentTimeMillis() - 60_000;
        PkiTestSupport.Node a = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "rc", true, from, 30 * DAY), pki.resolve("rgA_rc"));
        PkiTestSupport.Node g = PkiTestSupport.write(PkiTestSupport.global(root, "t1", "glob", "gc", from, 30 * DAY), pki.resolve("glob_gc"));
        ControllerEngine regionA = provisioned(a, anchors, "rgA", "rc"), global = provisioned(g, anchors, "glob", "gc", true);
        assertTrue(global.getCertificateManager().isProvisionedGlobal() && !regionA.getCertificateManager().isProvisionedGlobal());
        DiscoveryNode found = exchange(regionA, global, DiscoveryType.GLOBAL);
        assertNotNull(found, "the real global (cresco:role:global, issued by the root) is not accepted");
        assertEquals("glob", found.discovered_region);
        // a controller certificate WITHOUT the global role cannot be configured as the global
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("is_global", "true");
        cfg.put("identity_keystore", a.keystore.toString());
        cfg.put("identity_keystore_password_file", a.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        String why = CertificateManager.provisioningRefusal(TestAgentService.plugin(Files.createTempDirectory("id3d-cfg").toString(), cfg, m -> { }, "rgA", "rc").getConfig());
        System.out.println("is_global with a region controller's certificate -> " + why);
        assertNotNull(why);
        assertTrue(why.contains("cresco:role:global"), why);
    }
}
