package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Bound peers stay trusted for TLS across a restart, both directions; nothing else does (third review, finding 5). */
class RestartTrustTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine at(Path dir, String region, String agent, boolean regional) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        for (String t : new String[]{"agent", "region", "global"}) cfg.put("discovery_secret_" + t, t + "-secret");
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State s = new TlsTestSupport.State(pb, region, agent);
        s.regional = regional;
        e.cstate = s;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static void join(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg h = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(h), "c");
        DiscoveryAuth.Msg r = s.serverResult(copy(h), ch, copy(c.clientProof(h, copy(ch), "s")), "c");
        assertNotNull(c.clientResult(h, copy(ch), copy(r), "127.0.0.1", 32005));
    }

    private static boolean trustsClient(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkClientTrusted(chain, "RSA"); return true; } catch (Exception x) { return false; }
    }

    private static boolean trustsServer(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkServerTrusted(chain, "RSA"); return true; } catch (Exception x) { return false; }
    }

    @Test
    void bridgesAndAgentsReconnectAfterEitherSideRestarts() throws Exception {
        Path g = Files.createTempDirectory("rt-g"), r = Files.createTempDirectory("rt-r"), a = Files.createTempDirectory("rt-a");
        ControllerEngine global = at(g, "glob", "gc", true), region = at(r, "rgr", "rc", true), agent = at(a, "rgr", "b1", false);
        join(region, global, DiscoveryType.GLOBAL);
        join(agent, region, DiscoveryType.AGENT);
        X509Certificate[] regionChain = region.getCertificateManager().getPublicCertificate();
        X509Certificate[] globalChain = global.getCertificateManager().getPublicCertificate();
        X509Certificate[] agentChain = agent.getCertificateManager().getPublicCertificate();

        ControllerEngine global2 = at(g, "glob", "gc", true);                     // the global restarts
        assertTrue(trustsClient(global2, regionChain), "after a global restart its broker refuses the region's bridge");
        ControllerEngine region2 = at(r, "rgr", "rc", true);                      // the region restarts
        assertTrue(trustsServer(region2, globalChain), "after a region restart its bridge refuses the global's broker");
        assertTrue(trustsClient(region2, agentChain), "after a region restart its broker refuses its agent");
        ControllerEngine agent2 = at(a, "rgr", "b1", false);                       // the agent restarts
        assertTrue(trustsServer(agent2, region.getCertificateManager().getPublicCertificate()), "after an agent restart it refuses its region's broker");

        // nothing that is not bound: a stranger, and a certificate that names a bound name with another key
        assertFalse(trustsClient(global2, TlsTestSupport.selfRooted("x", "t1", "rgx", "x1").chain));
        assertFalse(trustsClient(region2, TlsTestSupport.selfRooted("x", "t1", "rgr", "b1").chain), "another key for a bound name was trusted");
    }
}
