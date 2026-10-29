package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Third review: broker_require_client_auth now defaults to broker_security_enabled (needClientAuth). Per-node trust (the
 * default, security_regional_ca=false): a controller's truststore is in memory, so after a controller restart the agents'
 * self-rooted chains are no longer PKIX-trusted, although their identity bindings persisted.
 */
class ThirdReviewRestartTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine engineAt(Path dir, String region, String agent, boolean regional) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("discovery_secret_agent", "agent-secret");
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

    private static boolean pkixTrustsClient(ControllerEngine c, ControllerEngine client) {
        try {
            c.getCertificateManager().currentPkixTrustManager().checkClientTrusted(client.getCertificateManager().getPublicCertificate(), "RSA");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void anAgentIsStillTrustedForTlsByItsControllerAfterTheControllerRestarts() throws Exception {
        Path cdir = Files.createTempDirectory("r3-ctl"), adir = Files.createTempDirectory("r3-agent");
        ControllerEngine ctl = engineAt(cdir, "rgA", "rc", true);
        ControllerEngine agent = engineAt(adir, "rgA", "a1", false);
        DiscoveryProcessor c = new DiscoveryProcessor(agent), s = new DiscoveryProcessor(ctl);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "a1");
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(c.clientProof(hello, copy(ch), "rc")), "a1");
        DiscoveryNode n = c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
        assertNotNull(n);
        assertTrue(pkixTrustsClient(ctl, agent), "precondition: after discovery the controller's broker trusts the agent's chain");
        assertTrue(ActiveBroker.requireClientAuth(ctl.getPluginBuilder().getConfig()), "precondition: client auth defaults on with broker security");

        ControllerEngine restarted = engineAt(cdir, "rgA", "rc", true);       // same data directory: identity + registry persist
        boolean bound = restarted.getCertificateManager().getPeerTrust().verifiedIdentity(agent.getCertificateManager().getPublicCertificate()) != null;
        boolean pkix = pkixTrustsClient(restarted, agent);
        System.out.println("after a controller restart: agent binding persisted=" + bound + "; broker PKIX trusts the agent's client chain=" + pkix
                + " (needClientAuth is now the default with broker_security_enabled)");
        assertTrue(pkix, "after a controller restart the broker no longer PKIX-trusts its agents (in-memory truststore): every agent's"
                + " failover reconnect fails the TLS handshake until it re-runs discovery");
    }
}
