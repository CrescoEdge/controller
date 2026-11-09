package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.communication.PkiTestSupport;
import io.cresco.agent.controller.communication.SecuritySettings;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * security_profile=production: discovery is authenticated by the operator's PKI alone. Each side signs the exchange's
 * transcript with its provisioned identity key and its chain verifies to the trust anchors; no discovery secret is
 * configured, used or accepted. The standard profile's secret-keyed exchange and the PKI exchange never meet.
 */
class PkiDiscoveryTest {

    private static final long DAY = 24L * 3600 * 1000;
    private static final Gson GSON = new Gson();
    private static PkiTestSupport.Ca root, rgA, otherRoot;
    private static Path pki, anchors;
    private static PkiTestSupport.Node global, regionA, agentA1, agentA2, stranger;

    @BeforeAll
    static void issue() throws Exception {
        TlsTestSupport.provider();
        root = PkiTestSupport.root("Cresco Fabric Root");
        rgA = PkiTestSupport.region(root, "t1", "rgA");
        otherRoot = PkiTestSupport.root("Not The Fabric's Root");
        pki = Files.createTempDirectory("pkid");
        anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        long from = System.currentTimeMillis() - 60_000;
        global = PkiTestSupport.write(PkiTestSupport.global(root, "t1", "glob", "gc", from, 30 * DAY), pki.resolve("glob_gc"));
        regionA = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "rc", true, from, 30 * DAY), pki.resolve("rgA_rc"));
        agentA1 = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, from, 30 * DAY), pki.resolve("rgA_a1"));
        agentA2 = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "a2", false, from, 30 * DAY), pki.resolve("rgA_a2"));
        PkiTestSupport.Ca otherRegion = PkiTestSupport.region(otherRoot, "t1", "rgA");
        stranger = PkiTestSupport.write(PkiTestSupport.node(otherRegion, "t1", "rgA", "a9", false, from, 30 * DAY), pki.resolve("x_a9"));
    }

    private static ControllerEngine node(PkiTestSupport.Node n, String region, String agent, String role, boolean production,
                                         String... extra) throws Exception {
        Path dir = Files.createTempDirectory("pkid-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        if (role != null) cfg.put(role, "true");
        if (production) cfg.put("security_profile", "production");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = role != null;
        st.global = "is_global".equals(role);
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned());
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

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
    void productionNodesJoinOverThePkiWithNoSecretAtAll() throws Exception {
        ControllerEngine g = node(global, "glob", "gc", "is_global", true);
        ControllerEngine r = node(regionA, "rgA", "rc", "is_region", true);
        ControllerEngine a = node(agentA1, "rgA", "a1", null, true);
        assertTrue(new DiscoveryProcessor(g).pkiDiscovery() && new DiscoveryProcessor(a).pkiDiscovery());
        DiscoveryAuth.Msg hello = new DiscoveryProcessor(r).clientHello(DiscoveryType.GLOBAL);
        assertNotNull(hello, "a production node needs no discovery secret to open an exchange");
        assertEquals(DiscoveryAuth.AUTH_PKI, hello.auth);
        DiscoveryNode n = exchange(r, g, DiscoveryType.GLOBAL);
        assertNotNull(n, "the region did not join the global over the PKI");
        assertEquals("glob", n.discovered_region);
        DiscoveryNode na = exchange(a, r, DiscoveryType.AGENT);
        assertNotNull(na, "the agent did not join its region over the PKI");
        assertEquals("rc", na.discovered_agent);
    }

    @Test
    void theTwoKindsOfExchangeNeverMeet() throws Exception {
        ControllerEngine prodRegion = node(regionA, "rgA", "rc", "is_region", true);
        // a standard-profile node (the discovery secret's exchange), even one of the same PKI, is not answered in production
        ControllerEngine stdAgent = node(agentA1, "rgA", "a1", null, false, "discovery_secret_agent", "a-shared-secret-not-real");
        assertNull(exchange(stdAgent, prodRegion, DiscoveryType.AGENT), "a secret-keyed hello was answered in production");
        // and a standard responder does not answer a PKI hello
        ControllerEngine stdRegion = node(regionA, "rgA", "rc", "is_region", false, "discovery_secret_agent", "a-shared-secret-not-real");
        ControllerEngine prodAgent = node(agentA1, "rgA", "a1", null, true);
        assertNull(new DiscoveryProcessor(stdRegion).serverChallenge(copy(new DiscoveryProcessor(prodAgent).clientHello(DiscoveryType.AGENT)), "c"),
                "a standard responder answered a PKI hello");
        // a secret-keyed locator is not answered in production; a PKI locator is, and carries no secret
        DiscoveryProcessor rp = new DiscoveryProcessor(prodRegion);
        DiscoveryNode loc = new DiscoveryProcessor(prodAgent).generateBroadCastDiscovery(DiscoveryType.AGENT, false);
        assertEquals(DiscoveryAuth.PKI_LOCATOR, loc.broadcast_validator);
        DiscoveryNode answer = rp.processIncomingBroadCastDiscovery(loc, "127.0.0.1", 32005, "127.0.0.1", 40001);
        assertNotNull(answer);
        assertEquals(DiscoveryNode.NodeType.DISCOVERED, answer.nodeType);
        assertTrue(new DiscoveryProcessor(prodAgent).isValidatedAuthenication(answer));
        DiscoveryNode secretLoc = new DiscoveryProcessor(stdAgent).generateBroadCastDiscovery(DiscoveryType.AGENT, false);
        assertNull(rp.processIncomingBroadCastDiscovery(secretLoc, "127.0.0.1", 32005, "127.0.0.1", 40002),
                "a secret-keyed locator was answered in production");
        assertNull(new DiscoveryProcessor(prodAgent).generateBroadCastDiscovery(DiscoveryType.AGENT, true),
                "production made a single-message certificate exchange");
    }

    @Test
    void aChainOutsideTheTrustAnchorsGetsNoSignature() throws Exception {
        ControllerEngine r = node(regionA, "rgA", "rc", "is_region", true);
        // the stranger's chain is to another root: refused at its hello, before the responder signs anything for it
        ControllerEngine s = node(stranger, "rgA", "a9", null, true, "trust_anchors",
                PkiTestSupport.pem(pki.resolve("other-anchors.pem"), otherRoot.cert).toString());
        DiscoveryAuth.Msg hello = new DiscoveryProcessor(s).clientHello(DiscoveryType.AGENT);
        assertNotNull(hello);
        assertNull(new DiscoveryProcessor(r).serverChallenge(copy(hello), "stranger"), "a chain outside the PKI got a challenge");
    }

    @Test
    void aCopiedChainWithoutItsKeyAndAlteredOrReplayedExchangesFail() throws Exception {
        ControllerEngine r = node(regionA, "rgA", "rc", "is_region", true);
        ControllerEngine victim = node(agentA1, "rgA", "a1", null, true);
        ControllerEngine thief = node(agentA2, "rgA", "a2", null, true);
        DiscoveryProcessor rp = new DiscoveryProcessor(r);

        // the thief presents a1's (public) chain but holds only its own key: its proof does not verify, nothing is granted
        DiscoveryAuth.Msg hello = new DiscoveryProcessor(victim).clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = rp.serverChallenge(copy(hello), "thief");
        assertNotNull(ch);
        DiscoveryAuth.Msg forged = new DiscoveryProcessor(thief).clientProof(hello, copy(ch), "server");
        assertNotNull(forged, "the responder's challenge itself is genuine");
        DiscoveryAuth.Msg result = rp.serverResult(copy(hello), ch, copy(forged), "thief");
        assertFalse(result.ok, "a proof signed by another key was accepted");

        // a genuine proof replayed against a fresh challenge fails (the transcript covers the responder's fresh nonce)
        DiscoveryProcessor vp = new DiscoveryProcessor(victim);
        DiscoveryAuth.Msg h1 = vp.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch1 = rp.serverChallenge(copy(h1), "victim");
        DiscoveryAuth.Msg p1 = vp.clientProof(h1, copy(ch1), "server");
        assertTrue(rp.serverResult(copy(h1), ch1, copy(p1), "victim").ok, "the genuine exchange");
        DiscoveryAuth.Msg ch2 = rp.serverChallenge(copy(h1), "replayer");
        assertFalse(rp.serverResult(copy(h1), ch2, copy(p1), "replayer").ok, "a replayed proof was accepted");

        // an altered challenge (another broker port) fails at the client
        DiscoveryAuth.Msg h3 = vp.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch3 = copy(rp.serverChallenge(copy(h3), "victim"));
        ch3.sport = ch3.sport + 1;
        assertNull(vp.clientProof(h3, ch3, "mitm"), "the client accepted a redirected broker port");
        // an altered result fails at the client
        DiscoveryAuth.Msg h4 = vp.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch4 = rp.serverChallenge(copy(h4), "victim");
        DiscoveryAuth.Msg r4 = copy(rp.serverResult(copy(h4), ch4, copy(vp.clientProof(h4, copy(ch4), "server")), "victim"));
        r4.reason = "altered";
        assertNull(vp.clientResult(h4, copy(ch4), r4, "127.0.0.1", 32005), "an altered result was accepted");
    }

    @Test
    void productionRefusesToStartWithADiscoverySecret() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("security_profile", "production");
        cfg.put("discovery_secret_agent", "a-leftover-secret-not-real");
        Path d = Files.createTempDirectory("pkid-cfg");
        java.util.List<String> refusals = SecuritySettings.profileRefusals(TestAgentService.plugin(d.toString(), cfg).getConfig(), System.currentTimeMillis());
        assertTrue(refusals.stream().anyMatch(s -> s.contains("discovery_secret_agent is set")), refusals.toString());
        cfg.remove("discovery_secret_agent");
        refusals = SecuritySettings.profileRefusals(TestAgentService.plugin(d.toString(), cfg).getConfig(), System.currentTimeMillis());
        assertTrue(refusals.stream().noneMatch(s -> s.contains("discovery secret")), "production still asks for a discovery secret: " + refusals);
    }
}
