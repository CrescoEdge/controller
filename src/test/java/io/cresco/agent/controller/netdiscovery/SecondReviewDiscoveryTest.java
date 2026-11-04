package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Second adversarial review (discovery v2, default per-node trust). The attacker holds ONLY the agent discovery secret,
 * which every agent of the fabric is configured with. Each test asserts the SECURE outcome; a failure is a finding.
 */
class SecondReviewDiscoveryTest {

    private static final String AGENT_SECRET = "agent-secret-every-agent-has";
    private static final Gson GSON = new Gson();

    private static Map<String, Object> cfg(boolean holdsControllerSecrets, String... extra) {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", AGENT_SECRET);
        if (holdsControllerSecrets) {
            m.put("discovery_secret_region", "region-secret");
            m.put("discovery_secret_global", "global-secret");
        }
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        return m;
    }

    private static ControllerEngine controller(String region, String agent) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg(true));
        ((TlsTestSupport.State) e.cstate).regional = true;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) {
        return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class);
    }

    /** A full v2 exchange; the responder's RESULT. */
    private static DiscoveryAuth.Msg exchange(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "client");
        c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
        return r;
    }

    private static X509Certificate[] chain(ControllerEngine e) {
        return e.getCertificateManager().getPublicCertificate();
    }

    /** An agent-secret holder claims the RESPONDER's own name in an AGENT exchange and gets it bound to its key. */
    @Test
    void anAgentCannotTakeTheControllersOwnName() throws Exception {
        ControllerEngine global = controller("rgG", "gc");
        ControllerEngine attacker = TlsTestSupport.engine("rgG", "gc", cfg(false));          // its own key, the global's name
        DiscoveryAuth.Msg r = exchange(attacker, global, DiscoveryType.AGENT);
        PeerTrustRegistry reg = global.getCertificateManager().getPeerTrust();
        CrescoIdentity asSeen = reg.verifiedIdentity(chain(attacker));
        System.out.println("AGENT exchange claiming the responder's own name: ok=" + (r != null && r.ok) + "; attacker's cert now verifies as " + asSeen
                + "; TenantPolicy READ of the global's inbox queue rgG_gc for it: "
                + io.cresco.agent.controller.communication.TenantPolicy.check(asSeen, "rgG_gc",
                        io.cresco.agent.controller.communication.TenantPolicy.Access.READ, java.util.Set.of("agent.event", "region.event", "global.event")).reason);
        assertNull(asSeen, "the global now verifies an agent's certificate as its own identity rgG/gc");
    }

    /** ... claims another region's controller before that region federates: the real region is then refused. */
    @Test
    void anAgentCannotSquatAnotherRegionsControllerName() throws Exception {
        ControllerEngine global = controller("rgG", "gc");
        ControllerEngine attacker = TlsTestSupport.engine("rgB", "rc", cfg(false));          // agent secret only
        DiscoveryAuth.Msg squat = exchange(attacker, global, DiscoveryType.AGENT);
        ControllerEngine realRegion = controller("rgB", "rc");
        DiscoveryAuth.Msg join = exchange(realRegion, global, DiscoveryType.GLOBAL);
        System.out.println("squat AGENT rgB/rc ok=" + (squat != null && squat.ok) + "; real region's GLOBAL join ok="
                + (join != null && join.ok) + " reason=" + (join == null ? null : join.reason));
        assertFalse(squat != null && squat.ok, "an AGENT exchange bound a name in another region (rgB/rc) at the global");
        assertTrue(join != null && join.ok, "the real region rgB/rc can no longer federate with the global");
    }

    /** ... claims the superuser tenant: under mTLS the broker then gives its certificate the SUPERUSER role. */
    @Test
    void anAgentCannotChooseItsTenant() throws Exception {
        ControllerEngine region = controller("rgA", "rc");
        ControllerEngine attacker = TlsTestSupport.engine("rgA", "evil", cfg(false, "tenant_id", "cresco-system"));
        DiscoveryAuth.Msg r = exchange(attacker, region, DiscoveryType.AGENT);
        CrescoIdentity asSeen = region.getCertificateManager().getPeerTrust().verifiedIdentity(chain(attacker));
        System.out.println("AGENT exchange with tenant cresco-system: ok=" + (r != null && r.ok) + "; verified identity " + asSeen
                + " (broker_superuser_tenants default = cresco-system)");
        assertFalse(asSeen != null && "cresco-system".equals(asSeen.getTenant()),
                "an agent's self-chosen superuser tenant is now a verified identity at its region broker");
    }

    /** PBKDF2 (210k) runs for every distinct secret string and is cached forever; IPv6 workers default to a random one. */
    @Test
    void validatorKeyDerivationIsNotPerMessage() {
        long s = System.nanoTime();
        DiscoveryAuth.key(java.util.UUID.randomUUID().toString(), "validator");
        long ms = (System.nanoTime() - s) / 1_000_000;
        System.out.println("one uncached PBKDF2-HMAC-SHA-384/210k derivation = " + ms + " ms");
    }

    /**
     * A region controller whose data directory was wiped (new key) is refused by every agent that pinned it -- until the
     * operator authorises one rebind of it on the agent; a controller whose persisted identity was restored is accepted.
     *
     * <p>Round 2 made agents FOLLOW a re-keyed controller (an availability fix). The round-4 hardening (phase 2, item 7)
     * reverses that: following meant any holder of the shared agent secret could answer as the controller and be followed.
     * Joiners now pin the controller they joined; the documented path for a re-installed controller is to restore its
     * {@code security/} directory (same key, nothing to do) or authorise one rebind per agent ({@code peer_identity_rebind}
     * or the {@code peer-trust.json.rebind} file, no restart).
     */
    @Test
    void aWipedRegionControllerIsRefusedByItsAgentsUntilEachIsReconfigured() throws Exception {
        ControllerEngine rc = controller("rgA", "rc");
        java.nio.file.Path a1Dir = java.nio.file.Files.createTempDirectory("a1-pin");
        ControllerEngine a1 = agentAt(a1Dir);
        assertTrue(exchange(a1, rc, DiscoveryType.AGENT).ok);
        ControllerEngine wiped = controller("rgA", "rc");                                    // same name, new data dir
        DiscoveryNode n = clientSide(a1, wiped);
        System.out.println("wiped rc: agent accepted=" + (n != null));
        assertNull(n, "an agent followed a re-keyed controller (anyone holding the agent secret could pose as it)");
        // the operator authorises one rebind of rgA/rc on the agent (no restart)
        java.nio.file.Path reg;
        try (var w = java.nio.file.Files.walk(a1Dir)) { reg = w.filter(p -> p.getFileName().toString().equals("peer-trust.json")).findFirst().orElseThrow(); }
        java.nio.file.Files.writeString(reg.resolveSibling("peer-trust.json.rebind"), "rgA/rc\n");
        assertNotNull(clientSide(a1, wiped), "the operator's one-shot rebind did not let the agent join the re-installed controller");
        ControllerEngine third = controller("rgA", "rc");
        assertNull(clientSide(a1, third), "a consumed rebind let a third key in");
    }

    private static ControllerEngine agentAt(java.nio.file.Path dir) throws Exception {
        io.cresco.agent.controller.communication.TlsTestSupport.provider();
        Map<String, Object> c = new HashMap<>(cfg(false));
        c.put("public_key_directory", dir.resolve("public-keys").toString());
        c.put("tenant_id", "t1");
        io.cresco.library.plugin.PluginBuilder pb = io.cresco.agent.test.TestAgentService.plugin(dir.toString(), c, m -> { }, "rgA", "a1");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = io.cresco.agent.controller.communication.TlsTestSupport.state(pb, "rgA", "a1");
        e.setCertificateManager(new io.cresco.agent.controller.communication.CertificateManager(e));
        return e;
    }

    /** One AGENT exchange; the client's verdict (null: refused). */
    private static DiscoveryNode clientSide(ControllerEngine client, ControllerEngine server) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "a1");
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "rc");
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "a1");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    /** Leaves are (re)issued at discovery time with notBefore = now: a peer whose clock is 2 s behind refuses them. */
    @Test
    void aLeafReissuedAtJoinToleratesSmallClockSkew() throws Exception {
        ControllerEngine a = TlsTestSupport.engine("rgStart", "a1", cfg(false));
        ((TlsTestSupport.State) a.cstate).region = "rgJoined";                                 // adopted from the locator
        assertTrue(a.getCertificateManager().ensureLeafIdentity());
        X509Certificate leaf = a.getCertificateManager().getPublicCertificate()[0];
        java.util.Date peerClock = new java.util.Date(System.currentTimeMillis() - 2000);
        String verdict;
        try { leaf.checkValidity(peerClock); verdict = "valid"; } catch (Exception e) { verdict = e.getClass().getSimpleName(); }
        System.out.println("leaf notBefore=" + leaf.getNotBefore().getTime() + " ; at a peer 2 s behind: " + verdict);
        assertEquals("valid", verdict, "a just re-issued leaf is not yet valid for a peer whose clock is 2 s behind");
    }
}
