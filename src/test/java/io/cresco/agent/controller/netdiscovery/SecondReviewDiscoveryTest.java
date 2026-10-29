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

    /** A region controller whose data directory was wiped (new key) is refused by every agent that pinned it. */
    @Test
    void aWipedRegionControllerIsRefusedByItsAgentsUntilEachIsReconfigured() throws Exception {
        ControllerEngine rc = controller("rgA", "rc");
        ControllerEngine a1 = TlsTestSupport.engine("rgA", "a1", cfg(false));
        assertTrue(exchange(a1, rc, DiscoveryType.AGENT).ok);
        ControllerEngine wiped = controller("rgA", "rc");                                    // same name, new data dir
        DiscoveryProcessor c = new DiscoveryProcessor(a1), s = new DiscoveryProcessor(wiped);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "a1");
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(c.clientProof(hello, copy(ch), "rc")), "a1");
        DiscoveryNode n = c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
        System.out.println("wiped rc: responder ok=" + r.ok + ", agent accepted=" + (n != null)
                + " (the responder has already bound a1 and answered ok; the agent refuses only on its side)");
        assertNotNull(n, "an agent refuses its re-installed region controller until peer_identity_rebind is set on the agent");
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
