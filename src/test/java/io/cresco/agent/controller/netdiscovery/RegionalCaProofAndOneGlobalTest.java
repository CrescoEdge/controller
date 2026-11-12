package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The regional CA ({@code security_regional_ca}) under the rules of the proof. Who a regional CA trusts is unchanged -- one
 * CA per region, a leaf by that CA -- but: a region has ONE global there too (any region controller that proved its own key
 * federated as a second one), and the CA issues and records nothing for a joiner that did not prove its key, unless that
 * joiner is recorded as it presents itself (a node of release 1.3.378) or the upgrade window is open.
 */
class RegionalCaProofAndOneGlobalTest {

    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";
    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean controller, boolean global, Path dir, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("security_regional_ca", "true");
        m.put("tenant_id", "t1");
        m.put("discovery_proof_grace_days", "0");
        if (dir != null) {
            m.put("node_identity_dir", dir.resolve("security").toString());
            m.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        }
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = controller;
        st.global = global;
        if (controller && !global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        if (controller) e.getCertificateManager().ensureRegionCA();
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }
    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type, String[] told) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        if (proof == null) return null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
        if (told != null) told[0] = result.reason;
        return c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005);
    }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) { return join(client, server, type, null); }

    /** A region under the regional CA has one global: another region controller that answers its GLOBAL exchange, proving its own key, is not a second one. */
    @Test
    void underTheRegionalCaARegionHasOneGlobal() throws Exception {
        Path dir = Files.createTempDirectory("rca-region");
        ControllerEngine global = node("rgG", "gc", true, true, null), other = node("rgH", "hc", true, true, null);
        ControllerEngine region = node("rgA", "rc", true, false, dir);
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "(a region federates with its global, as before)");
        assertEquals("rgG/gc", reg(region).globalRecorded());
        assertNotEquals("none", reg(region).caState("rgG"));
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "(and again)");
        assertNull(join(region, other, DiscoveryType.GLOBAL), "another region's controller federated as a second global");
        assertEquals("none", reg(region).caState("rgH"), "the second global's CA was recorded");
        assertFalse(reg(region).isBridgePeer(CrescoIdentity.of("t1", "rgH", "hc", null)), "the second global is a bridge peer");
        // as a PEER region it federates, as before -- and is then not the global either
        assertNotNull(join(region, other, DiscoveryType.REGION), "(a peer region federates, as before)");
        assertNull(join(region, other, DiscoveryType.GLOBAL), "a federated peer region was taken as the global");
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        // the operator moves the region with the same line as under per-node trust
        String otherKey = PeerIdentity.spkiFingerprint(other.getCertificateManager().getPublicCertificate()[0]);
        Files.writeString(dir.resolve("security").resolve("peer-trust.json.rebind"), "rgH/hc=" + otherKey + "\n");
        assertNotNull(join(region, other, DiscoveryType.GLOBAL), "the operator's rebind line did not move the region");
        assertEquals("rgH/hc", reg(region).globalRecorded());
        assertNull(join(region, global, DiscoveryType.GLOBAL));
    }

    /** The CA issues and records nothing for a joiner that did not prove its key -- an agent, a federating controller -- unless it is recorded as it presents itself. */
    @Test
    void underTheRegionalCaNothingIsIssuedOrRecordedWithoutProof() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, false, null), a1 = node("rgA", "a1", false, false, null), a2 = node("rgA", "a2", false, false, null);
        // an agent's first enrollment without proof: refused, told to upgrade; nothing bound
        DiscoveryAuth.Msg first = Release378.join(a1, rc, DiscoveryType.AGENT, SECRET);
        assertFalse(first.ok, "the region CA enrolled a joiner that did not prove its key");
        assertTrue(first.reason.contains("UPGRADE rgA/a1"), first.reason);
        assertNull(reg(rc).pinnedKey(new PeerIdentity.Expected("rgA", "a1")));
        // with proof: enrolled, as before, and recorded as proved
        assertNotNull(join(a1, rc, DiscoveryType.AGENT), "(an agent enrolls, as before)");
        assertTrue(reg(rc).provedKey("rgA", "a1"));
        assertNotNull(join(a1, rc, DiscoveryType.AGENT), "(and re-enrolls)");
        assertFalse(Release378.join(a1, rc, DiscoveryType.AGENT, SECRET).ok, "an agent that proved its key was re-enrolled without proof (no grace given)");
        // an agent of release 1.3.378 that IS enrolled (recorded by that release): re-enrolled as before
        assertTrue(reg(rc).bind("t1", "rgA", "a2", a2.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "enrolled").ok);
        DiscoveryAuth.Msg again = Release378.join(a2, rc, DiscoveryType.AGENT, SECRET);
        assertTrue(again.ok, "an enrolled agent of release 1.3.378 is no longer re-enrolled: " + again.reason);
        // a controller that federates without proof: nothing of it is recorded
        ControllerEngine global = node("rgG", "gc", true, true, null), region = node("rgB", "rc", true, false, null);
        DiscoveryAuth.Msg fed = Release378.join(region, global, DiscoveryType.GLOBAL, SECRET);
        assertFalse(fed.ok, "a controller federated without proving its key");
        assertEquals("none", reg(global).caState("rgB"), "the CA of a controller that did not prove its key was recorded");
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "(a region federates with proof, as before)");
        assertTrue(reg(global).provedKey("rgB", "rc"));
        // and the joiner's side: a region's first federation with a responder that does not prove its key is refused
        ControllerEngine fresh = node("rgC", "rc", true, false, null);
        assertNull(Release378.answer(fresh, global, DiscoveryType.GLOBAL, SECRET, "10.1.2.3", 32010));
        assertNull(reg(fresh).globalRecorded());
    }
}
