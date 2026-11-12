package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The review of the first "one controller per region" (2026-11-12), the responder's side: a holder of the discovery secret
 * answered a joiner's exchange with the PUBLIC certificate chain of the controller the joiner had recorded -- from another
 * address, naming another broker port -- and the joiner took it and went there. The secret's MACs said "a member sent this
 * certificate", never "its sender holds the key". Now the responder signs the exchange with that key, and a joiner that has
 * seen its controller do so takes nothing less from that name and key afterwards.
 */
class DiscoveryResponderProofTest {

    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";

    private static ControllerEngine node(String region, String agent, boolean controller, boolean global) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("broker_security_enabled", "true");
        m.put("msg_signing_sessions", "false");
        m.put("tenant_id", "t1");
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = controller;
        st.global = global;
        if (controller && !global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static final com.google.gson.Gson GSON = new com.google.gson.Gson();

    /** As a message arrives: through its JSON. */
    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
        return c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005);
    }

    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(e.getCertificateManager().getPublicCertificate()[0]); }

    /** R1: an agent that recorded its controller is not sent elsewhere by a secret holder with the controller's public chain. */
    @Test
    void anAgentIsNotSentElsewhereByAHolderOfTheSecretWithItsControllersPublicChain() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, false), agent = node("rgA", "a1", false, false);
        assertNotNull(join(agent, rc, DiscoveryType.AGENT), "precondition");
        DiscoveryNode n = Release378.answer(agent, rc, DiscoveryType.AGENT, SECRET, "10.66.66.66", 6666);
        assertNull(n, "the agent took an answer made of rgA/rc's PUBLIC certificate and the secret's MACs, and is sent to "
                + (n == null ? "" : n.discovered_ip + ":" + n.discovered_broker_port));
        assertEquals(key(rc), agent.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgA", "rc")));
        assertNotNull(join(agent, rc, DiscoveryType.AGENT), "the real controller no longer joins the agent");
    }

    /** R2: the same for a region and its global. */
    @Test
    void aRegionIsNotSentElsewhereByAHolderOfTheSecretWithItsGlobalsPublicChain() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, true), region = node("rgB", "rc", true, false);
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "precondition");
        DiscoveryNode n = Release378.answer(region, global, DiscoveryType.GLOBAL, SECRET, "10.66.66.66", 6666);
        assertNull(n, "the region took an answer made of its global's PUBLIC certificate and the secret's MACs, and is sent to "
                + (n == null ? "" : n.discovered_ip + ":" + n.discovered_broker_port));
        assertEquals(key(global), region.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgG", "gc")));
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "the real global no longer joins the region");
    }
}
