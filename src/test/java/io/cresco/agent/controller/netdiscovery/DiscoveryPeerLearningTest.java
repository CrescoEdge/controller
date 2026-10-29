package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Discovery v2: what a certified exchange teaches each side (pinned key, bridge peer) happens only after both sides proved
 * the secret over fresh nonces; a replay, a man in the middle or a wrong secret records nothing; the legacy single-message
 * exchange is refused unless discovery_auth=compat.
 */
class DiscoveryPeerLearningTest {

    private static final String SECRET = "test-secret-not-real";
    private static final Gson GSON = new Gson();

    private static Map<String, Object> secrets(String secret, String... extra) {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", secret);
        m.put("discovery_secret_region", secret);
        m.put("discovery_secret_global", secret);
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        return m;
    }

    private static ControllerEngine responder(String region, String agent, Map<String, Object> cfg) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = true;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) {
        return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class);
    }

    private static PeerTrustRegistry reg(ControllerEngine e) {
        return e.getCertificateManager().getPeerTrust();
    }

    /** A full exchange; the client's result node, or null. */
    private static DiscoveryNode exchange(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "client");
        return c.clientResult(hello, copy(ch), copy(result), "127.0.0.1", 32005);
    }

    @Test
    void anAuthenticatedExchangeTeachesBothSides() throws Exception {
        ControllerEngine global = responder("rgG", "gc", secrets(SECRET));
        ControllerEngine region = responder("rgB", "rc", secrets(SECRET));
        DiscoveryNode n = exchange(region, global, DiscoveryType.GLOBAL);
        assertNotNull(n);
        assertEquals("rgG", n.discovered_region);
        assertEquals("gc", n.discovered_agent);
        assertNotNull(reg(region).pinnedKey(new PeerIdentity.Expected("rgG", "gc")), "the client did not pin the responder");
        assertTrue(reg(region).isBridgePeer(CrescoIdentity.of("t1", "rgG", "gc", null)));
        assertNotNull(reg(global).pinnedKey(new PeerIdentity.Expected("rgB", "rc")), "the responder did not pin the client");
        assertTrue(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));

        ControllerEngine agent = TlsTestSupport.engine("rgG", "a1", secrets(SECRET));
        assertNotNull(exchange(agent, global, DiscoveryType.AGENT));
        assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgG", "a1", null)), "an agent-secret exchange made a bridge peer");
    }

    @Test
    void aWrongSecretRecordsNothingOnEitherSide() throws Exception {
        ControllerEngine global = responder("rgG", "gc", secrets(SECRET));
        ControllerEngine intruder = responder("rgX", "rc", secrets("another-secret"));
        DiscoveryProcessor c = new DiscoveryProcessor(intruder), s = new DiscoveryProcessor(global);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "intruder");
        assertNull(c.clientProof(hello, copy(ch), "global"), "a responder with another secret was accepted");
        DiscoveryAuth.Msg forged = new DiscoveryAuth.Msg();
        forged.step = "proof";
        forged.type = "GLOBAL";
        forged.mac = DiscoveryAuth.mac(DiscoveryAuth.key("another-secret", "handshake|GLOBAL"), "C",
                DiscoveryAuth.transcript(hello, ch, null, null));
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, forged, "intruder");
        assertFalse(r.ok);
        assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgX", "rc", null)));
        assertNull(reg(global).pinnedKey(new PeerIdentity.Expected("rgX", "rc")));
    }

    @Test
    void aReplayedHelloAndProofRecordNothing() throws Exception {
        ControllerEngine global = responder("rgG", "gc", secrets(SECRET));
        ControllerEngine region = responder("rgB", "rc", secrets(SECRET));
        DiscoveryProcessor c = new DiscoveryProcessor(region), s = new DiscoveryProcessor(global);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "region");
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "global");
        assertNotNull(proof);                                       // observed on the wire, never delivered
        // an attacker replays the observed hello and proof in a new connection
        DiscoveryAuth.Msg ch2 = s.serverChallenge(copy(hello), "attacker");
        assertNotEquals(ch.ns, ch2.ns);
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch2, copy(proof), "attacker");
        assertFalse(r.ok, "a replayed proof was accepted");
        assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)), "a replay registered a bridge peer");
        assertNull(reg(global).pinnedKey(new PeerIdentity.Expected("rgB", "rc")), "a replay pinned a key");
    }

    @Test
    void aManInTheMiddleCannotSwapTheCertificateOrRedirectThePort() throws Exception {
        ControllerEngine global = responder("rgG", "gc", secrets(SECRET));
        ControllerEngine region = responder("rgB", "rc", secrets(SECRET));
        ControllerEngine mitm = responder("rgB", "rc", secrets("unknown"));      // same names, its own key
        DiscoveryProcessor c = new DiscoveryProcessor(region), s = new DiscoveryProcessor(global);

        // client -> server: the certificate in the hello is swapped for the attacker's (naming the same identity)
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg tampered = copy(hello);
        tampered.ccert = mitm.getCertificateManager().getJsonFromCerts(mitm.getCertificateManager().getPublicCertificate());
        DiscoveryAuth.Msg ch = s.serverChallenge(tampered, "client");
        assertNull(c.clientProof(hello, copy(ch), "server"), "the client accepted a challenge over a swapped certificate");
        DiscoveryAuth.Msg r = s.serverResult(tampered, ch, c.clientProof(hello, copy(s.serverChallenge(copy(hello), "client")), "server"), "client");
        assertFalse(r.ok, "the server accepted a proof made over another certificate");
        assertNull(reg(global).pinnedKey(new PeerIdentity.Expected("rgB", "rc")));

        // server -> client: the advertised broker port is changed in flight
        DiscoveryAuth.Msg hello2 = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch2 = copy(s.serverChallenge(copy(hello2), "client"));
        ch2.sport = 1;
        assertNull(c.clientProof(hello2, ch2, "server"), "the client accepted a redirected broker port");
    }

    @Test
    void theLegacySingleMessageCertifyIsAgentMembershipOnlyAndOnlyWithCompat() throws Exception {
        for (String mode : new String[]{"v2", "compat"}) {
            ControllerEngine me = responder("rgG", "gc", secrets(SECRET, DiscoveryAuth.MODE_KEY, mode));
            DiscoveryProcessor dp = new DiscoveryProcessor(me);
            DiscoveryCrypto legacyCrypto = new DiscoveryCrypto(TlsTestSupport.engine("rgX", "x", secrets(SECRET, DiscoveryAuth.MODE_KEY, "compat")));
            // a legacy region join: never accepted, compat or not (it would make a bridge peer from a replayable token)
            ControllerEngine regionCtl = TlsTestSupport.engine("rgB", "rc", secrets(SECRET));
            DiscoveryNode join = new DiscoveryNode(DiscoveryType.REGION);
            join.setCertify(legacyCrypto.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET),
                    regionCtl.getCertificateManager().getJsonFromCerts(regionCtl.getCertificateManager().getPublicCertificate()), "rgB", "rc");
            DiscoveryNode out = dp.processIncomingBroadCastDiscovery(join, "127.0.0.1", 32005, "127.0.0.1", 40001);
            assertTrue(out == null || out.nodeType != DiscoveryNode.NodeType.CERTIFIED, "a legacy REGION certify was certified (" + mode + ")");
            assertFalse(reg(me).isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)), "a legacy certify made a bridge peer (" + mode + ")");
            // a legacy agent join: membership, only with compat
            ControllerEngine agent = TlsTestSupport.engine("rgG", "a7", secrets(SECRET));
            DiscoveryNode aj = new DiscoveryNode(DiscoveryType.AGENT);
            aj.setCertify(legacyCrypto.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET),
                    agent.getCertificateManager().getJsonFromCerts(agent.getCertificateManager().getPublicCertificate()), "rgG", "a7");
            DiscoveryNode aout = dp.processIncomingBroadCastDiscovery(aj, "127.0.0.1", 32005, "127.0.0.1", 40002);
            boolean certified = aout != null && aout.nodeType == DiscoveryNode.NodeType.CERTIFIED;
            assertEquals(mode.equals("compat"), certified, "legacy agent membership with discovery_auth=" + mode);
        }
    }

    @Test
    void validatorsAreFreshAndLegacyOnesAreRefusedByDefault() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        DiscoveryCrypto v2 = new DiscoveryCrypto(e);
        String a = v2.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET), b = v2.encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET);
        assertNotEquals(a, b);
        assertEquals("DISCOVERY_MESSAGE_VERIFIED", v2.decrypt(a, SECRET));
        assertNull(v2.decrypt(a, "another"));
        String legacy = new DiscoveryCrypto(TlsTestSupport.engine("rgA", "x", Map.of(DiscoveryAuth.MODE_KEY, "compat")))
                .encrypt("DISCOVERY_MESSAGE_VERIFIED", SECRET);
        assertNull(v2.decrypt(legacy, SECRET), "a legacy (replayable) validator was accepted with discovery_auth=v2");
    }

    @Test
    void theJoiningSideRefusesAResponderOutsideItsRegionOrTenant() throws Exception {
        ControllerEngine otherRegion = responder("rgB", "rc", secrets(SECRET));
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", secrets(SECRET));        // joined (adopted) region rgA
        assertNull(exchange(agent, otherRegion, DiscoveryType.AGENT), "an agent accepted a responder answering for another region");
        assertNull(reg(agent).pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        ControllerEngine otherTenant = responder("rgA", "rc", secrets(SECRET, "tenant_id", "t2"));
        assertNull(exchange(agent, otherTenant, DiscoveryType.AGENT), "an agent accepted a responder of another tenant");
    }

    @Test
    void anAgentCannotJoinInASuperuserTenant() throws Exception {
        ControllerEngine region = responder("rgA", "rc", secrets(SECRET, "tenant_id", "cresco-system"));
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", secrets(SECRET, "tenant_id", "cresco-system"));
        assertNull(exchange(agent, region, DiscoveryType.AGENT));
        assertNull(reg(region).verifiedIdentity(agent.getCertificateManager().getPublicCertificate()),
                "an agent was bound in a superuser tenant by claiming it");
    }

    @Test
    void derivedKeysAreBoundedAndNoSecretIsNeverInvented() {
        for (int i = 0; i < DiscoveryAuth.KEY_CACHE + 5; i++) DiscoveryAuth.key("secret-" + i, "validator");
        assertTrue(DiscoveryAuth.cachedKeys() <= DiscoveryAuth.KEY_CACHE, "the derived-key cache grew past its bound");
        assertThrows(IllegalArgumentException.class, () -> DiscoveryAuth.key(null, "validator"));
    }
}
