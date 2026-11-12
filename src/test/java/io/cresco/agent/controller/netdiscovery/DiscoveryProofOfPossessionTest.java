package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proof of possession in the secret's discovery exchange: each side signs the exchange with the key of the leaf it
 * presents, next to the secret's MAC. What is signed cannot be used in another exchange, toward another responder or in
 * another type of exchange; a build that proves says so in its nonce, which every MAC covers, so it cannot be made to look
 * like an earlier build; and a node of release 1.3.378, which sends no proof, is given exactly what it already has.
 */
class DiscoveryProofOfPossessionTest {

    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";
    private static final String WINDOW_END = java.time.LocalDate.now(java.time.ZoneOffset.UTC).plusDays(7).toString();
    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean controller, boolean global, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("broker_security_enabled", "true");
        m.put("msg_signing_sessions", "false");
        m.put("tenant_id", "t1");
        m.put("discovery_proof_grace_days", "0");      // (the grace after a first proof has its own tests: DiscoveryProofGraceTest)
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = controller;
        st.global = global;
        if (controller && !global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static ControllerEngine global(String... extra) throws Exception { return node("rgG", "gc", true, true, extra); }

    private static ControllerEngine windowOpen(String region, String agent, boolean controller, boolean global) throws Exception {
        return node(region, agent, controller, global, DiscoveryAuth.MODE_KEY, "compat", "discovery_compat_until", WINDOW_END);
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }
    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }
    private static X509Certificate[] chain(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate(); }
    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(chain(e)[0]); }
    private static String pinned(ControllerEngine at, String region, String agent) { return reg(at).pinnedKey(new PeerIdentity.Expected(region, agent)); }

    /** One exchange as it ran: every message, and what the client took from it (null: nothing). */
    private static final class Run {
        DiscoveryAuth.Msg hello, ch, proof, result;
        DiscoveryNode node;
    }

    /**
     * A real exchange, every message through its JSON. {@code onTheWay} (optional) sees each message as it arrives at the
     * other side -- "hello" and "proof" at the responder, "challenge" and "result" at the client -- and may alter it.
     */
    private static Run exchange(ControllerEngine client, ControllerEngine server, DiscoveryType type, BiConsumer<String, DiscoveryAuth.Msg> onTheWay) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        Run r = new Run();
        r.hello = c.clientHello(type);
        DiscoveryAuth.Msg helloThere = copy(r.hello);
        if (onTheWay != null) onTheWay.accept("hello", helloThere);
        r.ch = s.serverChallenge(helloThere, "10.1.2.3");
        if (r.ch == null) return r;
        DiscoveryAuth.Msg chHere = copy(r.ch);
        if (onTheWay != null) onTheWay.accept("challenge", chHere);
        r.proof = c.clientProof(r.hello, chHere, "10.9.9.9");
        if (r.proof == null) return r;
        DiscoveryAuth.Msg proofThere = copy(r.proof);
        if (onTheWay != null) onTheWay.accept("proof", proofThere);
        r.result = s.serverResult(helloThere, r.ch, proofThere, "10.1.2.3");
        DiscoveryAuth.Msg resultHere = copy(r.result);
        if (onTheWay != null) onTheWay.accept("result", resultHere);
        r.node = c.clientResult(r.hello, chHere, resultHere, "10.9.9.9", 32005);
        return r;
    }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) { return exchange(client, server, type, null).node; }

    /**
     * A holder of the secret that SAYS it is a build that proves its key (a marked nonce), presents {@code chain} as
     * {@code region/agent} and sends {@code pop} as its signature (one it captured, or none): the responder's RESULT.
     */
    private static DiscoveryAuth.Msg claimingProof(ControllerEngine server, DiscoveryType type, String region, String agent, X509Certificate[] chain,
                                                  String nonce, String pop) {
        DiscoveryProcessor s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = new DiscoveryAuth.Msg();
        hello.step = "hello"; hello.type = type.name(); hello.nc = nonce == null ? DiscoveryAuth.proofNonce() : nonce;
        hello.creg = region; hello.cagt = agent; hello.ccert = server.getCertificateManager().getJsonFromCerts(chain);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.6.6.6");
        if (ch == null) return null;
        byte[] k = DiscoveryAuth.key(SECRET, "handshake|" + hello.type);
        DiscoveryAuth.Msg proof = new DiscoveryAuth.Msg();
        proof.step = "proof"; proof.type = hello.type; proof.pop = pop;
        proof.mac = DiscoveryAuth.mac(k, "C", DiscoveryAuth.transcript(hello, ch, chain[0], server.getCertificateManager().getCertsfromJson(ch.scert)[0], true));
        return s.serverResult(copy(hello), ch, copy(proof), "10.6.6.6");
    }

    // ------------------------------------------------------------------ the proof

    /** Two nodes of this build: both say what they are in their nonces, both sign, and each records that the other proved its key. */
    @Test
    void bothSidesProveTheKeyOfTheCertificateTheyPresent() throws Exception {
        ControllerEngine global = global(), region = node("rgB", "rc", true, false), agent = node("rgB", "a1", false, false);
        Run r = exchange(region, global, DiscoveryType.GLOBAL, null);
        assertNotNull(r.node);
        assertTrue(DiscoveryAuth.claimsProof(r.hello.nc) && DiscoveryAuth.claimsProof(r.ch.ns), "a nonce of this build does not say that it proves its key");
        assertNotNull(r.proof.pop, "the joiner sent no signature by its key");
        assertNotNull(r.result.pop, "the responder sent no signature by its key");
        assertNull(r.ch.pop, "(the responder signs only for a client whose MAC verified: nothing in the challenge)");
        assertTrue(reg(global).provedKey("rgB", "rc"), "the global did not record that the region's controller proved its key");
        assertTrue(reg(region).provedKey("rgG", "gc"), "the region did not record that its global proved its key");
        assertEquals("rgG/gc", reg(region).globalRecorded());
        assertNotNull(join(agent, region, DiscoveryType.AGENT));
        assertTrue(reg(region).provedKey("rgB", "a1") && reg(agent).provedKey("rgB", "rc"));
        // a refusal is signed as well: the joiner knows who refused it
        Run no = exchange(node("rgB", "zz", true, false), global, DiscoveryType.GLOBAL, null);
        assertNull(no.node);
        assertTrue(!no.result.ok && no.result.pop != null);
    }

    /** A signature made for one exchange verifies in no other: not toward another responder, not in another type of exchange, not in a second exchange. */
    @Test
    void aProofIsNotReplayedTowardAnotherResponderOrExchange() throws Exception {
        ControllerEngine global = global(), other = node("rgH", "gc", true, true), region = node("rgB", "rc", true, false);
        Run real = exchange(region, global, DiscoveryType.GLOBAL, null);
        assertNotNull(real.node);
        String captured = real.proof.pop;
        // toward ANOTHER responder: the region's public chain, the secret's MACs, and the signature the region made for the global
        DiscoveryAuth.Msg r = claimingProof(other, DiscoveryType.GLOBAL, "rgB", "rc", chain(region), null, captured);
        assertFalse(r.ok, "a proof made for rgG/gc was taken by rgH/gc");
        assertTrue(r.reason.contains("no proof of the key"), r.reason);
        assertNull(pinned(other, "rgB", "rc"));
        // with the very nonce of the first exchange, too (the responder's nonce is its own, fresh)
        assertFalse(claimingProof(other, DiscoveryType.GLOBAL, "rgB", "rc", chain(region), real.hello.nc, captured).ok);
        // toward the same responder in another TYPE of exchange, and in a second exchange of the same type
        assertFalse(claimingProof(global, DiscoveryType.REGION, "rgB", "rc", chain(region), real.hello.nc, captured).ok,
                "a proof made in a GLOBAL exchange was taken in a REGION exchange");
        assertFalse(claimingProof(global, DiscoveryType.GLOBAL, "rgB", "rc", chain(region), real.hello.nc, captured).ok,
                "a proof was taken a second time");
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "(the real exchange still works)");
        // under another name, with the same key's certificate re-issued by the attacker: the signature covers the name and the chain
        // (and the responder's signature likewise: one made for another exchange is refused by the client)
        ControllerEngine agent = node("rgB", "a1", false, false);
        Run first = exchange(agent, region, DiscoveryType.AGENT, null);
        assertNotNull(first.node);
        ControllerEngine agent2 = node("rgB", "a2", false, false);
        Run swapped = exchange(agent2, region, DiscoveryType.AGENT, (step, m) -> { if (step.equals("result")) m.pop = first.result.pop; });
        assertNull(swapped.node, "a responder's signature made for another exchange was taken");
        assertNull(pinned(agent2, "rgB", "rc"));
    }

    /** A build that proves cannot be made to look like an earlier one, and a missing proof is never read as "an earlier build". */
    @Test
    void aDowngradeIsRefused() throws Exception {
        // the joiner's signature removed on the way: its nonce says it proves, so nothing of it is recorded
        ControllerEngine global = global(), region = node("rgB", "rc", true, false);
        Run noPop = exchange(region, global, DiscoveryType.GLOBAL, (step, m) -> { if (step.equals("proof")) m.pop = null; });
        assertNull(noPop.node);
        assertTrue(!noPop.result.ok && noPop.result.reason.contains("no proof of the key"), noPop.result.reason);
        assertNull(pinned(global, "rgB", "rc"), "a joiner that says it proves its key, and did not, was recorded");
        // the responder's signature removed on the way: the joiner records nothing
        Run noSig = exchange(region, global, DiscoveryType.GLOBAL, (step, m) -> { if (step.equals("result")) m.pop = null; });
        assertNull(noSig.node, "a responder that says it proves its key, and did not, was taken");
        assertNull(pinned(region, "rgG", "gc"));
        // what a build says it is (its nonce) changed on the way, by someone without the secret: the MACs no longer agree
        Run helloStripped = exchange(region, global(), DiscoveryType.GLOBAL,
                (step, m) -> { if (step.equals("hello")) m.nc = m.nc.substring(DiscoveryAuth.PROOF_NONCE.length()); });
        assertNull(helloStripped.node, "the joiner's nonce was changed on the way and the exchange still completed");
        Run chStripped = exchange(region, global(), DiscoveryType.GLOBAL,
                (step, m) -> { if (step.equals("challenge")) m.ns = m.ns.substring(DiscoveryAuth.PROOF_NONCE.length()); });
        assertNull(chStripped.node, "the responder's nonce was changed on the way and the exchange still completed");
        assertNull(pinned(region, "rgG", "gc"));
        // a holder of the secret that says it is this build, with a certificate it has no key for, and no signature at all
        DiscoveryAuth.Msg r = claimingProof(global, DiscoveryType.GLOBAL, "rgC", "rc", chain(node("rgC", "rc", true, false)), null, null);
        assertTrue(!r.ok && r.reason.contains("no proof of the key"), r.reason);
        // once a node has proved its key, the same name and key WITHOUT proof (as release 1.3.378 sends it) is refused, on both sides
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        DiscoveryAuth.Msg back = Release378.join(region, global, DiscoveryType.GLOBAL, SECRET);
        assertFalse(back.ok, "a name and key that proved itself was taken without proof afterwards");
        assertTrue(back.reason.contains("proved its key there before"), back.reason);
        assertNull(Release378.answer(region, global, DiscoveryType.GLOBAL, SECRET, "10.1.2.3", 32010));
        assertNotNull(join(region, global, DiscoveryType.GLOBAL), "the real exchange no longer works");
    }

    // ------------------------------------------------------------------ a node of release 1.3.378 (no proof)

    /** A JOINER of release 1.3.378 at a responder of this build. */
    @Test
    void aJoinerOfRelease378IsAcceptedAsRecordedAndIsNotRecordedAnew() throws Exception {
        ControllerEngine global = global(), old = node("rgB", "rc", true, false), unknown = node("rgC", "rc", true, false);
        // the global's registry as release 1.3.378 left it: rgB/rc bound, a bridge peer
        assertNull(reg(global).pin("rgB", "rc", chain(old)[0], PeerTrustRegistry.ROLE_CONTROLLER, false));
        reg(global).allowBridgePeer("rgB", "rc");
        // (ii) its re-join, exactly as recorded: accepted as before
        DiscoveryAuth.Msg r = Release378.join(old, global, DiscoveryType.GLOBAL, SECRET);
        assertTrue(r.ok, "a recorded controller of release 1.3.378 no longer joins: " + r.reason);
        assertFalse(reg(global).provedKey("rgB", "rc"));
        // (i) a FIRST join without proof: refused, and the joiner is told to upgrade (or the window)
        DiscoveryAuth.Msg first = Release378.join(unknown, global, DiscoveryType.GLOBAL, SECRET);
        assertFalse(first.ok, "a first join without proof of the key was recorded");
        assertTrue(first.reason.contains("UPGRADE rgC/rc") && first.reason.contains("upgrade window") && first.reason.contains("discovery_compat_until"), first.reason);
        assertNull(pinned(global, "rgC", "rc"));
        // ... another key for the recorded name, another name of the recorded key, another controller of the region: refused
        assertFalse(Release378.join(global, DiscoveryType.GLOBAL, SECRET, "rgB", "rc", chain(node("rgB", "rc", true, false))).ok);
        assertFalse(Release378.join(global, DiscoveryType.GLOBAL, SECRET, "rgB", "zz", chain(node("rgB", "zz", true, false))).ok);
        assertEquals(key(old), pinned(global, "rgB", "rc"));
        // an agent of release 1.3.378 at its (upgraded) controller: the same
        ControllerEngine rc = node("rgA", "rc", true, false), a1 = node("rgA", "a1", false, false), a2 = node("rgA", "a2", false, false);
        assertNull(reg(rc).pin("rgA", "a1", chain(a1)[0], PeerTrustRegistry.ROLE_AGENT, false));
        assertTrue(Release378.join(a1, rc, DiscoveryType.AGENT, SECRET).ok);
        assertFalse(Release378.join(a2, rc, DiscoveryType.AGENT, SECRET).ok);
        // once upgraded, it proves its key: recorded as proved, and from then on never taken without
        assertNotNull(join(old, global, DiscoveryType.GLOBAL));
        assertTrue(reg(global).provedKey("rgB", "rc"));
        assertFalse(Release378.join(old, global, DiscoveryType.GLOBAL, SECRET).ok);
    }

    /** (iv) A fabric of release 1.3.378 joining a NEW global of this build: the upgrade window admits first joins, and nothing more. */
    @Test
    void theUpgradeWindowAdmitsAFirstJoinWithoutProofAndNothingElse() throws Exception {
        ControllerEngine global = windowOpen("rgG", "gc", true, true), old = node("rgB", "rc", true, false);
        DiscoveryAuth.Msg first = Release378.join(old, global, DiscoveryType.GLOBAL, SECRET);
        assertTrue(first.ok, "the upgrade window did not admit a first join of release 1.3.378: " + first.reason);
        assertEquals(key(old), pinned(global, "rgB", "rc"));
        assertFalse(reg(global).provedKey("rgB", "rc"));
        assertTrue(Release378.join(old, global, DiscoveryType.GLOBAL, SECRET).ok);
        // not: another controller of that region, another name of its key, another key for its name
        DiscoveryAuth.Msg second = Release378.join(global, DiscoveryType.GLOBAL, SECRET, "rgB", "zz", chain(node("rgB", "zz", true, false)));
        assertFalse(second.ok, "the window admitted a SECOND controller of a region without proof");
        assertTrue(second.reason.contains("more than a first join"), second.reason);
        assertFalse(Release378.join(global, DiscoveryType.GLOBAL, SECRET, "rgB", "rc", chain(node("rgB", "rc", true, false))).ok);
        assertEquals(key(old), pinned(global, "rgB", "rc"));
        // a joiner of this build's first discovery of a responder of release 1.3.378: refused, or admitted in ITS window
        ControllerEngine oldController = node("rgA", "rc", true, false);
        ControllerEngine agent = node("rgA", "a1", false, false), agentInWindow = windowOpen("rgA", "a2", false, false);
        assertNull(Release378.answer(agent, oldController, DiscoveryType.AGENT, SECRET, "10.1.2.3", 32010),
                "an agent recorded, at its first discovery, a controller that did not prove its key");
        assertNull(pinned(agent, "rgA", "rc"));
        assertNotNull(Release378.answer(agentInWindow, oldController, DiscoveryType.AGENT, SECRET, "10.1.2.3", 32010));
        assertEquals(key(oldController), pinned(agentInWindow, "rgA", "rc"));
        // the node is upgraded and proves its key: the window no longer matters for it
        assertNotNull(join(old, global, DiscoveryType.GLOBAL));
        assertTrue(reg(global).provedKey("rgB", "rc"));
    }

    /**
     * A RESPONDER of release 1.3.378 that a joiner has recorded is taken as recorded -- and so is anyone who holds the
     * secret and a copy of its certificate, from any address: the joiner then fails at TLS, where the recorded key is
     * needed. That ends the first time the real controller proves its key to the joiner.
     */
    @Test
    void aResponderOfRelease378IsTakenAsRecordedUntilItProvedItsKeyOnce() throws Exception {
        ControllerEngine global = global(), region = node("rgB", "rc", true, false);
        // the region's registry as release 1.3.378 left it
        assertNull(reg(region).pin("rgG", "gc", chain(global)[0], PeerTrustRegistry.ROLE_CONTROLLER, false));
        reg(region).allowBridgePeer("rgG", "gc");
        DiscoveryNode n = Release378.answer(region, global, DiscoveryType.GLOBAL, SECRET, "10.1.2.3", 32010);
        assertNotNull(n, "a region no longer takes its recorded global of release 1.3.378");
        DiscoveryNode elsewhere = Release378.answer(region, global, DiscoveryType.GLOBAL, SECRET, "10.66.66.66", 6666);
        assertNotNull(elsewhere, "(what an unproved pair still allows: this is the documented limit, not a goal)");
        assertEquals("10.66.66.66", elsewhere.discovered_ip);
        assertNull(reg(region).globalRecorded(), "a global that never proved its key was marked as THE global");
        // the global is upgraded: one real exchange, and its name and key are never taken without proof again
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        assertEquals("rgG/gc", reg(region).globalRecorded());
        assertNull(Release378.answer(region, global, DiscoveryType.GLOBAL, SECRET, "10.66.66.66", 6666),
                "after its global proved its key, the region still took an answer without proof");
    }

    /** The refused joiner is always told its own remedy, whatever this node's log has room for. */
    @Test
    void theRefusedJoinerIsToldItsRemedyWhateverTheLogHolds() throws Exception {
        ControllerEngine global = global(), rc = node("rgB", "rc", true, false), replacement = node("rgB", "rc2", true, false);
        assertNotNull(join(rc, global, DiscoveryType.GLOBAL));
        // a flood of refused joiners fills the log state of the global
        for (int i = 0; i < PeerTrustRegistry.REFUSED_MAX + 8; i++) reg(global).refusalLogged(PeerTrustRegistry.KIND_CONTROLLER, "rgB", "x" + i, "k" + i);
        assertFalse(reg(global).refusalLogged(PeerTrustRegistry.KIND_CONTROLLER, "rgB", "y", "ky"), "(the log state is full: this line is not said)");
        Run r = exchange(replacement, global, DiscoveryType.GLOBAL, null);
        assertNull(r.node);
        assertTrue(r.result.reason.contains("'rgB/rc2=" + key(replacement) + "'") && r.result.reason.contains("peer-trust.json.rebind"),
                "the refused replacement controller is not told its rebind line: " + r.result.reason);
    }

    /**
     * E1. The proof's algorithm is fixed per key type; it is not each node's crypto policy. A global that holds the fabric's
     * policy (one that allows no SHA-384 signature) and a joiner that holds none yet sign and verify alike.
     */
    @Test
    void twoNodesWithDifferentCryptoPoliciesProveTheirKeysToEachOther() throws Exception {
        ControllerEngine global = global(), region = node("rgB", "rc", true, false);
        io.cresco.library.crypto.CryptoPolicy before = io.cresco.library.crypto.CryptoPolicy.current();
        String beforeSrc = io.cresco.library.crypto.CryptoPolicy.currentSource();
        io.cresco.library.crypto.CryptoPolicy fabric = io.cresco.library.crypto.CryptoPolicy.of(io.cresco.library.crypto.CryptoPolicy.LEVEL_FIPS140_3, Map.of(io.cresco.library.crypto.CryptoPolicy.Family.SIGNATURE, "SHA512withRSA,SHA512withECDSA"));
        try {
            DiscoveryProcessor c = new DiscoveryProcessor(region), s = new DiscoveryProcessor(global);
            io.cresco.library.crypto.CryptoPolicy.uninstall();                                       // the joiner: no fabric policy yet
            DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
            io.cresco.library.crypto.CryptoPolicy.install(fabric, "config of the global");            // the global
            DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
            io.cresco.library.crypto.CryptoPolicy.uninstall();
            DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
            assertNotNull(proof);
            io.cresco.library.crypto.CryptoPolicy.install(fabric, "config of the global");
            DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
            assertTrue(result.ok, "a joiner of this build with its own key was refused by a global with another crypto policy: " + result.reason);
            io.cresco.library.crypto.CryptoPolicy.uninstall();
            assertNotNull(c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005),
                    "a result signed under one crypto policy was not taken under another");
            assertTrue(reg(global).provedKey("rgB", "rc"));
            assertTrue(reg(region).provedKey("rgG", "gc"));
        } finally {
            if (before == null) io.cresco.library.crypto.CryptoPolicy.uninstall(); else io.cresco.library.crypto.CryptoPolicy.install(before, beforeSrc);
        }
    }
}
