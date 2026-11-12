package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.CrescoIdentity;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the proof of possession covers, field by field. In each test a holder of the discovery secret sits BETWEEN an honest
 * joiner and an honest responder, alters exactly one thing at the nonces both chose, and recomputes every MAC so that each
 * side's secret-keyed checks pass: only the signatures can then tell, and they must. (A replay test that also changes a
 * nonce guards nothing but the nonce: a proof reduced to the nonces and the two keys passed every test of the first
 * round.) Also: the proof's algorithm does not depend on a node's crypto policy; a signature is not taken in the other
 * direction; and a peer that says it proves, and does not, is refused even where no proof would have been admitted.
 */
class DiscoveryProofFieldsTest {

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
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = controller;
        st.global = global;
        if (controller && !global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }
    private static X509Certificate[] chain(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate(); }
    private static String json(ControllerEngine any, X509Certificate[] chain) { return any.getCertificateManager().getJsonFromCerts(chain); }
    private static String pinned(ControllerEngine at, String region, String agent) {
        return at.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected(region, agent));
    }

    private static X509Certificate cert(X500Name subject, X500Name issuer, PublicKey key, java.security.PrivateKey signer, boolean ca) throws Exception {
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(issuer, new BigInteger(64, new SecureRandom()),
                new Date(now - 60_000L), new Date(now + 86_400_000L), subject, key);
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").build(signer)));
    }

    /** What a holder of the secret makes without anybody's private key: [a leaf carrying {@code key} under a name of its choice, its own root]. */
    private static X509Certificate[] forged(PublicKey key, String tenant, String region, String agent) throws Exception {
        KeyPair root = TlsTestSupport.rsa();
        X500Name rn = new X500Name("CN=rootCA-between");
        return new X509Certificate[]{cert(new X500Name(CrescoIdentity.of(tenant, region, agent, null).toX500Name()), rn, key, root.getPrivate(), false),
                cert(rn, rn, root.getPublic(), root.getPrivate(), true)};
    }

    /**
     * A holder of the secret between an honest joiner and an honest responder. It may alter the HELLO on its way to the
     * responder, the CHALLENGE and the RESULT on their way to the joiner; it then recomputes the MACs "S1", "C" and "S2" for
     * each side's own view (it holds the secret), and leaves both nonces and both signatures as they were made.
     */
    private static final class Between {
        final ControllerEngine client, server;
        final DiscoveryType type;
        Consumer<DiscoveryAuth.Msg> hello = m -> { }, challenge = m -> { }, result = m -> { };
        DiscoveryAuth.Msg answered;      // the responder's RESULT, as it made it
        DiscoveryNode taken;             // what the joiner took from the exchange (null: nothing)

        Between(ControllerEngine client, ControllerEngine server, DiscoveryType type) { this.client = client; this.server = server; this.type = type; }

        private X509Certificate leaf(String chainJson) { return client.getCertificateManager().getCertsfromJson(chainJson)[0]; }

        Between run() {
            DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
            DiscoveryAuth.Msg h = c.clientHello(type);
            DiscoveryAuth.Msg hThere = copy(h);
            hello.accept(hThere);
            DiscoveryAuth.Msg ch = s.serverChallenge(copy(hThere), "10.6.6.6");
            assertNotNull(ch, "(the responder did not answer the hello it was given)");
            byte[] kHere = DiscoveryAuth.key(SECRET, "handshake|" + h.type), kThere = DiscoveryAuth.key(SECRET, "handshake|" + hThere.type);
            DiscoveryAuth.Msg chHere = copy(ch);
            chHere.type = h.type;
            challenge.accept(chHere);
            chHere.mac = DiscoveryAuth.mac(kHere, "S1", DiscoveryAuth.transcript(h, chHere, leaf(h.ccert), leaf(chHere.scert), false));
            DiscoveryAuth.Msg proof = c.clientProof(h, copy(chHere), "10.6.6.6");
            assertNotNull(proof, "(the joiner did not take the challenge: this test's man in the middle did not re-MAC it right)");
            DiscoveryAuth.Msg proofThere = copy(proof);
            proofThere.type = hThere.type;
            proofThere.mac = DiscoveryAuth.mac(kThere, "C", DiscoveryAuth.transcript(hThere, ch, leaf(hThere.ccert), leaf(ch.scert), true));
            answered = s.serverResult(copy(hThere), ch, proofThere, "10.6.6.6");
            DiscoveryAuth.Msg rHere = copy(answered);
            rHere.type = h.type;
            result.accept(rHere);
            rHere.mac = DiscoveryAuth.mac(kHere, "S2", DiscoveryAuth.transcript(h, chHere, leaf(h.ccert), leaf(chHere.scert), true), DiscoveryAuth.resultDigest(rHere));
            taken = c.clientResult(h, copy(chHere), rHere, "10.6.6.6", 32005);
            return this;
        }

        /** The RESPONDER saw that the joiner's signature does not cover what it was shown. */
        void responderRefusedTheProof(String what) {
            assertFalse(answered.ok, what + ": the responder took a joiner's signature made over something else");
            assertTrue(answered.reason != null && answered.reason.contains("no proof of the key"), what + ": " + answered.reason);
        }

        /** The responder granted (it saw nothing wrong) and the JOINER saw that the responder's signature does not cover what it was shown. */
        void joinerRefusedTheProof(String what) {
            assertTrue(answered.ok, what + ": (the responder was to grant: " + answered.reason + ")");
            assertNull(taken, what + ": the joiner took a responder's signature made over something else");
        }
    }

    private static Between between(ControllerEngine client, ControllerEngine server, DiscoveryType type) { return new Between(client, server, type); }

    /** The man in the middle of these tests, altering nothing: both sides are satisfied (so each refusal below is the alteration's). */
    @Test
    void theManInTheMiddleThatAltersNothingIsNotSeen() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, true), region = node("rgB", "rc", true, false);
        Between b = between(region, global, DiscoveryType.GLOBAL).run();
        assertTrue(b.answered.ok, b.answered.reason);
        assertNotNull(b.taken);
    }

    /** The exchange type, and with it each side's role: a signature made in one type of exchange is not taken in another. */
    @Test
    void theExchangeTypeAndTheRolesAreCovered() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, true), region = node("rgB", "rc", true, false);
        // GLOBAL shown to the responder as REGION (the responder's role: global -> controller)
        Between g = between(region, global, DiscoveryType.GLOBAL);
        g.hello = m -> m.type = "REGION";
        g.run().responderRefusedTheProof("exchange type GLOBAL shown as REGION");
        // REGION shown to the responder as AGENT (the joiner's role: controller -> agent)
        ControllerEngine peer = node("rgP", "rc", true, false);
        Between r = between(region, peer, DiscoveryType.REGION);
        r.hello = m -> m.type = "AGENT";
        r.run().responderRefusedTheProof("exchange type REGION shown as AGENT");
        assertNull(pinned(global, "rgB", "rc"));
        assertNull(pinned(peer, "rgB", "rc"));
    }

    /** The joiner's region, agent, tenant, key and chain: the joiner's signature shown to the responder with any of them altered. */
    @Test
    void theJoinersNameTenantKeyAndChainAreCovered() throws Exception {
        ControllerEngine region = node("rgB", "rc", true, false);
        PublicKey its = chain(region)[0].getPublicKey();
        Object[][] cases = {
                {"another agent name", "rgB", "zz", forged(its, "t1", "rgB", "zz")},
                {"another region", "rgC", "rc", forged(its, "t1", "rgC", "rc")},
                {"another tenant", "rgB", "rc", forged(its, "t2", "rgB", "rc")},
                {"another key", "rgB", "rc", forged(TlsTestSupport.rsa().getPublic(), "t1", "rgB", "rc")},
                {"its chain with one more certificate", "rgB", "rc", new X509Certificate[]{chain(region)[0], chain(region)[chain(region).length - 1], forged(its, "t1", "x", "y")[1]}},
        };
        for (Object[] k : cases) {
            ControllerEngine global = node("rgG", "gc", true, true);
            Between b = between(region, global, DiscoveryType.GLOBAL);
            b.hello = m -> { m.creg = (String) k[1]; m.cagt = (String) k[2]; m.ccert = json(global, (X509Certificate[]) k[3]); };
            b.run().responderRefusedTheProof("the joiner shown with " + k[0]);
            assertNull(pinned(global, (String) k[1], (String) k[2]), (String) k[0]);
        }
    }

    /** The responder's region, agent, tenant, key and chain, its broker port and the rebind statement it served: covered by BOTH signatures. */
    @Test
    void theRespondersNameTenantKeyChainPortAndRebindStatementAreCovered() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, true);
        PublicKey its = chain(global)[0].getPublicKey();
        Map<String, Consumer<DiscoveryAuth.Msg>> cases = new java.util.LinkedHashMap<>();
        X509Certificate[] otherName = forged(its, "t1", "rgH", "gc"), otherAgent = forged(its, "t1", "rgG", "zz"), otherTenant = forged(its, "t2", "rgG", "gc");
        X509Certificate[] otherKey = forged(TlsTestSupport.rsa().getPublic(), "t1", "rgG", "gc");
        X509Certificate[] longer = {chain(global)[0], chain(global)[chain(global).length - 1], otherName[1]};
        cases.put("another region", m -> { m.sreg = "rgH"; m.scert = json(global, otherName); });
        cases.put("another agent name", m -> { m.sagt = "zz"; m.scert = json(global, otherAgent); });
        cases.put("another tenant", m -> m.scert = json(global, otherTenant));
        cases.put("another key", m -> m.scert = json(global, otherKey));
        cases.put("its chain with one more certificate", m -> m.scert = json(global, longer));
        cases.put("another broker port", m -> m.sport = 6666);
        cases.put("a rebind statement it did not serve", m -> m.rebind = "{\"anything\":\"a holder of the secret adds\"}");
        int n = 0;
        for (Map.Entry<String, Consumer<DiscoveryAuth.Msg>> k : cases.entrySet()) {
            ControllerEngine region = node("rgB" + (n++), "rc", true, false);
            Between b = between(region, global, DiscoveryType.GLOBAL);
            b.challenge = k.getValue();
            // the JOINER signs what it was shown of the responder: the responder sees that this is not what it sent (and the
            // joiner, shown the responder's signature over what it did send, takes nothing either)
            b.run().responderRefusedTheProof("the responder shown to the joiner with " + k.getKey());
            assertNull(b.taken, k.getKey());
            assertNull(pinned(region, "rgG", "gc"), k.getKey());
            assertNull(pinned(region, "rgH", "gc"), k.getKey());
            assertNull(pinned(region, "rgG", "zz"), k.getKey());
        }
    }

    /** The result: granted or refused, its reason, the chain it returns. */
    @Test
    void theResultIsCovered() throws Exception {
        // (the joiners' upgrade window is open: without the proof they WOULD take a responder -- so it is the signature, checked
        // by the joiner, that refuses here, not the registry's "nothing new without proof")
        String[] window = {DiscoveryAuth.MODE_KEY, "compat", "discovery_compat_until", WINDOW_END};
        ControllerEngine global = node("rgG", "gc", true, true);
        ControllerEngine r1 = node("rgB", "rc", true, false, window), r2 = node("rgC", "rc", true, false, window);
        Between reason = between(r1, global, DiscoveryType.GLOBAL);
        reason.result = m -> m.reason = "a reason the responder did not give";
        reason.run().joinerRefusedTheProof("the result shown with another reason");
        Between rcert = between(r2, global, DiscoveryType.GLOBAL);
        String another = json(global, forged(TlsTestSupport.rsa().getPublic(), "t1", "rgG", "gc"));
        rcert.result = m -> m.rcert = another;
        rcert.run().joinerRefusedTheProof("the result shown with another returned chain");
        // a REFUSAL shown as granted: the second controller of a region
        ControllerEngine second = node("rgB", "zz", true, false, window);
        Between ok = between(second, global, DiscoveryType.GLOBAL);
        ok.result = m -> { m.ok = true; m.reason = null; m.rcert = json(global, chain(global)); };
        ok.run();
        assertFalse(ok.answered.ok, "(the responder was to refuse a second controller of region rgB)");
        assertNull(ok.taken, "a refusal shown as granted was taken");
        assertNull(pinned(second, "rgG", "gc"));
    }

    /** The two signatures have labels of their own: one made by a key as the joiner is not one made by it as the responder. */
    @Test
    void aSignatureIsNotTakenInTheOtherDirection() throws Exception {
        KeyPair k = TlsTestSupport.rsa();
        byte[] t = "the same transcript".getBytes(StandardCharsets.UTF_8);
        String asJoiner = DiscoveryAuth.proofSign(k.getPrivate(), DiscoveryAuth.POP_CLIENT, t), asResponder = DiscoveryAuth.proofSign(k.getPrivate(), DiscoveryAuth.POP_RESULT, t);
        assertTrue(DiscoveryAuth.proofVerify(k.getPublic(), asJoiner, DiscoveryAuth.POP_CLIENT, t));
        assertTrue(DiscoveryAuth.proofVerify(k.getPublic(), asResponder, DiscoveryAuth.POP_RESULT, t));
        assertFalse(DiscoveryAuth.proofVerify(k.getPublic(), asJoiner, DiscoveryAuth.POP_RESULT, t), "a joiner's signature verifies as a responder's");
        assertFalse(DiscoveryAuth.proofVerify(k.getPublic(), asResponder, DiscoveryAuth.POP_CLIENT, t), "a responder's signature verifies as a joiner's");
        // the reflection: a holder of the secret answers a joiner AS A CONTROLLER THAT CARRIES THE JOINER'S OWN KEY, and hands the
        // joiner's signature back to it as the responder's (the joiner's upgrade window is open: only the signature refuses)
        ControllerEngine region = node("rgB", "rc", true, false, DiscoveryAuth.MODE_KEY, "compat", "discovery_compat_until", WINDOW_END);
        DiscoveryProcessor c = new DiscoveryProcessor(region);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        X509Certificate[] mirror = forged(chain(region)[0].getPublicKey(), "t1", "rgZ", "gz");
        DiscoveryAuth.Msg ch = new DiscoveryAuth.Msg();
        ch.step = "challenge"; ch.type = hello.type; ch.nc = hello.nc; ch.ns = DiscoveryAuth.proofNonce();
        ch.sreg = "rgZ"; ch.sagt = "gz"; ch.scert = json(region, mirror); ch.sport = 32010;
        byte[] key = DiscoveryAuth.key(SECRET, "handshake|" + hello.type);
        X509Certificate cleaf = chain(region)[0];
        ch.mac = DiscoveryAuth.mac(key, "S1", DiscoveryAuth.transcript(hello, ch, cleaf, mirror[0], false));
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.6.6.6");
        assertNotNull(proof);
        DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
        r.step = "result"; r.type = hello.type; r.ok = true; r.rcert = ch.scert;
        r.pop = proof.pop;                                                  // the joiner's own signature, reflected
        r.mac = DiscoveryAuth.mac(key, "S2", DiscoveryAuth.transcript(hello, ch, cleaf, mirror[0], true), DiscoveryAuth.resultDigest(r));
        assertNull(c.clientResult(hello, copy(ch), copy(r), "10.6.6.6", 32005), "a joiner took its own signature, reflected, as the responder's proof");
        assertNull(pinned(region, "rgZ", "gz"));
    }

    private static List<String> fields(byte[] transcript) {
        List<String> out = new ArrayList<>();
        ByteBuffer b = ByteBuffer.wrap(transcript);
        while (b.remaining() >= 4) {
            byte[] f = new byte[b.getInt()];
            b.get(f);
            out.add(new String(f, StandardCharsets.UTF_8));
        }
        return out;
    }

    /** What is signed, exactly: every field, in its place (a field dropped from the proof fails here whatever else covers it). */
    @Test
    void theSignedTranscriptHoldsEveryField() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, true), agent = node("rgG", "a1", false, false);
        for (DiscoveryType type : new DiscoveryType[]{DiscoveryType.AGENT, DiscoveryType.REGION, DiscoveryType.GLOBAL}) {
            ControllerEngine joiner = type == DiscoveryType.AGENT ? agent : node("rgB", "rc" + type.ordinal(), true, false);
            DiscoveryProcessor c = new DiscoveryProcessor(joiner), s = new DiscoveryProcessor(global);
            DiscoveryAuth.Msg hello = c.clientHello(type);
            DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
            assertNotNull(ch, type.name());
            ch.rebind = "a statement";
            X509Certificate cleaf = chain(joiner)[0], sleaf = chain(global)[0];
            List<String> expected = List.of("cresco-discovery-pop-v1", type.name(), hello.nc, ch.ns,
                    "joiner", type == DiscoveryType.AGENT ? "agent" : "controller", joiner.cstate.getRegion(), joiner.cstate.getAgent(), "t1",
                    DiscoveryAuth.fp(cleaf), DiscoveryAuth.chainFp(hello.ccert),
                    "responder", type == DiscoveryType.GLOBAL ? "global" : "controller", "rgG", "gc", "t1",
                    DiscoveryAuth.fp(sleaf), DiscoveryAuth.chainFp(ch.scert), Integer.toString(ch.sport), "rebind", DiscoveryAuth.chainFp("a statement"));
            assertEquals(expected, fields(DiscoveryAuth.possessionTranscript(hello, ch, cleaf, sleaf)), type.name());
            assertTrue(DiscoveryAuth.claimsProof(hello.nc) && DiscoveryAuth.claimsProof(ch.ns));
        }
    }

    /**
     * A peer whose nonce says it proves, and which sends no valid signature, is refused by the EXCHANGE -- also where the
     * registry would have admitted a peer without proof (a first join in the upgrade window): otherwise a missing check of
     * the signature would never show, because the registry refuses what is new without proof anyway.
     */
    @Test
    void aPeerThatSaysItProvesAndDoesNotIsRefusedEvenWhereNoProofWouldBeAdmitted() throws Exception {
        String[] window = {DiscoveryAuth.MODE_KEY, "compat", "discovery_compat_until", WINDOW_END};
        // the joiner's side: its window is open, the responder's signature is removed (or is not its own)
        ControllerEngine global = node("rgG", "gc", true, true), region = node("rgB", "rc", true, false, window);
        assertNotNull(Release378.answer(node("rgB", "r0", true, false, window), global, DiscoveryType.GLOBAL, SECRET, "10.1.2.3", 32010),
                "(precondition: this joiner's window admits a first join WITHOUT proof from a responder that does not say it proves)");
        for (String how : new String[]{"removed", "another key's"}) {
            DiscoveryProcessor c = new DiscoveryProcessor(region), s = new DiscoveryProcessor(global);
            DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
            DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
            DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
            DiscoveryAuth.Msg result = copy(s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3"));
            assertTrue(result.ok, result.reason);
            result.pop = how.equals("removed") ? null : DiscoveryAuth.proofSign(TlsTestSupport.rsa().getPrivate(), DiscoveryAuth.POP_RESULT,
                    DiscoveryAuth.possessionTranscript(hello, ch, chain(region)[0], chain(global)[0]), DiscoveryAuth.resultDigest(result));
            assertNull(c.clientResult(hello, copy(ch), result, "10.9.9.9", 32005), "the joiner did not check the responder's signature (" + how + ")");
            assertNull(pinned(region, "rgG", "gc"), how);
        }
        // the responder's side: its window is open, the joiner's signature is removed
        ControllerEngine openGlobal = node("rgG", "gc", true, true, window), joiner = node("rgC", "rc", true, false);
        DiscoveryProcessor c = new DiscoveryProcessor(joiner), s = new DiscoveryProcessor(openGlobal);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        DiscoveryAuth.Msg proof = copy(c.clientProof(hello, copy(ch), "10.9.9.9"));
        proof.pop = null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, proof, "10.1.2.3");
        assertTrue(!result.ok && result.reason.contains("no proof of the key"), "the responder did not check the joiner's signature: " + result.reason);
        assertNull(pinned(openGlobal, "rgC", "rc"));
    }

    /** The proof's algorithm is fixed per key type (RSA, EC); a key of another type cannot be proved, and its holder is told so by name. */
    @Test
    void theProofsAlgorithmIsFixedPerKeyType() throws Exception {
        assertEquals("SHA384withRSA", DiscoveryAuth.proofAlgorithm("RSA"));
        assertEquals("SHA384withECDSA", DiscoveryAuth.proofAlgorithm("EC"));
        assertNull(DiscoveryAuth.proofAlgorithm("Ed25519"));
        KeyPair ed = java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        assertNull(DiscoveryAuth.proofSign(ed.getPrivate(), DiscoveryAuth.POP_CLIENT, new byte[1]));
        assertFalse(DiscoveryAuth.proofVerify(ed.getPublic(), "AAAA", DiscoveryAuth.POP_CLIENT, new byte[1]));
        KeyPair ec = java.security.KeyPairGenerator.getInstance("EC").generateKeyPair();
        assertTrue(DiscoveryAuth.proofVerify(ec.getPublic(), DiscoveryAuth.proofSign(ec.getPrivate(), DiscoveryAuth.POP_CLIENT, new byte[1]), DiscoveryAuth.POP_CLIENT, new byte[1]));
    }
}
