package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A controller that comes back under its name with another key (its identity was lost, or the operator re-issued it), and
 * the agents that were attested by the key before. Every message here is signed as it leaves its node and verified as the
 * receiving node's consumer verifies it (the ping too: on the release, the re-keyed controller refused its own agents'
 * pings -- "the attestation's signature does not verify" -- and with the ping the only way to a new attestation, for ever).
 *
 * <p>An agent signs for the controller that attested it with its certificate, never with that controller's attestation;
 * and it drops an attestation as soon as it sees, verified, that its controller holds another key than the one that made it.
 * Nothing a receiver verifies changed: a message under the former key's attestation is refused wherever it arrives.
 */
class ReKeyedParentTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional, boolean global) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        // every message carries its sender's credential (no session MAC in place of it): the credential is what is tested
        cfg.put("msg_signing_sessions", "false");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        ((TlsTestSupport.State) e.cstate).global = global;
        return e;
    }

    private static ControllerEngine agent(String region, String name, ControllerEngine parent) throws Exception {
        ControllerEngine a = node(region, name, false, false);
        ((TlsTestSupport.State) a.cstate).parentRegion = parent.cstate.getRegion();
        ((TlsTestSupport.State) a.cstate).parentAgent = parent.cstate.getAgent();
        return a;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static Caller controller(ControllerEngine e) { return Caller.controller("t1", e.cstate.getRegion(), e.cstate.getAgent()); }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    /**
     * {@code agent} joins {@code controller}, or follows it to its new key (the operator's rebind, or its PKI): each holds
     * the other's key, as the discovery exchange and the TLS handshake leave it.
     */
    private static void joined(ControllerEngine agent, ControllerEngine controller) {
        assertNull(agent.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(agent.cstate.getRegion(), agent.cstate.getAgent(),
                leaf(agent), PeerTrustRegistry.ROLE_AGENT, false));
        // a link made anew: the health watcher has the first ping signed (MsgSigning.parentLinkReset)
        agent.msgSigning().parentLinkReset();
    }

    /** {@code m} as {@code from} sends it: stamped, and signed (or marked direct) as it leaves the node. */
    private static MsgEvent leaves(ControllerEngine from, MsgEvent m, Caller as) {
        CallerStamps.stamp(m, as);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /**
     * {@code m} as {@code to}'s consumer verifies it: with the sender's stamp, and (a message its sender marked as its own
     * over the direct link) attested first hand by the receiving broker, which took it from the sender's own connection.
     */
    private static MsgSigning.Verdict arrives(ControllerEngine from, ControllerEngine to, MsgEvent m, Caller as) {
        CrescoIdentity firstHand = m.getParam(MsgSigning.DIRECT) == null ? null
                : CrescoIdentity.of("t1", from.cstate.getRegion(), from.cstate.getAgent(), null);
        MsgSigning.notPending(m);
        return to.msgSigning().verify(m, as, false, firstHand);
    }

    private static MsgEvent message(ControllerEngine from, ControllerEngine to, String toPlugin) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), toPlugin, false, false);
        m.setParam("action", "getstatus");
        return m;
    }

    /** What a round of the agent's ping left: the ping as it reached the parent, the parent's verdict on it, the pong. */
    private static final class Round {
        MsgEvent ping, pong;
        MsgSigning.Verdict atParent;
    }

    /**
     * One ping of {@code child} as its health watcher sends it and its parent's consumer and executor take it; the pong,
     * when the ping was accepted, as the child's consumer and watcher take it. A refused ping is never answered with a pong.
     */
    private static Round ping(ControllerEngine child, ControllerEngine parent) {
        Round r = new Round();
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        r.ping = leaves(child, p, controller(child));
        MsgEvent atParent = wire(r.ping);
        r.atParent = arrives(child, parent, atParent, controller(child));
        if (!r.atParent.ok()) {
            child.msgSigning().parentPingUnanswered();
            return r;
        }
        parent.msgSigning().onPingAtParent(atParent, false);
        atParent.setReturn();
        r.pong = leaves(parent, atParent, controller(parent));
        MsgEvent atChild = wire(r.pong);
        MsgSigning.Verdict back = arrives(parent, child, atChild, controller(parent));
        assertTrue(back.ok(), "the agent refused its parent's pong: " + back.refusal);
        child.msgSigning().onParentPong(atChild, parent.cstate.getRegion(), parent.cstate.getAgent());
        return r;
    }

    /** An agent that joined {@code rc}, was attested by it and signs with that attestation. */
    private static ControllerEngine attested(String name, ControllerEngine rc) throws Exception {
        ControllerEngine a = agent(rc.cstate.getRegion(), name, rc);
        joined(a, rc);
        Round first = ping(a, rc);
        assertTrue(first.atParent.ok(), first.atParent.refusal);
        assertNotNull(first.pong.getParam(MsgSigning.P_ATT), "the first ping was not attested");
        assertTrue(a.msgSigning().credential().startsWith("att:"));
        return a;
    }

    private static void recoversUnderAReKeyedParent(boolean parentIsGlobal) throws Exception {
        ControllerEngine rcOld = node("rgA", "rc", true, parentIsGlobal);
        ControllerEngine a1 = attested("a1", rcOld);
        String former = a1.msgSigning().credential();

        // the controller comes back under its name with a new key; a1 follows it there (rebind / PKI), a2 joins it afresh
        ControllerEngine rcNew = node("rgA", "rc", true, parentIsGlobal);
        assertNotEquals(leaf(rcOld).getPublicKey(), leaf(rcNew).getPublicKey());
        ControllerEngine a2 = attested("a2", rcNew);
        joined(a1, rcNew);

        // NOT weaker: what a1 still signs with the former key's attestation is refused by a node that holds the present key
        MsgSigning.Verdict stale = arrives(a1, a2, leaves(a1, message(a1, a2, null), plugin(a1)), plugin(a1));
        assertFalse(stale.ok(), "a message under the former key's attestation was accepted");
        assertEquals(MsgSigning.Refusal.CREDENTIAL, stale.reason);

        // its ping reaches the re-keyed controller and verifies there: by the agent's certificate, as the controller holds it
        Round r = ping(a1, rcNew);
        assertTrue(r.atParent.ok(), "the re-keyed controller refused its own agent's ping: " + r.atParent.refusal);
        assertEquals(0, rcNew.msgSigning().refusedCount(), "refusals at the re-keyed controller during the changeover");
        assertNotNull(r.pong.getParam(MsgSigning.P_ATT), "the re-keyed controller did not attest the agent anew");

        // from that pong on the agent signs with an attestation of the present key, which the nodes beyond its parent verify
        String now = a1.msgSigning().credential();
        assertTrue(now.startsWith("att:"));
        assertNotEquals(former, now, "the agent still signs with the attestation of the key before");
        MsgSigning.Verdict v = arrives(a1, a2, leaves(a1, message(a1, a2, null), plugin(a1)), plugin(a1));
        assertTrue(v.ok(), "a node that holds the controller's present key refuses its agent: " + v.refusal);
        assertNull(ping(a1, rcNew).pong.getParam(MsgSigning.P_ATT), "attested again although it holds the present one");
    }

    @Test
    void anAgentsPingVerifiesAtItsReKeyedControllerAndBringsAnAttestationOfTheNewKey() throws Exception {
        recoversUnderAReKeyedParent(false);
    }

    /** A global is the region controller of the agents in its own region: the same mechanism, the same recovery. */
    @Test
    void anAgentUnderAReKeyedGlobalRecoversTheSameWay() throws Exception {
        recoversUnderAReKeyedParent(true);
    }

    @Test
    void aMessageForTheAttestingControllerCarriesTheCertificateAndOneBeyondItTheAttestation() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, false);
        ControllerEngine a1 = attested("a1", rc), a2 = attested("a2", rc);
        a1.msgSigning().parentLinkReset();                   // signed, not over the direct link
        assertTrue(leaves(a1, message(a1, a2, null), plugin(a1)).getParam(MsgSigning.CRED).startsWith("att:"),
                "a node beyond the parent knows the agent only by its attestation");
        MsgEvent toParent = leaves(a1, message(a1, rc, null), plugin(a1));
        assertTrue(toParent.getParam(MsgSigning.CRED).startsWith("cert:"),
                "the controller is shown its own attestation, which only the key that made it can verify");
        assertTrue(leaves(a1, message(a1, rc, "plugin/9"), plugin(a1)).getParam(MsgSigning.CRED).startsWith("cert:"),
                "a plugin of the controller is verified by the controller");
        MsgSigning.Verdict v = arrives(a1, rc, toParent, plugin(a1));
        assertTrue(v.ok(), "the controller refuses its agent's certificate: " + v.refusal);
    }

    @Test
    void anAgentDropsItsAttestationWhenItsControllerSignsWithAnotherKeyAndAsksToBeAttestedAnew() throws Exception {
        ControllerEngine rcOld = node("rgA", "rc", true, false);
        ControllerEngine a1 = attested("a1", rcOld);
        MsgSigning s = a1.msgSigning();

        // a message the controller signs with the key that attested the agent changes nothing
        assertTrue(arrives(rcOld, a1, leaves(rcOld, message(rcOld, a1, null), controller(rcOld)), controller(rcOld)).ok());
        assertTrue(s.hasAttestation(), "dropped although its issuer holds the key that made it");
        assertFalse(s.takeTrustWanted(System.currentTimeMillis()));

        // the controller comes back re-keyed; the first thing of it that reaches the agent is signed with its new certificate
        // (an answer, a refusal of something the agent sent, a request): the agent has seen its parent's present key
        ControllerEngine rcNew = node("rgA", "rc", true, false);
        joined(a1, rcNew);
        MsgSigning.Verdict v = arrives(rcNew, a1, leaves(rcNew, message(rcNew, a1, null), controller(rcNew)), controller(rcNew));
        assertTrue(v.ok(), v.refusal);
        assertFalse(s.hasAttestation(), "the agent keeps an attestation made by a key its controller no longer has");
        assertEquals(1, s.attestationsDroppedCount());
        assertTrue(s.credential().startsWith("cert:"), "it signs with the former key's attestation, refused by every node beyond");

        // and it asks at once: its watcher pings now (once), and the ping says it holds no attestation
        long at = System.currentTimeMillis();
        assertTrue(s.takeTrustWanted(at), "the agent waits for its next ping interval");
        assertFalse(s.takeTrustWanted(at + 250), "asked again although it has just pinged");
        Round r = ping(a1, rcNew);
        assertEquals("0", r.ping.getParam(MsgSigning.P_ATT_EXP));
        assertNull(r.ping.getParam(MsgSigning.P_ATT_ISS));
        assertTrue(r.atParent.ok(), r.atParent.refusal);
        assertNotNull(r.pong.getParam(MsgSigning.P_ATT));
        assertTrue(s.hasAttestation());
        // the present key's attestation is kept when the controller signs again
        assertTrue(arrives(rcNew, a1, leaves(rcNew, message(rcNew, a1, null), controller(rcNew)), controller(rcNew)).ok());
        assertTrue(s.hasAttestation(), "the present key's attestation was dropped");
        assertEquals(1, s.attestationsDroppedCount());
    }

    /**
     * An agent attested by a controller that does not name the key it attests with (an earlier build: no sig_att_iss on its
     * pong); returns the trust bundle that pong carried, which the agent has not taken yet.
     */
    private static String attestedByAnEarlierBuild(ControllerEngine a, ControllerEngine rc) throws Exception {
        joined(a, rc);
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, a.cstate.getRegion(), a.cstate.getAgent(), null,
                rc.cstate.getRegion(), rc.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        a.msgSigning().onChildPing(p);
        p = wire(p);
        controller(a).stamp(p);
        rc.msgSigning().onPingAtParent(p, false);
        assertNotNull(p.getParam(MsgSigning.P_ATT));
        p.removeParam(MsgSigning.P_ATT_ISS);
        String bundle = p.getCompressedParam(MsgSigning.P_BUNDLE);
        p.removeParam(MsgSigning.P_BUNDLE);
        a.msgSigning().onParentPong(wire(p), rc.cstate.getRegion(), rc.cstate.getAgent());
        assertTrue(a.msgSigning().hasAttestation());
        return bundle;
    }

    /**
     * A pong over the direct link is unsigned (it shows no key), and a controller of an earlier build neither names the key
     * that attests nor attests anew for a key the agent names: the trust bundle the re-keyed controller signs shows its key.
     */
    @Test
    void anAgentStopsPresentingItsAttestationTheMomentAConnectionShowsItsControllersOtherKey() throws Exception {
        // nightly matrix, production profile, rekey-region: in the ~100 ms between reconnecting to its re-keyed controller
        // and that controller's first pong, the agent signed an answer for the global with the FORMER key's attestation;
        // the global (which knew the new key) held it 5 s and refused it: "the attestation's signature does not verify"
        ControllerEngine rcOld = node("rgA", "rc", true, false);
        ControllerEngine far = node("rgB", "rc2", true, false);
        ControllerEngine a1 = attested("a7", rcOld);
        MsgSigning s = a1.msgSigning();
        assertTrue(s.credential("rgB_rc2").startsWith("att:"));

        // a connection that shows the key that attested it changes nothing; nor does another node's key
        a1.getCertificateManager().peerKeySeen(new PeerIdentity.Expected("rgA", "rc"), leaf(rcOld));
        s.controllerKeySeen("rgB", "rc2", leaf(far).getPublicKey());
        assertTrue(s.hasAttestation());
        assertFalse(s.attestationPending());

        // the controller is back with another key; the agent's connection to its broker verifies (the TLS handshake)
        ControllerEngine rcNew = node("rgA", "rc", true, false);
        joined(a1, rcNew);
        a1.getCertificateManager().peerKeySeen(new PeerIdentity.Expected("rgA", "rc"), leaf(rcNew));
        assertFalse(s.hasAttestation(), "the agent still presents an attestation made by a key its controller no longer has");
        assertEquals(1, s.attestationsDroppedCount());
        assertTrue(s.credential("rgB_rc2").startsWith("cert:"));
        // from this moment what would go beyond its controller waits for the new attestation; what goes to its controller
        // (which verifies its certificate) does not
        MsgEvent beyond = message(a1, far, null), toController = message(a1, rcNew, null);
        for (MsgEvent m : new MsgEvent[]{beyond, toController}) {
            CallerStamps.stamp(m, plugin(a1));
            a1.markForSigning(m);
        }
        assertTrue(s.attestationPending());
        assertTrue(s.awaitsAttestation(beyond), "it would leave with a credential the node beyond cannot verify");
        assertFalse(s.awaitsAttestation(toController));
        assertTrue(s.takeTrustWanted(System.currentTimeMillis()), "and the agent pings now, not at its next interval");

        Round r = ping(a1, rcNew);
        assertTrue(r.atParent.ok(), r.atParent.refusal);
        assertTrue(s.hasAttestation(), "attested by the new key");
        assertFalse(s.awaitsAttestation(beyond), "what waited goes now");
        assertTrue(s.credential("rgB_rc2").startsWith("att:"));
        // the present key, seen again on a later connection, keeps it
        a1.getCertificateManager().peerKeySeen(new PeerIdentity.Expected("rgA", "rc"), leaf(rcNew));
        assertTrue(s.hasAttestation());
    }

    @Test
    void aHandshakeThatVerifiesAsAPeerSaysWhichKeyThatPeerHolds() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, false);
        ControllerEngine a1 = attested("a8", rc);
        javax.net.ssl.X509TrustManager pkix = new javax.net.ssl.X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) { }
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        java.util.List<String> seen = new java.util.ArrayList<>();
        PeerIdentityTrustManager tm = new PeerIdentityTrustManager(() -> pkix, new PeerIdentity.Expected("rgA", "rc"),
                a1.getCertificateManager().getPeerTrust(), PeerIdentity.Mode.ENFORCE, null, "test", null,
                (peer, cert) -> seen.add(peer.region + "/" + peer.agent + " " + PeerTrustRegistry.fingerprint(cert.getPublicKey().getEncoded())));
        tm.checkServerTrusted(rc.getCertificateManager().getPublicCertificate(), "RSA");
        assertEquals(java.util.List.of("rgA/rc " + PeerTrustRegistry.fingerprint(leaf(rc).getPublicKey().getEncoded())), seen);
        // a chain that does not verify as that peer is refused, and says nothing
        ControllerEngine other = node("rgA", "rc", true, false);
        assertThrows(java.security.cert.CertificateException.class,
                () -> tm.checkServerTrusted(other.getCertificateManager().getPublicCertificate(), "RSA"));
        assertEquals(1, seen.size());
    }

    @Test
    void theTrustBundleOfAReKeyedControllerDropsAnAttestationItsFormerKeyMade() throws Exception {
        ControllerEngine rcOld = node("rgA", "rc", true, false);
        ControllerEngine a1 = agent("rgA", "a1", rcOld), a4 = agent("rgA", "a4", rcOld);
        MsgSigning s = a1.msgSigning();
        String bundleOfFormerKey = attestedByAnEarlierBuild(a1, rcOld);
        attestedByAnEarlierBuild(a4, rcOld);

        // the bundle of the key that made the attestation: checked against the attestation's own signature, kept
        s.acceptBundle(bundleOfFormerKey, "rgA", "rc");
        assertTrue(s.hasAttestation(), "dropped although the key that signed the bundle made it");
        s.acceptBundle(bundleOfFormerKey, "rgA", "rc");
        assertEquals(0, s.attestationsDroppedCount());

        // the controller re-keyed: the agent takes the bundle its new key signed (it verifies here as the parent's)
        ControllerEngine rcNew = node("rgA", "rc", true, false);
        joined(a1, rcNew);
        s.acceptBundle(rcNew.msgSigning().servedBundle(), "rgA", "rc");
        assertFalse(s.hasAttestation(), "the agent keeps an attestation made by a key its controller no longer has");
        assertTrue(s.takeTrustWanted(System.currentTimeMillis()), "and does not ask to be attested anew");
        MsgEvent next = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        s.onChildPing(next);
        assertEquals("0", next.getParam(MsgSigning.P_ATT_EXP), "its ping must say it holds no attestation, so the parent attests it");

        // an agent that never saw the former key's bundle: the attestation's own signature says the new key did not make it
        joined(a4, rcNew);
        a4.msgSigning().acceptBundle(rcNew.msgSigning().servedBundle(), "rgA", "rc");
        assertFalse(a4.msgSigning().hasAttestation(), "an attestation whose issuer named no key outlives that issuer's key");

        // a bundle that does not verify as the parent's shows nothing: another controller's key drops no attestation
        ControllerEngine a3 = attested("a3", rcNew);
        ControllerEngine other = node("rgA", "rc", true, false);
        a3.msgSigning().acceptBundle(other.msgSigning().servedBundle(), "rgA", "rc");
        assertTrue(a3.msgSigning().hasAttestation(), "a bundle that does not verify as the parent's dropped the attestation");
    }
}
