package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A name has one key. From the moment a node binds another key to a name (an operator's rebind, a controller it follows)
 * or learns one for it (a trust bundle that lists the name with another key), what it verified on the name's FORMER key is
 * no longer served: the cached credentials of that name, the attestations its former key issued, the sessions agreed with
 * them. Before, a verified credential stayed in the cache for up to an hour and a session for its life, and nothing but a
 * revocation cleared them: a message signed with the former key was accepted long after the rebind was taken.
 *
 * <p>Only that name pays: every other name's credential is served from the cache as before (the same signer, not verified
 * again), and nothing is added to what a message costs.
 */
class FormerKeyRefusedTest {

    private static final Gson GSON = new Gson();
    private static int seq;

    private static ControllerEngine node(String region, String agent, boolean regional, boolean global, boolean sessions) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_sessions", sessions ? "true" : "false");
        // every message carries a signature or a MAC (none travels unsigned between a node and its parent): what is tested
        cfg.put("msg_signing_direct", "false");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        ((TlsTestSupport.State) e.cstate).global = global;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static String keyOf(ControllerEngine e) { return PeerTrustRegistry.fingerprint(leaf(e).getPublicKey().getEncoded()); }

    private static PeerTrustRegistry registry(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static Caller controller(ControllerEngine e) { return Caller.controller("t1", e.cstate.getRegion(), e.cstate.getAgent()); }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    /** {@code child} joins {@code parent}: each holds the other's key (the child follows its controller, the parent binds first come). */
    private static void joined(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        TlsTestSupport.State st = (TlsTestSupport.State) child.cstate;
        if (childIsRegion) { st.globalRegion = parent.cstate.getRegion(); st.globalAgent = parent.cstate.getAgent(); }
        else { st.parentRegion = parent.cstate.getRegion(); st.parentAgent = parent.cstate.getAgent(); }
        assertNull(registry(child).pin(parent.cstate.getRegion(), parent.cstate.getAgent(), leaf(parent), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(registry(parent).pin(child.cstate.getRegion(), child.cstate.getAgent(), leaf(child),
                childIsRegion ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (childIsRegion) registry(parent).allowBridgePeer(child.cstate.getRegion(), child.cstate.getAgent());
    }

    /** The operator authorises ONE rebind of {@code node}'s name to its key at {@code at}, and it is taken (its next join). */
    private static void rebound(ControllerEngine at, ControllerEngine node, String role) {
        String name = node.cstate.getRegion() + "/" + node.cstate.getAgent();
        registry(at).withRebind(List.of(name + "=" + keyOf(node)));
        assertNull(registry(at).pin(node.cstate.getRegion(), node.cstate.getAgent(), leaf(node), role, false), "the rebind was not taken");
    }

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to, Caller as) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("payload", "message " + (++seq));
        CallerStamps.stamp(m, as);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to) { return sent(from, to, plugin(from)); }

    /** As the receiving node's inbox verifies it when it cannot hold a message: accepted, or refused at once. */
    private static MsgSigning.Verdict at(ControllerEngine to, MsgEvent m, Caller as) {
        return to.msgSigning().verify(wire(m), as, false, null, false);
    }

    private static MsgEvent delivered(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = sent(from, to);
        MsgSigning.Verdict v = at(to, m, plugin(from));
        assertTrue(v.ok(), from.cstate.getAgent() + " -> " + to.cstate.getAgent() + ": " + v.refusal);
        return m;
    }

    private static void refusedAtOnce(ControllerEngine from, ControllerEngine to, String what) {
        MsgSigning.Verdict v = at(to, sent(from, to), plugin(from));
        assertFalse(v.ok(), what);
        assertFalse(v.later, what + " (held, not refused)");
        assertEquals(MsgSigning.Refusal.CREDENTIAL, v.reason, v.refusal);
    }

    /** One ping of {@code child} and its pong, each signed as it leaves and verified as it arrives. */
    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        CallerStamps.stamp(p, controller(child));
        child.markForSigning(p);
        child.msgSigning().signIfPending(p);
        MsgEvent atParent = wire(p);
        MsgSigning.Verdict v = parent.msgSigning().verify(atParent, controller(child), false);
        assertTrue(v.ok(), "ping of " + child.cstate.getAgent() + ": " + v.refusal);
        parent.msgSigning().onPingAtParent(atParent, childIsRegion);
        atParent.setReturn();
        CallerStamps.stamp(atParent, controller(parent));
        parent.markForSigning(atParent);
        parent.msgSigning().signIfPending(atParent);
        MsgEvent pong = wire(atParent);
        v = child.msgSigning().verify(pong, controller(parent), false);
        assertTrue(v.ok(), "pong of " + parent.cstate.getAgent() + ": " + v.refusal);
        child.msgSigning().onParentPong(pong, parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    private static boolean macd(MsgEvent m) {
        return m.getParam(MsgSessions.MAC) != null && m.getParam(MsgSigning.SIG) == null && m.getParam(MsgSigning.CRED) == null;
    }

    private static void established(ControllerEngine a, ControllerEngine b) {
        for (int i = 0; i < 5; i++) {
            boolean ab = macd(delivered(a, b)), ba = macd(delivered(b, a));
            if (ab && ba) return;
        }
        fail("no session between " + a.cstate.getAgent() + " and " + b.cstate.getAgent());
    }

    @Test
    void afterARebindWhatTheFormerKeySignedAndAttestedIsRefusedAtOnceAndOtherNamesAreStillServedFromTheCache() throws Exception {
        ControllerEngine g = node("rgG", "gc", true, true, false), rcOld = node("rgA", "rc", true, false, false);
        ControllerEngine a1 = node("rgA", "a1", false, false, false), a2 = node("rgA", "a2", false, false, false);
        joined(rcOld, g, true);
        joined(a1, rcOld, false);
        joined(a2, rcOld, false);
        for (int i = 0; i < 2; i++) { ping(rcOld, g, true); ping(a1, rcOld, false); ping(a2, rcOld, false); }
        MsgSigning s = a1.msgSigning();

        // a1 verified, and holds in its cache: its controller's certificate, the attestation its controller's key gave a2,
        // and the certificate of the global (another name, on the trust bundle's word)
        String ofController = delivered(rcOld, a1).getParam(MsgSigning.CRED);
        String ofAgent = delivered(a2, a1).getParam(MsgSigning.CRED);
        String ofGlobal = delivered(g, a1).getParam(MsgSigning.CRED);
        assertTrue(ofController.startsWith("cert:") && ofAgent.startsWith("att:") && ofGlobal.startsWith("cert:"));
        MsgSigning.Signer global = s.cachedSigner(ofGlobal);
        assertNotNull(s.cachedSigner(ofController));
        assertNotNull(s.cachedSigner(ofAgent));
        assertNotNull(global);

        // the controller comes back under its name with another key, and a1 takes it (the operator's rebind)
        ControllerEngine rcNew = node("rgA", "rc", true, false, false);
        rebound(a1, rcNew, PeerTrustRegistry.ROLE_CONTROLLER);

        // from that moment: nothing of the former key is served from the cache, and none of it verifies again
        assertNull(s.cachedSigner(ofController), "the former key's certificate is still cached");
        assertNull(s.cachedSigner(ofAgent), "an attestation the former key issued is still cached");
        assertEquals(2, s.formerKeyCredentialsDropped());
        refusedAtOnce(rcOld, a1, "a message signed with the controller's FORMER key was accepted after the rebind");
        refusedAtOnce(a2, a1, "a message under an attestation the FORMER key issued was accepted after the rebind");

        // the name's present key verifies; and another name's credential is the one that was cached, not verified again
        MsgSigning.Verdict v = at(a1, sent(rcNew, a1), plugin(rcNew));
        assertTrue(v.ok(), "the controller's present key: " + v.refusal);
        assertSame(global, s.cachedSigner(ofGlobal), "another name's cached credential was dropped (everything was flushed)");
        delivered(g, a1);
        assertSame(global, s.cachedSigner(ofGlobal));
    }

    @Test
    void afterATrustBundleThatReKeysANameAMessageSignedWithItsFormerKeyIsRefusedAtOnce() throws Exception {
        ControllerEngine g0 = node("rgG", "gc", true, true, false), rA = node("rgA", "rc", true, false, false);
        ControllerEngine rB = node("rgB", "rc2", true, false, false), a1 = node("rgA", "a1", false, false, false);
        joined(rA, g0, true);
        joined(rB, g0, true);
        joined(a1, rA, false);
        for (int i = 0; i < 2; i++) { ping(rA, g0, true); ping(rB, g0, true); }
        ping(rA, g0, true);
        ping(a1, rA, false);
        MsgSigning s = a1.msgSigning();
        String ofGlobal = delivered(g0, a1).getParam(MsgSigning.CRED);
        String ofOtherRegion = delivered(rB, a1).getParam(MsgSigning.CRED);
        MsgSigning.Signer other = s.cachedSigner(ofOtherRegion);
        assertNotNull(s.cachedSigner(ofGlobal));
        assertNotNull(other);

        // the global comes back with another key; the regions follow it there, and its new bundle reaches a1 on a pong
        ControllerEngine g1 = node("rgG", "gc", true, true, false);
        assertNotEquals(keyOf(g0), keyOf(g1));
        joined(rA, g1, true);
        joined(rB, g1, true);
        ping(rB, g1, true);
        ping(rA, g1, true);
        ping(rA, g1, true);
        assertNotNull(s.cachedSigner(ofGlobal), "a1 has not heard of the new key yet");
        ping(a1, rA, false);

        assertNull(s.cachedSigner(ofGlobal), "the former key's certificate is still cached after the bundle named another key");
        refusedAtOnce(g0, a1, "a message signed with the global's FORMER key was accepted after the bundle that re-keyed it");
        MsgSigning.Verdict v = at(a1, sent(g1, a1), plugin(g1));
        assertTrue(v.ok(), "the global's present key: " + v.refusal);
        assertSame(other, s.cachedSigner(ofOtherRegion), "another name's cached credential was dropped (everything was flushed)");
        // and at the region, which bound the global's new key itself
        refusedAtOnce(g0, rA, "the region accepts the global's FORMER key after it followed the global to its new one");
    }

    @Test
    void theSessionsAgreedWithAFormerKeyAndWithTheAgentsItAttestedAreDroppedWithIt() throws Exception {
        ControllerEngine g = node("rgG", "gc", true, true, true), rcOld = node("rgA", "rc", true, false, true);
        ControllerEngine rB = node("rgB", "rc2", true, false, true), a1 = node("rgA", "a1", false, false, true);
        joined(rcOld, g, true);
        joined(rB, g, true);
        joined(a1, rcOld, false);
        for (int i = 0; i < 2; i++) { ping(rcOld, g, true); ping(rB, g, true); }
        ping(a1, rcOld, false);
        established(rcOld, g);
        established(a1, g);
        established(rB, g);

        // the operator rebinds the region controller's name at the global: its key is another one from now on
        ControllerEngine rcNew = node("rgA", "rc", true, false, true);
        rebound(g, rcNew, PeerTrustRegistry.ROLE_CONTROLLER);
        assertEquals(2, g.msgSigning().formerKeySessionsDropped(), "the controller's session, and that of the agent its former key attested");

        // a MAC under the session agreed with the former key, and under the session of an agent only that key vouched for
        MsgEvent m = sent(rcOld, g);
        assertTrue(macd(m));
        MsgSigning.Verdict v = at(g, m, plugin(rcOld));
        assertFalse(v.ok(), "a message under the session agreed with the FORMER key was accepted after the rebind");
        assertEquals(MsgSigning.Refusal.SESSION_KEY, v.reason);
        m = sent(a1, g);
        assertTrue(macd(m));
        v = at(g, m, plugin(a1));
        assertFalse(v.ok(), "a message under the session of an agent the FORMER key attested was accepted after the rebind");
        assertEquals(MsgSigning.Refusal.SESSION_KEY, v.reason);

        // another region's session stands
        m = sent(rB, g);
        assertTrue(macd(m));
        v = at(g, m, plugin(rB));
        assertTrue(v.ok(), "another name's session was dropped: " + v.refusal);
    }

    /** As a build before 2e30af7 signs for its controller: with the attestation that controller issued. */
    private static MsgEvent signedWithItsAttestation(ControllerEngine agent, ControllerEngine controller) throws Exception {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, agent.cstate.getRegion(), agent.cstate.getAgent(), "p1",
                controller.cstate.getRegion(), controller.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("payload", "message " + (++seq));
        CallerStamps.stamp(m, plugin(agent));
        Caller c = Caller.fromMsgEvent(m);
        m.setParam(MsgSigning.TO, controller.cstate.getRegion() + "/" + controller.cstate.getAgent() + "/");
        m.setParam(MsgSigning.S_CALLER, c.getPrincipal());
        m.setParam(MsgSigning.S_TENANT, c.getTenant());
        m.setParam(MsgSigning.S_ROLES, c.getRolesCsv());
        m.setParam(MsgSigning.BY, "t1/" + agent.cstate.getRegion() + "/" + agent.cstate.getAgent());
        m.setParam(MsgSigning.TS, Long.toString(System.currentTimeMillis()));
        String att = agent.msgSigning().credential();
        assertTrue(att.startsWith("att:"));
        m.setParam(MsgSigning.CRED, att);
        m.setParam(MsgSigning.SIG, Base64.getEncoder().encodeToString(agent.getCertificateManager().signWithIdentity(MsgSigning.canonical(m))));
        return wire(m);
    }

    @Test
    void anAgentReboundAtItsControllerIsRefusedThereUnderItsFormerKeyAndUnderTheAttestationThatKeyWasGiven() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true, false, false);
        ControllerEngine a1Old = node("rgA", "a1", false, false, false), a2 = node("rgA", "a2", false, false, false);
        joined(a1Old, rc, false);
        joined(a2, rc, false);
        ping(a1Old, rc, false);
        ping(a2, rc, false);
        MsgSigning s = rc.msgSigning();
        String cert = delivered(a1Old, rc).getParam(MsgSigning.CRED);
        assertTrue(cert.startsWith("cert:"));
        MsgEvent underAttestation = signedWithItsAttestation(a1Old, rc);
        MsgSigning.Verdict v = at(rc, underAttestation, plugin(a1Old));
        assertTrue(v.ok(), "the attestation this controller issued: " + v.refusal);
        String ofOther = delivered(a2, rc).getParam(MsgSigning.CRED);
        MsgSigning.Signer other = s.cachedSigner(ofOther);
        assertNotNull(s.cachedSigner(cert));
        assertNotNull(s.cachedSigner(underAttestation.getParam(MsgSigning.CRED)));

        // the agent was re-installed (a new key under its name) and the operator authorises the rebind at its controller
        ControllerEngine a1New = node("rgA", "a1", false, false, false);
        ((TlsTestSupport.State) a1New.cstate).parentRegion = "rgA";
        ((TlsTestSupport.State) a1New.cstate).parentAgent = "rc";
        rebound(rc, a1New, PeerTrustRegistry.ROLE_AGENT);

        assertNull(s.cachedSigner(cert), "the former key's certificate is still cached");
        assertNull(s.cachedSigner(underAttestation.getParam(MsgSigning.CRED)), "the attestation of the former key is still cached");
        refusedAtOnce(a1Old, rc, "the controller accepts the agent's FORMER key after the rebind");
        v = at(rc, signedWithItsAttestation(a1Old, rc), plugin(a1Old));
        assertFalse(v.ok(), "the controller accepts its own attestation of a key the agent no longer has there");
        assertEquals(MsgSigning.Refusal.CREDENTIAL, v.reason);
        assertTrue(v.refusal.contains("no longer has here"), v.refusal);

        assertNull(registry(a1New).pin("rgA", "rc", leaf(rc), PeerTrustRegistry.ROLE_CONTROLLER, true));
        v = at(rc, sent(a1New, rc), plugin(a1New));
        assertTrue(v.ok(), "the agent's present key: " + v.refusal);
        assertSame(other, s.cachedSigner(ofOther), "another agent's cached credential was dropped");
    }
}
