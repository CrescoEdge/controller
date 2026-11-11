package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node's first moments with a parent: its trust bundle arrives on the parent's first pong, and until then the signer of
 * a message from another region does not verify. Such a message is answered "not yet" to an inbox that can hold it
 * (nothing counted or logged), verifies once the bundle is in, and is refused for good only by an inbox that cannot hold
 * it or when the reason is not one newer trust data could cure. A global plus regions rgA and rgB; a1 an agent of rgA.
 */
class HeldUntilTrustArrivesTest {

    private static final Gson GSON = new Gson();
    private static ControllerEngine g, rA, rB, a1;

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
    }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    /**
     * The ping as an agent's health watcher runs it -- the join seen, then both halves (revocations, signatures) of the ping
     * and of its pong, the pong through the engine --; {@code seq} false: the region is of an earlier build and states no
     * position of its revocation list.
     */
    private static void watcherPing(ControllerEngine child, ControllerEngine parent, boolean seq) {
        RevocationSync.onParentLink(child, true);
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(child, p);
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        RevocationSync.onPingAtParent(parent, p, false);
        parent.msgSigning().onPingAtParent(p, false);
        if (!seq) p.removeParam(RevocationSync.SEQ);
        p.setReturn();
        child.onParentPong(wire(p));
    }

    @BeforeAll
    static void fabric() throws Exception {
        g = node("rgG", "gc", true);
        ((TlsTestSupport.State) g.cstate).global = true;
        rA = node("rgA", "rc", true);
        rB = node("rgB", "rc2", true);
        a1 = node("rgA", "a1", false);
        joined(rA, g, true);
        joined(rB, g, true);
        joined(a1, rA, false);
        for (int i = 0; i < 2; i++) { ping(rA, g, true); ping(rB, g, true); }
        ping(a1, rA, false);
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    @Test
    void aMessageThatArrivesBeforeTheTrustBundleIsHeldNotRefusedAndVerifiesOnceItIsIn() throws Exception {
        // b2 has just joined rgB: it knows its region controller, and holds no trust bundle yet
        ControllerEngine b2 = node("rgB", "b2", false);
        TlsTestSupport.State st = (TlsTestSupport.State) b2.cstate;
        st.parentRegion = "rgB";
        st.parentAgent = "rc2";
        joined(b2, rB, false);
        assertFalse(b2.trustReady(), "ready before its parent's first pong");
        int[] changes = new int[1];
        b2.msgSigning().onTrustChange(() -> changes[0]++);

        MsgEvent m = sent(a1, b2);
        long refusedBefore = b2.msgSigning().refusedCount();
        MsgSigning.Verdict v = b2.msgSigning().verify(m, plugin(a1), false, null, true);
        assertFalse(v.ok());
        assertTrue(v.later, "not answered 'not yet': " + v.refusal);
        assertEquals(refusedBefore, b2.msgSigning().refusedCount(), "a held message was counted as refused");

        // an inbox that cannot hold it refuses it, as before
        MsgSigning.Verdict atOnce = b2.msgSigning().verify(sent(a1, b2), plugin(a1), false, null, false);
        assertFalse(atOnce.ok());
        assertFalse(atOnce.later);
        assertEquals(refusedBefore + 1, b2.msgSigning().refusedCount());

        // the parent's first pong brings the bundle: the node is ready, its listeners are told, the held message verifies
        watcherPing(b2, rB, true);
        assertTrue(b2.trustReady());
        assertTrue(changes[0] > 0, "no trust change was announced: a held message would wait out its whole time");
        v = b2.msgSigning().verify(m, plugin(a1), false, null, true);
        assertTrue(v.ok(), v.refusal);
    }

    @Test
    void whatNewerTrustDataCannotCureIsRefusedAtOnce() throws Exception {
        ControllerEngine b3 = node("rgB", "b3", false);
        joined(b3, rB, false);
        ping(b3, rB, false);
        // a credential that is no attestation at all
        MsgEvent m = sent(a1, b3);
        m.setParam(MsgSigning.CRED, "att:{}");
        MsgSigning.Verdict v = b3.msgSigning().verify(m, plugin(a1), false, null, true);
        assertFalse(v.ok());
        assertFalse(v.later, "a malformed credential was held: " + v.refusal);
        // an unsigned message
        MsgEvent unsigned = sent(a1, b3);
        MsgSigning.clear(unsigned);
        v = b3.msgSigning().verify(unsigned, plugin(a1), false, null, true);
        assertFalse(v.ok());
        assertFalse(v.later, "an unsigned message was held");
    }

    @Test
    void aNodeIsReadyOnlyWithTheLastPartOfItsParentsRevocationList() throws Exception {
        // rgE's controller holds 40 revocations and sends 16 with a pong: e1, which just joined, holds them all with the third
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("revocation_sync_max_entries", "16");
        ControllerEngine rE = TlsTestSupport.engine("rgE", "rc5", cfg);
        ((TlsTestSupport.State) rE.cstate).regional = true;
        java.security.SecureRandom r = new java.security.SecureRandom();
        java.util.List<String> fps = new java.util.ArrayList<>();
        byte[] b = new byte[32];
        for (int i = 0; i < 40; i++) { r.nextBytes(b); fps.add(java.util.HexFormat.of().formatHex(b)); }
        rE.getCertificateManager().getPeerTrust().mergeRevocations(fps, "test", null);
        ControllerEngine e1 = node("rgE", "e1", false);
        TlsTestSupport.State st = (TlsTestSupport.State) e1.cstate;
        st.parentRegion = "rgE";
        st.parentAgent = "rc5";
        joined(e1, rE, false);
        int[] changes = new int[1];
        e1.msgSigning().onTrustChange(() -> changes[0]++);
        watcherPing(e1, rE, true);
        assertFalse(e1.trustReady(), "ready with 16 of its region's 40 revocations: a key among the other 24 is still accepted");
        watcherPing(e1, rE, true);
        assertFalse(e1.trustReady(), "ready with 32 of 40");
        int before = changes[0];
        watcherPing(e1, rE, true);
        assertTrue(e1.trustReady(), "not ready with the whole list");
        assertEquals(40, e1.getCertificateManager().getPeerTrust().revocations().size());
        assertTrue(changes[0] > before, "the last part came and nothing was announced: what the inbox holds waits out its whole time");
    }

    @Test
    void aParentOfAnEarlierBuildDoesNotHoldANodeBack() throws Exception {
        // its pongs state no position of its list: whether the list is whole cannot be known, and the node judges as before
        ControllerEngine f1 = node("rgB", "f1", false);
        TlsTestSupport.State st = (TlsTestSupport.State) f1.cstate;
        st.parentRegion = "rgB";
        st.parentAgent = "rc2";
        joined(f1, rB, false);
        assertFalse(f1.trustReady());
        watcherPing(f1, rB, false);
        assertFalse(RevocationSync.parentListComplete(f1), "a list of unknown length was taken for whole");
        assertTrue(f1.trustReady(), "a node under a region of an earlier build holds every message its whole time at each start");
    }

    @Test
    void aNodeWithNoParentIsAlwaysReady() throws Exception {
        assertTrue(g.trustReady(), "a global waits for nobody");
        ControllerEngine alone = node("rgC", "c1", false);
        assertTrue(alone.trustReady(), "an agent between parents has nothing to wait for");
        ControllerEngine region = node("rgD", "rc4", true);
        assertTrue(region.trustReady(), "a region with no global joined");
    }
}
