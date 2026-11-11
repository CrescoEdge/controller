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
 * Trust data on demand. A node that holds a message it cannot judge yet (its signer's controller is not in the trust bundle
 * it holds) asked for nothing: the bundle came with the next ping, 5 s later -- as long as a message may be held --, so the
 * message was refused at the moment the data it needed arrived. Now such a node asks: its watcher pings the parent at once,
 * at most once a second and for 5 s after the last such message; a region asked so asks the global in turn (what it lacks,
 * the global holds), and a change of the trust data ends the asking. A global plus regions rgA and rgB; a1 an agent of rgA.
 */
class TrustOnDemandTest {

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

    /** One ping and its pong; returns the ping as it reached the parent (before the parent's handler) and the pong. */
    private static MsgEvent[] ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        MsgEvent asSent = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        MsgEvent pong = wire(p);
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
        return new MsgEvent[]{asSent, pong};
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
    void aNodeThatHoldsAMessageItCannotJudgeAsksItsParentAtOnce() throws Exception {
        ControllerEngine b2 = node("rgB", "b2", false);
        joined(b2, rB, false);
        MsgSigning s = b2.msgSigning();
        long now = System.currentTimeMillis();
        assertFalse(s.takeTrustWanted(now), "a node that holds nothing asks for trust data");
        assertNull(ping(b2, rB, false)[0].getParam(MsgSigning.P_WANT), "its ordinary ping says it wants trust data");

        // b2 took its region's first pong before rgA joined the bundle rgB serves? No: here it simply holds no bundle entry for
        // rgA's controller yet -- make that so with a node that never took a pong
        ControllerEngine b3 = node("rgB", "b3", false);
        joined(b3, rB, false);
        s = b3.msgSigning();
        MsgSigning.Verdict v = s.verify(sent(a1, b3), plugin(a1), false, null, true);
        assertTrue(v.later, "not held: " + v.refusal);
        now = System.currentTimeMillis();
        assertTrue(s.takeTrustWanted(now), "a message it cannot judge yet, and its watcher is not told to ping");
        assertFalse(s.takeTrustWanted(now + 200), "asked again within a second");
        assertTrue(s.takeTrustWanted(now + MsgSigning.WANT_EVERY_MS), "not asked again a second later, while nothing changed");
        assertFalse(s.takeTrustWanted(now + MsgSigning.WANT_FOR_MS + 2_000L), "still asking long after the last such message");
        assertFalse(s.takeTrustWanted(now + MsgSigning.WANT_FOR_MS + 4_000L));

        // an inbox that cannot hold the message refuses it: nothing is asked for
        ControllerEngine b4 = node("rgB", "b4", false);
        joined(b4, rB, false);
        assertFalse(b4.msgSigning().verify(sent(a1, b4), plugin(a1), false, null, false).ok());
        assertFalse(b4.msgSigning().takeTrustWanted(System.currentTimeMillis()));
    }

    @Test
    void thePingSaysSoAndTheAskingEndsWhenTheTrustDataChanged() throws Exception {
        ControllerEngine b5 = node("rgB", "b5", false);
        joined(b5, rB, false);
        MsgSigning s = b5.msgSigning();
        MsgEvent m = sent(a1, b5);
        assertTrue(s.verify(m, plugin(a1), false, null, true).later);
        MsgEvent[] pp = ping(b5, rB, false);
        assertEquals("1", pp[0].getParam(MsgSigning.P_WANT), "the ping of a node that wants trust data does not say so");
        assertNull(pp[1].getParam(MsgSigning.P_WANT), "the wish came back on the pong");
        // that pong brought the bundle: the message verifies, and nothing more is asked for
        assertTrue(s.verify(m, plugin(a1), false, null, true).ok());
        assertFalse(s.takeTrustWanted(System.currentTimeMillis()), "still asking after the trust data changed");
        assertNull(ping(b5, rB, false)[0].getParam(MsgSigning.P_WANT));
    }

    @Test
    void aRegionAskedByAChildAsksTheGlobalAndTheGlobalAsksNobody() throws Exception {
        ControllerEngine rC = node("rgC", "rc3", true), c1 = node("rgC", "c1", false);
        joined(rC, g, true);
        joined(c1, rC, false);
        ping(rC, g, true);
        ping(c1, rC, false);
        assertFalse(rC.msgSigning().takeTrustWanted(System.currentTimeMillis()));
        // c1 cannot judge a message: its ping says so, and its region asks the global (which may hold what the region lacks)
        c1.msgSigning().wantTrust();
        ping(c1, rC, false);
        long now = System.currentTimeMillis();
        assertTrue(rC.msgSigning().takeTrustWanted(now), "a region whose child wants trust data does not ask the global");
        MsgEvent[] up = ping(rC, g, true);
        assertEquals("1", up[0].getParam(MsgSigning.P_WANT));
        assertFalse(g.msgSigning().takeTrustWanted(System.currentTimeMillis()), "a global has no parent to ask");
    }
}
