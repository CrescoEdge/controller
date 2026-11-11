package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every refusal names its reason, is counted under it, and is logged under it: the reason's first refusal at once, then
 * at most one line per 10 s per reason. With one counter and one line per 10 s for all of them, a node that refused
 * hundreds of unsigned messages said nothing of the one replay among them, and no test could gate on either.
 * A global plus regions rgA and rgB; a1 an agent of rgA whose messages the nodes of rgB receive.
 */
class RefusalsCountedByReasonTest {

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

    /** A node of rgB that has just taken its region's trust data: it can judge a1's messages, and has refused nothing yet. */
    private static ControllerEngine receiver(String agent) throws Exception {
        ControllerEngine b = node("rgB", agent, false);
        joined(b, rB, false);
        ping(b, rB, false);
        return b;
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    /** A message of a1's plugin for {@code to}, signed, as it comes off the wire. */
    private static MsgEvent sent(ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, a1.cstate.getRegion(), a1.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, plugin(a1));
        a1.markForSigning(m);
        a1.msgSigning().signIfPending(m);
        return wire(m);
    }

    private static MsgSigning.Verdict refused(ControllerEngine at, MsgEvent m, Caller fromBroker, MsgSigning.Refusal reason) {
        MsgSigning.Verdict v = at.msgSigning().verify(m, fromBroker, false);
        assertFalse(v.ok(), "accepted: expected a refusal for " + reason);
        assertEquals(reason, v.reason, v.refusal);
        return v;
    }

    @Test
    void everyRefusalIsCountedUnderItsReasonAndInTheNodesTotal() throws Exception {
        ControllerEngine b = receiver("b10");
        MsgSigning s = b.msgSigning();
        assertTrue(s.verify(sent(b), plugin(a1), false).ok(), "the fabric of this test does not verify a good message");
        assertEquals(0, s.refusedCount());

        MsgEvent unsigned = sent(b);
        MsgSigning.clear(unsigned);
        refused(b, unsigned, plugin(a1), MsgSigning.Refusal.UNSIGNED);
        refused(b, unsigned, plugin(a1), MsgSigning.Refusal.UNSIGNED);

        refused(b, sent(rB), plugin(a1), MsgSigning.Refusal.NOT_FOR_THIS_NODE);          // signed for another node

        MsgEvent nobody = sent(b);
        nobody.removeParam(MsgSigning.TO);
        refused(b, nobody, plugin(a1), MsgSigning.Refusal.NO_RECIPIENT);

        MsgEvent old = sent(b);
        old.setParam(MsgSigning.TS, Long.toString(System.currentTimeMillis() - 3_600_000L));
        refused(b, old, plugin(a1), MsgSigning.Refusal.TOO_OLD);

        MsgEvent ahead = sent(b);
        ahead.setParam(MsgSigning.TS, Long.toString(System.currentTimeMillis() + 3_600_000L));
        refused(b, ahead, plugin(a1), MsgSigning.Refusal.AHEAD_OF_CLOCK);

        MsgEvent once = sent(b);
        assertTrue(s.verify(wire(once), plugin(a1), false).ok());
        refused(b, wire(once), plugin(a1), MsgSigning.Refusal.REPLAY);

        MsgEvent changed = sent(b);
        changed.setParam("action", "removeplugin");
        refused(b, changed, plugin(a1), MsgSigning.Refusal.BAD_SIGNATURE);

        MsgEvent noCredential = sent(b);
        noCredential.setParam(MsgSigning.CRED, "att:{}");
        refused(b, noCredential, plugin(a1), MsgSigning.Refusal.CREDENTIAL);

        // the broker says another caller sent it than the one its origin signed
        refused(b, sent(b), Caller.plugin("t1", "rgA", "a1", "p2"), MsgSigning.Refusal.CALLER_MISMATCH);

        MsgEvent unreadable = sent(b);
        unreadable.setParam(MsgSigning.TS, "yesterday");
        refused(b, unreadable, plugin(a1), MsgSigning.Refusal.OTHER);

        Map<MsgSigning.Refusal, Long> expected = new HashMap<>();
        expected.put(MsgSigning.Refusal.UNSIGNED, 2L);
        for (MsgSigning.Refusal r : List.of(MsgSigning.Refusal.NOT_FOR_THIS_NODE, MsgSigning.Refusal.NO_RECIPIENT, MsgSigning.Refusal.TOO_OLD,
                MsgSigning.Refusal.AHEAD_OF_CLOCK, MsgSigning.Refusal.REPLAY, MsgSigning.Refusal.BAD_SIGNATURE, MsgSigning.Refusal.CREDENTIAL,
                MsgSigning.Refusal.CALLER_MISMATCH, MsgSigning.Refusal.OTHER)) expected.put(r, 1L);
        for (MsgSigning.Refusal r : MsgSigning.Refusal.values()) {
            assertEquals(expected.getOrDefault(r, 0L).longValue(), s.refusedCount(r), "refusals counted as " + r);
        }
        assertEquals(11, s.refusedCount(), "the node's total is not the sum of its reasons");
    }

    @Test
    void theFirstRefusalOfEachReasonIsLoggedAtOnceWithTheNodesTotalAndTheReasonsOwnCount() throws Exception {
        assertEquals(10_000L, MsgSigning.REFUSAL_LINE_EVERY_MS);
        ControllerEngine b = receiver("b11");
        MsgSigning s = b.msgSigning();
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        s.refusalLineTap = lines::add;

        MsgEvent unsigned = sent(b);
        MsgSigning.clear(unsigned);
        for (int i = 0; i < 3; i++) refused(b, unsigned, plugin(a1), MsgSigning.Refusal.UNSIGNED);
        assertEquals(1, lines.size(), "three refusals of one reason within 10 s: " + lines);
        String first = lines.get(0);
        assertTrue(first.startsWith("MESSAGE SIGNATURE: REFUSED an unsigned message (from "), first);
        assertTrue(first.contains(" [EXEC getstatus] "), "the line does not name the message's type and action: " + first);
        // what tools read today -- "(<N> refused so far" with the node's total -- then this reason's own count
        assertTrue(first.endsWith(" (1 refused so far; 1 of this kind, unsigned; each kind is logged at most every 10 s)"), first);

        // one replay among them: logged at once, never behind the unsigned line
        MsgEvent once = sent(b);
        assertTrue(s.verify(wire(once), plugin(a1), false).ok());
        refused(b, wire(once), plugin(a1), MsgSigning.Refusal.REPLAY);
        assertEquals(2, lines.size(), "the first replay was not logged at once: " + lines);
        assertTrue(lines.get(1).startsWith("MESSAGE SIGNATURE: REFUSED a signature seen before (a replay) from "), lines.get(1));
        assertTrue(lines.get(1).endsWith(" (4 refused so far; 1 of this kind, replay; each kind is logged at most every 10 s)"), lines.get(1));

        refused(b, wire(once), plugin(a1), MsgSigning.Refusal.REPLAY);
        refused(b, unsigned, plugin(a1), MsgSigning.Refusal.UNSIGNED);
        assertEquals(2, lines.size(), "a reason was logged twice within 10 s: " + lines);
        assertEquals(6, s.refusedCount());
        assertEquals(4, s.refusedCount(MsgSigning.Refusal.UNSIGNED));
        assertEquals(2, s.refusedCount(MsgSigning.Refusal.REPLAY));
    }

    @Test
    void aMessageRefusedAfterItCouldNotBeHeldIsCountedUnderTheReasonOfItsVerdict() throws Exception {
        // b12 has joined rgB and holds no trust bundle yet: a1's credential does not verify here YET
        ControllerEngine b = node("rgB", "b12", false);
        TlsTestSupport.State st = (TlsTestSupport.State) b.cstate;
        st.parentRegion = "rgB";
        st.parentAgent = "rc2";
        joined(b, rB, false);
        MsgSigning s = b.msgSigning();
        MsgEvent m = sent(b);
        MsgSigning.Verdict v = s.verify(m, plugin(a1), false, null, true);
        assertTrue(v.later, v.refusal);
        assertEquals(MsgSigning.Refusal.CREDENTIAL, v.reason, "a verdict to hold a message does not say what it would be refused for");
        assertEquals(0, s.refusedCount(), "a message to hold was counted as refused");
        // its inbox could not hold it (no room), or held it to the end: refused after all
        s.refusedAfterHold(m, v.reason, v.refusal);
        assertEquals(1, s.refusedCount());
        assertEquals(1, s.refusedCount(MsgSigning.Refusal.CREDENTIAL));
    }

    @Test
    void whatThisNodeCouldNotSignIsCountedByItselfAndNotAmongTheMessagesItRefused() throws Exception {
        ControllerEngine b = receiver("b13");
        MsgSigning s = b.msgSigning();
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        s.refusalLineTap = lines::add;
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b13", "p1", null, null, null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, Caller.plugin("t1", "rgB", "b13", "p1"));
        b.markForSigning(m);
        s.signIfPending(m);                                  // no destination to sign for
        assertNull(m.getParam(MsgSigning.SIG));
        assertEquals(1, s.refusedCount(MsgSigning.Refusal.CANNOT_SIGN));
        assertEquals(0, s.refusedCount(), "a message this node sends unsigned is refused where it arrives, and counted there");
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("it goes unsigned") && lines.get(0).contains("(0 refused so far; 1 of this kind, cannot_sign;"), lines.get(0));
    }
}
