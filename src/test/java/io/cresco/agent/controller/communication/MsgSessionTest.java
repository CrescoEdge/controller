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
 * End-to-end session keys (MsgSessions): two nodes in different regions -- every message between them passes a relay --
 * agree on a key in one round trip on authenticated messages, then authenticate with a MAC under it. What passes a relay
 * stays unforgeable, unalterable and unreplayable by it; a restarted peer is sent signatures again; a revoked signer is
 * refused under its session; a node without session keys is never sent a MAC.
 */
class MsgSessionTest {

    private static final Gson GSON = new Gson();
    private static ControllerEngine g, rA, rB, a1, b1, a2, a3, b3, a4, b4, off;

    private static ControllerEngine node(String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
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
        g = node("rgG", "gc", true, Map.of());
        ((TlsTestSupport.State) g.cstate).global = true;
        rA = node("rgA", "rc", true, Map.of());
        rB = node("rgB", "rc2", true, Map.of());
        a1 = node("rgA", "a1", false, Map.of());
        b1 = node("rgB", "b1", false, Map.of());
        a2 = node("rgA", "a2", false, Map.of());
        a3 = node("rgA", "a3", false, Map.of());
        b3 = node("rgB", "b3", false, Map.of());
        a4 = node("rgA", "a4", false, Map.of());
        b4 = node("rgB", "b4", false, Map.of());
        off = node("rgB", "b9", false, Map.of("msg_signing_sessions", "false"));
        joined(rA, g, true);
        joined(rB, g, true);
        for (ControllerEngine a : new ControllerEngine[]{a1, a2, a3, a4}) joined(a, rA, false);
        for (ControllerEngine b : new ControllerEngine[]{b1, b3, b4, off}) joined(b, rB, false);
        ping(rA, g, true);
        ping(rB, g, true);
        ping(rA, g, true);
        ping(rB, g, true);
        for (ControllerEngine a : new ControllerEngine[]{a1, a2, a3, a4}) ping(a, rA, false);
        for (ControllerEngine b : new ControllerEngine[]{b1, b3, b4, off}) ping(b, rB, false);
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    private static int seq;

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("payload", "message " + (++seq));
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /** {@code from} sends to {@code to}, which must accept it. */
    private static MsgEvent delivered(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = sent(from, to);
        MsgSigning.Verdict v = to.msgSigning().verify(m, plugin(from), false);
        assertTrue(v.ok(), v.refusal);
        return m;
    }

    private static boolean macd(MsgEvent m) {
        return m.getParam(MsgSessions.MAC) != null && m.getParam(MsgSigning.SIG) == null && m.getParam(MsgSigning.CRED) == null;
    }

    /** Talk until {@code a} and {@code b} send each other MACs (at most a few round trips). */
    private static void established(ControllerEngine a, ControllerEngine b) {
        for (int i = 0; i < 4; i++) {
            boolean ab = macd(delivered(a, b)), ba = macd(delivered(b, a));
            if (ab && ba) return;
        }
        fail("no session between " + a.cstate.getAgent() + " and " + b.cstate.getAgent());
    }

    @Test
    void aSessionFormsInOneRoundTripAndThenMessagesCarryAMac() {
        MsgEvent hello = delivered(b4, a4);                     // a4 learns b4 reads session keys
        assertTrue(hello.getParam(MsgSigning.CAPS).contains("session"));
        assertNull(hello.getParam(MsgSessions.KX), "b4 does not know yet what a4 reads");
        MsgEvent offer = delivered(a4, b4);
        assertNotNull(offer.getParam(MsgSigning.SIG), "the offer rides a signed message");
        assertNotNull(offer.getParam(MsgSessions.KX), "a4 offered a session key");
        MsgEvent answer = delivered(b4, a4);
        assertNotNull(answer.getParam(MsgSigning.SIG), "b4 answers on a signed message (a4 has not confirmed yet)");
        assertNotNull(answer.getParam(MsgSessions.KX_ACK));
        MsgEvent first = delivered(a4, b4);
        assertTrue(macd(first), "a4's next message carries a MAC, no signature and no credential");
        assertTrue(macd(delivered(b4, a4)), "and b4's next one, once a4 said it holds the session");
        int signedLen = GSON.toJson(offer).length(), macLen = GSON.toJson(first).length();
        System.out.printf("session keys: a signed message %d bytes, under a session %d bytes%n", signedLen, macLen);
        assertTrue(macLen < signedLen - 1500, "the credential no longer travels");
        assertTrue(a4.msgSigning().macSentCount() > 0 && b4.msgSigning().macVerifiedCount() > 0);
    }

    @Test
    void aSessionMessageIsRefusedWhenTamperedReplayedMisattributedOrDeliveredElsewhere() {
        established(a1, b1);
        MsgEvent m = sent(a1, b1);
        assertTrue(macd(m));
        MsgEvent tampered = wire(m);
        tampered.setParam("payload", "something else");
        assertFalse(b1.msgSigning().verify(tampered, plugin(a1), false).ok(), "an altered message");
        MsgEvent forgedBy = wire(m);
        forgedBy.setParam(MsgSigning.BY, "t1/rgA/a2");
        assertFalse(b1.msgSigning().verify(forgedBy, Caller.plugin("t1", "rgA", "a2", "p1"), false).ok(), "another signer's name");
        MsgEvent stale = wire(m);
        stale.setParam(MsgSigning.TS, Long.toString(System.currentTimeMillis() - 1000));
        assertFalse(b1.msgSigning().verify(stale, plugin(a1), false).ok(), "another signing time (the time is under the MAC)");
        MsgEvent unknown = wire(m);
        unknown.setParam(MsgSessions.SID, "AAAAAAAAAAAAAAAAAAAAAAAA");
        assertFalse(b1.msgSigning().verify(unknown, plugin(a1), false).ok(), "a session nobody holds");
        assertFalse(rB.msgSigning().verify(wire(m), plugin(a1), false).ok(), "delivered to a node other than its peer");
        assertTrue(b1.msgSigning().verify(m, plugin(a1), false).ok());
        assertFalse(b1.msgSigning().verify(wire(m), plugin(a1), false).ok(), "a replay");
        MsgEvent asCaller = sent(a1, b1);
        assertFalse(b1.msgSigning().verify(asCaller, Caller.controller("t1", "rgG", "gc"), false).ok(),
                "a broker stamp that is not the authenticated caller");
    }

    @Test
    void aRestartedPeerIsSentSignaturesAgain() {
        established(a1, b1);
        b1.msgSigning().restartSessions();                       // b1 restarted: no sessions, a new incarnation
        MsgEvent lost = sent(a1, b1);
        assertTrue(macd(lost), "a1 cannot know yet");
        assertFalse(b1.msgSigning().verify(lost, plugin(a1), false).ok(), "under a session the restarted node no longer holds");
        delivered(b1, a1);                                       // b1's next word: a new incarnation
        MsgEvent again = delivered(a1, b1);
        assertNotNull(again.getParam(MsgSigning.SIG), "a1 returned to signatures");
        assertTrue(again.getParam(MsgSessions.KX) != null || again.getParam(MsgSessions.KX_ACK) != null, "a new session is under way");
        established(a1, b1);
    }

    @Test
    void aRevokedSignerIsRefusedUnderItsSession() {
        established(a2, b1);
        MsgEvent m = sent(a2, b1);
        assertTrue(macd(m));
        String fp = PeerTrustRegistry.fingerprint(leaf(a2).getPublicKey().getEncoded());
        assertNull(b1.getCertificateManager().getPeerTrust().revoke(fp, "test"));
        MsgSigning.Verdict v = b1.msgSigning().verify(m, plugin(a2), false);
        assertFalse(v.ok(), "a revoked key's session");
        // the revocation dropped the session at once (and each message's signer is checked against the revocations anyway)
        assertTrue(v.refusal.contains("does not hold") || v.refusal.contains("revoked"), v.refusal);
        assertFalse(b1.msgSigning().verify(sent(a2, b1), plugin(a2), false).ok(), "a revoked key's next message");
    }

    @Test
    void bothOfferingAtOnceAgreeOnOneSession() {
        // each learns the other reads session keys, before either offers
        MsgEvent fromB = sent(b3, a3), fromA = sent(a3, b3);
        assertTrue(a3.msgSigning().verify(fromB, plugin(b3), false).ok());
        assertTrue(b3.msgSigning().verify(fromA, plugin(a3), false).ok());
        MsgEvent x = sent(a3, b3), y = sent(b3, a3);
        assertNotNull(x.getParam(MsgSessions.KX));
        assertNotNull(y.getParam(MsgSessions.KX));
        assertTrue(b3.msgSigning().verify(x, plugin(a3), false).ok());
        assertTrue(a3.msgSigning().verify(y, plugin(b3), false).ok());
        established(a3, b3);
    }

    @Test
    void aNodeWithoutSessionKeysIsNeverSentAMac() {
        for (int i = 0; i < 4; i++) {
            MsgEvent fromOff = delivered(off, a1), toOff = delivered(a1, off);
            assertNull(fromOff.getParam(MsgSessions.KX));
            assertNull(fromOff.getParam(MsgSessions.INC));
            assertFalse(String.valueOf(fromOff.getParam(MsgSigning.CAPS)).contains("session"));
            assertNull(toOff.getParam(MsgSessions.KX), "an offer to a node that does not read them");
            assertFalse(macd(toOff));
        }
    }

    /** What a message under a session costs, against a signature (reported). */
    @Test
    void sessionCost() {
        established(a1, b1);
        int n = 2000;
        MsgEvent[] ms = new MsgEvent[n];
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) ms[i] = sent(a1, b1);
        long t1 = System.nanoTime();
        for (MsgEvent m : ms) assertTrue(b1.msgSigning().verify(m, plugin(a1), false).ok());
        long t2 = System.nanoTime();
        for (MsgEvent m : ms) assertTrue(macd(m));
        System.out.printf("session keys (HMAC-SHA-384): authenticate+serialize %.1f us, verify %.1f us per message%n",
                (t1 - t0) / 1e3 / n, (t2 - t1) / 1e3 / n);
    }
}
