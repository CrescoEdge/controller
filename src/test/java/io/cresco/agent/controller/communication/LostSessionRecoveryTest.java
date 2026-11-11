package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.SessionId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node that restarted holds none of the sessions its peers still send under. Until this change every message a peer
 * sent it under the lost session was refused until the restarted node happened to send that peer something, or 30 s
 * passed (90 to 100 refused messages per storage node per restart in a three-site storage test). Now the node that
 * refuses tells the sender at once ({@link MsgSigning#LOST}): recovery takes one round trip, an RPC refused so is sent
 * again once, signed, and its caller sees no refusal. And none of that is a lever for an attacker: a forged notice, a
 * notice replayed for an older session and a flood of bad MACs each have a case here.
 *
 * <p>Nodes of two regions under a global, as in {@link MsgSessionTest}; what a node's controller sends by itself (a
 * notice, a 401) passes {@link #wire}: stamped and signed as the node does it, then handed to the node it is for, as
 * that node's inbox does it ({@link #arrive}).
 */
class LostSessionRecoveryTest {

    private static final Gson GSON = new Gson();
    private static final Map<String, ControllerEngine> NODES = new ConcurrentHashMap<>();
    /** What each node's application was handed (region_agent -> messages), and how often a request ran there. */
    private static final Map<String, List<MsgEvent>> HANDED = new ConcurrentHashMap<>();
    /** What each node's controller sent by itself, in order (region_agent -> messages as they left). */
    private static final Map<String, List<MsgEvent>> SENT = new ConcurrentHashMap<>();
    /** Not null: what the controllers send by themselves is kept here and not delivered (a slow wire). */
    private static volatile List<Object[]> slowWire;

    private static ControllerEngine g, rA, rB;

    private static ControllerEngine node(String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put("broker_security_enabled", "true");
        String key = region + "_" + agent;
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg, m -> wire(NODES.get(key), m));
        ((TlsTestSupport.State) e.cstate).regional = regional;
        NODES.put(key, e);
        HANDED.put(key, new ArrayList<>());
        SENT.put(key, new ArrayList<>());
        return e;
    }

    private static String key(ControllerEngine e) { return e.cstate.getRegion() + "_" + e.cstate.getAgent(); }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
    }

    private static MsgEvent copy(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = copy(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(copy(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    @BeforeAll
    static void fabric() throws Exception {
        g = node("rgG", "gc", true, Map.of());
        ((TlsTestSupport.State) g.cstate).global = true;
        rA = node("rgA", "rc", true, Map.of());
        rB = node("rgB", "rc2", true, Map.of());
        joined(rA, g, true);
        joined(rB, g, true);
        ping(rA, g, true);
        ping(rB, g, true);
        ping(rA, g, true);
        ping(rB, g, true);
    }

    /** An agent of region A (or B), joined and holding the trust data; each case takes its own pair. */
    private static ControllerEngine agent(String name, Map<String, Object> extra) throws Exception {
        boolean inA = name.startsWith("a");
        ControllerEngine e = node(inA ? "rgA" : "rgB", name, false, extra);
        joined(e, inA ? rA : rB, false);
        ping(e, inA ? rA : rB, false);
        return e;
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    private static Caller controller(ControllerEngine e) { return Caller.controller("t1", e.cstate.getRegion(), e.cstate.getAgent()); }

    private static int seq;

    /** A one-way message of {@code from}'s plugin for {@code to}'s controller, as it leaves {@code from}. */
    private static MsgEvent sent(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "report");
        m.setParam("payload", "message " + (++seq));
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return copy(m);
    }

    /** An RPC request of {@code from}'s plugin for {@code to}'s controller, as it leaves {@code from}. */
    private static MsgEvent request(ControllerEngine from, ControllerEngine to, String callId) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-p1", callId);
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return copy(m);
    }

    /** As {@link #request(ControllerEngine, ControllerEngine, String)}, saying how long its caller waits ({@code rpc_timeout_ms}). */
    private static MsgEvent request(ControllerEngine from, ControllerEngine to, String callId, long timeoutMs) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-p1", callId);
        m.setParam(RpcExpiry.TIMEOUT_PARAM, Long.toString(timeoutMs));
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return copy(m);
    }

    private static boolean macd(MsgEvent m) {
        return m.getParam(MsgSessions.MAC) != null && m.getParam(MsgSigning.SIG) == null && m.getParam(MsgSigning.CRED) == null;
    }

    private static boolean signed(MsgEvent m) { return m.getParam(MsgSigning.SIG) != null && m.getParam(MsgSessions.MAC) == null; }

    /**
     * What a node's controller sends by itself (PluginBuilder.msgOut: a notice, the 401 of a refused request) goes the way
     * AgentServiceImpl.msgOut and the producer send it: stamped as that controller, signed as it leaves, and delivered to
     * the node it is for -- at once, or when the test lets a slow wire go.
     */
    private static void wire(ControllerEngine from, MsgEvent m) {
        from.stampSelf(m);
        from.msgSigning().signIfPending(m);
        MsgEvent out = copy(m);
        SENT.get(key(from)).add(out);
        List<Object[]> slow = slowWire;
        if (slow != null) {
            slow.add(new Object[]{from, out});
            return;
        }
        ControllerEngine to = NODES.get(out.getDstRegion() + "_" + out.getDstAgent());
        if (to != null) arrive(to, copy(out), controller(from));
    }

    enum Arrival { REFUSED, TAKEN, HANDED }

    /** As a node's inbox does it (AgentConsumer): verify; a refusal is answered; the signing layer may take it; else it is handed on. */
    private static Arrival arrive(ControllerEngine to, MsgEvent m, Caller fromBroker) {
        MsgSigning.Verdict v = to.msgSigning().verify(m, fromBroker, false);
        if (!v.ok()) {
            to.msgSigning().answerRefused(m, v);
            return Arrival.REFUSED;
        }
        if (to.msgSigning().afterVerified(m, v)) return Arrival.TAKEN;
        HANDED.get(key(to)).add(m);
        return Arrival.HANDED;
    }

    /** Talk both ways until {@code a} and {@code b} send each other MACs. */
    private static void established(ControllerEngine a, ControllerEngine b) {
        for (int i = 0; i < 5; i++) {
            MsgEvent ab = sent(a, b), ba;
            assertEquals(Arrival.HANDED, arrive(b, ab, plugin(a)));
            ba = sent(b, a);
            assertEquals(Arrival.HANDED, arrive(a, ba, plugin(b)));
            if (macd(ab) && macd(ba)) return;
        }
        fail("no session between " + a.cstate.getAgent() + " and " + b.cstate.getAgent());
    }

    private static long refusedAt(ControllerEngine e) { return e.msgSigning().refusedCount(MsgSigning.Refusal.SESSION_KEY); }

    // ================================================================ recovery

    @Test
    void aFlowWithNoRepliesRecoversWithinOneRoundTripAndOneMessageIsRefused() throws Exception {
        ControllerEngine a = agent("a1", Map.of()), b = agent("b1", Map.of());
        established(a, b);
        long refused = refusedAt(b);
        int bSent = SENT.get(key(b)).size();
        b.msgSigning().restartSessions();                        // b restarted: no sessions, a new incarnation

        // b has nothing to say to a: until now every one of these was refused, for 30 s
        List<Arrival> flow = new ArrayList<>();
        MsgEvent first = sent(a, b);
        assertTrue(macd(first), "a cannot know yet");
        flow.add(arrive(b, first, plugin(a)));
        MsgEvent second = sent(a, b);
        assertTrue(signed(second), "one round trip later a signs again: b told it");
        assertNotNull(second.getParam(MsgSessions.KX), "and offers a new session on that very message");
        flow.add(arrive(b, second, plugin(a)));
        for (int i = 0; i < 18; i++) flow.add(arrive(b, sent(a, b), plugin(a)));

        assertEquals(Arrival.REFUSED, flow.get(0));
        assertEquals(1, refusedAt(b) - refused, "at most 2 messages are refused after a restart; with no message in flight, one");
        for (int i = 1; i < flow.size(); i++) assertEquals(Arrival.HANDED, flow.get(i), "message " + i + " of the flow");
        assertEquals(1, b.msgSigning().lostToldCount());
        assertEquals(1, a.msgSigning().lostHeardCount());

        // what b sent: the notice and nothing else. Signed, never under a session; shaped as the reply to no call
        List<MsgEvent> fromB = SENT.get(key(b)).subList(bSent, SENT.get(key(b)).size());
        assertEquals(1, fromB.size(), "b sent one notice");
        MsgEvent notice = fromB.get(0);
        assertTrue(signed(notice), "a notice is verified under its sender's key");
        assertEquals(first.getParam(MsgSessions.SID), notice.getParam(MsgSigning.LOST), "it names the session it speaks of");
        assertEquals("t1/rgA/a1", notice.getParam(MsgSigning.LOST_BY), "and the signer of the message that was refused");
        assertTrue(Long.parseLong(notice.getParam(MsgSigning.LOST_FOR)) >= 0, "and for how long it has not held it");
        assertFalse(MsgRouter.isRpcRequest(notice), "it asks for nothing: nobody answers it");
        assertEquals(MsgSigning.LOST_CALL, notice.getParam("callId-rgA-a1-null"));
        assertTrue(HANDED.get(key(a)).stream().noneMatch(m -> MsgSigning.LOST_ACTION.equals(m.getParam("action"))), "nobody at a is handed the notice");

        established(a, b);                                       // and a new session forms the usual way
    }

    @Test
    void withMessagesInFlightAtMostTwoAreRefusedAndOneNoticeIsSent() throws Exception {
        ControllerEngine a = agent("a2", Map.of()), b = agent("b2", Map.of());
        established(a, b);
        long refused = refusedAt(b);
        b.msgSigning().restartSessions();
        slowWire = new ArrayList<>();
        List<Object[]> onTheWire;
        try {
            // two messages left a before b's notice reached it
            assertEquals(Arrival.REFUSED, arrive(b, sent(a, b), plugin(a)));
            assertEquals(Arrival.REFUSED, arrive(b, sent(a, b), plugin(a)));
            onTheWire = slowWire;
        } finally {
            slowWire = null;
        }
        assertEquals(1, onTheWire.size(), "one notice for the two (one a second per peer)");
        assertEquals(1, b.msgSigning().lostToldCount());
        assertEquals(1, b.msgSigning().lostWithheldCount());
        MsgEvent notice = (MsgEvent) onTheWire.get(0)[1];
        assertEquals(Arrival.TAKEN, arrive(a, copy(notice), controller(b)));
        MsgEvent next = sent(a, b);
        assertTrue(signed(next));
        assertEquals(Arrival.HANDED, arrive(b, next, plugin(a)));
        assertEquals(2, refusedAt(b) - refused, "at most 2 refused");
    }

    /** {@code a}'s producer for a request sent again: signed as it leaves, kept for the test to deliver. */
    private static List<MsgEvent> resends(ControllerEngine a) {
        List<MsgEvent> sentAgain = new ArrayList<>();
        a.msgSigning().resendTap = again -> {
            a.msgSigning().signIfPending(again);
            sentAgain.add(copy(again));
            return true;
        };
        return sentAgain;
    }

    @Test
    void anRpcSentAfterThePeerLostTheSessionIsSentAgainOnceSignedAndItsCallerSeesSuccess() throws Exception {
        ControllerEngine a = agent("a3", Map.of()), b = agent("b3", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = resends(a);
        b.msgSigning().restartSessions();                        // b restarted: it holds nothing from here on
        Thread.sleep(150);
        // a does not know yet, and calls b: the request is sent AFTER b stopped holding the session
        String callId = UUID.randomUUID().toString();
        MsgEvent request = request(a, b, callId);
        assertTrue(macd(request));
        assertEquals(1, a.msgSigning().lostRetriesHeld(), "a remembers the request it sent under the session");
        int handedToA = HANDED.get(key(a)).size(), ranAtB = HANDED.get(key(b)).size(), bSent = SENT.get(key(b)).size();

        // b refuses it before anything ran; its 401 names the session, whose message it refused and for how long it has
        // not held the session; a sends the request again by itself
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
        assertEquals(ranAtB, HANDED.get(key(b)).size(), "refused before any handler saw it");
        MsgEvent refusal = SENT.get(key(b)).get(bSent);
        assertEquals("401", refusal.getParam("status_code"));
        assertEquals("t1/rgA/a3", refusal.getParam(MsgSigning.LOST_BY));
        assertTrue(Long.parseLong(refusal.getParam(MsgSigning.LOST_FOR)) >= 150, "b has not held it since it restarted: "
                + refusal.getParam(MsgSigning.LOST_FOR) + " ms");
        assertEquals(1, sentAgain.size(), "sent again, once");
        MsgEvent again = sentAgain.get(0);
        assertTrue(signed(again), "signed: never under a session, never unsigned");
        assertEquals(callId, again.getParam("callId-rgA-a3-p1"), "the same call");
        assertEquals(request.getParam("payload"), again.getParam("payload"));
        assertEquals(handedToA, HANDED.get(key(a)).size(), "the caller is not handed the refusal");
        assertEquals(1, a.msgSigning().lostRetriedCount());
        assertEquals(0, a.msgSigning().lostRetriesHeld(), "its record is gone: one decision per call");

        // b runs it, once, and answers; the caller sees the answer
        assertEquals(Arrival.HANDED, arrive(b, copy(again), plugin(a)));
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size(), "it ran once");
        MsgEvent answer = HANDED.get(key(b)).get(ranAtB);
        answer.setParam("status_code", "10");
        answer.setReturn();
        CallerStamps.stamp(answer, controller(b));
        b.markForSigning(answer);
        b.msgSigning().signIfPending(answer);
        assertEquals(Arrival.HANDED, arrive(a, copy(answer), controller(b)));
        MsgEvent seen = HANDED.get(key(a)).get(handedToA);
        assertEquals("10", seen.getParam("status_code"), "the caller sees success");
        assertEquals(callId, seen.getParam("callId-rgA-a3-p1"));

        // replay protection is as it was: the request sent again is refused when it comes a second time, the first copy
        // is under a session b will never hold again, and neither brings a second retry
        MsgSigning.Verdict replayed = b.msgSigning().verify(copy(again), plugin(a), false);
        assertEquals(MsgSigning.Refusal.REPLAY, replayed.reason, "the retry, replayed");
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)), "the first copy, replayed: b answers 401 again");
        assertEquals(1, sentAgain.size(), "at most one retry per call: the second refusal goes to the caller");
        assertEquals("401", HANDED.get(key(a)).get(handedToA + 1).getParam("status_code"));
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size(), "nothing ran twice");
    }

    @Test
    void aRequestThatMayHaveRunBeforeThePeerRestartedIsNotSentAgainAndItsCallerSeesThe401() throws Exception {
        // review: R runs at b, b restarts before it answers, a copy of R arrives (the broker delivers an unacknowledged
        // message again, or a relay kept one). b never held the session in this life -- and R ran all the same.
        ControllerEngine a = agent("a22", Map.of()), b = agent("b22", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = resends(a);
        String callId = UUID.randomUUID().toString();
        MsgEvent request = request(a, b, callId);
        assertTrue(macd(request));
        int ranAtB = HANDED.get(key(b)).size(), handedToA = HANDED.get(key(a)).size();
        assertEquals(Arrival.HANDED, arrive(b, copy(request), plugin(a)), "it ran");
        b.msgSigning().restartSessions();                        // b crashed before it answered, and is back
        long notRetried = a.msgSigning().lostNotRetriedCount();
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)), "its copy is refused: b does not hold the session");

        assertEquals(0, sentAgain.size(), "the request was sent BEFORE b stopped holding the session: it may have run, and is not sent again");
        assertEquals(notRetried + 1, a.msgSigning().lostNotRetriedCount());
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size(), "it ran once");
        MsgEvent seen = HANDED.get(key(a)).get(handedToA);
        assertEquals("401", seen.getParam("status_code"), "its caller sees the 401, exactly as on 1.3.378");
        assertEquals(callId, seen.getParam("callId-rgA-a22-p1"));
        assertNull(seen.getParam(MsgSigning.LOST), "and nothing of the notice");
        // a took b's word on the session all the same: it signs again
        assertEquals(1, a.msgSigning().lostHeardCount());
        assertTrue(signed(sent(a, b)));
    }

    @Test
    void aRequestUnderASessionThatEndedAtThePeerIsSentAgainOnlyWhenItWasSentAfterTheEnd() throws Exception {
        ControllerEngine a = agent("a23", Map.of()), b = agent("b23", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = resends(a);
        MsgEvent before = request(a, b, "before-the-end");
        assertTrue(macd(before));
        int ranAtB = HANDED.get(key(b)).size();
        assertEquals(Arrival.HANDED, arrive(b, copy(before), plugin(a)), "it ran while b held the session");
        // b's clock passes the session's end (as when an answer to the offer came late: see the offerer's end, below)
        MsgSessions.Session atB = b.msgSigning().sessionWith("rgA_a23");
        assertNotNull(atB);
        b.msgSigning().endSession(atB);
        int bSent = SENT.get(key(b)).size();
        assertEquals(Arrival.REFUSED, arrive(b, copy(before), plugin(a)), "a copy of it, delivered after the end");
        assertEquals("401", SENT.get(key(b)).get(bSent).getParam("status_code"));
        assertNotNull(SENT.get(key(b)).get(bSent).getParam(MsgSigning.LOST_FOR), "the ended session is kept, as ended at an instant");
        assertEquals(0, sentAgain.size(), "sent before the end: it may have run, and is not sent again");
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size());
        assertNull(b.msgSigning().sessionWith("rgA_a23"), "b sends nothing under it either");
    }

    @Test
    void theOfferersSessionEndsNoLaterThanTheAcceptorsHoweverLateTheAnswerCame() throws Exception {
        long life = 3_600_000L, t0 = 1_000_000_000_000L;
        MsgSessions a = new MsgSessions(life), b = new MsgSessions(life);
        MsgSigning.Signer sa = new MsgSigning.Signer("t1", "rgA", "a1", null, "fp-a", List.of(), null, null, null, Long.MAX_VALUE, false);
        MsgSigning.Signer sb = new MsgSigning.Signer("t1", "rgB", "b1", null, "fp-b", List.of(), null, null, null, Long.MAX_VALUE, false);
        String offer = a.offerFor("rgB_b1", t0, false);                                         // a offers at t0
        assertNotNull(offer);
        b.received("t1/rgB/b1", "t1/rgA/a1", "rgA_a1", "incA", sa, offer, null, null, null, true, t0 + 1_000);   // b takes it a second later
        String answer = b.ackFor("rgA_a1", t0 + 1_000, false);
        assertNotNull(answer);
        long late = t0 + 29_000;                                                                // and its answer reaches a 28 s after that
        a.received("t1/rgA/a1", "t1/rgB/b1", "rgB_b1", "incB", sb, null, answer, answer.split(":")[0], null, true, late);
        MsgSessions.Session atA = a.usable("rgB_b1", late), atB = b.find("rgA_a1", answer.split(":")[0]).session;
        assertNotNull(atA, "a holds the session");
        assertEquals(t0 + life, atA.expires, "counted from its offer, not from the answer");
        assertEquals(t0 + 1_000 + life, atB.expires);
        assertTrue(atA.expires <= atB.expires, "a, which sends under it, stops before b's end");
        assertNull(a.usable("rgB_b1", atB.expires - MsgSessions.USABLE_MARGIN_MS), "a minute before b's end a no longer sends under it");
    }

    @Test
    void aRefusalThatNamesNoSessionIsNeverRetried() throws Exception {
        // b behaves as release 1.3.378 does on the wire: it sends no notice and its 401 names no session
        ControllerEngine a = agent("a4", Map.of()), b = agent("b4", Map.of("msg_signing_lost_notices_per_second", "0"));
        established(a, b);
        List<MsgEvent> sentAgain = new ArrayList<>();
        a.msgSigning().resendTap = again -> { sentAgain.add(again); return true; };
        b.msgSigning().restartSessions();
        int handedToA = HANDED.get(key(a)).size(), bSent = SENT.get(key(b)).size();

        assertEquals(Arrival.REFUSED, arrive(b, sent(a, b), plugin(a)), "a one-way message: refused, and b says nothing");
        assertEquals(bSent, SENT.get(key(b)).size());
        MsgEvent request = request(a, b, UUID.randomUUID().toString());
        assertTrue(macd(request), "a still sends under the session: nobody told it");
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
        assertEquals(0, sentAgain.size(), "no retry on a refusal that does not name the session");
        assertEquals("401", HANDED.get(key(a)).get(handedToA).getParam("status_code"), "the caller sees the 401, as before");
        // and a recovered the old way: the 401 is b's word, with its new incarnation
        assertTrue(signed(sent(a, b)));
        assertEquals(0, a.msgSigning().lostHeardCount());
        established(a, b);
    }

    // ================================================================ the attacker

    @Test
    void aForgedNoticeDropsNoSession() throws Exception {
        ControllerEngine a = agent("a5", Map.of()), b = agent("b5", Map.of()), c = agent("b6", Map.of());
        established(a, b);
        established(a, c);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        assertNotNull(sid);
        String forB = "callId-rgA-a5-null";

        // a relay writes one: no signature at all
        MsgEvent bare = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b5", null, "rgA", "a5", null, false, false);
        bare.setParam("action", MsgSigning.LOST_ACTION);
        bare.setParam("is_rpc", "true");
        bare.setParam(forB, MsgSigning.LOST_CALL);
        bare.setParam(MsgSigning.LOST, sid);
        assertEquals(Arrival.REFUSED, arrive(a, copy(bare), controller(b)));

        // another node of the fabric signs one in b's name
        MsgEvent inBsName = notice(c, a, sid);
        inBsName.setParam(MsgSigning.BY, "t1/rgB/b5");
        assertEquals(Arrival.REFUSED, arrive(a, inBsName, controller(b)));

        // another node signs one as itself, about the session a holds with b: it speaks for its own sessions only
        long ignored = a.msgSigning().lostIgnoredCount();
        assertEquals(Arrival.TAKEN, arrive(a, notice(c, a, sid), controller(c)));
        assertEquals(ignored + 1, a.msgSigning().lostIgnoredCount());

        // b's own notice of another session, altered on the way to name this one: the signature covers the name
        MsgEvent altered = notice(b, a, "AAAAAAAAAAAAAAAAAAAAAAAA");
        altered.setParam(MsgSigning.LOST, sid);
        assertEquals(Arrival.REFUSED, arrive(a, altered, controller(b)));

        // under the session's own MAC (which only b could make): a notice counts under a signature, never under the
        // session it declares lost
        MsgEvent underMac = sent(b, a);
        assertTrue(macd(underMac));
        underMac.setParam(MsgSigning.LOST, sid);
        underMac.setParam(MsgSessions.MAC, Base64.getEncoder().encodeToString(MsgSessions.mac(
                b.msgSigning().sessionWith("rgA_a5").key, "t1/rgB/b5", MsgSigning.canonical(underMac))));
        assertEquals(Arrival.HANDED, arrive(a, underMac, plugin(b)), "a genuine MAC: the message is accepted");
        assertNull(HANDED.get(key(a)).get(HANDED.get(key(a)).size() - 1).getParam(MsgSigning.LOST), "and the notice in it is removed, unread");

        // b's own notice, genuine and about this very session -- of a message refused under ANOTHER signer's name (the same
        // region and agent in another tenant): not a message a signed, so not a's business
        ignored = a.msgSigning().lostIgnoredCount();
        assertEquals(Arrival.TAKEN, arrive(a, notice(b, a, sid, "tX/rgA/a5"), controller(b)));
        assertEquals(ignored + 1, a.msgSigning().lostIgnoredCount());

        assertEquals(0, a.msgSigning().lostHeardCount(), "a dropped nothing");
        MsgEvent still = sent(a, b);
        assertTrue(macd(still), "a still sends b messages under their session");
        assertEquals(sid, still.getParam(MsgSessions.SID));
        assertEquals(Arrival.HANDED, arrive(b, still, plugin(a)));
        assertTrue(macd(sent(a, c)), "and c's under theirs");
    }

    /** A notice as {@code from}'s signing layer makes it for {@code to} about {@code sid}: signed by {@code from}. */
    private static MsgEvent notice(ControllerEngine from, ControllerEngine to, String sid) {
        return notice(from, to, sid, "t1/" + to.cstate.getRegion() + "/" + to.cstate.getAgent());
    }

    /** As above, about a message refused from the signer {@code refused}. */
    private static MsgEvent notice(ControllerEngine from, ControllerEngine to, String sid, String refused) {
        slowWire = new ArrayList<>();
        try {
            // seconds apart from any notice before it: the rate limit is not what is tested here
            from.msgSigning().tellLost("t1", to.cstate.getRegion(), to.cstate.getAgent(), sid, refused,
                    System.currentTimeMillis() + (++noticeSeq) * 5_000L);
            assertEquals(1, slowWire.size(), "a notice was made");
            return copy((MsgEvent) slowWire.get(0)[1]);
        } finally {
            slowWire = null;
        }
    }

    private static int noticeSeq;

    @Test
    void aNoticeOfAnOlderSessionDeliveredLateOrReplayedIsIgnored() throws Exception {
        ControllerEngine a = agent("a7", Map.of()), b = agent("b7", Map.of());
        established(a, b);
        String first = sent(a, b).getParam(MsgSessions.SID);
        b.msgSigning().restartSessions();
        // b's notice of the first session is kept on the wire...
        slowWire = new ArrayList<>();
        MsgEvent notice;
        try {
            assertEquals(Arrival.REFUSED, arrive(b, sent(a, b), plugin(a)));
            notice = (MsgEvent) slowWire.get(0)[1];
        } finally {
            slowWire = null;
        }
        assertEquals(first, notice.getParam(MsgSigning.LOST));
        // ...while a and b agree a new session (b spoke: a returned to signatures the old way)
        assertEquals(Arrival.HANDED, arrive(a, sent(b, a), plugin(b)));
        established(a, b);
        String second = sent(a, b).getParam(MsgSessions.SID);
        assertNotEquals(first, second);

        // now it arrives: authentic, never seen before -- and about a session a no longer sends under
        long heard = a.msgSigning().lostHeardCount(), ignored = a.msgSigning().lostIgnoredCount();
        assertEquals(Arrival.TAKEN, arrive(a, copy(notice), controller(b)));
        assertEquals(heard, a.msgSigning().lostHeardCount(), "it names a session that is not the current one: nothing is dropped");
        assertEquals(ignored + 1, a.msgSigning().lostIgnoredCount());
        // a keeps the session it holds now. (Like ANY message b signed before the two agreed it, this one names no
        // session b holds, so a waits for b's next word before it sends under the session again -- the rule that was
        // there before notices. Nothing is agreed anew: no offer, the same session.)
        long made = a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount();
        MsgEvent still = sent(a, b);
        assertEquals(second, still.getParam(MsgSessions.HELD), "a keeps the session it holds now");
        assertNull(still.getParam(MsgSessions.KX), "and offers no other");
        assertEquals(Arrival.HANDED, arrive(b, still, plugin(a)));
        MsgEvent word = sent(b, a);
        assertTrue(macd(word));
        assertEquals(second, word.getParam(MsgSessions.SID));
        assertEquals(Arrival.HANDED, arrive(a, word, plugin(b)));
        still = sent(a, b);
        assertTrue(macd(still));
        assertEquals(second, still.getParam(MsgSessions.SID));
        assertEquals(Arrival.HANDED, arrive(b, still, plugin(a)));
        assertEquals(made, a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount());

        // and a second copy of it is refused like any replayed signature, and answered by nothing
        int aSent = SENT.get(key(a)).size();
        MsgSigning.Verdict again = a.msgSigning().verify(copy(notice), controller(b), false);
        assertEquals(MsgSigning.Refusal.REPLAY, again.reason);
        a.msgSigning().answerRefused(copy(notice), again);
        assertEquals(aSent, SENT.get(key(a)).size(), "a refused notice is answered by nothing");
        still = sent(a, b);
        assertTrue(macd(still), "a replayed notice changes nothing at all");
        assertEquals(second, still.getParam(MsgSessions.SID));
    }

    @Test
    void aFloodOfBadMacsBringsOneNoticeASecondPerPeerAndNoNoticeForANotice() throws Exception {
        ControllerEngine a = agent("a8", Map.of()), b = agent("b8", Map.of("msg_signing_lost_notices_per_second", "3"));
        established(a, b);
        MsgEvent template = sent(a, b);
        assertTrue(macd(template));
        b.msgSigning().restartSessions();
        slowWire = new ArrayList<>();                            // nothing b sends is delivered: a goes on as if deaf
        try {
            // one peer, 500 bad MACs, well inside a second
            long t0 = System.currentTimeMillis();
            for (int i = 0; i < 500; i++) {
                MsgEvent bad = copy(template);
                bad.setParam("payload", "flood " + i);
                assertEquals(Arrival.REFUSED, arrive(b, bad, plugin(a)));
            }
            long seconds = (System.currentTimeMillis() - t0) / 1000 + 1;
            assertTrue(b.msgSigning().lostToldCount() <= seconds, b.msgSigning().lostToldCount() + " notices to one peer in " + seconds + " s");
            assertTrue(b.msgSigning().lostToldCount() >= 1);
            assertEquals(500, b.msgSigning().lostToldCount() + b.msgSigning().lostWithheldCount());

            // 40 names nobody ever heard of: the node's own bound (3 a second here), and nothing remembered per name
            long told = b.msgSigning().lostToldCount();
            t0 = System.currentTimeMillis();
            for (int i = 0; i < 40; i++) {
                MsgEvent bad = copy(template);
                bad.setParam(MsgSigning.BY, "t1/rgA/ghost" + i);
                assertEquals(Arrival.REFUSED, arrive(b, bad, Caller.plugin("t1", "rgA", "ghost" + i, "p1")));
            }
            long windows = System.currentTimeMillis() / 1000 - t0 / 1000 + 1;
            long more = b.msgSigning().lostToldCount() - told;
            assertTrue(more <= 3 * windows, more + " notices in " + windows + " second(s), the node's bound being 3 a second");
            assertTrue(b.msgSigning().lostNoticePeers() <= b.msgSigning().lostToldCount(), "an entry per notice sent, none per message refused");

            // a signer's name that is no name (it would name a list of queues, or every queue) is told nothing
            Thread.sleep(1100);
            told = b.msgSigning().lostToldCount();
            for (String noName : new String[]{"t1/rgA/x,T.t1.rgA_a8", "t1/rgA/*", "t1/rg>/x", "t1/rgA/a_8", "t.1/rgA/x", "t1/rgA/" + "x".repeat(65)}) {
                MsgEvent bad = copy(template);
                bad.setParam(MsgSigning.BY, noName);
                assertFalse(b.msgSigning().verify(bad, plugin(a), false).ok());
            }
            MsgEvent noSid = copy(template);
            noSid.setParam(MsgSigning.BY, "t1/rgA/ghost-sid");
            noSid.setParam(MsgSessions.SID, "not a session id; DROP");
            assertFalse(b.msgSigning().verify(noSid, plugin(a), false).ok());
            assertEquals(told, b.msgSigning().lostToldCount(), "nothing of an attacker's reaches a destination name or a notice");

            // a message under a MAC that carries a notice is refused, and not answered with a notice
            told = b.msgSigning().lostToldCount();
            long withheld = b.msgSigning().lostWithheldCount();
            MsgEvent noticeUnderMac = copy(template);
            noticeUnderMac.setParam(MsgSigning.BY, "t1/rgA/ghost-notice");
            noticeUnderMac.setParam(MsgSigning.LOST, template.getParam(MsgSessions.SID));
            assertEquals(Arrival.REFUSED, arrive(b, noticeUnderMac, Caller.plugin("t1", "rgA", "ghost-notice", "p1")));
            assertEquals(told, b.msgSigning().lostToldCount(), "no notice for a notice");
            assertEquals(withheld, b.msgSigning().lostWithheldCount(), "it was not even weighed against the rate");

            // an RPC request is answered as before, once each: the answer names the session, and there is no notice beside it
            int sentByB = SENT.get(key(b)).size();
            MsgEvent rpc = copy(template);
            rpc.setParam("is_rpc", "true");
            rpc.setParam("callId-rgA-a8-p1", "c1");
            assertEquals(Arrival.REFUSED, arrive(b, rpc, plugin(a)));
            assertEquals(sentByB + 1, SENT.get(key(b)).size());
            MsgEvent answer = SENT.get(key(b)).get(sentByB);
            assertEquals("401", answer.getParam("status_code"));
            assertEquals(template.getParam(MsgSessions.SID), answer.getParam(MsgSigning.LOST));
            assertTrue(signed(answer));
            assertEquals(told + 1, b.msgSigning().lostToldCount(), "counted as told");
            // ...and only when it goes to the node the request names as its signer: a request that says it came from
            // one node and was signed by another is answered 401 as before, with no word about any session
            MsgEvent elsewhere = copy(template);
            elsewhere.setParam("is_rpc", "true");
            elsewhere.setParam("callId-rgA-a8-p1", "c2");
            elsewhere.setParam(MsgSigning.BY, "t1/rgA/ghost-rpc");
            assertEquals(Arrival.REFUSED, arrive(b, elsewhere, Caller.plugin("t1", "rgA", "ghost-rpc", "p1")));
            assertEquals(sentByB + 2, SENT.get(key(b)).size());
            assertEquals("401", SENT.get(key(b)).get(sentByB + 1).getParam("status_code"));
            assertNull(SENT.get(key(b)).get(sentByB + 1).getParam(MsgSigning.LOST));
        } finally {
            slowWire = null;
        }
        // a never heard any of it: its own word is what counts, and it still (wrongly, but harmlessly) holds the session
        assertEquals(0, a.msgSigning().lostHeardCount());
    }

    @Test
    void theHolderOfASessionNeverSaysNotHeldWhateverSignerNameIsWrittenOverTheMessage() throws Exception {
        // review (critical): a relay reads a's session id off the wire and sends b one message with that id, a junk MAC
        // and ANOTHER tenant's name for the same region and agent. b looked its sessions up by node and name, missed,
        // and signed "not held" of a session it held: a dropped it; one such message every 10 s kept the pair on signatures
        ControllerEngine a = agent("a20", Map.of()), b = agent("b20", Map.of());
        established(a, b);
        MsgEvent seen = sent(a, b);                              // what a relay sees: the session id in the clear
        assertTrue(macd(seen));
        String sid = seen.getParam(MsgSessions.SID);
        assertEquals(Arrival.HANDED, arrive(b, copy(seen), plugin(a)), "b HOLDS the session: the genuine message is accepted");
        long heard = a.msgSigning().lostHeardCount(), told = b.msgSigning().lostToldCount(), withheld = b.msgSigning().lostWithheldCount();
        int bSent = SENT.get(key(b)).size();
        for (Caller stamp : new Caller[]{plugin(a), Caller.plugin("tX", "rgA", "a20", "p1"), null}) {
            MsgEvent forged = copy(seen);
            forged.setParam(MsgSigning.BY, "tX/rgA/a20");
            forged.setParam(MsgSessions.MAC, "AAAA");
            forged.setParam("payload", "forged " + stamp);
            MsgSigning.Verdict v = b.msgSigning().verify(forged, stamp, false);
            assertFalse(v.ok());
            assertNull(v.notHeld, "a session b holds is never reported as not held");
            assertTrue(v.refusal.contains("this node holds"), v.refusal);
            b.msgSigning().answerRefused(forged, v);
        }
        assertEquals(told, b.msgSigning().lostToldCount(), "b told nobody anything");
        assertEquals(withheld, b.msgSigning().lostWithheldCount(), "and spent nobody's notice on it");
        assertEquals(bSent, SENT.get(key(b)).size());
        assertEquals(heard, a.msgSigning().lostHeardCount());
        MsgEvent next = sent(a, b);
        assertTrue(macd(next), "a still sends under the session");
        assertEquals(sid, next.getParam(MsgSessions.SID));
        assertEquals(Arrival.HANDED, arrive(b, next, plugin(a)));
    }

    @Test
    void aRelayCannotGetARequestRunTwiceWhileTheCalleeHoldsTheSession() throws Exception {
        // review (critical): the relay delivers request R (it runs), then a copy of it with another tenant's name over the
        // signer's and a junk MAC. b answered a signed 401 naming session and call; a sent R again, signed; R ran twice
        ControllerEngine a = agent("a21", Map.of()), b = agent("b21", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = resends(a);
        String callId = UUID.randomUUID().toString();
        MsgEvent request = request(a, b, callId);
        assertTrue(macd(request));
        int ranAtB = HANDED.get(key(b)).size(), handedToA = HANDED.get(key(a)).size(), bSent = SENT.get(key(b)).size();
        assertEquals(Arrival.HANDED, arrive(b, copy(request), plugin(a)), "the genuine request runs");
        MsgEvent forged = copy(request);
        forged.setParam(MsgSigning.BY, "tX/rgA/a21");
        forged.setParam(MsgSessions.MAC, "AAAA");
        assertEquals(Arrival.REFUSED, arrive(b, forged, plugin(a)));
        // b answers the forged request 401, as it answers any refused request -- with no word about any session
        assertEquals(bSent + 1, SENT.get(key(b)).size());
        MsgEvent refusal = SENT.get(key(b)).get(bSent);
        assertEquals("401", refusal.getParam("status_code"));
        assertNull(refusal.getParam(MsgSigning.LOST));
        assertNull(refusal.getParam(MsgSigning.LOST_BY));
        assertEquals(0, sentAgain.size(), "a sends nothing again");
        assertEquals(0, a.msgSigning().lostRetriedCount());
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size(), "the request ran once");
        assertEquals("401", HANDED.get(key(a)).get(handedToA).getParam("status_code"), "the relay made the call fail, which it could do by dropping it");
        assertTrue(macd(sent(a, b)), "and the session stands");
    }

    @Test
    void anotherNodesMessageThatNamesACallDoesNotRemoveItsRecord() throws Exception {
        ControllerEngine a = agent("a24", Map.of()), b = agent("b24", Map.of()), c = agent("b25", Map.of());
        established(a, b);
        established(a, c);
        List<MsgEvent> sentAgain = resends(a);
        b.msgSigning().restartSessions();
        Thread.sleep(150);
        String callId = UUID.randomUUID().toString();
        MsgEvent request = request(a, b, callId);
        assertEquals(1, a.msgSigning().lostRetriesHeld());
        // c (any node a verifies) sends a message that names the call: it is handed on, and the record stays
        MsgEvent fromC = sent(c, a);
        MsgEvent naming = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b25", "p1", "rgA", "a24", "p1", false, false);
        naming.setParam("action", "report");
        naming.setParam("is_rpc", "true");
        naming.setParam("callId-rgA-a24-p1", callId);
        CallerStamps.stamp(naming, plugin(c));
        c.markForSigning(naming);
        c.msgSigning().signIfPending(naming);
        assertEquals(Arrival.HANDED, arrive(a, copy(naming), plugin(c)));
        assertEquals(1, a.msgSigning().lostRetriesHeld(), "only the peer the request went to answers for it");
        assertEquals(Arrival.HANDED, arrive(a, fromC, plugin(c)));
        // b's refusal still brings the retry
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
        assertEquals(1, sentAgain.size());
    }

    @Test
    void aNodeThatOnlyWritesAnothersNameSpendsNobodysNotice() throws Exception {
        // review: the per-peer second and the node's budget were spent on the name a message merely declared
        ControllerEngine a = agent("a26", Map.of()), b = agent("b26", Map.of("msg_signing_lost_notices_per_second", "2"));
        established(a, b);
        MsgEvent template = sent(a, b);
        assertTrue(macd(template));
        b.msgSigning().restartSessions();
        slowWire = new ArrayList<>();
        try {
            // another node's flood in a's name (its broker stamp says who it is), and messages with no stamp at all
            for (int i = 0; i < 50; i++) {
                MsgEvent bad = copy(template);
                bad.setParam("payload", "flood " + i);
                assertFalse(b.msgSigning().verify(bad, i % 2 == 0 ? Caller.plugin("t1", "rgA", "other", "p1") : null, false).ok());
            }
            assertEquals(0, b.msgSigning().lostToldCount(), "no notice for a message the broker did not stamp as that node's");
            assertEquals(0, b.msgSigning().lostWithheldCount(), "and neither a's second nor the node's budget was spent");
            // a's own message, right after: it is told at once
            assertEquals(Arrival.REFUSED, arrive(b, copy(template), plugin(a)));
            assertEquals(1, b.msgSigning().lostToldCount());
        } finally {
            slowWire = null;
        }
    }

    // ================================================================ a link known to be down

    @Test
    void whenTheWayToAPeerIsKnownDownWhatIsSentItIsSignedAndVerifiesAfterItsRestart() throws Exception {
        // the stored backlog: a sender went on under a session for 30 s after its peer last confirmed it, also when its own
        // broker had just lost the way to that peer; what a broker stored of that for a peer that restarted was refused
        ControllerEngine a = agent("a50", Map.of()), b = agent("b50", Map.of()), c = agent("b51", Map.of());
        established(a, b);
        established(a, c);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        a.msgSigning().inboxConsumerLeft("rgB_b50");             // a's broker lost the consumer of b's inbox
        MsgEvent stored = sent(a, b);                            // what a sends now waits in a broker for b
        assertTrue(signed(stored), "signed: not under the session");
        assertEquals(sid, stored.getParam(MsgSessions.HELD), "the session is kept, not dropped");
        assertNull(stored.getParam(MsgSessions.KX), "and nothing is agreed anew");
        assertTrue(macd(sent(a, c)), "the session with another peer is not touched: one peer per event");
        long refused = b.msgSigning().refusedCount();
        b.msgSigning().restartSessions();                        // b restarts
        assertEquals(Arrival.HANDED, arrive(b, stored, plugin(a)), "what was stored for it verifies when it is back");
        assertEquals(refused, b.msgSigning().refusedCount(), "nothing is refused");
    }

    @Test
    void aPeerThatDidNotRestartGoesOnWithTheSessionOnceItConfirmsIt() throws Exception {
        ControllerEngine a = agent("a52", Map.of()), b = agent("b52", Map.of());
        established(a, b);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        long made = a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount();
        a.msgSigning().inboxConsumerLeft("rgB_b52");             // the way to b went, and came back
        MsgEvent first = sent(a, b);
        assertTrue(signed(first));
        assertEquals(Arrival.HANDED, arrive(b, first, plugin(a)));
        MsgEvent word = sent(b, a);                              // b's next word names the session it still holds
        assertEquals(Arrival.HANDED, arrive(a, word, plugin(b)));
        MsgEvent next = sent(a, b);
        assertTrue(macd(next), "a sends under it again");
        assertEquals(sid, next.getParam(MsgSessions.SID), "the same session");
        assertEquals(Arrival.HANDED, arrive(b, next, plugin(a)));
        assertEquals(made, a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount(), "nothing was agreed anew");
    }

    @Test
    void anAgentWhoseLinkToItsControllerIsInterruptedUsesNoneOfItsSessionsAndAControllerIsToldNodeByNode() throws Exception {
        ControllerEngine a = agent("a53", Map.of()), b = agent("b53", Map.of()), c = agent("b54", Map.of());
        established(a, b);
        established(a, c);
        a.msgSigning().parentLinkDown();                         // the agent's one link: every session ran over it
        assertTrue(signed(sent(a, b)));
        assertTrue(signed(sent(a, c)));
        // each resumes with its peer's next word
        assertEquals(Arrival.HANDED, arrive(a, sent(b, a), plugin(b)));
        assertTrue(macd(sent(a, b)));
        assertTrue(signed(sent(a, c)), "c has not confirmed yet");

        // a controller's own broker tells it node by node: its link to the node above it says nothing about the others
        established(rA, rB);
        rA.msgSigning().parentLinkDown();
        assertTrue(macd(sent(rA, rB)), "a controller's sessions are ended by its broker's events, one node each");
        rA.msgSigning().inboxConsumerLeft("rgB_rc2");
        assertTrue(signed(sent(rA, rB)));
        // and when the node whose inbox consumer left is its own parent, what it sends it is signed too (no direct link)
        rA.msgSigning().inboxConsumerLeft("rgG_gc");
        MsgEvent up = sent(rA, g);
        assertTrue(signed(up), "to the global: signed, not unsigned over a link that is gone");
        assertNull(up.getParam(MsgSigning.DIRECT));
    }

    @Test
    void aPluginCannotMakeItsNodeSayASessionIsLost() throws Exception {
        ControllerEngine a = agent("a9", Map.of()), b = agent("b9", Map.of());
        established(a, b);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        // a plugin of b writes the notice into a message of its own: every stamping point removes it before the node signs
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b9", "p1", "rgA", "a9", null, false, false);
        m.setParam("action", "report");
        m.setParam(MsgSigning.LOST, sid);
        b.stampAs(m, plugin(b), null);
        b.msgSigning().signIfPending(m);
        assertNull(m.getParam(MsgSigning.LOST), "only the signing layer writes a notice");
        assertEquals(Arrival.HANDED, arrive(a, copy(m), plugin(b)));
        assertEquals(0, a.msgSigning().lostHeardCount());
        assertTrue(macd(sent(a, b)));
    }

    // ================================================================ together with the route gate (RouteGate, RpcExpiry)

    private static int consumerIds;

    private static ConsumerId consumer() {
        return new ConsumerId(new SessionId(new ConnectionId("lost-session-" + (++consumerIds)), 1), 1);
    }

    /** The queue a message for {@code node} is sent to, as AgentProducer names it in tenant t1. */
    private static String inbox(String node) { return "T.t1." + node; }

    /**
     * {@code e} as a controller with a broker of its own: its route gate reads that broker (told of its consumers by the
     * test), and what it hands its producer is signed as the producer does it and kept, in order.
     */
    private static List<MsgEvent> broker(ControllerEngine e) throws Exception {
        List<MsgEvent> toBroker = new CopyOnWriteArrayList<>();
        java.lang.reflect.Field client = ControllerEngine.class.getDeclaredField("activeClient");
        client.setAccessible(true);
        client.set(e, new ActiveClient(e) {
            @Override public boolean sendMessage(MsgEvent sm) {
                e.msgSigning().signIfPending(sm);
                return toBroker.add(copy(sm));
            }
            @Override public String queueOf(MsgEvent msg) { return msg.getForwardDst() == null ? null : inbox(msg.getForwardDst()); }
        });
        e.routeGate().bound(System.currentTimeMillis() - 600_000L);
        return toBroker;
    }

    /** A controller of a region of its own under the global, known to region B's nodes made after it. */
    private static ControllerEngine controllerOfItsOwnRegion(String region, String agent, Map<String, Object> extra) throws Exception {
        ControllerEngine c = node(region, agent, true, extra);
        joined(c, g, true);
        ping(c, g, true);
        ping(c, g, true);
        ping(rB, g, true);                                       // region B takes the trust bundle that has the new controller
        return c;
    }

    private static void awaitTrue(java.util.function.BooleanSupplier c, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + 5_000;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(5);
        assertTrue(c.getAsBoolean(), what);
    }

    @Test
    void aRequestSentAgainLeavesAsAFirstSendDoesThroughTheRouteGateAndSayingWhatItSaid() throws Exception {
        // before the two branches met, the request sent again went straight to the producer: past the route gate (it
        // could wait in a queue nothing consumes), where every first send is judged
        ControllerEngine c = controllerOfItsOwnRegion("rgC", "rc60", Map.of("route_rebuild_grace_ms", "2000"));
        ControllerEngine b1 = agent("b60", Map.of()), b2 = agent("b63", Map.of()), b3 = agent("b64", Map.of());
        established(c, b1);
        established(c, b2);
        established(c, b3);
        List<MsgEvent> toBroker = broker(c);
        ConsumerId way1 = consumer(), way2 = consumer(), way3 = consumer();
        c.routeGate().consumerAdded(way1, new ActiveMQQueue(inbox("rgB_b60")));       // the demand of the bridge toward each
        c.routeGate().consumerAdded(way2, new ActiveMQQueue(inbox("rgB_b63")));
        c.routeGate().consumerAdded(way3, new ActiveMQQueue(inbox("rgB_b64")));
        try {
            for (ControllerEngine b : List.of(b1, b2, b3)) b.msgSigning().restartSessions();
            Thread.sleep(150);
            int handed = HANDED.get(key(c)).size();

            // 1. there is a way to the peer: the copy goes to the producer, saying what the request said of its caller's wait
            String first = UUID.randomUUID().toString();
            MsgEvent r1 = request(c, b1, first, 20_000);
            assertTrue(macd(r1));
            assertEquals(Arrival.REFUSED, arrive(b1, copy(r1), plugin(c)));           // its 401 comes back; c sends the request again
            assertEquals(1, toBroker.size(), "sent again, through the producer");
            MsgEvent again = toBroker.get(0);
            assertTrue(signed(again));
            assertEquals(first, again.getParam("callId-rgC-rc60-p1"));
            assertEquals("20000", again.getParam(RpcExpiry.TIMEOUT_PARAM), "what the request said stays (an application may have signed it)");
            assertEquals(80_000L, RpcExpiry.ttlMs(again, 300_000L, 60_000L), "so the brokers on its way expire it as they would the first");
            assertEquals(handed, HANDED.get(key(c)).size(), "its caller is not handed the refusal");
            assertEquals(Arrival.HANDED, arrive(b1, copy(again), plugin(c)), "and the peer runs it");

            // 2. the way to the peer is being made anew: the gate holds the copy, and sends it when the way is back
            c.routeGate().consumerRemoved(way2, System.currentTimeMillis());
            String second = UUID.randomUUID().toString();
            MsgEvent r2 = request(c, b2, second, 20_000);
            assertTrue(macd(r2));
            assertEquals(Arrival.REFUSED, arrive(b2, copy(r2), plugin(c)));
            assertEquals(1, c.routeGate().heldNow(), "held by the gate, as a first send is");
            assertEquals(1, toBroker.size(), "not handed to a broker that has no way for it");
            assertEquals(handed, HANDED.get(key(c)).size(), "the refusal is not a second answer");
            c.routeGate().consumerAdded(consumer(), new ActiveMQQueue(inbox("rgB_b63")));
            awaitTrue(() -> toBroker.size() == 2, "the held copy was not sent when the way came back");
            assertTrue(signed(toBroker.get(1)), "signed still: " + toBroker.get(1).getParams().keySet());
            assertEquals(second, toBroker.get(1).getParam("callId-rgC-rc60-p1"));
            assertEquals(Arrival.HANDED, arrive(b2, copy(toBroker.get(1)), plugin(c)));

            // 3. there is no way to the peer: the gate answers the caller, once, and the refusal is not handed on as well
            // (this controller does not know the peer's tenant first hand, so the gate answers after one grace, as it does
            // for every caller it cannot place: NoRouteAcrossTenantsTest)
            c.routeGate().consumerRemoved(way3, System.currentTimeMillis() - 120_000L);
            String third = UUID.randomUUID().toString();
            MsgEvent r3 = request(c, b3, third, 20_000);
            assertTrue(macd(r3));
            long noRoute = c.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE);
            slowWire = new ArrayList<>();                         // what the two nodes send is handed over by the test
            try {
                assertEquals(Arrival.REFUSED, arrive(b3, copy(r3), plugin(c)));
                MsgEvent refusal = (MsgEvent) slowWire.remove(0)[1];
                assertEquals("401", refusal.getParam("status_code"));
                int sentByC = SENT.get(key(c)).size();
                assertEquals(Arrival.TAKEN, arrive(c, copy(refusal), controller(b3)), "the 401 is taken: the answer is the gate's now");
                awaitTrue(() -> SENT.get(key(c)).size() > sentByC, "the gate did not answer the caller");
                Thread.sleep(100);
                assertEquals(sentByC + 1, SENT.get(key(c)).size(), "one answer");
                MsgEvent answer = SENT.get(key(c)).get(sentByC);
                assertEquals(MsgSigning.RESEND_HOP, answer.getParam(MsgRouter.ROUTE_UNAVAILABLE));
                assertEquals(third, answer.getParam("callId-rgC-rc60-p1"), "to the call that waits");
                assertEquals("p1", answer.getDstPlugin());
                assertTrue(String.valueOf(answer.getParam("error")).contains("no route to rgB_b64"), String.valueOf(answer.getParam("error")));
            } finally {
                slowWire = null;
            }
            assertEquals(2, toBroker.size(), "nothing was left in a queue nothing consumes");
            assertEquals(noRoute + 1, c.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE), "counted once, as the gate counts it");
            assertEquals(handed, HANDED.get(key(c)).size());
        } finally {
            c.routeGate().stop();
        }
    }

    @Test
    void aCallerThatHasStoppedWaitingGetsNothingSentAgain() throws Exception {
        ControllerEngine a = agent("a62", Map.of()), b = agent("b62", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = resends(a);
        b.msgSigning().restartSessions();
        Thread.sleep(150);
        MsgEvent request = request(a, b, UUID.randomUUID().toString(), 40);    // its caller waits 40 ms
        assertTrue(macd(request));
        Thread.sleep(60);
        int handed = HANDED.get(key(a)).size();
        long not = a.msgSigning().lostNotRetriedCount();
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
        assertTrue(sentAgain.isEmpty(), "nobody waits for its answer: it is not sent again");
        assertEquals(not + 1, a.msgSigning().lostNotRetriedCount());
        assertEquals("401", HANDED.get(key(a)).get(handed).getParam("status_code"), "the refusal goes where it went before");
    }

    @Test
    void theGatesAnswerARefusalAndANoticeAreNeverAnsweredByOneAnother() throws Exception {
        // three answers a node makes by itself: `route unavailable` (the route gate), `401` and the lost-session notice
        // (the signing layer). Each is answered only to an RPC REQUEST, and none of them is one
        ControllerEngine a = agent("a61", Map.of()), b = agent("b61", Map.of());
        established(a, b);
        ControllerEngine c = node("rgC", "rc61", true, Map.of("route_rebuild_grace_ms", "0"));
        List<MsgEvent> toBroker = broker(c);                     // a controller whose broker has no way to anybody
        slowWire = new ArrayList<>();
        try {
            b.msgSigning().restartSessions();
            Thread.sleep(20);
            MsgEvent request = request(a, b, "call-61");
            assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
            MsgEvent refusal = (MsgEvent) slowWire.remove(0)[1];             // b's 401, bearing the notice
            assertEquals("401", refusal.getParam("status_code"));
            assertNotNull(refusal.getParam(MsgSigning.LOST));
            assertEquals(Arrival.REFUSED, arrive(b, sent(a, b), plugin(a)));
            MsgEvent notice = (MsgEvent) slowWire.remove(0)[1];              // b's notice of its own
            assertEquals(MsgSigning.LOST_ACTION, notice.getParam("action"));
            assertTrue(MsgRouter.answerWithError(g.getPluginBuilder(), request(a, b, "call-62"), MsgRouter.ROUTE_UNAVAILABLE, "REMOTE_REGION",
                    "route unavailable at rgG_gc: REMOTE_REGION: no route to rgB_b61"));
            MsgEvent unavailable = (MsgEvent) slowWire.remove(0)[1];         // the global's answer that there is no route
            assertEquals("REMOTE_REGION", unavailable.getParam(MsgRouter.ROUTE_UNAVAILABLE));
            assertEquals("a61", unavailable.getDstAgent());

            // the gate judged a request here (the control): answered at once, nothing handed to the broker
            assertFalse(c.routeKnown(request(a, b, "call-63"), "REMOTE_REGION", true));
            assertEquals(1, c.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE));
            assertEquals(1, slowWire.size(), "the gate's answer to a request");
            slowWire.clear();
            int n = 0;
            for (MsgEvent answer : List.of(refusal, notice, unavailable)) {
                n++;
                assertFalse(MsgRouter.isRpcRequest(answer), "answer " + n + " is no request");
                // 1. on its way through a controller with no route for it: never held, never answered `route unavailable`
                assertTrue(c.routeKnown(copy(answer), "REMOTE_REGION", true), "answer " + n + " was judged by the route gate");
                assertEquals(0, c.routeGate().heldNow());
                assertEquals(1, c.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE), "answer " + n);
                // 2. refused where it arrives (changed on its way): never answered 401, and no notice for it
                MsgEvent changed = copy(answer);
                changed.setParam("action", "changed-on-the-way");
                ControllerEngine to = NODES.get(changed.getDstRegion() + "_" + changed.getDstAgent());
                assertSame(a, to);
                assertEquals(Arrival.REFUSED, arrive(a, changed, n == 3 ? controller(g) : controller(b)));
                assertTrue(slowWire.isEmpty(), "answer " + n + " was answered: " + (slowWire.isEmpty() ? "" : ((MsgEvent) slowWire.get(0)[1]).getParams()));
            }
            assertTrue(toBroker.isEmpty());
        } finally {
            slowWire = null;
            c.routeGate().stop();
        }
    }

    // ================================================================ a call the peer does not answer

    /** A request of {@code from}'s plugin for {@code to}, stamped and marked to be signed, not yet handed to the producer. */
    private static MsgEvent unsent(ControllerEngine from, ControllerEngine to, String callId) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-p1", callId);
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        return m;
    }

    /** {@code b}'s answer to the request it was handed last, as it leaves {@code b}. */
    private static MsgEvent answerOf(ControllerEngine b) {
        List<MsgEvent> handed = HANDED.get(key(b));
        MsgEvent answer = copy(handed.get(handed.size() - 1));
        answer.setParam("status_code", "10");
        answer.setReturn();
        CallerStamps.stamp(answer, controller(b));
        b.markForSigning(answer);
        b.msgSigning().signIfPending(answer);
        return copy(answer);
    }

    @Test
    void afterOneUnansweredCallTheNextMessageToThatPeerIsSignedAndARestartedPeerAcceptsIt() throws Exception {
        // the backlog between agents: an agent has no broker to tell it that a node beyond its controller went away. It
        // went on under the session for 30 s after the peer's last word, and what waited for the peer in a broker was
        // refused when the peer came back restarted (90 to 100 messages per storage core per restart)
        ControllerEngine a = agent("a70", Map.of()), b = agent("b70", Map.of()), c = agent("b71", Map.of());
        established(a, b);
        established(a, c);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        MsgEvent call = request(a, b, UUID.randomUUID().toString(), 60);     // its caller waits 60 ms; b is gone: no answer
        assertTrue(macd(call));
        assertEquals(1, a.msgSigning().callsWatched(), "the call is awaited");
        assertTrue(macd(sent(a, b)), "while its caller still waits nothing changes");
        Thread.sleep(80);
        MsgEvent stored = sent(a, b);                            // the next message to b: it waits in a broker for b
        assertTrue(signed(stored), "signed: the call to b was not answered");
        assertEquals(sid, stored.getParam(MsgSessions.HELD), "the session is kept, not dropped");
        assertNull(stored.getParam(MsgSessions.KX), "and nothing is agreed anew");
        assertEquals(0, a.msgSigning().callsWatched(), "the call is no longer awaited");
        assertEquals(1, a.msgSigning().callsUnansweredCount());
        assertTrue(macd(sent(a, c)), "the session with another peer is not touched: one peer per event");
        long refused = b.msgSigning().refusedCount();
        b.msgSigning().restartSessions();                        // b comes back, restarted
        assertEquals(Arrival.HANDED, arrive(b, stored, plugin(a)), "what was stored for it verifies");
        assertEquals(refused, b.msgSigning().refusedCount(), "nothing is refused");
    }

    @Test
    void aPeerThatIsAliveAndOnlySlowCostsSignaturesUntilItsNextWordAndTheSessionGoesOn() throws Exception {
        ControllerEngine a = agent("a72", Map.of()), b = agent("b72", Map.of());
        established(a, b);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        long made = a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount();
        MsgEvent call = request(a, b, UUID.randomUUID().toString(), 50);
        assertEquals(Arrival.HANDED, arrive(b, copy(call), plugin(a)));          // b has it, and takes its time
        Thread.sleep(70);                                                        // a's caller has stopped waiting
        MsgEvent first = sent(a, b);
        assertTrue(signed(first), "one timeout: what a sends b is signed");
        assertEquals(sid, first.getParam(MsgSessions.HELD));
        assertEquals(Arrival.HANDED, arrive(b, first, plugin(a)));
        MsgEvent word = sent(b, a);                                              // b's next word, whatever it is
        assertTrue(macd(word), "b still holds the session and uses it");
        assertEquals(Arrival.HANDED, arrive(a, word, plugin(b)));
        MsgEvent next = sent(a, b);
        assertTrue(macd(next), "a sends under it again");
        assertEquals(sid, next.getParam(MsgSessions.SID), "the same session");
        assertEquals(Arrival.HANDED, arrive(b, next, plugin(a)));
        assertEquals(made, a.msgSigning().sessionsMadeCount() + b.msgSigning().sessionsMadeCount(), "nothing was agreed anew");
        assertEquals(1, a.msgSigning().callsUnansweredCount());
    }

    @Test
    void aCallThatWasAnsweredIsNoTimeoutAndAMessageThatOnlyNamesItsIdIsNoAnswer() throws Exception {
        ControllerEngine a = agent("a73", Map.of()), b = agent("b73", Map.of()), c = agent("b74", Map.of());
        established(a, b);
        established(a, c);
        String id = UUID.randomUUID().toString();
        MsgEvent call = request(a, b, id, 80);
        assertEquals(1, a.msgSigning().callsWatched());
        // another node's own message that names the call id is no answer to it
        MsgEvent other = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b74", "p1", "rgA", "a73", "p1", false, false);
        other.setParam("action", "report");
        other.setParam("is_rpc", "true");
        other.setParam("callId-rgA-a73-p1", id);
        CallerStamps.stamp(other, plugin(c));
        c.markForSigning(other);
        c.msgSigning().signIfPending(other);
        assertEquals(Arrival.HANDED, arrive(a, copy(other), plugin(c)));
        assertEquals(1, a.msgSigning().callsWatched(), "still awaited from b");
        // b answers in time
        assertEquals(Arrival.HANDED, arrive(b, copy(call), plugin(a)));
        assertEquals(Arrival.HANDED, arrive(a, answerOf(b), controller(b)));
        assertEquals(0, a.msgSigning().callsWatched(), "answered: no longer awaited");
        Thread.sleep(100);
        assertTrue(macd(sent(a, b)), "no timeout: a goes on under the session");

        // a controller on the way answers in b's place with something else than "no route" (the tier's refusal): the
        // caller has its answer, the call does not time out, and it says nothing of b
        MsgEvent refusedCall = request(a, b, UUID.randomUUID().toString(), 80);
        assertEquals(1, a.msgSigning().callsWatched());
        slowWire = new ArrayList<>();
        try {
            assertTrue(MsgRouter.answerWithError(g.getPluginBuilder(), copy(refusedCall), MsgRouter.ROUTE_REFUSED, "tier", "refused at rgG_gc: not here"));
            assertEquals(Arrival.HANDED, arrive(a, copy((MsgEvent) slowWire.remove(0)[1]), controller(g)));
        } finally {
            slowWire = null;
        }
        assertEquals(0, a.msgSigning().callsWatched());
        Thread.sleep(100);
        assertTrue(macd(sent(a, b)), "answered, though not by b: no timeout");
        assertEquals(0, a.msgSigning().callsUnansweredCount());
        // a call that states no wait is not watched (an older library): nothing changes for it
        request(a, b, UUID.randomUUID().toString());
        assertEquals(0, a.msgSigning().callsWatched());
    }

    @Test
    void aCallAnsweredRouteUnavailableSetsTheSessionAsideAtOnceButOnlyACallThisNodeAwaits() throws Exception {
        ControllerEngine a = agent("a75", Map.of()), b = agent("b75", Map.of());
        established(a, b);
        String sid = sent(a, b).getParam(MsgSessions.SID);
        String id = UUID.randomUUID().toString();
        MsgEvent call = request(a, b, id, 20_000);
        assertTrue(macd(call));
        slowWire = new ArrayList<>();                            // what the global answers is handed over by the test
        try {
            // a node of the fabric says "no route to b" for a call a does not await: nothing changes
            assertEquals(1, a.msgSigning().callsWatched());
            MsgEvent unknown = copy(call);
            unknown.setParam("callId-rgA-a75-p1", "a-call-a-never-made");
            assertTrue(MsgRouter.answerWithError(g.getPluginBuilder(), unknown, MsgRouter.ROUTE_UNAVAILABLE, "REMOTE_REGION",
                    "route unavailable at rgG_gc: REMOTE_REGION: no route to rgB_b75"));
            MsgEvent notAwaited = (MsgEvent) slowWire.remove(0)[1];
            assertEquals(Arrival.HANDED, arrive(a, copy(notAwaited), controller(g)));
            assertTrue(macd(sent(a, b)), "an answer for a call this node does not await sets nothing aside");
            assertEquals(0, a.msgSigning().callsUnansweredCount());

            // the global answers the call itself: there is no route to b (its region is gone)
            assertTrue(MsgRouter.answerWithError(g.getPluginBuilder(), copy(call), MsgRouter.ROUTE_UNAVAILABLE, "REMOTE_REGION",
                    "route unavailable at rgG_gc: REMOTE_REGION: no route to rgB_b75"));
            MsgEvent unavailable = (MsgEvent) slowWire.remove(0)[1];
            int handed = HANDED.get(key(a)).size();
            assertEquals(Arrival.HANDED, arrive(a, copy(unavailable), controller(g)), "the caller gets the answer, as before");
            assertEquals(id, HANDED.get(key(a)).get(handed).getParam("callId-rgA-a75-p1"));
            MsgEvent stored = sent(a, b);                        // at once: no wait for the caller's timeout
            assertTrue(signed(stored), "signed: there is no route to b");
            assertEquals(sid, stored.getParam(MsgSessions.HELD), "the session is kept");
            assertEquals(1, a.msgSigning().callsUnansweredCount());
            assertEquals(0, a.msgSigning().callsWatched());
            long refused = b.msgSigning().refusedCount();
            b.msgSigning().restartSessions();
            assertEquals(Arrival.HANDED, arrive(b, stored, plugin(a)), "b, back and restarted, verifies it");
            assertEquals(refused, b.msgSigning().refusedCount());
        } finally {
            slowWire = null;
        }
    }

    @Test
    void theCallsAwaitedFromOnePeerAreBoundedAndACallThatIsNotWatchedChangesNothing() throws Exception {
        ControllerEngine a = agent("a76", Map.of()), b = agent("b76", Map.of());
        established(a, b);
        int over = 5;
        for (int i = 0; i < MsgSessions.CALLS_PER_PEER + over; i++) {
            assertTrue(macd(request(a, b, UUID.randomUUID().toString(), 60_000)), "call " + i);
        }
        assertEquals(MsgSessions.CALLS_PER_PEER, a.msgSigning().callsWatched(), "at most " + MsgSessions.CALLS_PER_PEER + " per peer");
        assertEquals(over, a.msgSigning().callsNotWatchedCount());
        assertTrue(macd(sent(a, b)));
        // switched off (msg_signing_unanswered_calls=false), nothing is watched and nothing is set aside
        ControllerEngine d = agent("a77", Map.of("msg_signing_unanswered_calls", "false")), e = agent("b77", Map.of());
        established(d, e);
        request(d, e, UUID.randomUUID().toString(), 30);
        assertEquals(0, d.msgSigning().callsWatched(), "switched off: nothing is watched");
        Thread.sleep(50);
        assertTrue(macd(sent(d, e)), "and the session is used as on 1.3.378");
    }

    @Test
    void aControllerWhoseOwnBrokerHasNoRouteToAPeerSignsToItFromThenOn() throws Exception {
        ControllerEngine c = controllerOfItsOwnRegion("rgD", "rc78", Map.of("route_rebuild_grace_ms", "0"));
        ControllerEngine b = agent("b78", Map.of());
        established(c, b);
        broker(c);                                               // nothing at c's broker consumes b's inbox
        slowWire = new ArrayList<>();
        try {
            assertTrue(macd(sent(c, b)));
            assertFalse(c.routeKnown(unsent(c, b, UUID.randomUUID().toString()), "REMOTE_REGION", false), "answered: no route");
            assertEquals(1, slowWire.size(), "its caller is told");
            assertTrue(signed(sent(c, b)), "and what c sends b from now on is signed");
            assertEquals(1, c.msgSigning().callsUnansweredCount());
            // a request c only carries on for another node says nothing of c's own session with b
            ControllerEngine a = agent("a78", Map.of());
            established(c, b);
            assertTrue(macd(sent(c, b)));
            assertFalse(c.routeKnown(unsent(a, b, UUID.randomUUID().toString()), "REMOTE_REGION", true));
            assertTrue(macd(sent(c, b)), "another node's request: that node hears the answer and acts on it");
        } finally {
            slowWire = null;
            c.routeGate().stop();
        }
    }
}
