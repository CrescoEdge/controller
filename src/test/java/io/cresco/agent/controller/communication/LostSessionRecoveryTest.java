package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
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

    @Test
    void anRpcRefusedForALostSessionIsSentAgainOnceSignedAndItsCallerSeesSuccess() throws Exception {
        ControllerEngine a = agent("a3", Map.of()), b = agent("b3", Map.of());
        established(a, b);
        List<MsgEvent> sentAgain = new ArrayList<>();
        // the producer's part: the request sent again is signed as it leaves, and reaches b
        a.msgSigning().resendTap = again -> {
            a.msgSigning().signIfPending(again);
            sentAgain.add(copy(again));
            return true;
        };
        String callId = UUID.randomUUID().toString();
        MsgEvent request = request(a, b, callId);
        assertTrue(macd(request));
        assertEquals(1, a.msgSigning().lostRetriesHeld(), "a keeps what it takes to send the request again");
        b.msgSigning().restartSessions();
        int handedToA = HANDED.get(key(a)).size(), ranAtB = HANDED.get(key(b)).size();

        // b refuses it before anything ran, and its 401 names the session; a sends the request again by itself
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)));
        assertEquals(ranAtB, HANDED.get(key(b)).size(), "refused before any handler saw it");
        assertEquals(1, sentAgain.size(), "sent again, once");
        MsgEvent again = sentAgain.get(0);
        assertTrue(signed(again), "signed: never under a session, never unsigned");
        assertEquals(callId, again.getParam("callId-rgA-a3-p1"), "the same call");
        assertEquals(request.getParam("payload"), again.getParam("payload"));
        assertEquals(handedToA, HANDED.get(key(a)).size(), "the caller is not handed the refusal");
        assertEquals(1, a.msgSigning().lostRetriedCount());
        assertEquals(0, a.msgSigning().lostRetriesHeld(), "its record is gone: one retry per call");

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
        assertEquals(Arrival.REFUSED, arrive(b, copy(request), plugin(a)), "the first copy, replayed: b answers 401 and names the session again");
        assertEquals(1, sentAgain.size(), "at most one retry per call: the second refusal goes to the caller");
        assertEquals("401", HANDED.get(key(a)).get(handedToA + 1).getParam("status_code"));
        assertEquals(ranAtB + 1, HANDED.get(key(b)).size(), "nothing ran twice");
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

        assertEquals(0, a.msgSigning().lostHeardCount(), "a dropped nothing");
        MsgEvent still = sent(a, b);
        assertTrue(macd(still), "a still sends b messages under their session");
        assertEquals(sid, still.getParam(MsgSessions.SID));
        assertEquals(Arrival.HANDED, arrive(b, still, plugin(a)));
        assertTrue(macd(sent(a, c)), "and c's under theirs");
    }

    /** A notice as {@code from}'s signing layer makes it for {@code to} about {@code sid}: signed by {@code from}. */
    private static MsgEvent notice(ControllerEngine from, ControllerEngine to, String sid) {
        slowWire = new ArrayList<>();
        try {
            // seconds apart from any notice before it: the rate limit is not what is tested here
            from.msgSigning().tellLost("t1", to.cstate.getRegion(), to.cstate.getAgent(), sid,
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
}
