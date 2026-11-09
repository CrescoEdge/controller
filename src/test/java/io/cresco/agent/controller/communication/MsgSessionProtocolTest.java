package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The session-key protocol ({@link MsgSessions}) on its own, on a simulated clock: what each side attaches to a message it
 * sends (as MsgSigning.signIfPending does) and makes of what it receives (as MsgSigning.verify does once a message is
 * authenticated). Renewal never interrupts a confirmed session; offers ride signed messages only and are accepted at most
 * once per interval per peer; a full table gives no new sessions and evicts none in use.
 */
class MsgSessionProtocolTest {

    private static final String A = "t1/rgA/a1", B = "t1/rgB/b1", C = "t1/rgC/c1", AN = "rgA_a1", BN = "rgB_b1", CN = "rgC_c1";
    private static final long LIFE = 3_600_000L, T0 = 1_000_000_000L;

    private static MsgSigning.Signer signer(String name) {
        String[] p = name.split("/");
        return new MsgSigning.Signer(p[0], p[1], p[2], null, "fp-" + name, List.of(), null, null, null, Long.MAX_VALUE, false);
    }

    /** What a message carries. */
    private static final class Out {
        String held, ack, kx, sid;
        boolean macd() { return sid != null; }
    }

    private static Out out(MsgSessions s, String peerNode, long now) throws Exception {
        Out o = new Out();
        MsgSessions.Session ss = s.usable(peerNode, now);
        o.held = s.heldWith(peerNode);
        o.ack = s.ackFor(peerNode, now, ss != null);
        o.kx = s.offerFor(peerNode, now, ss != null);
        if (ss != null && o.kx == null && o.ack == null) o.sid = ss.sid;
        return o;
    }

    /** {@code self} receives {@code o} from {@code by}: a MAC'd one must name a session it holds. */
    private static void in(MsgSessions s, String self, String by, String byNode, Out o, String inc, long now) throws Exception {
        if (o.macd()) assertNotNull(s.lookup(byNode, by, o.sid), self + " does not hold the session " + by + " sent under");
        s.received(self, by, byNode, inc, signer(by), o.kx, o.ack, o.held, o.sid, !o.macd(), now);
    }

    private static final String INC_A = "incA", INC_B = "incB";

    /** A and B agree a session and confirm it both ways; returns the time reached. */
    private static long established(MsgSessions a, MsgSessions b, long t) throws Exception {
        in(b, B, A, AN, out(a, BN, t), INC_A, t);                 // A's offer
        in(a, A, B, BN, out(b, AN, t + 1), INC_B, t + 1);         // B's answer (signed)
        Out ab = out(a, BN, t + 2);
        assertTrue(ab.macd(), "A sends a MAC once it holds the answer");
        in(b, B, A, AN, ab, INC_A, t + 2);
        Out ba = out(b, AN, t + 3);
        assertTrue(ba.macd(), "B sends a MAC once A named the session");
        in(a, A, B, BN, ba, INC_B, t + 3);
        return t + 4;
    }

    @Test
    void aRenewalNeverInterruptsTheConfirmedSession() throws Exception {
        MsgSessions a = new MsgSessions(LIFE), b = new MsgSessions(LIFE);
        long t = established(a, b, T0);
        String s1 = a.heldWith(BN);
        assertEquals(s1, b.heldWith(AN));
        // keep talking until A's session is due for renewal (a tenth of its life before its end)
        long renewAt = T0 + LIFE - 300_000L + 10;
        for (long now = t; now < renewAt - 10_000; now += 10_000) {
            Out ab = out(a, BN, now), ba = out(b, AN, now + 1);
            assertTrue(ab.macd() && ba.macd(), "at +" + (now - T0) + " ms: A " + (ab.macd() ? "MAC" : "signed kx=" + (ab.kx != null) + " ack=" + (ab.ack != null))
                    + ", B " + (ba.macd() ? "MAC" : "signed kx=" + (ba.kx != null) + " ack=" + (ba.ack != null)));
            in(b, B, A, AN, ab, INC_A, now);
            in(a, A, B, BN, ba, INC_B, now + 1);
        }
        Out offer = out(a, BN, renewAt);
        assertNotNull(offer.kx, "A offers a renewal");
        assertFalse(offer.macd(), "an offer rides a signed message");
        assertEquals(s1, offer.held, "A still holds the session");
        in(b, B, A, AN, offer, INC_A, renewAt);
        assertEquals(s1, b.heldWith(AN), "the renewal displaced B's confirmed session");
        Out answer = out(b, AN, renewAt + 1);
        assertNotNull(answer.ack, "B answers on its next message (signed)");
        Out meanwhile = out(b, AN, renewAt + 2);
        assertTrue(meanwhile.macd(), "B keeps sending MACs under the confirmed session while the renewal is under way");
        assertEquals(s1, meanwhile.sid);
        Out aMeanwhile = out(a, BN, renewAt + 2);
        assertTrue(aMeanwhile.macd(), "A keeps sending MACs too (the offer rides at most one message a second)");
        in(b, B, A, AN, aMeanwhile, INC_A, renewAt + 2);
        in(a, A, B, BN, meanwhile, INC_B, renewAt + 2);
        in(a, A, B, BN, answer, INC_B, renewAt + 3);
        String s2 = a.heldWith(BN);
        assertNotEquals(s1, s2, "A took the renewed session");
        Out underNew = out(a, BN, renewAt + 4);
        assertEquals(s2, underNew.sid, "A sends under the new session");
        in(b, B, A, AN, underNew, INC_A, renewAt + 4);
        assertEquals(s2, b.heldWith(AN), "B promoted the new session once A used it");
        assertEquals(s2, out(b, AN, renewAt + 5).sid);
        assertNotNull(b.lookup(AN, A, s1), "a message sent under the old one meanwhile still verifies");
    }

    @Test
    void anOfferIsTakenOnlyFromASignedMessageAndAtMostOncePerInterval() throws Exception {
        MsgSessions a = new MsgSessions(LIFE), b = new MsgSessions(LIFE);
        long t = established(a, b, T0) + MsgSessions.OFFER_ACCEPT_MIN_MS;
        long accepted = b.offersAccepted.get();
        // a stream of fresh offers from A (a node making a new key pair for every message)
        for (int i = 0; i < 50; i++) {
            MsgSessions fresh = new MsgSessions(LIFE);
            Out o = out(fresh, BN, t + i);
            in(b, B, A, AN, o, INC_A, t + i);
        }
        assertEquals(accepted + 1, b.offersAccepted.get(), "more than one offer accepted within the interval");
        // an offer on a MAC'd message (no signature: no fresh credential) is never acted on
        MsgSessions other = new MsgSessions(LIFE);
        Out o = out(other, BN, t + 20_000);
        long before = b.offersAccepted.get();
        b.received(B, A, AN, INC_A, signer(A), o.kx, null, a.heldWith(BN), a.heldWith(BN), false, t + 20_000);
        assertEquals(before, b.offersAccepted.get(), "an offer under a MAC was taken");
    }

    @Test
    void aPeerThatNamesAnotherSessionStopsTheMacsAtOnce() throws Exception {
        MsgSessions a = new MsgSessions(LIFE), b = new MsgSessions(LIFE);
        long t = established(a, b, T0);
        assertNotNull(b.usable(AN, t));
        Out lost = new Out();                                     // A's signed message naming no session (it lost it)
        in(b, B, A, AN, lost, INC_A, t);
        assertNull(b.usable(AN, t), "B kept sending MACs to a peer that said it holds no session");
    }

    @Test
    void aFullTableGivesNoNewSessionAndEvictsNoneInUse() throws Exception {
        MsgSessions a = new MsgSessions(LIFE, 1), b = new MsgSessions(LIFE), c = new MsgSessions(LIFE);
        long t = established(a, b, T0);
        String sab = a.heldWith(BN);
        // C offers A a session: A's table is full
        in(a, A, C, CN, out(c, AN, t), "incC", t);
        assertNull(a.heldWith(CN), "a session made past the table's bound");
        assertNull(out(a, CN, t + 1).kx, "an offer made past the table's bound");
        assertEquals(sab, a.heldWith(BN), "the session in use was evicted");
        assertTrue(out(a, BN, t + 2).macd());
    }
}
