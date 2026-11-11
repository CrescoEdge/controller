package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An inbox holds what it cannot judge while its node waits for its parent's trust data: in the first moments of EVERY
 * join, for a bounded time. The wait was counted from the making of the inbox: a region, whose inbox is made when the
 * region starts, never waited when it joined (or rejoined) a global later -- and accepted, until the global's first pong,
 * a key the global had revoked.
 */
class TrustWaitTest {

    @Test
    void theWaitBelongsToEachJoinAndIsBounded() {
        TrustWait w = new TrustWait(15_000L);
        long t = 1_800_000_000_000L;
        assertFalse(w.waiting(true, t), "a node that holds its parent's trust data waits");
        // a join an hour after the inbox was made: not ready until the parent's pong is in
        t += 3_600_000L;
        assertTrue(w.waiting(false, t), "a join long after the inbox was made is not waited for");
        assertTrue(w.waiting(false, t + 14_999L));
        assertFalse(w.waiting(false, t + 15_000L), "a parent that never answers holds the node back for ever");
        assertFalse(w.waiting(false, t + 600_000L));
        // the pong came; the node rejoins later (its global was away): a new wait
        assertFalse(w.waiting(true, t + 700_000L));
        assertTrue(w.waiting(false, t + 800_000L), "the next join is not waited for");
        assertFalse(w.waiting(true, t + 800_050L), "ready, and still waiting");
        assertTrue(w.waiting(false, t + 900_000L));
        assertFalse(w.waiting(false, t + 915_000L));
    }

    @Test
    void aClockSetBackDoesNotEndOrExtendTheWaitBeyondItsTime() {
        TrustWait w = new TrustWait(10_000L);
        long t = 1_800_000_000_000L;
        assertTrue(w.waiting(false, t));
        assertTrue(w.waiting(false, t - 60_000L), "the clock went back: the wait starts anew from there");
        assertFalse(w.waiting(false, t - 50_000L));
        assertFalse(new TrustWait(0L).waiting(false, t), "no hold time, no wait");
    }
}
