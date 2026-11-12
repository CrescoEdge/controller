package io.cresco.agent.controller.netdiscovery;

import org.junit.jupiter.api.Test;

import static io.cresco.agent.controller.netdiscovery.TCPDiscoveryStatic.Outcome.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * When a joining node tries again: as often as before while nothing listens (a fabric forms as fast as before once its
 * controller is up), a bounded exponential backoff with jitter when the responder is there and behind -- never every node
 * every 3 s in step -- and one line that says who was asked, how long it took and when the next attempt is.
 */
class JoinBackoffTest {

    @Test
    void whileNothingListensItAsksAsOftenAsBeforeAndNotInStep() {
        long min = Long.MAX_VALUE, max = 0, sum = 0;
        JoinBackoff b = new JoinBackoff(3000, 1000, 30_000);
        for (int i = 0; i < 2000; i++) {
            long w = b.failed(UNREACHABLE, -1);
            min = Math.min(min, w); max = Math.max(max, w); sum += w;
        }
        assertTrue(min >= 2250 && max <= 3750, "waits " + min + ".." + max + " ms");
        assertTrue(max - min > 1000, "every node waits the same: " + min + ".." + max);
        assertEquals(3000, sum / 2000.0, 100, "the mean wait is no longer the 3 s it was");
    }

    @Test
    void whenTheResponderIsBehindTheWaitDoublesUpToItsBoundWithJitter() {
        // the largest and the smallest random number: the range of each wait
        JoinBackoff hi = new JoinBackoff(3000, 1000, 30_000, () -> 0.999999), lo = new JoinBackoff(3000, 1000, 30_000, () -> 0.0);
        long[] steps = {1000, 2000, 4000, 8000, 16_000, 30_000, 30_000, 30_000};
        for (long step : steps) {
            long h = hi.failed(NO_ANSWER, -1), l = lo.failed(BUSY, 0);
            assertEquals(step, h, 2, "longest wait at this failure");
            assertEquals(step / 2, l, 2, "shortest wait at this failure");
        }
        assertEquals(8, hi.attempts());
        hi.succeeded();
        assertEquals(0, hi.attempts());
        assertEquals(1000, hi.failed(INCOMPLETE, -1), 2, "after a success the wait starts over");
        // the first attempt that fails is retried sooner than the 3 s it was
        assertTrue(new JoinBackoff(3000, 1000, 30_000).failed(NO_ANSWER, -1) <= 1000);
    }

    @Test
    void aBusyRespondersTimeIsKeptAndNeverMoreThanTheBound() {
        JoinBackoff lo = new JoinBackoff(3000, 1000, 30_000, () -> 0.0), hi = new JoinBackoff(3000, 1000, 30_000, () -> 0.999999);
        assertEquals(5000, lo.failed(BUSY, 5000), "asked again before the time the responder gave");
        assertEquals(7500, hi.failed(BUSY, 5000), 2, "the nodes it sent away return together (no spread over its time)");
        // the time is not authenticated: a responder (or whoever answers in its place) cannot park a node for long
        assertEquals(30_000, lo.failed(BUSY, 86_400_000L));
        assertEquals(30_000, hi.failed(BUSY, Long.MAX_VALUE));
    }

    @Test
    void oneLineSaysWhoWasAskedWhatHappenedHowLongItTookAndWhenItTriesAgain() {
        TCPDiscoveryStatic.Attempt a = new TCPDiscoveryStatic.Attempt();
        a.host = "10.30.0.16"; a.port = 30005; a.elapsedMs = 10_012; a.outcome = NO_ANSWER;
        assertEquals("Discovery: joining the controller at 10.30.0.16:30005: 10.30.0.16:30005 did not answer in 10012 ms; attempt 3,"
                + " trying again in 4.2 s", JoinBackoff.line(a, "joining the controller at 10.30.0.16:30005", 3, 4210));
        a.outcome = BUSY; a.busyRetryMs = 2000; a.elapsedMs = 15;
        String busy = JoinBackoff.line(a, "joining", 1, 2600);
        assertTrue(busy.contains("10.30.0.16:30005 is busy and asked this node to come back in 2000 ms") && busy.endsWith("trying again in 2.6 s"), busy);
        a.outcome = UNREACHABLE; a.detail = "Connection refused: /10.30.0.16:30005"; a.elapsedMs = 2;
        String gone = JoinBackoff.line(a, "joining", 7, 3100);
        assertTrue(gone.contains("cannot be reached (Connection refused: /10.30.0.16:30005, after 2 ms)") && gone.contains("attempt 7"), gone);
        for (TCPDiscoveryStatic.Outcome o : new TCPDiscoveryStatic.Outcome[]{NO_ANSWER, BUSY, UNREACHABLE, INCOMPLETE}) {
            a.outcome = o;
            String line = JoinBackoff.line(a, "joining", 2, 1500);
            assertFalse(line.contains("Exception") || line.contains("Index 0 out of bounds"), "an exception text: " + line);
        }
    }
}
