package io.cresco.agent.controller.statemachine;

import io.cresco.library.agent.ControllerMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parent-loss recovery debounce must never lose a loss. Live (2026-10-16) two global restarts in quick
 * succession stranded an agent for more than 2.5 minutes: its second loss report fell inside the 30 s window after
 * the first recovery and was dropped, and link:parent is edge-triggered (it fires once on the crossing to CRITICAL),
 * so nothing ever raised it again. Regions had a recheck for this; agents did not.
 */
class LossReportDebounceTest {

    private Timer timer;

    @BeforeEach
    void setUp() {
        timer = new Timer("loss-debounce-test", true);
    }

    @AfterEach
    void tearDown() {
        timer.cancel();
    }

    @Test
    void reportsAreSuppressedOnlyInsideTheWindowAfterARecovery() {
        AtomicLong now = new AtomicLong(1_000_000L);
        LossReportDebounce d = new LossReportDebounce(30_000L, now::get);

        assertFalse(d.suppress(), "no recovery yet: the first loss is never suppressed");
        assertEquals(-1L, d.sinceRecovery());

        d.recoveryCompleted();
        now.addAndGet(29_999L);
        assertTrue(d.suppress());
        now.addAndGet(1L);
        assertFalse(d.suppress(), "the window is half-open: a report at exactly 30 s recovers");
    }

    @Test
    void theRecheckRunsJustPastTheEndOfTheWindow() {
        AtomicLong now = new AtomicLong(5_000_000L);
        LossReportDebounce d = new LossReportDebounce(30_000L, now::get);
        d.recoveryCompleted();

        now.addAndGet(28_000L); // second loss reported 28 s after the recovery
        assertEquals(2_000L + LossReportDebounce.RECHECK_SLACK_MS, d.recheckDelayMs());
        now.addAndGet(d.recheckDelayMs());
        assertFalse(d.suppress(), "the recheck's re-raise must land outside the window");

        now.addAndGet(60_000L);
        assertEquals(LossReportDebounce.RECHECK_SLACK_MS, d.recheckDelayMs(), "never scheduled in the past");
    }

    @Test
    void theLossToReraiseFollowsTheNodesCurrentTier() {
        assertEquals("regionalControllerLost", LossReportDebounce.lossEventToReraise(ControllerMode.AGENT, false));
        assertEquals("globalControllerLost", LossReportDebounce.lossEventToReraise(ControllerMode.REGION_GLOBAL, false));
        // the link came back: the suppressed report was a duplicate, nothing to recover
        assertNull(LossReportDebounce.lossEventToReraise(ControllerMode.AGENT, true));
        assertNull(LossReportDebounce.lossEventToReraise(ControllerMode.REGION_GLOBAL, true));
        // no parent link in these tiers, or the node is mid-recovery already
        for (ControllerMode m : new ControllerMode[]{ControllerMode.AGENT_FAILED, ControllerMode.AGENT_INIT,
                ControllerMode.REGION, ControllerMode.REGION_GLOBAL_FAILED, ControllerMode.GLOBAL,
                ControllerMode.STANDALONE, null}) {
            assertNull(LossReportDebounce.lossEventToReraise(m, false), String.valueOf(m));
        }
    }

    @Test
    void rechecksCoalesceAndReArmOnceRun() throws Exception {
        LossReportDebounce d = new LossReportDebounce(200L);
        d.recoveryCompleted();
        AtomicInteger runs = new AtomicInteger();

        assertTrue(d.scheduleRecheck(timer, runs::incrementAndGet));
        for (int i = 0; i < 5; i++) {
            assertFalse(d.scheduleRecheck(timer, runs::incrementAndGet), "a burst costs one recheck");
        }
        assertTrue(d.recheckPending());
        await("recheck ran", 5_000L, () -> runs.get() == 1);
        assertFalse(d.recheckPending());
        Thread.sleep(300L);
        assertEquals(1, runs.get());

        assertTrue(d.scheduleRecheck(timer, runs::incrementAndGet), "a later suppressed report arms a new recheck");
        await("second recheck ran", 5_000L, () -> runs.get() == 2);
    }

    @Test
    void aStoppedTimerLeavesNoRecheckPending() {
        LossReportDebounce d = new LossReportDebounce(200L);
        d.recoveryCompleted();
        timer.cancel();
        assertFalse(d.scheduleRecheck(timer, () -> { }));
        assertFalse(d.recheckPending());
    }

    // --- the handler's use of the debounce, against an edge-triggered detector ---------------------------------

    /**
     * Mirrors ControllerSMHandler.regionalControllerLost / recheckLoss: a report outside the window recovers
     * (re-attaching to whatever parent is up), one inside it is suppressed and rechecked after the window.
     */
    private static final class Node {
        final LossReportDebounce debounce;
        final Timer timer;
        final AtomicBoolean parentUp = new AtomicBoolean(true);
        final AtomicInteger recoveries = new AtomicInteger();
        final AtomicInteger suppressed = new AtomicInteger();
        volatile ControllerMode mode = ControllerMode.AGENT;

        Node(long windowMs, Timer timer) {
            this.debounce = new LossReportDebounce(windowMs);
            this.timer = timer;
        }

        synchronized void regionalControllerLost() {
            if (debounce.suppress()) {
                suppressed.incrementAndGet();
                debounce.scheduleRecheck(timer, this::recheck);
                return;
            }
            recoveries.incrementAndGet();
            parentUp.set(true); // stateInit loops until it is re-attached to a (restarted) parent
            mode = ControllerMode.AGENT;
            debounce.recoveryCompleted();
        }

        void recheck() {
            if (LossReportDebounce.lossEventToReraise(mode, parentUp.get()) != null) {
                new Thread(this::regionalControllerLost, "parent-loss-recheck").start();
            }
        }
    }

    @Test
    void aNewLossInsideTheWindowIsRecoveredAfterIt() throws Exception {
        Node n = new Node(300L, timer);
        n.parentUp.set(false);
        n.regionalControllerLost();           // global restart #1: recovered, re-attached
        assertEquals(1, n.recoveries.get());

        n.parentUp.set(false);                // global restart #2 right after the recovery
        n.regionalControllerLost();           // link:parent fires ONCE, inside the window
        assertEquals(1, n.suppressed.get());
        assertEquals(1, n.recoveries.get(), "inside the window the report does not recover at once");

        // no further report ever comes (edge-triggered): the recheck alone must recover the node
        await("recovered after the window", 5_000L, () -> n.recoveries.get() == 2);
        assertTrue(n.parentUp.get());
    }

    @Test
    void aDuplicateReportOfTheRecoveredLossDoesNotRecoverAgain() throws Exception {
        Node n = new Node(200L, timer);
        n.parentUp.set(false);
        n.regionalControllerLost();
        n.regionalControllerLost();           // the second detector repeating the same loss; link is up
        n.regionalControllerLost();
        assertEquals(2, n.suppressed.get());

        await("recheck ran", 5_000L, () -> !n.debounce.recheckPending());
        Thread.sleep(300L);
        assertEquals(1, n.recoveries.get(), "the debounce still stops a duplicate from replaying a recovery");
    }

    @Test
    void aStormOfReportsCostsOneExtraRecovery() throws Exception {
        Node n = new Node(300L, timer);
        n.parentUp.set(false);
        n.regionalControllerLost();
        n.parentUp.set(false);
        for (int i = 0; i < 10; i++) {
            n.regionalControllerLost();
        }
        await("recovered after the window", 5_000L, () -> n.recoveries.get() == 2);
        Thread.sleep(500L);
        assertEquals(2, n.recoveries.get(), "suppressed reports coalesce into one recheck");
    }

    @Test
    void aRecheckLandingInANewerWindowReArmsInsteadOfBeingLost() throws Exception {
        Node n = new Node(1_000L, timer);
        n.parentUp.set(false);
        n.regionalControllerLost();               // recovery A
        n.parentUp.set(false);
        n.regionalControllerLost();               // suppressed, recheck armed for ~A + window + slack (2 s)

        Thread.sleep(1_500L);
        n.debounce.recoveryCompleted();           // another recovery completed meanwhile (window moves on)
        // the recheck fires inside the newer window: its re-raise is suppressed again and must re-arm
        await("recovered after the newer window", 10_000L, () -> n.recoveries.get() == 2);
        assertTrue(n.suppressed.get() >= 2, "the re-raise was suppressed once and rechecked again");
    }

    private static void await(String what, long ms, java.util.function.BooleanSupplier cond) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > end) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(10L);
        }
    }
}
