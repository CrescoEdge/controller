package io.cresco.agent.controller.statemachine;

import io.cresco.library.agent.ControllerMode;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * The recovery debounce for parent-loss reports ({@code regionalControllerLost} on an agent,
 * {@code globalControllerLost} on a region under a global), which coalesces a report inside the window instead of
 * dropping it.
 *
 * <p>Two detectors can report the SAME loss episode (the HC-&gt;MINA bridge and, on a region, BrokeredAgent), and
 * MINA queues the second event behind the first recovery; replaying it would tear down the controller that was
 * just recovered. So a report arriving within {@code windowMs} of a completed recovery does not recover again.
 * But it can also be a NEW loss -- the parent restarted again right after the node re-attached -- and the
 * detectors are edge-triggered: the HC bridge fires once when {@code link:parent} crosses to CRITICAL and never
 * again while it stays CRITICAL, BrokeredAgent once per bridge. A suppressed report is therefore never simply
 * dropped: {@link #scheduleRecheck} looks again once the window has passed and, if the parent link is still
 * down, the loss is raised again (which may itself be suppressed by a newer recovery, and re-arms the recheck).
 * Rechecks coalesce: a burst of suppressed reports costs one recheck, not one per report.
 */
final class LossReportDebounce {

    /** Slack added past the window end so the recheck's re-raise is never itself inside the window. */
    static final long RECHECK_SLACK_MS = 1000L;

    private final long windowMs;
    private final LongSupplier clock;
    private final AtomicBoolean recheckPending = new AtomicBoolean();
    private volatile long lastRecoveryComplete = 0L;

    LossReportDebounce(long windowMs) {
        this(windowMs, System::currentTimeMillis);
    }

    LossReportDebounce(long windowMs, LongSupplier clock) {
        this.windowMs = windowMs;
        this.clock = clock;
    }

    /** A loss recovery has just completed: reports in the next {@code windowMs} are suppressed (and rechecked). */
    void recoveryCompleted() {
        lastRecoveryComplete = clock.getAsLong();
    }

    /** Milliseconds since the last completed recovery, or -1 if none has completed. */
    long sinceRecovery() {
        long last = lastRecoveryComplete;
        return (last > 0) ? clock.getAsLong() - last : -1L;
    }

    /** True when a loss report now falls inside the window of the last completed recovery. */
    boolean suppress() {
        long since = sinceRecovery();
        return since >= 0 && since < windowMs;
    }

    /** Delay from now until just past the end of the current window (at least the slack). */
    long recheckDelayMs() {
        long since = sinceRecovery();
        long remaining = (since >= 0) ? windowMs - since : 0L;
        return Math.max(RECHECK_SLACK_MS, remaining + RECHECK_SLACK_MS);
    }

    /** True while a recheck is scheduled and has not run yet. */
    boolean recheckPending() {
        return recheckPending.get();
    }

    /**
     * Schedule {@code recheck} on {@code timer} for just after the window ends, unless one is already pending
     * (coalesced). The pending flag is cleared before {@code recheck} runs, so a report suppressed again by a
     * newer recovery arms a fresh recheck.
     *
     * @return true if a recheck was scheduled by this call
     */
    boolean scheduleRecheck(Timer timer, Runnable recheck) {
        if (!recheckPending.compareAndSet(false, true)) {
            return false;
        }
        try {
            timer.schedule(new TimerTask() {
                public void run() {
                    recheckPending.set(false);
                    recheck.run();
                }
            }, recheckDelayMs());
            return true;
        } catch (IllegalStateException stopping) {
            // timer cancelled: the controller is stopping, nothing to recheck
            recheckPending.set(false);
            return false;
        }
    }

    /**
     * The loss event a recheck must raise again: the one the node's current mode answers (AGENT -&gt;
     * {@code regionalControllerLost}, REGION_GLOBAL -&gt; {@code globalControllerLost}) when its parent link is
     * still down, else null (the link came back, or the node left the tier the loss was reported in).
     */
    static String lossEventToReraise(ControllerMode mode, boolean parentLinkLive) {
        if (parentLinkLive) {
            return null;
        }
        if (mode == ControllerMode.AGENT) {
            return "regionalControllerLost";
        }
        if (mode == ControllerMode.REGION_GLOBAL) {
            return "globalControllerLost";
        }
        return null;
    }
}
