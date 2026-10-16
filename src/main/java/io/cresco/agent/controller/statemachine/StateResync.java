package io.cresco.agent.controller.statemachine;

import io.cresco.agent.db.DBEngine;
import io.cresco.library.messaging.MsgEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * "Send me your full state": the parent's answer to a child it holds as not ACTIVE.
 *
 * <p>When a parent marks a child LOST but the child never notices (it keeps its session, e.g. a frozen process
 * that resumes), the child never re-registers, so nothing asks it for a full export: its rows stay LOST at the
 * parent and its plugins hidden until the diff-gated export's periodic full tick (~60 s). The parent therefore
 * checks, on every ping it answers, the status it holds for the pinging child (one primary-key lookup) and, when
 * that is not ACTIVE (LOST, STALE, or no row at all), stamps the pong with {@link #PARAM}={@link #FULL}. The
 * child then requests one full state export ({@link ControllerSMHandler#requestFullStateUpdate}), which restores
 * its rows at the parent; the next pong is clean.
 *
 * <p>Bounded and idempotent: the parent only marks a reply it sends anyway; the child acts at most once per
 * {@code minIntervalMs} however many marked pongs arrive, and the full-export request itself coalesces.
 */
public final class StateResync {

    public static final String PARAM = "state_resync";
    public static final String FULL = "full";
    public static final String STATUS_PARAM = "state_resync_status";
    static final int ACTIVE = 10;

    private final long minIntervalMs;
    private final AtomicLong lastRequestMs = new AtomicLong();

    public StateResync(long minIntervalMs) {
        this.minIntervalMs = Math.max(0L, minIntervalMs);
    }

    /**
     * Parent side: mark the pong to a child whose row here has status {@code childStatus}.
     *
     * @return true when the pong now asks the child for its full state
     */
    public static boolean stampPong(MsgEvent pong, int childStatus) {
        if (childStatus == ACTIVE || childStatus == DBEngine.STATUS_UNKNOWN) {
            pong.removeParam(PARAM);
            return false;
        }
        pong.setParam(PARAM, FULL);
        pong.setParam(STATUS_PARAM, String.valueOf(childStatus));
        return true;
    }

    /** Child side: does this pong ask for a full state export that this node should send now? */
    public boolean shouldResync(MsgEvent pong, long nowMs) {
        if (pong == null || !FULL.equals(pong.getParam(PARAM))) {
            return false;
        }
        long prev = lastRequestMs.get();
        if (prev != 0L && nowMs - prev < minIntervalMs) {
            return false;
        }
        return lastRequestMs.compareAndSet(prev, nowMs);
    }
}
