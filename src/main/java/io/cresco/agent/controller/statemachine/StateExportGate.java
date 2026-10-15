package io.cresco.agent.controller.statemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The diff-gate of a node's periodic state export to its parent (ControllerSMHandler.stateUpdateTask).
 *
 * <p>Each tick a node exports its region/agent/plugin configs, but sends a section only when it changed since
 * the last send; every {@code fullEveryTicks}-th tick sends everything (best-effort topics can lose an export).
 * That gate is what kept a node's plugins hidden after its parent marked it LOST: LOST sets the node's rows at
 * the parent to status 50, only an export restores them to 10, and the node -- whose configs had not changed --
 * sent none until the next periodic full tick (up to 4 x 15 s). {@link #requestFull()} lets a node that has just
 * (re-)registered force a full export now.
 *
 * <p>Not thread-safe except {@link #requestFull()}: the other methods run on the export timer's single thread.
 */
final class StateExportGate {

    private final int fullEveryTicks;
    private final Map<String, String> lastSent = new HashMap<>();
    private final AtomicLong fullRequests = new AtomicLong();
    private long fullServed;
    private long tick;
    private long tickRequest;

    StateExportGate(int fullEveryTicks) {
        this.fullEveryTicks = Math.max(1, fullEveryTicks);
    }

    /** Ask for the next export to send every section (any thread; repeated requests coalesce). */
    void requestFull() {
        fullRequests.incrementAndGet();
    }

    /** True while a requested full export has not been sent yet. */
    boolean fullPending() {
        return fullRequests.get() > fullServed;
    }

    /**
     * Start an export tick.
     *
     * @return true if this tick serves a {@linkplain #requestFull() requested} full export (as opposed to the
     *         periodic one); every section is sent either way when this tick is a full one
     */
    boolean beginTick() {
        tick++;
        tickRequest = fullRequests.get();
        boolean requested = tickRequest > fullServed;
        if (requested || tick % fullEveryTicks == 0) {
            lastSent.clear();
        }
        return requested;
    }

    /**
     * The value to send for a section this tick, or null when it is unchanged since the last send (or absent).
     * A non-null result is recorded as sent.
     */
    String changed(String section, String value) {
        if (value == null || value.equals(lastSent.get(section))) {
            return null;
        }
        lastSent.put(section, value);
        return value;
    }

    /**
     * The export of this tick went out. A full export that was requested before the tick began is now
     * served; a request made while the tick ran stays pending for the next one. When the send failed, call
     * {@link #sendFailed()} instead.
     */
    void sent() {
        fullServed = Math.max(fullServed, tickRequest);
    }

    /** The export of this tick did not go out: forget what it recorded so the next tick re-sends it. */
    void sendFailed() {
        lastSent.clear();
    }
}
