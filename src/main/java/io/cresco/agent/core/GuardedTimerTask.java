package io.cresco.agent.core;

import io.cresco.library.utilities.CLogger;

import java.util.TimerTask;

/**
 * A {@link java.util.Timer} task that cannot end its timer. A Throwable that leaves {@link TimerTask#run()} cancels the
 * whole Timer for good, silently: every later run of every task on it is gone (an agent's watchdog timer ended by one
 * failed read stops the agent's watchdogs, and its region ages it to LOST while its pings are still answered). The
 * work goes in {@link #tick()}; whatever it throws is logged in one line and the task keeps its schedule.
 *
 * <p>A fatal JVM error (an OutOfMemoryError, also a wrapped one) is passed on: it must end the thread, so that
 * {@link FatalErrorGuard} halts the JVM instead of leaving a zombie.
 */
public abstract class GuardedTimerTask extends TimerTask {

    private final CLogger logger;

    protected GuardedTimerTask(CLogger logger) {
        this.logger = logger;
    }

    /** One run of the task. */
    protected abstract void tick();

    @Override
    public final void run() {
        String name = getClass().getSimpleName();
        guarded(name.isEmpty() ? getClass().getName() : name, logger, this::tick);
    }

    /**
     * Run {@code body} on a timer thread: what it throws is logged in one line and not passed on (a fatal JVM error is).
     * For a one-shot task that shares its Timer with others.
     *
     * @return whether {@code body} returned normally
     */
    public static boolean guarded(String name, CLogger logger, Runnable body) {
        try {
            body.run();
            return true;
        } catch (Throwable t) {
            if (FatalErrorGuard.fatal(t)) throw t;
            try {
                StackTraceElement[] at = t.getStackTrace();
                String line = "timer task " + name + " failed (it keeps its schedule): " + t
                        + (at != null && at.length > 0 ? " at " + at[0] : "");
                if (logger != null) logger.error(line);
                else System.err.println(line);
            } catch (Throwable ignore) {
                // the log line is not worth the timer
            }
            return false;
        }
    }
}
