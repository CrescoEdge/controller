package io.cresco.agent.core;

import io.cresco.library.utilities.CLogger;

/**
 * Halts the agent JVM when a thread dies of a fatal JVM error (controller#23).
 *
 * <p>An {@link OutOfMemoryError} (or any {@link VirtualMachineError}) that kills the ActiveMQ client's task,
 * session or transport threads leaves the JVM running with no broker connection: it answers no MsgEvent, its
 * dataplane sessions stay registered and never drain, and its health checks keep reporting the last good state.
 * Its peers then wait on it for minutes. A process that is gone is restarted by whatever supervises it; a zombie
 * is not. So the default uncaught-exception handler halts the JVM when the throwable, or any of its causes, is a
 * VirtualMachineError (an OOM is often wrapped, e.g. in an ExceptionInInitializerError). Other throwables keep the
 * previous behaviour.
 *
 * <p>Opt out with {@code -Dcresco_halt_on_fatal_error=false} (or the environment variable
 * {@code CRESCO_HALT_ON_FATAL_ERROR=false}). The JVM flag {@code -XX:+ExitOnOutOfMemoryError} remains the stronger
 * form for OOMs: it also covers an OOM that some code catches and swallows.
 */
public final class FatalErrorGuard {

    /** The exit status of a halt for a fatal error (EX_SOFTWARE). */
    public static final int EXIT_STATUS = 70;

    private static volatile boolean installed;

    private FatalErrorGuard() { }

    /** Whether the throwable or any cause in its chain is a fatal JVM error. */
    static boolean fatal(Throwable t) {
        for (int depth = 0; t != null && depth < 32; depth++, t = t.getCause())
            if (t instanceof VirtualMachineError) return true;
        return false;
    }

    static boolean enabled() {
        String p = System.getProperty("cresco_halt_on_fatal_error");
        if (p == null) p = System.getenv("CRESCO_HALT_ON_FATAL_ERROR");
        return p == null || !"false".equalsIgnoreCase(p.trim());
    }

    /** Install once per JVM; later calls do nothing. */
    public static synchronized void install(CLogger logger) {
        if (installed || !enabled()) return;
        installed = true;
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {
            if (fatal(t)) {
                String what = "FATAL: thread '" + thread.getName() + "' died of " + t
                        + "; halting the agent JVM (exit " + EXIT_STATUS + ") so it is restarted, not left a zombie";
                try {
                    System.err.println(what);
                    t.printStackTrace();
                    if (logger != null) logger.error(what);
                } catch (Throwable ignore) {
                    // out of memory may fail the logging too: halt regardless
                }
                Runtime.getRuntime().halt(EXIT_STATUS);
            }
            if (previous != null) previous.uncaughtException(thread, t);
            else {
                System.err.print("Exception in thread \"" + thread.getName() + "\" ");
                t.printStackTrace();
            }
        });
        if (logger != null) logger.info("FatalErrorGuard installed: a thread dying of a VirtualMachineError halts the JVM (cresco_halt_on_fatal_error)");
    }
}
