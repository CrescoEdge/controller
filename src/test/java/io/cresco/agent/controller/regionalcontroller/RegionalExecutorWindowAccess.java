package io.cresco.agent.controller.regionalcontroller;

/** Test access to RegionalExecutor's package-private upgrade-window rule. */
public final class RegionalExecutorWindowAccess {
    private RegionalExecutorWindowAccess() { }
    public static boolean legacyMembership(io.cresco.library.messaging.MsgEvent m) { return RegionalExecutor.legacyMembership(m); }

    /** The regional tier's admission (the global tier's when {@code global}): tables, registry binding, the 403 answer. */
    public static boolean admitted(RegionalExecutor executor, io.cresco.library.messaging.MsgEvent m, boolean global) {
        return executor.admitted(m, global);
    }
}
