package io.cresco.agent.controller.regionalcontroller;

/** Test access to RegionalExecutor's package-private upgrade-window rule. */
public final class RegionalExecutorWindowAccess {
    private RegionalExecutorWindowAccess() { }
    public static boolean legacyMembership(io.cresco.library.messaging.MsgEvent m) { return RegionalExecutor.legacyMembership(m); }
}
