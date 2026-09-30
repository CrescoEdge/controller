package io.cresco.agent.controller.health;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where a stalled parent link is stuck. On the DGX (2026-09-26, two agents on one node) an agent's
 * liveness ping, its watchdog and its net-autotuner all went silent for 40-85 s right after start-up
 * with no timeout logged anywhere: the threads were parked outside every bounded wait, and the logs
 * cannot say where. The first time a parent link turns stale this dumps the stacks of the threads that
 * carry the control plane (and of every thread holding or waiting for a lock), once per episode and at
 * most once per {@code minIntervalMs}, so the next occurrence names its own cause.
 */
final class StallDiagnostics {

    private static final AtomicLong LAST_DUMP_MS = new AtomicLong(0);
    private static final int MAX_FRAMES = 14;

    private StallDiagnostics() {}

    /** A thread worth showing: a control-plane carrier, an ActiveMQ transport/session thread, or any lock holder/waiter. */
    static boolean relevant(ThreadInfo ti) {
        String n = ti.getThreadName();
        if (n.startsWith("AgentActivePingTimer") || n.startsWith("AgentCommHealthTimer") || n.startsWith("net-autotuner")
                || n.startsWith("ActiveMQ") || n.contains("Session Task") || n.startsWith("pool-1-thread")
                || n.startsWith("hc-") || n.startsWith("CrescoHealth")) return true;
        if (ti.getLockOwnerName() != null || ti.getLockedSynchronizers().length > 0) return true;
        for (MonitorInfo mi : ti.getLockedMonitors()) if (mi != null) return true;
        return false;
    }

    /** The dump text, or null when one was written less than minIntervalMs ago. */
    static String dumpIfDue(String reason, long minIntervalMs) {
        long now = System.currentTimeMillis();
        long last = LAST_DUMP_MS.get();
        if (now - last < minIntervalMs || !LAST_DUMP_MS.compareAndSet(last, now)) return null;
        return dump(reason);
    }

    static String dump(String reason) {
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        ThreadInfo[] all = mx.dumpAllThreads(true, true);
        StringBuilder sb = new StringBuilder("parent-link stall diagnostics (").append(reason).append("): ");
        int shown = 0;
        for (ThreadInfo ti : all) {
            if (ti == null || !relevant(ti)) continue;
            shown++;
            sb.append("\n\"").append(ti.getThreadName()).append("\" ").append(ti.getThreadState());
            if (ti.getLockName() != null) sb.append(" on ").append(ti.getLockName());
            if (ti.getLockOwnerName() != null) sb.append(" owned by \"").append(ti.getLockOwnerName()).append('"');
            StackTraceElement[] st = ti.getStackTrace();
            for (int i = 0; i < Math.min(MAX_FRAMES, st.length); i++) {
                sb.append("\n    at ").append(st[i]);
                for (MonitorInfo mi : ti.getLockedMonitors()) {
                    if (mi.getLockedStackDepth() == i) sb.append("\n    - locked ").append(mi);
                }
            }
            if (st.length > MAX_FRAMES) sb.append("\n    ...");
        }
        sb.append("\n(").append(shown).append(" of ").append(all.length).append(" threads)");
        return sb.toString();
    }
}
