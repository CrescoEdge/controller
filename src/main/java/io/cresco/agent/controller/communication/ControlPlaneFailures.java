package io.cresco.agent.controller.communication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Send-side failures of this agent's control plane, counted (gfs#25): until now they were only logged, so the
 * stranded-sender signature (CrescoEdge/controller#24/#25/#26: a sender or inbox that stays dead after a hub restart
 * while the agent still looks up in the mesh) was invisible in the metric inventory. PerfControllerMonitor exports
 * each as a Micrometer counter (group "controlplane"); a long-horizon chaos stage asserts that none of them keeps
 * growing on a fabric that reports itself healthy. Process-wide and monotonic since the JVM started.
 */
public final class ControlPlaneFailures {

    private ControlPlaneFailures() { }

    /** controlplane.send.failed: control-plane send attempts that threw (each of the sender's two attempts). */
    public static final AtomicLong SEND_FAILED = new AtomicLong();
    /** controlplane.send.refused: control-plane sends refused because the sender was closed. */
    public static final AtomicLong SEND_REFUSED = new AtomicLong();
    /** controlplane.send.lost: control-plane messages not delivered after every attempt (the caller saw false). */
    public static final AtomicLong SEND_LOST = new AtomicLong();
    /** controlplane.producer.send.lost: messages the pooled producer could not deliver after all of its retries. */
    public static final AtomicLong PRODUCER_LOST = new AtomicLong();
    /** controlplane.inbox.failures: failures of the agent's dedicated inbox connection (each starts a rebuild). */
    public static final AtomicLong INBOX_FAILURES = new AtomicLong();
    /** controlplane.inbox.rebuild.failures: inbox rebuild attempts that failed (and were retried). */
    public static final AtomicLong INBOX_REBUILD_FAILURES = new AtomicLong();
    /** controlplane.state.export.failed: state exports (the WATCHDOG to the parent) that could not be sent. */
    public static final AtomicLong STATE_EXPORT_FAILED = new AtomicLong();
}
