package io.cresco.agent.controller.health;

import org.apache.felix.hc.api.Result;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of health that flows <em>through</em> the mesh on the existing liveness ping/pong — no new
 * messages. Direction is bidirectional over one round trip:
 * <ul>
 *   <li><b>up</b> — a child stamps its rolled-up HC status (worst of its {@code local} + {@code mesh}
 *       checks) into the PING; the parent records it here keyed by the child's path. A region therefore
 *       holds the live health of every agent beneath it, and — because a region's own advertised status
 *       already folds in its {@code mesh} rollup — the global transitively holds the health of the whole
 *       fabric.</li>
 *   <li><b>down</b> — the parent stamps its own rolled-up status into the PONG; the child records it here
 *       so a node can see that its parent is degraded (surfaced WARN-only; it never drives failover —
 *       that stays with {@code link:parent}).</li>
 * </ul>
 *
 * <p>This is pure observability plumbing: it turns per-node health into a mesh-wide health view. Loss of
 * a child (it stopped pinging) is <em>not</em> represented as CRITICAL here — that is the child's own
 * {@code link:parent} concern and the regional node-status watchdog's DB concern; stale entries are
 * simply aged out by {@link SubtreeHealthCheck}.
 */
public class MeshHealth {

    /** A child's last-reported rolled-up health. */
    public static final class ChildHealth {
        public final String path;
        public final Result.Status status;
        public final String detail;
        public final long ts;

        ChildHealth(String path, Result.Status status, String detail, long ts) {
            this.path = path;
            this.status = status;
            this.detail = (detail != null) ? detail : "";
            this.ts = ts;
        }
    }

    private final Map<String, ChildHealth> children = new ConcurrentHashMap<>();

    private volatile Result.Status parentStatus = Result.Status.OK;
    private volatile String parentDetail = "";
    private volatile long parentTs = 0L;
    private volatile boolean parentRecorded = false;

    // ---- child health (reported up, recorded by the parent) ----

    /** Records a child's reported health; returns true if this is a new child or its status changed. */
    public boolean recordChild(String path, Result.Status status, String detail, long ts) {
        if (path == null || status == null) {
            return false;
        }
        ChildHealth prev = children.put(path, new ChildHealth(path, status, detail, ts));
        return prev == null || prev.status != status;
    }

    public Map<String, ChildHealth> children() {
        return children;
    }

    public void forgetChild(String path) {
        if (path != null) {
            children.remove(path);
        }
    }

    // ---- parent health (reported down, recorded by the child) ----

    /**
     * Records the parent's reported health; returns true if this is the first record since (re)start
     * or a loss, or the status changed — so a freshly-(re)established parent link is always observable.
     */
    public boolean recordParent(Result.Status status, String detail, long ts) {
        if (status == null) {
            return false;
        }
        boolean changed = (!parentRecorded) || (this.parentStatus != status);
        this.parentStatus = status;
        this.parentDetail = (detail != null) ? detail : "";
        this.parentTs = ts;
        this.parentRecorded = true;
        return changed;
    }

    /** Clears recorded parent health on a parent-loss event so the next record re-logs the recovery. */
    public void resetParent() {
        this.parentRecorded = false;
        this.parentStatus = Result.Status.OK;
        this.parentDetail = "";
        this.parentTs = 0L;
    }

    public Result.Status parentStatus() { return parentStatus; }
    public String parentDetail() { return parentDetail; }
    public long parentTs() { return parentTs; }

    // ---- helpers ----

    /** Lenient parse of a {@link Result.Status} name; unknown/null -&gt; TEMPORARILY_UNAVAILABLE. */
    public static Result.Status parseStatus(String s) {
        if (s == null) {
            return Result.Status.TEMPORARILY_UNAVAILABLE;
        }
        try {
            return Result.Status.valueOf(s.trim());
        } catch (Exception e) {
            return Result.Status.TEMPORARILY_UNAVAILABLE;
        }
    }
}
