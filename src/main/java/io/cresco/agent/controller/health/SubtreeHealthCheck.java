package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Rolls up the health of this controller's directly-connected children (reported over the ping by
 * {@link MeshHealth}). This surfaces child <em>degradation</em> — a child that is alive but unhealthy
 * (disk filling, a plugin failed, memory pressure) — in this node's own health summary, and because a
 * node's advertised status folds in its {@code mesh} rollup, the degradation propagates one more hop up
 * the fabric. A leaf agent's failing disk is therefore visible at the region and at the global.
 *
 * <p>It is deliberately <em>not</em> a liveness check: a child that stopped pinging is aged out (its last
 * report goes stale) rather than reported CRITICAL, because child loss is already owned by that child's
 * {@code link:parent} check and by the regional node-status watchdog. This check also never drives a MINA
 * transition — the {@link HealthMinaBridge} only acts on {@code link:parent} — so a CRITICAL child is
 * reported truthfully up the mesh without ever failing this node over.
 */
public class SubtreeHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public SubtreeHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            MeshHealth mh = ce.getMeshHealth();
            if (mh == null || mh.children().isEmpty()) {
                return new Result(Result.Status.OK, "no children");
            }
            long staleMs = ce.getPluginBuilder().getConfig().getLongParam("health_subtree_stale_ms", 30000L);
            long now = System.currentTimeMillis();

            Result.Status worst = Result.Status.OK;
            String worstChild = null;
            int fresh = 0;
            for (MeshHealth.ChildHealth c : mh.children().values()) {
                if (now - c.ts > staleMs) {
                    continue; // gone/stale -> owned by link:parent + node-status watchdog, not here
                }
                fresh++;
                if (c.status.ordinal() > worst.ordinal()) {
                    worst = c.status;
                    worstChild = c.path + (c.detail.isEmpty() ? "" : " (" + c.detail + ")");
                }
            }

            if (fresh == 0) {
                return new Result(Result.Status.OK, "no fresh children");
            }
            if (worst == Result.Status.OK) {
                return new Result(Result.Status.OK, fresh + " child(ren) ok");
            }
            return new Result(worst, fresh + " child(ren), worst=" + worst + " " + worstChild);
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "subtree check error: " + t);
        }
    }
}
