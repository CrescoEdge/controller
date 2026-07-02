package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;
import org.apache.felix.hc.api.Result;

/**
 * The wire glue for {@link MeshHealth}: reads/writes the health params carried on the existing liveness
 * ping/pong. Kept as static helpers so the watchers (senders) and the executors (responders) share one
 * definition of the param contract and stay tiny.
 *
 * <p>Contract — a node advertises {@code aggregate("local","mesh")} (its own subsystem health folded
 * with its subtree rollup, deliberately excluding {@code link} so a node's parent-link state is not
 * conflated with its health):
 * <ul>
 *   <li>PING (child-&gt;parent): {@code child_health}, {@code child_path}, {@code child_detail}</li>
 *   <li>PONG (parent-&gt;child): {@code parent_health}, {@code parent_detail}</li>
 * </ul>
 */
public final class MeshHealthPing {

    public static final String P_CHILD_HEALTH = "child_health";
    public static final String P_CHILD_PATH = "child_path";
    public static final String P_CHILD_DETAIL = "child_detail";
    public static final String P_PARENT_HEALTH = "parent_health";
    public static final String P_PARENT_DETAIL = "parent_detail";

    private MeshHealthPing() {}

    /** This node's advertised rolled-up status: worst of its {@code local} + {@code mesh} checks. */
    public static Result.Status selfStatus(ControllerEngine ce) {
        if (ce == null || ce.getHealthExecutor() == null) {
            return Result.Status.OK;
        }
        return ce.getHealthExecutor().aggregate(HealthTags.LOCAL, MeshHealthChecks.MESH);
    }

    /** Child side: stamp our rolled-up health onto an outbound PING. */
    public static void advertiseChild(ControllerEngine ce, MsgEvent ping) {
        try {
            Result.Status s = selfStatus(ce);
            ping.setParam(P_CHILD_HEALTH, s.name());
            String path = (ce.cstate != null) ? ce.cstate.getAgentPath() : null;
            if (path != null) {
                ping.setParam(P_CHILD_PATH, path);
            }
        } catch (Throwable ignore) {
            // health is best-effort observability; never let it break the liveness ping
        }
    }

    /** Parent side: record the child's advertised health from an inbound PING. */
    public static void recordChild(ControllerEngine ce, MsgEvent ping) {
        try {
            String h = ping.getParam(P_CHILD_HEALTH);
            if (h == null || ce.getMeshHealth() == null) {
                return;
            }
            String path = ping.getParam(P_CHILD_PATH);
            if (path == null) {
                path = ping.getSrcRegion() + "_" + ping.getSrcAgent();
            }
            Result.Status st = MeshHealth.parseStatus(h);
            boolean changed = ce.getMeshHealth().recordChild(path, st,
                    ping.getParam(P_CHILD_DETAIL), System.currentTimeMillis());
            if (changed) {
                ce.getPluginBuilder().getLogger(MeshHealthPing.class.getName(), CLogger.Level.Info)
                        .info("mesh: child {} health {}", path, st);
            }
        } catch (Throwable ignore) {
            // ignore
        }
    }

    /** Parent side: stamp our rolled-up health onto the outbound PONG. */
    public static void stampParent(ControllerEngine ce, MsgEvent pong) {
        try {
            pong.setParam(P_PARENT_HEALTH, selfStatus(ce).name());
        } catch (Throwable ignore) {
            // ignore
        }
    }

    /** Child side: record the parent's advertised health from an inbound PONG. */
    public static void recordParent(ControllerEngine ce, MsgEvent pong) {
        try {
            String h = pong.getParam(P_PARENT_HEALTH);
            if (h == null || ce.getMeshHealth() == null) {
                return;
            }
            Result.Status st = MeshHealth.parseStatus(h);
            boolean changed = ce.getMeshHealth().recordParent(st,
                    pong.getParam(P_PARENT_DETAIL), System.currentTimeMillis());
            if (changed) {
                ce.getPluginBuilder().getLogger(MeshHealthPing.class.getName(), CLogger.Level.Info)
                        .info("mesh: parent health {}", st);
            }
        } catch (Throwable ignore) {
            // ignore
        }
    }
}
