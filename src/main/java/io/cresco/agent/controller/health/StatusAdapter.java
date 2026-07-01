package io.cresco.agent.controller.health;

import io.cresco.agent.db.NodeStatusType;
import org.apache.felix.hc.api.Result;

/**
 * Maps Cresco's two legacy status ladders onto the single Felix HC {@link Result.Status}
 * vocabulary. Both ladders are hand-rolled OK->transient->dead progressions; this is the one
 * migration point so the whole framework speaks {@code Result.Status}.
 *
 * <p>Ordering (worst-wins for aggregation) follows the enum ordinal:
 * OK &lt; WARN &lt; TEMPORARILY_UNAVAILABLE &lt; CRITICAL &lt; HEALTH_CHECK_ERROR.
 */
public final class StatusAdapter {

    private StatusAdapter() {}

    /** {@link NodeStatusType} (child-node liveness ladder) -> {@link Result.Status}. */
    public static Result.Status fromNodeStatus(NodeStatusType t) {
        if (t == null) {
            return Result.Status.HEALTH_CHECK_ERROR;
        }
        switch (t) {
            case ACTIVE:
                return Result.Status.OK;
            case STARTING:
            case PENDINGSTALE:
            case PENDINGLOST:
                return Result.Status.TEMPORARILY_UNAVAILABLE;
            case STALE:
                return Result.Status.WARN;
            case LOST:
            case FAILED:
                return Result.Status.CRITICAL;
            case STOPPPING:
            case DISABLED:
                // intentional stop/disable is not a health failure
                return Result.Status.OK;
            case ERROR:
            default:
                return Result.Status.HEALTH_CHECK_ERROR;
        }
    }

    /** Plugin {@code status_code} (see PluginNode) -> {@link Result.Status}. */
    public static Result.Status fromPluginStatusCode(int code) {
        switch (code) {
            case 10:            // started and working
                return Result.Status.OK;
            case 3:             // agentcontroller init
            case 40:            // WATCHDOG check STALE
                return Result.Status.TEMPORARILY_UNAVAILABLE;
            case 90:            // exception on timeout shutdown
            case 91:            // exception on timeout verification
            case 92:            // timeout on disable verification
                return Result.Status.WARN;
            case 7:             // plugin instance could not be started
            case 9:             // bundle could not be installed/started
            case 50:            // WATCHDOG check LOST
            case 80:            // failed to start
                return Result.Status.CRITICAL;
            case 8:             // agentcontroller disabled (intentional)
                return Result.Status.OK;
            case 41:            // missing status parameter
                return Result.Status.HEALTH_CHECK_ERROR;
            default:
                return Result.Status.WARN;
        }
    }
}
