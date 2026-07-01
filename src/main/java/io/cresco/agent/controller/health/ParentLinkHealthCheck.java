package io.cresco.agent.controller.health;

import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.library.agent.ControllerMode;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Link check: liveness of this node's PARENT controller (agent-&gt;region, region-&gt;global).
 *
 * <p>The verdict is computed <em>locally</em> from the last-pong timestamp maintained by the
 * (now transport-only) health watcher; the only communicated input is the ping/pong itself. A stale
 * link returns {@code TEMPORARILY_UNAVAILABLE} so {@link CrescoHealthExecutor}'s grace window absorbs
 * transient blips and only a sustained loss escalates to {@code CRITICAL} — which the
 * {@link HealthMinaBridge} turns into the corresponding MINA event. Nodes with no parent (GLOBAL,
 * STANDALONE, transient/plain-REGION states) report OK.
 */
public class ParentLinkHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public ParentLinkHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            if (ce.cstate == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "state not ready");
            }
            ControllerMode mode = ce.cstate.getControllerState();

            // Agent -> regional parent
            if (mode == ControllerMode.AGENT) {
                AgentHealthWatcher w = ce.getAgentHealthWatcher();
                if (w == null) {
                    return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "agent watcher not ready");
                }
                return evaluate("regional", w.getLastParentPongTs(), w.getPingIntervalMs());
            }

            // Region connected to a (remote) global -> global parent.
            //
            // A region reaches its global over a network-of-brokers BRIDGE, and BrokeredAgent already
            // detects bridge loss and fires globalControllerLost fast and reliably (a dropped TCP
            // bridge is a stronger, lower-latency signal than an application ping). The RPC-ping path
            // is therefore redundant for regions, and on a same-host federation it is unreliable
            // (pongs arrive jittery ~1/15s), which would make link:parent flap. So by default the
            // region's global link is reported healthy here and its loss is driven by BrokeredAgent.
            // Set health_link_region_ping=true to additionally drive it from the ping (useful only on
            // multi-host fabrics where the ping is reliable).
            if (mode == ControllerMode.REGION_GLOBAL) {
                boolean usePing = ce.getPluginBuilder().getConfig().getBooleanParam("health_link_region_ping", false);
                if (!usePing) {
                    return new Result(Result.Status.OK, "global link via broker bridge (BrokeredAgent-detected)");
                }
                RegionHealthWatcher w = ce.getRegionHealthWatcher();
                if (w == null) {
                    return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "region watcher not ready");
                }
                return evaluate("global", w.getLastGlobalPongTs(), w.getPingIntervalMs());
            }

            // GLOBAL / STANDALONE / transient states: no parent link to monitor.
            return new Result(Result.Status.OK, "no parent link (" + mode + ")");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "link:parent check error: " + t);
        }
    }

    private Result evaluate(String label, long lastPongTs, long pingIntervalMs) {
        long staleMs = ce.getPluginBuilder().getConfig().getLongParam("health_link_stale_ms", 0L);
        if (staleMs <= 0) {
            staleMs = Math.max(pingIntervalMs * 2, 10000L);
        }
        long age = System.currentTimeMillis() - lastPongTs;
        if (age < staleMs) {
            return new Result(Result.Status.OK, label + " link ok (pong age " + age + "ms)");
        }
        return new Result(Result.Status.TEMPORARILY_UNAVAILABLE,
                label + " link stale (pong age " + age + "ms >= " + staleMs + "ms)");
    }
}
