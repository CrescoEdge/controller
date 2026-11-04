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
 * {@link HealthMinaBridge} turns into the corresponding MINA event. A region judges its global the same way (by default;
 * see {@link #regionStaleThresholdMs}). Nodes with no parent (GLOBAL, STANDALONE, transient/plain-REGION states) report OK.
 */
public class ParentLinkHealthCheck implements HealthCheck {

    private final ControllerEngine ce;
    /** True from the first stale verdict until the link is OK again (one stall dump per episode). */
    private volatile boolean staleEpisode = false;

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

            // Region connected to a (remote) global -> global parent, judged by the age of the global's last pong (audit:
            // a region with no bridge of its own -- its global reached over another region's route -- never saw a bridge
            // drop, so judging by the bridge alone meant it NEVER declared its global lost). The ping runs whenever the
            // region is attached to a global (RegionHealthWatcher), retries within each tick, and the region threshold
            // tolerates a jittery same-host federation; CrescoHealthExecutor's grace window absorbs a single blip.
            // health_link_region_ping=false is an explicit opt-out (bridge loss only), not recommended.
            if (mode == ControllerMode.REGION_GLOBAL) {
                if (!regionPingJudged(ce.getPluginBuilder())) {
                    return new Result(Result.Status.OK, "global link via broker bridge only (health_link_region_ping=false)");
                }
                long[] pong = globalPong();
                if (pong == null) {
                    return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "region watcher not ready");
                }
                return evaluateAgainst("global", pong[0], regionStaleThresholdMs(ce.getPluginBuilder(), pong[1]));
            }

            // GLOBAL / STANDALONE / transient states: no parent link to monitor.
            return new Result(Result.Status.OK, "no parent link (" + mode + ")");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "link:parent check error: " + t);
        }
    }

    /**
     * Pong age at which the parent link counts as stale: {@code health_link_stale_ms}, or by default twice the
     * ping interval (at least 10 s). Shared with the state machine's post-debounce loss recheck so both judge
     * the link the same way.
     */
    public static long staleThresholdMs(io.cresco.library.plugin.PluginBuilder plugin, long pingIntervalMs) {
        long staleMs = plugin.getConfig().getLongParam("health_link_stale_ms", 0L);
        if (staleMs <= 0) {
            staleMs = Math.max(pingIntervalMs * 2, 10000L);
        }
        return staleMs;
    }

    /** Is a region's global link judged by pong age ({@code health_link_region_ping}, default true)? */
    public static boolean regionPingJudged(io.cresco.library.plugin.PluginBuilder plugin) {
        return plugin.getConfig().getBooleanParam("health_link_region_ping", true);
    }

    /**
     * Pong age at which a region's GLOBAL link counts as stale: {@code health_link_region_stale_ms}, or by default three
     * ping intervals and at least 30 s (a same-host federation answers jittery). Bounded: a region whose global stopped
     * answering goes non-OK within this time, and CRITICAL after the health executor's grace window.
     */
    public static long regionStaleThresholdMs(io.cresco.library.plugin.PluginBuilder plugin, long pingIntervalMs) {
        long staleMs = plugin.getConfig().getLongParam("health_link_region_stale_ms", 0L);
        if (staleMs <= 0) {
            staleMs = Math.max(pingIntervalMs * 3, 30000L);
        }
        return staleMs;
    }

    /** {lastGlobalPongTs, pingIntervalMs} of this region, or null when its watcher is not up. */
    protected long[] globalPong() {
        RegionHealthWatcher w = ce.getRegionHealthWatcher();
        return w == null ? null : new long[]{w.getLastGlobalPongTs(), w.getPingIntervalMs()};
    }

    private Result evaluate(String label, long lastPongTs, long pingIntervalMs) {
        return evaluateAgainst(label, lastPongTs, staleThresholdMs(ce.getPluginBuilder(), pingIntervalMs));
    }

    private Result evaluateAgainst(String label, long lastPongTs, long staleMs) {
        long age = System.currentTimeMillis() - lastPongTs;
        if (age < staleMs) {
            staleEpisode = false;
            return new Result(Result.Status.OK, label + " link ok (pong age " + age + "ms)");
        }
        if (!staleEpisode) {
            staleEpisode = true;
            // once per episode (and per 5 min): where the ping/watchdog threads are parked
            if (ce.getPluginBuilder().getConfig().getBooleanParam("health_link_stale_threaddump", true)) {
                String d = StallDiagnostics.dumpIfDue(label + " pong age " + age + "ms",
                        ce.getPluginBuilder().getConfig().getLongParam("health_link_stale_threaddump_interval_ms", 300_000L));
                if (d != null) {
                    ce.getPluginBuilder().getLogger(ParentLinkHealthCheck.class.getName(),
                            io.cresco.library.utilities.CLogger.Level.Info).warn(d);
                }
            }
        }
        return new Result(Result.Status.TEMPORARILY_UNAVAILABLE,
                label + " link stale (pong age " + age + "ms >= " + staleMs + "ms)");
    }
}
