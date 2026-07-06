package io.cresco.agent.controller.netmetrics;

import io.cresco.agent.controller.core.ControllerEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The set of coordinator ("global") nodes in the fabric, and the elected leader among them — the piece that
 * makes MORE THAN ONE global possible and removes the single static global.
 *
 * <p>Coordinators are discovered with ZERO new plumbing: every controller already advertises its role
 * (global/region/agent) in its link-state, pushed over the data plane into every node's {@link RouteView}
 * ({@link RouteAdvertiser}). So any node can enumerate all live coordinators simply by filtering the
 * RouteView for {@code role == "global"} (plus itself, if it is a coordinator). No node holds a scalar
 * "the global" pointer any more.
 *
 * <p><b>Election.</b> Given the same RouteView, every node computes the same leader deterministically. Two
 * placement policies:
 * <ul>
 *   <li>{@code identity} — lowest coordinator path (stable, dependency-free); and</li>
 *   <li>{@code centroid} — the coordinator with minimum eccentricity over the learned latency graph
 *       (k-center-style placement, Phase E) so the leader sits where control-plane latency is lowest.</li>
 * </ul>
 * Because the choice is a pure function of the shared view it needs no consensus round to <i>select</i>;
 * consensus (Phase D) is layered on top only to <i>commit</i> strong-duty state under the leader's epoch.
 *
 * <p><b>Duty sharding.</b> {@link #coordinatorForDuty(String)} maps a duty+namespace key onto a coordinator
 * by consistent hashing across the live set, so strong duties (liveness verdict, global optimization) shard
 * across coordinators instead of piling on one, while still resolving to the leader for the singleton duties.
 */
public final class CoordinatorRegistry {

    private final ControllerEngine ce;

    public CoordinatorRegistry(ControllerEngine ce) { this.ce = ce; }

    /** True if THIS node is a coordinator (role global). */
    public boolean selfIsCoordinator() {
        try { return ce.cstate.isGlobalController(); } catch (Exception e) { return false; }
    }

    private String selfPath() {
        return ce.cstate.getRegion() + "_" + ce.cstate.getAgent();
    }

    /** All live coordinator node paths (region_agent), sorted, de-duplicated, including self if coordinator. */
    public List<String> coordinators() {
        List<String> out = new ArrayList<>();
        try {
            RouteView rv = ce.getRouteView();
            if (rv != null) {
                for (RouteView.NodeState ns : rv.fresh()) {
                    if ("global".equals(ns.role) && ns.node != null && !out.contains(ns.node)) out.add(ns.node);
                }
            }
            if (selfIsCoordinator()) {
                String self = selfPath();
                if (!out.contains(self)) out.add(self);
            }
        } catch (Exception ignore) { }
        Collections.sort(out);
        return out;
    }

    public int size() { return coordinators().size(); }

    /**
     * The elected leader among all live coordinators, or null if there are none. Policy is chosen by config
     * {@code coordinator_election_policy} = identity | centroid (default identity). Both are deterministic
     * functions of the shared RouteView, so all nodes agree without a vote.
     */
    public String leader() {
        List<String> coords = coordinators();
        if (coords.isEmpty()) return null;
        String policy = "identity";
        try { policy = ce.getPluginBuilder().getConfig().getStringParam("coordinator_election_policy", "identity"); }
        catch (Exception ignore) { }
        if ("centroid".equalsIgnoreCase(policy)) {
            String c = centroidLeader(coords);
            if (c != null) return c;
        }
        return coords.get(0); // identity policy: lowest path (also the tie-break / fallback for centroid)
    }

    /** True if THIS node is the elected leader. */
    public boolean isLeader() {
        String l = leader();
        return l != null && l.equals(selfPath());
    }

    /**
     * Resolve which coordinator owns {@code dutyKey} (e.g. "liveness:region3", "optimize:tenantA"). Consistent
     * hash over the sorted live set so a duty stays on the same coordinator as the set is stable, and shards
     * evenly across coordinators. Falls back to the leader when there is a single coordinator.
     */
    public String coordinatorForDuty(String dutyKey) {
        List<String> coords = coordinators();
        if (coords.isEmpty()) return null;
        if (coords.size() == 1 || dutyKey == null) return coords.get(0);
        int idx = (dutyKey.hashCode() & 0x7fffffff) % coords.size();
        return coords.get(idx);
    }

    /**
     * k-center-style leader: the coordinator whose worst-case (maximum) measured latency to any other live
     * node in the RouteView is smallest — i.e. the most central coordinator. Ties break by lowest path.
     * Returns null if the graph has no usable latencies (fall back to identity).
     */
    private String centroidLeader(List<String> coords) {
        try {
            RouteView rv = ce.getRouteView();
            if (rv == null) return null;
            String best = null; double bestEcc = Double.MAX_VALUE;
            for (String c : coords) {
                double ecc = 0; boolean any = false;
                for (RouteView.NodeState ns : rv.fresh()) {
                    if (ns.node == null || ns.node.equals(c)) continue;
                    RouteComputer.Path p = RouteComputer.compute(rv, c, ns.node);
                    if (p != null) { any = true; ecc = Math.max(ecc, p.cost); }
                }
                if (any && (ecc < bestEcc || (ecc == bestEcc && (best == null || c.compareTo(best) < 0)))) {
                    bestEcc = ecc; best = c;
                }
            }
            return best;
        } catch (Exception e) { return null; }
    }
}
