package io.cresco.agent.controller.netmetrics;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes this controller's live view of the DYNAMIC network for the dashboard: the learned mesh
 * graph (RouteView, populated by pushed link-state) plus this node's own path choices. Called on the
 * global (which receives every node's LSA) to render the whole topology. Every edge carries rtt/cost/
 * conns, so link changes, inferred links appearing, connector scaling, and path flips are all visible.
 */
public final class NetworkStateJson {
    private NetworkStateJson() {}

    /**
     * Builds the dashboard network-state document for {@code ce}. The result is a JSON object with:
     * <ul>
     *   <li>{@code observer} — the {@code region_agent} identity of the node that produced this snapshot;</li>
     *   <li>{@code ts} — snapshot wall-clock millis;</li>
     *   <li>{@code nodes[]} — every node currently fresh in the {@link RouteView}, each with its region,
     *       role, dialable {@code addrs}, and {@code ageMs} since its last advertisement (staleness);</li>
     *   <li>{@code edges[]} — one entry per advertised neighbour edge, carrying {@code rtt}, {@code cost}
     *       and the AutoTuner {@code conns} count so link scaling and inferred links surface visually;</li>
     *   <li>{@code routes[]} — the observer's own per-peer path choices (direct vs. via-global) with the
     *       measured RTT of each option, so live path flips are visible.</li>
     * </ul>
     * Never throws: any failure is captured into an {@code error} field on the returned object so the
     * dashboard poll degrades gracefully rather than 500-ing.
     *
     * @param ce the controller engine whose {@link RouteView} and {@link PathTable} are serialized
     * @return a JSON string; always non-null
     */
    public static String build(ControllerEngine ce) {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            out.put("observer", ce.cstate.getRegion() + "_" + ce.cstate.getAgent());
            out.put("ts", System.currentTimeMillis());
            List<Object> nodes = new ArrayList<>();
            List<Object> edges = new ArrayList<>();
            RouteView rv = ce.getRouteView();
            if (rv != null) {
                for (RouteView.NodeState ns : rv.fresh()) {
                    Map<String, Object> n = new LinkedHashMap<>();
                    n.put("node", ns.node);
                    n.put("region", ns.region);
                    n.put("role", ns.role);
                    n.put("addrs", ns.addrs);
                    n.put("ageMs", System.currentTimeMillis() - ns.updatedTs);
                    nodes.add(n);
                    if (ns.edges != null) for (RouteView.Edge e : ns.edges) {
                        Map<String, Object> ed = new LinkedHashMap<>();
                        ed.put("from", ns.node);
                        ed.put("to", e.toPath);
                        ed.put("rtt", round1(e.srtt));
                        ed.put("cost", round1(e.cost));
                        ed.put("conns", e.conns);
                        edges.add(ed);
                    }
                }
            }
            out.put("nodes", nodes);
            out.put("edges", edges);
            // the observer's own chosen paths (peer -> direct|via-G with rtts) so its decisions show live
            List<Object> routes = new ArrayList<>();
            PathTable pt = ce.getPathTable();
            if (pt != null && rv != null) {
                for (RouteView.NodeState ns : rv.fresh()) {
                    PathTable.Choice c = pt.get(ns.node);
                    if (c == null) continue;
                    Map<String, Object> r = new LinkedHashMap<>();
                    r.put("from", out.get("observer"));
                    r.put("to", ns.node);
                    r.put("choice", c.viaG ? "via-G" : "direct");
                    r.put("directRtt", round1(c.directRtt));
                    r.put("viaGRtt", round1(c.viaGRtt));
                    routes.add(r);
                }
            }
            out.put("routes", routes);
        } catch (Exception ex) {
            out.put("error", String.valueOf(ex.getMessage()));
        }
        return new Gson().toJson(out);
    }

    /** Rounds a millisecond/cost reading to one decimal place to keep the JSON compact and readable. */
    private static double round1(double v) { return Math.round(v * 10) / 10.0; }
}
