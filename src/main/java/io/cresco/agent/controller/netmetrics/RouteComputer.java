package io.cresco.agent.controller.netmetrics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Topology-aware path computation over the learned mesh graph (the {@link RouteView}, populated by
 * pushed link-state). Runs Dijkstra from this node to any destination, weighting each edge by measured
 * smoothed RTT so the result is the LOWEST-LATENCY path -- not the fewest broker hops. This generalizes
 * the earlier per-peer "direct vs via-global" heuristic to real N-hop routing across the whole network:
 * for R1->R3 it will pick R1-R2-R3, R1-G-R3, or (after inference) R1-R4-R3 -- whichever the live measured
 * latencies make cheapest -- and emit the source-route stack the MsgRouter enforces.
 *
 * The graph is treated as UNDIRECTED (an advertised edge A->B implies B->A at the same cost); links are
 * roughly symmetric and not every node measures both directions, so this fills the reverse edges.
 */
public final class RouteComputer {

    private RouteComputer() {}

    /** Min-latency total-cost + hop count result. */
    public static final class Path {
        public final List<String> nodes;   // ordered region_agent nodes, including from & to
        public final double cost;           // summed edge srtt (ms)
        public Path(List<String> nodes, double cost) { this.nodes = nodes; this.cost = cost; }
        public int hops() { return nodes.size() - 1; }
    }

    /**
     * Lowest-latency path from {@code fromNode} to {@code toNode} over the current RouteView, or null if
     * unreachable. Edge weight = max(0.1, srtt); an unmeasured/negative srtt edge is skipped.
     */
    public static Path compute(RouteView rv, String fromNode, String toNode) {
        if (rv == null || fromNode == null || toNode == null || fromNode.equals(toNode)) return null;

        // Build undirected adjacency from every fresh node's advertised edges.
        Map<String, Map<String, Double>> adj = new HashMap<>();
        for (RouteView.NodeState ns : rv.fresh()) {
            if (ns.edges == null) continue;
            for (RouteView.Edge e : ns.edges) {
                double w = (e.srtt > 0) ? e.srtt : -1;
                if (w < 0) continue;
                addEdge(adj, ns.node, e.toPath, w);
                addEdge(adj, e.toPath, ns.node, w);   // undirected
            }
        }
        if (!adj.containsKey(fromNode)) return null;

        // Dijkstra.
        Map<String, Double> dist = new HashMap<>();
        Map<String, String> prev = new HashMap<>();
        PriorityQueue<String> pq = new PriorityQueue<>(Comparator.comparingDouble(n -> dist.getOrDefault(n, Double.MAX_VALUE)));
        dist.put(fromNode, 0.0);
        pq.add(fromNode);
        while (!pq.isEmpty()) {
            String u = pq.poll();
            if (u.equals(toNode)) break;
            double du = dist.getOrDefault(u, Double.MAX_VALUE);
            Map<String, Double> nbrs = adj.get(u);
            if (nbrs == null) continue;
            for (Map.Entry<String, Double> nb : nbrs.entrySet()) {
                double nd = du + nb.getValue();
                if (nd < dist.getOrDefault(nb.getKey(), Double.MAX_VALUE)) {
                    dist.put(nb.getKey(), nd);
                    prev.put(nb.getKey(), u);
                    pq.remove(nb.getKey());
                    pq.add(nb.getKey());
                }
            }
        }
        if (!dist.containsKey(toNode)) return null;

        // Reconstruct.
        List<String> path = new ArrayList<>();
        for (String at = toNode; at != null; at = prev.get(at)) path.add(0, at);
        if (path.isEmpty() || !path.get(0).equals(fromNode)) return null;
        return new Path(path, dist.get(toNode));
    }

    /**
     * Source-route stack ("region,agent;...") of the waypoints AFTER {@code fromNode} on the lowest-
     * latency path to {@code toNode}, or null if the path is trivial (direct single hop -- let the
     * default handle it) or unreachable. The head of the returned stack is the message's next hop.
     */
    public static String computeSrcRoute(RouteView rv, String fromNode, String toNode) {
        Path p = compute(rv, fromNode, toNode);
        if (p == null || p.hops() < 2) return null;    // only steer genuinely multi-hop optimal paths
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < p.nodes.size(); i++) {     // skip the source
            if (sb.length() > 0) sb.append(';');
            sb.append(toWaypoint(p.nodes.get(i)));
        }
        return sb.toString();
    }

    /** Adds/updates directed edge {@code a->b} with weight {@code w}, keeping the cheapest parallel edge. */
    private static void addEdge(Map<String, Map<String, Double>> adj, String a, String b, double w) {
        Map<String, Double> m = adj.computeIfAbsent(a, k -> new HashMap<>());
        Double cur = m.get(b);
        if (cur == null || w < cur) m.put(b, w);       // keep the cheapest parallel edge
    }

    /** "region_agent" -> "region,agent" (region names carry no underscore in this fabric). */
    private static String toWaypoint(String node) {
        int us = node.indexOf('_');
        return (us <= 0) ? node : node.substring(0, us) + "," + node.substring(us + 1);
    }
}
