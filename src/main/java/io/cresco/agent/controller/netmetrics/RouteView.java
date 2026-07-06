package io.cresco.agent.controller.netmetrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mesh-wide link-state view, assembled from route advertisements PUSHED over the dataplane
 * (publish/subscribe), never RPC-pulled -- a pull fan-out to every node does not scale. Every
 * controller keeps one instance; a cost-aware route selector reads it to compute cross-federation
 * paths (e.g. is R1->G->R2 cheaper than the direct R1->R2 link?).
 *
 * Each node periodically advertises its OWN neighbour edges (locally measured RTT/cost + the current
 * AutoTuner connector count, so dynamic scaling shows up in the shared view). Entries carry a receive
 * timestamp and are treated as stale after {@code staleMs} so a node that goes silent drops out.
 */
public class RouteView {

    /** One advertised edge: this node -> {@code toPath}. */
    public static final class Edge {
        public final String toPath;
        public final double srtt;   // smoothed RTT ms (-1 = no sample)
        public final double cost;   // composite link cost (lower = better)
        public final int conns;     // active parallel bridge connectors (AutoTuner scaling signal)
        public Edge(String toPath, double srtt, double cost, int conns) {
            this.toPath = toPath; this.srtt = srtt; this.cost = cost; this.conns = conns;
        }
    }

    /** A node's advertised local link-state. */
    /** One node's advertised state: its identity, role, dialable addresses, neighbour edges, and freshness. */
    public static final class NodeState {
        public final String node;    // region_agent
        public final String region;
        public final String role;    // global | region | agent
        public final List<Edge> edges;
        public final List<String> addrs; // dialable discovery addresses "ip:port" (all data-plane NICs)
        public volatile long updatedTs;
        public NodeState(String node, String region, String role, List<Edge> edges, List<String> addrs) {
            this.node = node; this.region = region; this.role = role; this.edges = edges;
            this.addrs = (addrs == null) ? new ArrayList<>() : addrs;
        }
    }

    private final ConcurrentHashMap<String, NodeState> nodes = new ConcurrentHashMap<>();
    private final long staleMs;

    /** @param staleMs age after which a node that stops advertising is dropped from {@link #fresh}/{@link #get}. */
    public RouteView(long staleMs) { this.staleMs = staleMs; }

    /** Ingest one node's advertisement (last-writer-wins by node path). */
    public void update(NodeState ns) {
        ns.updatedTs = System.currentTimeMillis();
        nodes.put(ns.node, ns);
    }

    /** All currently-fresh node states. */
    public Collection<NodeState> fresh() {
        long cutoff = System.currentTimeMillis() - staleMs;
        List<NodeState> out = new ArrayList<>();
        for (NodeState ns : nodes.values()) if (ns.updatedTs >= cutoff) out.add(ns);
        return out;
    }

    /** @return the fresh {@link NodeState} for {@code node}, or null if unknown or stale. */
    public NodeState get(String node) {
        NodeState ns = nodes.get(node);
        if (ns == null) return null;
        return (ns.updatedTs >= System.currentTimeMillis() - staleMs) ? ns : null;
    }

    /** Cost of the direct edge from {@code fromNode} to {@code toNode}, or MAX if not advertised/fresh. */
    public double directCost(String fromNode, String toNode) {
        NodeState ns = get(fromNode);
        if (ns == null) return Double.MAX_VALUE;
        for (Edge e : ns.edges) if (e.toPath.equals(toNode)) return e.cost;
        return Double.MAX_VALUE;
    }

    /** Snapshot for logging/inspection. */
    public Map<String, NodeState> snapshot() { return new ConcurrentHashMap<>(nodes); }

    /** @return the number of currently-fresh nodes in the view. */
    public int size() { return (int) fresh().size(); }
}
