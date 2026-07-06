package io.cresco.agent.controller.netmetrics;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.BrokeredAgent;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * PUSH-based link-state sharing over the dataplane.
 *
 * Each controller periodically advertises its OWN locally-measured neighbour edges (smoothed RTT,
 * composite cost, and the live AutoTuner connector count so dynamic link scaling is reflected) onto
 * the GLOBAL dataplane topic, and subscribes to everyone else's advertisements to build a mesh-wide
 * {@link RouteView}. This is publish/subscribe, not an RPC fan-out: metrics are pushed as they change,
 * which scales; a pull would not. RPC stays reserved for config/query + one-time reconciliation.
 *
 * QoS: advertisements are NON_PERSISTENT, lowest priority (0), and carry a short time-to-live (a few
 * advertise intervals) so they never sit in a queue or compete with data-plane traffic -- a dropped or
 * expired advertisement is simply refreshed on the next tick.
 */
public class RouteAdvertiser {

    static final String SELECTOR_PROP = "cresco_msg_type";
    static final String SELECTOR_VAL = "route_lsa";
    private static final String SELECTOR = SELECTOR_PROP + " = '" + SELECTOR_VAL + "'";

    private final ControllerEngine ce;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final RouteView routeView;
    private final Gson gson = new Gson();
    private final long ttlMs;
    private final String selfNode;

    private volatile boolean subscribed = false;
    private String listenerId;
    private volatile String lastViewSig = "";

    /**
     * @param ce                  the owning controller engine (source of identity, metrics and the dataplane)
     * @param routeView           the mesh-wide view this advertiser publishes into and populates from peers
     * @param advertiseIntervalMs the AutoTuner tick period; the advertisement TTL is set to 3x this (min 3s)
     *                            so an LSA survives a couple of missed ticks and then self-expires
     */
    public RouteAdvertiser(ControllerEngine ce, RouteView routeView, long advertiseIntervalMs) {
        this.ce = ce;
        this.plugin = ce.getPluginBuilder();
        this.logger = plugin.getLogger(RouteAdvertiser.class.getName(), CLogger.Level.Info);
        this.routeView = routeView;
        this.ttlMs = Math.max(3000L, advertiseIntervalMs * 3); // survive a couple missed ticks, then expire
        this.selfNode = safe(ce.cstate.getRegion()) + "_" + safe(ce.cstate.getAgent());
    }

    /** Wire-format DTO (gson) for one advertisement. */
    static final class Lsa {
        String node, region, role;
        long ts;
        List<double[]> edgesNum; // parallel to edgePaths: [srtt, cost, conns]
        List<String> edgePaths;
        List<String> addrs;      // dialable discovery addresses "ip:port" of every data-plane NIC
    }

    /** This node's dialable discovery addresses: each non-loopback IPv4 on the data plane + discovery port. */
    private List<String> localAddrs() {
        List<String> out = new ArrayList<>();
        int dport = plugin.getConfig().getIntegerParam("netdiscoveryport", 32005);
        try {
            java.util.Enumeration<java.net.NetworkInterface> ifs = java.net.NetworkInterface.getNetworkInterfaces();
            while (ifs.hasMoreElements()) {
                java.net.NetworkInterface ni = ifs.nextElement();
                if (ni.isLoopback() || !ni.isUp()) continue;
                for (java.net.InterfaceAddress ia : ni.getInterfaceAddresses()) {
                    java.net.InetAddress a = ia.getAddress();
                    if (a instanceof java.net.Inet4Address && !a.isLoopbackAddress()) {
                        String ip = a.getHostAddress();
                        // skip the containerlab management subnet (172.20.x) -- advertise data-plane only
                        if (ip.startsWith("172.20.")) continue;
                        out.add(ip + ":" + dport);
                    }
                }
            }
        } catch (Exception ignore) { }
        return out;
    }

    /** Called once per AutoTuner tick. Lazily subscribes (once the dataplane is up), then publishes. */
    public void tick() {
        try {
            if (!ce.getActiveClient().isFaultURIActive()) return; // dataplane not ready yet
            ensureSubscribed();
            publish();
            logViewIfChanged();
        } catch (Exception ex) {
            logger.debug("RouteAdvertiser.tick error: " + ex.getMessage());
        }
    }

    /** Log the mesh-wide view (received via push) whenever its shape changes -- proof the share works. */
    private void logViewIfChanged() {
        StringBuilder sb = new StringBuilder();
        for (RouteView.NodeState ns : routeView.fresh()) {
            sb.append(ns.node).append("{");
            for (RouteView.Edge e : ns.edges) {
                sb.append(e.toPath).append("=rtt").append(String.format("%.0f", e.srtt))
                  .append("/cost").append(String.format("%.0f", e.cost)).append("/c").append(e.conns).append(" ");
            }
            sb.append("} ");
        }
        String sig = sb.toString();
        if (!sig.equals(lastViewSig)) {
            lastViewSig = sig;
            logger.info("RouteView (pushed from " + routeView.size() + " peers): " + (sig.isEmpty() ? "(empty)" : sig));
        }
    }

    /** Idempotently attaches the GLOBAL-topic listener (filtered by the route_lsa selector) exactly once. */
    private synchronized void ensureSubscribed() {
        if (subscribed) return;
        try {
            DataPlaneService dps = ce.getDataPlaneService();
            if (dps == null) return;
            MessageListener ml = new MessageListener() {
                @Override public void onMessage(Message message) { onLsa(message); }
            };
            listenerId = dps.addMessageListener(TopicType.GLOBAL, ml, SELECTOR);
            subscribed = true;
            logger.info("RouteAdvertiser subscribed to pushed link-state (selector " + SELECTOR + ")");
        } catch (Exception ex) {
            logger.debug("RouteAdvertiser.ensureSubscribed error: " + ex.getMessage());
        }
    }

    /**
     * Publishes this node's current link-state: one edge per measured neighbour (srtt/cost/conns) plus its
     * dialable addresses, as a NON_PERSISTENT, priority-0, TTL-bounded message on the GLOBAL topic. Emits
     * even with no measured edges (as long as it has an address) so peers can learn its address and infer a
     * direct link. A no-op if the link-metrics registry or dataplane is not yet available.
     */
    private void publish() {
        LinkMetricsRegistry reg = ce.getLinkMetricsRegistry();
        if (reg == null) return;

        Lsa lsa = new Lsa();
        lsa.node = selfNode;
        lsa.region = ce.cstate.getRegion();
        lsa.role = role();
        lsa.ts = System.currentTimeMillis();
        lsa.edgePaths = new ArrayList<>();
        lsa.edgesNum = new ArrayList<>();
        for (LinkMetrics lm : reg.all()) {
            String path = lm.getPath();
            if (path == null || path.equals(selfNode)) continue; // skip the self/loopback entry
            lsa.edgePaths.add(path);
            lsa.edgesNum.add(new double[]{ lm.getSrtt(), lm.cost(), connsFor(path) });
        }
        lsa.addrs = localAddrs();          // publish where this node's broker can be dialed
        // advertise even with no measured edges yet, so peers can LEARN our address and infer a link
        if (lsa.edgePaths.isEmpty() && lsa.addrs.isEmpty()) return;

        try {
            DataPlaneService dps = ce.getDataPlaneService();
            TextMessage tm = dps.createTextMessage();
            tm.setText(gson.toJson(lsa));
            tm.setStringProperty(SELECTOR_PROP, SELECTOR_VAL);
            dps.sendMessage(TopicType.GLOBAL, tm, DeliveryMode.NON_PERSISTENT, 0, (int) ttlMs);
        } catch (Exception ex) {
            logger.debug("RouteAdvertiser.publish error: " + ex.getMessage());
        }
    }

    /** Listener callback: parses a peer's advertisement and folds it into the {@link RouteView} (ignores our own echo). */
    private void onLsa(Message message) {
        try {
            if (!(message instanceof TextMessage)) return;
            Lsa lsa = gson.fromJson(((TextMessage) message).getText(), Lsa.class);
            if (lsa == null || lsa.node == null) return;
            if (lsa.node.equals(selfNode)) return; // ignore our own echo
            List<RouteView.Edge> edges = new ArrayList<>();
            if (lsa.edgePaths != null && lsa.edgesNum != null) {
                for (int i = 0; i < lsa.edgePaths.size() && i < lsa.edgesNum.size(); i++) {
                    double[] n = lsa.edgesNum.get(i);
                    edges.add(new RouteView.Edge(lsa.edgePaths.get(i), n[0], n[1], (int) n[2]));
                }
            }
            routeView.update(new RouteView.NodeState(lsa.node, lsa.region, lsa.role, edges, lsa.addrs));
        } catch (Exception ex) {
            logger.debug("RouteAdvertiser.onLsa parse error: " + ex.getMessage());
        }
    }

    /** Active parallel bridge connectors to the broker hosting {@code path} (the scaling signal). */
    private int connsFor(String path) {
        try {
            BrokeredAgent ba = ce.getBrokeredAgents().get(path);
            if (ba == null) return 0;
            String host = ba.getActiveAddress();
            if (host == null) return 0;
            return ce.getBroker().getBridgeConnectionCount(host);
        } catch (Exception e) { return 0; }
    }

    private String role() {
        if (ce.cstate.isGlobalController()) return "global";
        if (ce.cstate.isRegionalController()) return "region";
        return "agent";
    }

    private static String safe(String s) { return s == null ? "" : s; }

    /** Detaches the dataplane listener; safe to call more than once and if never subscribed. */
    public void shutdown() {
        try {
            if (listenerId != null && ce.getDataPlaneService() != null) {
                ce.getDataPlaneService().removeMessageListener(listenerId);
            }
        } catch (Exception ignore) { }
    }
}
