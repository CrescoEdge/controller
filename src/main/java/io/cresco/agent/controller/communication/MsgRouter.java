package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class MsgRouter {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private volatile boolean loggedCost = false;

    // --- Source / segment routing (W4) ---------------------------------------------------------
    // The data-plane half of performance-aware routing. Instead of naming only the final destination
    // and letting the ActiveMQ network-of-brokers pick the hops, the ingress can attach an ordered
    // stack of node waypoints ("region,agent" each, ';'-separated). Each waypoint's controller pops
    // itself and re-addresses forwardDst to the next waypoint (a normal ActiveMQ leg); when the stack
    // empties, the true destination (this node + the saved dst-plugin) is restored and delivered
    // locally. This lets a route-computer force a message down a chosen path (e.g. via a low-latency
    // region, or around a congested one) rather than ActiveMQ's default shortest-broker-hop route.
    // Gated by net_source_routing (default off); a message with no SRCROUTE param is untouched.
    public static final String SRCROUTE = "srcroute";            // "r1,a1;rg,gc;r2,a2" (remaining hops)
    public static final String SRCROUTE_DST_PLUGIN = "srcroute_dst_plugin"; // preserved final dst plugin
    private static final int SRCROUTE_MAX_HOPS = 16;             // safety bound on the stack length
    private volatile boolean loggedSrcRoute = false;
    private volatile boolean loggedInject = false;

    public MsgRouter(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(MsgRouter.class.getName(),CLogger.Level.Trace);
    }

    private void forwardToLocalAgent(MsgEvent rm) {
                    controllerEngine.getPluginBuilder().msgIn(rm);
    }

    private void forwardToLocalPlugin(MsgEvent rm) {
                    controllerEngine.getPluginAdmin().msgIn(rm);
    }

    private void forwardToLocalRegionalController(MsgEvent rm) {
                controllerEngine.getRegionHealthWatcher().sendRegionalMsg(rm);
    }

    private void forwardToRemoteRegionalController(MsgEvent rm) {

        //set remote regional controller address
        rm.setForwardDst(controllerEngine.cstate.getRegionalRegion(),controllerEngine.cstate.getRegionalAgent(),null);
        controllerEngine.getActiveClient().sendMessage(rm);

    }

    private void forwardToLocalRegion(MsgEvent rm) {
        controllerEngine.getActiveClient().sendMessage(rm);

    }

    private void forwardToRemoteRegion(MsgEvent rm) {

        controllerEngine.getActiveClient().sendMessage(rm);

    }

    private void forwardToLocalGlobal2(MsgEvent rm) {
        logger.error("forwardToLocalGlobal() " + rm.getParams());
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            logger.error("forwardToLocalGlobal() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
       
    }

    private void forwardToLocalGlobal(MsgEvent rm) {
        logger.debug("forwardToLocalGlobal() " + rm.getParams());
        forwardToLocalRegion(rm);
    }

    private void forwardToRemoteGlobal(MsgEvent rm) {
        logger.error("forwardToRemoteGlobal() " + rm.getParams());
        forwardToRemoteRegion(rm);
    }



    public void route(MsgEvent rm) {
        long messageTimeStamp = System.nanoTime();
        try {

            rm = getTTL(rm);

            if(rm != null) {

                // Cost-aware source-route INJECTION (ingress enforcement). ActiveMQ will not pick an
                // optimal path for us -- it pins one arbitrary path and never load-balances or routes
                // around a slow link. So when this node has PROBED a faster path to the destination region
                // than the default, we attach that path's waypoint stack here, at the origin, and the
                // source-routing machinery below carries the flow along it. This is Cresco controlling the
                // path end-to-end; the probe/selector decided it, this line enforces it on real traffic.
                maybeInjectCostRoute(rm);

                // Source/segment routing: if this message carries a waypoint stack and this node is the
                // current head, pop-and-forward to the next waypoint. Returns true when the message was
                // re-forwarded (nothing more to do here); false to continue to normal routing (either the
                // final waypoint reached with the true dst restored, or no/irrelevant source route).
                if (plugin.getConfig().getBooleanParam("net_source_routing", false)
                        && rm.getParam(SRCROUTE) != null) {
                    if (advanceSourceRoute(rm)) {
                        return; // forwarded to the next waypoint; finally-block still records the timer
                    }
                }

                int routePath = getRoutePath(rm);
                rm.setParam("routepath-" + plugin.getAgent(), String.valueOf(routePath));

                // Cost-aware routing hook: publish this hop's uplink cost (srtt + backpressure + inverse
                // throughput) into the message trail. The routing tree is strictly deterministic, so
                // min-cost SELECTION only has alternates to choose from where redundant federation bridges
                // exist (region<->region / multi-global) -- there a selector reads costOf()/lowestCostEdge().
                // Gated; default off. See docs/link-metrics-design.md.
                if (plugin.getConfig().getBooleanParam("net_cost_routing", false)) {
                    try {
                        io.cresco.agent.controller.netmetrics.LinkMetricsRegistry reg = controllerEngine.getLinkMetricsRegistry();
                        if (reg != null) {
                            String up = io.cresco.agent.controller.netmetrics.LinkMetricsRegistry.parentLinkKey(controllerEngine);
                            String cost = String.format("%.2f", reg.costOf(up));
                            rm.setParam("linkcost-" + plugin.getAgent(), cost);
                            if (!loggedCost) { loggedCost = true; logger.info("cost-routing: annotated linkcost-" + plugin.getAgent() + "=" + cost); }
                        }
                    } catch (Exception ignore) { }
                }

                //if(rm.paramsContains("inodemap")) {
                //    logger.error("MESSAGE HEADER [" + rm.printHeader() + "] Route Path: [" + routePath + "]");
                //}


                switch (routePath) {

                    case 335:
                        logger.debug("remote agent sending message to local agent 463");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 351:
                        logger.debug("remote agent sending message to local plugin 351");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 383:
                        logger.debug("remote agent sending message to local plugin 383");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 463:
                        logger.debug("remote agent sending message to local agent 463");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 495:
                        logger.debug("remote plugin sending message to local agent 495");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 479:
                        logger.debug("remote agent sending message to local plugin 479");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 511:
                        logger.debug("remote plugin sending message to local plugin 511");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 655:
                        logger.debug("Local agent sending message to remote global agent 655");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 671:
                        logger.debug("Local agent sending message to remote global agentcontroller 671");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 687:
                        logger.debug("Local agentcontroller sending message to remote global agent 687");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 703:
                        logger.debug("Local agentcontroller sending message to remote global agentcontroller 703");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 719:
                        logger.debug("Local agent sending message to remote regional agent 719");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 735:
                        logger.debug("Local agent sending message to remote regional agentcontroller 735");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 751:
                        logger.debug("Local agentcontroller sending message to remote regional agent 751");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 767:
                        logger.debug("Local agentcontroller sending message to remote regional agentcontroller 767");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 991:
                        logger.debug("Local agent sending message to local agentcontroller 991");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 975:
                        logger.debug("Local agent sending message to self 1007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 1007:
                        logger.debug("Local agentcontroller sending message to local agent 1007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 1023:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 1023");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 4447:
                        logger.debug("Remote globalcontroller sending message to local agentcontroller plugin 4447");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 4431:
                        logger.debug("Remote region sending message to local agent 4431");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 4751:
                        logger.debug("Local agentcontroller sending message to remote global agent 4751");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 4767:
                        logger.debug("Local agentcontroller sending message to remote global agent 4767");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 4783:
                        logger.debug("Local agentcontroller sending message to remote global agent 4783");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4799:
                        logger.debug("Local agentcontroller sending message to remote global agentcontroller 4799");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4815:
                        logger.debug("Local agent sending message to local regional agent 4815");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 4831:
                        logger.debug("Local agent sending message to local regional agentcontroller 4831");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 4847:
                        logger.debug("Local agentcontroller sending message to local regional agent 4847");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 4863:
                        logger.debug("Local agentcontroller sending message to local regional agentcontroller 4863");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 5071:
                        logger.debug("Local agent sending message to self 5071");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 5087:
                        logger.debug("Local agent sending message to local agentcontroller 5087");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 5103:
                        logger.debug("Local agentcontroller sending message to local agent 5103");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 5119:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 5119");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12623:
                        logger.debug("Remote Global or Region sending message to local agent 12623");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 12639:
                        logger.debug("remote agent sending message to local agent plugin");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12655:
                        logger.debug("remote global agent sending message to local global agent");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 12671:
                        logger.debug("remote agent sending message to local agent");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12751:
                        logger.debug("Remote agent sending message to local agent 12751");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 12767:
                        logger.debug("remote agent sending message to local plugin 12767");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12799:
                        logger.debug("remote agent sending message to local plugin 12799");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12943:
                        logger.debug("Local region sending message to remote region 12943");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        //forwardToLocalGlobal(rm);
                        break;

                    case 12959:
                        logger.debug("Local agent sending message to local regional agentcontroller 12959");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 12975:
                        logger.debug("Local agentcontroller sending message to local global agent 12975");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 12991:
                        logger.debug("Local agentcontroller sending message to local global agentcontroller 12991");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 13007:
                        logger.debug("Local agent sending message to local regional agent 13007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13023:
                        logger.debug("Local agent sending message to local regional agentcontroller 13023");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13039:
                        logger.debug("Local agentcontroller sending message to local regional agent 13039");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13055:
                        logger.debug("Local agentcontroller sending message to local regional agentcontroller 13055");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13279:
                        logger.debug("Local agent sending message to local agentcontroller 13279");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 13311:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 13311");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 13263:
                        logger.debug("Local agent sending message to self 13263");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 13295:
                        logger.debug("Local agentcontroller sending message to local agent 13295");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 16863:
                        logger.debug("remote region sending message to local agent controller 16863");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 17359:
                        logger.debug("Local agentcontroller sending message to remote regional or global controller 17359");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegionalController(rm);
                        break;

                    case 17391:
                        logger.debug("Local agentcontroller sending message to remote regional or global controller 17391");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegionalController(rm);
                        break;

                    case 20943:
                        logger.debug("remote agent sending message to local regional controller 20943");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 20975:
                        logger.debug("remote plugin sending message to local regional controller 20975");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 21135:
                        logger.debug("Local region sending message to remote global controller 21135");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 21199:
                        logger.debug("Local region sending message to local regional controller 21199");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 21455:
                        logger.debug("Local agent sending message to local regional or global controller 21455");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 21487:
                        logger.debug("Local agent controller sending message to local regional or remote global controller 21487");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29007:
                        logger.debug("Remote regional controller sending message to local global controller 29007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29039:
                        logger.debug("Remote plugin sending message to local global controller 29039");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;


                    case 29135:
                        logger.debug("Remote agent sending message to local regional controller 29135");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29167:
                        logger.debug("Remote agent sending message to local global controller 29167");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29327:
                        logger.debug("Local global controller sending message to remote regional controller 29327");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 29391:
                        logger.debug("Local agent sending message to remote agent 29391");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 29407:
                        logger.debug("Local agent sending message to remote agent 29407");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 29647:
                        logger.debug("Local or remote agent sending message to local regional controller 29647");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29663:
                        logger.debug("Local regional or local global controller sending message back to plugin 29663");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 29679:
                        logger.debug("Local agentcontroller sending message to local global controller 29679");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 4463:
                        // Remote controller (another region, or the global) -> THIS region's controller.
                        // A cross-region controller<->controller RPC (e.g. the global's getmetricinventory /
                        // listagents fan-out to a child region). ActiveMQ delivered it here because it is
                        // addressed to this region's queue, so this is LOCAL delivery (not transit): hand it
                        // to the regional controller executor, which processes the action and returns the RPC
                        // reply. The strict-tree single-node builds never exercised this hop, so the static
                        // table omitted it and the message fell to default-drop (silent RPC timeout).
                        logger.debug("Remote controller sending message to local regional controller 4463");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    default:
                        // CATCH-ALL. The static table enumerates the hop patterns the tree/loopback builds
                        // exercised; a real multi-region mesh produces a few more. Resolve by DESTINATION:
                        //   - addressed to THIS node  -> deliver locally (controller or a hosted plugin);
                        //   - addressed ELSEWHERE     -> RELAY it toward its destination. Relaying is the
                        //     whole point of a bridge: a region acts as a router, forwarding traffic for
                        //     other regions so the mesh can carry (and, with source-routing/cost selection,
                        //     steer) cross-region flows -- e.g. R1 -> G -> R2 around a slow direct link.
                        // SECURITY: this is not an open relay. Every message here already arrived over an
                        // mTLS-authenticated, secret-gated broker bridge (trusted fabric traffic), it is
                        // forwarded only toward the destination named in its own header, and getTTL() above
                        // bounds hop count (drops at >10) so a relay loop cannot amplify. Which bridge a
                        // transit hop takes is the PATH LOOKUP (gated net_source_routing / cost selector);
                        // absent a route header we hand it to the broker network toward its dst.
                        boolean forThisNode = rm.getDstRegion() != null
                                && rm.getDstRegion().equals(plugin.getRegion())
                                && rm.getDstAgent() != null
                                && rm.getDstAgent().equals(plugin.getAgent());
                        if (forThisNode && (rm.getDstPlugin() == null
                                || rm.getDstPlugin().equals(plugin.getPluginID()))) {
                            logger.debug("route catch-all: local controller delivery rp=" + routePath);
                            forwardToLocalRegionalController(rm);
                        } else if (forThisNode && rm.getDstPlugin() != null) {
                            logger.debug("route catch-all: local plugin delivery rp=" + routePath);
                            forwardToLocalPlugin(rm);
                        } else {
                            logger.debug("route catch-all: relay toward " + rm.getForwardDst()
                                    + " rp=" + routePath);
                            forwardToRemoteRegion(rm);
                        }
                        break;
                }

            }

        } catch (Exception ex) {
            logger.error("MsgRouter.route Route Failed " + ex.toString() + " " + rm.getParams().toString(), ex);

        }
        finally
        {
            if(controllerEngine.getMeasurementEngine() != null) {
                controllerEngine.getMeasurementEngine().updateTimer("message.transaction.time", messageTimeStamp);
            }
        }

    }

    /**
     * At the ingress region, steer a peer-bound flow onto the path the cost selector chose. If this node
     * originated the message, it is bound for another region, it has no explicit route already, and the
     * {@link io.cresco.agent.controller.netmetrics.PathTable} holds a faster probed path to that region,
     * attach that waypoint stack (head becomes forwardDst). {@link #advanceSourceRoute} then carries it.
     * Gated by net_cost_routing. Only locally-originated traffic is steered, so a transiting message is
     * never re-injected (which would loop); the SRCROUTE guard also stops double-injection.
     */
    private void maybeInjectCostRoute(MsgEvent rm) {
        try {
            if (!plugin.getConfig().getBooleanParam("net_cost_routing", false)) return;
            if ("1".equals(rm.getParam("no_cost_route"))) return;            // prober opts out (measures raw path)
            if (rm.getParam(SRCROUTE) != null) return;                       // already routed
            if (rm.getDstRegion() == null || rm.getDstAgent() == null) return;
            if (rm.getDstRegion().equals(plugin.getRegion())) return;        // not leaving our region
            if (!plugin.getRegion().equals(rm.getSrcRegion())) return;       // only steer OUR own egress
            String dstNode = rm.getDstRegion() + "_" + rm.getDstAgent();
            // Primary: Dijkstra over the whole learned graph -> lowest-latency N-hop path to any region.
            String route = io.cresco.agent.controller.netmetrics.RouteComputer.computeSrcRoute(
                    controllerEngine.getRouteView(), plugin.getRegion() + "_" + plugin.getAgent(), dstNode);
            // Fallback: the per-peer probe choice (direct-vs-viaG) if the graph has no better multi-hop path.
            if (route == null) {
                io.cresco.agent.controller.netmetrics.PathTable pt = controllerEngine.getPathTable();
                route = (pt != null) ? pt.chosenSrcroute(dstNode) : null;
            }
            if (route == null) return;                                       // default (direct) path is best -> leave it
            int comma = route.indexOf(','), semi = route.indexOf(';');
            if (comma <= 0) return;
            String headRegion = route.substring(0, comma);
            String headAgent = route.substring(comma + 1, semi < 0 ? route.length() : semi);
            rm.setParam(SRCROUTE, route);
            rm.setParam(SRCROUTE_DST_PLUGIN, rm.getDstPlugin() == null ? "" : rm.getDstPlugin());
            rm.setForwardDst(headRegion, headAgent, null);
            if (!loggedInject) {
                loggedInject = true;
                logger.info("cost-routing: steering egress to " + dstNode + " via chosen path [" + route + "]");
            }
        } catch (Exception ignore) { }
    }

    /**
     * Advance a source-routed message by one waypoint. The {@link #SRCROUTE} param is an ordered,
     * ';'-separated stack of "region,agent" node waypoints (the hops still to visit); the message's
     * forwardDst is always the head of that stack, so ActiveMQ delivers the message here because this
     * node IS the head. We pop ourselves and:
     *   - if waypoints remain: re-address forwardDst to the next waypoint and forward it (one ActiveMQ
     *     leg) -> return true (message consumed here).
     *   - if the stack is now empty: this is the final waypoint; restore the true destination
     *     (this node + the saved dst-plugin), drop the routing headers, and return false so normal
     *     routing delivers it locally.
     * Defensive: if the head is not this node, leave the message alone (return false) and let normal
     * routing carry it toward forwardDst. TTL (getTTL) still bounds total hops; the stack is also
     * length-capped. Any parse error falls back to normal routing (return false) — never drops silently.
     *
     * @param rm the message being routed
     * @return true if this call consumed/forwarded the message (caller should stop), false to let normal
     *         routing proceed (final waypoint reached, not source-routed, or a defensive fallback)
     */
    private boolean advanceSourceRoute(MsgEvent rm) {
        try {
            String path = rm.getParam(SRCROUTE);
            if (path == null || path.isEmpty()) return false;

            java.util.List<String> hops = new java.util.ArrayList<>();
            for (String h : path.split(";")) { h = h.trim(); if (!h.isEmpty()) hops.add(h); }
            if (hops.isEmpty()) { rm.removeParam(SRCROUTE); return false; }
            if (hops.size() > SRCROUTE_MAX_HOPS) {
                logger.error("source-routing: stack too long (" + hops.size() + " > " + SRCROUTE_MAX_HOPS + "), dropping route header");
                rm.removeParam(SRCROUTE); rm.removeParam(SRCROUTE_DST_PLUGIN);
                return false;
            }

            String me = plugin.getRegion() + "," + plugin.getAgent();
            // The head must be this node (ActiveMQ delivered here because forwardDst=head=me). If not,
            // this node is only a transit broker at the ActiveMQ layer — let normal routing continue.
            if (!hops.get(0).equals(me)) return false;

            // Prove the path taken: stamp this waypoint into the trail (visible on the final message).
            rm.setParam("srcroute-hop-" + plugin.getAgent(), me);
            hops.remove(0); // pop myself

            if (hops.isEmpty()) {
                // Final waypoint: restore the true destination and deliver locally.
                String dstPlugin = rm.getParam(SRCROUTE_DST_PLUGIN);
                rm.removeParam(SRCROUTE);
                rm.removeParam(SRCROUTE_DST_PLUGIN);
                rm.setForwardDst(plugin.getRegion(), plugin.getAgent(),
                        (dstPlugin != null && !dstPlugin.isEmpty()) ? dstPlugin : null);
                if (!loggedSrcRoute) { loggedSrcRoute = true; logger.info("source-routing: active (final-hop delivery at " + me + ")"); }
                return false; // continue to normal routing -> local delivery
            } else {
                // More waypoints: re-address to the next hop and forward one ActiveMQ leg.
                String next = hops.get(0);
                int comma = next.indexOf(',');
                if (comma <= 0 || comma >= next.length() - 1) {
                    logger.error("source-routing: malformed next hop '" + next + "', dropping route header");
                    rm.removeParam(SRCROUTE); rm.removeParam(SRCROUTE_DST_PLUGIN);
                    return false;
                }
                String nextRegion = next.substring(0, comma);
                String nextAgent = next.substring(comma + 1);
                rm.setParam(SRCROUTE, String.join(";", hops));
                rm.setForwardDst(nextRegion, nextAgent, null);
                if (!loggedSrcRoute) { loggedSrcRoute = true; logger.info("source-routing: active (forwarding " + me + " -> " + next + ")"); }
                controllerEngine.getActiveClient().sendMessage(rm);
                return true; // consumed here
            }
        } catch (Exception ex) {
            logger.error("advanceSourceRoute error: " + ex.getMessage(), ex);
            return false; // never drop silently — fall back to normal routing
        }
    }

    private int getRoutePath(MsgEvent rm) {
        int routePath;
        try {
            String RC = "0";
            if(controllerEngine.cstate.isRegionalController()) {
                RC = "1";
            }

            String GC = "0";
            if(controllerEngine.cstate.isGlobalController()) {
                GC = "1";
            }

            // Message-scope route bits. IMPORTANT (do NOT "fix" GM to be set on isGlobal):
            // A global message is intentionally routed through the REGIONAL (RM) handling cases.
            // GM is the top positional bit in routeString below; it is a RESERVED placeholder that
            // keeps every routePath value aligned with the switch(routePath) case numbers (which top
            // out at 29679, i.e. GM always 0). No GM=1 case exists — setting GM here would push global
            // traffic to routePath >= 32768, which the switch's default only logs-and-drops (silent
            // loss of global control-plane traffic). Splitting global vs regional routing is a full
            // routing-table rework (new GM cases + a routing test matrix), not this one line.
            String RM = (rm.isRegional() || rm.isGlobal()) ? "1" : "0";
            String GM = "0"; // reserved positional bit only (see note above); never set to 1

            String RXre = "0";
            String RXr = "0";
            String RXae = "0";
            String RXa = "0";
            String RXp = "0";
            String RXpe = "0";


            String TXr = "0";
            String TXre = "0";
            String TXa = "0";
            String TXae = "0";
            String TXp = "0";
            String TXpe = "0";


            if (rm.getDstRegion() != null) {
                RXre = "1";
                if (rm.getDstRegion().equals(/*PluginEngine.region*/plugin.getRegion())) {
                    RXr = "1";
                }
            }

            if (rm.getDstAgent() != null) {
                RXae = "1";
                if (rm.getDstAgent().equals(/*PluginEngine.agent*/plugin.getAgent())) {
                    RXa = "1";
                }
            }

            if (rm.getDstPlugin() != null) {
                RXpe = "1";
                if (rm.getDstPlugin().equals(/*PluginEngine.agentcontroller*/plugin.getPluginID())) {
                    RXp = "1";
                }
            }

            if (rm.getSrcRegion() != null) {
                TXre = "1";
                if (rm.getSrcRegion().equals(/*PluginEngine.region*/plugin.getRegion())) {
                    TXr = "1";
                }
            }
            if (rm.getSrcAgent() != null) {
                TXae = "1";
                if (rm.getSrcAgent().equals(/*PluginEngine.agent*/plugin.getAgent())) {
                    TXa = "1";
                }
            }
            if ( rm.getSrcPlugin() != null) {
                TXpe = "1";
                if ( rm.getSrcPlugin().equals(/*PluginEngine.agentcontroller*/plugin.getPluginID())) {
                    TXp = "1";
                }
            }

            // 001011 10 11 11
            String routeString = GM + RM + GC + RC + TXp + RXp + TXa + RXa + TXr + RXr + TXpe + RXpe + TXae + RXae + TXre + RXre;
            routePath = Integer.parseInt(routeString, 2);
            //System.out.println("desc:" + rm.getParam("desc") + "\nroutePath:" + routePath + " RouteString:\n" + routeString + "\n" + rm.getParams());
        } catch (Exception ex) {
            if(rm != null) {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " " + rm.getParams().toString());
            } else {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " RM=NULL");
            }
            logger.error("MsgRouter.getRoutePath getRoutePath failure", ex);
            routePath = -1;
        }
        //System.out.println("REGIONAL CONTROLLER ROUTEPATH=" + routePath + " MsgType=" + rm.getMsgType() + " Params=" + rm.getParams());

        return routePath;
    }

    private MsgEvent getTTL(MsgEvent rm) {

        boolean isValid = true;
        try {
            if (rm.getParam("ttl") != null) {
                int ttlCount = Integer.valueOf(rm.getParam("ttl"));

                if (ttlCount > 10) {
                    logger.error("**Controller : MsgRoute : High Loop Count**");
                    logger.error("MsgType=" + rm.getMsgType().toString());
                    logger.error("params=" + rm.getParams());
                    isValid = false;
                }

                ttlCount++;
                rm.setParam("ttl", String.valueOf(ttlCount));
            } else {
                rm.setParam("ttl", "0");
            }
        } catch (Exception ex) {
            isValid = false;
        }
        if(isValid) {
            return rm;
        } else {
            return null;
        }

    }

    /*

    private void forwardToLocalAgent(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-agent")) {
                try {
                    controllerEngine.getPluginBuilder().msgIn(rm);
                    isOk = true;
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalAgent() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalPlugin(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-plugin")) {
                try {
                    controllerEngine.getPluginAdmin().msgIn(rm);
                    isOk = true;
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalPlugin() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalRegionalController(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-rc")) {
                controllerEngine.getRegionHealthWatcher().sendRegionalMsg(rm);
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalRegionalController() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToRemoteRegionalController(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-rc")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegionalController() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalRegion(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }

    }

    private void forwardToRemoteRegion(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalGlobal() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }

    }

    private void forwardToRemoteGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteGlobal(rm) BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

     */


}
