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


                Hop hop = decide(routePath, rm, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                logger.debug("route rp=" + routePath + " -> " + hop);
                logger.trace(rm.getParams().toString());

                switch (hop) {
                    case LOCAL_AGENT:                forwardToLocalAgent(rm); break;
                    case LOCAL_PLUGIN:               forwardToLocalPlugin(rm); break;
                    case LOCAL_REGIONAL_CONTROLLER:  forwardToLocalRegionalController(rm); break;
                    case REMOTE_REGIONAL_CONTROLLER: forwardToRemoteRegionalController(rm); break;
                    case LOCAL_REGION:               forwardToLocalRegion(rm); break;
                    case REMOTE_REGION:              forwardToRemoteRegion(rm); break;
                    case LOCAL_GLOBAL:               forwardToLocalGlobal(rm); break;
                    case REMOTE_GLOBAL:              forwardToRemoteGlobal(rm); break;
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
            routePath = routePath(rm, controllerEngine.cstate.isRegionalController(),
                    controllerEngine.cstate.isGlobalController(),
                    plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
        } catch (Exception ex) {
            if(rm != null) {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " " + rm.getParams().toString());
            } else {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " RM=NULL");
            }
            logger.error("MsgRouter.getRoutePath getRoutePath failure", ex);
            routePath = -1;
        }
        return routePath;
    }

    /**
     * The route code of {@code rm} as seen by the node (region, agent, pluginId) whose controller is a
     * regional controller and/or the global controller: one bit per header comparison, see routeString.
     */
    static int routePath(MsgEvent rm, boolean regionalController, boolean globalController,
                         String region, String agent, String pluginId) {
        String RC = "0";
        if (regionalController) {
            RC = "1";
        }

        String GC = "0";
        if (globalController) {
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
            if (rm.getDstRegion().equals(region)) {
                RXr = "1";
            }
        }

        if (rm.getDstAgent() != null) {
            RXae = "1";
            if (rm.getDstAgent().equals(agent)) {
                RXa = "1";
            }
        }

        if (rm.getDstPlugin() != null) {
            RXpe = "1";
            if (rm.getDstPlugin().equals(pluginId)) {
                RXp = "1";
            }
        }

        if (rm.getSrcRegion() != null) {
            TXre = "1";
            if (rm.getSrcRegion().equals(region)) {
                TXr = "1";
            }
        }
        if (rm.getSrcAgent() != null) {
            TXae = "1";
            if (rm.getSrcAgent().equals(agent)) {
                TXa = "1";
            }
        }
        if ( rm.getSrcPlugin() != null) {
            TXpe = "1";
            if ( rm.getSrcPlugin().equals(pluginId)) {
                TXp = "1";
            }
        }

        // 001011 10 11 11
        String routeString = GM + RM + GC + RC + TXp + RXp + TXa + RXa + TXr + RXr + TXpe + RXpe + TXae + RXae + TXre + RXre;
        return Integer.parseInt(routeString, 2);
    }

    /** Where {@link #route} hands a message: one value per forwardTo* method. */
    enum Hop {
        LOCAL_AGENT, LOCAL_PLUGIN, LOCAL_REGIONAL_CONTROLLER, REMOTE_REGIONAL_CONTROLLER,
        LOCAL_REGION, REMOTE_REGION, LOCAL_GLOBAL, REMOTE_GLOBAL
    }

    /**
     * The routing decision for {@code rm} with route code {@code routePath} at the node (region, agent,
     * pluginId). The static table names the hop patterns the fabric produces; every other code takes the
     * destination-resolved catch-all ({@link #catchAll}).
     */
    static Hop decide(int routePath, MsgEvent rm, String region, String agent, String pluginId) {
        switch (routePath) {
            case 335: return Hop.LOCAL_AGENT; // remote agent sending message to local agent
            case 351: return Hop.LOCAL_PLUGIN; // remote agent sending message to local plugin
            case 383: return Hop.LOCAL_PLUGIN; // remote agent sending message to local plugin
            case 463: return Hop.LOCAL_AGENT; // remote agent sending message to local agent
            case 495: return Hop.LOCAL_AGENT; // remote plugin sending message to local agent
            case 479: return Hop.LOCAL_PLUGIN; // remote agent sending message to local plugin
            case 511: return Hop.LOCAL_PLUGIN; // remote plugin sending message to local plugin
            case 655: return Hop.REMOTE_REGION; // Local agent sending message to remote global agent
            case 671: return Hop.REMOTE_REGION; // Local agent sending message to remote global agentcontroller
            case 687: return Hop.REMOTE_GLOBAL; // Local agentcontroller sending message to remote global agent
            case 703: return Hop.REMOTE_GLOBAL; // Local agentcontroller sending message to remote global agentcontroller
            case 719: return Hop.REMOTE_REGION; // Local agent sending message to remote regional agent
            case 735: return Hop.REMOTE_REGION; // Local agent sending message to remote regional agentcontroller
            case 751: return Hop.REMOTE_REGION; // Local agentcontroller sending message to remote regional agent
            case 767: return Hop.REMOTE_REGION; // Local agentcontroller sending message to remote regional agentcontroller
            case 991: return Hop.LOCAL_PLUGIN; // Local agent sending message to local agentcontroller
            case 975: return Hop.LOCAL_AGENT; // Local agent sending message to self
            case 1007: return Hop.LOCAL_AGENT; // Local agentcontroller sending message to local agent
            case 1023: return Hop.LOCAL_PLUGIN; // Local agentcontroller sending message to local agentcontroller
            case 4447: return Hop.LOCAL_PLUGIN; // Remote globalcontroller sending message to local agentcontroller plugin
            case 4431: return Hop.LOCAL_AGENT; // Remote region sending message to local agent
            case 4751: return Hop.REMOTE_REGION; // Local agentcontroller sending message to remote global agent
            case 4767: return Hop.REMOTE_REGION; // Local agentcontroller sending message to remote global agent
            case 4783: return Hop.REMOTE_GLOBAL; // Local agentcontroller sending message to remote global agent
            case 4799: return Hop.REMOTE_GLOBAL; // Local agentcontroller sending message to remote global agentcontroller
            case 4815: return Hop.LOCAL_REGION; // Local agent sending message to local regional agent
            case 4831: return Hop.LOCAL_REGION; // Local agent sending message to local regional agentcontroller
            case 4847: return Hop.LOCAL_REGION; // Local agentcontroller sending message to local regional agent
            case 4863: return Hop.LOCAL_REGION; // Local agentcontroller sending message to local regional agentcontroller
            case 5071: return Hop.LOCAL_AGENT; // Local agent sending message to self
            case 5087: return Hop.LOCAL_PLUGIN; // Local agent sending message to local agentcontroller
            case 5103: return Hop.LOCAL_AGENT; // Local agentcontroller sending message to local agent
            case 5119: return Hop.LOCAL_PLUGIN; // Local agentcontroller sending message to local agentcontroller
            case 12623: return Hop.LOCAL_AGENT; // Remote Global or Region sending message to local agent
            case 12639: return Hop.LOCAL_PLUGIN; // remote agent sending message to local agent plugin
            case 12655: return Hop.LOCAL_AGENT; // remote global agent sending message to local global agent
            case 12671: return Hop.LOCAL_PLUGIN; // remote agent sending message to local agent
            case 12751: return Hop.LOCAL_AGENT; // Remote agent sending message to local agent
            case 12767: return Hop.LOCAL_PLUGIN; // remote agent sending message to local plugin
            case 12799: return Hop.LOCAL_PLUGIN; // remote agent sending message to local plugin
            case 12943: return Hop.REMOTE_REGION; // Local region sending message to remote region
            case 12959: return Hop.LOCAL_GLOBAL; // Local agent sending message to local regional agentcontroller
            case 12975: return Hop.REMOTE_REGION; // Local agentcontroller sending message to local global agent
            case 12991: return Hop.LOCAL_GLOBAL; // Local agentcontroller sending message to local global agentcontroller
            case 13007: return Hop.LOCAL_REGION; // Local agent sending message to local regional agent
            case 13023: return Hop.LOCAL_REGION; // Local agent sending message to local regional agentcontroller
            case 13039: return Hop.LOCAL_REGION; // Local agentcontroller sending message to local regional agent
            case 13055: return Hop.LOCAL_REGION; // Local agentcontroller sending message to local regional agentcontroller
            case 13279: return Hop.LOCAL_PLUGIN; // Local agent sending message to local agentcontroller
            case 13311: return Hop.LOCAL_PLUGIN; // Local agentcontroller sending message to local agentcontroller
            case 13263: return Hop.LOCAL_AGENT; // Local agent sending message to self
            case 13295: return Hop.LOCAL_AGENT; // Local agentcontroller sending message to local agent
            case 16863: return Hop.LOCAL_PLUGIN; // remote region sending message to local agent controller
            case 17359: return Hop.REMOTE_REGIONAL_CONTROLLER; // Local agentcontroller sending message to remote regional or global controller
            case 17391: return Hop.REMOTE_REGIONAL_CONTROLLER; // Local agentcontroller sending message to remote regional or global controller
            case 20943: return Hop.LOCAL_REGIONAL_CONTROLLER; // remote agent sending message to local regional controller
            case 20975: return Hop.LOCAL_REGIONAL_CONTROLLER; // remote plugin sending message to local regional controller
            case 21135: return Hop.REMOTE_REGION; // Local region sending message to remote global controller
            case 21199: return Hop.LOCAL_REGION; // Local region sending message to local regional controller
            case 21455: return Hop.LOCAL_REGIONAL_CONTROLLER; // Local agent sending message to local regional or global controller
            case 21487: return Hop.LOCAL_REGIONAL_CONTROLLER; // Local agent controller sending message to local regional or remote global controller
            case 29007: return Hop.LOCAL_REGIONAL_CONTROLLER; // Remote regional controller sending message to local global controller
            case 29039: return Hop.LOCAL_REGIONAL_CONTROLLER; // Remote plugin sending message to local global controller
            case 29135: return Hop.LOCAL_REGIONAL_CONTROLLER; // Remote agent sending message to local regional controller
            case 29167: return Hop.LOCAL_REGIONAL_CONTROLLER; // Remote agent sending message to local global controller
            case 29327: return Hop.REMOTE_REGION; // Local global controller sending message to remote regional controller
            case 29391: return Hop.REMOTE_REGION; // Local agent sending message to remote agent
            case 29407: return Hop.LOCAL_REGION; // Local agent sending message to remote agent
            case 29647: return Hop.LOCAL_REGIONAL_CONTROLLER; // Local or remote agent sending message to local regional controller
            case 29663: return Hop.LOCAL_PLUGIN; // Local regional or local global controller sending message back to plugin
            case 29679: return Hop.LOCAL_REGIONAL_CONTROLLER; // Local agentcontroller sending message to local global controller
            // Remote controller (another region, or the global) -> THIS region's controller.
            // A cross-region controller<->controller RPC (e.g. the global's getmetricinventory /
            // listagents fan-out to a child region). ActiveMQ delivered it here because it is
            // addressed to this region's queue, so this is LOCAL delivery (not transit): hand it
            // to the regional controller executor, which processes the action and returns the RPC
            // reply. The strict-tree single-node builds never exercised this hop, so the static
            // table omitted it and the message fell to default-drop (silent RPC timeout).
            case 4463: return Hop.LOCAL_REGIONAL_CONTROLLER;
            // A REGIONAL-CONTROLLER message from another region (a plugin/wsapi there = 20847, a controller =
            // 20815) addressed to THIS region's controller: the region-side twin of 29039/29007 at the global
            // (same bits, GC=0). E.g. regional_controller_msgevent(region_id, agent_id) sent at the global's
            // wsapi for a remote region: the global relays it here; it must run on this node's
            // RegionalExecutor (rpipelinesubmit, agent_enable, ...), whose unknown CONFIG/EXEC actions still
            // fall back to the agent executor. The catch-all handed it straight to the AgentExecutor
            // ("Unknown configtype", RPC timeout). Local delivery only: sendRegionalMsg runs a non-global
            // message addressed to this node and never re-forwards it. Global-flagged messages keep the
            // catch-all (unchanged).
            case 20815:
            case 20847:
                if (rm.isRegional() && !rm.isGlobal()) return Hop.LOCAL_REGIONAL_CONTROLLER;
                return catchAll(rm, region, agent, pluginId);
            default: return catchAll(rm, region, agent, pluginId);
        }
    }

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
    static Hop catchAll(MsgEvent rm, String region, String agent, String pluginId) {
        boolean forThisNode = rm.getDstRegion() != null
                && rm.getDstRegion().equals(region)
                && rm.getDstAgent() != null
                && rm.getDstAgent().equals(agent);
        if (forThisNode && (rm.getDstPlugin() == null || rm.getDstPlugin().equals(pluginId))) {
            // Deliver to THIS node's own controller executor (pluginBuilder.msgIn -> executor),
            // not sendRegionalMsg (which forwards up to the regional controller). Using the
            // regional path here silently dropped agent-addressed RPCs (e.g. pluginlist) that
            // reach a leaf agent via the catch-all, so the reply never came back (RPC timeout).
            return Hop.LOCAL_AGENT;
        } else if (forThisNode && rm.getDstPlugin() != null) {
            return Hop.LOCAL_PLUGIN;
        } else {
            return Hop.REMOTE_REGION;
        }
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
