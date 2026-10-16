package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Which controller tier serves each controller function -- the authoritative table behind controller-tier routing
 * (docs/controller-tier-routing.md renders it).
 *
 * <p>A controller message ({@code global_controller_msgevent} / {@code regional_controller_msgevent}: the regional
 * flag set, optionally the global flag, no destination plugin) names a FUNCTION (its {@code action}) and an
 * ADDRESS. The function decides the tier, the address only picks the node within a tier that has more than one:
 * <ul>
 *   <li>{@link Tier#GLOBAL}: needs the global DB / schedulers / registry / repo index. Runs on the real global
 *       controller, whatever node the message is addressed to and whichever flag it carries (a region id in the
 *       address is NOT a reason to run it on that region).</li>
 *   <li>{@link Tier#REGIONAL}: region membership and region-local placement; needs the region's DB and agents.
 *       Runs on the regional controller of the addressed region. Sent with the global flag it is refused with an
 *       error (the caller asked the global for a region's function; nothing guesses which region it meant).</li>
 *   <li>{@link Tier#EITHER}: both tiers serve it with their own scope (liveness, node inventories, listagents).
 *       The flag picks the tier: global flag = the global, regional flag = the addressed controller.</li>
 * </ul>
 * An action not in the table is not a controller function of either tier (an agent-tier action such as
 * pluginlist sent as a controller message): the flag decides, as for EITHER; the global answers an unknown action
 * with an error, a regional controller hands it to the addressed node's agent executor (as before).
 *
 * <p>Only CONFIG and EXEC carry actions. The other types keep their flag routing: WATCHDOG is the child-to-parent
 * keep-alive (regional flag from an agent, global flag from a region), KPI runs at the global (a region forwards it
 * when forward_global_kpi), INFO/DISCOVER/ERROR are no-ops at both controllers.
 */
public final class ControllerTiers {

    public enum Tier { GLOBAL, REGIONAL, EITHER }

    /** One table row: the tier and why that tier owns the function. */
    public static final class Entry {
        public final MsgEvent.Type type;
        public final String action;
        public final Tier tier;
        public final String why;
        Entry(MsgEvent.Type type, String action, Tier tier, String why) {
            this.type = type; this.action = action; this.tier = tier; this.why = why;
        }
    }

    private static final Map<String, Entry> TABLE = new LinkedHashMap<>();

    private static void row(MsgEvent.Type type, String action, Tier tier, String why) {
        TABLE.put(key(type, action), new Entry(type, action, tier, why));
    }

    private static String key(MsgEvent.Type type, String action) {
        return type + ":" + action;
    }

    static {
        final MsgEvent.Type C = MsgEvent.Type.CONFIG, E = MsgEvent.Type.EXEC;
        final Tier G = Tier.GLOBAL, R = Tier.REGIONAL, B = Tier.EITHER;
        // --- GLOBAL: the fabric registry, global DB, schedulers and repo index live only on the global
        row(C, "region_enable",      G, "region federation: registers a region in the global DB");
        row(C, "region_disable",     G, "region federation: unregisters a region from the global DB");
        row(C, "regionalimport",     G, "imports a region's state into the global DB");
        row(C, "addplugin",          G, "global resource scheduler queue (iNode placement)");
        row(C, "removeplugin",       G, "global resource scheduler queue (iNode removal)");
        row(C, "gpipelinesubmit",    G, "global application scheduler (cross-region pipelines)");
        row(C, "gpipelineremove",    G, "global application scheduler");
        row(C, "plugindownload",     G, "global plugin repository cache");
        row(C, "setinodestatus",     G, "iNode table of the global DB");
        row(C, "savetorepo",         G, "fabric-wide repo replication from the global's repo index");
        row(E, "listregions",        G, "region registry (global DB) + the global's bridges");
        row(E, "listplugins",        G, "fabric-wide plugin rows in the global DB");
        row(E, "listpluginsbytype",  G, "fabric-wide plugin rows in the global DB");
        row(E, "listpluginsrepo",    G, "fabric-wide repo index");
        row(E, "listrepoinstances",  G, "fabric-wide repo index");
        row(E, "plugininfo",         G, "plugin rows of any region (global DB)");
        row(E, "pluginkpi",          G, "KPI rows of any region (global DB)");
        row(E, "resourceinfo",       G, "resource aggregates of any region (global DB)");
        row(E, "netresourceinfo",    G, "fabric resource totals (global DB)");
        row(E, "resourceinventory",  G, "fabric resource totals (global DB)");
        row(E, "getenvstatus",       G, "environment index over every node (global DB)");
        row(E, "getinodestatus",     G, "iNode table of the global DB");
        row(E, "getisassignmentinfo",G, "iNode/resource assignment of the global scheduler");
        row(E, "getgpipeline",       G, "pipeline table of the global DB");
        row(E, "getgpipelineexport", G, "pipeline table of the global DB");
        row(E, "getgpipelinestatus", G, "pipeline table of the global DB");
        row(E, "plugininventory",    G, "the global node's plugin jar repository");
        // --- REGIONAL: region membership and region-local placement need the region's DB and its agents
        row(C, "agent_enable",       R, "region membership: registers an agent in the region DB");
        row(C, "agent_disable",      R, "region membership: unregisters an agent from the region DB");
        row(C, "rpipelinesubmit",    R, "region-local placement on the region's agents (escalates cross-region work itself)");
        // --- EITHER: both tiers answer with their own scope; the flag picks the tier
        row(E, "ping",               B, "parent liveness: an agent pings its region (regional), a region its global (global)");
        row(E, "listagents",         B, "global: any region from the global DB; regional: the region's own agents");
        row(E, "getmetricinventory", B, "the answering controller's inventory, fanned out by action_scope");
        row(E, "gethealthinventory", B, "the answering controller's health inventory");
        row(E, "getcapabilities",    B, "the answering tier's capability document");
        row(E, "getcapabilityinventory", B, "the answering controller's capability inventory, fanned out by action_scope");
        row(E, "getnetworkstate",    B, "the answering controller's view of the learned network graph");
        row(E, "getcoordinators",    B, "the answering controller's view of the coordinator set");
    }

    private ControllerTiers() { }

    /** The table, in rendering order (global, regional, either). */
    public static Map<String, Entry> table() {
        return Collections.unmodifiableMap(TABLE);
    }

    /** The tier serving {@code action} of {@code type}, or null when it is not a controller function of either tier. */
    public static Tier tierOf(MsgEvent.Type type, String action) {
        if (type == null || action == null) {
            return null;
        }
        Entry e = TABLE.get(key(type, action));
        return e == null ? null : e.tier;
    }

    public static Tier tierOf(MsgEvent m) {
        return tierOf(m.getMsgType(), m.getParam("action"));
    }

    /** A controller message: regional and/or global flag, no destination plugin. */
    public static boolean isControllerMessage(MsgEvent m) {
        return (m.isRegional() || m.isGlobal()) && m.getDstPlugin() == null;
    }

    /**
     * A reply travelling back to its caller (never re-classified): an error reply the router made, or an RPC reply,
     * i.e. it carries the call id of its destination (the caller) and not one of its source. A self-addressed RPC
     * request carries both and stays a request.
     */
    public static boolean isReply(MsgEvent m) {
        if (m.getParam(MsgRouter.ROUTE_UNAVAILABLE) != null || m.getParam(MsgRouter.ROUTE_REFUSED) != null) {
            return true;
        }
        boolean toCaller = m.getParam("callId-" + m.getDstRegion() + "-" + m.getDstAgent() + "-" + m.getDstPlugin()) != null;
        boolean fromCaller = m.getParam("callId-" + m.getSrcRegion() + "-" + m.getSrcAgent() + "-" + m.getSrcPlugin()) != null;
        return toCaller && !fromCaller;
    }

    /** A controller-function request the tier table applies to: a CONFIG/EXEC controller message that is not a reply. */
    public static boolean isTierRouted(MsgEvent m) {
        if (m.getMsgType() != MsgEvent.Type.CONFIG && m.getMsgType() != MsgEvent.Type.EXEC) {
            return false;
        }
        return isControllerMessage(m) && !isReply(m);
    }

    /**
     * True when {@code m} must run on the real global controller: a GLOBAL function (either flag), or a global-flagged
     * message whose function is not regional-only. Every other message keeps its flag (global flag = global).
     */
    public static boolean targetsGlobal(MsgEvent m) {
        if (!isTierRouted(m)) {
            return m.isGlobal();
        }
        Tier t = tierOf(m);
        if (t == Tier.GLOBAL) {
            return true;
        }
        if (t == Tier.REGIONAL) {
            return false;
        }
        return m.isGlobal();
    }

    /** Why {@code m} is refused (a regional-only function sent with the global flag), or null when it is not. */
    public static String refusal(MsgEvent m) {
        if (isTierRouted(m) && m.isGlobal() && tierOf(m) == Tier.REGIONAL) {
            return "'" + m.getParam("action") + "' is a regional-controller function; a global controller message is "
                    + "never run on a region. Send it as a regional controller message (regional_controller_msgevent) "
                    + "addressed to the region's controller";
        }
        return null;
    }
}
