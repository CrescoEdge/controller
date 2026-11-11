package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * What became of the control-plane messages (MsgEvents) this node had in hand, counted: the ones it originated, carried on
 * for another node, delivered, held a moment, and dropped -- every drop under the cause a person would act on. With the
 * refusals {@link MsgSigning} counts by reason, a node's messages are accounted for:
 *
 * <pre>   originated (summed over the nodes of a quiet fabric) = delivered + refused + dropped (summed over them)</pre>
 *
 * <ul>
 *   <li><b>originated</b>: stamped with its caller on this node (the controller's own send, a plugin's, a send on behalf
 *       of an API user) -- every message a node sends, a reply and an error reply included, is one. A reply handed
 *       straight to a call waiting on the same node is no message (neither originated nor delivered);</li>
 *   <li><b>delivered</b>: handed to this node's controller (its agent, regional or global executor), to one of its
 *       plugins, or to a call waiting for it as its reply. What the recipient then makes of it (an action it does not
 *       know, a caller it does not authorize: answered 403 and counted by the action tables) is the recipient's;</li>
 *   <li><b>refused</b>: failed its authentication on this node ({@link MsgSigning#refusedCount()});</li>
 *   <li><b>dropped</b>: ended on this node without reaching its recipient ({@link Drop});</li>
 *   <li><b>relayed</b> and <b>held</b> are not ends: a message this node took off the wire and sent on toward another
 *       node, and one its inbox held until it could judge it (which is then delivered, refused or dropped).</li>
 * </ul>
 *
 * A message a broker loses between two nodes (it expired, a bridge may not carry it, its queue has no consumer) ends on no
 * node: the sum then falls short by it, which is what the identity is for.
 *
 * <p>One per controller. A count is a {@link LongAdder} increment: O(1), nothing allocated. A drop is logged at its
 * cause's first occurrence, then at most once per {@value #LINE_EVERY_MS} ms per cause with that cause's count; the line
 * is built only then.
 */
public final class MsgCounts {

    /** After its first occurrence, one cause of drops is logged at most this often. */
    static final long LINE_EVERY_MS = 10_000L;

    /** Why a message ended on this node without reaching its recipient: one cause per thing a person would look at. */
    public enum Drop {
        /** MsgRouter: routed more than {@link MsgRouter#TTL_MAX} times, or with a count no router writes. */
        TTL("ttl", "a message routed too many times, or with a ttl no router writes (a loop)"),
        /** The router, the regional dispatch or the engine failed on it (an exception; the stack is logged where it happened). */
        ROUTE_ERROR("route_error", "a message this node failed to route"),
        /** A controller function sent to a tier that does not serve it ({@link ControllerTiers#refusal}). */
        TIER_REFUSED("tier_refused", "a controller function sent to the wrong tier"),
        /** A caller's message travelling in another tenant's namespace ({@link MsgRouter#tenantRefusal}). */
        CROSS_TENANT("cross_tenant", "a message of one tenant's caller in another tenant's namespace"),
        /** An RPC request of another tenant's caller whose source is not the node that verified as its sender (MsgRouter.senderRefusal). */
        SENDER_UNBOUND("sender_unbound", "an RPC request of another tenant's caller that names another node as its sender"),
        /** For a plugin of this node while the node has no plugin admin (it is starting or stopping). */
        PLUGIN_ADMIN_UNAVAILABLE("plugin_admin_unavailable", "a message for a plugin while this node's plugin admin is not available"),
        /** For a plugin this node does not run. */
        PLUGIN_UNKNOWN("plugin_unknown", "a message for a plugin this node does not run"),
        /** For a plugin that is not active (not started yet, or being removed). */
        PLUGIN_INACTIVE("plugin_inactive", "a message for a plugin that is not active (MESSAGE SENT TO INACTIVE PLUGIN)"),
        /** For this node's regional controller, which was being (re)built and was not there within route_ready_wait_ms. */
        CONTROLLER_NOT_READY("controller_not_ready", "a message for this node's regional controller, which was not ready in time"),
        /** For this node's regional controller while the controller is shutting down. */
        CONTROLLER_STOPPING("controller_stopping", "a message for this node's regional controller, which is shutting down"),
        /** For the global, at a region that has joined none. */
        NO_GLOBAL("no_global", "a message for the global while this region has joined none"),
        /** For this agent's regional controller while the agent knows none (between a parent's loss and the next join). */
        PARENT_UNKNOWN("parent_unknown", "a message for this node's regional controller while it knows none"),
        /** Source-routed on through this node, which relays no source routes (net_source_routing is off). */
        SOURCE_ROUTE_NOT_RELAYED("source_route_not_relayed", "a source-routed message this node does not relay"),
        /** Waited msgin_fault_wait_ms for the link to the parent's broker, which stayed down. */
        PARENT_LINK_DOWN("parent_link_down", "a message held for the parent link, which stayed down until the wait ended or the sender gave up"),
        /** An RPC request for a node this node's broker has no way to ({@link RouteGate}): answered at once, never sent. */
        NO_ROUTE("no_route", "an RPC request for a node this node's broker has no way to (nothing consumes its inbox here)"),
        /** This node has no producer (or broker client) now: its link is being made anew. */
        NO_SENDER("no_sender", "a message to send while this node has no producer (AgentProducer is null. Message not sent)"),
        /** Behind messages this node holds to be sent first, and the hold has no room: sending it now would overtake them. */
        HOLD_FULL("hold_full", "a message to send behind those this node holds to be sent first, with no room left to hold it"),
        /** Held while this node had no link, for the controller it had then; it has joined another since. */
        PARENT_CHANGED("parent_changed", "a message held for the controller this node had, which it has left for another"),
        /** A message to send that names no destination. */
        NO_DESTINATION("no_destination", "a message to send that names no destination"),
        /** Every attempt to hand it to the broker failed. */
        SEND_FAILED("send_failed", "a message the broker did not take after every attempt"),
        /** At this node's regional or global executor, addressed to another node. */
        MISROUTED("misrouted", "a message at this node's controller that is addressed to another node"),
        /** The inbox could not read it (not a MsgEvent) or failed on it. */
        INBOX_ERROR("inbox_error", "a message this node's inbox could not read or failed on"),
        /** A message with files whose file group this node did not register (the group id is another caller's, or no room). */
        FILE_GROUP_REFUSED("file_group_refused", "a message with files whose file group was not registered"),
        /** A message with files whose parts did not all arrive: its group was abandoned and expired. */
        FILE_GROUP_EXPIRED("file_group_expired", "a message with files whose parts never all arrived (its group expired)"),
        /** Held by the inbox (it could not be judged yet) when the inbox closed. */
        HOLD_CLOSED("hold_closed", "a message the inbox held when it closed"),
        /** Taken off the wire as this node stopped: its routing pool takes nothing more. */
        NODE_STOPPING("node_stopping", "a message taken off the wire as this node stops"),
        /** A bundle's send as a plugin it does not run: never stamped, so counted as originated where it is dropped. */
        SENDER_NOT_PLUGIN("sender_not_plugin", "a send by a bundle as a plugin it does not run"),
        /**
         * A file part this node did not take (not from its group's caller, for no group it holds, not for this node, or it
         * failed): not a MsgEvent, so counted by itself and never in {@link #droppedCount()}.
         */
        FILE_PART("file_part", "a file part this node did not take", "msg.filepart.dropped");

        private final String key, what, metric;
        private final boolean message;

        Drop(String key, String what) {
            this.key = key; this.what = what; this.metric = "msg.dropped." + key; this.message = true;
        }

        Drop(String key, String what, String metric) {
            this.key = key; this.what = what; this.metric = metric; this.message = false;
        }

        /** The cause's stable name: in the log line, and the last part of its metric's name. */
        public String key() { return key; }

        /** The cause in words, as the log line says it. */
        public String what() { return what; }

        /** The cause's meter in the metric inventory. */
        public String metric() { return metric; }

        /** A control-plane message's end (counted in the node's total), not something else an inbox takes (a file part). */
        public boolean ofMessage() { return message; }
    }

    private static final Drop[] DROPS = Drop.values();

    private final LongAdder originated = new LongAdder(), relayed = new LongAdder(), delivered = new LongAdder(),
            held = new LongAdder(), notHeld = new LongAdder();
    private final KindTally drops;
    private final CLogger logger;
    /** Tests: every drop line this node logs is handed here as well. */
    volatile Consumer<String> lineTap;

    public MsgCounts(CLogger logger) {
        this(logger, new KindTally(DROPS.length, LINE_EVERY_MS));
    }

    MsgCounts(CLogger logger, KindTally drops) {
        this.logger = logger;
        this.drops = drops;
    }

    /** This node stamped a message as its own (its controller's, a plugin's, an API user's): it originates it. */
    public void originated() { originated.increment(); }

    /** A message this node took off the wire went on toward another node. */
    public void relayed() { relayed.increment(); }

    /**
     * A message counted as relayed when it was held to be sent did not go on after all (it is counted as dropped, under
     * its cause): it ends in ONE count, not two.
     */
    public void notRelayed() { relayed.decrement(); }

    /** A message was handed to its recipient on this node. */
    public void delivered() { delivered.increment(); }

    /** The inbox holds a message it cannot judge yet (counted once per message, however often it is held again). */
    public void held() { held.increment(); }

    /** The inbox could not hold a message it cannot judge yet (no room, in all or for its sender): it is judged as it stands. */
    public void notHeld() { notHeld.increment(); }

    /** {@code m} ends here without reaching its recipient, for {@code cause}. */
    public void dropped(Drop cause, MsgEvent m) {
        if (drops.count(cause.ordinal())) line(cause, m, null, null);
    }

    /**
     * As {@link #dropped(Drop, MsgEvent)} for {@code n} messages at once, or for what is no MsgEvent in hand: {@code what}
     * (a constant: nothing is built for a drop that is not logged) says which.
     */
    public void dropped(Drop cause, long n, String what) {
        if (n > 0 && drops.add(cause.ordinal(), n)) line(cause, null, what, null);
    }

    /** As {@link #dropped(Drop, long, String)}; {@code name} (a group id, off the wire) follows {@code what} in the line. */
    public void dropped(Drop cause, long n, String what, String name) {
        if (n > 0 && drops.add(cause.ordinal(), n)) line(cause, null, what, name);
    }

    private final LongAdder tenantUnresolved = new LongAdder();
    private volatile long lastUnresolvedLine;

    /**
     * A superuser tenant's node originated a message for a node of {@code region} without knowing that region's tenant: no
     * controller of it completed discovery with this node, or ({@code recordedTenants}, e.g. {@code "cresco-system, ta"})
     * controllers of several tenants are recorded for it. The message goes out in this node's own namespace, which
     * controllers read and no tenant's leaf does -- it is not ended here, so it is counted by itself
     * ({@code msg.tenant.unresolved}), never as a drop, and logged at most every 10 s.
     */
    public void tenantUnresolved(String region, String recordedTenants) {
        tenantUnresolved.increment();
        long now = System.currentTimeMillis();
        if (now - lastUnresolvedLine < LINE_EVERY_MS && now >= lastUnresolvedLine) return;
        lastUnresolvedLine = now;
        String line = "TENANT UNRESOLVED: a message for a node of region " + ParamLog.safeName(region) + " is sent in this node's own namespace: "
                + (recordedTenants == null ? "no controller of that region completed discovery with this node"
                        : "controllers of tenants " + recordedTenants + " are recorded for that region, and a region's names are one tenant's"
                                + " (revoke the one that does not belong: peer_bridge_revoke)")
                + "; a tenant's leaf does not read it there. An operator's client can name the tenant (cresco_tenant). ("
                + tenantUnresolved.sum() + " so far; logged at most every " + LINE_EVERY_MS / 1000L + " s)";
        try {
            if (logger != null) logger.warn(line);
        } catch (RuntimeException ignore) {
            // a count is never lost to its log line
        }
        Consumer<String> tap = lineTap;
        if (tap != null) tap.accept(line);
    }

    public long tenantUnresolvedCount() { return tenantUnresolved.sum(); }

    public long originatedCount() { return originated.sum(); }
    public long relayedCount() { return relayed.sum(); }
    public long deliveredCount() { return delivered.sum(); }
    public long heldCount() { return held.sum(); }
    public long notHeldCount() { return notHeld.sum(); }

    /** Messages dropped on this node so far, whatever the cause. */
    public long droppedCount() {
        long n = 0;
        for (Drop d : DROPS) if (d.message) n += drops.of(d.ordinal());
        return n;
    }

    /** Dropped for {@code cause} so far. */
    public long droppedCount(Drop cause) { return drops.of(cause.ordinal()); }

    /**
     * {@code MESSAGE DROPPED: <cause in words> [<type> <action>] <source> -> <destination> (<N> dropped so far; <K> of this
     * kind, <cause>; each kind is logged at most every 10 s)}: built only when the cause's line is due. What the
     * message says of itself is written as a name or by its length ({@link ParamLog}): a sender does not write this
     * node's log.
     */
    private void line(Drop cause, MsgEvent m, String what, String name) {
        StringBuilder b = new StringBuilder(200).append("MESSAGE DROPPED: ").append(cause.what);
        if (m != null) {
            b.append(" [").append(m.getMsgType()).append(' ').append(ParamLog.safeAction(m.getParams() == null ? null : m.getParam("action"))).append("] ")
                    .append(ParamLog.safeName(m.getSrcRegion())).append('_').append(ParamLog.safeName(m.getSrcAgent())).append('/')
                    .append(ParamLog.safeName(m.getSrcPlugin())).append(" -> ")
                    .append(ParamLog.safeName(m.getDstRegion())).append('_').append(ParamLog.safeName(m.getDstAgent())).append('/')
                    .append(ParamLog.safeName(m.getDstPlugin()));
        } else if (what != null) {
            b.append(" [").append(what);
            if (name != null) b.append(' ').append(ParamLog.safeName(name));
            b.append(']');
        }
        b.append(" (").append(droppedCount()).append(" dropped so far; ").append(drops.of(cause.ordinal())).append(" of this kind, ")
                .append(cause.key).append("; each kind is logged at most every ").append(LINE_EVERY_MS / 1000L).append(" s)");
        String line = b.toString();
        try {
            if (logger != null) logger.warn(line);
        } catch (RuntimeException ignore) {
            // a count is never lost to its log line
        }
        Consumer<String> tap = lineTap;
        if (tap != null) tap.accept(line);
    }
}
