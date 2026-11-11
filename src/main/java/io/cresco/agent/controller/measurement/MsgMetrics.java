package io.cresco.agent.controller.measurement;

import io.cresco.agent.controller.communication.ActiveBroker;
import io.cresco.agent.controller.communication.CrescoAuthorizationBroker;
import io.cresco.agent.controller.communication.MsgCounts;
import io.cresco.agent.controller.communication.MsgSigning;
import io.cresco.agent.controller.core.ControllerEngine;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

/**
 * What became of this node's control-plane messages, and what its broker stripped, withheld or discarded, as meters in
 * the unified metric inventory ({@code getmetricinventory}): until now a refusal or a drop was a log line at best, and
 * nothing a test or an operator could put a number on.
 *
 * <p>The inventory names a meter by its name alone (a tagged family shows as one arbitrary member), so the reason of a
 * refusal and the cause of a drop are the last part of the name, not a tag:
 * <ul>
 *   <li>group {@value #GROUP}: {@code msg.originated}, {@code msg.relayed}, {@code msg.delivered}, {@code msg.held},
 *       {@code msg.hold.full}, {@code msg.send.held}; {@code msg.session.lost.told}, {@code msg.session.lost.heard},
 *       {@code msg.session.lost.retried}; {@code msg.refused} (every message this node refused) and {@code msg.refused.<reason>}
 *       ({@link MsgSigning.Refusal}); {@code msg.dropped} (every message dropped here) and {@code msg.dropped.<cause>}
 *       ({@link MsgCounts.Drop}). Three counts are not messages this node received and are in neither total:
 *       {@code msg.sign.failed}, {@code msg.trust.refused}, {@code msg.filepart.dropped};</li>
 *   <li>group {@value #BROKER_GROUP}: {@code broker.caller.stripped}, {@code broker.bridge.withheld},
 *       {@code broker.bridge.discarded}, {@code broker.anonymous.discarded}, {@code broker.recipient.withheld},
 *       {@code broker.quarantine.connections} (a gauge: now).</li>
 * </ul>
 * On a quiet fabric, summed over its nodes: {@code msg.originated = msg.delivered + msg.refused + msg.dropped}
 * ({@link MsgCounts}).
 *
 * <p>Every meter reads a count that is kept anyway (a function counter): registering them costs the message paths nothing.
 */
public final class MsgMetrics {

    private MsgMetrics() { }

    public static final String GROUP = "msg", BROKER_GROUP = "broker";

    /** Registers every meter on {@code registry}; returns each meter's name with its inventory group, in registration order. */
    public static Map<String, String> register(MeterRegistry registry, ControllerEngine ce) {
        Map<String, String> names = new LinkedHashMap<>();
        count(registry, names, GROUP, "msg.originated", "Messages this node originated (stamped as its controller's, a plugin's or an API user's).",
                ce, e -> e.msgCounts().originatedCount());
        count(registry, names, GROUP, "msg.relayed", "Messages this node took off the wire and sent on toward another node.",
                ce, e -> e.msgCounts().relayedCount());
        count(registry, names, GROUP, "msg.delivered", "Messages handed to their recipient on this node (its controller, a plugin, or the call waiting for a reply).",
                ce, e -> e.msgCounts().deliveredCount());
        count(registry, names, GROUP, "msg.held", "Messages this node's inbox held until it could judge them (then delivered, refused or dropped).",
                ce, e -> e.msgCounts().heldCount());
        count(registry, names, GROUP, "msg.hold.full", "Messages this node's inbox could not hold (no room): judged as they stood.",
                ce, e -> e.msgCounts().notHeldCount());

        count(registry, names, GROUP, "msg.refused", "Messages this node received and refused (their authentication failed), whatever the reason.",
                ce, e -> e.msgSigning().refusedCount());
        for (MsgSigning.Refusal r : MsgSigning.Refusal.values()) {
            count(registry, names, GROUP, r.metric(), r.ofMessage()
                            ? "Messages this node refused: " + r.key() + "."
                            : (r == MsgSigning.Refusal.CANNOT_SIGN
                                    ? "Messages this node could not sign: they left unsigned and are refused where they arrive."
                                    : "Trust data (a trust bundle, the signing data on a ping or pong) this node did not take."),
                    ce, e -> e.msgSigning().refusedCount(r));
        }

        // a lost session (MsgSigning.LOST): what this node told its peers, what it took from them, what it sent again
        count(registry, names, GROUP, "msg.session.lost.told", "Times this node told a sender, on a signed message, that it does not hold"
                        + " the session a message came under: in the 401 of a request, or in a notice of its own (one a second per peer).",
                ce, e -> e.msgSigning().lostToldCount());
        count(registry, names, GROUP, "msg.session.lost.heard", "Sessions this node dropped because the peer said, under its signature,"
                        + " that it does not hold them: this node signs to that peer again at once.",
                ce, e -> e.msgSigning().lostHeardCount());
        count(registry, names, GROUP, "msg.session.lost.retried", "RPC requests this node sent again, signed, after the peer refused them"
                        + " for a session it does not hold: their callers saw no refusal.",
                ce, e -> e.msgSigning().lostRetriedCount());
        count(registry, names, GROUP, "msg.send.held", "Messages held because this node had no producer yet (its first moments, or its link"
                        + " being made anew): sent when it had one, else counted under msg.dropped.no_sender.",
                ce, e -> e.getActiveClient() == null ? 0 : e.getActiveClient().sendsHeldCount());

        count(registry, names, GROUP, "msg.dropped", "Messages that ended on this node without reaching their recipient, whatever the cause.",
                ce, e -> e.msgCounts().droppedCount());
        for (MsgCounts.Drop d : MsgCounts.Drop.values()) {
            count(registry, names, GROUP, d.metric(), "Dropped on this node: " + d.what() + ".", ce, e -> e.msgCounts().droppedCount(d));
        }

        count(registry, names, GROUP, "msg.tenant.unresolved", "Messages a superuser tenant's node sent in its own namespace because it"
                        + " does not know the tenant of the region they are for (a tenant's leaf does not read them there).",
                ce, e -> e.msgCounts().tenantUnresolvedCount());

        // the broker's own counts: read through the engine, and kept across a broker this node makes anew
        broker(registry, names, "broker.caller.stripped", "Caller stamps this node's broker stripped (forged, or carried by a bridge that may not carry them).",
                ce, CrescoAuthorizationBroker::forgedStampCount);
        broker(registry, names, "broker.bridge.withheld", "Messages a bridge demanded and this node's broker did not give it.",
                ce, CrescoAuthorizationBroker::bridgeWithheldCount);
        broker(registry, names, "broker.bridge.discarded", "Messages a bridge brought in for a destination its peer may not write: discarded.",
                ce, CrescoAuthorizationBroker::bridgeDiscardedCount);
        broker(registry, names, "broker.anonymous.discarded", "Messages sent by connections without a client certificate: discarded.",
                ce, CrescoAuthorizationBroker::discardedCount);
        broker(registry, names, "broker.recipient.withheld", "Messages this node's broker withheld from a consumer that is not their recipient.",
                ce, CrescoAuthorizationBroker::withheldCount);
        Gauge.builder("broker.quarantine.connections", ce, e -> {
                    CrescoAuthorizationBroker a = authorization(e);
                    return a == null ? 0 : a.quarantinedConnectionCount();
                })
                .description("Connections without a verified identity up now on this node's broker (they receive nothing; their sends are discarded).")
                .strongReference(true).register(registry);
        names.put("broker.quarantine.connections", BROKER_GROUP);
        return names;
    }

    private static void count(MeterRegistry registry, Map<String, String> names, String group, String name, String description,
                              ControllerEngine ce, ToDoubleFunction<ControllerEngine> f) {
        FunctionCounter.builder(name, ce, f).description(description).register(registry);
        names.put(name, group);
    }

    private static void broker(MeterRegistry registry, Map<String, String> names, String name, String description,
                               ControllerEngine ce, ToLongFunction<CrescoAuthorizationBroker> f) {
        BrokerCount c = new BrokerCount(f);
        count(registry, names, BROKER_GROUP, name, description, ce, e -> c.read(authorization(e)));
    }

    private static CrescoAuthorizationBroker authorization(ControllerEngine ce) {
        try {
            ActiveBroker b = ce.getBroker();
            return b == null ? null : b.authorization();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * One count of this node's broker. A node may stop its broker and make another (a region that starts over), whose
     * counts begin at zero: what the brokers before had counted when they were last read is kept, so the meter never goes
     * back. (What a broker counted between its last reading and its end is not seen.)
     */
    static final class BrokerCount {
        private final ToLongFunction<CrescoAuthorizationBroker> f;
        private CrescoAuthorizationBroker last;
        private long lastValue, before;

        BrokerCount(ToLongFunction<CrescoAuthorizationBroker> f) { this.f = f; }

        synchronized long read(CrescoAuthorizationBroker now) {
            if (now != last) {
                before += lastValue;
                lastValue = 0L;
                last = now;
            }
            if (now != null) lastValue = f.applyAsLong(now);
            return before + lastValue;
        }
    }
}
