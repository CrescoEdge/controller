package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;

import jakarta.jms.DeliveryMode;

/**
 * Quality-of-service classification for outbound MsgEvents. This is the single source of truth
 * for the traffic-priority hierarchy the fabric MUST honor so that a flood of one class can never
 * starve a higher one:
 *
 *   LIVENESS  > CONTROL > TELEMETRY > BULK
 *
 * "Contact between agents" (liveness ping/pong + watchdog) is the highest priority and is made
 * PERSISTENT so the broker's pending-message-limit strategy can never evict it, and JMS priority 9
 * so it is dispatched ahead of everything else. CONTROL (config/commands) is next. TELEMETRY
 * (info/kpi/log) is best-effort/non-persistent (evictable under memory pressure — acceptable loss).
 * BULK (files/binary data) keeps its existing PERSISTENT delivery (used as a flow-control mechanism)
 * but rides the lowest priority so it never delays the control plane.
 */
public final class MsgQoS {

    private MsgQoS() { }

    public enum Tier {
        LIVENESS(9, DeliveryMode.PERSISTENT),
        CONTROL(7, DeliveryMode.PERSISTENT),
        TELEMETRY(4, DeliveryMode.NON_PERSISTENT),
        BULK(1, DeliveryMode.PERSISTENT);

        public final int priority;
        public final int deliveryMode;

        Tier(int priority, int deliveryMode) {
            this.priority = priority;
            this.deliveryMode = deliveryMode;
        }

        /** Control plane = liveness + control. These get an isolated producer/session and never block. */
        public boolean isControlPlane() {
            return this == LIVENESS || this == CONTROL;
        }
    }

    /** Classify a MsgEvent into its QoS tier. Bulk (file) payloads are handled on a separate path. */
    public static Tier classify(MsgEvent me) {
        if (me == null || me.getMsgType() == null) return Tier.TELEMETRY;
        // The active liveness ping/pong travels as EXEC with action=ping/pong; it must be LIVENESS,
        // not lumped in with ordinary command EXECs.
        String action = me.getParam("action");
        return classify(me.getMsgType(), action);
    }

    /** Classify by message type (used by the dataplane, which carries no ping/pong action). */
    public static Tier classify(MsgEvent.Type type) {
        return classify(type, null);
    }

    public static Tier classify(MsgEvent.Type type, String action) {
        if (type == null) return Tier.TELEMETRY;
        switch (type.toString().toUpperCase()) {
            case "WATCHDOG":
                return Tier.LIVENESS;
            case "EXEC":
                if (action != null && (action.equalsIgnoreCase("ping") || action.equalsIgnoreCase("pong"))) {
                    return Tier.LIVENESS;
                }
                return Tier.CONTROL;
            case "CONFIG":
                return Tier.CONTROL;
            case "INFO":
            case "LOG":
            case "KPI":
            case "GC":
            case "DISCOVER":
            case "ERROR":
            default:
                return Tier.TELEMETRY;
        }
    }
}
