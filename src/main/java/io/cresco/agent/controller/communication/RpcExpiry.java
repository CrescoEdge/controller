package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;

/**
 * How long a broker keeps an RPC request: as long as its caller waits for the answer, and no longer.
 *
 * <p>Every message left with a JMS time to live of five minutes ({@code controlplane_ttl}, {@code activeproducerworker_ttl}),
 * whatever it was. A request that found no consumer on its way (its node's connection or a bridge was down) waited in a
 * broker's queue and was delivered when the consumer came back -- to be run, and answered, long after its caller had given
 * up (a ping of a 5 s timeout delivered 30 s later, a configuration request up to five minutes later).
 *
 * <p>A request now says how long its caller waits: the parameter {@value #TIMEOUT_PARAM} (milliseconds; the name the
 * wsapi's clients already use for the same thing). The controller sets it on its own timed calls ({@link #call}); a
 * request without it is sent as before. A request that carries it leaves with a time to live of that timeout plus the
 * clock margin, never more than the five minutes it had.
 *
 * <p><b>Clocks: a margin, not a broker's own stamp.</b> A JMS expiry is a time of day written by the sender's clock and
 * compared with the clock of whoever holds the message: every broker on its way, and the consumer's client library.
 * Nodes may be {@code msg_signature_max_future_ms} (60 s) apart -- the signing rule accepts that -- so an expiry of
 * exactly the timeout would make a broker 10 s ahead discard every 5 s ping the moment it arrives. The margin is that
 * allowance: with clocks within it no request is discarded while its caller still waits, on any mix of builds. The price is
 * that a request may still be delivered up to the margin (plus the difference of the clocks) after its caller gave up,
 * instead of up to five minutes. A stamp by each broker's own clock would be exact, but only where every node reads it:
 * a node of an earlier build (a broker, or a consumer's client library, which checks the expiry by its own clock too)
 * would judge the exact stamp by its clock and discard live requests during a rolling update. What waits longest never
 * reaches a queue at all: a controller that has no route answers or drops the request itself ({@link RouteGate}).
 */
public final class RpcExpiry {

    private RpcExpiry() { }

    /** The request's parameter: how long its caller waits for the answer, in milliseconds. */
    public static final String TIMEOUT_PARAM = "rpc_timeout_ms";
    /** The margin's own setting; unset, it is the clock difference the signing rule accepts. */
    static final String MARGIN_KEY = "rpc_expiry_margin_ms";
    static final long DEFAULT_MARGIN_MS = 60_000L;

    /** {@code plugin.sendRPC(request, timeoutMs)}, the request saying that its caller waits that long. */
    public static MsgEvent call(PluginBuilder plugin, MsgEvent request, long timeoutMs) {
        if (request != null && timeoutMs > 0) request.setParam(TIMEOUT_PARAM, Long.toString(timeoutMs));
        return plugin.sendRPC(request, timeoutMs);
    }

    /** The margin added to a request's timeout: {@value #MARGIN_KEY}, else the signing rule's clock allowance. */
    public static long marginMs(io.cresco.library.plugin.Config config) {
        long allowed = Math.max(1_000L, config.getLongParam("msg_signature_max_future_ms", DEFAULT_MARGIN_MS));
        return Math.max(0L, config.getLongParam(MARGIN_KEY, allowed));
    }

    /** The timeout an RPC request states, in milliseconds; 0 when {@code m} is no RPC request or states none (or nonsense). */
    public static long statedTimeoutMs(MsgEvent m) {
        if (m == null) return 0L;
        String v = m.getParam(TIMEOUT_PARAM);
        if (v == null || !MsgRouter.isRpcRequest(m)) return 0L;
        try {
            long t = Long.parseLong(v.trim());
            return t > 0 ? t : 0L;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * The JMS time to live {@code m} is sent with: {@code usualMs} (what every message had) unless it is an RPC request
     * that states its caller's timeout -- then that timeout plus {@code marginMs}, and never more than {@code usualMs}
     * (0 there means "for ever": then the request's own).
     */
    public static long ttlMs(MsgEvent m, long usualMs, long marginMs) {
        long timeout = statedTimeoutMs(m);
        if (timeout <= 0L) return usualMs;
        long own = timeout + Math.max(0L, marginMs);
        if (own <= 0L) return usualMs;                                       // overflow: a timeout no caller waits out
        return usualMs > 0L ? Math.min(usualMs, own) : own;
    }
}
