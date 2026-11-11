package io.cresco.agent.controller.communication;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Does this node's broker have a way to a node's inbox -- and what becomes of an RPC request while it has none.
 *
 * <p><b>What a node knows.</b> A message for another node is handed to this node's broker, addressed to that node's inbox
 * queue. The broker has a way to it exactly while something consumes that queue here: the node itself (an agent of this
 * region), or the demand of the bridge that leads to it (a region below this global, the global above this region, a
 * peer region). When the bridge to a region goes, its demand goes with it; when an agent's connection breaks, so does its
 * consumer. A request sent then waited in the queue for a consumer that did not come, and its caller waited out its
 * whole timeout. This class keeps, from the broker's own consumer events, the inbox queues that have a consumer now
 * (one map entry per subscribed inbox; a request costs at most four lookups) and when each lost its last one.
 *
 * <p><b>The rule</b> for an RPC request ({@link MsgRouter}) whose inbox has no consumer here. The grace is
 * {@code route_rebuild_grace_ms} (default {@value #DEFAULT_GRACE_MS} ms):
 * <ul>
 *   <li>this broker saw the inbox's last consumer leave longer ago than the grace: the node KNOWS there is no route, and
 *       the request is answered {@code route unavailable} at once;</li>
 *   <li>it saw it leave less than the grace ago -- a bridge or a node's connection that may be being made anew: the
 *       request is HELD here, not sent, until a consumer of that inbox is back (then it goes on at once) or the grace
 *       after the loss has passed (then it is answered);</li>
 *   <li>it never saw one leave (this broker never had that consumer: a node that never connected, a bridge that is just
 *       being built for the first time, everything after this broker's own restart): the request is held for one grace
 *       from its arrival, and answered when no consumer came -- the demand of a bridge reaches a broker a moment after
 *       the bridge itself, and a request that overtook it must not fail.</li>
 * </ul>
 * A request that states its caller's timeout ({@link RpcExpiry}) is held for at most half of it. So a caller never waits
 * longer than the grace for the answer that there is no route, and a route that is back within the grace costs no request.
 * A held request is never in the broker: once its caller has been told that there is no route, nothing can deliver it.
 * At most {@code route_hold_max} (default {@value #DEFAULT_HOLD_MAX}) requests are held; one more is answered at once.
 * Nobody sleeps: a held request is a map entry and one timer task, and the broker's consumer event releases it.
 *
 * <p><b>When it does not judge</b> (the request is sent as before): the node has no broker (an agent); the queue is not a
 * node's inbox ({@code [T.<tenant>.]<region>_<agent>}); or something consumes queues here by a pattern this class does not
 * read (anything but a controller's {@code T.*.<region>_<agent>}), which may match any inbox.
 */
public final class RouteGate {

    static final long DEFAULT_GRACE_MS = 1500L;
    static final int DEFAULT_HOLD_MAX = 1024;
    /** More inboxes than this that lost their consumer are not remembered (all are forgotten): each is then one this broker never saw leave. */
    static final int LOST_MAX = 50_000;

    private static final String OTHER_PATTERN = "";

    private final long graceMs;
    private final int holdMax;

    /** Inbox queue, by the name it is subscribed under (an inbox, or a controller's T.*.inbox) -> its consumers now. */
    private final ConcurrentHashMap<String, Integer> consumers = new ConcurrentHashMap<>();
    /** Each consumer counted above -> the name it was counted under ({@link #OTHER_PATTERN}: another pattern). */
    private final ConcurrentHashMap<ConsumerId, String> counted = new ConcurrentHashMap<>();
    /** Inbox queue (same names) -> when its last consumer left; there until a consumer is back. At most {@link #LOST_MAX}. */
    private final ConcurrentHashMap<String, Long> lostAt = new ConcurrentHashMap<>();
    private final AtomicInteger otherPatterns = new AtomicInteger();
    /** When the broker this gate reads was installed; 0: none (this node has no broker). */
    private volatile long boundAt;

    /** Node ({@code <region>_<agent>}) -> the requests held for its inbox. */
    private final ConcurrentHashMap<String, ArrayDeque<Held>> held = new ConcurrentHashMap<>();
    private final AtomicInteger heldNow = new AtomicInteger();
    private final LongAdder heldEver = new LongAdder();
    private volatile ScheduledThreadPoolExecutor timer;

    /** One held request: exactly one of {@code sendOn} (a consumer is back) and {@code giveUp} (the grace passed) runs. */
    private static final class Held {
        final Runnable sendOn, giveUp;
        final AtomicBoolean settled = new AtomicBoolean();
        Held(Runnable sendOn, Runnable giveUp) { this.sendOn = sendOn; this.giveUp = giveUp; }
    }

    public RouteGate(long graceMs, int holdMax) {
        this.graceMs = Math.max(0L, graceMs);
        this.holdMax = Math.max(0, holdMax);
    }

    public RouteGate(io.cresco.library.plugin.Config config) {
        this(config.getLongParam("route_rebuild_grace_ms", DEFAULT_GRACE_MS), config.getIntegerParam("route_hold_max", DEFAULT_HOLD_MAX));
    }

    public long graceMs() { return graceMs; }

    /** Requests held so far for a route that was being made anew (each then went on, or was answered that there is none). */
    public long heldCount() { return heldEver.sum(); }

    /** Requests held now. */
    public int heldNow() { return heldNow.get(); }

    /** Does this gate read a broker (a controller's own)? An agent's does not: it judges nothing. */
    public boolean active() { return boundAt != 0L; }

    // ---- the broker's consumer events ----

    /** The plugin that tells this gate of every consumer that joins or leaves the broker it is installed in. */
    public BrokerPlugin plugin() {
        return next -> {
            bound(System.currentTimeMillis());
            return new BrokerFilter(next) {
                @Override
                public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                    Subscription s = super.addConsumer(context, info);
                    if (info != null) consumerAdded(info.getConsumerId(), info.getDestination());
                    return s;
                }

                @Override
                public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                    try {
                        super.removeConsumer(context, info);
                    } finally {
                        if (info != null) consumerRemoved(info.getConsumerId(), System.currentTimeMillis());
                    }
                }
            };
        };
    }

    /** A new broker: nothing consumes anything on it yet, and what the last one had is gone. */
    void bound(long now) {
        consumers.clear();
        counted.clear();
        lostAt.clear();
        otherPatterns.set(0);
        boundAt = now == 0L ? 1L : now;
    }

    /** The broker is gone (the node stops being a controller): nothing is judged until the next one. */
    public void unbound() {
        boundAt = 0L;
    }

    void consumerAdded(ConsumerId id, ActiveMQDestination dest) {
        if (id == null || dest == null || !dest.isQueue() || dest.isTemporary()) return;
        String name = dest.isComposite() ? OTHER_PATTERN : keyOf(dest.getPhysicalName());
        if (counted.putIfAbsent(id, name) != null) return;                   // told twice of one consumer
        if (name.isEmpty()) {
            otherPatterns.incrementAndGet();
            return;
        }
        consumers.merge(name, 1, Integer::sum);                              // one step of the map: no count is lost to a
        lostAt.remove(name);                                                 // consumer leaving at the same moment
        if (!held.isEmpty()) {
            String node = CrescoAuthorizationBroker.inboxNode(dest);
            if (node != null && held.containsKey(node)) release(node);
        }
    }

    void consumerRemoved(ConsumerId id, long now) {
        if (id == null) return;
        String name = counted.remove(id);
        if (name == null) return;
        if (name.isEmpty()) {
            otherPatterns.decrementAndGet();
            return;
        }
        // the count and its removal at zero are one step of the map, so a consumer joining meanwhile is not lost
        boolean[] last = new boolean[1];
        consumers.computeIfPresent(name, (k, n) -> {
            if (n > 1) return n - 1;
            last[0] = true;
            return null;
        });
        if (last[0]) {
            if (lostAt.size() >= LOST_MAX) lostAt.clear();
            lostAt.put(name, now);
        }
    }

    /**
     * The name an inbox subscription is counted under: the queue's own name for a node's inbox ({@code <region>_<agent>},
     * {@code T.<tenant>.<region>_<agent>}) and for a controller's inbox in every tenant ({@code T.*.<region>_<agent>});
     * {@link #OTHER_PATTERN} for any other pattern (it may match any inbox). A queue that is no inbox is counted under its
     * name too: nothing asks for it.
     */
    static String keyOf(String physicalName) {
        if (physicalName == null) return OTHER_PATTERN;
        if (physicalName.indexOf('*') < 0 && physicalName.indexOf('>') < 0) return physicalName;
        String node = wildcardInbox(physicalName);
        return node == null ? OTHER_PATTERN : physicalName;
    }

    /** {@code <region>_<agent>} when {@code name} is {@code T.*.<region>_<agent>}, else null. */
    private static String wildcardInbox(String name) {
        String head = io.cresco.library.security.TenantNamespace.MARKER + "." + io.cresco.library.security.TenantNamespace.WILDCARD + ".";
        if (!name.startsWith(head)) return null;
        String node = name.substring(head.length());
        return node.indexOf('*') < 0 && node.indexOf('>') < 0 && node.indexOf('.') < 0 && !node.isEmpty() ? node : null;
    }

    // ---- the judgement ----

    /** There is a consumer: send. */
    public static final long ROUTE = 0L;

    /**
     * Is there a way from this broker to the inbox queue {@code queue} (the name a message for it is sent to)?
     * {@link #ROUTE} when something consumes it here, or when this gate does not judge it (see the class). Otherwise the
     * time from which this node KNOWS there is none: the loss of its last consumer plus the grace -- a time that has
     * passed when the loss is older -- or, for an inbox this broker never saw a consumer leave, {@code now} plus the grace.
     * Until that time the route may be being made: the request is worth holding.
     */
    public long noRouteFrom(String queue, long now) {
        if (boundAt == 0L || queue == null || otherPatterns.get() > 0) return ROUTE;
        String node = ActiveBroker.bridgedInboxPath(queue);
        if (node == null) return ROUTE;                                      // not a node's inbox: not judged
        if (consumers.containsKey(queue)) return ROUTE;
        Long lost = lostAt.get(queue);
        if (io.cresco.library.security.TenantNamespace.isNamespaced(queue)) {
            String everyTenant = io.cresco.library.security.TenantNamespace.wildcard(node);
            if (consumers.containsKey(everyTenant)) return ROUTE;
            Long l = lostAt.get(everyTenant);
            if (l != null && (lost == null || l > lost)) lost = l;
        }
        long known = (lost != null ? lost : now) + graceMs;
        return known == ROUTE ? 1L : known;
    }

    // ---- requests held for a route that is being made anew ----

    /**
     * Hold a request for the inbox queue {@code queue} of {@code node} ({@code <region>_<agent>}) until {@code until}:
     * {@code sendOn} runs (on this gate's thread) as soon as a consumer of that inbox joins the broker, {@code giveUp}
     * when the time comes first. False when it is not held (no room): the caller answers it now.
     */
    public boolean hold(String queue, String node, long until, long now, Runnable sendOn, Runnable giveUp) {
        if (node == null || until <= now) return false;
        if (heldNow.incrementAndGet() > holdMax) {
            heldNow.decrementAndGet();
            return false;
        }
        Held h = new Held(sendOn, giveUp);
        held.compute(node, (k, q) -> {
            ArrayDeque<Held> waiting = q == null ? new ArrayDeque<>() : q;
            waiting.add(h);
            return waiting;
        });
        heldEver.increment();
        try {
            timer().schedule(() -> expire(node, h), until - now, TimeUnit.MILLISECONDS);
        } catch (RuntimeException rejected) {                               // the timer was stopped under us
            expire(node, h);
            return true;
        }
        // a consumer that joined between the caller's look and the entry above found nothing to release
        if (noRouteFrom(queue, now) == ROUTE) release(node);
        return true;
    }

    /** A consumer of {@code node}'s inbox joined: everything held for it goes on, off the broker's thread. */
    private void release(String node) {
        try {
            timer().execute(() -> {
                ArrayDeque<Held> queue = held.remove(node);
                if (queue == null) return;
                for (Held h : queue) {
                    if (!h.settled.compareAndSet(false, true)) continue;
                    heldNow.decrementAndGet();
                    try { h.sendOn.run(); } catch (RuntimeException ignore) { }
                }
            });
        } catch (RuntimeException rejected) {
            // stopping: each request's own timer task (or stop) answers it
        }
    }

    private void expire(String node, Held h) {
        if (!h.settled.compareAndSet(false, true)) return;
        heldNow.decrementAndGet();
        held.computeIfPresent(node, (k, q) -> {
            q.remove(h);
            return q.isEmpty() ? null : q;
        });
        try { h.giveUp.run(); } catch (RuntimeException ignore) { }
    }

    private ScheduledThreadPoolExecutor timer() {
        ScheduledThreadPoolExecutor t = timer;
        if (t != null) return t;
        synchronized (this) {
            if (timer == null) {
                // two threads: a send that waits on the broker does not keep the other requests' answers waiting
                ScheduledThreadPoolExecutor made = new ScheduledThreadPoolExecutor(2, r -> {
                    Thread th = new Thread(r, "cresco-route-hold");
                    th.setDaemon(true);
                    return th;
                });
                made.setRemoveOnCancelPolicy(true);
                made.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                timer = made;
            }
            return timer;
        }
    }

    /** The node stops: every held request is answered now (a request held after this starts a timer of its own again). */
    public void stop() {
        ScheduledThreadPoolExecutor t;
        synchronized (this) {
            t = timer;
            timer = null;
        }
        if (t != null) t.shutdownNow();
        for (String node : new java.util.ArrayList<>(held.keySet())) {
            ArrayDeque<Held> waiting = held.remove(node);
            if (waiting == null) continue;
            for (Held h : waiting) {
                if (!h.settled.compareAndSet(false, true)) continue;
                heldNow.decrementAndGet();
                try { h.giveUp.run(); } catch (RuntimeException ignore) { }
            }
        }
    }
}
