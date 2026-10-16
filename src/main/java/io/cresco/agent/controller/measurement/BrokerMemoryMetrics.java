package io.cresco.agent.controller.measurement;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * The embedded ActiveMQ broker's own memory, store and temp usage as Micrometer gauges, so
 * getmetricinventory shows them (GaiaKeep OUT-32). Group {@code broker}:
 *
 * <ul>
 *   <li>{@code broker.memory.used|limit|percent}: the broker-wide {@link SystemUsage} memory: the sum of
 *       every destination's in-memory message usage, against broker_memory_limit.</li>
 *   <li>{@code broker.store.used|limit|percent}, {@code broker.temp.used|limit|percent}: the persistent
 *       store (KahaDB) and the temp store (non-persistent spill) against broker_store_limit / broker_temp_limit.</li>
 *   <li>{@code broker.destinations}: queues plus topics on this broker (two map sizes, O(1)).</li>
 *   <li>{@code broker.destination.memory.max} and {@code .max.percent}: the largest per-destination memory
 *       usage among the destinations sampled, and that destination's percent of its own limit
 *       (activemq_destination_memory_limit; the point where it evicts or blocks).</li>
 * </ul>
 *
 * <p>The broker-wide numbers are O(1) reads. Per-destination numbers are bounded so a scrape never scans a
 * broker with thousands of queues: at most once per {@code refreshMs}, and only when a scrape asks, a
 * cursor visits at most {@code scanMax} destinations, resuming where the last refresh stopped (a weakly
 * consistent iterator over ActiveMQ's own concurrent destination maps; no copy). The few largest are kept as
 * candidates and re-read on each refresh, so a destination that drains drops out at once; one that grows
 * is seen within (destinations / scanMax) refreshes. On a broker with at most scanMax destinations every
 * refresh is exact.
 *
 * <p>The broker is resolved on every read, so a broker restarted under the controller is followed and a
 * node without a running broker reports NaN, not a stale value.
 */
public final class BrokerMemoryMetrics {

    public static final String GROUP = "broker";
    static final int CANDIDATES = 8;

    private final Supplier<BrokerService> brokerSupplier;
    private final int scanMax;
    private final long refreshMs;

    // per-destination sampling state, guarded by this
    private Object cursorOwner;                  // the RegionBroker the cursor walks; a new broker restarts it
    private Iterator<Destination> cursor;
    private int cursorRegion;                    // 0 = queues, 1 = topics
    private final Map<ActiveMQDestination, Destination> candidates = new LinkedHashMap<>();
    private long lastRefresh = Long.MIN_VALUE;
    private long maxUsage = 0;
    private double maxPercent = 0;
    private int lastVisited = 0;

    public BrokerMemoryMetrics(Supplier<BrokerService> brokerSupplier, int scanMax, long refreshMs) {
        this.brokerSupplier = brokerSupplier;
        this.scanMax = Math.max(1, scanMax);
        this.refreshMs = Math.max(0, refreshMs);
    }

    /** The gauge names this registers, in registration order. */
    public static final List<String> NAMES = List.of(
            "broker.memory.used", "broker.memory.limit", "broker.memory.percent",
            "broker.store.used", "broker.store.limit", "broker.store.percent",
            "broker.temp.used", "broker.temp.limit", "broker.temp.percent",
            "broker.destinations",
            "broker.destination.memory.max", "broker.destination.memory.max.percent");

    /** Registers every gauge on {@code registry}; returns the names registered (see {@link #NAMES}). */
    public List<String> register(MeterRegistry registry) {
        usage(registry, "broker.memory", "in-memory message usage (all destinations)", SystemUsage::getMemoryUsage);
        usage(registry, "broker.store", "persistent store (KahaDB) usage", SystemUsage::getStoreUsage);
        usage(registry, "broker.temp", "temp store (non-persistent spill) usage", SystemUsage::getTempUsage);
        gauge(registry, "broker.destinations", "Queues plus topics on this node's broker.", "destinations",
                m -> destinationCount());
        gauge(registry, "broker.destination.memory.max",
                "Largest per-destination memory usage on this node's broker (bounded sample, see BrokerMemoryMetrics).",
                "bytes", m -> { refreshIfDue(); synchronized (m) { return brokerOrNull() == null ? Double.NaN : maxUsage; } });
        gauge(registry, "broker.destination.memory.max.percent",
                "That destination's usage as a percent of its own memory limit.",
                "percent", m -> { refreshIfDue(); synchronized (m) { return brokerOrNull() == null ? Double.NaN : maxPercent; } });
        return NAMES;
    }

    private interface UsageOf { Usage<?> of(SystemUsage s); }

    private void usage(MeterRegistry registry, String prefix, String what, UsageOf u) {
        gauge(registry, prefix + ".used", "Broker " + what + ", bytes used.", "bytes",
                m -> { Usage<?> x = usageOf(u); return x == null ? Double.NaN : x.getUsage(); });
        gauge(registry, prefix + ".limit", "Broker " + what + ", limit in bytes.", "bytes",
                m -> { Usage<?> x = usageOf(u); return x == null ? Double.NaN : x.getLimit(); });
        gauge(registry, prefix + ".percent", "Broker " + what + ", percent of limit.", "percent",
                m -> { Usage<?> x = usageOf(u); return x == null ? Double.NaN : x.getPercentUsage(); });
    }

    private void gauge(MeterRegistry registry, String name, String description, String unit, ToDoubleFunction<BrokerMemoryMetrics> f) {
        Gauge.builder(name, this, f).description(description).baseUnit(unit).strongReference(true).register(registry);
    }

    private BrokerService brokerOrNull() {
        try {
            BrokerService b = brokerSupplier.get();
            return (b != null && b.isStarted()) ? b : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private Usage<?> usageOf(UsageOf u) {
        BrokerService b = brokerOrNull();
        if (b == null) return null;
        try {
            SystemUsage s = b.getSystemUsage();
            return s == null ? null : u.of(s);
        } catch (Exception e) {
            return null;
        }
    }

    private RegionBroker regionBroker(BrokerService b) {
        try {
            return (b.getRegionBroker() instanceof RegionBroker) ? (RegionBroker) b.getRegionBroker() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<ActiveMQDestination, Destination> map(Region r) {
        return r == null ? null : r.getDestinationMap();
    }

    double destinationCount() {
        BrokerService b = brokerOrNull();
        RegionBroker rb = b == null ? null : regionBroker(b);
        if (rb == null) return Double.NaN;
        Map<ActiveMQDestination, Destination> q = map(rb.getQueueRegion()), t = map(rb.getTopicRegion());
        return (q == null ? 0 : q.size()) + (t == null ? 0 : t.size());
    }

    /** Destinations the last refresh visited (bounded by scanMax): for tests. */
    synchronized int lastVisited() { return lastVisited; }

    void refreshIfDue() {
        long now = System.currentTimeMillis();
        synchronized (this) {
            if (lastRefresh != Long.MIN_VALUE && now - lastRefresh < refreshMs) return;
            lastRefresh = now;
        }
        refresh();
    }

    /** One bounded sampling pass: re-read the candidates, then advance the cursor by at most scanMax destinations. */
    synchronized void refresh() {
        BrokerService b = brokerOrNull();
        RegionBroker rb = b == null ? null : regionBroker(b);
        if (rb == null) {
            cursorOwner = null; cursor = null; candidates.clear(); maxUsage = 0; maxPercent = 0; lastVisited = 0;
            return;
        }
        if (cursorOwner != rb) { cursorOwner = rb; cursor = null; cursorRegion = 0; candidates.clear(); }

        Map<ActiveMQDestination, Destination> queues = map(rb.getQueueRegion()), topics = map(rb.getTopicRegion());
        // candidates that went away (removed or disposed) are dropped; the rest are re-read below
        candidates.entrySet().removeIf(e -> e.getValue().isDisposed()
                || !((queues != null && queues.get(e.getKey()) == e.getValue()) || (topics != null && topics.get(e.getKey()) == e.getValue())));

        int visited = 0, opened = 0;            // opened: region iterators started in this refresh
        while (visited < scanMax) {
            if (cursor == null || !cursor.hasNext()) {
                if (opened == 2) break;              // both regions walked to the end in this refresh: all seen
                if (cursor != null) cursorRegion ^= 1;
                Map<ActiveMQDestination, Destination> m = cursorRegion == 0 ? queues : topics;
                cursor = (m == null) ? java.util.Collections.emptyIterator() : m.values().iterator();
                opened++;
                continue;
            }
            Destination d = cursor.next();
            visited++;
            if (d == null || d.isDisposed()) continue;
            offer(d);
        }
        lastVisited = visited;

        long best = 0;
        double bestPct = 0;
        for (Destination d : candidates.values()) {
            long u = usageBytes(d);
            if (u > best || (u == best && percent(d) > bestPct)) { best = u; bestPct = percent(d); }
        }
        maxUsage = best;
        maxPercent = bestPct;
    }

    /** Keeps d if it is among the CANDIDATES largest seen. */
    private void offer(Destination d) {
        ActiveMQDestination key = d.getActiveMQDestination();
        if (key == null) return;
        if (candidates.containsKey(key) || candidates.size() < CANDIDATES) { candidates.put(key, d); return; }
        long u = usageBytes(d);
        ActiveMQDestination smallest = null;
        long smallestUsage = Long.MAX_VALUE;
        List<Map.Entry<ActiveMQDestination, Destination>> es = new ArrayList<>(candidates.entrySet());
        for (Map.Entry<ActiveMQDestination, Destination> e : es) {
            long cu = usageBytes(e.getValue());
            if (cu < smallestUsage) { smallestUsage = cu; smallest = e.getKey(); }
        }
        if (smallest != null && u > smallestUsage) { candidates.remove(smallest); candidates.put(key, d); }
    }

    private static long usageBytes(Destination d) {
        try { return d.getMemoryUsage() == null ? 0 : d.getMemoryUsage().getUsage(); } catch (RuntimeException e) { return 0; }
    }

    private static double percent(Destination d) {
        try { return d.getMemoryUsage() == null ? 0 : d.getMemoryUsage().getPercentUsage(); } catch (RuntimeException e) { return 0; }
    }
}
