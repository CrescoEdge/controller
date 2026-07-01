package io.cresco.agent.controller.health;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Controller-owned Health Check executor.
 *
 * <p>Discovers {@link HealthCheck} OSGi services (matched by {@code hc.tags}), runs each on a
 * schedule, and maintains a cached <em>effective</em> verdict per check applying three mesh-tuned
 * primitives:
 * <ul>
 *   <li><b>grace</b> — a continuous {@code TEMPORARILY_UNAVAILABLE} longer than the grace window is
 *       promoted to {@code CRITICAL} (matches Felix HC semantics). This is what absorbs a transient
 *       link/GC blip so it never triggers a spurious failover.</li>
 *   <li><b>sticky</b> — a check that was non-OK within the sticky window stays visible ({@code WARN})
 *       after recovering, so a flap that self-heals is not silently masked.</li>
 *   <li><b>cache</b> — {@link #execute}/{@link #aggregate} are cheap atomic snapshots of the last
 *       verdicts (no re-run storm).</li>
 * </ul>
 *
 * <p>Checks are real {@code org.apache.felix.hc.api.HealthCheck} services, so if the Felix HC
 * <em>core</em> bundle is ever provisioned it can take over unchanged. Until then this executor is
 * where the executor + grace + aggregation (and the mesh-tuning) live.
 */
public class CrescoHealthExecutor {

    private final BundleContext bc;
    private final PluginBuilder plugin;
    private final CLogger logger;

    private final long defaultIntervalMs;
    private final long defaultGraceMs;
    private final long defaultStickyMs;

    private final ScheduledExecutorService scheduler;
    private final Map<Long, CheckState> states = new ConcurrentHashMap<>();
    private final List<HealthListener> listeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServiceTracker<HealthCheck, HealthCheck> tracker;

    public CrescoHealthExecutor(BundleContext bc, PluginBuilder plugin) {
        this.bc = bc;
        this.plugin = plugin;
        this.logger = plugin.getLogger(CrescoHealthExecutor.class.getName(), CLogger.Level.Info);
        this.defaultIntervalMs = plugin.getConfig().getLongParam("health_check_interval_ms", 5000L);
        this.defaultGraceMs = plugin.getConfig().getLongParam("health_grace_ms", 20000L);
        this.defaultStickyMs = plugin.getConfig().getLongParam("health_sticky_ms", 60000L);
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "CrescoHealth");
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }
        this.tracker = new ServiceTracker<>(bc, HealthCheck.class, new Customizer());
        this.tracker.open();
        logger.info("CrescoHealthExecutor started (interval={}ms grace={}ms sticky={}ms)",
                defaultIntervalMs, defaultGraceMs, defaultStickyMs);

        // Periodic one-line health rollup (observability; also the seed of the inventory surface).
        long summaryMs = plugin.getConfig().getLongParam("health_summary_interval_ms", 30000L);
        if (summaryMs > 0) {
            scheduler.scheduleWithFixedDelay(this::logSummary, summaryMs, summaryMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Periodic one-line rollup of all check verdicts. */
    private void logSummary() {
        try {
            List<HealthResult> all = all();
            if (all.isEmpty()) {
                return;
            }
            Result.Status agg = Result.Status.OK;
            StringBuilder sb = new StringBuilder();
            for (HealthResult r : all) {
                if (r.status.ordinal() > agg.ordinal()) {
                    agg = r.status;
                }
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                sb.append(r.name).append('=').append(r.status);
            }
            logger.info("health summary [{}]: {}", agg, sb);
        } catch (Throwable t) {
            logger.error("health summary error: {}", String.valueOf(t));
        }
    }

    public synchronized void shutdown() {
        if (!running.getAndSet(false)) {
            return;
        }
        try {
            if (tracker != null) {
                tracker.close();
            }
        } catch (Exception ignore) {
            // best effort
        }
        for (CheckState cs : states.values()) {
            cs.cancel();
        }
        states.clear();
        scheduler.shutdownNow();
        logger.info("CrescoHealthExecutor stopped");
    }

    public void addListener(HealthListener l) {
        if (l != null) {
            listeners.add(l);
        }
    }

    public void removeListener(HealthListener l) {
        listeners.remove(l);
    }

    // ---- snapshot reads (atomic) ----

    /** Effective results for checks matching the given tags (OR; a {@code "-tag"} entry excludes). */
    public List<HealthResult> execute(String... tags) {
        List<HealthResult> out = new ArrayList<>();
        for (CheckState cs : states.values()) {
            if (matches(cs.tags, tags)) {
                out.add(cs.snapshot());
            }
        }
        return out;
    }

    /** Worst effective status among matching checks (OK if none registered). */
    public Result.Status aggregate(String... tags) {
        Result.Status worst = Result.Status.OK;
        for (CheckState cs : states.values()) {
            if (matches(cs.tags, tags) && cs.effective.ordinal() > worst.ordinal()) {
                worst = cs.effective;
            }
        }
        return worst;
    }

    /** All check snapshots (for inventory). */
    public List<HealthResult> all() {
        return execute();
    }

    // ---- tag matching ----

    private static boolean matches(Set<String> checkTags, String... query) {
        if (query == null || query.length == 0) {
            return true;
        }
        boolean anyPositive = false;
        boolean matchedPositive = false;
        for (String q : query) {
            if (q == null || q.isEmpty()) {
                continue;
            }
            if (q.charAt(0) == '-') {
                if (checkTags.contains(q.substring(1))) {
                    return false; // explicit exclusion
                }
            } else {
                anyPositive = true;
                if (checkTags.contains(q)) {
                    matchedPositive = true;
                }
            }
        }
        return !anyPositive || matchedPositive;
    }

    // ---- service tracking ----

    private final class Customizer implements ServiceTrackerCustomizer<HealthCheck, HealthCheck> {
        @Override
        public HealthCheck addingService(ServiceReference<HealthCheck> ref) {
            HealthCheck hc = bc.getService(ref);
            if (hc == null) {
                return null;
            }
            CheckState cs = new CheckState(ref, hc);
            states.put((Long) ref.getProperty(Constants.SERVICE_ID), cs);
            cs.schedule();
            logger.info("health check registered: {} tags={}", cs.name, cs.tags);
            return hc;
        }

        @Override
        public void modifiedService(ServiceReference<HealthCheck> ref, HealthCheck svc) {
            // service properties are read once at registration; nothing to do
        }

        @Override
        public void removedService(ServiceReference<HealthCheck> ref, HealthCheck svc) {
            Long id = (Long) ref.getProperty(Constants.SERVICE_ID);
            CheckState cs = states.remove(id);
            if (cs != null) {
                cs.cancel();
            }
            try {
                bc.ungetService(ref);
            } catch (Exception ignore) {
                // best effort
            }
            logger.info("health check unregistered: {}", id);
        }
    }

    // ---- per-check state ----

    private final class CheckState {
        final HealthCheck check;
        final String name;
        final Set<String> tags;
        final long intervalMs;
        final long graceMs;
        final long stickyMs;

        volatile Result.Status effective = Result.Status.TEMPORARILY_UNAVAILABLE;
        volatile Result.Status raw = Result.Status.TEMPORARILY_UNAVAILABLE;
        volatile String message = "not yet run";
        volatile long lastRunTs = 0L;
        volatile long tempUnavailSince = 0L;
        volatile long lastNonOkTs = 0L;
        ScheduledFuture<?> future;

        CheckState(ServiceReference<HealthCheck> ref, HealthCheck check) {
            this.check = check;
            Object n = ref.getProperty(HealthCheck.NAME);
            this.name = (n != null) ? n.toString() : check.getClass().getSimpleName();
            this.tags = parseTags(ref.getProperty(HealthCheck.TAGS));
            this.intervalMs = propMs(ref.getProperty(HealthCheck.ASYNC_INTERVAL_IN_SEC), defaultIntervalMs);
            this.graceMs = propMs(ref.getProperty(HealthTags.HC_GRACE_IN_SEC), defaultGraceMs);
            this.stickyMs = propMs(ref.getProperty(HealthCheck.KEEP_NON_OK_RESULTS_STICKY_FOR_SEC), defaultStickyMs);
        }

        void schedule() {
            future = scheduler.scheduleWithFixedDelay(this::run, 0L, intervalMs, TimeUnit.MILLISECONDS);
        }

        void cancel() {
            if (future != null) {
                future.cancel(true);
            }
        }

        void run() {
            long now = System.currentTimeMillis();
            Result r;
            try {
                r = check.execute();
                if (r == null) {
                    r = new Result(Result.Status.HEALTH_CHECK_ERROR, "null result");
                }
            } catch (Throwable t) {
                r = new Result(Result.Status.HEALTH_CHECK_ERROR, "check threw: " + t);
            }
            Result.Status s = r.getStatus();
            this.raw = s;
            this.message = r.toString();
            this.lastRunTs = now;

            // grace: continuous TEMPORARILY_UNAVAILABLE beyond the grace window -> CRITICAL
            if (s == Result.Status.TEMPORARILY_UNAVAILABLE) {
                if (tempUnavailSince == 0L) {
                    tempUnavailSince = now;
                }
                if (graceMs > 0 && (now - tempUnavailSince) >= graceMs) {
                    s = Result.Status.CRITICAL;
                }
            } else {
                tempUnavailSince = 0L;
            }

            // sticky: keep a recovered-but-recently-bad check visible
            Result.Status eff = s;
            if (s != Result.Status.OK) {
                lastNonOkTs = now;
            } else if (stickyMs > 0 && lastNonOkTs != 0L && (now - lastNonOkTs) < stickyMs) {
                eff = Result.Status.WARN;
            }

            Result.Status old = this.effective;
            this.effective = eff;
            if (eff != old) {
                logger.info("health '{}' {} -> {} ({})", name, old, eff, message);
                for (HealthListener l : listeners) {
                    try {
                        l.onHealthChange(name, tags, old, eff);
                    } catch (Throwable t) {
                        logger.error("health listener error: {}", t.getMessage());
                    }
                }
            }
        }

        HealthResult snapshot() {
            return new HealthResult(name, tags, effective, raw, message, lastRunTs);
        }
    }

    // ---- helpers ----

    private static Set<String> parseTags(Object prop) {
        Set<String> out = new LinkedHashSet<>();
        if (prop instanceof String[]) {
            for (String s : (String[]) prop) {
                if (s != null && !s.trim().isEmpty()) {
                    out.add(s.trim());
                }
            }
        } else if (prop instanceof String) {
            for (String s : ((String) prop).split(",")) {
                if (!s.trim().isEmpty()) {
                    out.add(s.trim());
                }
            }
        }
        return out;
    }

    /** Interprets a Felix HC "in seconds" property as milliseconds; falls back to {@code defMs}. */
    private static long propMs(Object prop, long defMs) {
        if (prop == null) {
            return defMs;
        }
        try {
            return Long.parseLong(prop.toString().trim()) * 1000L;
        } catch (Exception e) {
            return defMs;
        }
    }
}
