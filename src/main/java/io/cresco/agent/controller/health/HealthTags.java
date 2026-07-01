package io.cresco.agent.controller.health;

/**
 * Canonical health-check tags and Cresco-specific service-property keys.
 *
 * Checks are real {@code org.apache.felix.hc.api.HealthCheck} OSGi services; the standard
 * Felix HC service-property keys ({@code hc.name}, {@code hc.tags},
 * {@code hc.async.intervalInSec}, {@code hc.keepNonOkResultsStickyForSec}) are used verbatim
 * (see {@code org.apache.felix.hc.api.HealthCheck} constants) so the Felix HC core bundle could
 * take these checks over unchanged if it is ever provisioned.
 */
public final class HealthTags {

    private HealthTags() {}

    /** In-JVM subsystem checks (broker, db, disk, memory, dataplane, plugins). No messaging. */
    public static final String LOCAL = "local";

    /** Fabric link checks whose verdict is local but whose input signal is communicated
     *  (parent liveness, child liveness). */
    public static final String LINK = "link";

    /** Sub-tag: the link to this node's parent controller (agent->region, region->global). */
    public static final String LINK_PARENT = "link:parent";

    /** Sub-tag prefix: the link to a child node (region's agents, global's regions). */
    public static final String LINK_CHILD = "link:child";

    /**
     * Cresco extension property: grace window (seconds) during which a continuous
     * TEMPORARILY_UNAVAILABLE is tolerated before it is promoted to CRITICAL. Mirrors the Felix HC
     * TEMPORARILY_UNAVAILABLE->CRITICAL grace behaviour, made explicit and per-check tunable so the
     * mesh can ride out transient link/GC blips without a spurious failover.
     */
    public static final String HC_GRACE_IN_SEC = "hc.cresco.graceInSec";
}
