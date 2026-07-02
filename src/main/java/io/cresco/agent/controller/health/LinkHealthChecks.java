package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.osgi.framework.BundleContext;

import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Registers the fabric-link ("link" tag) health checks. Currently the parent-link check; child-link
 * checks are added when the child watchdogs are migrated. Registered once at controller-engine start
 * (auto-unregistered when the controller bundle stops). The verdict is local; only the ping/pong is
 * communicated.
 */
public final class LinkHealthChecks {

    private LinkHealthChecks() {}

    public static void register(BundleContext bc, ControllerEngine ce) {
        long intervalSec = ce.getPluginBuilder().getConfig().getLongParam("health_link_interval_sec", 5L);
        long graceSec = ce.getPluginBuilder().getConfig().getLongParam("health_link_grace_sec", 10L);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(HealthCheck.NAME, HealthTags.LINK_PARENT);
        props.put(HealthCheck.TAGS, new String[]{HealthTags.LINK, HealthTags.LINK_PARENT});
        props.put(HealthCheck.ASYNC_INTERVAL_IN_SEC, String.valueOf(intervalSec));
        props.put(HealthTags.HC_GRACE_IN_SEC, String.valueOf(graceSec));
        bc.registerService(HealthCheck.class, new ParentLinkHealthCheck(ce), props);

        // Parent-link QUALITY (degraded) — reads the LinkMetrics measurement subsystem. Separate check
        // so a degraded-but-live link surfaces as WARN without touching the liveness verdict above.
        Dictionary<String, Object> qprops = new Hashtable<>();
        qprops.put(HealthCheck.NAME, HealthTags.LINK_QUALITY);
        qprops.put(HealthCheck.TAGS, new String[]{HealthTags.LINK, HealthTags.LINK_QUALITY});
        qprops.put(HealthCheck.ASYNC_INTERVAL_IN_SEC, String.valueOf(intervalSec));
        bc.registerService(HealthCheck.class, new LinkQualityHealthCheck(ce), qprops);
    }
}
