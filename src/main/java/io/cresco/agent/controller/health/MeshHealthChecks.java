package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.osgi.framework.BundleContext;

import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Registers the mesh-rollup ("mesh" tag) health check ({@link SubtreeHealthCheck}). Registered on every
 * controller — a leaf agent simply has no children, so it reports OK. Auto-unregistered when the
 * controller bundle stops.
 */
public final class MeshHealthChecks {

    /** Tag for checks that roll up health reported across the mesh. */
    public static final String MESH = "mesh";

    private MeshHealthChecks() {}

    public static void register(BundleContext bc, ControllerEngine ce) {
        long intervalSec = ce.getPluginBuilder().getConfig().getLongParam("health_subtree_interval_sec", 10L);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(HealthCheck.NAME, "subtree");
        props.put(HealthCheck.TAGS, new String[]{MESH});
        props.put(HealthCheck.ASYNC_INTERVAL_IN_SEC, String.valueOf(intervalSec));
        bc.registerService(HealthCheck.class, new SubtreeHealthCheck(ce), props);
    }
}
