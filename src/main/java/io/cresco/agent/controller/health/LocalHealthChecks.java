package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.osgi.framework.BundleContext;

import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Registers the in-JVM ("local" tag) health checks as {@code org.apache.felix.hc.api.HealthCheck}
 * OSGi services. The {@link CrescoHealthExecutor}'s ServiceTracker picks them up and schedules them.
 * Registered once at controller-engine start; auto-unregistered when the controller bundle stops.
 * They self-guard (subsystem-not-ready -&gt; TEMPORARILY_UNAVAILABLE), so registering before the
 * subsystems exist is safe.
 */
public final class LocalHealthChecks {

    private LocalHealthChecks() {}

    public static void register(BundleContext bc, ControllerEngine ce) {
        register(bc, "broker", new BrokerHealthCheck(ce));
        register(bc, "dataplane", new DataPlaneHealthCheck(ce));
        register(bc, "db", new DbHealthCheck(ce));
        register(bc, "disk", new DiskHealthCheck(ce));
        register(bc, "memory", new MemoryHealthCheck(ce));
        register(bc, "plugins", new PluginHealthCheck(ce));
    }

    private static void register(BundleContext bc, String name, HealthCheck hc) {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(HealthCheck.NAME, name);
        props.put(HealthCheck.TAGS, new String[]{HealthTags.LOCAL});
        bc.registerService(HealthCheck.class, hc, props);
    }
}
