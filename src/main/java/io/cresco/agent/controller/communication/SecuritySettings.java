package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.Config;

/**
 * Security settings read in one place, so a default can never differ between two components that read the same key.
 *
 * <p>See {@code docs/caller-authorization.md} ("Defaults" and "Operator-visible changes").
 */
public final class SecuritySettings {

    private SecuritySettings() { }

    // ------------------------------------------------------------------ broker security

    public static final String BROKER_SECURITY = "broker_security_enabled";
    public static final String CLIENT_AUTH = "broker_require_client_auth";
    public static final String NAMESPACING = "tenant_namespacing";

    /** {@code broker_security_enabled}: broker authorization, verified identities, recipient binding. */
    public static boolean brokerSecurity(Config c) {
        return c != null && c.getBooleanParam(BROKER_SECURITY, false);
    }

    /** {@code broker_require_client_auth}, defaulting to {@link #brokerSecurity} (identities are proven when security is on). */
    public static boolean requireClientAuth(Config c) {
        return c != null && c.getBooleanParam(CLIENT_AUTH, brokerSecurity(c));
    }

    /** {@code tenant_namespacing}: every destination is {@code T.<tenant>.<name>}. */
    public static boolean tenantNamespacing(Config c) {
        return c != null && c.getBooleanParam(NAMESPACING, false);
    }

    // ------------------------------------------------------------------ tenant

    /** {@code tenant_id}: this node's tenant. */
    public static final String TENANT_ID = "tenant_id";

    /** This node's tenant ({@code tenant_id}, default {@code default}). */
    public static String tenant(Config config) {
        String raw = config == null ? null : config.getStringParam(TENANT_ID, "default");
        return raw == null ? "default" : raw;
    }

    /** {@code broker_superuser_tenants}: tenants whose infra and admin act across tenants (default {@code cresco-system}). */
    public static java.util.Set<String> superuserTenants(Config config) {
        return csv(config == null ? null : config.getStringParam("broker_superuser_tenants", "cresco-system"));
    }

    // ------------------------------------------------------------------ verified callers (phase 3)

    /**
     * {@code on_behalf_bundles}: the bundles whose messages the controller sends on behalf of an API user they
     * authenticated ({@code PluginBuilder.msgOutOnBehalfOf}); default {@code io.cresco.wsapi}.
     */
    public static java.util.Set<String> onBehalfBundles(Config config) {
        return csv(config == null ? "io.cresco.wsapi" : config.getStringParam("on_behalf_bundles", "io.cresco.wsapi"));
    }

    /**
     * {@code plugin_signing_required}: jars arriving over the network must be signed by a signer chaining to
     * {@code plugin_signing_truststore}. Defaults to {@link #brokerSecurity}: true whenever security is on; false only when
     * set explicitly (development, logged as a WARNING on every boot) or in insecure development mode.
     */
    public static boolean pluginSigningRequired(Config config) {
        return config != null && config.getBooleanParam("plugin_signing_required", brokerSecurity(config));
    }

    /** {@code file_access_roots}: the directories {@code getfiledata}/{@code getfileinfo} may read; empty denies both. */
    public static java.util.List<String> fileAccessRoots(Config config) {
        return new java.util.ArrayList<>(csv(config == null ? null : config.getStringParam("file_access_roots", "")));
    }

    static java.util.Set<String> csv(String s) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        if (s == null) return out;
        for (String part : s.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }
}
