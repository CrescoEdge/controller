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
}
