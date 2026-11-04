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

    /**
     * {@code broker_security_enabled}, default TRUE (secure by default): broker authorization, verified identities and
     * callers, recipient binding, message signatures. False only when set explicitly (insecure development mode).
     */
    public static boolean brokerSecurity(Config c) {
        return c == null || c.getBooleanParam(BROKER_SECURITY, true);
    }

    /** {@code broker_require_client_auth}, defaulting to {@link #brokerSecurity} (identities are proven when security is on). */
    public static boolean requireClientAuth(Config c) {
        return c == null || c.getBooleanParam(CLIENT_AUTH, brokerSecurity(c));
    }

    /** {@code tenant_namespacing}, defaulting to {@link #brokerSecurity}: every destination is {@code T.<tenant>.<name>}. */
    public static boolean tenantNamespacing(Config c) {
        return c == null || c.getBooleanParam(NAMESPACING, brokerSecurity(c));
    }

    /** True only when {@code broker_security_enabled=false} is set explicitly (development only). */
    public static boolean insecureDevelopmentMode(Config c) {
        return !brokerSecurity(c);
    }

    /**
     * Why this node must not start, one line each (empty: it may). A node with broker security on but client certificates
     * or tenant namespacing off would run with identities that are typed usernames, or with destinations a tenant cannot be
     * kept to; a tenant name that is not a valid identity cannot be put in a certificate.
     */
    public static java.util.List<String> startupRefusals(Config c) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (brokerSecurity(c) && !requireClientAuth(c)) {
            out.add("broker_security_enabled=true with broker_require_client_auth=false: without client certificates an identity is a"
                    + " typed username. Remove broker_require_client_auth=false (client certificates are required with broker security),"
                    + " or set broker_security_enabled=false explicitly for an insecure development node");
        }
        if (brokerSecurity(c) && !tenantNamespacing(c)) {
            out.add("broker_security_enabled=true with tenant_namespacing=false: un-namespaced destinations cannot keep tenants apart."
                    + " Remove tenant_namespacing=false (namespacing is required with broker security), or set"
                    + " broker_security_enabled=false explicitly for an insecure development node");
        }
        String tenant = tenantRefusal(c);
        if (tenant != null) out.add(tenant);
        return out;
    }

    /** WARNINGs (and ERRORs) a node logs on EVERY boot: temporary settings in effect. */
    public static java.util.List<String> bootWarnings(Config c, long now) {
        java.util.List<String> out = new java.util.ArrayList<>(compatProblems(c, now));
        if (compatOpen(c, now)) {
            out.add("UPGRADE WINDOW OPEN until " + compatUntil(c, now) + " (discovery_auth=compat, " + DISCOVERY_COMPAT_UNTIL + "): agents"
                    + " not yet upgraded may join over the legacy unauthenticated discovery exchange and register and keep alive their own"
                    + " node with minimal rights (agent:<tenant>/<region>/<agent>, role user; their messages unsigned). Upgrade each"
                    + " before then; after the window closes they are refused");
        }
        return out;
    }

    // ------------------------------------------------------------------ tenant

    /** {@code tenant_id}: this node's tenant. */
    public static final String TENANT_ID = "tenant_id";

    /**
     * {@code tenant_id_dot_replacement}: the explicit migration (M6b) for a deployment whose {@code tenant_id} has a {@code .}
     * (legal before 1.3): every {@code .} is replaced by this one character ({@code -} or a letter or digit) for every
     * security use (certificates, namespaces, bindings, stamps). Without it such a tenant is refused at start: a dotted
     * tenant is never an identity ({@code T.t1.} would be a prefix of {@code T.t1.x.}), and an implicit mapping could
     * silently merge two tenants ({@code acme.prod} and {@code acme-prod}).
     */
    public static final String TENANT_DOT_REPLACEMENT = "tenant_id_dot_replacement";

    /** This node's tenant as every security decision uses it ({@code tenant_id}, default {@code default}; {@link #TENANT_DOT_REPLACEMENT} applied). */
    public static String tenant(Config config) {
        String raw = config == null ? null : config.getStringParam(TENANT_ID, "default");
        if (raw == null) raw = "default";
        if (raw.indexOf('.') < 0 || config == null) return raw;
        String rep = config.getStringParam(TENANT_DOT_REPLACEMENT);
        if (rep == null || rep.length() != 1 || !(Character.isLetterOrDigit(rep.charAt(0)) || rep.charAt(0) == '-')) return raw;
        return raw.replace('.', rep.charAt(0));
    }

    /** Why the configured tenant cannot be used, or null. */
    public static String tenantRefusal(Config config) {
        String raw = config == null ? "default" : config.getStringParam(TENANT_ID, "default");
        String t = tenant(config);
        if (CrescoNames.isValidTenant(t)) return null;
        if (raw != null && raw.indexOf('.') >= 0 && CrescoNames.isValid(raw)) {
            return "tenant_id '" + raw + "' has a '.', which a tenant name may no longer carry (tenant t1's namespace T.t1. would be a"
                    + " prefix of tenant t1.x's). To keep this deployment, set " + TENANT_DOT_REPLACEMENT + "=- on EVERY node of the"
                    + " tenant (it then runs as '" + raw.replace('.', '-') + "'; make sure no other tenant already has that name)";
        }
        return CrescoNames.whyInvalid("tenant_id", raw);
    }

    // ------------------------------------------------------------------ the upgrade window (M6a)

    /** {@code discovery_compat_until}: the end of the rolling-upgrade window opened with {@code discovery_auth=compat}. */
    public static final String DISCOVERY_COMPAT_UNTIL = "discovery_compat_until";
    /** The longest upgrade window that may be configured, from now. */
    public static final long MAX_COMPAT_WINDOW_MS = 90L * 24 * 3600 * 1000;

    /**
     * The end of the upgrade window ({@code discovery_compat_until}: an ISO-8601 date, read as the end of that day UTC, or an
     * instant), or null when it is absent, unparseable, or more than {@link #MAX_COMPAT_WINDOW_MS} ahead.
     */
    public static java.time.Instant compatUntil(Config config, long now) {
        String v = config == null ? null : config.getStringParam(DISCOVERY_COMPAT_UNTIL);
        if (v == null || v.isBlank()) return null;
        java.time.Instant until;
        try {
            v = v.trim();
            until = v.length() == 10 ? java.time.LocalDate.parse(v).plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant()
                    : java.time.Instant.parse(v);
        } catch (Exception e) {
            return null;
        }
        if (until.toEpochMilli() - now > MAX_COMPAT_WINDOW_MS) return null;
        return until;
    }

    /** {@code discovery_auth=compat} is configured (whatever its window says). */
    public static boolean compatConfigured(Config config) {
        if (config == null) return false;
        String mode = config.getStringParam("discovery_auth", "v2");
        return mode != null && mode.trim().toLowerCase(java.util.Locale.ROOT).equals("compat");
    }

    /**
     * Is the upgrade window open now: {@code discovery_auth=compat} with a {@code discovery_compat_until} that has not passed
     * (at most 90 days ahead)? Inside it, agents not yet upgraded may use the legacy discovery exchange and register and keep
     * alive their own node with minimal rights; outside it, never.
     */
    public static boolean compatOpen(Config config, long now) {
        if (!compatConfigured(config)) return false;
        java.time.Instant until = compatUntil(config, now);
        return until != null && now < until.toEpochMilli();
    }

    /** Problems with the window settings worth an ERROR at every boot (they leave the window closed). */
    public static java.util.List<String> compatProblems(Config config, long now) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (!compatConfigured(config)) return out;
        String v = config.getStringParam(DISCOVERY_COMPAT_UNTIL);
        java.time.Instant until = compatUntil(config, now);
        if (v == null || v.isBlank()) {
            out.add("discovery_auth=compat is IGNORED: it needs " + DISCOVERY_COMPAT_UNTIL + " (an ISO-8601 date at most 90 days ahead)"
                    + " that ends the upgrade window; only the authenticated exchange (v2) is used");
        } else if (until == null) {
            out.add("discovery_auth=compat is IGNORED: " + DISCOVERY_COMPAT_UNTIL + "=" + v + " is not an ISO-8601 date or instant"
                    + " at most 90 days ahead; only the authenticated exchange (v2) is used");
        } else if (now >= until.toEpochMilli()) {
            out.add("the upgrade window has CLOSED (" + DISCOVERY_COMPAT_UNTIL + "=" + v + "): only the authenticated exchange (v2) is"
                    + " used, and agents that are not upgraded are refused; remove discovery_auth=compat");
        }
        return out;
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
