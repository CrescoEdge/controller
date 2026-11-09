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
        if (production(c)) return true;                       // production forces it on (and refuses to start if set off)
        return c == null || c.getBooleanParam(BROKER_SECURITY, true);
    }

    /** {@code broker_require_client_auth}, defaulting to {@link #brokerSecurity} (identities are proven when security is on). */
    public static boolean requireClientAuth(Config c) {
        if (production(c)) return true;
        return c == null || c.getBooleanParam(CLIENT_AUTH, brokerSecurity(c));
    }

    /** {@code tenant_namespacing}, defaulting to {@link #brokerSecurity}: every destination is {@code T.<tenant>.<name>}. */
    public static boolean tenantNamespacing(Config c) {
        if (production(c)) return true;
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
        out.addAll(profileRefusals(c, System.currentTimeMillis()));
        return out;
    }

    // ------------------------------------------------------------------ the security profile

    /** {@code security_profile}: {@code production} or {@code standard} (default). */
    public static final String PROFILE = io.cresco.library.security.SecurityProfile.KEY;
    /** Provisioned identity (production): the node's key and chain, its password file, the fabric's trust anchors. */
    public static final String IDENTITY_KEYSTORE = "identity_keystore", IDENTITY_KEYSTORE_PASSWORD_FILE = "identity_keystore_password_file",
            TRUST_ANCHORS = "trust_anchors", REVOCATION_FILE = "revocation_file";

    /**
     * This node's profile: {@code INSECURE} in explicit insecure debug mode ({@code broker_security_enabled=false}), else
     * {@code security_profile} ({@code STANDARD} when unset; an unknown value reads as {@code STANDARD} here and refuses
     * the node's start in {@link #startupRefusals}).
     */
    public static io.cresco.library.security.SecurityProfile profile(Config c) {
        if (!brokerSecurity(c)) return io.cresco.library.security.SecurityProfile.INSECURE;
        io.cresco.library.security.SecurityProfile p = io.cresco.library.security.SecurityProfile.parse(
                c == null ? null : c.getStringParam(PROFILE));
        return p == null ? io.cresco.library.security.SecurityProfile.STANDARD : p;
    }

    /** {@code security_profile=production} (and not refused for an unknown value). */
    public static boolean production(Config c) {
        return c != null && io.cresco.library.security.SecurityProfile.parse(c.getStringParam(PROFILE))
                == io.cresco.library.security.SecurityProfile.PRODUCTION;
    }

    /** Credentials that are passwords (debug and testing only): never configured in production. */
    static final java.util.List<String> PASSWORD_PARAMS = java.util.Arrays.asList(
            "cresco_service_key", "broker_security_secret", "keystorepwd", "truststorepwd", "plugin_signing_truststore_password");
    /** Secrets that must come from a {@code _file} (or {@code CRESCO_} environment) source in production, never inline. */
    static final java.util.List<String> FILE_ONLY_SECRETS = java.util.Arrays.asList("db_password");
    /** The shared discovery secrets: the standard profile's discovery; never used in production (the PKI's discovery). */
    static final java.util.List<String> DISCOVERY_SECRETS = java.util.Arrays.asList(
            "discovery_secret_agent", "discovery_secret_region", "discovery_secret_global");

    /**
     * Why the profile forbids this node's start, one line per problem (empty: it may start). An unknown
     * {@code security_profile} is refused in any mode. {@code production}: every node identity is pre-provisioned by the
     * operator's PKI and nothing that would mint, enroll, pin on first use or accept a password is allowed.
     */
    public static java.util.List<String> profileRefusals(Config c, long now) {
        java.util.List<String> out = new java.util.ArrayList<>();
        String raw = c == null ? null : c.getStringParam(PROFILE);
        if (raw != null && !raw.isBlank() && io.cresco.library.security.SecurityProfile.parse(raw) == null) {
            out.add(PROFILE + "=" + raw + " is not a profile: use production or standard (insecure debug mode is"
                    + " broker_security_enabled=false, under the standard profile only)");
            return out;
        }
        if (!production(c)) return out;
        String pre = "security_profile=production: ";
        if (!c.getBooleanParam(BROKER_SECURITY, true)) out.add(pre + "broker_security_enabled=false is not allowed (insecure debug mode is never production)");
        if (!c.getBooleanParam(CLIENT_AUTH, true)) out.add(pre + "broker_require_client_auth=false is not allowed");
        if (!c.getBooleanParam(NAMESPACING, true)) out.add(pre + "tenant_namespacing=false is not allowed");
        if (!c.getBooleanParam("plugin_signing_required", true)) out.add(pre + "plugin_signing_required=false is not allowed");
        if (!c.getBooleanParam("msg_signing_required", true)) out.add(pre + "msg_signing_required=false is not allowed");
        String pim = c.getStringParam(PeerIdentity.MODE_KEY);
        if (pim != null && !pim.trim().equalsIgnoreCase("enforce")) out.add(pre + PeerIdentity.MODE_KEY + "=" + pim + " is not allowed (enforce only)");
        if (compatConfigured(c) || c.getStringParam(DISCOVERY_COMPAT_UNTIL) != null) {
            out.add(pre + "the upgrade window (discovery_auth=compat, " + DISCOVERY_COMPAT_UNTIL + ") is not allowed");
        }
        if (c.getBooleanParam("security_regional_ca", false)) {
            out.add(pre + "security_regional_ca=true (a region issuing certificates to agents that join with the discovery secret)"
                    + " is not allowed: every node's certificate is issued by the operator's PKI");
        }
        if (c.getBooleanParam("broker_allow_unverified_bridges", false)) out.add(pre + "broker_allow_unverified_bridges=true is not allowed");
        for (String k : new String[]{"peer_identity_rebind", "public_key_trusted_fingerprints"}) {
            String v = c.getStringParam(k);
            if (v != null && !v.isBlank()) {
                out.add(pre + k + " is not allowed (trust comes from " + TRUST_ANCHORS + " only; re-provision a node instead of"
                        + " rebinding it)");
            }
        }
        requiredFile(out, pre, c, IDENTITY_KEYSTORE, "the node's operator-issued key and certificate chain (PKCS12)");
        requiredFile(out, pre, c, IDENTITY_KEYSTORE_PASSWORD_FILE, "the password of " + IDENTITY_KEYSTORE + " (an owner-only file)");
        requiredFile(out, pre, c, TRUST_ANCHORS, "the fabric's root and region CA certificates (PEM)");
        requiredFile(out, pre, c, "plugin_signing_truststore", "the anchors plugin jars are signed to (plugin signing is mandatory)");
        String rev = c.getStringParam(REVOCATION_FILE);
        if (rev != null && !rev.isBlank() && !java.nio.file.Files.isReadable(java.nio.file.Paths.get(rev.trim()))) {
            out.add(pre + REVOCATION_FILE + "=" + rev + " is not a readable file");
        }
        for (String k : PASSWORD_PARAMS) {
            String v = c.getStringParam(k);
            String f = c.getStringParam(k + "_file");
            if ((v != null && !v.isEmpty()) || (f != null && !f.isBlank())) {
                out.add(pre + k + " is a password, which production does not allow (debug and testing only)");
            }
        }
        // discovery in production is the PKI's alone: both sides sign the exchange with their provisioned identity keys and
        // their chains verify to trust_anchors (role, name, validity, revocation); nothing is issued, enrolled or pinned on
        // first use, and no shared discovery secret is used or accepted -- one configured is a leftover, refused by name
        for (String k : DISCOVERY_SECRETS) {
            String v = c.getStringParam(k);
            if ((v != null && !v.isEmpty()) || System.getProperty(k) != null) {
                out.add(pre + k + " is set: production discovery is authenticated by the operator's PKI only, no discovery"
                        + " secret is used or accepted (remove " + k + ", its _file and CRESCO_" + k.toUpperCase(java.util.Locale.ROOT) + ")");
            }
        }
        for (String k : FILE_ONLY_SECRETS) {
            if (System.getProperty(k) != null) {
                out.add(pre + k + " is given inline (-D" + k + "): give it as " + k + "_file (an owner-only file) or CRESCO_"
                        + k.toUpperCase(java.util.Locale.ROOT));
            }
        }
        return out;
    }

    private static void requiredFile(java.util.List<String> out, String pre, Config c, String key, String what) {
        String v = c.getStringParam(key);
        if (v == null || v.isBlank()) {
            out.add(pre + key + " is required: " + what);
        } else if (!java.nio.file.Files.isRegularFile(java.nio.file.Paths.get(v.trim())) || !java.nio.file.Files.isReadable(java.nio.file.Paths.get(v.trim()))) {
            out.add(pre + key + "=" + v + " is not a readable file (" + what + ")");
        }
    }

    /** WARNINGs (and ERRORs) a node logs on EVERY boot: temporary settings in effect. */
    public static java.util.List<String> bootWarnings(Config c, long now) {
        java.util.List<String> out = new java.util.ArrayList<>(compatProblems(c, now));
        if (compatOpen(c, now)) {
            out.add("UPGRADE WINDOW OPEN until " + compatOpenUntil(c, now) + " (discovery_auth=compat, " + DISCOVERY_COMPAT_UNTIL + "): agents"
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
     * The window as decided when a node's configuration was first read (its start): {@code discovery_compat_until} as it was
     * set then, and its end -- null when it was absent, unparseable or more than 90 days ahead AT THAT TIME. Decided once per
     * configuration (identity) and setting: a date that was too far ahead is never accepted later as the days pass (a
     * restart, which reads the setting anew, is needed to open a window). Weak keys: a configuration let go is forgotten.
     */
    private static final com.google.common.cache.Cache<Config, CompatDecision> COMPAT_DECIDED =
            com.google.common.cache.CacheBuilder.newBuilder().weakKeys().build();

    private static final class CompatDecision {
        final String setting;
        final java.time.Instant until;
        final long decidedAt;
        CompatDecision(String setting, java.time.Instant until, long decidedAt) { this.setting = setting; this.until = until; this.decidedAt = decidedAt; }
    }

    private static CompatDecision compatDecision(Config config, long now) {
        String v = config.getStringParam(DISCOVERY_COMPAT_UNTIL);
        CompatDecision d = COMPAT_DECIDED.getIfPresent(config);
        if (d != null && java.util.Objects.equals(d.setting, v)) return d;
        synchronized (COMPAT_DECIDED) {
            d = COMPAT_DECIDED.getIfPresent(config);
            if (d == null || !java.util.Objects.equals(d.setting, v)) {
                d = new CompatDecision(v, compatUntil(config, now), now);
                COMPAT_DECIDED.put(config, d);
            }
            return d;
        }
    }

    /**
     * Is the upgrade window open now: {@code discovery_auth=compat} with a {@code discovery_compat_until} that has not passed
     * and was at most 90 days ahead when the node started (decided ONCE, {@link #compatDecision}: one more than 90 days
     * ahead stays closed for good, it never opens by itself as the date comes nearer)? Inside it, agents not yet upgraded
     * may use the legacy discovery exchange and register and keep alive their own node with minimal rights; outside it,
     * never.
     */
    public static boolean compatOpen(Config config, long now) {
        if (!compatConfigured(config)) return false;
        java.time.Instant until = compatDecision(config, now).until;
        return until != null && now < until.toEpochMilli();
    }

    /** The end of the open window as decided at start (see {@link #compatOpen}), or null when none is open. */
    public static java.time.Instant compatOpenUntil(Config config, long now) {
        if (!compatOpen(config, now)) return null;
        return compatDecision(config, now).until;
    }

    /** Problems with the window settings worth an ERROR at every boot (they leave the window closed). */
    public static java.util.List<String> compatProblems(Config config, long now) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (!compatConfigured(config)) return out;
        String v = config.getStringParam(DISCOVERY_COMPAT_UNTIL);
        java.time.Instant until = compatDecision(config, now).until;
        if (v == null || v.isBlank()) {
            out.add("discovery_auth=compat is IGNORED: it needs " + DISCOVERY_COMPAT_UNTIL + " (an ISO-8601 date at most 90 days ahead)"
                    + " that ends the upgrade window; only the authenticated exchange (v2) is used");
        } else if (until == null) {
            out.add("discovery_auth=compat is IGNORED: " + DISCOVERY_COMPAT_UNTIL + "=" + v + " is not an ISO-8601 date or instant"
                    + " at most 90 days ahead; only the authenticated exchange (v2) is used, and the window stays closed until a"
                    + " restart with a date at most 90 days ahead (it never opens by itself as the date comes nearer)");
        } else if (now >= until.toEpochMilli()) {
            out.add("the upgrade window has CLOSED (" + DISCOVERY_COMPAT_UNTIL + "=" + v + "): only the authenticated exchange (v2) is"
                    + " used, and agents that are not upgraded are refused; remove discovery_auth=compat");
        }
        return out;
    }

    /** {@code broker_superuser_tenants}: tenants whose infra and admin act across tenants (default {@code cresco-system}). */
    public static java.util.Set<String> superuserTenants(Config config) {
        String raw = config == null ? null : config.getStringParam("broker_superuser_tenants", "cresco-system");
        // read on every message checked (MsgSigning.verify): parsed again only when the setting changes
        Object[] last = lastSuperuser;
        if (last != null && java.util.Objects.equals(last[0], raw)) {
            @SuppressWarnings("unchecked") java.util.Set<String> parsed = (java.util.Set<String>) last[1];
            return parsed;
        }
        java.util.Set<String> parsed = java.util.Collections.unmodifiableSet(csv(raw));
        lastSuperuser = new Object[]{raw, parsed};
        return parsed;
    }

    /** The last {@code broker_superuser_tenants} value read and its parsed set. */
    private static volatile Object[] lastSuperuser;

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
        if (production(config)) return true;
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
