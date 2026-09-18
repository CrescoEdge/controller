package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.security.TenantNamespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Pure, side-effect-free tenant authorization logic — "who can see what, who can do what" — decoupled
 * from ActiveMQ so it can be unit-tested exhaustively. {@link CrescoAuthorizationBroker} is the thin
 * broker adapter that calls this on every consumer/producer/send.
 *
 * <p><b>Role model.</b> Every connection is resolved to a {@link Role}:
 * <ul>
 *   <li>{@link Role#SUPERUSER} — full access to every destination in every tenant (cross-tenant "god"
 *       view). The local in-JVM controller ({@code vm://}) and broker bridges are superuser-equivalent
 *       (exempted at the adapter); a network client is superuser only if its cert-bound tenant is in the
 *       configured superuser set ({@code broker_superuser_tenants}).</li>
 *   <li>{@link Role#INTERNAL} — reserved for system/relay operations that must span tenants on the
 *       control plane; currently evaluated as superuser and kept distinct so it can be tightened to
 *       "control destinations only, no tenant application data" without touching call sites.</li>
 *   <li>{@link Role#TENANT} — the default for an ordinary client: access is confined to its own tenant.</li>
 * </ul>
 *
 * <p>Rules for a {@link Role#TENANT} principal, evaluated in order:
 * <ol>
 *   <li>Advisory destinations ({@code ActiveMQ.Advisory.*}) — allow (broker needs them to function).</li>
 *   <li>Fabric shared control destinations (config {@code broker_shared_destinations}) — allow.</li>
 *   <li><b>Tenant-namespaced destination</b> ({@code T.<tenant>.*}, when {@code tenant_namespacing} is on) —
 *       allow iff it is in the principal's own {@code T.<tenant>.} subtree; any other tenant's subtree (or a
 *       {@code T.*.} wildcard) is denied. This is the strong, prefix-clean isolation boundary and it closes
 *       the flat-name same-region write hole.</li>
 *   <li>(Legacy, un-namespaced) the principal's tenant namespace ({@code <tenant>}/{@code <tenant>.*}),
 *       own inbox ({@code <region>_<agent>}), same-region peer write — as before, for back-compat when
 *       namespacing is off.</li>
 *   <li>Anything else — DENY (outside the tenant).</li>
 * </ol>
 * An {@code null}/identity-less principal on a secured, non-local connection is always denied.
 */
public final class TenantPolicy {

    private TenantPolicy() {}

    public enum Access { READ, WRITE }

    /** Authorization role of a connection. See class javadoc. */
    public enum Role { SUPERUSER, INTERNAL, TENANT }

    /**
     * W-GFS-1: a NAMED cross-tenant sink. A principal whose tenant matches {@code src} may {@code access}
     * destinations matching {@code dst} even though they lie outside its own {@code T.<tenant>.} subtree.
     * This is the only way across the tenant boundary short of SUPERUSER, and it is explicit: an
     * allow-list distributed with the fabric configuration ({@code broker_cross_tenant_sinks}), e.g.
     * <pre>
     *   *->T.gfs-federation.*:write ; gfs-federation->T.*:write ; *->T.*.global.event:write
     * </pre>
     * (any site may write to the federation core's inboxes; the core may write to any tenant's inbox;
     * any tenant may publish fragments on any tenant's dataplane topic). Globs: {@code *} = any run of
     * characters, {@code ?} = one character. Access is {@code write} (default), {@code read} or {@code any}.
     * A rule never grants READ unless it says so, so a tenant still cannot consume another tenant's inbox.
     */
    public static final class CrossTenantSink {
        public final String src, dst;
        public final Access access; // null = any
        private final java.util.regex.Pattern srcRe, dstRe;

        public CrossTenantSink(String src, String dst, Access access) {
            this.src = src; this.dst = dst; this.access = access;
            this.srcRe = glob(src); this.dstRe = glob(dst);
        }

        public boolean matches(String tenant, String destination, Access a) {
            if (tenant == null || destination == null) return false;
            if (access != null && access != a) return false;
            return srcRe.matcher(tenant).matches() && dstRe.matcher(destination).matches();
        }

        static java.util.regex.Pattern glob(String g) {
            StringBuilder sb = new StringBuilder();
            for (char c : g.toCharArray()) {
                if (c == '*') sb.append(".*");
                else if (c == '?') sb.append('.');
                else sb.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
            return java.util.regex.Pattern.compile(sb.toString());
        }

        /** Parse {@code src->dst[:write|read|any]} entries separated by ';' or ','. Malformed entries are dropped. */
        public static List<CrossTenantSink> parse(String spec) {
            List<CrossTenantSink> out = new ArrayList<>();
            if (spec == null) return out;
            for (String raw : spec.split("[;,]")) {
                String e = raw.trim();
                if (e.isEmpty()) continue;
                int arrow = e.indexOf("->");
                if (arrow <= 0 || arrow + 2 >= e.length()) continue;
                String src = e.substring(0, arrow).trim();
                String rest = e.substring(arrow + 2).trim();
                Access acc = Access.WRITE;
                int colon = rest.lastIndexOf(':');
                if (colon > 0) {
                    String a = rest.substring(colon + 1).trim().toLowerCase();
                    if (a.equals("write")) { acc = Access.WRITE; rest = rest.substring(0, colon).trim(); }
                    else if (a.equals("read")) { acc = Access.READ; rest = rest.substring(0, colon).trim(); }
                    else if (a.equals("any")) { acc = null; rest = rest.substring(0, colon).trim(); }
                }
                if (src.isEmpty() || rest.isEmpty()) continue;
                out.add(new CrossTenantSink(src, rest, acc));
            }
            return out;
        }

        @Override public String toString() { return src + "->" + dst + ":" + (access == null ? "any" : access.name().toLowerCase()); }
    }

    private static Decision sinkDecision(String tenant, String destination, Access access, List<CrossTenantSink> sinks) {
        if (sinks == null) return null;
        for (CrossTenantSink s : sinks) {
            if (s.matches(tenant, destination, access)) return Decision.allow("cross-tenant-sink " + s);
        }
        return null;
    }

    public static final class Decision {
        public final boolean allowed;
        public final String reason;
        private Decision(boolean allowed, String reason) { this.allowed = allowed; this.reason = reason; }
        public static Decision allow(String reason) { return new Decision(true, reason); }
        public static Decision deny(String reason)  { return new Decision(false, reason); }
    }

    private static boolean isAdvisory(String dest) {
        return dest.startsWith("ActiveMQ.Advisory");
    }

    private static boolean matchesShared(String dest, Set<String> sharedPrefixes) {
        if (sharedPrefixes == null) return false;
        for (String p : sharedPrefixes) {
            if (p == null || p.isEmpty()) continue;
            // exact, or a sharded/sub form like "global.event.3"
            if (dest.equals(p) || dest.startsWith(p + ".")) return true;
        }
        return false;
    }

    /** Back-compat entry point: evaluates as an ordinary {@link Role#TENANT} principal. */
    public static Decision check(CrescoIdentity principal, String destination, Access access, Set<String> sharedPrefixes) {
        return check(principal, destination, access, sharedPrefixes, Role.TENANT);
    }

    public static Decision check(CrescoIdentity principal, String destination, Access access,
                                 Set<String> sharedPrefixes, Role role) {
        return check(principal, destination, access, sharedPrefixes, role, null);
    }

    public static Decision check(CrescoIdentity principal, String destination, Access access,
                                 Set<String> sharedPrefixes, Role role, List<CrossTenantSink> sinks) {
        if (destination == null || destination.isEmpty()) {
            return Decision.deny("null/empty destination");
        }
        // SUPERUSER (and, for now, INTERNAL) — cross-tenant "god" access. Infra/vm:// and bridges are
        // exempted before we ever reach here; this covers an explicitly-granted superuser network client.
        if (role == Role.SUPERUSER || role == Role.INTERNAL) {
            return Decision.allow(role == Role.SUPERUSER ? "superuser" : "internal");
        }
        if (isAdvisory(destination)) {
            return Decision.allow("advisory");
        }
        if (matchesShared(destination, sharedPrefixes)) {
            return Decision.allow("fabric-shared");
        }
        if (principal == null || principal.getTenant() == null) {
            return Decision.deny("unidentified principal on secured connection");
        }
        String tenant = principal.getTenant();

        // Tenant-namespaced destination (T.<tenant>.*): the strong, prefix-clean boundary. A TENANT
        // principal may only touch its OWN subtree; any other tenant's subtree — or a T.*. wildcard —
        // is denied. This is what closes the flat-name same-region cross-tenant write hole.
        if (TenantNamespace.isNamespaced(destination)) {
            if (destination.startsWith(TenantNamespace.prefix(tenant))) {
                return Decision.allow("tenant-namespace");
            }
            // W-GFS-1: an explicitly named cross-tenant sink is the ONLY way across the boundary.
            Decision sink = sinkDecision(tenant, destination, access, sinks);
            if (sink != null) return sink;
            return Decision.deny("cross-tenant namespaced dest (own tenant '" + tenant + "')");
        }

        // Legacy tenant namespace (un-namespaced app destinations) — the primary isolation boundary.
        if (destination.equals(tenant) || destination.startsWith(tenant + ".")) {
            return Decision.allow("tenant-namespace");
        }

        // Own inbox queue.
        String agentPath = principal.getAgentPath();
        if (agentPath != null && destination.equals(agentPath)) {
            return Decision.allow("own-queue");
        }

        // Same-region peer queue: may send to it, may not read from it.
        if (principal.getRegion() != null && destination.startsWith(principal.getRegion() + "_")) {
            return (access == Access.WRITE)
                    ? Decision.allow("same-region-write")
                    : Decision.deny("read of peer inbox in region denied");
        }

        Decision sink = sinkDecision(tenant, destination, access, sinks);
        if (sink != null) return sink;
        return Decision.deny("outside tenant '" + tenant + "' namespace");
    }
}
