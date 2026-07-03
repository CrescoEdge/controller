package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.security.TenantNamespace;

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

        return Decision.deny("outside tenant '" + tenant + "' namespace");
    }
}
