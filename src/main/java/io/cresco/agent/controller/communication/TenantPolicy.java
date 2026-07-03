package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;

import java.util.Set;

/**
 * Pure, side-effect-free tenant authorization logic — "who can see what, who can do what" — decoupled
 * from ActiveMQ so it can be unit-tested exhaustively. {@link CrescoAuthorizationBroker} is the thin
 * broker adapter that calls this on every consumer/producer/send.
 *
 * <p>Rules for an external (network-client) principal, evaluated in order:
 * <ol>
 *   <li>Advisory destinations ({@code ActiveMQ.Advisory.*}) — allow (broker needs them to function).</li>
 *   <li>Fabric shared control destinations (config {@code broker_shared_destinations}, default
 *       agent/region/global.event) — allow. These carry selector-routed control traffic and, on a
 *       region broker, are already isolated by physical per-region broker separation.</li>
 *   <li>The principal's tenant namespace ({@code <tenant>} or {@code <tenant>.*}) — allow. This is the
 *       isolation boundary for application/dataplane data: a cross-tenant destination fails here.</li>
 *   <li>The principal's own agent inbox queue ({@code <region>_<agent>}) — allow read+write.</li>
 *   <li>A same-region peer queue ({@code <region>_*}) — allow WRITE (normal messaging), DENY READ
 *       (an agent must not drain a peer's inbox).</li>
 *   <li>Anything else — DENY (outside the tenant).</li>
 * </ol>
 * An {@code null}/identity-less principal on a secured, non-local connection is always denied.
 */
public final class TenantPolicy {

    private TenantPolicy() {}

    public enum Access { READ, WRITE }

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

    public static Decision check(CrescoIdentity principal, String destination, Access access, Set<String> sharedPrefixes) {
        if (destination == null || destination.isEmpty()) {
            return Decision.deny("null/empty destination");
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

        // Tenant namespace — the primary isolation boundary.
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
