package io.cresco.agent.controller.communication;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * The nodes of this controller's region that are not upgraded yet, as seen inside the upgrade window
 * (docs/upgrade-1.3-secure-defaults.md section 3).
 *
 * <p>A node of an earlier 1.3 build runs without tenant namespacing: it writes to its controller's raw inbox
 * ({@code <region>_<agent>}) and consumes its own raw inbox. An upgraded controller consumes its {@code T.*.} inbox, and,
 * only while the window is open, its raw inbox as well ({@link AgentConsumer}). A message taken from the raw inbox whose
 * broker stamp is the legacy {@code agent:<tenant>/<region>/<agent>} (a verified connection that declares no caller: the
 * certificate names the node) marks that node here, and what this controller sends to it then goes to its raw inbox, where
 * it listens ({@link AgentProducer}). A message from the same node as a controller (an upgraded build: it stamps and signs)
 * clears the mark. Entries expire 15 minutes after the node was last heard from (a legacy node keeps alive far more often)
 * and the set is bounded, so every operation is O(1) and the set cannot grow without limit.
 */
public final class LegacyNodes {

    static final long MAX_NODES = 100_000;
    static final long TTL_MINUTES = 15;

    private final Cache<String, Boolean> nodes = CacheBuilder.newBuilder()
            .maximumSize(MAX_NODES).expireAfterWrite(java.time.Duration.ofMinutes(TTL_MINUTES)).build();
    /** Fast path: no lookup at all while no legacy node was ever seen (the normal case outside an upgrade). */
    private volatile boolean any;

    /** {@code region/agent} was heard from as a node that is not upgraded (its message came stamped {@code agent:}). */
    public void seen(String region, String agent) {
        if (region == null || agent == null) return;
        nodes.put(region + "_" + agent, Boolean.TRUE);
        any = true;
    }

    /** {@code region/agent} spoke as an upgraded node: it is no longer legacy. */
    public void upgraded(String region, String agent) {
        if (!any || region == null || agent == null) return;
        nodes.invalidate(region + "_" + agent);
    }

    /** Is the node at the raw path {@code <region>_<agent>} a legacy node of this region (see the class comment)? */
    public boolean isLegacy(String agentPath) {
        return any && agentPath != null && nodes.getIfPresent(agentPath) != null;
    }

    /** Forget every mark (the window closed). */
    public void clear() {
        nodes.invalidateAll();
        any = false;
    }

    /** How many nodes are marked (diagnostics and tests). */
    public long size() {
        nodes.cleanUp();
        return nodes.size();
    }
}
