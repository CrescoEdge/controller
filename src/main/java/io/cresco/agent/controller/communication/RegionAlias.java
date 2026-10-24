package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;

/**
 * A node that keeps answering at its former address after it moved to another region (GaiaKeep gfs#26).
 *
 * <p>An application can name its nodes by Cresco address ({@code region_agent}): the GaiaKeep storage core does (its
 * Raft peers, node pins and block locations). A deployment that starts with every agent on one broker (one region,
 * the global's) and then gives each site its own regional broker would change all of those addresses. With
 * {@code region_alias} = the former region, an agent moved into its site's region also consumes the inbox queue of its
 * former address ({@code <region_alias>_<agent>}, tenant-qualified like its own inbox) on the broker it is attached
 * to. A message addressed there is routed by the brokers' demand-forwarding bridges to that consumer the fewest-hops
 * way (inside the region for the region's own agents, over a direct region bridge between regions, through the
 * global only when that is the only path), and is delivered as if addressed to the agent itself.
 *
 * <p>Requirements: agent names are unique across the fabric (the global's DB already keys agents by name alone), and
 * the former region keeps no agent of the same name. Unset (the default), or equal to the node's own region, nothing
 * changes. The global controller never takes an alias.
 */
public final class RegionAlias {

    /** Config key. */
    public static final String KEY = "region_alias";

    private RegionAlias() { }

    /** The alias region of this node, or null when it has none (unset, blank, its own region, or a global). */
    public static String of(PluginBuilder plugin, boolean isGlobal) {
        if (isGlobal || plugin == null || plugin.getConfig() == null) return null;
        return of(plugin.getConfig().getStringParam(KEY), plugin.getRegion());
    }

    static String of(String alias, String region) {
        if (alias == null) return null;
        alias = alias.trim();
        if (alias.isEmpty() || alias.equals(region) || alias.contains("_") || alias.contains(".")) return null;
        return alias;
    }

    /**
     * The inbox queue of the former address: {@code inbox} (this node's own inbox queue, {@code [prefix]<region>_<agent>})
     * with the region replaced by the alias, or null when the inbox does not end in this node's own address.
     */
    public static String aliasQueue(String inbox, String alias, String region, String agent) {
        if (inbox == null || alias == null) return null;
        String own = region + "_" + agent;
        if (!inbox.endsWith(own)) return null;
        return inbox.substring(0, inbox.length() - own.length()) + alias + "_" + agent;
    }

    /**
     * A message addressed to this node's former address is re-addressed to the node itself (its plugin kept), so it
     * is delivered here. True when it was.
     */
    public static boolean normalize(MsgEvent m, String alias, String region, String agent) {
        if (alias == null || m == null) return false;
        if (!alias.equals(m.getDstRegion()) || !agent.equals(m.getDstAgent())) return false;
        m.setForwardDst(region, agent, m.getDstPlugin());
        return true;
    }
}
