package io.cresco.agent.controller.communication;

import io.cresco.library.security.ActionAuthorizer.Position;

/**
 * Where a message's speaker stands relative to the node that received it (authority follows the tree,
 * {@code docs/caller-authorization.md} section 3a). The speaker is the node whose word the caller is: a controller's or
 * plugin's own node; for an API user, the node that vouched for it ({@link MsgSigning#SPEAKER}).
 *
 * <p>One pure function of the speaker's verified name and what the receiving node knows ({@link Node}):
 * <ul>
 *   <li>{@code SELF}: the receiving node.</li>
 *   <li>{@code ABOVE}: the receiving node's parent (an agent's region controller; a region controller's global), or, for
 *       an agent, the global its parent's trust bundle names. Nothing is above a global.</li>
 *   <li>{@code BELOW}: for a region controller, any other node of its own region; for the global, any other node of its
 *       own region, or a node of a region whose controller joined it. Nothing is below an agent.</li>
 *   <li>{@code ELSEWHERE}: any other node -- a sibling agent, another region's node, another region's controller, the
 *       global seen from a region that has not joined it.</li>
 *   <li>{@code UNKNOWN}: there is no speaker to place, or this node has no name yet.</li>
 * </ul>
 * A node is named by its region and agent, as its credential verified (message signing binds a name to one key here);
 * the node itself is also held to its own tenant. O(1): a few comparisons and, at the global, one map lookup.
 */
public final class SpeakerPosition {

    private SpeakerPosition() { }

    /** What the receiving node knows when it places a speaker. Every answer is O(1) and takes no lock of its own. */
    public interface Node {
        /** This node's tenant, region and agent (null while it has no name). */
        String tenant();
        String region();
        String agent();

        /** This node is a region controller (the global is one too). */
        boolean regionController();

        /** This node is the global controller. */
        boolean global();

        /**
         * This node's parent: an agent's region controller, a region controller's global; null when it has joined none.
         * Never asked of the global.
         */
        String parentRegion();
        String parentAgent();

        /**
         * An agent only: is {@code region/agent} the global above this node's region -- named with kind {@code global}
         * in the trust bundle the parent {@code parentRegion/parentAgent} signed for this node?
         */
        boolean globalAbove(String region, String agent, String parentRegion, String parentAgent);

        /**
         * The global only: is {@code tenant/region/agent} a node of a region whose controller joined this global -- that
         * controller itself, as discovery recorded it, or any node of a region whose controller presented itself here?
         */
        boolean ofJoinedRegion(String tenant, String region, String agent);
    }

    /**
     * The position of the speaker {@code tenant/region/agent} (its verified name; {@code tenant} null when it is not
     * known) as {@code node} sees it.
     */
    public static Position of(String tenant, String region, String agent, Node node) {
        if (region == null || agent == null || node == null) return Position.UNKNOWN;
        String ownRegion = node.region(), ownAgent = node.agent();
        if (ownRegion == null || ownAgent == null) return Position.UNKNOWN;
        boolean ownRegionSpeaker = region.equals(ownRegion);
        if (ownRegionSpeaker && agent.equals(ownAgent)) {
            // this node's name under another tenant is not this node
            return tenant == null || tenant.equals(node.tenant()) ? Position.SELF : Position.ELSEWHERE;
        }
        if (node.global()) {
            // the top of the tree: nobody is above
            return ownRegionSpeaker || node.ofJoinedRegion(tenant, region, agent) ? Position.BELOW : Position.ELSEWHERE;
        }
        boolean regionController = node.regionController();
        // a region controller's own region: every other node of it is one of its agents (its global is of another region)
        if (regionController && ownRegionSpeaker) return Position.BELOW;
        String parentRegion = node.parentRegion(), parentAgent = node.parentAgent();
        if (parentRegion == null || parentAgent == null) return Position.ELSEWHERE;         // no parent joined: nobody is above
        if (region.equals(parentRegion) && agent.equals(parentAgent)) return Position.ABOVE;
        // an agent: also the global above its region, on its parent's signed word; nothing is below an agent
        if (!regionController && node.globalAbove(region, agent, parentRegion, parentAgent)) return Position.ABOVE;
        return Position.ELSEWHERE;
    }

    /**
     * As {@link #of(String, String, String, Node)} for a speaker written {@code <tenant>/<region>/<agent>}
     * ({@link MsgSigning#SPEAKER}); {@code UNKNOWN} for null or anything else.
     */
    public static Position of(String speaker, Node node) {
        if (speaker == null) return Position.UNKNOWN;
        int a = speaker.indexOf('/'), b = a < 0 ? -1 : speaker.indexOf('/', a + 1);
        if (a <= 0 || b <= a + 1 || b == speaker.length() - 1 || speaker.indexOf('/', b + 1) >= 0) return Position.UNKNOWN;
        return of(speaker.substring(0, a), speaker.substring(a + 1, b), speaker.substring(b + 1), node);
    }
}
