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
 * the node itself is also held to its own tenant. O(1): a few comparisons and, at the global, one map lookup; no lock is
 * taken and nothing is allocated for a controller's or a plugin's speaker.
 */
public final class SpeakerPosition {

    private SpeakerPosition() { }

    /**
     * A node as it stands at one moment: its name, what it is, and its parent. Immutable, and replaced whole on every
     * change of the node's state ({@code ControllerStateImp}), so a reader never sees half of a change -- the name of one
     * state with the parent or the role of another.
     */
    public static final class Standing {
        /** This node's region and agent; null while it has no name. */
        public final String region, agent;
        /** This node is a region controller (the global is one too); this node is the global controller. */
        public final boolean regionController, global;
        /**
         * This node's parent: an agent's region controller, a region controller's global. Both null when it has joined
         * none, and for the global.
         */
        public final String parentRegion, parentAgent;

        public Standing(String region, String agent, boolean regionController, boolean global, String parentRegion, String parentAgent) {
            this.region = region;
            this.agent = agent;
            this.regionController = regionController || global;
            this.global = global;
            boolean parent = !global && parentRegion != null && parentAgent != null;
            this.parentRegion = parent ? parentRegion : null;
            this.parentAgent = parent ? parentAgent : null;
        }

        /** A node that has no name yet: it places nobody. */
        public static final Standing NONE = new Standing(null, null, false, false, null, null);

        @Override
        public String toString() {
            return region + "/" + agent + (global ? " (global)" : regionController ? " (region controller)" : " (agent)")
                    + (parentRegion == null ? "" : " under " + parentRegion + "/" + parentAgent);
        }
    }

    /**
     * What the receiving node knows when it places a speaker. No answer enters a monitor or blocks: each is a volatile
     * read of an immutable value, a comparison, or (the global) one concurrent map lookup; nothing is allocated.
     */
    public interface Node {
        /** This node's tenant (configuration: it does not change with the node's state). */
        String tenant();

        /** This node as it stands now: one immutable snapshot, never half of a state change. Null: not known. */
        Standing standing();

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
     * known) as {@code node} sees it. The node's state is read once: a state change while a speaker is placed gives the
     * position of the state before or of the state after, never a mix.
     */
    public static Position of(String tenant, String region, String agent, Node node) {
        if (region == null || agent == null || node == null) return Position.UNKNOWN;
        Standing s = node.standing();
        if (s == null || s.region == null || s.agent == null) return Position.UNKNOWN;
        boolean ownRegionSpeaker = region.equals(s.region);
        if (ownRegionSpeaker && agent.equals(s.agent)) {
            // this node's name under another tenant is not this node
            return tenant == null || tenant.equals(node.tenant()) ? Position.SELF : Position.ELSEWHERE;
        }
        if (s.global) {
            // the top of the tree: nobody is above
            return ownRegionSpeaker || node.ofJoinedRegion(tenant, region, agent) ? Position.BELOW : Position.ELSEWHERE;
        }
        // a region controller's own region: every other node of it is one of its agents (its global is of another region)
        if (s.regionController && ownRegionSpeaker) return Position.BELOW;
        if (s.parentRegion == null || s.parentAgent == null) return Position.ELSEWHERE;     // no parent joined: nobody is above
        if (region.equals(s.parentRegion) && agent.equals(s.parentAgent)) return Position.ABOVE;
        // an agent: also the global above its region, on its parent's signed word; nothing is below an agent
        if (!s.regionController && node.globalAbove(region, agent, s.parentRegion, s.parentAgent)) return Position.ABOVE;
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
