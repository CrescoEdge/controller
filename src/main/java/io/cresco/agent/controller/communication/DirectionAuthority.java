package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.ActionAuthorizer.Decision;
import io.cresco.library.security.ActionAuthorizer.Mode;
import io.cresco.library.security.ActionAuthorizer.Position;
import io.cresco.library.security.Caller;
import io.cresco.library.utilities.CLogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * This node's answer to "where does the speaker of this message stand, and what does a call from the wrong place meet":
 * what {@code PluginBuilder.authorize} asks for every action that has a direction, on every tier of the controller and in
 * every plugin (authority follows the tree, {@code docs/caller-authorization.md} section 3a).
 *
 * <p><b>The speaker</b> of a message is the node whose word its verified caller is: a controller's or plugin's own node
 * (message signing lets only that node sign for it; in the JVM the controller stamps it); for an API user, the node that
 * vouched for it, which this node noted on the message when it verified it ({@link MsgSigning#SPEAKER}: the signer, the
 * node the broker attests delivered it first hand, or this node for a user its own gateway sends for). An API user's
 * message without that note has no speaker this node can name ({@code UNKNOWN}).
 *
 * <p><b>The mode</b> is {@code authority_direction} ({@link SecuritySettings#authorityDirection}), read again at most
 * once a second. In {@code log} a call {@code enforce} would refuse is allowed, counted ({@link #wouldRefuseCount()};
 * {@code authz.direction.would_refuse} in the metric inventory) and logged once per action and position.
 *
 * <p>On the path of every message with a direction: a position costs a few comparisons (and at the global one map
 * lookup); nothing is allocated for a controller's or a plugin's message.
 */
public final class DirectionAuthority implements ActionAuthorizer.Authority {

    private static final Position[] POSITIONS = Position.values();
    /** Distinct actions counted by name; a plugin that declares more directions than this is counted under {@link #OTHER}. */
    static final int ACTIONS_MAX = 512;
    static final String OTHER = "other";
    private static final long MODE_READ_EVERY_MS = 1000L;

    private final ControllerEngine ce;
    private final SpeakerPosition.Node node;
    private volatile Mode mode;
    private volatile long modeReadAt = Long.MIN_VALUE / 2;

    /** Would-refuse calls by action (as the tables bound it: {@link Decision#directed}) and position; each pair's line once. */
    private final ConcurrentHashMap<String, KindTally> wouldRefuse = new ConcurrentHashMap<>();
    private final LongAdder wouldRefuseTotal = new LongAdder();
    /** Tests: every would-refuse line this node logs is handed here as well. */
    volatile Consumer<String> lineTap;

    public DirectionAuthority(ControllerEngine ce) {
        this.ce = ce;
        this.node = new EngineNode(ce);
    }

    /** What this node knows when it places a speaker (its state, the trust bundle, who joined). */
    public SpeakerPosition.Node node() { return node; }

    @Override
    public Mode mode() {
        long now = System.currentTimeMillis();
        Mode m = mode;
        if (m == null || now - modeReadAt >= MODE_READ_EVERY_MS || now < modeReadAt) {
            m = SecuritySettings.authorityDirection(ce.getPluginBuilder().getConfig());
            mode = m;
            modeReadAt = now;
        }
        return m;
    }

    @Override
    public Position positionOf(MsgEvent msg, Caller caller) {
        if (caller == null) return Position.UNKNOWN;
        switch (caller.getKind()) {
            case CONTROLLER:
            case PLUGIN:
            case AGENT:
                // its own node speaks: only that node's key signs for it, only its own controller stamps it in the JVM
                return SpeakerPosition.of(caller.getTenant(), caller.getRegion(), caller.getAgent(), node);
            case API:
                // the node that vouched for the user, as this node noted it when it verified the message
                return SpeakerPosition.of(MsgSigning.speakerOf(msg), node);
            default:
                return Position.UNKNOWN;
        }
    }

    /** The speaker of {@code msg} in words, for a log line: {@code <tenant>/<region>/<agent>}, or that there is none. */
    static String speakerName(MsgEvent msg, Caller caller) {
        if (caller != null && caller.getKind() != Caller.Kind.API && caller.getRegion() != null) {
            return caller.getTenant() + "/" + caller.getRegion() + "/" + caller.getAgent();
        }
        String noted = MsgSigning.speakerOf(msg);
        return noted == null ? "no node this node can name" : noted;
    }

    /**
     * A call that {@code enforce} would refuse was allowed (mode {@code log}): counted under its action, and logged the
     * first time that action is called from that position --
     * {@code AUTHORITY: WOULD REFUSE <type> <action> from <caller>, the word of <speaker> (<POSITION>): it is honoured
     * only from the node itself or a controller above it; allowed (authority_direction=log); logged once per action and
     * position, every one counted in authz.direction.would_refuse}.
     */
    @Override
    public void wouldRefuse(MsgEvent msg, Decision d) {
        if (msg == null || d == null || d.position == null) return;
        wouldRefuseTotal.increment();
        String action = d.directed == null ? OTHER : d.directed;
        KindTally tally = wouldRefuse.get(action);
        if (tally == null) {
            if (wouldRefuse.size() >= ACTIONS_MAX) action = OTHER;
            tally = wouldRefuse.computeIfAbsent(action, a -> new KindTally(POSITIONS.length, Long.MAX_VALUE));
        }
        if (!tally.count(d.position.ordinal())) return;
        String line = "AUTHORITY: WOULD REFUSE " + msg.getMsgType() + " " + d.action + " from " + d.caller + ", the word of "
                + speakerName(msg, d.caller) + " (" + d.position + ")"
                + (msg.getDstPlugin() == null ? "" : " for plugin " + msg.getDstPlugin())
                + ": it " + ActionAuthorizer.honouredFrom(d.direction) + "; allowed (authority_direction=log); logged once per action"
                + " and position, every one counted in authz.direction.would_refuse";
        try {
            CLogger log = ce.getPluginBuilder().getLogger(DirectionAuthority.class.getName(), CLogger.Level.Info);
            if (log != null) log.warn(line);
        } catch (RuntimeException ignore) {
            // the count stands
        }
        Consumer<String> tap = lineTap;
        if (tap != null) tap.accept(line);
    }

    /** Calls allowed in mode {@code log} that {@code enforce} would refuse, so far. */
    public long wouldRefuseCount() { return wouldRefuseTotal.sum(); }

    /** As {@link #wouldRefuseCount()}, of one action ({@link Decision#directed}; {@link #OTHER} past {@link #ACTIONS_MAX} actions). */
    public long wouldRefuseCount(String action) {
        KindTally t = action == null ? null : wouldRefuse.get(action);
        if (t == null) return 0L;
        long n = 0;
        for (int i = 0; i < t.kinds(); i++) n += t.of(i);
        return n;
    }

    /** As {@link #wouldRefuseCount(String)}, from one position. */
    public long wouldRefuseCount(String action, Position position) {
        KindTally t = action == null ? null : wouldRefuse.get(action);
        return t == null ? 0L : t.of(position.ordinal());
    }

    /** Would-refuse calls of actions that have no meter of their own ({@code names}: the actions that have one). */
    public long wouldRefuseCountBesides(java.util.Set<String> names) {
        long n = 0;
        for (String action : wouldRefuse.keySet()) if (!names.contains(action)) n += wouldRefuseCount(action);
        return n;
    }

    /** The receiving node as the engine knows it: its state, its trust bundle, who joined it. */
    private static final class EngineNode implements SpeakerPosition.Node {
        private final ControllerEngine ce;

        EngineNode(ControllerEngine ce) { this.ce = ce; }

        @Override public String tenant() { return ce.tenant(); }

        @Override public String region() { ControllerStateImp cs = ce.cstate; return cs == null ? null : cs.getRegion(); }

        @Override public String agent() { ControllerStateImp cs = ce.cstate; return cs == null ? null : cs.getAgent(); }

        @Override public boolean regionController() { ControllerStateImp cs = ce.cstate; return cs != null && cs.isRegionalController(); }

        @Override public boolean global() { ControllerStateImp cs = ce.cstate; return cs != null && cs.isGlobalController(); }

        @Override
        public String parentRegion() {
            ControllerStateImp cs = ce.cstate;
            if (cs == null) return null;
            return cs.isRegionalController() ? cs.getGlobalRegion() : cs.getRegionalRegion();
        }

        @Override
        public String parentAgent() {
            ControllerStateImp cs = ce.cstate;
            if (cs == null) return null;
            return cs.isRegionalController() ? cs.getGlobalAgent() : cs.getRegionalAgent();
        }

        @Override
        public boolean globalAbove(String region, String agent, String parentRegion, String parentAgent) {
            return ce.msgSigning().isGlobalAbove(region, agent, parentRegion, parentAgent);
        }

        @Override
        public boolean ofJoinedRegion(String tenant, String region, String agent) {
            // any node of a region whose controller presented itself here on its pings
            if (ce.msgSigning().isChildRegion(region)) return true;
            // before its first ping, the region's controller itself: as discovery recorded it joining
            return ce.joinedAsController(tenant, region, agent);
        }
    }
}
