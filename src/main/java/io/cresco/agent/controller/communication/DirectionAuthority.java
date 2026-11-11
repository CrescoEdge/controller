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
 * {@code authz.direction.would_refuse} in the metric inventory) and logged once per action and position. In
 * {@code enforce} it is refused, and counted ({@link #refusedCount()}; {@code authz.direction.refused}).
 *
 * <p>On the path of every message with a direction (every WATCHDOG and KPI among them): a position is one volatile read
 * of this node's standing ({@code ControllerStateImp.standing()}), a few comparisons and, at the global, one concurrent
 * map lookup. No monitor is entered -- never the controller state's synchronized getters -- and nothing is allocated
 * for a controller's or a plugin's message.
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
    /** This node's tenant (configuration), read with the mode: placing a speaker reads no configuration. */
    private volatile String tenant;

    /** Would-refuse calls by action (as the tables bound it: {@link Decision#directed}) and position; each pair's line once. */
    private final ConcurrentHashMap<String, KindTally> wouldRefuse = new ConcurrentHashMap<>();
    private final LongAdder wouldRefuseTotal = new LongAdder();
    /** Calls refused for their direction (mode {@code enforce}), counted the same way. */
    private final ConcurrentHashMap<String, KindTally> refused = new ConcurrentHashMap<>();
    private final LongAdder refusedTotal = new LongAdder();
    /** Tests: every would-refuse line this node logs is handed here as well. */
    volatile Consumer<String> lineTap;

    public DirectionAuthority(ControllerEngine ce) {
        this.ce = ce;
        this.node = new EngineNode();
    }

    /** What this node knows when it places a speaker (its state, the trust bundle, who joined). */
    public SpeakerPosition.Node node() { return node; }

    @Override
    public Mode mode() {
        long now = System.currentTimeMillis();
        Mode m = mode;
        if (m == null || now - modeReadAt >= MODE_READ_EVERY_MS || now < modeReadAt) {
            m = SecuritySettings.authorityDirection(ce.getPluginBuilder().getConfig());
            tenant = ce.tenant();
            mode = m;
            modeReadAt = now;
        }
        return m;
    }

    /** This node's tenant, as last read with the mode (read now when it never was). */
    private String tenant() {
        String t = tenant;
        if (t == null) {
            t = ce.tenant();
            tenant = t;
        }
        return t;
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
        if (!count(wouldRefuse, d)) return;
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

    /**
     * Count {@code d} in {@code tallies} under its action (as the tables bound it) and position; true the first time that
     * action is counted from that position. The actions are bounded by what is declared, and by {@link #ACTIONS_MAX}.
     */
    private static boolean count(ConcurrentHashMap<String, KindTally> tallies, Decision d) {
        String action = d.directed == null ? OTHER : d.directed;
        KindTally tally = tallies.get(action);
        if (tally == null) {
            if (tallies.size() >= ACTIONS_MAX) action = OTHER;
            tally = tallies.computeIfAbsent(action, a -> new KindTally(POSITIONS.length, Long.MAX_VALUE));
        }
        return tally.count(d.position.ordinal());
    }

    private static long of(ConcurrentHashMap<String, KindTally> tallies, String action) {
        KindTally t = action == null ? null : tallies.get(action);
        if (t == null) return 0L;
        long n = 0;
        for (int i = 0; i < t.kinds(); i++) n += t.of(i);
        return n;
    }

    private static long besides(ConcurrentHashMap<String, KindTally> tallies, java.util.Set<String> names) {
        long n = 0;
        for (String action : tallies.keySet()) if (!names.contains(action)) n += of(tallies, action);
        return n;
    }

    /**
     * A call was refused for where its speaker stands (mode {@code enforce}; the 403 and its log line are
     * {@code PluginBuilder.admit}'s): counted under its action -- {@code authz.direction.refused} in the metric inventory.
     */
    @Override
    public void refused(MsgEvent msg, Decision d) {
        if (d == null || d.position == null || !d.directionRefused) return;
        refusedTotal.increment();
        count(refused, d);
    }

    /** Calls refused for their direction (mode {@code enforce}), so far. */
    public long refusedCount() { return refusedTotal.sum(); }

    /** As {@link #refusedCount()}, of one action ({@link Decision#directed}; {@link #OTHER} past {@link #ACTIONS_MAX} actions). */
    public long refusedCount(String action) { return of(refused, action); }

    /** As {@link #refusedCount(String)}, from one position. */
    public long refusedCount(String action, Position position) {
        KindTally t = action == null ? null : refused.get(action);
        return t == null ? 0L : t.of(position.ordinal());
    }

    /** Refused calls of actions that have no meter of their own ({@code names}: the actions that have one). */
    public long refusedCountBesides(java.util.Set<String> names) { return besides(refused, names); }

    /** Calls allowed in mode {@code log} that {@code enforce} would refuse, so far. */
    public long wouldRefuseCount() { return wouldRefuseTotal.sum(); }

    /** As {@link #wouldRefuseCount()}, of one action ({@link Decision#directed}; {@link #OTHER} past {@link #ACTIONS_MAX} actions). */
    public long wouldRefuseCount(String action) { return of(wouldRefuse, action); }

    /** As {@link #wouldRefuseCount(String)}, from one position. */
    public long wouldRefuseCount(String action, Position position) {
        KindTally t = action == null ? null : wouldRefuse.get(action);
        return t == null ? 0L : t.of(position.ordinal());
    }

    /** Would-refuse calls of actions that have no meter of their own ({@code names}: the actions that have one). */
    public long wouldRefuseCountBesides(java.util.Set<String> names) { return besides(wouldRefuse, names); }

    /**
     * The receiving node as the engine knows it: its standing (one volatile read of the controller state's immutable
     * snapshot -- never its synchronized getters), its tenant as last read, its trust bundle, who joined it.
     */
    private final class EngineNode implements SpeakerPosition.Node {

        @Override public String tenant() { return DirectionAuthority.this.tenant(); }

        @Override
        public SpeakerPosition.Standing standing() {
            ControllerStateImp cs = ce.cstate;
            return cs == null ? null : cs.standing();
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
