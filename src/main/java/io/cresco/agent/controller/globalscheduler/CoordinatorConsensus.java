package io.cresco.agent.controller.globalscheduler;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netmetrics.CoordinatorRegistry;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinator consensus, epoch fencing, and leader failover (Phase D / W5) for a fabric with MORE THAN ONE
 * global. Sits on top of {@link CoordinatorRegistry} (which enumerates the live coordinator set and computes
 * a deterministic leader from the shared RouteView) and adds the two things a strong duty needs: a
 * monotonic <b>epoch</b> that fences stale leaders, and a <b>majority quorum</b> so a cross-domain verdict
 * (D3 region-LOST) or global-optimization commit (D6) is only durable when f+1 of 2f+1 coordinators agree.
 *
 * <p><b>Transport.</b> Coordinators heartbeat over the data plane (GLOBAL topic, selector
 * {@code cresco_msg_type='coord_beat'}, NON_PERSISTENT, TTL-bounded) — the same push bus as link-state, no
 * new connection. Each beat carries the sender, its view of the leader, its epoch, and a timestamp (lease).
 *
 * <p><b>Election + epoch.</b> The leader is the deterministic {@link CoordinatorRegistry#leader()} over the
 * live set. When leadership changes to THIS node, it bumps the epoch to {@code max(epoch seen)+1} and claims
 * it in its beats — so a new term always strictly exceeds every prior term. Any state-mutating control
 * message stamped with an epoch strictly less than the highest observed is <b>rejected</b>
 * ({@link #acceptEpoch}), which is the split-brain guard: a partitioned old leader cannot corrupt state.
 *
 * <p><b>Quorum.</b> {@link #quorum()} = floor(N/2)+1 over the live coordinator set; {@link #hasQuorum()} is
 * true when at least that many coordinators are currently heartbeating. A leader that cannot see a quorum
 * must not commit strong-duty state (it fences itself), which is exactly the harvest/yield behaviour under a
 * coordinator-set partition (Phase F).
 */
public final class CoordinatorConsensus {

    static final String SELECTOR_PROP = "cresco_msg_type";
    static final String SELECTOR_VAL = "coord_beat";
    private static final String SELECTOR = SELECTOR_PROP + " = '" + SELECTOR_VAL + "'";

    private final ControllerEngine ce;
    private final PluginBuilder plugin;
    private final CLogger logger;
    private final CoordinatorRegistry registry;
    private final Gson gson = new Gson();
    private final long leaseMs;
    private final String selfPath;

    private volatile boolean subscribed = false;
    private String listenerId;

    private volatile long epoch = 0;                 // this node's current term
    private volatile String leader = null;           // last computed/known leader
    private volatile long highestEpochSeen = 0;      // for fencing
    private volatile String lastLogSig = "";
    private volatile int rejectedStaleEpochs = 0;
    private volatile int maxCoordinatorsSeen = 0;    // stable-membership baseline for quorum

    /** Last-heard timestamp per coordinator (lease tracking). */
    private final ConcurrentHashMap<String, Long> beats = new ConcurrentHashMap<>();
    /** Ack collection for an in-flight quorum proposal, keyed by proposalId. */
    private final ConcurrentHashMap<String, Map<String, Boolean>> proposalAcks = new ConcurrentHashMap<>();

    public CoordinatorConsensus(ControllerEngine ce, CoordinatorRegistry registry, long leaseMs) {
        this.ce = ce;
        this.plugin = ce.getPluginBuilder();
        this.logger = plugin.getLogger(CoordinatorConsensus.class.getName(), CLogger.Level.Info);
        this.registry = registry;
        this.leaseMs = Math.max(3000L, leaseMs);
        this.selfPath = ce.cstate.getRegion() + "_" + ce.cstate.getAgent();
    }

    public long epoch() { return epoch; }
    public String leader() { return leader; }
    public boolean isLeader() { return selfPath.equals(leader); }
    public int rejectedStaleEpochs() { return rejectedStaleEpochs; }

    /** Live coordinators = those whose last beat is within the lease (plus self if coordinator). */
    public List<String> liveCoordinators() {
        long cutoff = System.currentTimeMillis() - leaseMs;
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Long> e : beats.entrySet()) if (e.getValue() >= cutoff) out.add(e.getKey());
        if (registry.selfIsCoordinator() && !out.contains(selfPath)) out.add(selfPath);
        return out;
    }

    /**
     * The stable membership size quorum is measured against — NOT the currently-live count (a lone survivor
     * must not believe it has a majority). Uses configured {@code coordinator_expected} when set, else the
     * high-water mark of coordinators ever seen together.
     */
    public int membershipSize() {
        int expected = 0;
        try { expected = plugin.getConfig().getIntegerParam("coordinator_expected", 0); } catch (Exception ignore) { }
        if (expected > 0) return expected;
        return Math.max(maxCoordinatorsSeen, liveCoordinators().size());
    }

    /** Majority of the stable membership: floor(M/2)+1. */
    public int quorum() { return (membershipSize() / 2) + 1; }

    /** True iff a majority of the STABLE membership is currently reachable (may commit strong duties). */
    public boolean hasQuorum() { return liveCoordinators().size() >= quorum(); }

    /**
     * Epoch fence: accept a control message stamped {@code msgEpoch} iff it is not older than the highest
     * epoch we have observed. A stale (partitioned old-leader) epoch is rejected and counted.
     */
    public boolean acceptEpoch(long msgEpoch) {
        if (msgEpoch < highestEpochSeen) {
            rejectedStaleEpochs++;
            logger.warn("EPOCH-FENCE: rejected stale-epoch control message epoch=" + msgEpoch
                    + " < highest=" + highestEpochSeen + " (partitioned/old leader; split-brain prevented)");
            return false;
        }
        if (msgEpoch > highestEpochSeen) highestEpochSeen = msgEpoch;
        return true;
    }

    /** Called each tick (from the AutoTuner cycle). Recomputes leadership, bumps epoch on change, beats. */
    public void tick() {
        try {
            if (!ce.getActiveClient().isFaultURIActive()) return;
            ensureSubscribed();
            String newLeader = registry.leader();

            // Leadership transition to THIS node -> start a new, strictly-higher term.
            if (newLeader != null && newLeader.equals(selfPath) && !selfPath.equals(leader)) {
                epoch = Math.max(epoch, highestEpochSeen) + 1;
                highestEpochSeen = Math.max(highestEpochSeen, epoch);
                logger.info("LEADER-ELECT: this coordinator (" + selfPath + ") became leader; new epoch=" + epoch
                        + " over " + registry.coordinators().size() + " coordinator(s), quorum=" + quorum());
            }
            leader = newLeader;

            publishBeat();
            maxCoordinatorsSeen = Math.max(maxCoordinatorsSeen, liveCoordinators().size());
            logIfChanged();
        } catch (Exception ex) {
            logger.debug("CoordinatorConsensus.tick error: " + ex.getMessage());
        }
    }

    private void logIfChanged() {
        String sig = leader + "|e" + epoch + "|n" + liveCoordinators().size() + "|q" + quorum() + "|Q" + hasQuorum();
        if (!sig.equals(lastLogSig)) {
            lastLogSig = sig;
            logger.info("COORD-STATE leader=" + leader + " epoch=" + epoch + " live=" + liveCoordinators().size()
                    + " quorum=" + quorum() + " hasQuorum=" + hasQuorum() + " coordinators=" + registry.coordinators());
        }
    }

    /**
     * Leader-only: propose a strong-duty commit (e.g. "region X is LOST") and require a majority ack. Returns
     * true if committed (quorum reached). Simplified synchronous model: the proposal is broadcast with the
     * leader's epoch; followers that accept the epoch ack; the leader counts acks against {@link #quorum()}.
     */
    public boolean proposeQuorumCommit(String subject) {
        if (!isLeader()) { logger.warn("proposeQuorumCommit called on non-leader; ignored"); return false; }
        if (!hasQuorum()) {
            logger.warn("VERDICT-BLOCKED: no quorum (" + liveCoordinators().size() + "<" + quorum()
                    + ") -> refusing to commit '" + subject + "' (region-first survives; verdict deferred)");
            return false;
        }
        String pid = selfPath + ":" + epoch + ":" + subject.hashCode();
        Map<String, Boolean> acks = new ConcurrentHashMap<>();
        acks.put(selfPath, true); // leader self-acks
        proposalAcks.put(pid, acks);
        publishProposal(pid, subject);
        // Give followers a moment to ack over the push bus.
        long deadline = System.currentTimeMillis() + Math.min(leaseMs, 4000L);
        while (System.currentTimeMillis() < deadline) {
            if (acks.size() >= quorum()) break;
            try { Thread.sleep(100L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        int got = acks.size();
        proposalAcks.remove(pid);
        boolean committed = got >= quorum();
        logger.info("QUORUM-COMMIT subject='" + subject + "' epoch=" + epoch + " acks=" + got + "/" + quorum()
                + " -> " + (committed ? "COMMITTED" : "FAILED"));
        return committed;
    }

    // ---- transport ----

    private synchronized void ensureSubscribed() {
        if (subscribed) return;
        try {
            DataPlaneService dps = ce.getDataPlaneService();
            if (dps == null) return;
            MessageListener ml = new MessageListener() {
                @Override public void onMessage(Message m) { onBeat(m); }
            };
            listenerId = dps.addMessageListener(TopicType.GLOBAL, ml, SELECTOR);
            subscribed = true;
            logger.info("CoordinatorConsensus subscribed to coordinator beats (selector " + SELECTOR + ")");
        } catch (Exception ex) {
            logger.debug("CoordinatorConsensus.ensureSubscribed error: " + ex.getMessage());
        }
    }

    private void publishBeat() {
        Map<String, Object> beat = new HashMap<>();
        beat.put("t", "beat");
        beat.put("from", selfPath);
        beat.put("leader", leader);
        beat.put("epoch", epoch);
        beat.put("ts", System.currentTimeMillis());
        send(beat);
        // count our own beat too
        beats.put(selfPath, System.currentTimeMillis());
    }

    private void publishProposal(String pid, String subject) {
        Map<String, Object> p = new HashMap<>();
        p.put("t", "propose");
        p.put("from", selfPath);
        p.put("pid", pid);
        p.put("subject", subject);
        p.put("epoch", epoch);
        p.put("ts", System.currentTimeMillis());
        send(p);
    }

    private void publishAck(String pid, long propEpoch) {
        Map<String, Object> a = new HashMap<>();
        a.put("t", "ack");
        a.put("from", selfPath);
        a.put("pid", pid);
        a.put("epoch", propEpoch);
        a.put("ts", System.currentTimeMillis());
        send(a);
    }

    private void send(Map<String, Object> obj) {
        try {
            DataPlaneService dps = ce.getDataPlaneService();
            if (dps == null) return;
            TextMessage tm = dps.createTextMessage();
            tm.setText(gson.toJson(obj));
            tm.setStringProperty(SELECTOR_PROP, SELECTOR_VAL);
            dps.sendMessage(TopicType.GLOBAL, tm, DeliveryMode.NON_PERSISTENT, 0, (int) leaseMs);
        } catch (Exception ex) {
            logger.debug("CoordinatorConsensus.send error: " + ex.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void onBeat(Message message) {
        try {
            if (!(message instanceof TextMessage)) return;
            Map<String, Object> obj = gson.fromJson(((TextMessage) message).getText(), Map.class);
            if (obj == null) return;
            String from = (String) obj.get("from");
            if (from == null || from.equals(selfPath)) return;
            String type = (String) obj.get("t");
            long msgEpoch = obj.get("epoch") == null ? 0 : (long) Math.round(((Number) obj.get("epoch")).doubleValue());
            if (msgEpoch > highestEpochSeen) highestEpochSeen = msgEpoch;

            if ("beat".equals(type)) {
                beats.put(from, System.currentTimeMillis());
            } else if ("propose".equals(type)) {
                // A follower acks a proposal iff the proposer's epoch is current (fencing).
                String pid = (String) obj.get("pid");
                if (acceptEpoch(msgEpoch) && registry.selfIsCoordinator()) {
                    publishAck(pid, msgEpoch);
                }
            } else if ("ack".equals(type)) {
                String pid = (String) obj.get("pid");
                Map<String, Boolean> acks = proposalAcks.get(pid);
                if (acks != null) acks.put(from, true);
            }
        } catch (Exception ex) {
            logger.debug("CoordinatorConsensus.onBeat parse error: " + ex.getMessage());
        }
    }

    public void shutdown() {
        try {
            if (listenerId != null && ce.getDataPlaneService() != null) {
                ce.getDataPlaneService().removeMessageListener(listenerId);
            }
        } catch (Exception ignore) { }
    }
}
