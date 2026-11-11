package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutorWindowAccess;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import org.apache.activemq.command.ActiveMQTextMessage;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A fabric of real controller engines for the authority tests (real identity keys, real message signing; keys travel as
 * in production, on the pings): the global rgG/gc with an agent of its own region rgG/ga; two regions that joined it,
 * rgA/rc (agents a1, a2) and rgB/rc2 (agent b1); and a region that did not, rgX/rcx (agent x1). Every node of tenant t1,
 * every node with {@code authority_direction} as given.
 *
 * <p>{@link #send} is what a node's controller does to a message it originates (stamp, note, sign as it leaves, the wire);
 * {@link #arrive} is what a node's inbox does to a message off the wire (AgentConsumer: the caller from the broker's
 * properties, the reserved and local parameters a sender wrote removed, the signature checked, the speaker noted).
 */
final class AuthorityFabric {

    static final String T = "t1";
    private static final Gson GSON = new Gson();

    final ControllerEngine g, ga, rA, rB, a1, a2, b1, rX, x1;
    /** What each node's controller sent (its answers: a 403 goes out this way). */
    private final Map<ControllerEngine, List<MsgEvent>> sent = new ConcurrentHashMap<>();

    AuthorityFabric(String mode) throws Exception {
        g = node("rgG", "gc", true, mode);
        ((TlsTestSupport.State) g.cstate).global = true;
        ga = node("rgG", "ga", false, mode);
        rA = node("rgA", "rc", true, mode);
        rB = node("rgB", "rc2", true, mode);
        a1 = node("rgA", "a1", false, mode);
        a2 = node("rgA", "a2", false, mode);
        b1 = node("rgB", "b1", false, mode);
        rX = node("rgX", "rcx", true, mode);
        x1 = node("rgX", "x1", false, mode);
        joined(rA, g, true);
        joined(rB, g, true);
        joined(ga, g, false);
        joined(a1, rA, false);
        joined(a2, rA, false);
        joined(b1, rB, false);
        joined(x1, rX, false);
        // keys travel: regions to the global, the bundle down, agents' attestations (twice: the second pong carries the
        // bundle with both regions in it)
        for (int i = 0; i < 2; i++) {
            ping(rA, g, true);
            ping(rB, g, true);
        }
        for (int i = 0; i < 2; i++) {
            ping(ga, g, false);
            ping(a1, rA, false);
            ping(a2, rA, false);
            ping(b1, rB, false);
            ping(x1, rX, false);
        }
    }

    private ControllerEngine node(String region, String agent, boolean regional, String mode) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        if (mode != null) cfg.put("authority_direction", mode);
        List<MsgEvent> out = new CopyOnWriteArrayList<>();
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg, out::add);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        // the controller's own AgentService, as AgentServiceImpl answers it: security on, its tenant, the engine's authority
        TestAgentService as = (TestAgentService) e.getPluginBuilder().getAgentService();
        as.securityEnabled = true;
        as.tenant = T;
        as.authority = e.authority();
        sent.put(e, out);
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    /** {@code joiner} joined {@code controller} (discovery): each binds the other's key, and the joiner knows its parent. */
    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        TlsTestSupport.State s = (TlsTestSupport.State) joiner.cstate;
        if (joinerIsController) {
            controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
            s.globalRegion = controller.cstate.getRegion();
            s.globalAgent = controller.cstate.getAgent();
        } else {
            s.parentRegion = controller.cstate.getRegion();
            s.parentAgent = controller.cstate.getAgent();
        }
    }

    static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    /** One ping/pong between child and parent, as the health watchers and executors run it. */
    static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller(T, child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);    // the verified caller
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    static Caller controller(ControllerEngine e) { return Caller.controller(T, e.cstate.getRegion(), e.cstate.getAgent()); }

    static Caller plugin(ControllerEngine e) { return Caller.plugin(T, e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    static Caller admin() { return Caller.api(T, "root", List.of("admin")); }

    static CrescoIdentity identity(ControllerEngine e) { return CrescoIdentity.of(T, e.cstate.getRegion(), e.cstate.getAgent(), null); }

    /** A request of {@code type}/{@code action} from {@code from} to {@code to}'s controller (an RPC, so a refusal is answered). */
    static MsgEvent request(MsgEvent.Type type, String action, ControllerEngine from, ControllerEngine to, boolean regional, boolean global) {
        MsgEvent m = new MsgEvent(type, from.cstate.getRegion(), from.cstate.getAgent(), null,
                to.cstate.getRegion(), to.cstate.getAgent(), null, regional, global);
        if (action != null) m.setParam("action", action);
        m.setParam("is_rpc", "true");
        // its own call id, as RPC.call gives every call (two identical messages in one millisecond are one signature: a replay)
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-null", java.util.UUID.randomUUID().toString());
        return m;
    }

    /**
     * What {@code from} sends as {@code caller}: stamped at its controller's stamping point (for an API user: the gateway's,
     * which notes this node as the speaker), signed as it leaves, and as it is on the wire.
     */
    static MsgEvent send(ControllerEngine from, Caller caller, MsgEvent m) {
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        if (caller.getKind() == Caller.Kind.API) from.msgSigning().spokenHere(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /**
     * What {@code to}'s inbox does with {@code m} off the wire, as AgentConsumer does it: the caller is taken from the
     * broker's properties ({@code declared}: what the sender's connection declared and its broker let through), whatever
     * the body says about caller, signing state and speaker is removed, and the message is verified -- {@code deliveredBy}
     * is the node the broker attests delivered it first hand (a direct link), or null. Null when it is refused there.
     */
    static MsgEvent arrive(ControllerEngine to, MsgEvent m, Caller declared, ControllerEngine deliveredBy) throws Exception {
        ActiveMQTextMessage jms = new ActiveMQTextMessage();
        CallerStamps.toJms(declared, jms);
        if (deliveredBy != null) {
            jms.setStringProperty(CallerStamps.P_DIRECT, T + "/" + deliveredBy.cstate.getRegion() + "/" + deliveredBy.cstate.getAgent());
        }
        if (m.getParams() != null && !(m.getParams() instanceof HashMap)) m.setParams(new HashMap<>(m.getParams()));
        Caller caller = CallerStamps.fromJms(m, jms, true);
        TlsTestSupport.State s = (TlsTestSupport.State) to.cstate;
        // a controller consumes from its own broker; an agent from its parent's, whose word it takes for the parent only
        CrescoIdentity firstHand = AgentConsumer.trustedFirstHand(CallerStamps.directOf(jms), s.regional, s.parentRegion, s.parentAgent);
        MsgSigning.Verdict v = to.msgSigning().verify(m, caller, false, caller == null ? null : firstHand, false);
        if (!v.ok()) {
            lastRefusal = v.refusal;
            return null;
        }
        if (caller == null || !caller.equals(v.caller)) CallerStamps.stamp(m, v.caller, null);
        return m;
    }

    /** Why the last message {@link #arrive} refused did not verify (for a failing test's message). */
    static volatile String lastRefusal;

    /** {@link #send} then {@link #arrive}: over the wire through relays (signed), nothing attested first hand. */
    static MsgEvent deliver(ControllerEngine from, Caller caller, MsgEvent m, ControllerEngine to) throws Exception {
        return arrive(to, send(from, caller, m), caller, null);
    }

    /** The agent tier's admission of {@code m} at {@code to} (its controller's PluginBuilder, as its message processor asks). */
    static boolean admittedByAgentTier(ControllerEngine to, MsgEvent m) {
        return to.getPluginBuilder().admit(m);
    }

    /** The regional tier's admission of {@code m} at {@code to} (the global tier's when {@code global}). */
    static boolean admittedByControllerTier(ControllerEngine to, MsgEvent m, boolean global) {
        return RegionalExecutorWindowAccess.admitted(to.getRegionalExecutor(), m, global);
    }

    /** What {@code e}'s controller sent so far (cleared by {@link #clearSent}). */
    List<MsgEvent> sent(ControllerEngine e) { return sent.get(e); }

    void clearSent() { for (List<MsgEvent> l : sent.values()) l.clear(); }
}
