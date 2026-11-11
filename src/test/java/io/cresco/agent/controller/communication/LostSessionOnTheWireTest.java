package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The retry of a request refused for a lost session, through everything a request goes through: two secured agents of
 * different regions, each with its real inbox, router and producer on one broker; a real RPC of the one's controller to
 * the other. ({@link LostSessionRecoveryTest} proves the rules on a model inbox and wire, where what is sent again leaves
 * through a tap; here it leaves through {@code ActiveClient.sendMessage}, is signed by the producer, crosses the broker,
 * and the caller's {@code sendRPC} returns the answer.)
 */
class LostSessionOnTheWireTest {

    private static final Gson GSON = new Gson();
    private static final String VM = "vm://lostwire?create=false";
    private BrokerService broker;
    private final List<ControllerEngine> online = new ArrayList<>();

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("lostwire");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterEach
    void stop() throws Exception {
        for (ControllerEngine e : online) {
            try { e.getActiveClient().stopHolding(); } catch (Exception ignore) { }
            try { e.getActiveClient().shutdown(); } catch (Exception ignore) { }
        }
        broker.stop();
        broker.waitUntilStopped();
    }

    /** A secured node; what its controller sends goes the way AgentServiceImpl.msgOut sends it (stamped, then routed). */
    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");
        cfg.put("route_ready_wait_ms", "100");
        cfg.put("msg_signing_hold_ms", "0");                     // these nodes hold their trust data before they talk
        ControllerEngine[] self = new ControllerEngine[1];
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg, m -> {
            self[0].stampSelf(m);
            self[0].msgIn(m);
        });
        ((TlsTestSupport.State) e.cstate).regional = regional;
        self[0] = e;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
    }

    private static MsgEvent copy(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = copy(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(copy(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    private void online(ControllerEngine e) {
        assertTrue(e.getActiveClient().initActiveAgentProducer(VM));
        assertTrue(e.getActiveClient().initActiveAgentConsumer(e.cstate.getAgentPath(), VM));
        online.add(e);
    }

    /**
     * A call of {@code a}'s controller to a plugin {@code b} does not run: b's router answers it with an error (route
     * unavailable). The cheapest request that is dispatched at b and answered -- each time it reaches b's dispatch, b
     * counts one message for a plugin it has no admin for: how often the request "ran".
     */
    private static MsgEvent call(ControllerEngine a, ControllerEngine b, String what) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, a.cstate.getRegion(), a.cstate.getAgent(), null,
                b.cstate.getRegion(), b.cstate.getAgent(), "plugin-x", false, false);
        m.setParam("action", "noop");
        m.setParam("what", what);
        return a.getPluginBuilder().sendRPC(m, 10_000);
    }

    private static long ranAt(ControllerEngine b) { return b.msgCounts().droppedCount(MsgCounts.Drop.PLUGIN_ADMIN_UNAVAILABLE); }

    @Test
    void aRequestRefusedForALostSessionIsSentAgainThroughTheProducerAndTheCallReturnsItsAnswer() throws Exception {
        ControllerEngine g = node("rgG", "gc", true), rA = node("rgA", "rc", true), rB = node("rgB", "rc2", true);
        ((TlsTestSupport.State) g.cstate).global = true;
        ControllerEngine a = node("rgA", "a1", false), b = node("rgB", "b1", false);
        joined(rA, g, true);
        joined(rB, g, true);
        joined(a, rA, false);
        joined(b, rB, false);
        ((TlsTestSupport.State) a.cstate).parentRegion = "rgA";
        ((TlsTestSupport.State) a.cstate).parentAgent = "rc";
        ((TlsTestSupport.State) b.cstate).parentRegion = "rgB";
        ((TlsTestSupport.State) b.cstate).parentAgent = "rc2";
        for (int i = 0; i < 2; i++) {
            ping(rA, g, true);
            ping(rB, g, true);
        }
        ping(a, rA, false);
        ping(b, rB, false);
        online(a);
        online(b);

        // a and b talk until they hold a session (requests one way, answers the other)
        for (int i = 0; i < 6 && a.msgSigning().sessionWith("rgB_b1") == null; i++) {
            MsgEvent answer = call(a, b, "warm-up " + i);
            assertNotNull(answer, "b answers (warm-up " + i + ")");
            assertNotNull(answer.getParam(MsgRouter.ROUTE_UNAVAILABLE));
        }
        assertNotNull(a.msgSigning().sessionWith("rgB_b1"), "a sends b messages under a session");
        long macBefore = a.msgSigning().macSentCount();

        b.msgSigning().restartSessions();                        // b restarted
        Thread.sleep(200);
        long ran = ranAt(b), refused = b.msgSigning().refusedCount(MsgSigning.Refusal.SESSION_KEY);
        MsgEvent answer = call(a, b, "after the restart");

        assertEquals(macBefore + 1, a.msgSigning().macSentCount(), "the request left under the session b had lost");
        assertEquals(refused + 1, b.msgSigning().refusedCount(MsgSigning.Refusal.SESSION_KEY), "b refused it");
        assertEquals(1, a.msgSigning().lostRetriedCount(), "a sent it again");
        assertNotNull(answer, "and the call returned");
        assertNull(answer.getParam(MsgSigning.LOST));
        assertNotEquals("401", answer.getParam("status_code"), "with b's answer, not with the refusal");
        assertNotNull(answer.getParam(MsgRouter.ROUTE_UNAVAILABLE), "b's answer to that request");
        assertEquals("after the restart", answer.getParam("what"));
        assertEquals(ran + 1, ranAt(b), "the request reached b's dispatch once");
        assertEquals(0, a.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER) + a.msgCounts().droppedCount(MsgCounts.Drop.SEND_FAILED));
        // and the two agree a new session over the same path
        for (int i = 0; i < 6 && a.msgSigning().sessionWith("rgB_b1") == null; i++) assertNotNull(call(a, b, "again " + i));
        assertNotNull(a.msgSigning().sessionWith("rgB_b1"));
    }
}
