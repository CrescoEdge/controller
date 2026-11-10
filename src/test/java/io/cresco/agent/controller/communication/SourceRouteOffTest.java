package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.Fixture;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The refusals between regional controllers (2026-11-10, ≥2 regions + a global): every 45 s each regional controller
 * cost-probed each peer region "via the global" with a source route (global;peer) although source routing
 * (net_source_routing) was off. The global, which relays source routes only with it on, took the probe as addressed to
 * itself and refused it (its signed recipient is the peer); its 401 reply kept the probe's source route, so it was
 * signed for the peer and the prober refused it in turn. Here the global is r1/a1 with source routing off; the
 * prober is r2/rc2 and its peer r3/rc3.
 */
class SourceRouteOffTest {

    /** r2/rc2's RPC to r3/rc3, source-routed through r1/a1 ({@code route}), as it reaches r1/a1. */
    private static MsgEvent viaGlobal(String action, String route) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r2", "rc2", null, "r3", "rc3", null, false, false);
        m.setParam("action", action);
        m.setParam("is_rpc", "true");
        m.setParam("callId-r2-rc2-null", "call-1");
        m.setParam("ttl", "1");
        m.setParam(MsgRouter.SRCROUTE, route);
        m.setParam(MsgRouter.SRCROUTE_DST_PLUGIN, "");
        m.setForwardDst("r1", "a1", null);
        return m;
    }

    private static MsgEvent firstSent(Fixture f) throws InterruptedException {
        for (int i = 0; i < 60 && f.sent.isEmpty(); i++) Thread.sleep(50);
        assertFalse(f.sent.isEmpty(), "nothing was sent");
        return f.sent.get(0);
    }

    /** The reply goes back to the caller and is signed for it (the signer names a source route's last node). */
    private static void assertForTheCaller(MsgEvent reply) {
        assertNull(reply.getParam(MsgRouter.SRCROUTE), reply.getParams().toString());
        assertEquals("r2", reply.getDstRegion());
        assertEquals("rc2", reply.getDstAgent());
        assertArrayEquals(new String[]{"r2", "rc2", null}, MsgSigning.effectiveDestination(reply), reply.getParams().toString());
    }

    @Test
    void aWaypointWithSourceRoutingOffDoesNotAnswerAPingMeantForTheNodeBeyondIt() throws Exception {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        f.router.route(viaGlobal("ping", "r1,a1;r3,rc3"));
        Thread.sleep(500);
        // before: delivered here as addressed to r1/a1 and answered with r1/a1's pong (with signing on: refused here)
        assertTrue(f.sent.isEmpty(), "r1/a1 answered a ping meant for r3/rc3: " + f.sent);
    }

    @Test
    void aWaypointWithSourceRoutingOffAnswersAnRpcBeyondItAsUnavailable() throws Exception {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        f.router.route(viaGlobal("listplugins", "r1,a1;r3,rc3"));
        MsgEvent reply = firstSent(f);
        assertNotNull(reply.getParam(MsgRouter.ROUTE_UNAVAILABLE), "run on r1/a1 instead: " + reply.getParams());
        assertTrue(String.valueOf(reply.getParam("error")).contains("net_source_routing"), reply.getParams().toString());
        assertForTheCaller(reply);
    }

    @Test
    void aSourceRouteThatEndsHereIsDeliveredAndItsReplyIsSignedForTheCaller() throws Exception {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = viaGlobal("ping", "r1,a1");
        f.router.route(m);
        MsgEvent pong = firstSent(f);
        assertEquals("pong", pong.getParam("action"), pong.getParams().toString());
        assertForTheCaller(pong);
    }

    @Test
    void aWaypointWithSourceRoutingOnRelaysTheRoute() throws Exception {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of("net_source_routing", "true"));
        f.router.route(viaGlobal("ping", "r1,a1;r3,rc3"));
        Thread.sleep(300);
        // relayed to r3/rc3 (the broker send fails in this fixture: no broker, and a ping is not answered then)
        assertTrue(f.sent.isEmpty(), f.sent.toString());
    }

    @Test
    void aRefusedSourceRoutedRequestIsAnsweredToItsCaller() {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = viaGlobal("ping", "r1,a1;r3,rc3");
        assertTrue(MsgRouter.answerUnauthenticated(f.plugin, m, "a message signed for r3/rc3, not for this node"));
        assertEquals(1, f.sent.size(), f.sent.toString());
        MsgEvent reply = f.sent.get(0);
        assertEquals("401", reply.getParam("status_code"));
        // before: signed for r3/rc3 and refused by r2/rc2, which then timed out
        assertForTheCaller(reply);
    }

    @Test
    void anErrorReplyToASourceRoutedRequestIsAnsweredToItsCaller() {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = viaGlobal("listplugins", "r3,rc3");
        assertTrue(MsgRouter.answerWithError(f.plugin, m, MsgRouter.ROUTE_UNAVAILABLE, "x", "test"));
        assertForTheCaller(f.sent.get(0));
    }
}
