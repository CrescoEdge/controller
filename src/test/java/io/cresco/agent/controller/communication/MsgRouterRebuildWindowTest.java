package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.Fixture;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The other windows in which a component route() dispatches into is missing: each either waits (bounded by
 * route_ready_wait_ms) for a transient rebuild, or gives up at once; an RPC request always gets an error reply
 * (params error + route_unavailable) instead of a silent timeout, a non-RPC is dropped with a log line, and
 * nothing throws.
 */
class MsgRouterRebuildWindowTest {

    private static final Map<String, Object> SHORT_WAIT = Map.of("route_ready_wait_ms", "300");

    /** A global-scoped RPC from the wsapi on r1/a2 to the node r1/a1 (route 20975 / 29039). */
    private static MsgEvent globalRpc(String action) {
        return rpc(action, true);
    }

    private static MsgEvent rpc(String action, boolean global) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", "plugin/1", "r1", "a1", null, true, global);
        m.setParam("action", action);
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a2-plugin/1", "call-9");
        m.setParam("ttl", "1");
        return m;
    }

    private static void assertErrorReply(Fixture f, String reasonPart) {
        assertEquals(1, f.sent.size(), f.sent.toString());
        MsgEvent reply = f.sent.get(0);
        assertNotNull(reply.getParam(MsgRouter.ROUTE_UNAVAILABLE), reply.getParams().toString());
        assertTrue(String.valueOf(reply.getParam("error")).contains(reasonPart), reply.getParams().toString());
        // back to the caller, under its own call id
        assertEquals("r1", reply.getDstRegion());
        assertEquals("a2", reply.getDstAgent());
        assertEquals("plugin/1", reply.getDstPlugin());
        assertEquals("call-9", reply.getParam("callId-r1-a2-plugin/1"));
        assertFalse(MsgRouter.isRpcRequest(reply), "an error reply must never be answered again");
    }

    @Test
    void aGlobalRpcAtARegionThatHasNoGlobalIsAnsweredWithAnErrorAtOnce() {
        Fixture f = new Fixture(ControllerMode.REGION_GLOBAL_FAILED, Map.of("route_ready_wait_ms", "5000"));
        long t0 = System.currentTimeMillis();
        f.router.route(globalRpc("listagents"));
        assertTrue(System.currentTimeMillis() - t0 < 2000, "a global outage is not waited on");
        assertErrorReply(f, "no global controller joined");
    }

    @Test
    void aGlobalMessageWhileTheGlobalSchedulersStartWaitsForThem() throws Exception {
        Fixture f = new Fixture(ControllerMode.GLOBAL_INIT, Map.of("route_ready_wait_ms", "5000"));
        // a region's ping to its global (region -> global link check), route 29007
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "r2", "rc2", null, "r1", "a1", null, true, true);
        ping.setParam("action", "ping");
        ping.setParam("is_rpc", "true");
        ping.setParam("callId-r2-rc2-null", "call-9");
        ping.setParam("ttl", "1");
        assertEquals(29007, MsgRouter.routePath(ping, true, true, "r1", "a1", null));
        Thread starter = new Thread(() -> {
            try { Thread.sleep(300); } catch (InterruptedException ignored) { }
            f.engine.setResourceScheduler(new ResourceScheduler(f.engine));
            f.engine.setAppScheduler(new AppScheduler(f.engine));
        });
        starter.start();
        f.router.route(ping);
        starter.join();

        assertEquals(1, f.sent.size(), f.sent.toString());
        MsgEvent pong = f.sent.get(0);
        assertEquals("pong", pong.getParam("action"), pong.getParams().toString());
        assertNull(pong.getParam(MsgRouter.ROUTE_UNAVAILABLE), pong.getParams().toString());
    }

    @Test
    void aGlobalRpcWhoseSchedulersNeverStartGetsAnErrorAfterTheBoundedWait() {
        Fixture f = new Fixture(ControllerMode.GLOBAL_INIT, SHORT_WAIT);
        long t0 = System.currentTimeMillis();
        f.router.route(globalRpc("listagents"));
        long took = System.currentTimeMillis() - t0;
        assertTrue(took >= 250 && took < 3000, "bounded wait, took " + took);
        assertErrorReply(f, "global controller starting");
    }

    @Test
    void aRegionalRpcDuringShutdownIsAnsweredWithAnErrorAtOnce() {
        Fixture f = new Fixture(ControllerMode.REGION_SHUTDOWN, Map.of("route_ready_wait_ms", "5000"));
        MsgEvent m = rpc("listplugins", false);
        long t0 = System.currentTimeMillis();
        f.router.route(m);
        assertTrue(System.currentTimeMillis() - t0 < 2000);
        assertErrorReply(f, "shutting down");
    }

    @Test
    void anAgentRegionalRpcWhileItsRegionalControllerIsUnknownGetsAnError() {
        // an agent mid re-init (AGENT_INIT after the region was lost): no regional controller to forward to.
        // Route 17391 (local plugin -> own regional controller) used to be forwarded to null_null.
        Fixture f = new Fixture(ControllerMode.AGENT_INIT, SHORT_WAIT);
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin/1", "r1", "a1", null, true, false);
        m.setParam("action", "listagents");
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a1-plugin/1", "call-3");
        m.setParam("ttl", "0");
        f.router.route(m);

        assertEquals(1, f.sent.size(), f.sent.toString());
        MsgEvent reply = f.sent.get(0);
        assertTrue(String.valueOf(reply.getParam("error")).contains("regional controller not known"), reply.getParams().toString());
        assertEquals("plugin/1", reply.getDstPlugin());
    }

    @Test
    void anAgentRegionalMessageIsForwardedOnceItsRegionalControllerIsKnown() {
        Fixture f = new Fixture(ControllerMode.AGENT_INIT, Map.of("route_ready_wait_ms", "5000"));
        MsgEvent m = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a1", null, true, false);
        m.setParam("ttl", "0");
        Thread joined = new Thread(() -> {
            try { Thread.sleep(300); } catch (InterruptedException ignored) { }
            f.state.regionalRegion = "r1";
            f.state.regionalAgent = "rc1";
            f.state.mode = ControllerMode.AGENT;
        });
        joined.start();
        f.router.route(m);
        // forwarded to the now-known regional controller (the broker send itself fails here: no broker)
        assertEquals("r1_rc1", m.getForwardDst());
    }

    @Test
    void aNonRpcThatCannotBeDeliveredIsDroppedWithoutAReply() {
        Fixture f = new Fixture(ControllerMode.REGION_SHUTDOWN, SHORT_WAIT);
        MsgEvent wd = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a2", null, "r1", "a1", null, true, false);
        wd.setParam("ttl", "1");
        f.router.route(wd);
        assertTrue(f.sent.isEmpty(), f.sent.toString());
        assertTrue(f.db.nodeUpdates.isEmpty());
    }

    @Test
    void aLivenessPingThatCannotBeServedIsNotAnsweredWithAnError() {
        // a pinger reads any reply as a pong: during shutdown the ping must record a miss, not a false pong
        Fixture f = new Fixture(ControllerMode.REGION_SHUTDOWN, SHORT_WAIT);
        f.router.route(RegionalRebuildWindowPingTest.agentPing());
        assertTrue(f.sent.isEmpty(), f.sent.toString());
    }

    @Test
    void anRpcReplyIsNotARequest() {
        MsgEvent m = globalRpc("x");
        assertTrue(MsgRouter.isRpcRequest(m));
        m.setReturn();
        assertFalse(MsgRouter.isRpcRequest(m));
    }
}
