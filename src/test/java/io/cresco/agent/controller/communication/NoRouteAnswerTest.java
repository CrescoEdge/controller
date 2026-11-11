package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.Fixture;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.SessionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A request for a node this node's broker has no way to is answered, not queued. Before: the global handed a request for
 * an agent of a region whose controller was down to its broker, where it waited in a queue nothing consumed; the caller
 * (a client at the global's gateway) waited out its whole timeout, and a request still there when the region came back
 * was run after its caller had given up.
 *
 * <p>The node r1/a1 of {@link RegionalRebuildWindowPingTest} as a controller with a broker (the gate is told of its
 * consumers directly); what it hands its broker is taken by {@code toBroker}, what it answers by {@code f.sent}.
 */
class NoRouteAnswerTest {

    private static final AtomicInteger ids = new AtomicInteger();
    private final List<Fixture> fixtures = new ArrayList<>();
    private final List<MsgEvent> toBroker = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        for (Fixture f : fixtures) f.engine.routeGate().stop();
    }

    private static ConsumerId consumer() {
        return new ConsumerId(new SessionId(new ConnectionId("n" + ids.incrementAndGet()), 1), 1);
    }

    /** The queue the stub client sends a message to, as AgentProducer names it in tenant t1. */
    private static String queue(String node) { return "T.t1." + node; }

    /** The controller r1/a1 in {@code mode}; its broker has been up for a minute. */
    private Fixture controller(ControllerMode mode, Map<String, Object> cfg) throws Exception {
        Fixture f = new Fixture(mode, cfg);
        fixtures.add(f);
        Field client = ControllerEngine.class.getDeclaredField("activeClient");
        client.setAccessible(true);
        client.set(f.engine, new ActiveClient(f.engine) {
            @Override public boolean sendMessage(MsgEvent sm) { return toBroker.add(sm); }
            @Override public boolean sendAPMessage(MsgEvent msg) { return toBroker.add(msg); }
            @Override public String queueOf(MsgEvent msg) { return msg.getForwardDst() == null ? null : queue(msg.getForwardDst()); }
        });
        f.engine.routeGate().bound(System.currentTimeMillis() - 60_000L);
        return f;
    }

    /** This broker saw the last consumer of {@code inbox} (a queue as subscribed) leave a minute ago: its node or bridge is gone. */
    private static void gone(Fixture f, String inbox) {
        ConsumerId was = consumer();
        f.engine.routeGate().consumerAdded(was, new ActiveMQQueue(inbox));
        f.engine.routeGate().consumerRemoved(was, System.currentTimeMillis() - 60_000L);
    }

    /** A request of the gateway plugin of this node (r1/a1/plugin/9) for the plugin plugin/2 of the agent r2/a21. */
    private static MsgEvent request(String action) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin/9", "r2", "a21", "plugin/2", false, false);
        m.setParam("action", action);
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a1-plugin/9", "call-7");
        return m;
    }

    private static MsgEvent onlyReply(Fixture f) {
        assertEquals(1, f.sent.size(), "exactly one answer: " + f.sent);
        MsgEvent reply = f.sent.get(0);
        String hop = reply.getParam(MsgRouter.ROUTE_UNAVAILABLE);            // the hop the route table chose for it
        assertNotNull(hop, reply.getParams().toString());
        assertTrue(String.valueOf(reply.getParam("error")).startsWith("route unavailable at r1_a1: " + hop + ": no route to r2_a21"),
                reply.getParams().toString());
        assertEquals("r1", reply.getDstRegion());
        assertEquals("a1", reply.getDstAgent());
        assertEquals("plugin/9", reply.getDstPlugin());
        assertEquals("call-7", reply.getParam("callId-r1-a1-plugin/9"), "the answer must reach the call that waits for it");
        assertFalse(MsgRouter.isRpcRequest(reply), "an error reply is never answered again");
        return reply;
    }

    private static void await(java.util.function.BooleanSupplier c, long ms, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(10);
        assertTrue(c.getAsBoolean(), what);
    }

    @Test
    void aRequestForANodeBehindADeadBridgeIsAnsweredAtOnceAndNeverSent() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of());
        gone(f, queue("r2_a21"));                                    // the demand of r2's bridge left with the bridge
        long t0 = System.nanoTime();
        f.router.route(request("list"));
        long tookMs = (System.nanoTime() - t0) / 1_000_000L;
        onlyReply(f);
        assertTrue(tookMs < 500, "the caller waited " + tookMs + " ms for the answer that there is no route");
        assertTrue(toBroker.isEmpty(), "the request was left in a queue nothing consumes: " + toBroker);
        assertEquals(1, f.engine.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE));
        assertEquals(1, f.engine.msgCounts().droppedCount(), "counted once");
    }

    @Test
    void aRequestForANodeTheBrokerHasAWayToIsSent() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of());
        f.engine.routeGate().consumerAdded(consumer(), new ActiveMQQueue(queue("r2_a21")));   // the demand of r2's bridge
        f.router.route(request("list"));
        assertEquals(1, toBroker.size());
        assertTrue(f.sent.isEmpty(), f.sent.toString());
        assertEquals(0, f.engine.msgCounts().droppedCount());
    }

    @Test
    void aRegionControllerAnswersForAnAgentOfItsRegionThatIsNotConnected() throws Exception {
        Fixture f = controller(ControllerMode.REGION_GLOBAL, Map.of());
        gone(f, queue("r1_a5"));                                     // the agent's own consumer went with its connection
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin/9", "r1", "a5", "plugin/2", false, false);
        m.setParam("action", "list");
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a1-plugin/9", "call-7");
        f.router.route(m);
        assertEquals(1, f.sent.size(), f.sent.toString());
        assertNotNull(f.sent.get(0).getParam(MsgRouter.ROUTE_UNAVAILABLE));
        assertTrue(String.valueOf(f.sent.get(0).getParam("error")).contains("no route to r1_a5"), f.sent.get(0).getParams().toString());
        assertTrue(toBroker.isEmpty());
    }

    @Test
    void aRequestWhoseRouteJustWentIsHeldAndGoesOnWhenTheRouteIsBack() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of("route_rebuild_grace_ms", "5000"));
        RouteGate gate = f.engine.routeGate();
        ConsumerId bridge = consumer();
        gate.consumerAdded(bridge, new ActiveMQQueue(queue("r2_a21")));
        gate.consumerRemoved(bridge, System.currentTimeMillis());                    // the bridge to r2 broke this moment
        f.router.route(request("list"));
        Thread.sleep(300);
        assertTrue(toBroker.isEmpty(), "a request without a route was handed to the broker");
        assertTrue(f.sent.isEmpty(), "a route that may be back in a moment was answered as gone: " + f.sent);
        assertEquals(1, gate.heldNow());
        gate.consumerAdded(consumer(), new ActiveMQQueue(queue("r2_a21")));           // the bridge is back
        await(() -> toBroker.size() == 1, 2000, "the held request did not go on when its route was back");
        assertTrue(f.sent.isEmpty(), f.sent.toString());
        assertEquals(0, f.engine.msgCounts().droppedCount());
        assertEquals(1, gate.heldCount());
    }

    @Test
    void aRequestWhoseRouteDoesNotComeBackIsAnsweredWhenTheGraceEnds() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of("route_rebuild_grace_ms", "400"));
        RouteGate gate = f.engine.routeGate();
        ConsumerId bridge = consumer();
        gate.consumerAdded(bridge, new ActiveMQQueue(queue("r2_a21")));
        long lost = System.currentTimeMillis();
        gate.consumerRemoved(bridge, lost);
        f.router.route(request("list"));
        assertTrue(f.sent.isEmpty(), "answered before the grace ended");
        await(() -> !f.sent.isEmpty(), 2000, "a held request was never answered: its caller waits out its timeout");
        long after = System.currentTimeMillis() - lost;
        assertTrue(after >= 380 && after < 1500, "answered " + after + " ms after the route went (grace 400 ms)");
        MsgEvent reply = onlyReply(f);
        assertTrue(String.valueOf(reply.getParam("error")).contains("none came while the request was held"), reply.getParams().toString());
        assertTrue(toBroker.isEmpty(), "a request answered as undeliverable was sent");
        assertEquals(1, f.engine.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE));
        // and a request that comes once the grace is over is answered at once
        f.sent.clear();
        f.router.route(request("list"));
        onlyReply(f);
    }

    @Test
    void aRequestIsHeldForAtMostHalfOfItsCallersTimeout() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of("route_rebuild_grace_ms", "30000"));
        RouteGate gate = f.engine.routeGate();
        ConsumerId bridge = consumer();
        gate.consumerAdded(bridge, new ActiveMQQueue(queue("r2_a21")));
        gate.consumerRemoved(bridge, System.currentTimeMillis());
        MsgEvent m = request("list");
        m.setParam(RpcExpiry.TIMEOUT_PARAM, "600");
        long t0 = System.currentTimeMillis();
        f.router.route(m);
        await(() -> !f.sent.isEmpty(), 2000, "held past its caller's timeout");
        long after = System.currentTimeMillis() - t0;
        assertTrue(after >= 280 && after < 600, "answered after " + after + " ms; the caller waits 600 ms");
    }

    @Test
    void aRequestThatOvertookTheDemandOfANewBridgeWaitsForItAndGoesOn() throws Exception {
        // this broker never had a consumer for r2/a21 (r2 is joining): the request must not fail for arriving first
        Fixture f = controller(ControllerMode.GLOBAL, Map.of());
        f.router.route(request("list"));
        Thread.sleep(200);
        assertTrue(f.sent.isEmpty(), "answered as gone a moment before its route was there: " + f.sent);
        assertTrue(toBroker.isEmpty());
        f.engine.routeGate().consumerAdded(consumer(), new ActiveMQQueue(queue("r2_a21")));
        await(() -> toBroker.size() == 1, 2000, "the request did not go on when the bridge's demand arrived");
        assertTrue(f.sent.isEmpty(), f.sent.toString());
    }

    @Test
    void aRequestForANodeThisBrokerNeverReachedIsAnsweredOneGraceLater() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of("route_rebuild_grace_ms", "300"));
        long t0 = System.currentTimeMillis();
        f.router.route(request("list"));
        await(() -> !f.sent.isEmpty(), 2000, "a request for a node nobody ever saw waits out its caller's timeout");
        long after = System.currentTimeMillis() - t0;
        assertTrue(after >= 280 && after < 1500, "answered after " + after + " ms (grace 300 ms)");
        onlyReply(f);
        assertTrue(toBroker.isEmpty());
    }

    @Test
    void aPingWithoutARouteIsDroppedAndNotAnswered() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of());
        gone(f, queue("r2_a21"));
        f.router.route(request("ping"));
        Thread.sleep(200);
        assertTrue(f.sent.isEmpty(), "an error reply to a ping reads as a pong: " + f.sent);
        assertTrue(toBroker.isEmpty(), "a ping nobody can take was queued, to be delivered after its pinger gave up");
        assertEquals(1, f.engine.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE));
    }

    @Test
    void whatIsNoRequestIsSentAsBefore() throws Exception {
        Fixture f = controller(ControllerMode.GLOBAL, Map.of());
        MsgEvent notice = new MsgEvent(MsgEvent.Type.INFO, "r1", "a1", "plugin/9", "r2", "a21", "plugin/2", false, false);
        f.router.route(notice);
        // an answer this node originates for a caller elsewhere (its call id is the destination's: not a request)
        MsgEvent reply = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin/9", "r2", "a21", "plugin/2", false, false);
        reply.setParam("is_rpc", "true");
        reply.setParam("callId-r2-a21-plugin/2", "call-1");
        assertFalse(MsgRouter.isRpcRequest(reply));
        f.router.route(reply);
        assertEquals(2, toBroker.size(), "only an RPC request is judged");
        assertTrue(f.sent.isEmpty());
        assertEquals(0, f.engine.msgCounts().droppedCount());
    }

    @Test
    void aNodeWithoutABrokerSendsAsBefore() throws Exception {
        Fixture f = controller(ControllerMode.AGENT, Map.of());
        f.state.regionalRegion = "r1";
        f.state.regionalAgent = "rc";
        f.engine.routeGate().unbound();                                              // an agent: nothing to ask
        f.router.route(request("list"));
        assertEquals(1, toBroker.size());
        assertTrue(f.sent.isEmpty());
    }

    @Test
    void aRegionsRequestForItsGlobalWhoseBridgeIsGoneIsAnswered() throws Exception {
        Fixture f = controller(ControllerMode.REGION_GLOBAL, Map.of());
        f.state.globalRegion = "rg";
        f.state.globalAgent = "g1";
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", null, "r1", "a1", null, true, true);   // an agent's, for the global
        m.setParam("action", "resourceinfo");
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a2-null", "call-3");
        m.setParam("ttl", "1");
        assertTrue(ControllerTiers.targetsGlobal(m));
        gone(f, "T.*.rg_g1");                                        // the global restarts: its bridge's demand is gone
        f.engine.msgIn(m, true);
        await(() -> !f.sent.isEmpty(), 3000, "the request waited for a global whose bridge is gone");
        MsgEvent reply = f.sent.get(0);
        assertEquals("REMOTE_GLOBAL", reply.getParam(MsgRouter.ROUTE_UNAVAILABLE), reply.getParams().toString());
        assertEquals("a2", reply.getDstAgent());
        assertTrue(toBroker.isEmpty());
        assertEquals(1, f.engine.msgCounts().droppedCount(MsgCounts.Drop.NO_ROUTE));
        // with the bridge there, it is carried on as before
        f.engine.routeGate().consumerAdded(consumer(), new ActiveMQQueue("T.*.rg_g1"));
        MsgEvent again = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", null, "r1", "a1", null, true, true);
        again.setParam("action", "resourceinfo");
        again.setParam("is_rpc", "true");
        again.setParam("callId-r1-a2-null", "call-4");
        again.setParam("ttl", "1");
        f.engine.msgIn(again, true);
        await(() -> toBroker.size() == 1, 3000, "a request for the global was not carried on over its bridge");
    }
}
