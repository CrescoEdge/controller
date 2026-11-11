package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.TenantNamespace;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.SessionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The no-route answer with tenants: two tenants' regions under a global of the superuser tenant, on engines that stamp
 * and name queues as the fabric does (nothing here is told a queue name by the test).
 *
 * <p>First: the queue a request is SENT to is the queue its node CONSUMES -- the gate matches names, and with tenant
 * namespacing a request travels in the namespace of the node it is for while a controller reads its inbox in every
 * namespace; were the two computed differently, every request would be answered {@code route unavailable}.
 * Second: a tenant's caller asking for another tenant's node learns nothing from how the request fails.
 */
class NoRouteAcrossTenantsTest {

    private static final String SU = "cresco-system";
    private static final Set<String> SUS = Set.of(SU);
    private static final String VM = "vm://localhost";
    private static final AtomicInteger ids = new AtomicInteger();
    private static KeyPair k1, k2;

    private BrokerService broker;
    private final List<Connection> connections = new ArrayList<>();
    private final List<AgentProducer> producers = new ArrayList<>();
    private final List<ControllerEngine> engines = new ArrayList<>();
    private final List<MsgEvent> answered = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void keys() throws Exception {
        k1 = TlsTestSupport.rsa();
        k2 = TlsTestSupport.rsa();
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (AgentProducer p : producers) p.shutdown();
        for (ControllerEngine e : engines) {
            e.routeGate().stop();
            e.getActiveClient().shutdown();
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /** The controller {@code region/agent} of {@code tenant}; what it answers goes to {@code answered}. */
    private ControllerEngine controller(String tenant, String region, String agent, boolean global) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, Map.of("tenant_id", tenant, "route_rebuild_grace_ms", "400"), answered::add);
        TlsTestSupport.State s = (TlsTestSupport.State) e.cstate;
        s.regional = true;
        s.global = global;
        engines.add(e);
        return e;
    }

    /** Give {@code e} this JVM's broker "localhost", told of its consumers, and a real producer on it. */
    private void broker(ControllerEngine e) throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setPlugins(new BrokerPlugin[]{e.routeGate().plugin()});
        broker.start();
        broker.waitUntilStarted();
        producer(e);
    }

    private AgentProducer producer(ControllerEngine e) throws Exception {
        AgentProducer p = new AgentProducer(e, VM, e.getActiveClient());
        producers.add(p);
        Field f = ActiveClient.class.getDeclaredField("agentProducer");
        f.setAccessible(true);
        f.set(e.getActiveClient(), p);
        return p;
    }

    private static void joined(PeerTrustRegistry at, String tenant, String region, String agent, KeyPair key) {
        assertTrue(at.bind(tenant, region, agent, key.getPublic(), "test", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        at.allowBridgePeer(region, agent);
    }

    /** A consumer of the inbox of {@code path} as that node itself names it (MsgRouter.inboxQueue). */
    private MessageConsumer inbox(String path, boolean controller, String tenant) throws Exception {
        Connection c = new ActiveMQConnectionFactory(VM + "?create=false").createConnection();
        connections.add(c);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return s.createConsumer(s.createQueue(MsgRouter.inboxQueue(path, controller, true, tenant, SUS)));
    }

    private static MsgEvent request(ControllerEngine from, String region, String agent) {
        // of a plugin of that node (its gateway): the answer to it goes out through the node's sender, where the test sees it
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "plugin/9", region, agent, null, false, false);
        m.setParam("action", "iscontrolleractive");
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-plugin/9", "call-1");
        from.stampSelf(m);                                                   // as every send: its caller, its namespace
        return m;
    }

    private static void await(java.util.function.BooleanSupplier c, long ms, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(10);
        assertTrue(c.getAsBoolean(), what);
    }

    @Test
    void theQueueARequestIsSentToIsTheQueueItsNodeConsumesInEveryTenant() throws Exception {
        ControllerEngine gc = controller(SU, "glob", "gc", true);
        broker(gc);
        PeerTrustRegistry peers = gc.getCertificateManager().getPeerTrust();
        joined(peers, "ta", "rta", "rcta", k1);
        joined(peers, "tb", "rtb", "rctb", k2);
        RouteGate gate = gc.routeGate();
        ActiveClient client = gc.getActiveClient();
        long now = System.currentTimeMillis();

        // nothing consumes anything yet: the gate does not take a queue for consumed because it can name it
        String leafA = client.queueOf(request(gc, "rta", "ata1"));
        assertEquals("T.ta.rta_ata1", leafA, "the superuser global sends into the tenant of the region it addresses");
        assertNotEquals(RouteGate.ROUTE, gate.noRouteFrom(leafA, now));
        assertFalse(ActiveBroker.queueHasConsumer(broker, leafA), "the broker's own account");

        // each node consumes its inbox under the name it gives it itself (what a bridge's demand mirrors)
        MessageConsumer a1 = inbox("rta_ata1", false, "ta");                 // a tenant's leaf: its own tenant's namespace
        inbox("rta_rcta", true, "ta");                                       // a tenant's controller: every namespace
        inbox("rtb_atb1", false, "tb");
        await(() -> gate.noRouteFrom(leafA, System.currentTimeMillis()) == RouteGate.ROUTE, 3000, "a leaf's inbox, consumed, has no route");
        for (String[] node : new String[][]{{"rta", "ata1"}, {"rta", "rcta"}, {"rtb", "atb1"}}) {
            String queue = client.queueOf(request(gc, node[0], node[1]));
            assertEquals(RouteGate.ROUTE, gate.noRouteFrom(queue, System.currentTimeMillis()),
                    "a request for " + node[0] + "/" + node[1] + " goes to " + queue + ", which the gate does not find consumed");
            assertTrue(ActiveBroker.queueHasConsumer(broker, queue), queue + " by the broker's own account");
        }
        // the other way: a tenant's controller for the superuser global, which reads its inbox in every namespace
        ControllerEngine rcta = controller("ta", "rta", "rcta", false);
        String toGlobal = producer(rcta).queueOf(request(rcta, "glob", "gc"));
        assertEquals("T.ta.glob_gc", toGlobal, "a tenant's request travels in its own namespace");
        assertNotEquals(RouteGate.ROUTE, gate.noRouteFrom(toGlobal, System.currentTimeMillis()));
        inbox("glob_gc", true, SU);
        await(() -> gate.noRouteFrom(toGlobal, System.currentTimeMillis()) == RouteGate.ROUTE, 3000,
                "the global reads T.*.glob_gc and a tenant's request for it goes to " + toGlobal + ": the gate does not match them");
        // a node of one tenant is not reached in another's namespace, and the gate does not say it is
        assertNotEquals(RouteGate.ROUTE, gate.noRouteFrom("T.tb.rta_ata1", System.currentTimeMillis()));
        assertFalse(ActiveBroker.queueHasConsumer(broker, "T.tb.rta_ata1"));

        // and through the router: sent while the leaf consumes, answered once it is gone
        MsgRouter router = new MsgRouter(gc);
        router.route(request(gc, "rta", "ata1"));
        assertNotNull(a1.receive(5000), "a request for a node whose inbox is consumed was not sent");
        assertTrue(answered.isEmpty(), answered.toString());
        a1.close();
        await(() -> gate.noRouteFrom(leafA, System.currentTimeMillis()) != RouteGate.ROUTE, 3000, "the leaf's consumer left and the gate still has it");
        router.route(request(gc, "rta", "ata1"));
        await(() -> !answered.isEmpty(), 5000, "a request for a node that left was not answered");
        assertNotNull(answered.get(0).getParam(MsgRouter.ROUTE_UNAVAILABLE), answered.get(0).getParams().toString());
        assertTrue(String.valueOf(answered.get(0).getParam("error")).contains("no route to rta_ata1"), answered.get(0).getParams().toString());
    }

    // ---- another tenant's caller ----

    private static ConsumerId consumer() {
        return new ConsumerId(new SessionId(new ConnectionId("x" + ids.incrementAndGet()), 1), 1);
    }

    /** The global with a client that takes what is sent; the gate is told of consumers by hand (their names as above). */
    private ControllerEngine globalWithoutABroker(List<MsgEvent> toBroker) throws Exception {
        ControllerEngine gc = controller(SU, "glob", "gc", true);
        Field client = ControllerEngine.class.getDeclaredField("activeClient");
        client.setAccessible(true);
        client.set(gc, new ActiveClient(gc) {
            @Override public boolean sendMessage(MsgEvent sm) { return toBroker.add(sm); }
            @Override public String queueOf(MsgEvent msg) {
                return TenantNamespace.qualify(msg.getParam(TenantNamespace.TENANT_PARAM), msg.getForwardDst());
            }
        });
        gc.routeGate().bound(System.currentTimeMillis() - 60_000L);
        PeerTrustRegistry peers = gc.getCertificateManager().getPeerTrust();
        joined(peers, "ta", "rta", "rcta", k1);
        joined(peers, "tb", "rtb", "rctb", k2);
        return gc;
    }

    /** A request of {@code caller} (a node of region rtb) that the global relays for the controller {@code region/agent}. */
    private static MsgEvent relayed(Caller caller, String region, String agent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rtb", "atb1", null, region, agent, null, false, false);
        m.setParam("action", "iscontrolleractive");
        m.setParam("is_rpc", "true");
        m.setParam("callId-rtb-atb1-null", "call-2");
        caller.stamp(m);
        m.setParam(TenantNamespace.TENANT_PARAM, caller.getTenant());        // a tenant's request travels in its own namespace
        return m;
    }

    /** Milliseconds until the global answered {@code m}, and the answer's error text. */
    private Object[] answer(ControllerEngine gc, MsgEvent m) throws Exception {
        answered.clear();
        long t0 = System.nanoTime();
        new MsgRouter(gc).route(m, true);
        await(() -> !answered.isEmpty(), 5000, "not answered");
        return new Object[]{(System.nanoTime() - t0) / 1_000_000L, answered.get(0).getParam("error")};
    }

    @Test
    void anotherTenantsCallerGetsOneAnswerAndOneTimingWhateverTheNodesState() throws Exception {
        List<MsgEvent> toBroker = new CopyOnWriteArrayList<>();
        ControllerEngine gc = globalWithoutABroker(toBroker);
        RouteGate gate = gc.routeGate();
        Caller tb = Caller.controller("tb", "rtb", "atb1");
        assertTrue(gc.foreignTenantCaller(relayed(tb, "rta", "rcta")), "a caller of tb asking for a controller of ta");
        assertFalse(gc.foreignTenantCaller(relayed(tb, "rtb", "rctb")), "its own tenant's controller");
        assertFalse(gc.foreignTenantCaller(relayed(Caller.controller(SU, "glob", "gc"), "rta", "rcta")), "the superuser tenant");

        // ta's controller: gone long ago
        ConsumerId was = consumer();
        gate.consumerAdded(was, new ActiveMQQueue("T.*.rta_rcta"));
        gate.consumerRemoved(was, System.currentTimeMillis() - 60_000L);
        Object[] goneLongAgo = answer(gc, relayed(tb, "rta", "rcta"));
        // gone this moment
        ConsumerId again = consumer();
        gate.consumerAdded(again, new ActiveMQQueue("T.*.rta_rcta"));
        gate.consumerRemoved(again, System.currentTimeMillis());
        Object[] justGone = answer(gc, relayed(tb, "rta", "rcta"));
        // never there
        Object[] neverThere = answer(gc, relayed(tb, "rta", "rcx"));
        assertTrue(toBroker.isEmpty(), "sent: " + toBroker);

        String text = "route unavailable at glob_gc: REMOTE_REGION: no route to rta_rcta";
        assertTrue(String.valueOf(goneLongAgo[1]).matches("route unavailable at glob_gc: [A-Z_]+: no route to rta_rcta"), String.valueOf(goneLongAgo[1]));
        assertEquals(goneLongAgo[1], justGone[1], "the text tells a node that left long ago from one that just left");
        assertEquals(String.valueOf(goneLongAgo[1]).replace("rta_rcta", "rta_rcx"), neverThere[1], "the text tells a node that was never there");
        for (Object[] a : new Object[][]{goneLongAgo, justGone, neverThere}) {
            long ms = (Long) a[0];
            assertTrue(ms >= 350 && ms < 1500, "answered after " + ms + " ms: every state is answered one grace (400 ms) after the request");
        }
        assertNotNull(text);

        // its own tenant's controller (and anyone, for a superuser's caller): told at once, and why
        ConsumerId own = consumer();
        gate.consumerAdded(own, new ActiveMQQueue("T.*.rtb_rctb"));
        gate.consumerRemoved(own, System.currentTimeMillis() - 60_000L);
        Object[] ownTenant = answer(gc, relayed(tb, "rtb", "rctb"));
        assertTrue((Long) ownTenant[0] < 300, "a caller of the node's own tenant waited " + ownTenant[0] + " ms");
        assertTrue(String.valueOf(ownTenant[1]).contains("nothing at this node's broker consumes its inbox"), String.valueOf(ownTenant[1]));
    }
}
