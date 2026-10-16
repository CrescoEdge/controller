package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Controller-tier routing end to end over real embedded brokers: a global (rg/gc) on its broker, a region (r1/rc1)
 * and its agent (r1/a1) on the region's broker, joined by the duplex bridge a region builds to its global. Every
 * node is a real {@link ControllerEngine} + {@link MsgRouter} (+ RegionalExecutor / GlobalExecutor) with its own
 * identity and controller state; its broker sends go to the queue named by the forward destination, its inbox
 * consumer and its msgOut both feed its router, as in the agent. Callers are the controllers themselves
 * ({@code PluginBuilder.sendRPC}), so replies travel the real reply path back to a waiting RPC.
 */
class ControllerTierBrokerTest {

    private static final Gson GSON = new Gson();

    private BrokerService globalBroker;
    private BrokerService regionBroker;
    private final List<Connection> connections = new ArrayList<>();
    private Node g, r1, a1;

    /** A controller state of one node (no persistence). */
    static final class State extends ControllerStateImp {
        final String region, agent;
        volatile ControllerMode mode;
        volatile String regionalRegion, regionalAgent, globalRegion, globalAgent;
        State(PluginBuilder pb, String region, String agent, ControllerMode mode) {
            super(pb, null);
            this.region = region; this.agent = agent; this.mode = mode;
        }
        @Override public ControllerMode getControllerState() { return mode; }
        @Override public boolean isRegionalController() { return mode.name().startsWith("REGION") || isGlobalController(); }
        @Override public boolean isGlobalController() { return mode.name().startsWith("GLOBAL"); }
        @Override public boolean isActive() { return true; }
        @Override public String getRegion() { return region; }
        @Override public String getAgent() { return agent; }
        @Override public String getRegionalRegion() { return regionalRegion; }
        @Override public String getRegionalAgent() { return regionalAgent; }
        @Override public String getGlobalRegion() { return globalRegion; }
        @Override public String getGlobalAgent() { return globalAgent; }
    }

    static final class Db extends DBInterfaceImpl {
        final String node;
        final List<MsgEvent> nodeUpdates = new CopyOnWriteArrayList<>();
        Db(PluginBuilder pb, String node) { super(pb, null); this.node = node; }
        @Override public int getNodeStatusCode(String regionId, String agentId) { return 10; }
        @Override public boolean nodeUpdate(MsgEvent de) { nodeUpdates.add(de); return true; }
        @Override public String getNetResourceInfo() { return "net-of-" + node; }
    }

    final class Node {
        final String region, agent;
        final PluginBuilder plugin;
        final State state;
        final Db db;
        final ControllerEngine engine;
        final MsgRouter router;
        Session session;

        Node(String region, String agent, ControllerMode mode) throws Exception {
            this.region = region;
            this.agent = agent;
            plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>(Map.of("route_ready_wait_ms", "2000")),
                    m -> router().route(m), region, agent);
            state = new State(plugin, region, agent, mode);
            db = new Db(plugin, region + "_" + agent);
            engine = new ControllerEngine(null, plugin, null, db);
            engine.cstate = state;
            router = new MsgRouter(engine);
            if (state.isGlobalController()) {
                engine.setResourceScheduler(new ResourceScheduler(engine));
                engine.setAppScheduler(new AppScheduler(engine));
            }
            Field f = ControllerEngine.class.getDeclaredField("activeClient");
            f.setAccessible(true);
            f.set(engine, new ActiveClient(engine) {
                @Override public boolean sendMessage(MsgEvent sm) { return send(sm); }
                @Override public void sendAPMessage(MsgEvent msg) { send(msg); }
            });
        }

        MsgRouter router() { return router; }

        void start(BrokerService broker) throws Exception {
            Connection c = new ActiveMQConnectionFactory("vm://" + broker.getBrokerName() + "?create=false").createConnection();
            connections.add(c);
            c.start();
            session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session consumerSession = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumerSession.createConsumer(new ActiveMQQueue(region + "_" + agent)).setMessageListener(msg -> {
                try {
                    router.route(GSON.fromJson(((TextMessage) msg).getText(), MsgEvent.class));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        synchronized boolean send(MsgEvent m) {
            try {
                MessageProducer p = session.createProducer(new ActiveMQQueue(m.getForwardDst()));
                p.send(session.createTextMessage(GSON.toJson(m)));
                p.close();
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        /** An RPC from this node's controller: the real RPC registry, the real reply path. */
        MsgEvent call(MsgEvent.Type type, String action, boolean global, String dstRegion, String dstAgent) {
            MsgEvent m = global ? plugin.getGlobalControllerMsgEvent(type, dstRegion, dstAgent)
                    : plugin.getRegionalControllerMsgEvent(type, dstRegion, dstAgent);
            m.setParam("action", action);
            return plugin.sendRPC(m, 8000);
        }
    }

    private static BrokerService broker(String name) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.addConnector("tcp://127.0.0.1:0");
        b.start();
        b.waitUntilStarted();
        return b;
    }

    private static boolean await(BooleanSupplier cond, long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    @BeforeEach
    void fabric() throws Exception {
        globalBroker = broker("tier-g-" + System.nanoTime());
        regionBroker = broker("tier-r1-" + System.nanoTime());
        g = new Node("rg", "gc", ControllerMode.GLOBAL);
        r1 = new Node("r1", "rc1", ControllerMode.REGION_GLOBAL);
        r1.state.globalRegion = "rg";
        r1.state.globalAgent = "gc";
        a1 = new Node("r1", "a1", ControllerMode.AGENT);
        a1.state.regionalRegion = "r1";
        a1.state.regionalAgent = "rc1";
        g.start(globalBroker);
        r1.start(regionBroker);
        a1.start(regionBroker);
        NetworkConnector nc = regionBroker.addNetworkConnector("static:("
                + globalBroker.getTransportConnectors().get(0).getPublishableConnectString() + ")?maxReconnectDelay=500");
        nc.setName("region-to-global");
        nc.setDuplex(true);
        nc.start();
        assertTrue(await(() -> ActiveBroker.hasLiveQueueRoute(globalBroker, "r1_rc1")
                && ActiveBroker.hasLiveQueueRoute(globalBroker, "r1_a1")
                && ActiveBroker.hasLiveQueueRoute(regionBroker, "rg_gc"), 15_000), "bridge carries the inboxes");
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) {
            try { c.close(); } catch (Exception ignored) { }
        }
        for (BrokerService b : new BrokerService[]{regionBroker, globalBroker}) {
            if (b != null) { b.stop(); b.waitUntilStopped(); }
        }
    }

    @Test
    void aGlobalFunctionAddressedToAnotherRegionIsAnsweredByTheGlobal() {
        // global_controller_msgevent(netresourceinfo, region_id=r2, agent_id=rc2) from the region: before, this went
        // to r2_rc2 (no such consumer here; with one, its AgentExecutor) and the caller timed out
        MsgEvent reply = r1.call(MsgEvent.Type.EXEC, "netresourceinfo", true, "r2", "rc2");
        assertNotNull(reply, "no reply (RPC timeout)");
        assertEquals("net-of-rg_gc", reply.getParam("netresourceinfo"), reply.getParams().toString());
        assertEquals("rg", reply.getSrcRegion());
        assertEquals("gc", reply.getSrcAgent());
    }

    @Test
    void aGlobalFunctionWithTheRegionalFlagFromAnAgentIsAnsweredByTheGlobal() {
        // regional_controller_msgevent(netresourceinfo) at the agent: agent -> its region -> the global
        MsgEvent reply = a1.call(MsgEvent.Type.EXEC, "netresourceinfo", false, "r1", "a1");
        assertNotNull(reply, "no reply (RPC timeout)");
        assertEquals("net-of-rg_gc", reply.getParam("netresourceinfo"), reply.getParams().toString());
    }

    @Test
    void aRegionalFunctionAddressedToAnAgentRunsOnItsRegionsController() throws Exception {
        // regional_controller_msgevent(agent_enable, region_id=r1, agent_id=a1) from the global: it crosses the
        // bridge to the agent, which hands it to its regional controller
        MsgEvent reply = g.call(MsgEvent.Type.CONFIG, "agent_enable", false, "r1", "a1");
        assertNotNull(reply, "no reply (RPC timeout)");
        assertEquals("true", reply.getParam("is_registered"), reply.getParams().toString());
        assertEquals("rc1", reply.getSrcAgent());
        assertEquals(1, r1.db.nodeUpdates.size());
        assertTrue(g.db.nodeUpdates.isEmpty() && a1.db.nodeUpdates.isEmpty());
    }

    @Test
    void aRegionalFunctionSentAsAGlobalMessageIsRefusedAtOnce() {
        long t0 = System.currentTimeMillis();
        MsgEvent reply = r1.call(MsgEvent.Type.CONFIG, "agent_enable", true, "r1", "a1");
        assertTrue(System.currentTimeMillis() - t0 < 2000, "refused at once, not timed out");
        assertNotNull(reply);
        assertNotNull(reply.getParam(MsgRouter.ROUTE_REFUSED), reply.getParams().toString());
        assertTrue(r1.db.nodeUpdates.isEmpty() && g.db.nodeUpdates.isEmpty(), "never run anywhere");
    }

    @Test
    void aGlobalFunctionAtARegionWhoseGlobalIsGoneFailsFast() {
        r1.state.mode = ControllerMode.REGION_GLOBAL_INIT;          // global restart: the region lost its global
        r1.state.globalRegion = null;
        r1.state.globalAgent = null;
        long t0 = System.currentTimeMillis();
        MsgEvent reply = a1.call(MsgEvent.Type.EXEC, "netresourceinfo", true, "r1", "a1");
        assertTrue(System.currentTimeMillis() - t0 < 4000, "answered, not timed out");
        assertNotNull(reply);
        assertTrue(String.valueOf(reply.getParam("error")).contains("no global controller joined"), reply.getParams().toString());
        assertNull(reply.getParam("netresourceinfo"), "not answered by the region");
    }
}
