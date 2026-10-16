package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.StubState;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.globalscheduler.AppScheduler;
import io.cresco.agent.controller.globalscheduler.ResourceScheduler;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutor;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.capability.CrescoAction;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Controller-tier routing through the real {@link MsgRouter#route}, {@link RegionalExecutor#dispatch} and
 * {@link GlobalExecutor}, at the node r1/a1 in a chosen controller mode: a controller function runs on the tier that
 * serves it ({@link ControllerTiers}), whatever its address; a regional-only function with the global flag is refused
 * with an error; nothing lands on the wrong tier silently.
 */
class ControllerTierRoutingTest {

    /** The node's DB: answers the calls the functions under test make, records membership writes. */
    static final class TierDb extends DBInterfaceImpl {
        final List<MsgEvent> nodeUpdates = new CopyOnWriteArrayList<>();
        TierDb(PluginBuilder pb) { super(pb, null); }
        @Override public int getNodeStatusCode(String regionId, String agentId) { return 10; }
        @Override public boolean nodeUpdate(MsgEvent de) { nodeUpdates.add(de); return true; }
        @Override public String getNetResourceInfo() { return "net-of-r1_a1"; }
    }

    static final class Node {
        final List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        final PluginBuilder plugin;
        final ControllerEngine engine;
        final StubState state;
        final TierDb db;
        final MsgRouter router;

        Node(ControllerMode mode, Map<String, Object> cfg) {
            plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>(cfg), sent::add);
            state = new StubState(plugin, mode);
            db = new TierDb(plugin);
            engine = new ControllerEngine(null, plugin, null, db);
            engine.cstate = state;
            router = new MsgRouter(engine);
            if (state.isGlobalController()) {
                engine.setResourceScheduler(new ResourceScheduler(engine));
                engine.setAppScheduler(new AppScheduler(engine));
            }
        }

        MsgEvent onlyReply() {
            assertEquals(1, sent.size(), sent.toString());
            return sent.get(0);
        }
    }

    /** A controller message RPC from the wsapi on r1/a2 (call id call-7), addressed to (dstRegion, dstAgent). */
    static MsgEvent rpc(MsgEvent.Type type, String action, boolean global, String dstRegion, String dstAgent) {
        MsgEvent m = new MsgEvent(type, "r1", "a2", "plugin/1", dstRegion, dstAgent, null, true, global);
        m.setParam("action", action);
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-a2-plugin/1", "call-7");
        m.setParam("ttl", "1");
        return m;
    }

    static void assertBackToTheCaller(MsgEvent reply) {
        assertEquals("r1", reply.getDstRegion());
        assertEquals("a2", reply.getDstAgent());
        assertEquals("plugin/1", reply.getDstPlugin());
        assertEquals("call-7", reply.getParam("callId-r1-a2-plugin/1"));
        assertFalse(MsgRouter.isRpcRequest(reply), "a reply is never answered again");
    }

    // --- the table -------------------------------------------------------------------------------------------

    @Test
    void theTableCoversEveryDeclaredGlobalAndRegionalAction() {
        // every action the global declares is GLOBAL or EITHER; every action the region declares is REGIONAL or EITHER
        for (CrescoAction a : GlobalExecutor.class.getAnnotationsByType(CrescoAction.class)) {
            ControllerTiers.Tier t = ControllerTiers.tierOf(MsgEvent.Type.valueOf(a.type()), a.name());
            assertTrue(t == ControllerTiers.Tier.GLOBAL || t == ControllerTiers.Tier.EITHER, a.name() + " -> " + t);
        }
        for (CrescoAction a : RegionalExecutor.class.getAnnotationsByType(CrescoAction.class)) {
            ControllerTiers.Tier t = ControllerTiers.tierOf(MsgEvent.Type.valueOf(a.type()), a.name());
            assertTrue(t == ControllerTiers.Tier.REGIONAL || t == ControllerTiers.Tier.EITHER, a.name() + " -> " + t);
        }
        assertNull(ControllerTiers.tierOf(MsgEvent.Type.CONFIG, "pluginlist"), "agent-tier actions are not controller functions");
        assertNull(ControllerTiers.tierOf(MsgEvent.Type.CONFIG, "listplugins"), "the action AND its type");
    }

    @Test
    void repliesAreNeverReclassified() {
        MsgEvent m = rpc(MsgEvent.Type.EXEC, "listplugins", true, "r2", "rc2");
        assertTrue(ControllerTiers.isTierRouted(m));
        m.setReturn();
        assertTrue(ControllerTiers.isReply(m));
        assertFalse(ControllerTiers.isTierRouted(m));
        // a controller's self-addressed request carries its call id as source AND destination: still a request
        MsgEvent self = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r1", "a1", null, true, true);
        self.setParam("action", "ping");
        self.setParam("callId-r1-a1-null", "c");
        assertFalse(ControllerTiers.isReply(self));
    }

    // --- at the global ---------------------------------------------------------------------------------------

    @Test
    void aGlobalFunctionAddressedToAnotherRegionRunsOnTheGlobal() {
        // global_controller_msgevent(netresourceinfo, region_id=r2, agent_id=rc2) arriving at the global's router
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        g.router.route(rpc(MsgEvent.Type.EXEC, "netresourceinfo", true, "r2", "rc2"));
        MsgEvent reply = g.onlyReply();
        assertEquals("net-of-r1_a1", reply.getParam("netresourceinfo"), reply.getParams().toString());
        assertEquals("r1", reply.getSrcRegion());                         // answered by the global itself
        assertEquals("a1", reply.getSrcAgent());
        assertNull(reply.getParam("error"), reply.getParams().toString());
        assertBackToTheCaller(reply);
    }

    @Test
    void aGlobalFunctionSentWithTheRegionalFlagRunsOnTheGlobal() {
        // regional_controller_msgevent(netresourceinfo) at a single-region global: the global serves it (it used to
        // fall through the RegionalExecutor to the AgentExecutor: "Unknown configtype", RPC timeout)
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        g.router.route(rpc(MsgEvent.Type.EXEC, "netresourceinfo", false, "r1", "a1"));
        assertEquals("net-of-r1_a1", g.onlyReply().getParam("netresourceinfo"));
    }

    @Test
    void aRegionalFunctionAtASingleRegionGlobalRunsOnItsRegionalController() {
        // the global is also its region's controller: an agent's agent_enable (regional flag) is a regional function
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        g.router.route(rpc(MsgEvent.Type.CONFIG, "agent_enable", false, "r1", "a1"));
        assertEquals(1, g.db.nodeUpdates.size(), "registered in the region DB");
        MsgEvent reply = g.onlyReply();
        assertEquals("true", reply.getParam("is_registered"), reply.getParams().toString());
        assertBackToTheCaller(reply);
    }

    @Test
    void anUnknownGlobalActionIsAnsweredWithAnError() {
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        g.router.route(rpc(MsgEvent.Type.CONFIG, "pluginlist", true, "r1", "a1"));
        MsgEvent reply = g.onlyReply();
        assertTrue(String.valueOf(reply.getParam("error")).contains("not a global-controller function"), reply.getParams().toString());
        assertBackToTheCaller(reply);
    }

    // --- refused -----------------------------------------------------------------------------------------------

    @Test
    void aRegionalFunctionWithTheGlobalFlagIsRefusedWithAnError() {
        for (ControllerMode mode : new ControllerMode[]{ControllerMode.GLOBAL, ControllerMode.REGION_GLOBAL, ControllerMode.AGENT}) {
            Node n = new Node(mode, Map.of());
            n.state.regionalRegion = "r1";
            n.state.regionalAgent = "rc1";
            n.router.route(rpc(MsgEvent.Type.CONFIG, "agent_enable", true, "r1", "a1"));
            MsgEvent reply = n.onlyReply();
            assertNotNull(reply.getParam(MsgRouter.ROUTE_REFUSED), mode + " " + reply.getParams());
            assertTrue(String.valueOf(reply.getParam("error")).contains("regional-controller function"), reply.getParams().toString());
            assertBackToTheCaller(reply);
            assertTrue(n.db.nodeUpdates.isEmpty(), mode + ": never run anywhere");
        }
    }

    @Test
    void aRefusedNonRpcIsDroppedWithoutAReply() {
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = rpc(MsgEvent.Type.CONFIG, "agent_disable", true, "r1", "a1");
        m.removeParam("is_rpc");
        g.router.route(m);
        assertTrue(g.sent.isEmpty(), g.sent.toString());
    }

    // --- at a region -------------------------------------------------------------------------------------------

    @Test
    void aGlobalFunctionAtARegionGoesToTheGlobalNotTheRegion() {
        Node r = new Node(ControllerMode.REGION_GLOBAL, Map.of());
        r.state.globalRegion = "rg";
        r.state.globalAgent = "gc";
        // addressed to this region's own controller, with the regional flag: still the global's function
        MsgEvent m = rpc(MsgEvent.Type.EXEC, "listplugins", false, "r1", "a1");
        r.router.route(m);
        assertEquals("rg_gc", m.getForwardDst(), "forwarded to the global (the broker send itself has no broker here)");
        assertTrue(r.sent.isEmpty(), "not answered by the region: " + r.sent);
    }

    @Test
    void aGlobalFunctionAtARegionWithNoGlobalIsAnsweredWithAnErrorNotRunRegionally() {
        // a region whose global is gone (global restart: the regional watcher is being rebuilt)
        Node r = new Node(ControllerMode.REGION_GLOBAL_INIT, Map.of("route_ready_wait_ms", "5000"));
        long t0 = System.currentTimeMillis();
        r.router.route(rpc(MsgEvent.Type.EXEC, "listplugins", false, "r1", "a1"));
        assertTrue(System.currentTimeMillis() - t0 < 2000, "not waited on");
        MsgEvent reply = r.onlyReply();
        assertTrue(String.valueOf(reply.getParam("error")).contains("no global controller joined"), reply.getParams().toString());
        assertBackToTheCaller(reply);
    }

    @Test
    void aRegionalFunctionStillRunsOnTheRegionWhileItsGlobalIsGone() {
        // the same window: region membership is the region's own function and keeps working
        Node r = new Node(ControllerMode.REGION_GLOBAL_INIT, Map.of());
        r.router.route(rpc(MsgEvent.Type.CONFIG, "agent_enable", false, "r1", "a1"));
        assertEquals(1, r.db.nodeUpdates.size());
        assertEquals("true", r.onlyReply().getParam("is_registered"));
    }

    @Test
    void aReplyToThisControllersOwnRpcIsHandedToTheCallerNotRunAgain() {
        // r1/a1's controller sent a regional controller RPC (agent_enable) to r2/rc2; the reply comes back (20815)
        Node r = new Node(ControllerMode.REGION_GLOBAL, Map.of());
        MsgEvent reply = new MsgEvent(MsgEvent.Type.CONFIG, "r2", "rc2", null, "r1", "a1", null, true, false);
        reply.setParam("action", "agent_enable");
        reply.setParam("is_rpc", "true");
        reply.setParam("callId-r1-a1-null", "own-call");
        reply.setParam("ttl", "2");
        r.router.route(reply);
        assertTrue(r.db.nodeUpdates.isEmpty(), "the reply was executed as a new request");
        assertTrue(r.sent.isEmpty(), r.sent.toString());
    }

    // --- at an agent ---------------------------------------------------------------------------------------------

    @Test
    void aGlobalFunctionAtAnAgentGoesToItsRegionalController() {
        Node a = new Node(ControllerMode.AGENT, Map.of());
        a.state.regionalRegion = "r1";
        a.state.regionalAgent = "rc1";
        MsgEvent m = rpc(MsgEvent.Type.EXEC, "listplugins", true, "r9", "a9");
        a.router.route(m);
        // on its way to the global via the region, not to r9_a9 (the fixture has no broker: the send to the
        // regional controller fails and the caller is told where it was going)
        assertTrue(String.valueOf(a.onlyReply().getParam("error")).contains("broker send to r1_rc1 failed"), a.sent.toString());
    }

    @Test
    void aGlobalFunctionAtAnAgentMidReinitGetsAnError() {
        Node a = new Node(ControllerMode.AGENT_INIT, Map.of("route_ready_wait_ms", "300"));
        a.router.route(rpc(MsgEvent.Type.EXEC, "listplugins", true, "r9", "a9"));
        assertTrue(String.valueOf(a.onlyReply().getParam("error")).contains("regional controller not known"));
    }

    @Test
    void aRegionalFunctionAddressedToAPlainAgentGoesToItsRegionalController() {
        Node a = new Node(ControllerMode.AGENT, Map.of());
        a.state.regionalRegion = "r1";
        a.state.regionalAgent = "rc1";
        MsgEvent m = rpc(MsgEvent.Type.CONFIG, "agent_enable", false, "r1", "a1");
        a.router.route(m);
        MsgEvent reply = a.onlyReply();                                   // no broker here: see above
        assertTrue(String.valueOf(reply.getParam("error")).contains("REMOTE_REGIONAL_CONTROLLER: broker send to r1_rc1 failed"),
                reply.getParams().toString());
        assertNull(reply.getParam("is_registered"), "not run on the agent");
    }
}
