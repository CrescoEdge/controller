package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The live NPE: during a global restart a region loses its global, isRegionalGlobalShutdown() nulls the
 * RegionHealthWatcher and stateInit() rebuilds it only once the global is re-joined. An agent ping (route
 * 20943) that arrives at the region in that window went MsgRouter -> getRegionHealthWatcher().sendRegionalMsg
 * and threw a NullPointerException; the ping was dropped and, after the parent-link grace window, the agent
 * declared its region lost. Here the region node r1/a1 sits in REGION_GLOBAL_INIT with no watcher (exactly
 * that window) and must still answer the ping of its agent r1/a2.
 */
class RegionalRebuildWindowPingTest {

    /** A controller state pinned to one mode (no persistence). */
    static final class StubState extends ControllerStateImp {
        volatile ControllerMode mode;
        volatile String regionalRegion, regionalAgent, globalRegion, globalAgent;

        StubState(PluginBuilder pb, ControllerMode mode) {
            super(pb, null);
            this.mode = mode;
        }

        @Override public ControllerMode getControllerState() { return mode; }
        @Override public boolean isRegionalController() { return mode.name().startsWith("REGION") || isGlobalController(); }
        @Override public boolean isGlobalController() { return mode.name().startsWith("GLOBAL"); }
        @Override public boolean isActive() { return false; }
        @Override public String getRegion() { return "r1"; }
        @Override public String getAgent() { return "a1"; }
        @Override public String getRegionalRegion() { return regionalRegion; }
        @Override public String getRegionalAgent() { return regionalAgent; }
        @Override public String getGlobalRegion() { return globalRegion; }
        @Override public String getGlobalAgent() { return globalAgent; }
    }

    /** The region DB: every agent ACTIVE, watchdog updates recorded. */
    static final class StubDb extends DBInterfaceImpl {
        final List<MsgEvent> nodeUpdates = new CopyOnWriteArrayList<>();
        StubDb(PluginBuilder pb) { super(pb, null); }
        @Override public int getNodeStatusCode(String regionId, String agentId) { return 10; }
        @Override public boolean nodeUpdate(MsgEvent de) { nodeUpdates.add(de); return true; }
    }

    /** The node r1/a1 (a controller: no plugin id) in {@code mode}, with no health watchers. */
    static final class Fixture {
        final List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        final PluginBuilder plugin;
        final ControllerEngine engine;
        final StubState state;
        final StubDb db;
        final MsgRouter router;

        Fixture(ControllerMode mode, Map<String, Object> cfg) {
            plugin = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), new HashMap<>(cfg), sent::add);
            state = new StubState(plugin, mode);
            db = new StubDb(plugin);
            engine = new ControllerEngine(null, plugin, null, db);
            engine.cstate = state;
            router = new MsgRouter(engine);
        }
    }

    /** An agent's ping as its region receives it: r1/a2 -> r1/a1, regional, an RPC with the agent's call id. */
    static MsgEvent agentPing() {
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", null, "r1", "a1", null, true, false);
        ping.setParam("action", "ping");
        ping.setParam("desc", "agent-ping-request");
        ping.setParam("is_rpc", "true");
        ping.setParam("callId-r1-a2-null", "call-1");
        ping.setParam("ttl", "1");
        return ping;
    }

    @Test
    void anAgentPingArrivingWhileTheRegionalWatcherIsRebuiltIsAnswered() {
        Fixture f = new Fixture(ControllerMode.REGION_GLOBAL_INIT, Map.of());
        assertNull(f.engine.getRegionHealthWatcher(), "the rebuild window: no RegionHealthWatcher");

        MsgEvent ping = agentPing();
        assertEquals(20943, MsgRouter.routePath(ping, true, false, "r1", "a1", null));

        f.router.route(ping);

        assertEquals(1, f.sent.size(), "the ping was dropped (no pong): " + f.sent);
        MsgEvent pong = f.sent.get(0);
        assertEquals("pong", pong.getParam("action"), pong.getParams().toString());
        assertEquals("r1", pong.getDstRegion());
        assertEquals("a2", pong.getDstAgent());
        assertEquals("call-1", pong.getParam("callId-r1-a2-null"));
        assertNull(pong.getParam("error"), pong.getParams().toString());
    }

    @Test
    void anAgentWatchdogArrivingWhileTheRegionalWatcherIsRebuiltUpdatesTheRegionDb() {
        Fixture f = new Fixture(ControllerMode.REGION_GLOBAL_INIT, Map.of());
        MsgEvent wd = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a2", null, "r1", "a1", null, true, false);
        wd.setParam("agent_watchdog_update", "a2");
        wd.setParam("ttl", "1");

        f.router.route(wd);

        assertEquals(1, f.db.nodeUpdates.size(), "the agent's keep-alive never reached the region DB (it would go STALE)");
        assertTrue(f.sent.isEmpty(), f.sent.toString());
    }
}
