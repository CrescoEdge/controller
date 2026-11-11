package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a region controller does with a message is counted once: delivered when its regional (or global) executor takes
 * it, relayed when it only carries it on to its global, dropped under its cause when it can do neither. The region
 * r1/a1 of {@link RegionalRebuildWindowPingTest}, with messages of its agent r1/a2 as they come off the wire.
 */
class RegionalDispatchIsCountedTest {

    private static RegionalRebuildWindowPingTest.Fixture region(ControllerMode mode) {
        return new RegionalRebuildWindowPingTest.Fixture(mode, Map.of("route_ready_wait_ms", "100"));
    }

    /** Give {@code engine} a broker client whose sends succeed ({@code sent} takes them) or fail. */
    private static void client(ControllerEngine engine, List<MsgEvent> sent, boolean up) throws Exception {
        Field f = ControllerEngine.class.getDeclaredField("activeClient");
        f.setAccessible(true);
        f.set(engine, new ActiveClient(engine) {
            @Override public boolean sendMessage(MsgEvent sm) { return up ? sent.add(sm) : super.sendMessage(sm); }
            @Override public boolean sendAPMessage(MsgEvent msg) { return up ? sent.add(msg) : super.sendAPMessage(msg); }
        });
    }

    private static MsgEvent kpi() {
        MsgEvent m = new MsgEvent(MsgEvent.Type.KPI, "r1", "a2", "plugin-1", "r1", "a1", null, true, false);
        m.setParam("ttl", "1");
        return m;
    }

    private static MsgEvent globalFunction() {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", null, "r1", "a1", null, true, true);
        m.setParam("action", "resourceinfo");
        m.setParam("ttl", "1");
        return m;
    }

    private static void only(MsgCounts c, MsgCounts.Drop cause, long n) {
        for (MsgCounts.Drop d : MsgCounts.Drop.values()) assertEquals(d == cause ? n : 0L, c.droppedCount(d), "dropped as " + d);
    }

    @Test
    void whatTheRegionalExecutorTakesIsDelivered() {
        RegionalRebuildWindowPingTest.Fixture f = region(ControllerMode.REGION_GLOBAL_INIT);
        f.engine.msgIn(RegionalRebuildWindowPingTest.agentPing(), true);
        MsgEvent wd = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a2", null, "r1", "a1", null, true, false);
        wd.setParam("ttl", "1");
        f.engine.msgIn(wd, true);
        assertEquals(1, f.db.nodeUpdates.size());
        assertEquals(2, f.engine.msgCounts().deliveredCount());
        assertEquals(0, f.engine.msgCounts().droppedCount());
        assertEquals(0, f.engine.msgCounts().relayedCount());
    }

    @Test
    void aFunctionOfTheGlobalAtARegionThatJoinedNoneIsDroppedAsNoGlobal() {
        RegionalRebuildWindowPingTest.Fixture f = region(ControllerMode.REGION_GLOBAL_INIT);
        assertTrue(ControllerTiers.targetsGlobal(globalFunction()));
        f.engine.msgIn(globalFunction(), true);
        only(f.engine.msgCounts(), MsgCounts.Drop.NO_GLOBAL, 1);
        assertEquals(0, f.engine.msgCounts().deliveredCount());
    }

    @Test
    void aFunctionOfTheGlobalIsRelayedByItsRegionNotDeliveredThere() throws Exception {
        RegionalRebuildWindowPingTest.Fixture f = region(ControllerMode.REGION_GLOBAL);
        f.state.globalRegion = "rg";
        f.state.globalAgent = "g1";
        List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        client(f.engine, sent, true);
        f.engine.msgIn(globalFunction(), true);              // an agent's, off the wire: carried on
        assertEquals(1, sent.size());
        assertEquals("rg_g1", sent.get(0).getForwardDst());
        assertEquals(1, f.engine.msgCounts().relayedCount());
        assertEquals(0, f.engine.msgCounts().deliveredCount(), "a message carried on to the global was counted as delivered at its region");
        // the region's own (it originates it): sent, and not a relay
        MsgEvent own = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r1", "a1", null, true, true);
        own.setParam("action", "resourceinfo");
        f.engine.msgIn(own, false);
        assertEquals(2, sent.size());
        assertEquals(1, f.engine.msgCounts().relayedCount());
        assertEquals(0, f.engine.msgCounts().droppedCount());
    }

    @Test
    void aKpiARegionOnlyCarriesOnToItsGlobalIsRelayedOrDroppedNeverDeliveredToo() throws Exception {
        // with a global joined: carried on
        RegionalRebuildWindowPingTest.Fixture f = region(ControllerMode.REGION_GLOBAL);
        f.state.globalRegion = "rg";
        f.state.globalAgent = "g1";
        List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        client(f.engine, sent, true);
        f.engine.msgIn(kpi(), true);
        assertEquals(1, sent.size());
        assertEquals(1, f.engine.msgCounts().relayedCount());
        assertEquals(0, f.engine.msgCounts().deliveredCount(), "one KPI would end twice: at its region and at the global");
        assertEquals(0, f.engine.msgCounts().droppedCount());

        // with none joined: it ends here, once
        RegionalRebuildWindowPingTest.Fixture alone = region(ControllerMode.REGION_GLOBAL_INIT);
        alone.engine.msgIn(kpi(), true);
        only(alone.engine.msgCounts(), MsgCounts.Drop.NO_GLOBAL, 1);
        assertEquals(0, alone.engine.msgCounts().deliveredCount());

        // a region with no producer (its link is being made anew) holds it for the producer: not dropped, not delivered;
        // and when no producer comes (here: the node stops) it is counted where it ended, once
        RegionalRebuildWindowPingTest.Fixture down = region(ControllerMode.REGION_GLOBAL);
        down.state.globalRegion = "rg";
        down.state.globalAgent = "g1";
        down.engine.msgIn(kpi(), true);
        assertEquals(0, down.engine.msgCounts().droppedCount());
        assertEquals(0, down.engine.msgCounts().deliveredCount());
        assertEquals(1, down.engine.getActiveClient().sendsHeldCount());
        down.engine.getActiveClient().stopHolding();
        only(down.engine.msgCounts(), MsgCounts.Drop.NO_SENDER, 1);

        // a region that keeps its KPIs (forward_global_kpi=false) is where they end: delivered
        RegionalRebuildWindowPingTest.Fixture keeps = new RegionalRebuildWindowPingTest.Fixture(ControllerMode.REGION_GLOBAL,
                Map.of("forward_global_kpi", "false"));
        keeps.engine.msgIn(kpi(), true);
        assertEquals(1, keeps.engine.msgCounts().deliveredCount());
        assertEquals(0, keeps.engine.msgCounts().droppedCount());
    }

    @Test
    void aMessageForARegionalControllerThatIsShuttingDownOrNotThereInTimeIsCountedUnderThat() {
        RegionalRebuildWindowPingTest.Fixture stopping = region(ControllerMode.REGION_SHUTDOWN);
        stopping.engine.msgIn(RegionalRebuildWindowPingTest.agentPing(), true);
        only(stopping.engine.msgCounts(), MsgCounts.Drop.CONTROLLER_STOPPING, 1);

        // a regional controller whose database is not there yet (it is being made): waited for route_ready_wait_ms
        io.cresco.library.plugin.PluginBuilder pb = io.cresco.agent.test.TestAgentService.plugin(System.getProperty("java.io.tmpdir"),
                new java.util.HashMap<>(Map.of("route_ready_wait_ms", "100", "broker_security_enabled", "false")), m -> { });
        ControllerEngine notYet = new ControllerEngine(null, pb, null, null);
        notYet.cstate = new RegionalRebuildWindowPingTest.StubState(pb, ControllerMode.REGION_INIT);
        long t0 = System.currentTimeMillis();
        notYet.msgIn(RegionalRebuildWindowPingTest.agentPing(), true);
        assertTrue(System.currentTimeMillis() - t0 >= 100, "not waited for");
        only(notYet.msgCounts(), MsgCounts.Drop.CONTROLLER_NOT_READY, 1);
        assertEquals(0, notYet.msgCounts().deliveredCount());
    }

    @Test
    void aControllerFunctionSentToTheWrongTierIsCountedAsRefusedByTheTiers() {
        RegionalRebuildWindowPingTest.Fixture f = region(ControllerMode.REGION_GLOBAL_INIT);
        // a regional-only function with the global flag
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a2", null, "r1", "a1", null, true, true);
        m.setParam("action", "agent_enable");
        m.setParam("ttl", "1");
        assertNotNull(ControllerTiers.refusal(m), "this test needs a function the tiers refuse");
        f.engine.msgIn(m, true);
        only(f.engine.msgCounts(), MsgCounts.Drop.TIER_REFUSED, 1);
        assertEquals(0, f.engine.msgCounts().deliveredCount());
    }
}
