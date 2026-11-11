package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.mina.statemachine.State;
import org.apache.mina.statemachine.context.DefaultStateContext;
import org.apache.mina.statemachine.context.StateContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region that loses its global keeps its own services: the discovery engines its agents join through and the watcher
 * that ages its agents. Only the uplink to the global is rebuilt. They used to be stopped on the loss and started again
 * only once the global was joined again (global_optional=false, the default): for the whole outage no agent could join
 * or rejoin any region and no dead agent was aged to STALE/LOST; and the discovery engines' threads were joined without
 * limit on the recovery thread.
 */
class GlobalOutageKeepsRegionServicesTest {

    private ControllerEngine engine;
    private ControllerSMHandler handler;
    private RegionHealthWatcher watcher;
    private Thread engineThread;

    /** A region controller joined to a global (config mode 24), its watcher up and its discovery marked running. */
    private void regionUnderAGlobal() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("is_region", "true");
        cfg.put("global_controller_host", "127.0.0.1");
        cfg.put("enable_tcp_discovery", "false");            // this test starts no listener of its own
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("global-outage").toString(), cfg, m -> { }, "r1", "rc");
        engine = new ControllerEngine(null, pb, null, null);
        ControllerStateImp cs = new ControllerStateImp(pb, null);
        cs.setRegionSuccess("r1", "rc", "test");
        cs.setRegionalGlobalSuccess("g1", "gc", "test");
        engine.cstate = cs;
        handler = new ControllerSMHandler(engine);
        watcher = new RegionHealthWatcher(engine);
        engine.setRegionHealthWatcher(watcher);
        engine.setDiscoveryActive(true);
        handler.shutdown();                                  // no global is joined here: the join loop ends at once
    }

    private static StateContext in(String state) {
        StateContext ctx = new DefaultStateContext();
        ctx.setCurrentState(new State(state));
        return ctx;
    }

    @AfterEach
    void stop() {
        if (engineThread != null) engineThread.interrupt();
        if (handler != null) handler.stopRegion();           // its timers, the watcher
        if (watcher != null) watcher.shutdown();
    }

    @Test
    void theAgentWatcherAndDiscoveryOutliveTheLossOfTheGlobal() throws Exception {
        regionUnderAGlobal();
        handler.globalControllerLost(in(ControllerSMHandler.REGION_GLOBAL), "test: the global was killed");

        assertEquals(ControllerMode.REGION_GLOBAL_FAILED, engine.cstate.getControllerState());
        assertSame(watcher, engine.getRegionHealthWatcher(), "the watcher that ages the region's agents was stopped with the global");
        assertNotNull(watcher.regionalUpdateTimer, "agent aging stopped for the outage");
        assertNotNull(watcher.communicationsHealthTimer, "the region's own watchdog stopped for the outage");
        assertTrue(engine.isDiscoveryActive(), "agent discovery was stopped with the global: no agent can join or rejoin the region");
    }

    @Test
    void theDiscoveryEnginesAreNotStoppedOrWaitedForOnTheRecoveryThread() throws Exception {
        regionUnderAGlobal();
        engineThread = new Thread(() -> { try { Thread.sleep(600_000); } catch (InterruptedException end) { } }, "discovery-engine-stand-in");
        engineThread.setDaemon(true);
        engineThread.start();
        engine.setDiscoveryTCPEngineThread(engineThread);

        assertTimeoutPreemptively(Duration.ofSeconds(15),
                () -> handler.globalControllerLost(in(ControllerSMHandler.REGION_GLOBAL), "test: the global was killed"),
                "the recovery thread waited without limit for a discovery engine to end");
        assertSame(engineThread, engine.getDiscoveryTCPEngineThread(), "the discovery engine was torn down with the global");
        assertTrue(engine.isDiscoveryActive());
    }

    @Test
    void stoppingTheRegionStopsItsServicesInBoundedTime() throws Exception {
        regionUnderAGlobal();
        engineThread = new Thread(() -> { try { Thread.sleep(600_000); } catch (InterruptedException end) { } }, "discovery-engine-stand-in");
        engineThread.setDaemon(true);
        engineThread.start();
        engine.setDiscoveryTCPEngineThread(engineThread);
        assertTimeoutPreemptively(Duration.ofSeconds(15),
                () -> handler.globalControllerLost(in(ControllerSMHandler.REGION_GLOBAL), "test: the global was killed"));

        long was = ControllerSMHandler.DISCOVERY_STOP_WAIT_MS;
        ControllerSMHandler.DISCOVERY_STOP_WAIT_MS = 300;
        try {
            // the global is still away (REGION_GLOBAL_FAILED): the services kept through the outage end with the region
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> handler.stopRegion(),
                    "stop waited without limit for a discovery engine that does not end");
        } finally {
            ControllerSMHandler.DISCOVERY_STOP_WAIT_MS = was;
        }
        assertNull(engine.getRegionHealthWatcher(), "the region stopped and left its watcher running");
        assertFalse(engine.isDiscoveryActive(), "the region stopped and left its discovery running");
    }

    @Test
    void aRejoinedGlobalIsNotJudgedByThePongBeforeTheOutage() throws Exception {
        regionUnderAGlobal();
        java.lang.reflect.Field pong = RegionHealthWatcher.class.getDeclaredField("lastGlobalPongTs");
        pong.setAccessible(true);
        pong.setLong(watcher, System.currentTimeMillis() - 600_000);      // the last pong before a ten-minute outage
        long before = System.currentTimeMillis();
        watcher.globalJoined();
        assertTrue(watcher.getLastGlobalPongTs() >= before, "the kept watcher reported the rejoined global stale at once"
                + " (link:parent CRITICAL -> the global lost again)");
    }
}
