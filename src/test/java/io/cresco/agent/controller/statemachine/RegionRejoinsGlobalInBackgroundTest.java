package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.mina.statemachine.State;
import org.apache.mina.statemachine.annotation.Transition;
import org.apache.mina.statemachine.annotation.Transitions;
import org.apache.mina.statemachine.context.DefaultStateContext;
import org.apache.mina.statemachine.context.StateContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * With global_optional=true a region gives up joining its global after global_connect_attempts and runs on its own.
 * Nothing tried again -- loss events were accepted only in REGION_GLOBAL -- and after a loss the region was left in
 * REGION_GLOBAL_FAILED, where isActive() is false. Now it is REGION (active for its agents) and, while a global is
 * configured or was joined before, keeps trying to join in the background with backoff.
 */
class RegionRejoinsGlobalInBackgroundTest {

    /** The handler with its join attempts scripted (the real one dials a global). */
    private static final class Scripted extends ControllerSMHandler {
        final AtomicInteger attempts = new AtomicInteger();
        final List<ControllerMode> modeAtAttempt = new ArrayList<>();
        volatile boolean globalIsUp;
        private final ControllerEngine engine;
        private final StateContext ctx;

        Scripted(ControllerEngine engine, StateContext ctx) { super(engine); this.engine = engine; this.ctx = ctx; }

        @Override boolean tryJoinGlobal(int configMode) {
            attempts.incrementAndGet();
            synchronized (modeAtAttempt) { modeAtAttempt.add(engine.cstate.getControllerState()); }
            if (!globalIsUp) return false;
            ctx.setCurrentState(STATES.get(REGION_GLOBAL));
            engine.cstate.setRegionalGlobalSuccess("g1", "gc", "test: joined");
            return true;
        }
    }

    private static final Map<String, State> STATES = new HashMap<>();
    static {
        for (ControllerMode m : ControllerMode.values()) STATES.put(m.name(), new State(m.name()));
    }

    private ControllerEngine engine;
    private Scripted handler;
    private StateContext ctx;
    private ControllerSM machine;
    private long firstMs, maxMs;

    @BeforeEach
    void fastBackoff() {
        firstMs = ControllerSMHandler.GLOBAL_REJOIN_FIRST_MS;
        maxMs = ControllerSMHandler.GLOBAL_REJOIN_MAX_MS;
        ControllerSMHandler.GLOBAL_REJOIN_FIRST_MS = 20;
        ControllerSMHandler.GLOBAL_REJOIN_MAX_MS = 80;
    }

    @AfterEach
    void stop() {
        ControllerSMHandler.GLOBAL_REJOIN_FIRST_MS = firstMs;
        ControllerSMHandler.GLOBAL_REJOIN_MAX_MS = maxMs;
        if (handler != null) {
            handler.shutdown();
            engine.cstate.setRegionalGlobalFailed("test over");      // no global to unregister from (a 3 s RPC)
            handler.stopRegion();
        }
    }

    /** A region controller (already REGION, as after regionInit) with a configured global, booted with the global up or down. */
    private void bootRegion(boolean globalOptional, boolean globalUp) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("is_region", "true");
        cfg.put("global_controller_host", "127.0.0.1");      // config mode 24
        cfg.put("global_optional", String.valueOf(globalOptional));
        cfg.put("global_connect_attempts", "2");
        cfg.put("retrywait", "1");
        cfg.put("enable_controllermon", "false");
        cfg.put("enable_tcp_discovery", "false");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("global-rejoin").toString(), cfg, m -> { }, "r1", "rc");
        engine = new ControllerEngine(null, pb, null, null);
        ControllerStateImp cs = new ControllerStateImp(pb, null);
        cs.setRegionSuccess("r1", "rc", "test");
        engine.cstate = cs;
        engine.setDiscoveryActive(true);
        ctx = new DefaultStateContext();
        ctx.setCurrentState(STATES.get(ControllerSMHandler.REGION));
        handler = new Scripted(engine, ctx);
        handler.setStateCollection(STATES.values());
        handler.globalIsUp = globalUp;
        // the engine's state machine: events one at a time, as the MINA proxy delivers them
        machine = new ControllerSM() {
            public void start() { synchronized (ctx) { handler.start(ctx); } }
            public void stop() { }
            public void regionalControllerLost(String desc) { }
            public void globalControllerLost(String desc) { synchronized (ctx) { handler.globalControllerLost(ctx, desc); } }
        };
        java.lang.reflect.Field sm = ControllerEngine.class.getDeclaredField("controllerSM");
        sm.setAccessible(true);
        sm.set(engine, machine);
        machine.start();
    }

    private static void await(String what, BooleanSupplier done) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!done.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("not within 10 s: " + what);
            Thread.sleep(10);
        }
    }

    private ControllerMode mode() { return engine.cstate.getControllerState(); }

    @Test
    void afterALossTheRegionIsActiveForItsAgentsAndKeepsTryingToJoin() throws Exception {
        bootRegion(true, true);
        assertEquals(ControllerMode.REGION_GLOBAL, mode());
        assertFalse(handler.rejoiningGlobal(), "joined at boot: nothing to retry");

        handler.globalIsUp = false;
        int atLoss = handler.attempts.get();
        machine.globalControllerLost("test: the global was killed");

        assertEquals(atLoss + 2, handler.attempts.get(), "global_connect_attempts bounds the recovery thread");
        assertEquals(ControllerMode.REGION, mode(), "the region was left in a failed state after its join attempts");
        assertTrue(engine.cstate.isActive(), "not active: its plugins do not load and its agents' controller reports not active");
        assertEquals(ControllerSMHandler.REGION, ctx.getCurrentState().getId());
        assertTrue(handler.rejoiningGlobal(), "nothing tries to join the configured global again");

        int given = handler.attempts.get();
        await("background join attempts", () -> handler.attempts.get() >= given + 3);
        assertEquals(ControllerMode.REGION, mode(), "between attempts the region stays active");

        handler.globalIsUp = true;                           // the global returns
        await("the region joins the returned global", () -> mode() == ControllerMode.REGION_GLOBAL);
        await("the background attempts end with the join", () -> !handler.rejoiningGlobal());
        int joinedAt = handler.attempts.get();
        Thread.sleep(300);
        assertEquals(joinedAt, handler.attempts.get(), "join attempts went on after the global was joined");
        assertEquals(ControllerMode.REGION_GLOBAL, mode(), "a background attempt was taken for a loss of the joined global");
    }

    @Test
    void aRegionBootedBeforeItsConfiguredGlobalJoinsItWhenItAppears() throws Exception {
        bootRegion(true, false);
        assertEquals(2, handler.attempts.get(), "the boot thread is held for global_connect_attempts only");
        assertEquals(ControllerMode.REGION, mode());
        assertTrue(engine.cstate.isActive());
        assertTrue(handler.rejoiningGlobal(), "a region that booted before its global never joined it");

        handler.globalIsUp = true;
        await("the region joins the global that appeared", () -> mode() == ControllerMode.REGION_GLOBAL);
    }

    @Test
    void everyBackgroundAttemptStartsFromAnActiveRegion() throws Exception {
        bootRegion(true, true);
        handler.globalIsUp = false;
        machine.globalControllerLost("test: the global was killed");
        int given = handler.attempts.get();
        await("background join attempts", () -> handler.attempts.get() >= given + 3);
        synchronized (handler.modeAtAttempt) {
            List<ControllerMode> background = handler.modeAtAttempt.subList(given, handler.modeAtAttempt.size());
            for (ControllerMode m : background) assertEquals(ControllerMode.REGION, m, "modes at the background attempts: " + background);
        }
    }

    @Test
    void stoppingTheRegionEndsTheBackgroundAttempts() throws Exception {
        bootRegion(true, false);
        assertTrue(handler.rejoiningGlobal());
        handler.shutdown();
        handler.stopRegion();
        await("the background attempts end with the region", () -> !handler.rejoiningGlobal());
        int stoppedAt = handler.attempts.get();
        Thread.sleep(300);
        assertEquals(stoppedAt, handler.attempts.get());
    }

    @Test
    void aBackgroundAttemptJoinsAlsoWhereTheStateMachineStillSaysJoined() throws Exception {
        // a join that ended early leaves the state machine in REGION_GLOBAL while the controller state says the global
        // is gone: every background attempt was then taken for "the global is joined, nothing to do" -- the thread ran
        // for ever and nothing joined
        bootRegion(true, true);
        assertEquals(ControllerMode.REGION_GLOBAL, mode());
        engine.cstate.setRegionalGlobalFailed("test: the join ended early");
        assertNotEquals(ControllerMode.REGION_GLOBAL, mode());
        int before = handler.attempts.get();
        machine.globalControllerLost(ControllerSMHandler.GLOBAL_REJOIN + "attempt 1");
        assertEquals(before + 1, handler.attempts.get(), "a background attempt did not try to join");
        assertEquals(ControllerMode.REGION_GLOBAL, mode());
        // with the global joined, a background attempt does nothing (it is no loss)
        machine.globalControllerLost(ControllerSMHandler.GLOBAL_REJOIN + "attempt 2");
        assertEquals(before + 1, handler.attempts.get());
        assertEquals(ControllerMode.REGION_GLOBAL, mode());
    }

    @Test
    void whenTheGlobalIsRequiredTheRecoveryThreadJoinsItItself() throws Exception {
        bootRegion(false, true);
        handler.globalIsUp = false;
        Thread back = new Thread(() -> {
            try { await("join attempts", () -> handler.attempts.get() >= 6); } catch (InterruptedException ignored) { }
            handler.globalIsUp = true;
        });
        back.start();
        machine.globalControllerLost("test: the global was killed");      // returns once the global is joined again
        back.join();
        assertEquals(ControllerMode.REGION_GLOBAL, mode());
        assertFalse(handler.rejoiningGlobal(), "no background attempts while the recovery thread itself keeps trying");
    }

    @Test
    void theLossEventIsAcceptedInEveryStateARegionWaitsForItsGlobalIn() throws Exception {
        Transitions table = ControllerSMHandler.class.getMethod("globalControllerLost", StateContext.class, String.class)
                .getAnnotation(Transitions.class);
        Set<String> in = new HashSet<>();
        for (Transition t : table.value()) in.addAll(java.util.Arrays.asList(t.in()));
        for (String state : new String[]{ControllerSMHandler.REGION_GLOBAL, ControllerSMHandler.REGION_GLOBAL_FAILED,
                ControllerSMHandler.REGION_GLOBAL_INIT, ControllerSMHandler.REGION}) {
            assertTrue(in.contains(state), "a join attempt raised in " + state + " is an unhandled event: nothing can retry there " + in);
        }
    }

    @Test
    void backoffAndWhoKeepsTrying() {
        ControllerSMHandler.GLOBAL_REJOIN_FIRST_MS = 5_000;
        ControllerSMHandler.GLOBAL_REJOIN_MAX_MS = 60_000;
        long[] expected = {5_000, 10_000, 20_000, 40_000, 60_000, 60_000};
        for (int attempt = 0; attempt < expected.length; attempt++) {
            assertEquals(expected[attempt], ControllerSMHandler.rejoinDelayMs(attempt), "attempt " + attempt);
        }
        assertEquals(60_000, ControllerSMHandler.rejoinDelayMs(10_000), "no overflow");

        assertTrue(ControllerSMHandler.rejoinsInBackground(24, false), "a configured global (global_controller_host) is always retried");
        assertTrue(ControllerSMHandler.rejoinsInBackground(8, true), "a global found by discovery and lost is retried");
        assertFalse(ControllerSMHandler.rejoinsInBackground(8, false), "no global configured, none ever joined: a region on its own");
    }
}
