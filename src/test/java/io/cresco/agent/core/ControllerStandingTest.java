package io.cresco.agent.core;

import io.cresco.agent.controller.communication.SpeakerPosition;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.DBEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.ActionAuthorizer.Position;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.cresco.library.security.ActionAuthorizer.Position.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Placing a speaker runs for every message that has a direction -- every WATCHDOG and KPI among them. It reads the node's
 * state as one immutable standing (a volatile read), never through the state's synchronized getters; and every method
 * of the state that changes the mode, the node's name or a parent publishes the standing with the change, so the message
 * paths never stay on the state before, and never see half of a change.
 */
class ControllerStandingTest {

    private static PluginBuilder plugin;
    private static DBEngine dbe;

    @BeforeAll
    static void database() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        cfg.put("authority_direction", "enforce");
        plugin = TestAgentService.plugin(Files.createTempDirectory("standing").toString(), cfg);
        dbe = new DBEngine(plugin);              // a real one: the shutdown states change only once they are recorded
        // a state that becomes the global (or standalone) installs the JVM's crypto policy: put back what was there
        policyBefore = io.cresco.library.crypto.CryptoPolicy.current();
        policySourceBefore = io.cresco.library.crypto.CryptoPolicy.currentSource();
    }

    private static io.cresco.library.crypto.CryptoPolicy policyBefore;
    private static String policySourceBefore;

    @AfterAll
    static void close() {
        try { dbe.shutdown(); } catch (Exception ignored) { }
        io.cresco.library.crypto.CryptoPolicy.uninstall();
        if (policyBefore != null) io.cresco.library.crypto.CryptoPolicy.install(policyBefore, policySourceBefore);
    }

    /** On this thread, while a speaker is placed: any synchronized getter of the state is a failure. */
    private static final ThreadLocal<Boolean> PLACING = ThreadLocal.withInitial(() -> false);

    private static void noLock(String getter) {
        if (PLACING.get()) throw new AssertionError("ControllerStateImp." + getter + " (synchronized) was called while a speaker was placed");
    }

    /** The real state, with every synchronized getter refusing to be called while a speaker is placed. */
    static final class Watched extends ControllerStateImp {
        Watched() { super(plugin, dbe); }
        @Override public synchronized ControllerMode getControllerState() { noLock("getControllerState"); return super.getControllerState(); }
        @Override public synchronized String getCurrentDesc() { noLock("getCurrentDesc"); return super.getCurrentDesc(); }
        @Override public synchronized boolean isRegionalController() { noLock("isRegionalController"); return super.isRegionalController(); }
        @Override public synchronized boolean isGlobalController() { noLock("isGlobalController"); return super.isGlobalController(); }
        @Override public synchronized String getRegion() { noLock("getRegion"); return super.getRegion(); }
        @Override public synchronized String getAgent() { noLock("getAgent"); return super.getAgent(); }
        @Override public synchronized String getGlobalAgent() { noLock("getGlobalAgent"); return super.getGlobalAgent(); }
        @Override public synchronized String getGlobalRegion() { noLock("getGlobalRegion"); return super.getGlobalRegion(); }
        @Override public synchronized String getRegionalAgent() { noLock("getRegionalAgent"); return super.getRegionalAgent(); }
        @Override public synchronized String getRegionalRegion() { noLock("getRegionalRegion"); return super.getRegionalRegion(); }
        @Override public synchronized String getGlobalControllerPath() { noLock("getGlobalControllerPath"); return super.getGlobalControllerPath(); }
        @Override public synchronized String getRegionalControllerPath() { noLock("getRegionalControllerPath"); return super.getRegionalControllerPath(); }
        @Override public synchronized String getAgentPath() { noLock("getAgentPath"); return super.getAgentPath(); }
    }

    @Test
    void theWatchedStateCoversEverySynchronizedMethodOfTheState() throws Exception {
        int watched = 0;
        for (Method m : ControllerStateImp.class.getDeclaredMethods()) {
            if (!Modifier.isSynchronized(m.getModifiers())) continue;
            assertTrue(Modifier.isPublic(m.getModifiers()), m.getName());
            assertNotNull(Watched.class.getDeclaredMethod(m.getName(), m.getParameterTypes()),
                    "ControllerStateImp." + m.getName() + " is synchronized and this test does not watch it");
            watched++;
        }
        assertEquals(13, watched, "the synchronized getters of the state");
        assertFalse(Modifier.isSynchronized(ControllerStateImp.class.getDeclaredMethod("standing").getModifiers()),
                "standing() is what the message paths read: it must not enter the state's monitor");
    }

    private static Position place(ControllerEngine engine, String region, String agent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.WATCHDOG, region, agent, null, "x", "y", null, true, false);
        Caller c = Caller.controller("t1", region, agent);
        c.stamp(m);
        PLACING.set(true);
        try {
            engine.authority().mode();           // as the authorizer asks it first (the configuration, at most once a second)
            return engine.authority().positionOf(m, c);
        } finally {
            PLACING.set(false);
        }
    }

    @Test
    void placingASpeakerEntersNoMonitorOfTheState() {
        ControllerEngine engine = new ControllerEngine(null, plugin, null, null);
        Watched state = new Watched();
        engine.cstate = state;
        // the premise of the test: the watched getters do throw while placing
        PLACING.set(true);
        try {
            assertThrows(AssertionError.class, state::getRegion);
            assertThrows(AssertionError.class, state::isRegionalController);
        } finally {
            PLACING.set(false);
        }
        assertEquals(UNKNOWN, place(engine, "rgA", "rc"), "a node with no name yet places nobody");

        // an agent: joining, joined, its parent lost, joined again, stopping
        state.setAgentInit("rgA", "a1", "test");
        assertEquals(SELF, place(engine, "rgA", "a1"));
        assertEquals(ELSEWHERE, place(engine, "rgA", "rc"), "an agent that has joined nobody has nobody above it");
        state.setAgentSuccess("rgA", "rc", "test");
        assertEquals(ABOVE, place(engine, "rgA", "rc"));
        assertEquals(ELSEWHERE, place(engine, "rgA", "a2"));
        assertEquals(ELSEWHERE, place(engine, "rgB", "rc"));
        state.setAgentFailed("test");
        assertEquals(ELSEWHERE, place(engine, "rgA", "rc"), "the parent an agent lost stayed above it");
        assertEquals(SELF, place(engine, "rgA", "a1"));
        state.setAgentInit("rgB", "a1", "test");
        state.setAgentSuccess("rgB", "rc2", "test");
        assertEquals(ABOVE, place(engine, "rgB", "rc2"));
        assertEquals(ELSEWHERE, place(engine, "rgA", "rc"), "the region controller of the region it left");
        assertEquals(ELSEWHERE, place(engine, "rgA", "a1"), "its own former name");
        assertTrue(state.setAgentShutdown("test"), "premise: the shutdown was recorded");
        assertEquals(ABOVE, place(engine, "rgB", "rc2"), "a stopping agent still takes its region controller's word");

        // a region controller: alone, joined to a global, the global lost
        state.setRegionInit("rgA", "rc", "test");
        assertEquals(SELF, place(engine, "rgA", "rc"));
        assertEquals(BELOW, place(engine, "rgA", "a1"));
        assertEquals(ELSEWHERE, place(engine, "rgB", "rc2"), "its parent of before, as an agent");
        state.setRegionSuccess("rgA", "rc", "test");
        state.setRegionGlobalInit("test");
        assertEquals(ELSEWHERE, place(engine, "rgG", "gc"), "a global it has not joined yet");
        state.setRegionalGlobalSuccess("rgG", "gc", "test");
        assertEquals(ABOVE, place(engine, "rgG", "gc"));
        assertEquals(BELOW, place(engine, "rgA", "a1"));
        assertEquals(ELSEWHERE, place(engine, "rgG", "ga"));
        state.setRegionalGlobalFailed("test");
        assertEquals(ELSEWHERE, place(engine, "rgG", "gc"), "the global a region lost stayed above it");
        assertEquals(BELOW, place(engine, "rgA", "a1"), "a region with no global keeps its agents");
        state.setRegionalGlobalSuccess("rgH", "gc2", "test");
        assertEquals(ABOVE, place(engine, "rgH", "gc2"));
        assertEquals(ELSEWHERE, place(engine, "rgG", "gc"), "the global it left");

        // the global: nobody above, its own region below
        state.setRegionInit("rgG", "gc", "test");
        state.setGlobalInit("test");
        assertEquals(SELF, place(engine, "rgG", "gc"));
        assertEquals(BELOW, place(engine, "rgG", "ga"));
        assertEquals(ELSEWHERE, place(engine, "rgH", "gc2"), "the global it had as a region");
        state.setGlobalSuccess("test");
        assertEquals(SELF, place(engine, "rgG", "gc"));
        assertEquals(ELSEWHERE, place(engine, "rgA", "rc"), "a region nothing records as joined");
        // and back to an agent of another region: nothing of the global's standing stays
        state.setAgentInit("rgA", "a9", "test");
        assertEquals(ELSEWHERE, place(engine, "rgG", "ga"), "a node of the region this node was the global of stayed below it");
        assertEquals(ELSEWHERE, place(engine, "rgG", "gc"));
    }

    /** What the synchronized getters say now, as a standing. */
    private static SpeakerPosition.Standing fromGetters(ControllerStateImp s) {
        boolean global = s.isGlobalController(), regional = s.isRegionalController();
        return new SpeakerPosition.Standing(s.getRegion(), s.getAgent(), regional, global,
                regional ? s.getGlobalRegion() : s.getRegionalRegion(), regional ? s.getGlobalAgent() : s.getRegionalAgent());
    }

    private static void same(String after, ControllerStateImp state) {
        SpeakerPosition.Standing published = state.standing(), now = fromGetters(state);
        assertNotNull(published, after);
        assertEquals(now.toString(), published.toString(), "after " + after + " the standing the message paths read is not the state");
        assertEquals(now.regionController, published.regionController, after);
        assertEquals(now.global, published.global, after);
    }

    @Test
    void everyMethodThatChangesTheStatePublishesTheStandingWithIt() throws Exception {
        ControllerStateImp s = new ControllerStateImp(plugin, dbe);
        assertSame(SpeakerPosition.Standing.NONE, s.standing(), "a state nothing was set on");
        Set<String> called = new TreeSet<>();
        // each setter after a state that differs from what it sets in mode, name or parent
        s.setPreInit(); called.add("setPreInit"); same("setPreInit", s);
        s.setAgentInit("rgA", "a1", "t"); called.add("setAgentInit"); same("setAgentInit", s);
        assertEquals("rgA/a1 (agent)", s.standing().toString());
        s.setAgentSuccess("rgA", "rc", "t"); called.add("setAgentSuccess"); same("setAgentSuccess", s);
        assertEquals("rgA/a1 (agent) under rgA/rc", s.standing().toString());
        assertTrue(s.setAgentShutdown("t")); called.add("setAgentShutdown"); same("setAgentShutdown", s);
        assertEquals(ControllerMode.AGENT_SHUTDOWN, s.getControllerState());
        s.setAgentFailed("t"); called.add("setAgentFailed"); same("setAgentFailed", s);
        assertNull(s.standing().parentRegion, "a failed agent kept its parent");
        s.setRegionInit("rgA", "rc", "t"); called.add("setRegionInit"); same("setRegionInit", s);
        assertEquals("rgA/rc (region controller)", s.standing().toString());
        s.setRegionFailed("t"); called.add("setRegionFailed"); same("setRegionFailed", s);
        s.setRegionSuccess("rgB", "rc2", "t"); called.add("setRegionSuccess"); same("setRegionSuccess", s);
        assertEquals("rgB/rc2 (region controller)", s.standing().toString());
        s.setRegionGlobalInit("t"); called.add("setRegionGlobalInit"); same("setRegionGlobalInit", s);
        s.setRegionalGlobalSuccess("rgG", "gc", "t"); called.add("setRegionalGlobalSuccess"); same("setRegionalGlobalSuccess", s);
        assertEquals("rgB/rc2 (region controller) under rgG/gc", s.standing().toString());
        assertTrue(s.setRegionShutdown("t")); called.add("setRegionShutdown"); same("setRegionShutdown", s);
        assertEquals(ControllerMode.REGION_SHUTDOWN, s.getControllerState());
        s.setRegionalGlobalFailed("t"); called.add("setRegionalGlobalFailed"); same("setRegionalGlobalFailed", s);
        assertNull(s.standing().parentRegion, "a region that lost its global kept it as its parent");
        s.setGlobalInit("t"); called.add("setGlobalInit"); same("setGlobalInit", s);
        assertTrue(s.standing().global);
        s.setGlobalFailed("t"); called.add("setGlobalFailed"); same("setGlobalFailed", s);
        s.setGlobalSuccess("t"); called.add("setGlobalSuccess"); same("setGlobalSuccess", s);
        assertEquals("rgB/rc2 (global)", s.standing().toString());
        assertNull(s.standing().parentRegion, "the global names itself as its global: never as its parent");
        assertTrue(s.setGlobalShutdown("t")); called.add("setGlobalShutdown"); same("setGlobalShutdown", s);
        assertEquals(ControllerMode.GLOBAL_SHUTDOWN, s.getControllerState());
        s.setStandaloneInit("rgS", "s1", "t"); called.add("setStandaloneInit"); same("setStandaloneInit", s);
        assertEquals("rgS/s1 (agent)", s.standing().toString());
        s.setStandaloneSuccess("rgS", "s2", "t"); called.add("setStandaloneSuccess"); same("setStandaloneSuccess", s);
        s.setStandaloneShutdown("t"); called.add("setStandaloneShutdown"); same("setStandaloneShutdown", s);
        s.setStandaloneFailed("rgS", "s3", "t"); called.add("setStandaloneFailed"); same("setStandaloneFailed", s);
        assertNull(s.standing().region, "a failed standalone node has no region: it places nobody");
        // the shutdown states are taken from another role too
        s.setRegionInit("rgA", "rc", "t");
        s.setRegionalGlobalSuccess("rgG", "gc", "t");
        assertTrue(s.setAgentShutdown("t"));
        same("setAgentShutdown of a region controller", s);
        assertFalse(s.standing().regionController);

        // every method of the state that sets something was called above: a new one must publish, and be added here
        Set<String> setters = new TreeSet<>();
        for (Method m : ControllerStateImp.class.getDeclaredMethods()) {
            if (m.getName().startsWith("set") && m.getReturnType() == boolean.class && Modifier.isPublic(m.getModifiers())) setters.add(m.getName());
        }
        assertEquals(setters, called, "a method that changes the state is not held to publishing its standing");
        assertEquals(20, called.size());
    }
}
