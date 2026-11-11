package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.mina.statemachine.context.DefaultStateContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The state machine's join of a global ({@code ControllerSMHandler.isRegionGlobal}) tells the engine whom the region is
 * joining for as long as the attempt runs, and how it ended: the broker takes that global's verified bridge for its
 * parent's during the attempt (ParentWhileJoiningTest), a failed attempt leaves the region joining nobody, a recorded
 * join leaves the global remembered as the one joined last.
 */
class JoiningGlobalIsToldTest {

    private ControllerSMHandler handler;
    private final AtomicBoolean reachable = new AtomicBoolean();

    private ControllerEngine region() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("is_region", "true");
        cfg.put("global_controller_host", "127.0.0.1");
        cfg.put("enable_tcp_discovery", "false");
        cfg.put("gc_connect_retry", "2");                    // the join waits two seconds for the bridge, then gives up
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("joining").toString(), cfg, m -> { }, "r1", "rc");
        ControllerEngine engine = new ControllerEngine(null, pb, null, null) {
            @Override public boolean isReachableAgent(String remoteAgentPath) { return reachable.get(); }
        };
        ControllerStateImp cs = new ControllerStateImp(pb, null);
        cs.setRegionSuccess("r1", "rc", "test");
        engine.cstate = cs;
        engine.setBrokeredAgents(new ConcurrentHashMap<>());
        engine.setIncomingCanidateBrokers(new LinkedBlockingQueue<>());
        handler = new ControllerSMHandler(engine);
        Field ctx = ControllerSMHandler.class.getDeclaredField("stateContext");
        ctx.setAccessible(true);
        ctx.set(handler, new DefaultStateContext());
        java.util.List<org.apache.mina.statemachine.State> states = new java.util.ArrayList<>();
        for (io.cresco.library.agent.ControllerMode m : io.cresco.library.agent.ControllerMode.values()) states.add(new org.apache.mina.statemachine.State(m.name()));
        handler.setStateCollection(states);
        return engine;
    }

    private static DiscoveryNode global() {
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.GLOBAL);
        n.discovered_region = "glob";
        n.discovered_agent = "gc";
        n.discovered_ip = "127.0.0.1";
        return n;
    }

    @AfterEach
    void stop() {
        if (handler != null) handler.stopRegion();
    }

    @Test
    void whileAJoinRunsTheRegionIsJoiningThatGlobalAndAFailedJoinEndsIt() throws Exception {
        ControllerEngine engine = region();
        assertNull(engine.parentWhileNotJoined());
        assertFalse(engine.knewAGlobal());
        AtomicReference<PeerIdentity.Expected> during = new AtomicReference<>();
        Thread watch = new Thread(() -> {
            long end = System.currentTimeMillis() + 10_000;
            while (during.get() == null && System.currentTimeMillis() < end) {
                during.set(engine.parentWhileNotJoined());
                try { Thread.sleep(20); } catch (InterruptedException e) { return; }
            }
        });
        watch.start();
        assertFalse(handler.isRegionGlobal(global()), "nothing leads to the global: the join fails");
        watch.join(11_000);
        assertNotNull(during.get(), "during the attempt the engine was not told whom the region is joining");
        assertEquals("glob", during.get().region);
        assertEquals("gc", during.get().agent);
        assertNull(engine.parentWhileNotJoined(), "a join that failed left the region joining the global");
        assertTrue(engine.knewAGlobal(), "a region that began to join a global has known one (it relays no more)");
    }

    @Test
    void aRecordedJoinLeavesTheGlobalRememberedWhenTheStateLosesIt() throws Exception {
        ControllerEngine engine = region();
        reachable.set(true);
        assertTrue(handler.isRegionGlobal(global()), "the global is reachable: the join is recorded");
        assertEquals("glob", engine.cstate.getGlobalRegion());
        engine.cstate.setRegionalGlobalFailed("test: the global was killed");
        assertNull(engine.cstate.getGlobalRegion());
        PeerIdentity.Expected last = engine.parentWhileNotJoined();
        assertNotNull(last, "the global a region joined and lost is forgotten: its bridge, back first, is a peer's again");
        assertEquals("glob", last.region);
    }
}
