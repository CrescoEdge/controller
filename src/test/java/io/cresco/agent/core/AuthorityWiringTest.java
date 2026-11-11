package io.cresco.agent.core;

import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.MockBundle;
import io.cresco.library.agent.AgentService;
import io.cresco.library.capability.CrescoAction;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.ActionAuthorizer.Authority;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A plugin asks where a caller's word comes from through the AgentService its bundle is given: it must get the engine's
 * answer, never the interface's default (a controller that knows nothing of directions) -- or every plugin's privileged
 * action would stay open to a sibling while the controller's own are closed.
 */
class AuthorityWiringTest {

    private static AgentServiceImpl core(ControllerEngine engine) throws Exception {
        AgentServiceImpl core = new AgentServiceImpl();
        for (String[] f : new String[][]{{"controllerEngine"}, {"plugin"}}) {
            Field field = AgentServiceImpl.class.getDeclaredField(f[0]);
            field.setAccessible(true);
            field.set(core, f[0].equals("plugin") ? engine.getPluginBuilder() : engine);
        }
        return core;
    }

    private static ControllerEngine agent(String mode) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("authority_direction", mode);
        ControllerEngine e = TlsTestSupport.engine("rgA", "a2", cfg);
        TlsTestSupport.State s = (TlsTestSupport.State) e.cstate;
        s.parentRegion = "rgA";
        s.parentAgent = "rc";
        return e;
    }

    @Test
    void everyDefaultOfTheAgentServiceIsAnsweredForABundleByTheController() throws Exception {
        int defaults = 0;
        for (Method m : AgentService.class.getMethods()) {
            if (!m.isDefault()) continue;
            defaults++;
            Method own = BundleAgentService.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
            assertNotNull(own, "BundleAgentService leaves " + m.getName() + " to the interface's default");
        }
        assertTrue(defaults >= 8, "the AgentService defaults were not found");
        assertNotNull(AgentServiceImpl.class.getDeclaredMethod("authority"));
    }

    @Test
    void aBundleIsAnsweredByTheEnginesAuthority() throws Exception {
        ControllerEngine engine = agent("enforce");
        AgentServiceImpl core = core(engine);
        assertSame(engine.authority(), core.authority());
        assertSame(engine.authority(), new BundleAgentService(core, new MockBundle("io.cresco.executor", "1.3")).authority());
        assertEquals(ActionAuthorizer.Mode.ENFORCE, core.authority().mode());
        // before the engine exists nobody can say where a caller stands: fail closed, never the permissive default
        Authority none = new AgentServiceImpl().authority();
        assertEquals(ActionAuthorizer.Mode.ENFORCE, none.mode());
        assertEquals(ActionAuthorizer.Position.UNKNOWN, none.positionOf(null, null));
    }

    @CrescoAction(name = "run_process", type = "CONFIG", summary = "x", roles = {"infra", "admin"})
    @CrescoAction(name = "list_process", type = "CONFIG", summary = "x")
    static final class ProcessExecutor implements Executor {
        final List<String> ran = new CopyOnWriteArrayList<>();
        private MsgEvent run(MsgEvent m) { ran.add(String.valueOf(m.getParam("action"))); return m; }
        public MsgEvent executeCONFIG(MsgEvent m) { return run(m); }
        public MsgEvent executeDISCOVER(MsgEvent m) { return run(m); }
        public MsgEvent executeERROR(MsgEvent m) { return run(m); }
        public MsgEvent executeINFO(MsgEvent m) { return run(m); }
        public MsgEvent executeEXEC(MsgEvent m) { return run(m); }
        public MsgEvent executeWATCHDOG(MsgEvent m) { return run(m); }
        public MsgEvent executeKPI(MsgEvent m) { return run(m); }
    }

    /** The executor plugin on {@code engine}'s node, over the AgentService its bundle is given. */
    private static PluginBuilder executorPlugin(ControllerEngine engine) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginID", "plugin/0");
        PluginBuilder pb = new PluginBuilder(new BundleAgentService(core(engine), new MockBundle("io.cresco.executor", "1.3")),
                "io.cresco.executor.Plugin", null, cfg);
        pb.getAuthorizer().setBundle("io.cresco.executor");
        pb.setExecutor(new ProcessExecutor());
        return pb;
    }

    private static MsgEvent runProcess(Caller from) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, from.getRegion(), from.getAgent(), null, "rgA", "a2", "plugin/0", false, false);
        m.setParam("action", "run_process");
        from.stamp(m);
        return m;
    }

    @Test
    void theExecutorPluginsRunProcessFromASiblingAgentIsRefusedInEnforce() throws Exception {
        PluginBuilder executor = executorPlugin(agent("enforce"));
        ActionAuthorizer.Decision sibling = executor.authorize(runProcess(Caller.controller("t1", "rgA", "a1")));
        assertFalse(sibling.allowed, "a sibling agent's controller (infra of the tenant) ran a process here");
        assertEquals("unauthorized: run_process is honoured only from the node itself or a controller above it"
                + " (caller controller:t1/rgA/a1[infra] speaks from a node elsewhere)", sibling.statusDesc());
        assertFalse(executor.authorize(runProcess(Caller.controller("t1", "rgB", "rc2"))).allowed, "another region's controller");
        assertTrue(executor.authorize(runProcess(Caller.controller("t1", "rgA", "rc"))).allowed, "the region controller above");
        assertTrue(executor.authorize(runProcess(Caller.controller("t1", "rgA", "a2"))).allowed, "the node's own controller");
        // an action of the plugin outside its floor, from the sibling
        MsgEvent list = runProcess(Caller.controller("t1", "rgA", "a1"));
        list.setParam("action", "list_process");
        assertTrue(executor.authorize(list).allowed);
        executor.close();
    }

    @Test
    void inLogModeThePluginsCallRunsAndTheNodeCountsIt() throws Exception {
        ControllerEngine engine = agent("log");
        PluginBuilder executor = executorPlugin(engine);
        assertTrue(executor.admit(runProcess(Caller.controller("t1", "rgA", "a1"))));
        assertTrue(executor.admit(runProcess(Caller.controller("t1", "rgA", "a1"))));
        assertEquals(2, engine.authority().wouldRefuseCount("run_process"), "a plugin's would-refuse is counted by its node's controller");
        assertTrue(executor.admit(runProcess(Caller.controller("t1", "rgA", "rc"))));
        assertEquals(2, engine.authority().wouldRefuseCount());
        executor.close();
    }
}
