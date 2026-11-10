package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.service.cm.Configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A plugin id is configured once, and a plugin that never started can still be removed. A second add of an id the agent
 * already held (a client's retry after its timeout, a pipeline resubmitted) used to wait for the id in a loop with no
 * sleep and no end -- a thread at 100% CPU for good -- and a plugin whose start had not completed (no service) could be
 * neither removed (its stop failed on the missing service) nor, so, added again.
 */
class PluginAddTwiceTest {

    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("plugin-add-twice");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dbe != null) {
            try { dbe.shutdown(); } catch (Exception ignored) { }
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static Object field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(o);
    }

    private static Configuration config(String id) {
        return (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(), new Class<?>[]{Configuration.class},
                (p, m, a) -> "getPid".equals(m.getName()) ? "pid." + id : null);
    }

    private static Map<String, Object> plugin() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco.x");
        cfg.put("jarfile", "x.jar");
        cfg.put("version", "1");
        cfg.put("persistence_code", "0");
        return cfg;
    }

    private PluginAdmin admin(PluginBuilder pb, DBInterfaceImpl gdb) {
        return new PluginAdmin(new TestAgentService(tmp.resolve("agent").toString()), pb, null, gdb, new MockBundleContext(), null);
    }

    private DBInterfaceImpl db(PluginBuilder pb) {
        dbe = new DBEngine(pb);
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.addANode("a1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode("r1", "a1");
        return new DBInterfaceImpl(pb, dbe);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aSecondAddOfAnIdIsRefusedAtOnce() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve("agent").toString(), new HashMap<>());
        PluginAdmin pa = admin(pb, db(pb));
        ((Map<String, Configuration>) field(pa, "configMap")).put("plugin-1", config("plugin-1"));
        ExecutorService pool = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        try {
            Future<Boolean> second = pool.submit(() -> pa.addConfig("plugin-1", plugin()));
            Boolean added = assertDoesNotThrow(() -> second.get(5, TimeUnit.SECONDS),
                    "a second add of a configured plugin id never returned (it used to spin for good)");
            assertFalse(added, "a second add of a configured plugin id was accepted");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aPluginThatNeverStartedCanBeRemoved() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve("agent").toString(), new HashMap<>());
        DBInterfaceImpl gdb = db(pb);
        PluginAdmin pa = admin(pb, gdb);
        // as addPlugin leaves a plugin whose start timed out: in the maps, with no service
        PluginNode node = new PluginNode(pb, gdb, 42L, "plugin-1", plugin(), null);
        node.setStatus(7, "Plugin instance could not be started");
        Map<String, PluginNode> pluginMap = (Map<String, PluginNode>) field(pa, "pluginMap");
        Map<String, Configuration> configMap = (Map<String, Configuration>) field(pa, "configMap");
        pluginMap.put("plugin-1", node);
        configMap.put("plugin-1", config("plugin-1"));
        ExecutorService pool = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        try {
            Future<Boolean> removed = pool.submit(() -> pa.removePlugin("plugin-1"));
            assertDoesNotThrow(() -> removed.get(10, TimeUnit.SECONDS));
            assertFalse(pluginMap.containsKey("plugin-1"), "a plugin with no service could not be removed");
            assertFalse(configMap.containsKey("plugin-1"), "its configuration stayed: the id can never be added again");
        } finally {
            pool.shutdownNow();
        }
    }
}
