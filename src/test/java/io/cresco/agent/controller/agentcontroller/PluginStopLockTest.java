package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression (2026-11-09, GaiaKeep regional_check M0): a plugin's stop that sends a message held up its own agent.
 *
 * <p>PluginAdmin.stopPlugin called the plugin's stop ({@code PluginService.isStopped()}) while it held the plugin lock.
 * Since every send of a plugin is checked against the plugin map (msgOutFromBundle -> bundleRunsPlugin, under the same
 * lock), a plugin that sends during its stop -- gfs's storage node announces its departure to its core peers -- waited
 * for the lock its own stop held: the stop hung until the plugin's own deadline (gfs_stop_max_ms, 25 s), and for all that
 * time the agent answered no pluginlist (the client's list_plugin_agent got no reply: 504) and delivered no message to
 * any of its plugins (msgIn takes the lock too). The plugin's stop now runs outside the lock.
 */
class PluginStopLockTest {

    private static final long BUNDLE = 42L;
    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("plugin-stop-lock");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dbe != null) {
            try { dbe.shutdown(); } catch (Exception ignored) { }
        }
        // DBEngine.shutdown() stops the whole Derby engine, which deregisters the driver
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

    /** A running plugin of bundle {@link #BUNDLE}, as addPlugin leaves it, whose stop is {@code stop}. */
    @SuppressWarnings("unchecked")
    private static void running(PluginAdmin pa, PluginBuilder pb, DBInterfaceImpl gdb, String id, Callable<Boolean> stop) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco.x");
        cfg.put("jarfile", "x.jar");
        cfg.put("persistence_code", "0");
        PluginNode node = new PluginNode(pb, gdb, BUNDLE, id, cfg, null);
        node.setStatus(10, "Plugin Active");
        node.setPluginService((PluginService) Proxy.newProxyInstance(PluginService.class.getClassLoader(),
                new Class<?>[]{PluginService.class},
                (p, m, a) -> "isStopped".equals(m.getName()) ? stop.call()
                        : m.getReturnType() == boolean.class ? Boolean.TRUE : null));
        Configuration conf = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[]{Configuration.class},
                (p, m, a) -> "getPid".equals(m.getName()) ? "pid." + id : null);
        ((Map<String, PluginNode>) field(pa, "pluginMap")).put(id, node);
        ((Map<String, Configuration>) field(pa, "configMap")).put(id, conf);
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
    void aPluginThatSendsDuringItsStopIsNotHeldUpAndTheAgentStillAnswers() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve("agent").toString(), new HashMap<>());
        DBInterfaceImpl gdb = db(pb);
        PluginAdmin pa = admin(pb, gdb);
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            CountDownLatch inStop = new CountDownLatch(1), release = new CountDownLatch(1);
            AtomicReference<Object> sendCheck = new AtomicReference<>();
            // as gfs's stop does: a departure notice from its own thread (the agent checks the bundle runs the plugin),
            // waited for within a deadline; then the stop goes on until the test lets it end
            running(pa, pb, gdb, "plugin-gfs", () -> {
                Future<Boolean> send = pool.submit(() -> pa.bundleRunsPlugin(BUNDLE, "plugin-gfs"));
                try {
                    sendCheck.set(send.get(3, TimeUnit.SECONDS));
                } catch (TimeoutException e) {
                    sendCheck.set("the send waited " + 3 + " s for the plugin lock its own stop held");
                }
                inStop.countDown();
                return release.await(20, TimeUnit.SECONDS);
            });
            running(pa, pb, gdb, "plugin-other", () -> true);

            Future<Boolean> removed = pool.submit(() -> pa.removePlugin("plugin-gfs"));
            assertTrue(inStop.await(10, TimeUnit.SECONDS), "the plugin's stop ran");
            assertEquals(Boolean.TRUE, sendCheck.get(), "a stopping plugin still sends as itself, at once");

            // while that plugin is still stopping, the agent answers: a plugin list, and the send check of another plugin
            Future<String> list = pool.submit(pa::getPluginList);
            String rows = assertDoesNotThrow(() -> list.get(3, TimeUnit.SECONDS), "pluginlist answered during a plugin's stop");
            assertNotNull(rows);
            assertTrue(rows.contains("plugin-other"), rows);
            assertTrue(pool.submit(() -> pa.bundleRunsPlugin(BUNDLE, "plugin-other")).get(3, TimeUnit.SECONDS));
            // the send check stays exact: another bundle never sends as these plugins
            assertFalse(pa.bundleRunsPlugin(BUNDLE + 1, "plugin-gfs"));
            assertFalse(removed.isDone(), "the stop is still under way");

            release.countDown();
            assertTrue(removed.get(10, TimeUnit.SECONDS));
            assertFalse(pa.pluginExist("plugin-gfs"));
            assertFalse(pa.bundleRunsPlugin(BUNDLE, "plugin-gfs"), "a removed plugin no longer sends");
            assertTrue(pa.pluginExist("plugin-other"));
        } finally {
            pool.shutdownNow();
            pa.close();
        }
    }

    @Test
    void concurrentStopsOfOnePluginStopItOnce() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve("agent").toString(), new HashMap<>());
        DBInterfaceImpl gdb = db(pb);
        PluginAdmin pa = admin(pb, gdb);
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            CountDownLatch inStop = new CountDownLatch(1), release = new CountDownLatch(1);
            AtomicInteger stops = new AtomicInteger();
            running(pa, pb, gdb, "plugin-a", () -> {
                stops.incrementAndGet();
                inStop.countDown();
                return release.await(20, TimeUnit.SECONDS);
            });
            // a pluginremove while the agent's shutdown is stopping the same plugin
            Future<Boolean> shutdown = pool.submit(() -> pa.stopPlugin("plugin-a"));
            assertTrue(inStop.await(10, TimeUnit.SECONDS));
            Future<Boolean> remove = pool.submit(() -> pa.removePlugin("plugin-a"));
            Thread.sleep(300);
            assertFalse(remove.isDone(), "the second stop waits for the one under way");
            release.countDown();
            assertTrue(shutdown.get(10, TimeUnit.SECONDS));
            assertTrue(remove.get(10, TimeUnit.SECONDS));
            assertEquals(1, stops.get(), "the plugin's stop ran once");
            assertFalse(pa.pluginExist("plugin-a"));
            assertFalse(gdb.getNodeList("r1", "a1").contains("plugin-a"), "the remove still drops the plugin's row");
        } finally {
            pool.shutdownNow();
            pa.close();
        }
    }
}
