package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.controller.communication.MsgCounts;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A message for a plugin of this node is counted as what became of it: delivered to its plugin, or dropped because the
 * plugin is not active, or because this node runs no such plugin. A message to an inactive plugin wrote one ERROR line
 * each (260 a minute on a live deployment while a plugin loaded) and was no number; one to a plugin the node does not
 * run vanished without a word.
 */
class MessagesToPluginsAreCountedTest {

    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("plugin-messages-counted");
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

    /** A plugin of this node as addPlugin leaves it; with {@code inMsg} it is started (active), without it is not yet. */
    @SuppressWarnings("unchecked")
    private static void plugin(PluginAdmin pa, PluginBuilder pb, DBInterfaceImpl gdb, String id, Consumer<MsgEvent> inMsg) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco." + id);
        cfg.put("jarfile", id + ".jar");
        cfg.put("persistence_code", "0");
        PluginNode node = new PluginNode(pb, gdb, 42L, id, cfg, null);
        node.setStatus(10, "Plugin Active");
        if (inMsg != null) node.setPluginService((PluginService) Proxy.newProxyInstance(PluginService.class.getClassLoader(),
                new Class<?>[]{PluginService.class}, (p, m, a) -> {
                    if ("inMsg".equals(m.getName())) {
                        inMsg.accept((MsgEvent) a[0]);
                        return Boolean.TRUE;
                    }
                    return m.getReturnType() == boolean.class ? Boolean.TRUE : null;
                }));
        Configuration conf = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[]{Configuration.class}, (p, m, a) -> "getPid".equals(m.getName()) ? "pid." + id : null);
        ((Map<String, PluginNode>) field(pa, "pluginMap")).put(id, node);
        ((Map<String, Configuration>) field(pa, "configMap")).put(id, conf);
    }

    private static MsgEvent to(String plugin) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", "plugin-x", "r1", "a1", plugin, false, false);
        m.setParam("action", "ping");
        return m;
    }

    @Test
    void deliveredToAnActivePluginDroppedForAnInactiveOrUnknownOneAndLoggedOncePerCause() throws Exception {
        String dir = tmp.resolve("agent").toString();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "false");
        PluginBuilder agent = TestAgentService.plugin(dir, cfg);
        dbe = new DBEngine(agent);
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.addANode("a1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode("r1", "a1");
        DBInterfaceImpl gdb = new DBInterfaceImpl(agent, dbe);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(dir), agent, null, gdb, new MockBundleContext(), null);
        ControllerEngine engine = new ControllerEngine(null, agent, pa, null);
        engine.cstate = TlsTestSupport.state(agent, "r1", "a1");
        MsgCounts counts = engine.msgCounts();
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        try {
            java.lang.reflect.Field tap = MsgCounts.class.getDeclaredField("lineTap");
            tap.setAccessible(true);
            tap.set(counts, (Consumer<String>) lines::add);
            AtomicInteger got = new AtomicInteger();
            plugin(pa, agent, gdb, "plugin-up", m -> got.incrementAndGet());
            plugin(pa, agent, gdb, "plugin-loading", null);

            assertEquals(PluginAdmin.Delivery.DELIVERED, pa.msgIn(to("plugin-up")));
            assertEquals(PluginAdmin.Delivery.INACTIVE, pa.msgIn(to("plugin-loading")));
            assertEquals(PluginAdmin.Delivery.UNKNOWN, pa.msgIn(to("plugin-gone")));
            assertEquals(1, got.get());

            // through the router, as a message off the wire for a plugin of this node goes
            for (int i = 0; i < 2; i++) engine.msgIn(to("plugin-up"), true);
            for (int i = 0; i < 260; i++) engine.msgIn(to("plugin-loading"), true);
            for (int i = 0; i < 3; i++) engine.msgIn(to("plugin-gone"), true);
            assertEquals(3, got.get());
            assertEquals(2, counts.deliveredCount());
            assertEquals(260, counts.droppedCount(MsgCounts.Drop.PLUGIN_INACTIVE));
            assertEquals(3, counts.droppedCount(MsgCounts.Drop.PLUGIN_UNKNOWN));
            assertEquals(263, counts.droppedCount());
            assertEquals(2, lines.size(), "260 messages to a plugin that is loading, 3 to one that is gone: one line per cause, " + lines.size() + " written");
            assertEquals("MESSAGE DROPPED: a message for a plugin that is not active (MESSAGE SENT TO INACTIVE PLUGIN) [EXEC ping] r1_a2/plugin-x -> r1_a1/plugin-loading"
                    + " (1 dropped so far; 1 of this kind, plugin_inactive; each kind is logged at most every 10 s)", lines.get(0));
            assertTrue(lines.get(1).endsWith(" r1_a2/plugin-x -> r1_a1/plugin-gone (261 dropped so far; 1 of this kind, plugin_unknown;"
                    + " each kind is logged at most every 10 s)"), lines.get(1));
        } finally {
            pa.close();
        }
    }
}
