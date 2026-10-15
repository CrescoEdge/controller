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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression (2026-10-15): a plugin removed from an agent stayed in the GLOBAL plugin inventory
 * (listplugins / clients' find_plugin), so deploy_plugin believed it was still loaded.
 *
 * Root cause: pluginremove went through PluginAdmin.stopPlugin, the shutdown path, which keeps the
 * pnode row of every persistent plugin (persistence_code &gt; 9: system plugins such as stunnel = 20,
 * pipeline-placed = 10). That row IS the inventory: the global reads it directly for its own agent, and
 * a remote agent keeps exporting it upstream, so the per-agent merge never had anything to drop.
 */
class PluginInventoryRemovalTest {

    private Path tmp;
    private DBEngine agentDbe, regionDbe, globalDbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("plugin-inventory");
    }

    @AfterEach
    void tearDown() throws Exception {
        for (DBEngine d : new DBEngine[]{agentDbe, regionDbe, globalDbe}) {
            if (d != null) {
                try { d.shutdown(); } catch (Exception ignored) { }
            }
        }
        // DBEngine.shutdown() stops the whole Derby engine, which deregisters the driver
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static PluginBuilder pb(String dataDir) {
        return TestAgentService.plugin(dataDir, new HashMap<>());
    }

    /** An (empty) controller DB holding region r1 with the given agents. */
    private static DBEngine db(PluginBuilder pb, String... agents) {
        DBEngine dbe = new DBEngine(pb);
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        for (String a : agents) {
            dbe.addANode(a, 10, "test", 15000, System.currentTimeMillis(), "{}");
            dbe.assoicateANodetoRNode("r1", a);
        }
        return dbe;
    }

    private static Object field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(o);
    }

    /** Register a running plugin with PluginAdmin exactly as addPlugin leaves it (pnode row via PluginNode). */
    @SuppressWarnings("unchecked")
    private static void running(PluginAdmin pa, PluginBuilder pb, DBInterfaceImpl gdb, String id, String name, int persistence) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", name);
        cfg.put("jarfile", name + ".jar");
        cfg.put("persistence_code", String.valueOf(persistence));
        PluginNode node = new PluginNode(pb, gdb, 42L, id, cfg, null);
        node.setStatus(10, "Plugin Active");   // as PluginAdmin does once the plugin is started
        node.setPluginService((PluginService) Proxy.newProxyInstance(PluginService.class.getClassLoader(),
                new Class<?>[]{PluginService.class},
                (p, m, a) -> m.getReturnType() == boolean.class ? Boolean.TRUE : null));
        Configuration conf = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[]{Configuration.class},
                (p, m, a) -> "getPid".equals(m.getName()) ? "pid." + id : null);
        ((Map<String, PluginNode>) field(pa, "pluginMap")).put(id, node);
        ((Map<String, Configuration>) field(pa, "configMap")).put(id, conf);
    }

    @Test
    void explicitRemoveDropsAnyPluginFromTheInventoryButShutdownKeepsPersistentOnes() throws Exception {
        String agentData = tmp.resolve("agent").toString();
        PluginBuilder pb = pb(agentData);
        agentDbe = db(pb, "a1");
        DBInterfaceImpl gdb = new DBInterfaceImpl(pb, agentDbe);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(agentData), pb, null, gdb, new MockBundleContext(), null);

        running(pa, pb, gdb, "system-stunnel", "io.cresco.stunnel", 20);
        running(pa, pb, gdb, "plugin-pipeline", "io.cresco.x", 10);
        running(pa, pb, gdb, "plugin-adhoc", "io.cresco.y", 0);
        running(pa, pb, gdb, "system-sysinfo", "io.cresco.sysinfo", 20);
        File stunnelData = new File(agentData + "/plugin-data/system-stunnel");
        File adhocData = new File(agentData + "/plugin-data/plugin-adhoc");
        assertTrue(stunnelData.mkdirs() && adhocData.mkdirs());
        assertEquals(4, gdb.getNodeList("r1", "a1").size());

        // shutdown path: persistent rows survive so StaticPluginLoader restarts them under the same id
        assertTrue(pa.stopPlugin("system-sysinfo"));
        assertTrue(gdb.getNodeList("r1", "a1").contains("system-sysinfo"), "shutdown keeps a system plugin's row");

        // explicit remove of a SYSTEM plugin (the observed bug): gone from the inventory
        assertTrue(pa.removePlugin("system-stunnel"));
        assertFalse(gdb.getNodeList("r1", "a1").contains("system-stunnel"), "removed system plugin must leave the inventory");
        assertTrue(stunnelData.isDirectory(), "a persistent plugin's identity-keyed plugin-data is kept");
        assertFalse(pa.pluginExist("system-stunnel"));

        // explicit remove of a pipeline-placed (code 10) plugin: gone, and so not restarted on boot
        assertTrue(pa.removePlugin("plugin-pipeline"));
        assertFalse(gdb.getNodeList("r1", "a1").contains("plugin-pipeline"));

        // non-persistent plugin: unchanged behaviour (row and plugin-data removed)
        assertTrue(pa.removePlugin("plugin-adhoc"));
        assertFalse(gdb.getNodeList("r1", "a1").contains("plugin-adhoc"));
        assertFalse(adhocData.exists());

        // a leftover row with no running plugin (e.g. one that failed to start) is dropped by a remove
        gdb.addPNode("a1", "plugin-orphan", 10, "stale", 15000, System.currentTimeMillis(), "io.cresco.z", "z.jar", "1", "m", "{}", 20);
        assertTrue(gdb.getNodeList("r1", "a1").contains("plugin-orphan"));
        assertTrue(pa.removePlugin("plugin-orphan"));
        assertFalse(gdb.getNodeList("r1", "a1").contains("plugin-orphan"));

        assertEquals(List.of("system-sysinfo"), gdb.getNodeList("r1", "a1"));
        pa.close();
    }

    @Test
    void aRemovedPluginLeavesTheRegionAndGlobalInventoryWithinOneExportPerTier() throws Exception {
        String agentData = tmp.resolve("agent").toString();
        PluginBuilder pb = pb(agentData);
        agentDbe = db(pb, "a1");
        DBInterfaceImpl gdb = new DBInterfaceImpl(pb, agentDbe);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(agentData), pb, null, gdb, new MockBundleContext(), null);
        running(pa, pb, gdb, "system-stunnel", "io.cresco.stunnel", 20);
        running(pa, pb, gdb, "system-sysinfo", "io.cresco.sysinfo", 20);

        // the region already holds another agent whose plugin must be untouched by a1's update
        regionDbe = db(pb(tmp.resolve("region").toString()), "a1", "a2");
        regionDbe.addPNode("a2", "a2-plugin", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.q", "q.jar", "1", "m", "{}", 0);
        globalDbe = new DBEngine(pb(tmp.resolve("global").toString()));

        Runnable agentTick = () -> {   // ControllerSMHandler.stateUpdateTask AGENT -> registerRegionalListener
            Map<String, String> ex = agentDbe.getDBExport(false, true, true, "r1", "a1", null);
            assertTrue(regionDbe.nodeUpdateStatus("AGENT", null, "a1", null, null, ex.get("agentconfigs"), ex.get("pluginconfigs")));
        };
        Runnable regionTick = () -> {  // stateUpdateTask REGION_GLOBAL -> registerGlobalListener
            Map<String, String> ex = regionDbe.getDBExport(true, true, true, "r1", null, null);
            assertTrue(globalDbe.nodeUpdateStatus("REGION", "r1", null, null, ex.get("regionconfigs"), ex.get("agentconfigs"), ex.get("pluginconfigs")));
        };

        agentTick.run();
        regionTick.run();
        assertTrue(globalDbe.getNodeList("r1", "a1").contains("system-stunnel"));
        assertEquals(List.of("a2-plugin"), globalDbe.getNodeList("r1", "a2"));

        assertTrue(pa.removePlugin("system-stunnel"));
        agentTick.run();
        assertEquals(List.of("system-sysinfo"), regionDbe.getNodeList("r1", "a1"), "region drops it after one agent export");
        regionTick.run();
        assertEquals(List.of("system-sysinfo"), globalDbe.getNodeList("r1", "a1"), "global drops it after one region export");
        assertEquals(List.of("a2-plugin"), globalDbe.getNodeList("r1", "a2"), "the merge is per-agent: a2 is untouched");

        // the agent's LAST plugin removed: the export still carries a1 with an empty list, so it is dropped
        assertTrue(pa.removePlugin("system-sysinfo"));
        agentTick.run();
        regionTick.run();
        assertTrue(globalDbe.getNodeList("r1", "a1").isEmpty());
        assertEquals(List.of("a2-plugin"), globalDbe.getNodeList("r1", "a2"));
        pa.close();
    }
}
