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
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: plugin-data purge. A failure is a finding.
 * Review-only: never pushed.
 *
 * <p>{@code pluginadd} takes the new plugin's id from the caller ({@code configparams.inode_id}, PluginAdmin.addPlugin)
 * without any check, and {@code pluginremove purge_data=true} deletes {@code <agent data>/plugin-data/<id>}
 * (PluginAdmin.pluginDataDirectory, FileUtils.deleteDirectory) -- so an id like {@code ../../x} deletes a directory outside
 * plugin-data: the agent's identity ({@code security/}), its registry, another tenant's mount, anything the agent's user
 * may delete. Any plugin id with a separator or ".." should be refused at add, and the purge confined to plugin-data.
 */
class ReviewPhase3PluginDataTest {

    private Path tmp;
    private DBEngine agentDbe;

    @BeforeEach
    void setUp() throws Exception {
        tmp = Files.createTempDirectory("rvp3-plugindata");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (agentDbe != null) try { agentDbe.shutdown(); } catch (Exception ignored) { }
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

    /** A plugin PluginAdmin runs under {@code id}, exactly as addPlugin leaves it (cf. PluginInventoryRemovalTest). */
    @SuppressWarnings("unchecked")
    private static void running(PluginAdmin pa, PluginBuilder pb, DBInterfaceImpl gdb, String id) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco.repo");
        cfg.put("jarfile", "repo.jar");
        cfg.put("persistence_code", "0");
        PluginNode node = new PluginNode(pb, gdb, 42L, id, cfg, null);
        node.setStatus(10, "Plugin Active");
        node.setPluginService((PluginService) Proxy.newProxyInstance(PluginService.class.getClassLoader(),
                new Class<?>[]{PluginService.class}, (p, m, a) -> m.getReturnType() == boolean.class ? Boolean.TRUE : null));
        Configuration conf = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[]{Configuration.class}, (p, m, a) -> "getPid".equals(m.getName()) ? "pid.x" : null);
        ((Map<String, PluginNode>) field(pa, "pluginMap")).put(id, node);
        ((Map<String, Configuration>) field(pa, "configMap")).put(id, conf);
    }

    @Test
    void aPurgeNeverLeavesPluginData() throws Exception {
        String agentData = tmp.resolve("agent").toString();
        PluginBuilder pb = TestAgentService.plugin(agentData, new HashMap<>());
        agentDbe = new DBEngine(pb);
        agentDbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        agentDbe.addANode("a1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        agentDbe.assoicateANodetoRNode("r1", "a1");
        DBInterfaceImpl gdb = new DBInterfaceImpl(pb, agentDbe);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(agentData), pb, null, gdb, new MockBundleContext(), null);

        // what lies next to the agent's data directory: here a stand-in for the agent's identity
        File victim = tmp.resolve("victim").toFile();
        assertTrue(victim.mkdirs());
        Files.writeString(victim.toPath().resolve("identity.p12"), "the agent's key");
        new File(agentData, "plugin-data").mkdirs();

        String id = "../../victim";                         // configparams.inode_id of an admin's pluginadd
        running(pa, pb, gdb, id);
        assertEquals(victim.getCanonicalFile(), pa.pluginDataDirectory(id).getCanonicalFile(), "test premise: the id escapes");
        pa.removePlugin(id, true);                          // pluginremove pluginid=../../victim purge_data=true (admin)
        assertTrue(new File(victim, "identity.p12").isFile(), "pluginremove purge_data=true of plugin id '" + id
                + "' deleted " + victim + ", outside " + agentData + "/plugin-data");
        pa.close();
    }
}
