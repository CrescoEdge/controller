package io.cresco.agent.controller.statemachine;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The registry keys an agent row (and a plugin row) by its id alone, for the whole fabric. At the global a region's
 * export was applied whatever agent ids it named: tenant ta's region controller exported an entry with the name of tenant
 * tb's agent, which overwrote that agent's row and moved it into ta's region, and its next export, leaving the name out,
 * removed the row. An entry for a name that is another tenant's node here is now refused (the row untouched), and an
 * export's omission removes only its writer's own tenant's rows. An agent that moves between regions of ONE tenant
 * (region_alias) is moved as before.
 */
class RegistryNamesAcrossTenantsTest {

    private Path tmp;
    private DBEngine global, regionA, regionA2, regionB;

    @BeforeEach
    void setUp() throws IOException { tmp = Files.createTempDirectory("registry-names"); }

    @AfterEach
    void tearDown() throws Exception {
        for (DBEngine d : new DBEngine[]{global, regionA, regionA2, regionB}) {
            if (d != null) try { d.shutdown(); } catch (Exception ignored) { }
        }
        try (Stream<Path> w = Files.walk(tmp)) { w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete()); }
    }

    private DBEngine db(String name, String region) {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve(name).toString(), new HashMap<>());
        DBEngine dbe = new DBEngine(pb);
        dbe.addRNode(region, 10, "test", 15000, System.currentTimeMillis(), "{}");
        return dbe;
    }

    private static void agent(DBEngine dbe, String region, String agent, String tenant, String plugin) {
        dbe.addANode(agent, 10, "test", 15000, System.currentTimeMillis(), "{\"tenant_id\":\"" + tenant + "\"}");
        dbe.assoicateANodetoRNode(region, agent);
        dbe.addPNode(agent, plugin, 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.x", "x.jar", "1", "m", "{}", 10);
    }

    /** {@code region}'s export as {@code from} holds it, applied at {@code to} as its controller's of {@code writerTenant}. */
    private static boolean export(DBEngine from, DBEngine to, String region, String writerTenant) {
        Map<String, String> e = from.getDBExport(true, true, true, region, null, null);
        return to.nodeUpdateStatus("REGION", region, null, null, e.get("regionconfigs"), e.get("agentconfigs"), e.get("pluginconfigs"), writerTenant);
    }

    @Test
    void aRegionsExportNeitherTakesOverNorRemovesAnotherTenantsAgent() {
        global = db("global", "glob");
        regionB = db("rB", "rB");
        agent(regionB, "rB", "shared", "tb", "p-b");
        assertTrue(export(regionB, global, "rB", "tb"));
        assertEquals("rB", global.getRNodeFromAnode("shared"));

        // tenant ta's region reports its own agent a1 and an agent with the name of tenant tb's
        regionA = db("rA", "rA");
        agent(regionA, "rA", "a1", "ta", "p-a1");
        agent(regionA, "rA", "shared", "ta", "p-a");
        assertFalse(export(regionA, global, "rA", "ta"), "an entry was refused: the update did not apply cleanly");
        assertEquals("rB", global.getRNodeFromAnode("shared"), "tenant tb's agent was moved into tenant ta's region");
        assertEquals("tb", global.tenantOfAgentRow("shared"), "its row was overwritten");
        assertEquals(List.of("p-b"), global.getNodeList("rB", "shared"), "its plugins were replaced");
        assertEquals("rA", global.getRNodeFromAnode("a1"), "the rest of the export is applied");
        assertTrue(global.foreignRefusedCount() >= 1);

        // its next export leaves the name out: nothing of tenant tb's is removed
        regionA.removeNode("rA", "shared", null);
        assertTrue(export(regionA, global, "rA", "ta"));
        assertTrue(global.nodeExist(null, "shared", null), "an export's omission removed another tenant's agent");
        assertEquals(List.of("shared"), global.getNodeList("rB", null));
        assertEquals(List.of("a1"), global.getNodeList("rA", null));
    }

    @Test
    void aRegionsExportDoesNotTakeOverAPluginHeldByAnotherTenantsAgent() {
        global = db("global", "glob");
        regionB = db("rB", "rB");
        agent(regionB, "rB", "b1", "tb", "p-shared");
        assertTrue(export(regionB, global, "rB", "tb"));
        regionA = db("rA", "rA");
        agent(regionA, "rA", "a1", "ta", "p-shared");                  // the id of tenant tb's plugin, under ta's own agent
        assertFalse(export(regionA, global, "rA", "ta"));
        assertEquals(List.of("p-shared"), global.getNodeList("rB", "b1"), "tenant tb's plugin row was taken over");
        assertTrue(global.getNodeList("rA", "a1") == null || global.getNodeList("rA", "a1").isEmpty());
    }

    @Test
    void anAgentThatMovesBetweenRegionsOfOneTenantIsStillMoved() {
        global = db("global", "glob");
        regionA = db("rA", "rA");
        agent(regionA, "rA", "m0", "ta", "p-m0");
        assertTrue(export(regionA, global, "rA", "ta"));
        // the move (region_alias): region rA2 of the same tenant holds it now
        regionA2 = db("rA2", "rA2");
        agent(regionA2, "rA2", "m0", "ta", "p-m0");
        assertTrue(export(regionA2, global, "rA2", "ta"));
        assertEquals("rA2", global.getRNodeFromAnode("m0"));
        assertEquals(0, global.foreignRefusedCount());
        // and an update bound to no tenant (security off, an operator's action) is applied as before
        regionB = db("rB", "rB");
        agent(regionB, "rB", "m0", "tb", "p-m0");
        assertTrue(export(regionB, global, "rB", null));
        assertEquals("rB", global.getRNodeFromAnode("m0"));
    }
}
