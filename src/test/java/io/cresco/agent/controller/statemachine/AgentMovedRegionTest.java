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
 * GaiaKeep gfs#26: an agent that moved from the global's own region into a region of its own (its site's regional
 * broker, region_alias) belongs to that region only at the global.
 *
 * <p>The agent row is keyed by agent id alone, but region membership (agentof) gained a second row when the new
 * region exported the agent, and the old one stayed. The global, which is also the regional controller of its own
 * region, then kept the moved agent among its own children: its watcher aged the shared row STALE and LOST (no
 * watchdog from it in that region any more), and its reconcile removed the agent with its plugins (removeNode in the
 * old region deleted the shared row) -- a live agent fell out of the fabric's listing a few minutes after it moved
 * (seen on the release candidate: sitea-core/-store/-tape gone from listagents after sitec and siteb moved). The
 * region's export also re-inserted the existing agent row (a duplicate key) instead of updating it.
 */
class AgentMovedRegionTest {

    private Path tmp;
    private DBEngine regionDbe, globalDbe;

    @BeforeEach
    void setUp() throws IOException { tmp = Files.createTempDirectory("agent-moved-region"); }

    @AfterEach
    void tearDown() throws Exception {
        for (DBEngine d : new DBEngine[]{regionDbe, globalDbe}) {
            if (d != null) try { d.shutdown(); } catch (Exception ignored) { }
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) { w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete()); }
    }

    private DBEngine db(String name, String region) {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve(name).toString(), new HashMap<>());
        DBEngine dbe = new DBEngine(pb);
        dbe.addRNode(region, 10, "test", 15000, System.currentTimeMillis(), "{}");
        return dbe;
    }

    private static void agent(DBEngine dbe, String region, String agent, String plugin) {
        dbe.addANode(agent, 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode(region, agent);
        dbe.addPNode(agent, plugin, 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.gfs", "gfs.jar", "1", "m", "{}", 10);
    }

    private static boolean regionExport(DBEngine from, DBEngine to, String region) {
        Map<String, String> export = from.getDBExport(true, true, true, region, null, null);
        return to.nodeUpdateStatus("REGION", region, null, null, export.get("regionconfigs"), export.get("agentconfigs"), export.get("pluginconfigs"));
    }

    @Test
    void a_moved_agent_belongs_to_its_new_region_only_and_survives_the_old_regions_sweeps() {
        // the hub shape: sitea-core attached to the global, in the global's own region
        globalDbe = db("global", "gaiakeep");
        agent(globalDbe, "gaiakeep", "sitea-core", "gk-core-sitea");
        // the move: sitea's regional controller holds it now, and exports its region to the global
        regionDbe = db("sitea", "sitea");
        agent(regionDbe, "sitea", "sitea-core", "gk-core-sitea");
        assertTrue(regionExport(regionDbe, globalDbe, "sitea"));

        assertEquals("sitea", globalDbe.getRNodeFromAnode("sitea-core"));
        assertEquals(List.of("sitea-core"), globalDbe.getNodeList("sitea", null));
        assertFalse(globalDbe.getNodeList("gaiakeep", null).contains("sitea-core"), "no longer a child of the global's own region");
        assertFalse(globalDbe.getNodeStatusCodeMap("gaiakeep", null).containsKey("sitea-core"),
                "the global's own-region watcher does not age it");

        // what the global's own region does to a child it no longer hears from: LOST, then removed
        globalDbe.setNodeStatusCode("gaiakeep", "sitea-core", null, 50, "agent set LOST by regional controller health watcher");
        globalDbe.removeNode("gaiakeep", "sitea-core", null);
        assertTrue(globalDbe.nodeExist(null, "sitea-core", null), "the moved agent's row survives the old region's sweep");
        assertEquals(Integer.valueOf(10), globalDbe.getNodeStatusCodeMap("sitea", null).get("sitea-core"));
        assertEquals(List.of("gk-core-sitea"), globalDbe.getNodeList("sitea", "sitea-core"));

        // and its region's next export keeps it current
        assertTrue(regionExport(regionDbe, globalDbe, "sitea"));
        assertEquals(List.of("sitea-core"), globalDbe.getNodeList("sitea", null));
    }

    @Test
    void moving_back_works_the_same_way() {
        globalDbe = db("global", "gaiakeep");
        globalDbe.addRNode("sitea", 10, "test", 15000, System.currentTimeMillis(), "{}");
        agent(globalDbe, "sitea", "sitea-core", "gk-core-sitea");
        globalDbe.assoicateANodetoRNode("gaiakeep", "sitea-core");       // rolled back into the global's region
        assertEquals("gaiakeep", globalDbe.getRNodeFromAnode("sitea-core"));
        assertFalse(globalDbe.getNodeList("sitea", null).contains("sitea-core"));
        assertTrue(globalDbe.getNodeList("gaiakeep", null).contains("sitea-core"));
    }
}
