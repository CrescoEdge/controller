package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.test.TestAgentService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A region's state export lists its agents and their plugins in two reads. An agent that joins between them must not
 * reach the global as plugins of an agent the export does not list (the global holds no region for it).
 */
class ExportConsistencyTest {

    private static final Type TYPE = new TypeToken<Map<String, List<Map<String, String>>>>() { }.getType();

    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("export-consistency");
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

    @Test
    void anAgentThatJoinsBetweenTheReadsIsLeftForTheNextExport() {
        // the agent a9 joins r1 right after the agent read lists r1's agents
        dbe = new DBEngine(TestAgentService.plugin(tmp.toString(), new HashMap<>())) {
            private int agentReads;

            @Override
            protected List<String> getNodeListStrict(String regionId, String agentId) throws Exception {
                List<String> list = super.getNodeListStrict(regionId, agentId);
                if ("r1".equals(regionId) && agentId == null && ++agentReads == 1) {
                    addANode("a9", 10, "test", 15000, System.currentTimeMillis(), "{}");
                    assoicateANodetoRNode("r1", "a9");
                }
                return list;
            }
        };
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        for (String a : new String[]{"a1", "a2"}) {
            dbe.addANode(a, 10, "test", 15000, System.currentTimeMillis(), "{}");
            dbe.assoicateANodetoRNode("r1", a);
        }
        dbe.addPNode("a2", "plugin/2", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.x", "x.jar", "1", "m", "{}", 20);

        Map<String, String> export = dbe.getDBExport(true, true, true, "r1", null, null);
        assertNotNull(export);
        Gson gson = new Gson();
        Map<String, List<Map<String, String>>> agents = gson.fromJson(export.get("agentconfigs"), TYPE);
        Map<String, List<Map<String, String>>> plugins = gson.fromJson(export.get("pluginconfigs"), TYPE);
        assertEquals(2, agents.get("r1").size());
        assertFalse(plugins.containsKey("a9"), "plugins of an agent the export does not list: " + plugins.keySet());
        assertEquals(1, plugins.get("a2").size());

        // the next export carries a9 whole
        export = dbe.getDBExport(true, true, true, "r1", null, null);
        agents = gson.fromJson(export.get("agentconfigs"), TYPE);
        plugins = gson.fromJson(export.get("pluginconfigs"), TYPE);
        assertEquals(3, agents.get("r1").size());
        assertTrue(plugins.containsKey("a9"));
    }

    /**
     * A list query that fails aborts the export (null): the lenient list answered with nothing, and an export saying this
     * region has no agents made the parent's reconcile delete every agent row of the region.
     */
    @Test
    void aFailedListReadAbortsTheExportInsteadOfSendingAnEmptyOne() {
        dbe = new DBEngine(TestAgentService.plugin(tmp.toString(), new HashMap<>())) {
            @Override
            protected List<String> getNodeListStrict(String regionId, String agentId) throws Exception {
                if ("r1".equals(regionId) && agentId == null) throw new java.sql.SQLException("lock timeout (test)");
                return super.getNodeListStrict(regionId, agentId);
            }
        };
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.addANode("a1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode("r1", "a1");
        assertNull(dbe.getDBExport(true, true, true, "r1", null, null), "an export was built on a list that could not be read");
        // the lenient read (other callers) still answers, with no rows
        assertTrue(dbe.getNodeList("r1", null).isEmpty());
    }
}
