package io.cresco.agent.controller.statemachine;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rows a parent holds as LOST/STALE are reconciled and removed like ACTIVE ones.
 *
 * <p>The parent's not-in-update sweep (DBEngine.nodeUpdateStatus) and its removal helpers listed children with
 * getNodeList, which returns status-10 rows only. A plugin (or agent) removed while its node was LOST at the parent
 * therefore kept a hidden status-50 row there forever, and removing an agent or region orphaned its non-ACTIVE
 * plugin rows (and, for a region, every agent row). And a LOST node that never re-registered (it kept its session)
 * was only restored by the periodic full export; the parent now asks it on the pong (StateResync).
 */
class LostRowReconcileTest {

    private Path tmp;
    private DBEngine childDbe, parentDbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("lost-row-reconcile");
    }

    @AfterEach
    void tearDown() throws Exception {
        for (DBEngine d : new DBEngine[]{childDbe, parentDbe}) {
            if (d != null) {
                try { d.shutdown(); } catch (Exception ignored) { }
            }
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private DBEngine db(String name, String region, String... agents) {
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve(name).toString(), new HashMap<>());
        DBEngine dbe = new DBEngine(pb);
        dbe.addRNode(region, 10, "test", 15000, System.currentTimeMillis(), "{}");
        for (String a : agents) {
            addAgent(dbe, region, a);
        }
        return dbe;
    }

    private static void addAgent(DBEngine dbe, String region, String agent) {
        dbe.addANode(agent, 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode(region, agent);
    }

    private static void addPlugin(DBEngine dbe, String agent, String pluginId) {
        dbe.addPNode(agent, pluginId, 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.x", "x.jar", "1", "m", "{}", 20);
    }

    /** An agent's full state export to its region (stateUpdateTask AGENT, or agent_enable). */
    private boolean agentExport(DBEngine from, DBEngine to, String region, String agent) {
        Map<String, String> export = from.getDBExport(false, true, true, region, agent, null);
        return to.nodeUpdateStatus("AGENT", null, agent, null, null, export.get("agentconfigs"), export.get("pluginconfigs"));
    }

    /** A region's full state export to the global (stateUpdateTask REGION_GLOBAL). */
    private boolean regionExport(DBEngine from, DBEngine to, String region) {
        Map<String, String> export = from.getDBExport(true, true, true, region, null, null);
        return to.nodeUpdateStatus("REGION", region, null, null, export.get("regionconfigs"), export.get("agentconfigs"), export.get("pluginconfigs"));
    }

    private static void markAgentLost(DBEngine parent, String region, String agent, String... plugins) {
        parent.setNodeStatusCode(region, agent, null, 50, "agent set LOST");
        for (String p : plugins) {
            parent.setNodeStatusCode(region, agent, p, 50, "plugin set LOST due to agent loss");
        }
    }

    private static boolean pluginRow(DBEngine dbe, String pluginId) {
        return dbe.nodeExist(null, null, pluginId);
    }

    @Test
    void a_plugin_removed_while_the_agent_was_lost_is_deleted_at_the_parent() {
        childDbe = db("agent", "r1", "a1");
        addPlugin(childDbe, "a1", "p-keep");
        addPlugin(childDbe, "a1", "p-gone");
        parentDbe = db("region", "r1", "a1");

        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));
        assertEquals(2, parentDbe.getNodeList("r1", "a1").size());

        markAgentLost(parentDbe, "r1", "a1", "p-keep", "p-gone");
        // while LOST at the parent, the plugin is removed on the agent
        assertTrue(childDbe.removeNode("r1", "a1", "p-gone"));

        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));
        assertEquals(List.of("p-keep"), parentDbe.getNodeList("r1", "a1"));
        assertFalse(pluginRow(parentDbe, "p-gone"), "the hidden LOST row is deleted, not kept forever");
        assertEquals(1, parentDbe.getNodeStatusCodeMap("r1", "a1").size());
    }

    @Test
    void an_agent_with_no_plugins_left_clears_all_its_rows() {
        childDbe = db("agent", "r1", "a1");
        addPlugin(childDbe, "a1", "p1");
        parentDbe = db("region", "r1", "a1");
        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));

        markAgentLost(parentDbe, "r1", "a1", "p1");
        assertTrue(childDbe.removeNode("r1", "a1", "p1"));
        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));
        assertFalse(pluginRow(parentDbe, "p1"));
        assertTrue(parentDbe.getNodeStatusCodeMap("r1", "a1").isEmpty());
    }

    @Test
    void the_global_drops_agents_and_plugins_a_returning_region_no_longer_reports() {
        // region r1 with agents a1 (plugin p1) and a2 (plugin p2)
        childDbe = db("region", "r1", "a1", "a2");
        addPlugin(childDbe, "a1", "p1");
        addPlugin(childDbe, "a2", "p2");
        parentDbe = db("global", "g");
        assertTrue(regionExport(childDbe, parentDbe, "r1"));
        assertEquals(2, parentDbe.getNodeList("r1", null).size());

        // the global marks the region LOST (GlobalHealthWatcher: region, its agents, their plugins -> 50)
        parentDbe.setNodeStatusCode("r1", null, null, 50, "region set LOST");
        markAgentLost(parentDbe, "r1", "a1", "p1");
        markAgentLost(parentDbe, "r1", "a2", "p2");

        // meanwhile a2 leaves the region for good and a1 drops p1
        assertTrue(childDbe.removeNode("r1", "a2", null));
        assertTrue(childDbe.removeNode("r1", "a1", "p1"));

        assertTrue(regionExport(childDbe, parentDbe, "r1"));
        assertEquals(10, parentDbe.getNodeStatusCode("r1", null));
        assertEquals(List.of("a1"), parentDbe.getNodeList("r1", null));
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode("r1", "a2"), "the LOST agent row is deleted");
        assertFalse(pluginRow(parentDbe, "p2"));
        assertFalse(pluginRow(parentDbe, "p1"));
    }

    @Test
    void removing_an_agent_removes_its_non_active_plugin_rows() {
        parentDbe = db("region", "r1", "a1");
        addPlugin(parentDbe, "a1", "p1");
        addPlugin(parentDbe, "a1", "p2");
        markAgentLost(parentDbe, "r1", "a1", "p1");          // p1 LOST, p2 ACTIVE

        assertTrue(parentDbe.removeNode("r1", "a1", null));  // agent_disable / sweep
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode("r1", "a1"));
        assertFalse(pluginRow(parentDbe, "p1"), "a LOST plugin row is not orphaned");
        assertFalse(pluginRow(parentDbe, "p2"));
    }

    @Test
    void removing_a_region_removes_its_agent_rows_too() {
        parentDbe = db("global", "g");
        parentDbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        addAgent(parentDbe, "r1", "a1");
        addAgent(parentDbe, "r1", "a2");
        addPlugin(parentDbe, "a1", "p1");
        addPlugin(parentDbe, "a2", "p2");
        markAgentLost(parentDbe, "r1", "a2", "p2");

        assertTrue(parentDbe.removeNode("r1", null, null));  // region unregisters (globalDisable)
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode("r1", null));
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode("r1", "a1"), "agent rows were orphaned before");
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode("r1", "a2"));
        assertFalse(pluginRow(parentDbe, "p1"));
        assertFalse(pluginRow(parentDbe, "p2"));
    }

    @Test
    void removing_an_agent_without_its_region_works() {
        parentDbe = db("region", "r1", "a1");
        addPlugin(parentDbe, "a1", "p1");
        parentDbe.removeNode(null, "a1", null);   // was "DELETE FROM anode WHERE region_id = ?": no such column
        assertEquals(DBEngine.NO_ROW, parentDbe.getNodeStatusCode(null, "a1"));
        assertFalse(pluginRow(parentDbe, "p1"));
    }

    @Test
    void a_lost_child_that_pings_is_asked_for_its_full_state_until_restored() {
        childDbe = db("agent", "r1", "a1");
        addPlugin(childDbe, "a1", "p1");
        parentDbe = db("region", "r1", "a1");
        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));

        MsgEvent pong = ping();
        assertFalse(StateResync.stampPong(pong, parentDbe.getNodeStatusCode("r1", "a1")), "ACTIVE: plain pong");
        assertFalse(pong.paramsContains(StateResync.PARAM));

        // LOST at the parent; the agent never noticed (kept its session) and pings again
        markAgentLost(parentDbe, "r1", "a1", "p1");
        assertTrue(parentDbe.getNodeList("r1", "a1").isEmpty());
        pong = ping();
        assertTrue(StateResync.stampPong(pong, parentDbe.getNodeStatusCode("r1", "a1")));
        assertEquals("50", pong.getParam(StateResync.STATUS_PARAM));

        StateResync child = new StateResync(10_000L);
        assertTrue(child.shouldResync(pong, 1_000L));
        // the full export it triggers restores the agent and its plugins
        assertTrue(agentExport(childDbe, parentDbe, "r1", "a1"));
        assertEquals(List.of("p1"), parentDbe.getNodeList("r1", "a1"));
        pong = ping();
        assertFalse(StateResync.stampPong(pong, parentDbe.getNodeStatusCode("r1", "a1")));
        assertFalse(child.shouldResync(pong, 2_000L));

        // no row at all (e.g. the parent's DB was reset) also asks; a DB error does not
        assertTrue(StateResync.stampPong(ping(), parentDbe.getNodeStatusCode("r1", "a-unknown")));
        assertFalse(StateResync.stampPong(ping(), DBEngine.STATUS_UNKNOWN));
    }

    @Test
    void the_child_resyncs_at_most_once_per_interval() {
        StateResync child = new StateResync(10_000L);
        MsgEvent asks = ping();
        StateResync.stampPong(asks, 50);
        assertTrue(child.shouldResync(asks, 100_000L));
        assertFalse(child.shouldResync(asks, 105_000L), "a burst of marked pongs costs one full export");
        assertTrue(child.shouldResync(asks, 110_000L));
        assertFalse(child.shouldResync(null, 200_000L));
        assertFalse(child.shouldResync(ping(), 200_000L));
    }

    private static MsgEvent ping() {
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r1", "rc", null, true, true);
        ping.setParam("action", "ping");
        return ping;
    }
}
