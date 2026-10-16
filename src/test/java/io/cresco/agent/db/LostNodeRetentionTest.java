package io.cresco.agent.db;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
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
 * How long a node that crashed and never came back stays listed as LOST: lost_node_retention_days (default 30),
 * measured from when it went LOST (lost_ts, a persisted column: a controller restart neither resets nor shortens
 * it, and a parent's full-state update rewriting watchdog_ts does not move it).
 */
class LostNodeRetentionTest {

    private static final long DAY = 86_400_000L;

    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("lost-retention");
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

    private PluginBuilder plugin(Map<String, Object> cfg) {
        return TestAgentService.plugin(tmp.toString(), new HashMap<>(cfg));         // the node r1/a1
    }

    /** The region r1 (this node is r1/a1) with agents rc-x and a2 under it, a2 hosting plugin/2. */
    private DBEngine fabric() {
        DBEngine d = new DBEngine(plugin(Map.of()));
        d.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        for (String a : new String[]{"a1", "a2", "a3"}) {
            d.addANode(a, 10, "test", 15000, System.currentTimeMillis(), "{}");
            d.assoicateANodetoRNode("r1", a);
        }
        d.addPNode("a2", "plugin/2", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.x", "x.jar", "1", "m", "{}", 20);
        return d;
    }

    private DBEngine restart() throws Exception {
        dbe.shutdown();
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        return new DBEngine(plugin(Map.of()));
    }

    @Test
    void lostSinceIsStampedOnceAndClearedWhenTheNodeComesBack() throws Exception {
        dbe = fabric();
        assertNull(dbe.getLostSince("r1", "a2", null));

        long before = System.currentTimeMillis();
        dbe.setNodeStatusCode("r1", "a2", null, 50, "agent set LOST by regional controller health watcher");
        Long lostSince = dbe.getLostSince("r1", "a2", null);
        assertNotNull(lostSince);
        assertTrue(lostSince >= before && lostSince <= System.currentTimeMillis());

        Thread.sleep(20);
        // still LOST on later writes: the watchdog marking it again, a parent's full-state update (new watchdog_ts)
        dbe.setNodeStatusCode("r1", "a2", null, 50, "again");
        dbe.updateNode("r1", "a2", null, 50, "from a full-state update", 15000, System.currentTimeMillis(), "{}");
        dbe.updateANode("a2", 50, "lost", 15000, System.currentTimeMillis(), "{}");
        assertEquals(lostSince, dbe.getLostSince("r1", "a2", null), "LOST time moved");

        // the node comes back: no longer LOST
        dbe.updateNode("r1", "a2", null, 10, "active", 15000, System.currentTimeMillis(), "{}");
        assertNull(dbe.getLostSince("r1", "a2", null));
        // and a later loss starts a new period
        dbe.setNodeStatusCode("r1", "a2", null, 50, "lost again");
        assertTrue(dbe.getLostSince("r1", "a2", null) > lostSince);
    }

    @Test
    void everyTierAndInsertPathRecordsTheLoss() {
        dbe = fabric();
        dbe.setNodeStatusCode("r1", "a2", "plugin/2", 50, "plugin lost");
        dbe.setNodeStatusCode("r1", null, null, 50, "region lost");
        assertNotNull(dbe.getLostSince(null, null, "plugin/2"));
        assertNotNull(dbe.getLostSince("r1", null, null));
        // a row first seen already LOST (a child's state export carrying a LOST agent)
        dbe.addANode("a9", 50, "lost", 15000, System.currentTimeMillis(), "{}");
        dbe.addPNode("a9", "plugin/9", 50, "lost", 15000, System.currentTimeMillis(), "n", "j", "1", "m", "{}", 20);
        assertNotNull(dbe.getLostSince(null, "a9", null));
        assertNotNull(dbe.getLostSince(null, null, "plugin/9"));
        dbe.addANode("a8", 10, "ok", 15000, System.currentTimeMillis(), "{}");
        assertNull(dbe.getLostSince(null, "a8", null));
        // every UPDATE path writes it (each statement really ran: its errors are only logged)
        dbe.updateANode("a3", 50, "lost", 15000, System.currentTimeMillis(), "{}");
        assertNotNull(dbe.getLostSince(null, "a3", null));
        dbe.updateANode("a3", 10, "back", 15000, System.currentTimeMillis(), "{}");
        assertNull(dbe.getLostSince(null, "a3", null));
        dbe.updateRNode("r1", 10, "back", 15000, System.currentTimeMillis(), "{}");
        assertNull(dbe.getLostSince("r1", null, null));
        dbe.updateRNode("r1", 50, "lost", 15000, System.currentTimeMillis(), "{}");
        assertNotNull(dbe.getLostSince("r1", null, null));
        dbe.updatePNode("a2", "plugin/2", 10, "back", 15000, System.currentTimeMillis(), "n", "j", "1", "m", "{}", 20);
        assertNull(dbe.getLostSince(null, null, "plugin/2"));
        dbe.updateNode(null, null, "plugin/2", 50, "lost", 15000, System.currentTimeMillis(), "{}");
        assertNotNull(dbe.getLostSince(null, null, "plugin/2"));
        dbe.updateNode("r1", null, null, 10, "back", 15000, System.currentTimeMillis(), "{}");
        assertNull(dbe.getLostSince("r1", null, null));
    }

    @Test
    void aControllerRestartNeitherResetsNorShortensTheRetention() throws Exception {
        dbe = fabric();
        dbe.setNodeStatusCode("r1", "a2", null, 50, "lost");
        Long lostSince = dbe.getLostSince("r1", "a2", null);
        Thread.sleep(20);

        dbe = restart();
        assertEquals(lostSince, dbe.getLostSince("r1", "a2", null), "a restart changed when the node went LOST");

        DBInterfaceImpl gdb = new DBInterfaceImpl(plugin(Map.of()), dbe);
        assertTrue(gdb.purgeExpiredLostNodes(lostSince + 30 * DAY - 1).isEmpty(), "purged before 30 days");
        assertNotNull(dbe.getANode("a2"), "still listed");
    }

    @Test
    void theDefaultRetentionIsThirtyDaysFromWhenTheNodeWentLost() throws Exception {
        dbe = fabric();
        dbe.setNodeStatusCode("r1", "a2", null, 50, "lost");
        dbe.setNodeStatusCode("r1", "a2", "plugin/2", 50, "lost with its agent");
        long lostSince = dbe.getLostSince("r1", "a2", null);
        DBInterfaceImpl gdb = new DBInterfaceImpl(plugin(Map.of()), dbe);
        assertEquals(30 * DAY, gdb.lostRetentionMs());

        assertTrue(gdb.purgeExpiredLostNodes(lostSince + 29 * DAY).isEmpty());
        assertTrue(gdb.purgeExpiredLostNodes(lostSince + 29 * DAY + 1000).isEmpty(), "at most one sweep per interval");

        List<String> purged = gdb.purgeExpiredLostNodes(lostSince + 30 * DAY + 1000);
        assertTrue(purged.contains("a2"), purged.toString());
        assertNull(dbe.getANode("a2"), "the agent row is gone");
        assertNull(dbe.getPNode("plugin/2"), "with its plugins");
        assertNotNull(dbe.getANode("a3"), "ACTIVE agents stay");
        assertNotNull(dbe.getRNode("r1"));
    }

    @Test
    void aLostRegionGoesWithEverythingItHeldButThisNodesOwnRowsNeverDo() throws Exception {
        dbe = fabric();
        dbe.addRNode("r2", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.addANode("b1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode("r2", "b1");
        dbe.setNodeStatusCode("r2", null, null, 50, "region lost");
        dbe.setNodeStatusCode("r2", "b1", null, 50, "agent lost with its region");
        dbe.setNodeStatusCode("r1", null, null, 50, "self marked LOST (a stale row)");
        dbe.setNodeStatusCode("r1", "a1", null, 50, "self marked LOST (a stale row)");

        List<String> purged = dbe.purgeLostNodes(System.currentTimeMillis() + 1, "r1", "a1");
        assertTrue(purged.contains("r2"), purged.toString());
        assertNull(dbe.getRNode("r2"));
        assertNull(dbe.getANode("b1"));
        assertNotNull(dbe.getRNode("r1"), "own region kept");
        assertNotNull(dbe.getANode("a1"), "own agent kept");
    }

    @Test
    void retentionIsConfigurableAndZeroKeepsLostNodesForever() throws Exception {
        dbe = fabric();
        dbe.setNodeStatusCode("r1", "a2", null, 50, "lost");
        long lostSince = dbe.getLostSince("r1", "a2", null);

        DBInterfaceImpl never = new DBInterfaceImpl(plugin(Map.of("lost_node_retention_days", "0")), dbe);
        assertEquals(-1L, never.lostRetentionMs());
        assertTrue(never.purgeExpiredLostNodes(lostSince + 3650 * DAY).isEmpty());

        DBInterfaceImpl short_ = new DBInterfaceImpl(plugin(Map.of("lost_node_retention_days", "2.5")), dbe);
        assertEquals((long) (2.5 * DAY), short_.lostRetentionMs());
        assertTrue(short_.purgeExpiredLostNodes(lostSince + 2 * DAY).isEmpty());
        assertNull(dbe.getLostSince("r1", "a3", null));
        DBInterfaceImpl short2 = new DBInterfaceImpl(plugin(Map.of("lost_node_retention_days", "2.5")), dbe);
        assertTrue(short2.purgeExpiredLostNodes(lostSince + 3 * DAY).contains("a2"));
    }

    @Test
    void aDatabaseFromBeforeRetentionIsMigratedAndItsLostRowsGetTheFullPeriod() throws Exception {
        dbe = fabric();
        dbe.setNodeStatusCode("r1", "a2", null, 50, "lost before the upgrade");
        dbe.shutdown();
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        // make it an old database: no lost_ts column anywhere
        String url = "jdbc:derby:" + tmp.resolve("derbydb-home").resolve("cresco-controller-db");
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            for (String t : new String[]{"rnode", "anode", "pnode"}) {
                st.executeUpdate("ALTER TABLE " + t + " DROP COLUMN lost_ts");
            }
            try (ResultSet rs = c.getMetaData().getColumns(null, null, "ANODE", "LOST_TS")) {
                assertFalse(rs.next());
            }
        }
        try { DriverManager.getConnection("jdbc:derby:;shutdown=true"); } catch (Exception expected) { }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();

        long upgrade = System.currentTimeMillis();
        dbe = new DBEngine(plugin(Map.of()));
        Long lostSince = dbe.getLostSince("r1", "a2", null);
        assertNotNull(lostSince, "the pre-existing LOST row was not stamped");
        assertTrue(lostSince >= upgrade, "counts from the upgrade, never earlier");
        assertNull(dbe.getLostSince("r1", "a3", null));
        // and the migrated schema takes new writes
        dbe.setNodeStatusCode("r1", "a3", null, 50, "lost after the upgrade");
        assertNotNull(dbe.getLostSince("r1", "a3", null));
    }
}
