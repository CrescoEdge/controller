package io.cresco.agent.core;

import io.cresco.agent.controller.communication.CallerStamps;
import io.cresco.agent.db.DBEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Who may write a region's records at the global through a state export. The global of a superuser tenant takes region
 * exports from every tenant's namespace, and the check let an {@code admin} stamp through before it looked at anything
 * else: any tenant's leaf could publish, as {@code api:<its tenant>/x[admin]} (a stamp its own broker accepts and its
 * region's bridge carries), an export for ANY region and rewrite that region's, its agents' and its plugins' rows. An
 * export is now its region's controller's word and nobody else's: a controller, the one that joined this global,
 * reporting its own region; and its agents' rows carry the tenant that controller was verified in.
 */
class RegionExportWriterTest {

    private Path tmp;
    private DBEngine global, regionB;
    private ControllerStatePersistance atGlobal;

    static final Caller RC_A = Caller.controller("ta", "rA", "rcA"), RC_B = Caller.controller("tb", "rB", "rcB");

    @BeforeEach
    void fabric() throws Exception {
        tmp = Files.createTempDirectory("region-export-writer");
        PluginBuilder pb = TestAgentService.plugin(tmp.resolve("global").toString(), new HashMap<>());
        global = new DBEngine(pb);
        atGlobal = new ControllerStatePersistance(pb, global);
        // the controllers that joined this global, each in the tenant it joined in (ControllerEngine.speaksForRegion)
        atGlobal.setRegionControllerCheck(c -> c.equals(RC_A) || c.equals(RC_B));
        // region rB as its own controller holds it: itself and one agent, which says it is of tenant ta
        regionB = new DBEngine(TestAgentService.plugin(tmp.resolve("rB").toString(), new HashMap<>()));
        regionB.addRNode("rB", 10, "ok", 15000, System.currentTimeMillis(), "{}");
        regionB.addANode("b1", 10, "ok", 15000, System.currentTimeMillis(), "{\"tenant_id\":\"ta\"}");
        regionB.assoicateANodetoRNode("rB", "b1");
    }

    @AfterEach
    void close() throws Exception {
        for (DBEngine d : new DBEngine[]{global, regionB}) if (d != null) try { d.shutdown(); } catch (Exception ignored) { }
        try (Stream<Path> w = Files.walk(tmp)) { w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete()); }
    }

    /** Region {@code region}'s export as {@code from} holds it, written by {@code writer}. */
    private static ActiveMQMapMessage export(DBEngine from, String region, Caller writer) throws Exception {
        Map<String, String> e = from.getDBExport(true, true, true, region, null, null);
        ActiveMQMapMessage m = new ActiveMQMapMessage();
        m.setStringProperty("update_mode", "REGION");
        m.setStringProperty("region_id", region);
        for (String k : List.of("regionconfigs", "agentconfigs", "pluginconfigs")) if (e.get(k) != null) m.setString(k, e.get(k));
        if (writer != null) CallerStamps.toJms(writer, m);
        return m;
    }

    private void refusedAndNothingWritten(Caller writer, String region, String why) throws Exception {
        assertNotNull(atGlobal.applyRegionExport(export(regionB, region, writer)), why);
        assertFalse(global.nodeExist("rB", null, null), why + ": the region's row was written");
        assertFalse(global.nodeExist(null, "b1", null), why + ": its agent's row was written");
    }

    @Test
    void anAdminStampOfTheRegionsOwnTenantWritesNoRegionExport() throws Exception {
        refusedAndNothingWritten(Caller.api("tb", "x", List.of("admin")), "rB", "api:tb/x[admin]");
    }

    @Test
    void anAdminStampOfAnotherTenantWritesNoRegionExport() throws Exception {
        refusedAndNothingWritten(Caller.api("ta", "x", List.of("admin")), "rB", "api:ta/x[admin]");
    }

    @Test
    void anotherRegionsControllerWritesNoExportForThisRegion() throws Exception {
        refusedAndNothingWritten(RC_A, "rB", "the controller of region rA");
        refusedAndNothingWritten(Caller.controller("tb", "rB", "b1"), "rB", "an agent of region rB that never joined as its controller");
        refusedAndNothingWritten(Caller.controller("ta", "rB", "rcB"), "rB", "the controller's name in another tenant");
        refusedAndNothingWritten(null, "rB", "no verified writer");
    }

    @Test
    void theRegionsControllerWritesNoExportThatNamesAnotherRegion() throws Exception {
        // rB's records under rA's controller's stamp, announced as region rA's export
        ActiveMQMapMessage m = export(regionB, "rB", RC_A);
        m.setStringProperty("region_id", "rA");
        assertNotNull(atGlobal.applyRegionExport(m));
        assertFalse(global.nodeExist("rB", null, null));
        assertFalse(global.nodeExist(null, "b1", null));
    }

    @Test
    void theRegionsControllersOwnExportIsAppliedWithTheTenantItWasVerifiedIn() throws Exception {
        assertNull(atGlobal.applyRegionExport(export(regionB, "rB", RC_B)));
        assertTrue(global.nodeExist("rB", null, null));
        assertEquals("rB", global.getRNodeFromAnode("b1"));
        assertEquals("tb", global.tenantOfAgentRow("b1"), "the agent's row carries the tenant the export claimed, not its writer's");
    }
}
