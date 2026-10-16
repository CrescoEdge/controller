package io.cresco.agent.controller.statemachine;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
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
 * An agent the parent marked LOST re-registers (agent_enable): its plugins must be visible at the parent as soon
 * as the registration is acknowledged. LOST sets the agent's plugin rows to status 50 (RegionHealthWatcher /
 * GlobalHealthWatcher setPluginsLost) and listplugins shows only status 10; agent_enable used to carry only the
 * agent row, so the plugins stayed hidden until a later state export restored them -- which the diff-gate sent
 * only on the periodic full tick (up to 60 s).
 */
class ReRegistrationAfterLostTest {

    private Path tmp;
    private DBEngine agentDbe, regionDbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("rereg-after-lost");
    }

    @AfterEach
    void tearDown() throws Exception {
        for (DBEngine d : new DBEngine[]{agentDbe, regionDbe}) {
            if (d != null) {
                try { d.shutdown(); } catch (Exception ignored) { }
            }
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static PluginBuilder pb(String dataDir) {
        return TestAgentService.plugin(dataDir, new HashMap<>());
    }

    private static DBEngine db(PluginBuilder pb, String... agents) {
        DBEngine dbe = new DBEngine(pb);
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        for (String a : agents) {
            dbe.addANode(a, 10, "test", 15000, System.currentTimeMillis(), "{}");
            dbe.assoicateANodetoRNode("r1", a);
        }
        return dbe;
    }

    /** The agent_enable registerAgent sends (ControllerSMHandler.registerAgent). */
    private MsgEvent agentEnable(boolean withPlugins) {
        MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a1", null, "r1", "rc", null, true, true);
        enable.setParam("action", "agent_enable");
        enable.setParam("region_name", "r1");
        enable.setParam("agent_name", "a1");
        enable.setParam("mode", "AGENT");
        Map<String, String> export = agentDbe.getDBExport(false, true, true, "r1", "a1", null);
        if (withPlugins) {
            assertTrue(ControllerSMHandler.putRegistrationExport(enable, export));
        } else {   // what registerAgent sent before
            enable.setCompressedParam("agentconfigs", export.get("agentconfigs"));
        }
        return enable;
    }

    /** RegionHealthWatcher: the agent went STALE then LOST, and its plugins with it. */
    private void markLost(DBEngine parent) {
        parent.setNodeStatusCode("r1", "a1", null, 50, "agent set LOST by regional controller health watcher");
        for (String p : List.of("system-stunnel", "system-sysinfo")) {
            parent.setNodeStatusCode("r1", "a1", p, 50, "plugin set LOST due to agent loss");
        }
    }

    @Test
    void plugins_are_visible_as_soon_as_the_re_registration_is_acknowledged() throws Exception {
        PluginBuilder apb = pb(tmp.resolve("agent").toString());
        agentDbe = db(apb, "a1");
        agentDbe.addPNode("a1", "system-stunnel", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.stunnel", "stunnel.jar", "1", "m", "{\"pluginname\":\"io.cresco.stunnel\"}", 20);
        agentDbe.addPNode("a1", "system-sysinfo", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.sysinfo", "sysinfo.jar", "1", "m", "{\"pluginname\":\"io.cresco.sysinfo\"}", 20);

        PluginBuilder rpb = pb(tmp.resolve("region").toString());
        regionDbe = db(rpb, "a1");
        DBInterfaceImpl region = new DBInterfaceImpl(rpb, regionDbe);

        // initial registration + first export: both plugins listed at the parent
        assertTrue(region.nodeUpdate(agentEnable(true)));
        assertEquals(2, region.getNodeList("r1", "a1").size());

        // parent marks the agent LOST: its plugins disappear from listplugins
        markLost(regionDbe);
        assertTrue(region.getNodeList("r1", "a1").isEmpty());

        // the old agent_enable (agent row only) re-registers the agent but leaves the plugins hidden
        assertTrue(region.nodeUpdate(agentEnable(false)));
        assertTrue(region.getNodeList("r1", "a1").isEmpty(), "before the fix: hidden until a later full export");

        // the agent_enable registerAgent now sends restores them in the same RPC
        markLost(regionDbe);
        assertTrue(region.nodeUpdate(agentEnable(true)));
        List<String> listed = region.getNodeList("r1", "a1");
        assertEquals(2, listed.size());
        assertTrue(listed.containsAll(List.of("system-stunnel", "system-sysinfo")));
    }

    @Test
    void a_plugin_removed_while_lost_is_not_revived() throws Exception {
        PluginBuilder apb = pb(tmp.resolve("agent").toString());
        agentDbe = db(apb, "a1");
        agentDbe.addPNode("a1", "system-stunnel", 10, "ok", 15000, System.currentTimeMillis(), "io.cresco.stunnel", "stunnel.jar", "1", "m", "{}", 20);

        PluginBuilder rpb = pb(tmp.resolve("region").toString());
        regionDbe = db(rpb, "a1");
        DBInterfaceImpl region = new DBInterfaceImpl(rpb, regionDbe);
        // the parent still holds a row for a plugin the agent removed while it was away (LOST)
        regionDbe.addPNode("a1", "plugin-gone", 50, "lost", 15000, System.currentTimeMillis(), "io.cresco.x", "x.jar", "1", "m", "{}", 0);

        assertTrue(region.nodeUpdate(agentEnable(true)));
        // listed = exactly the agent's own list; the stale row is not revived -- and it is deleted, not left
        // hidden (LOST) forever (LostRowReconcileTest)
        assertEquals(List.of("system-stunnel"), region.getNodeList("r1", "a1"));
        assertFalse(regionDbe.nodeExist(null, null, "plugin-gone"));
    }

    @Test
    void no_registration_without_the_agent_row() {
        MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a1", null, "r1", "rc", null, true, true);
        assertFalse(ControllerSMHandler.putRegistrationExport(enable, null));
        assertFalse(ControllerSMHandler.putRegistrationExport(enable, new HashMap<>()));
        assertFalse(enable.paramsContains("pluginconfigs"));
    }
}
