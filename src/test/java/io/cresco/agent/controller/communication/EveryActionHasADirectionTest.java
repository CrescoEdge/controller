package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutor;
import io.cresco.library.capability.CrescoAction;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.ActionAuthorizer.Direction;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every controller action is given a direction on purpose (authority follows the tree): every action an executor
 * declares or dispatches, and every entry of the three role tables, has a direction declared for it on its tier -- none is
 * left to a default -- and the whole table is the checked-in copy ({@code src/test/resources/authority-directions.txt}),
 * so a change of who may manage whom is a change somebody made and reviewed.
 */
class EveryActionHasADirectionTest {

    private static Map<String, ActionAuthorizer> tiers() {
        Map<String, ActionAuthorizer> t = new LinkedHashMap<>();
        t.put("agent", ControllerActionRoles.agentTier(new ActionAuthorizer(ActionAuthorizer.CONTROLLER_BUNDLE)));
        t.put("region", ControllerActionRoles.regionalTier());
        t.put("global", ControllerActionRoles.globalTier());
        return t;
    }

    private static void everyAnnotatedActionHasADirection(Class<?> executor, String tier, ActionAuthorizer table) {
        CrescoAction[] actions = executor.getAnnotationsByType(CrescoAction.class);
        assertTrue(actions.length > 5, executor.getSimpleName() + " declares its actions");
        for (CrescoAction a : actions) {
            assertNotNull(table.declaredDirection(a.type(), a.name()), executor.getSimpleName() + " " + a.type() + " " + a.name()
                    + " has no direction on the " + tier + " tier in ControllerActionRoles");
        }
    }

    @Test
    void everyActionAnExecutorDeclaresHasADirectionOnItsTier() {
        Map<String, ActionAuthorizer> t = tiers();
        everyAnnotatedActionHasADirection(AgentExecutor.class, "agent", t.get("agent"));
        everyAnnotatedActionHasADirection(RegionalExecutor.class, "region", t.get("region"));
        everyAnnotatedActionHasADirection(GlobalExecutor.class, "global", t.get("global"));
    }

    /** {@code TYPE|action} of every {@code case "<action>":} under executeCONFIG and executeEXEC of an executor's source. */
    private static List<String> dispatched(String sourceFile) throws Exception {
        Path src = Paths.get("src/main/java/io/cresco/agent/controller", sourceFile);
        assertTrue(Files.isReadable(src), "the executor's source is read from the module directory: " + src.toAbsolutePath());
        Pattern method = Pattern.compile("public MsgEvent execute([A-Z]+)\\(");
        Pattern aCase = Pattern.compile("case \"([a-z_]+)\":");
        List<String> out = new ArrayList<>();
        String type = null;
        for (String line : Files.readAllLines(src, StandardCharsets.UTF_8)) {
            Matcher m = method.matcher(line);
            if (m.find()) type = m.group(1);
            Matcher c = aCase.matcher(line);
            while (c.find()) {
                assertNotNull(type, "an action dispatched outside an execute method: " + line.trim());
                out.add(type + "|" + c.group(1));
            }
        }
        return out;
    }

    @Test
    void everyActionAnExecutorDispatchesHasADirectionOnItsTier() throws Exception {
        Map<String, ActionAuthorizer> t = tiers();
        String[][] executors = {{"agentcontroller/AgentExecutor.java", "agent", "30"}, {"regionalcontroller/RegionalExecutor.java", "region", "8"},
                {"globalcontroller/GlobalExecutor.java", "global", "30"}};
        for (String[] e : executors) {
            List<String> actions = dispatched(e[0]);
            assertTrue(actions.size() >= Integer.parseInt(e[2]), e[0] + ": its dispatch was not found (" + actions.size() + " actions)");
            for (String key : actions) {
                int bar = key.indexOf('|');
                assertNotNull(t.get(e[1]).declaredDirection(key.substring(0, bar), key.substring(bar + 1)),
                        e[0] + " dispatches " + key + ", which has no direction on the " + e[1] + " tier in ControllerActionRoles");
            }
        }
    }

    @Test
    void everyEntryOfTheRoleTablesHasADirection() {
        for (Map.Entry<String, ActionAuthorizer> tier : tiers().entrySet()) {
            Set<String> keys = tier.getValue().declaredKeys();
            assertTrue(keys.size() > 15, tier.getKey() + " tier: its table was not read");
            for (String key : keys) {
                int bar = key.indexOf('|');
                assertNotNull(tier.getValue().declaredDirection(key.substring(0, bar), key.substring(bar + 1)),
                        "the " + tier.getKey() + " tier gives " + key + " roles and no direction");
            }
            // and no direction for something the table gives no roles (a mistyped name would otherwise go unnoticed)
            for (String key : tier.getValue().directions().keySet()) {
                assertTrue(keys.contains(key), "the " + tier.getKey() + " tier gives " + key + " a direction and no roles");
            }
        }
    }

    /** The directions of every tier, one {@code <tier> <TYPE|action> <direction>} per line, and the floor's defaults. */
    static String table() {
        StringBuilder b = new StringBuilder();
        for (Map.Entry<String, ActionAuthorizer> tier : tiers().entrySet()) {
            for (Map.Entry<String, Direction> d : tier.getValue().directions().entrySet()) {
                b.append(tier.getKey()).append(' ').append(d.getKey()).append(' ').append(d.getValue()).append('\n');
            }
        }
        // what no table lists: the privileged floor of every bundle is DOWN (a plugin's own annotation may say otherwise)
        for (String bundle : new TreeSet<>(ActionAuthorizer.PRIVILEGED.keySet())) {
            ActionAuthorizer plain = new ActionAuthorizer(bundle);
            for (String action : new TreeSet<>(ActionAuthorizer.PRIVILEGED.get(bundle))) {
                b.append("floor ").append(bundle).append(' ').append(action).append(' ').append(plain.directionOf("EXEC", action)).append('\n');
            }
        }
        return b.toString();
    }

    @Test
    void theWholeTableIsTheCheckedInCopy() throws Exception {
        String expected;
        try (InputStream in = EveryActionHasADirectionTest.class.getResourceAsStream("/authority-directions.txt")) {
            assertNotNull(in, "src/test/resources/authority-directions.txt is missing");
            expected = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        StringBuilder copy = new StringBuilder();
        for (String line : expected.split("\n", -1)) {
            if (!line.startsWith("#") && !line.trim().isEmpty()) copy.append(line.trim()).append('\n');
        }
        assertEquals(copy.toString(), table(), "the direction of an action changed, or an action was added or removed: if that is"
                + " meant, change src/test/resources/authority-directions.txt with it (and docs/caller-authorization.md section 3a)");
    }

    @Test
    void whatTheBriefNamesIsWhatTheTablesSay() {
        Map<String, ActionAuthorizer> t = tiers();
        for (String tier : new String[]{"agent", "region"}) {
            ActionAuthorizer a = t.get(tier);
            for (String down : new String[]{"pluginadd", "pluginremove", "pluginupload", "pluginrepopull", "controllerupdate", "stopcontroller",
                    "restartcontroller", "restartframework", "killjvm", "setloglevel", "setlogdp", "getislogdp", "getagentinfo"}) {
                assertEquals(Direction.DOWN, a.directionOf("CONFIG", down), tier + " " + down);
            }
            for (String down : new String[]{"getlog", "getfileinfo", "getfiledata", "getbroadcastdiscovery"}) {
                assertEquals(Direction.DOWN, a.directionOf("EXEC", down), tier + " " + down);
            }
            for (String any : new String[]{"cepadd", "cepremove", "cepinput", "pluginlist", "pluginstatus"}) {
                assertEquals(Direction.ANY, a.directionOf("CONFIG", any), tier + " " + any);
            }
            for (String any : new String[]{"ping", "indirectprobe", "getcontrollerstatus", "getmetricinventory", "listagents"}) {
                assertEquals(Direction.ANY, a.directionOf("EXEC", any), tier + " " + any);
            }
        }
        ActionAuthorizer region = t.get("region"), global = t.get("global"), agent = t.get("agent");
        assertEquals(Direction.UP, region.directionOf("CONFIG", "agent_enable"));
        assertEquals(Direction.UP, region.directionOf("CONFIG", "agent_disable"));
        assertEquals(Direction.DOWN, region.directionOf("CONFIG", "rpipelinesubmit"));
        // on a tier that does not serve them, the registration actions keep the floor's default
        assertEquals(Direction.DOWN, agent.directionOf("CONFIG", "agent_enable"));
        assertEquals(Direction.DOWN, global.directionOf("CONFIG", "agent_disable"));
        for (String up : new String[]{"region_enable", "region_disable", "regionalimport"}) {
            assertEquals(Direction.UP, global.directionOf("CONFIG", up), up);
        }
        for (String down : new String[]{"addplugin", "removeplugin", "gpipelinesubmit", "gpipelineremove", "plugindownload", "savetorepo",
                "setinodestatus"}) {
            assertEquals(Direction.DOWN, global.directionOf("CONFIG", down), down);
        }
        for (ActionAuthorizer a : t.values()) {
            assertEquals(Direction.UP, a.directionOf("WATCHDOG", null));
            assertEquals(Direction.UP, a.directionOf("WATCHDOG", "anything"));
            assertEquals(Direction.UP, a.directionOf("KPI", null));
            for (String type : new String[]{"INFO", "DISCOVER", "ERROR"}) assertEquals(Direction.ANY, a.directionOf(type, null), type);
            // an action nobody listed: the floor's default, else from anywhere (its roles still fail closed)
            assertEquals(Direction.ANY, a.directionOf("EXEC", "nosuchaction"));
        }
    }
}
