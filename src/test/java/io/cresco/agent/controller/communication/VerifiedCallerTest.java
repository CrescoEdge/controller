package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.agentcontroller.AgentExecutor;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutor;
import io.cresco.library.capability.CrescoAction;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 of docs/caller-authorization.md, unit level: what a connection may declare, how a stamp crosses the wire, the
 * controller's action tables on every tier, registry binding, the stdin guard and the relay tenant rule.
 */
class VerifiedCallerTest {

    static final String T = "t1", SU = "cresco-system";
    static final CrescoIdentity A1 = CrescoIdentity.of(T, "rgA", "a1", null);

    // ------------------------------------------------------------------ what a node may declare

    @Test
    void aNodeDeclaresOnlyItselfItsPluginsAndItsTenantsUsers() {
        Set<String> su = Set.of(SU);
        assertNull(CallerStamps.acceptDeclared(A1, Caller.controller(T, "rgA", "a1"), su));
        assertNull(CallerStamps.acceptDeclared(A1, Caller.plugin(T, "rgA", "a1", "p1"), su));
        assertNull(CallerStamps.acceptDeclared(A1, Caller.api(T, "alice", List.of("user")), su));
        assertNull(CallerStamps.acceptDeclared(A1, Caller.api(T, "root", List.of("admin")), su));
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.controller(T, "rgA", "gc"), su), "another node's controller");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.controller(T, "rgB", "a1"), su), "the same agent name in another region");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.plugin(T, "rgA", "a2", "p1"), su), "another node's plugin");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.plugin("t2", "rgA", "a1", "p1"), su), "its plugin in another tenant");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.api("t2", "eve", List.of("admin")), su), "another tenant's admin");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.api(SU, "op", List.of("admin")), su), "a superuser tenant's admin");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.anonymous(), su), "anonymous never crosses a secured wire");
        assertNotNull(CallerStamps.acceptDeclared(A1, Caller.parse("agent:t1/rgA/a1", T, "user"), su), "agent: is the broker's to give");
        CrescoIdentity suNode = CrescoIdentity.of(SU, "rg", "gc", null);
        assertNull(CallerStamps.acceptDeclared(suNode, Caller.api(T, "alice", List.of("user")), su),
                "a superuser tenant's node sends for any tenant's users");
        assertNotNull(CallerStamps.acceptDeclared(null, Caller.controller(T, "rgA", "a1"), su));
    }

    @Test
    void theStampTravelsAsPropertiesAndTheBodyIsNeverRead() throws Exception {
        MsgEvent out = msg(MsgEvent.Type.EXEC, "x");
        Caller.plugin(T, "rgA", "a1", "p1").stamp(out);
        ActiveMQTextMessage jms = new ActiveMQTextMessage();
        CallerStamps.toJms(out, jms);
        assertEquals("plugin:t1/rgA/a1/p1", jms.getStringProperty("cresco_caller"));

        MsgEvent in = msg(MsgEvent.Type.EXEC, "x");
        Caller.controller(T, "rgA", "gc").stamp(in);                      // a forged body stamp
        Caller got = CallerStamps.fromJms(in, jms, true);
        assertEquals("plugin:t1/rgA/a1/p1", got.getPrincipal());
        assertEquals(got, Caller.fromMsgEvent(in), "the body carries the properties' caller, not its own");

        ActiveMQTextMessage none = new ActiveMQTextMessage();
        CallerStamps.toJms((Caller) null, none);
        assertEquals("", none.getStringProperty("cresco_caller"), "an unstamped message declares an empty caller");
        MsgEvent in2 = msg(MsgEvent.Type.EXEC, "x");
        Caller.controller(T, "rgA", "gc").stamp(in2);
        assertNull(CallerStamps.fromJms(in2, none, true));
        assertFalse(Caller.hasAny(in2), "a body stamp survived on a message the wire names nobody for");

        ActiveMQTextMessage anon = new ActiveMQTextMessage();
        CallerStamps.toJms(Caller.anonymous(), anon);
        assertNull(CallerStamps.fromJms(msg(MsgEvent.Type.EXEC, "x"), anon, true), "anonymous is refused on a secured fabric");
        MsgEvent in3 = msg(MsgEvent.Type.EXEC, "x");
        assertTrue(CallerStamps.fromJms(in3, none, false).isAnonymous(), "insecure mode: every caller is anonymous");
    }

    @Test
    void aFileListInABodyIsDropped() {
        MsgEvent m = msg(MsgEvent.Type.EXEC, "x");
        m.addFile("/etc/passwd");
        assertTrue(m.hasFiles());
        CallerStamps.dropFiles(m);
        assertFalse(m.hasFiles());
        assertTrue(m.getFileList().isEmpty());
    }

    // ------------------------------------------------------------------ the controller's action tables

    private static MsgEvent msg(MsgEvent.Type type, String action) {
        MsgEvent m = new MsgEvent(type, "rgA", "x", null, "rgA", "a1", null, true, false);
        if (action != null) m.setParam("action", action);
        return m;
    }

    private static boolean ok(ActionAuthorizer a, MsgEvent.Type type, String action, Caller c) {
        MsgEvent m = msg(type, action);
        if (c != null) c.stamp(m);
        return a.decide(m, T, Set.of(SU)).allowed;
    }

    static final Caller INFRA = Caller.controller(T, "rgA", "a2"), ADMIN = Caller.api(T, "root", List.of("admin")),
            USER = Caller.api(T, "bob", List.of("user")), PLUGIN = Caller.plugin(T, "rgA", "a1", "p1"),
            OTHER_INFRA = Caller.controller("t2", "rgZ", "z1"), OTHER_ADMIN = Caller.api("t2", "root", List.of("admin")),
            SU_ADMIN = Caller.api(SU, "op", List.of("admin"));

    private static void everyActionDeclared(Class<?> executor, ActionAuthorizer table) {
        CrescoAction[] actions = executor.getAnnotationsByType(CrescoAction.class);
        assertTrue(actions.length > 5, executor.getSimpleName() + " declares its actions");
        for (CrescoAction a : actions) {
            assertTrue(table.isDeclared(a.type(), a.name()), executor.getSimpleName() + " " + a.type() + " " + a.name()
                    + " has no entry in ControllerActionRoles");
        }
    }

    @Test
    void everyControllerActionHasRolesOnItsTier() {
        everyActionDeclared(AgentExecutor.class, ControllerActionRoles.agentTier(new ActionAuthorizer(null)));
        everyActionDeclared(RegionalExecutor.class, ControllerActionRoles.regionalTier());
        everyActionDeclared(GlobalExecutor.class, ControllerActionRoles.globalTier());
    }

    @Test
    void theAgentTierFailsClosedAndKeepsLogsAndFilesFromUsers() {
        ActionAuthorizer a = ControllerActionRoles.agentTier(new ActionAuthorizer(null));
        for (String act : List.of("setlogdp", "setloglevel", "getislogdp", "killjvm", "pluginadd", "stopcontroller", "pluginrepopull")) {
            assertFalse(ok(a, MsgEvent.Type.CONFIG, act, USER), act + " by a user");
            assertFalse(ok(a, MsgEvent.Type.CONFIG, act, PLUGIN), act + " by a plugin");
            assertFalse(ok(a, MsgEvent.Type.CONFIG, act, OTHER_ADMIN), act + " by another tenant's admin");
            assertTrue(ok(a, MsgEvent.Type.CONFIG, act, ADMIN), act + " by the tenant's admin");
            assertTrue(ok(a, MsgEvent.Type.CONFIG, act, SU_ADMIN), act + " by a superuser tenant's admin");
        }
        for (String act : List.of("getlog", "getfiledata", "getfileinfo", "getbroadcastdiscovery", "listagents", "getnetworkstate")) {
            assertFalse(ok(a, MsgEvent.Type.EXEC, act, USER), act + " by a user");
            assertTrue(ok(a, MsgEvent.Type.EXEC, act, INFRA), act + " by infra");
        }
        assertFalse(ok(a, MsgEvent.Type.EXEC, "indirectprobe", ADMIN), "only a controller makes this node probe another");
        assertTrue(ok(a, MsgEvent.Type.EXEC, "getmetricinventory", USER));
        assertFalse(ok(a, MsgEvent.Type.EXEC, "getmetricinventory", OTHER_ADMIN), "inventories stay in the tenant");
        assertTrue(ok(a, MsgEvent.Type.CONFIG, "pluginlist", USER));
        assertTrue(ok(a, MsgEvent.Type.CONFIG, "cepadd", PLUGIN));
        assertFalse(ok(a, MsgEvent.Type.CONFIG, "cepadd", USER));
        assertFalse(ok(a, MsgEvent.Type.EXEC, "nosuchaction", USER), "an undeclared controller action fails closed");
        assertTrue(ok(a, MsgEvent.Type.EXEC, "nosuchaction", ADMIN));
        assertTrue(ok(a, MsgEvent.Type.INFO, null, PLUGIN), "a plugin's print");
        assertTrue(ok(a, MsgEvent.Type.EXEC, "ping", OTHER_INFRA), "liveness across tenants");
        assertFalse(ok(a, MsgEvent.Type.EXEC, "ping", OTHER_ADMIN));
    }

    @Test
    void theRegionalAndGlobalTiersServeEveryTenantsInfraOnlyForFabricMechanics() {
        ActionAuthorizer r = ControllerActionRoles.regionalTier();
        assertTrue(ok(r, MsgEvent.Type.CONFIG, "agent_enable", OTHER_INFRA), "every tenant's agents register");
        assertTrue(ok(r, MsgEvent.Type.WATCHDOG, null, OTHER_INFRA));
        assertFalse(ok(r, MsgEvent.Type.WATCHDOG, null, PLUGIN), "a plugin is no keep-alive");
        assertFalse(ok(r, MsgEvent.Type.CONFIG, "agent_enable", USER));
        assertFalse(ok(r, MsgEvent.Type.CONFIG, "rpipelinesubmit", OTHER_INFRA), "placement stays in the tenant");
        assertFalse(ok(r, MsgEvent.Type.CONFIG, "rpipelinesubmit", USER));
        assertFalse(ok(r, MsgEvent.Type.CONFIG, "setlogdp", USER), "the agent tier's floor holds where the region hands it on");
        assertTrue(ok(r, MsgEvent.Type.KPI, null, PLUGIN));
        assertFalse(ok(r, MsgEvent.Type.KPI, null, USER));

        ActionAuthorizer g = ControllerActionRoles.globalTier();
        assertTrue(ok(g, MsgEvent.Type.CONFIG, "region_enable", OTHER_INFRA));
        assertFalse(ok(g, MsgEvent.Type.CONFIG, "region_enable", ADMIN), "only a controller registers a region");
        assertFalse(ok(g, MsgEvent.Type.CONFIG, "gpipelinesubmit", USER));
        assertTrue(ok(g, MsgEvent.Type.CONFIG, "gpipelinesubmit", ADMIN));
        assertFalse(ok(g, MsgEvent.Type.CONFIG, "gpipelinesubmit", OTHER_INFRA), "scheduling stays in the tenant");
        assertTrue(ok(g, MsgEvent.Type.EXEC, "listagents", USER), "the global's own tenant reads the registry");
        assertFalse(ok(g, MsgEvent.Type.EXEC, "listagents", OTHER_ADMIN), "another tenant's admin does not");
        assertTrue(ok(g, MsgEvent.Type.EXEC, "listagents", SU_ADMIN));
        assertFalse(ok(g, MsgEvent.Type.EXEC, "getnetworkstate", USER));
        assertFalse(ok(g, MsgEvent.Type.EXEC, "nosuchaction", USER));
    }

    // ------------------------------------------------------------------ registry binding

    private static MsgEvent registry(MsgEvent.Type type, String action, Map<String, String> params) {
        MsgEvent m = msg(type, action);
        params.forEach(m::setParam);
        return m;
    }

    @Test
    void aControllerUpdatesOnlyItsOwnRegistryRecords() {
        Caller a1 = Caller.controller(T, "rgA", "a1");
        assertNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("agent_watchdog_update", "a1")), a1, false, null));
        assertNotNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("agent_watchdog_update", "a2")), a1, false, null));
        assertNotNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("region_watchdog_update", "rgB")), a1, false, null));
        assertNotNull(RegistryBinding.refusal(registry(MsgEvent.Type.CONFIG, "agent_disable",
                Map.of("unregister_region_id", "rgA", "unregister_agent_id", "a2")), a1, false, null), "unregistering a peer");
        MsgEvent enable = registry(MsgEvent.Type.CONFIG, "agent_enable", Map.of());
        enable.setCompressedParam("agentconfigs", "{\"rgA\":[{\"agent_id\":\"a1\",\"status_code\":\"10\"}]}");
        assertNull(RegistryBinding.refusal(enable, a1, false, null));
        enable.setCompressedParam("agentconfigs", "{\"rgA\":[{\"agent_id\":\"a2\",\"status_code\":\"10\"}]}");
        assertNotNull(RegistryBinding.refusal(enable, a1, false, null), "registering another agent");
        enable.setCompressedParam("agentconfigs", "{\"rgB\":[{\"agent_id\":\"a1\"}]}");
        assertNotNull(RegistryBinding.refusal(enable, a1, false, null), "registering in another region");
        // at the global: a region reports itself and its agents
        Caller rgA = Caller.controller(T, "rgA", "rc");
        MsgEvent region = registry(MsgEvent.Type.CONFIG, "region_enable", Map.of("region_watchdog_update", "rgA"));
        region.setCompressedParam("agentconfigs", "{\"rgA\":[{\"agent_id\":\"a1\"},{\"agent_id\":\"a7\"}]}");
        region.setCompressedParam("pluginconfigs", "{\"a1\":[{\"plugin_id\":\"p1\"}]}");
        // review F3: at the global only the region's own controller (the one that joined it) speaks for the region
        java.util.function.Predicate<Caller> joined = c -> c.equals(rgA);
        assertNull(RegistryBinding.refusal(region, rgA, true, agent -> "a1".equals(agent) ? "rgA" : null, joined));
        assertNotNull(RegistryBinding.refusal(region, rgA, true, agent -> "a1".equals(agent) ? "rgA" : null),
                "without knowing who joined, no controller speaks for a region");
        assertNotNull(RegistryBinding.refusal(region, Caller.controller(T, "rgA", "a1"), true, agent -> "rgA", joined),
                "an agent of the region is not its controller");
        assertNotNull(RegistryBinding.refusal(region, rgA, true, agent -> "rgB", joined), "plugin records of another region's agent");
        region.setCompressedParam("regionconfigs", "{\"x\":[{\"region_id\":\"rgB\"}]}");
        assertNotNull(RegistryBinding.refusal(region, rgA, true, null, joined), "another region's record");
        assertNotNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("agent_watchdog_update", "a1")),
                Caller.plugin(T, "rgA", "a1", "p"), false, null), "a plugin is no controller");
        assertNotNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("plugin_watchdog_update", "p9")), a1, false, null));
        assertNull(RegistryBinding.refusal(registry(MsgEvent.Type.WATCHDOG, null, Map.of("agent_watchdog_update", "a9")),
                Caller.api(T, "root", List.of("admin")), false, null), "an admin (in scope) is not bound to one node");
        assertTrue(RegistryBinding.applies(registry(MsgEvent.Type.WATCHDOG, null, Map.of())));
        assertFalse(RegistryBinding.applies(msg(MsgEvent.Type.EXEC, "agent_enable")), "EXEC agent_enable is no registry update");
    }

    // ------------------------------------------------------------------ stdin, relays

    @Test
    void onlyTheExecutorSeesStdin() {
        assertNotNull(StdinGuard.refusal("type LIKE 'input-%'"));
        assertNotNull(StdinGuard.refusal("type = 'input-0a1b'"));
        assertNotNull(StdinGuard.refusal("type IN ('event','input')"));
        assertNull(StdinGuard.refusal("type = 'inputs'"));
        assertNull(StdinGuard.refusal("region_id = 'rgA'"));
        assertNull(StdinGuard.refusal("stream_name = 's' AND " + StdinGuard.NO_STDIN), "a selector that excludes stdin");
        assertNull(StdinGuard.refusal("type NOT IN ('input', 'input-1')"));
        assertNotNull(StdinGuard.refusal("type <> 'event' OR type = 'input-7'"), "naming it besides an exclusion");
        assertEquals(StdinGuard.NO_STDIN, StdinGuard.narrow(null));
        assertEquals("(region_id = 'rgA') AND " + StdinGuard.NO_STDIN, StdinGuard.narrow("region_id = 'rgA'"));
        assertTrue(StdinGuard.isDataplaneTopic(new org.apache.activemq.command.ActiveMQTopic("T.t1.global.event")));
        assertTrue(StdinGuard.isDataplaneTopic(new org.apache.activemq.command.ActiveMQTopic("global.event.3")));
        assertFalse(StdinGuard.isDataplaneTopic(new org.apache.activemq.command.ActiveMQQueue("T.t1.rgA_a1")));
    }

    @Test
    void aRelayNeverCarriesAMessageIntoAnotherTenantsNamespace() {
        MsgEvent m = msg(MsgEvent.Type.EXEC, "x");
        m.setParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM, "t1");
        Caller.plugin("t2", "rgA", "a9", "p").stamp(m);
        assertNotNull(MsgRouter.tenantRefusal(m, Set.of(SU)));
        Caller.plugin(T, "rgA", "a9", "p").stamp(m);
        assertNull(MsgRouter.tenantRefusal(m, Set.of(SU)));
        Caller.api(SU, "op", List.of("admin")).stamp(m);
        assertNull(MsgRouter.tenantRefusal(m, Set.of(SU)), "a superuser tenant's caller sends into any tenant");
        Caller.strip(m);
        assertNull(MsgRouter.tenantRefusal(m, Set.of(SU)), "an unstamped message is refused at its destination, not here");
    }

    @Test
    void aLoggedMessageShowsParameterNamesAndSizesNeverValues() {
        MsgEvent m = msg(MsgEvent.Type.CONFIG, "nosuch");
        m.setParam("password", "hunter2-secret");
        m.setParam("jardata", "QUJDREVGRw==");
        String line = ParamLog.describe(m);
        assertFalse(line.contains("hunter2-secret") || line.contains("QUJDREVGRw=="), line);
        assertTrue(line.contains("password:14") && line.contains("action=nosuch"), line);
        MsgEvent odd = msg(MsgEvent.Type.EXEC, "x' OR secret");
        assertFalse(ParamLog.describe(odd).contains("secret"), "an action that is not a plain token is logged by its size");
        assertTrue(ParamLog.allow("test-key"));
        assertFalse(ParamLog.allow("test-key"), "rate limited");
    }
}
