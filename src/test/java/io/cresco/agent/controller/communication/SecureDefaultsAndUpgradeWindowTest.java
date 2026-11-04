package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Config;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Phase 4: secure by default, the refuse-to-start interlock, the time-bounded upgrade window (M6a), dotted tenants (M6b). */
class SecureDefaultsAndUpgradeWindowTest {

    private static Config cfg(Object... kv) throws Exception {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return TestAgentService.plugin(Files.createTempDirectory("p4").toString(), m).getConfig();
    }

    private static String day(int fromToday) {
        return LocalDate.now(ZoneOffset.UTC).plusDays(fromToday).toString();
    }

    @Test
    void securityIsOnByDefault() throws Exception {
        Config c = cfg();
        assertTrue(SecuritySettings.brokerSecurity(c));
        assertTrue(SecuritySettings.requireClientAuth(c));
        assertTrue(SecuritySettings.tenantNamespacing(c));
        assertTrue(SecuritySettings.startupRefusals(c).isEmpty(), "the defaults start");
        Config dev = cfg("broker_security_enabled", "false");
        assertFalse(SecuritySettings.brokerSecurity(dev));
        assertFalse(SecuritySettings.tenantNamespacing(dev));
        assertTrue(SecuritySettings.startupRefusals(dev).isEmpty(), "explicit insecure development mode starts (with its WARNING)");
    }

    @Test
    void inconsistentSecuritySettingsRefuseToStart() throws Exception {
        assertEquals(1, SecuritySettings.startupRefusals(cfg("broker_require_client_auth", "false")).size());
        assertEquals(1, SecuritySettings.startupRefusals(cfg("tenant_namespacing", "false")).size());
        assertEquals(1, SecuritySettings.startupRefusals(cfg("tenant_id", "acme.prod")).size(), "a dotted tenant without its migration");
        assertTrue(SecuritySettings.startupRefusals(cfg("tenant_id", "acme.prod", "tenant_id_dot_replacement", "-")).isEmpty());
        assertEquals("acme-prod", SecuritySettings.tenant(cfg("tenant_id", "acme.prod", "tenant_id_dot_replacement", "-")));
        assertFalse(SecuritySettings.startupRefusals(cfg("tenant_id", "bad tenant")).isEmpty());
    }

    @Test
    void theUpgradeWindowIsExplicitAndTimeBounded() throws Exception {
        long now = System.currentTimeMillis();
        assertFalse(SecuritySettings.compatOpen(cfg(), now));
        assertFalse(SecuritySettings.compatOpen(cfg("discovery_auth", "compat"), now), "no end: closed");
        assertFalse(SecuritySettings.compatProblems(cfg("discovery_auth", "compat"), now).isEmpty());
        assertTrue(SecuritySettings.compatOpen(cfg("discovery_auth", "compat", "discovery_compat_until", day(7)), now));
        assertFalse(SecuritySettings.compatOpen(cfg("discovery_auth", "compat", "discovery_compat_until", day(120)), now), "over 90 days");
        assertFalse(SecuritySettings.compatOpen(cfg("discovery_auth", "compat", "discovery_compat_until", day(-1)), now), "expired");
        assertFalse(SecuritySettings.compatOpen(cfg("discovery_auth", "compat", "discovery_compat_until", "soon"), now));
        assertFalse(SecuritySettings.compatOpen(cfg("discovery_compat_until", day(7)), now), "an end without compat opens nothing");
        assertEquals(DiscoveryAuth.Mode.V2, DiscoveryAuth.Mode.of(cfg("discovery_auth", "compat")), "compat without a window is v2");
        assertEquals(DiscoveryAuth.Mode.COMPAT, DiscoveryAuth.Mode.of(cfg("discovery_auth", "compat", "discovery_compat_until", day(3))));
        assertTrue(SecuritySettings.bootWarnings(cfg("discovery_auth", "compat", "discovery_compat_until", day(3)), now).get(0)
                .startsWith("UPGRADE WINDOW OPEN"));
    }

    private static MsgEvent enable(String agent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", agent, null, "rgA", "rc", null, true, false);
        m.setParam("action", "agent_enable");
        m.setCompressedParam("agentconfigs", "{\"rgA\":[{\"agent_id\":\"" + agent + "\",\"status_code\":\"10\"}]}");
        return m;
    }

    @Test
    void insideTheWindowALegacyNodeKeepsOnlyItsOwnMembership() {
        Caller legacy = Caller.parse("agent:t1/rgA/a5", "t1", "user");
        assertNotNull(RegistryBinding.refusal(enable("a5"), legacy, false, null, null, false), "outside the window: refused");
        assertNull(RegistryBinding.refusal(enable("a5"), legacy, false, null, null, true), "inside: its own registration");
        assertNotNull(RegistryBinding.refusal(enable("a6"), legacy, false, null, null, true), "never another node's");
        assertNotNull(RegistryBinding.refusal(enable("a5"), legacy, true, a -> "rgA", c -> true, true), "never at the global");
        Caller plugin = Caller.plugin("t1", "rgA", "a5", "p");
        assertNotNull(RegistryBinding.refusal(enable("a5"), plugin, false, null, null, true), "a plugin is still no controller");
        MsgEvent wd = new MsgEvent(MsgEvent.Type.WATCHDOG, "rgA", "a5", null, "rgA", "rc", null, true, false);
        legacy.stamp(wd);
        assertTrue(io.cresco.agent.controller.regionalcontroller.RegionalExecutorWindowAccess.legacyMembership(wd));
        MsgEvent other = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a5", null, "rgA", "rc", null, true, false);
        other.setParam("action", "listagents");
        legacy.stamp(other);
        assertFalse(io.cresco.agent.controller.regionalcontroller.RegionalExecutorWindowAccess.legacyMembership(other));
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a5", null, "rgA", "rc", null, true, false);
        ping.setParam("action", "ping");
        plugin.stamp(ping);
        assertFalse(io.cresco.agent.controller.regionalcontroller.RegionalExecutorWindowAccess.legacyMembership(ping), "agent: stamps only");
    }
}
