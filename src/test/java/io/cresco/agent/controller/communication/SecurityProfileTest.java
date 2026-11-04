package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.Config;
import io.cresco.library.security.SecurityProfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** security_profile: production refuses to start, naming each problem; standard is the default; insecure is explicit. */
class SecurityProfileTest {

    private static Config cfg(Map<String, Object> m) throws Exception {
        return TestAgentService.plugin(Files.createTempDirectory("prof").toString(), new HashMap<>(m)).getConfig();
    }

    /** A complete production node: provisioned identity files present, nothing disallowed. */
    private static Map<String, Object> production() throws Exception {
        Path d = Files.createTempDirectory("prod-files");
        Map<String, Object> m = new HashMap<>();
        m.put("security_profile", "production");
        m.put("discovery_secret_agent", "from-a-file");      // SecretSources resolves <name>_file into the config
        for (String k : new String[]{"identity_keystore", "identity_keystore_password_file", "trust_anchors", "plugin_signing_truststore"}) {
            Path f = d.resolve(k);
            Files.writeString(f, "x");
            m.put(k, f.toString());
        }
        return m;
    }

    @Test
    void standardIsTheDefaultAndInsecureIsExplicit() throws Exception {
        assertEquals(SecurityProfile.STANDARD, SecuritySettings.profile(cfg(Map.of())));
        assertEquals(SecurityProfile.INSECURE, SecuritySettings.profile(cfg(Map.of("broker_security_enabled", "false"))));
        assertEquals(SecurityProfile.PRODUCTION, SecuritySettings.profile(cfg(Map.of("security_profile", "production"))));
        assertTrue(SecuritySettings.startupRefusals(cfg(Map.of())).isEmpty());
        assertEquals(1, SecuritySettings.startupRefusals(cfg(Map.of("security_profile", "prod"))).size(), "an unknown profile");
    }

    @Test
    void aCompleteProductionNodeStarts() throws Exception {
        assertEquals(List.of(), SecuritySettings.startupRefusals(cfg(production())));
    }

    @Test
    void productionNamesEveryMissingRequirement() throws Exception {
        List<String> why = SecuritySettings.startupRefusals(cfg(Map.of("security_profile", "production")));
        for (String k : new String[]{"identity_keystore ", "identity_keystore_password_file ", "trust_anchors ", "plugin_signing_truststore "}) {
            assertTrue(why.stream().anyMatch(w -> w.contains(k + "is required")), "not named: " + k + " in " + why);
        }
        Map<String, Object> m = production();
        m.put("trust_anchors", "/nonexistent/anchors.pem");
        assertTrue(SecuritySettings.startupRefusals(cfg(m)).get(0).contains("trust_anchors=/nonexistent/anchors.pem is not a readable file"));
    }

    @Test
    void productionDiscoveryNeedsItsAdmissionSecret() throws Exception {
        Map<String, Object> m = production();
        m.remove("discovery_secret_agent");
        assertTrue(SecuritySettings.startupRefusals(cfg(m)).stream().anyMatch(w -> w.contains("no discovery secret")));
    }

    @Test
    void productionNamesEveryDisallowedSetting() throws Exception {
        String[][] bad = {
                {"broker_security_enabled", "false"}, {"plugin_signing_required", "false"}, {"msg_signing_required", "false"},
                {"peer_identity_mode", "warn"}, {"discovery_auth", "compat"}, {"discovery_compat_until", "2030-01-01"},
                {"security_regional_ca", "true"}, {"broker_allow_unverified_bridges", "true"}, {"peer_identity_rebind", "rgA/rc"},
                {"public_key_trusted_fingerprints", "ab"}, {"cresco_service_key", "k"}, {"broker_security_secret", "s"},
                {"plugin_signing_truststore_password", "p"}, {"revocation_file", "/nonexistent/crl"},
        };
        for (String[] kv : bad) {
            Map<String, Object> m = production();
            m.put(kv[0], kv[1]);
            List<String> why = SecuritySettings.startupRefusals(cfg(m));
            assertFalse(why.isEmpty(), "production accepted " + kv[0] + "=" + kv[1]);
            assertTrue(why.stream().anyMatch(w -> w.contains(kv[0])), kv[0] + " not named in " + why);
        }
        System.setProperty("discovery_secret_agent", "inline");
        try {
            assertTrue(SecuritySettings.startupRefusals(cfg(production())).stream().anyMatch(w -> w.contains("-Ddiscovery_secret_agent")));
        } finally {
            System.clearProperty("discovery_secret_agent");
        }
    }

    /** Identity round-3 review P1 (ReviewIdentity3Test, review/identity3-20261104 acdeebb): production forces security on. */
    @Test
    void theProductionProfileTurnsBrokerSecurityOn() throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("security_profile", "production");
        m.put("identity_keystore", "/nonexistent/node.p12");
        Config c = cfg(m);
        assertTrue(SecuritySettings.brokerSecurity(c) && SecuritySettings.requireClientAuth(c) && SecuritySettings.pluginSigningRequired(c),
                "security_profile=production leaves broker security, client certificates and plugin signing OFF unless set separately");
        // forced on even when set off explicitly -- and the node refuses to start, naming the setting
        Map<String, Object> off = production();
        off.put("broker_security_enabled", "false");
        off.put("tenant_namespacing", "false");
        Config o = cfg(off);
        assertTrue(SecuritySettings.brokerSecurity(o) && SecuritySettings.tenantNamespacing(o));
        List<String> why = SecuritySettings.startupRefusals(o);
        assertTrue(why.stream().anyMatch(w -> w.contains("broker_security_enabled=false")), String.valueOf(why));
        assertTrue(why.stream().anyMatch(w -> w.contains("tenant_namespacing=false")), String.valueOf(why));
    }
}
