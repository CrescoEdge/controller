package io.cresco.agent.core;

import io.cresco.library.plugin.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** OUT-03b: agent secret params from an owner-only {@code <param>_file} or {@code CRESCO_<PARAM>}, ahead of -D. */
class AgentConfigSecretSourceTest {

    private Path tmp;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("secret-source");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private Path secretFile(String name, String content, String mode) throws IOException {
        Path f = tmp.resolve(name);
        Files.write(f, content.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(f, PosixFilePermissions.fromString(mode));
        return f;
    }

    private static Map<String, String> env(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    private static void assertNoValue(List<String> notes, String... values) {
        for (String n : notes) for (String v : values) assertFalse(n.contains(v), "a note leaked a secret: " + n);
    }

    @Test
    void aFileWinsOverTheEnvironmentAndTheCommandLineWhichIsCleared() throws Exception {
        Path f = secretFile("broker.secret", "from-file-s3cr3t\n", "rw-------");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_secret", "from-ini");
        Properties sys = new Properties();
        sys.setProperty("broker_security_secret", "from-cmdline");
        sys.setProperty("broker_security_secret_file", f.toString());
        List<String> notes = SecretSources.resolve(cfg, env("CRESCO_BROKER_SECURITY_SECRET", "from-env"), sys);
        assertEquals("from-file-s3cr3t", cfg.get("broker_security_secret"));
        assertNull(sys.getProperty("broker_security_secret"), "the -D copy is cleared so the library Config cannot read it first");
        assertTrue(notes.stream().anyMatch(n -> n.startsWith("WARN: -Dbroker_security_secret ignored")), notes.toString());
        assertNoValue(notes, "from-file-s3cr3t", "from-cmdline", "from-env", "from-ini");
    }

    @Test
    void theUpperCaseEnvironmentVariableWinsOverTheCommandLine() {
        Map<String, Object> cfg = new HashMap<>();
        Properties sys = new Properties();
        sys.setProperty("keystorepwd", "from-cmdline");
        List<String> notes = SecretSources.resolve(cfg, env("CRESCO_KEYSTOREPWD", "from-env"), sys);
        assertEquals("from-env", cfg.get("keystorepwd"));
        assertNull(sys.getProperty("keystorepwd"));
        assertNoValue(notes, "from-env", "from-cmdline");
    }

    @Test
    void theLibrarysExactCaseEnvironmentFormIsAccepted() {
        Map<String, Object> cfg = new HashMap<>();
        SecretSources.resolve(cfg, env("CRESCO_db_password", "exact-case"), new Properties());
        assertEquals("exact-case", cfg.get("db_password"));
    }

    @Test
    void fileSourcesFromTheEnvironmentAndAgentIni() throws Exception {
        Path f1 = secretFile("disc", "discovery-region-secret", "r--------");
        Path f2 = secretFile("trust", "trust-store-password", "rw-------");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("truststorepwd_file", f2.toString());
        SecretSources.resolve(cfg, env("CRESCO_DISCOVERY_SECRET_REGION_FILE", f1.toString()), new Properties());
        assertEquals("discovery-region-secret", cfg.get("discovery_secret_region"));
        assertEquals("trust-store-password", cfg.get("truststorepwd"));
    }

    @Test
    void anyUnlistedSecretLookingParamCanUseAFileOrTheEnvironment() throws Exception {
        Path f = secretFile("svc", "service-key-value", "rw-------");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("cresco_service_key_file", f.toString());
        SecretSources.resolve(cfg, env("CRESCO_GFS_SECRET", "gfs-value"), new Properties());
        assertEquals("service-key-value", cfg.get("cresco_service_key"));
        assertEquals("gfs-value", cfg.get("gfs_secret"));
    }

    @Test
    void nonSecretParamsAndDbKeyFileAreLeftAlone() throws Exception {
        Path key = secretFile("db.key", "derby boot password 1234", "rw-------");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("db_key_file", key.toString());
        cfg.put("plugin_config_file", "conf/plugins.ini");
        Map<String, Object> before = new HashMap<>(cfg);
        SecretSources.resolve(cfg, env("CRESCO_PLATFORM", "linux", "CRESCO_REGION_PING_INTERVAL", "5000"), new Properties());
        assertEquals(before, cfg, "db_key_file is DBEngine's own key file, not a source for a 'db_key' param");
    }

    @Test
    void aCommandLineOnlySecretStillWorksWithAWarning() {
        Map<String, Object> cfg = new HashMap<>();
        Properties sys = new Properties();
        sys.setProperty("db_password", "cmdline-only");
        List<String> notes = SecretSources.resolve(cfg, env(), sys);
        assertEquals("cmdline-only", sys.getProperty("db_password"), "unchanged: -D keeps working");
        assertFalse(cfg.containsKey("db_password"));
        assertTrue(notes.stream().anyMatch(n -> n.contains("db_password is on the JVM command line")), notes.toString());
        assertNoValue(notes, "cmdline-only");
    }

    @Test
    void noSourcesNoChange() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("db_password", "from-ini");
        Map<String, Object> before = new HashMap<>(cfg);
        assertTrue(SecretSources.resolve(cfg, env(), new Properties()).isEmpty());
        assertEquals(before, cfg);
    }

    @Test
    void anUnsafeOrMissingFileRefusesToStart() throws Exception {
        Path loose = secretFile("loose", "a-secret-value", "rw-r--r--");
        Properties sys = new Properties();
        sys.setProperty("db_password_file", loose.toString());
        SecretSources.SecretSourceException e = assertThrows(SecretSources.SecretSourceException.class,
                () -> SecretSources.resolve(new HashMap<>(), env(), sys));
        assertTrue(e.getMessage().contains("0600"), e.getMessage());
        assertFalse(e.getMessage().contains("a-secret-value"));
        assertThrows(SecretSources.SecretSourceException.class, () -> SecretSources.resolve(new HashMap<>(),
                env("CRESCO_DB_PASSWORD_FILE", tmp.resolve("missing").toString()), new Properties()));
        Path real = secretFile("real", "a-secret-value", "rw-------");
        Path link = Files.createSymbolicLink(tmp.resolve("link"), real);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("db_password_file", link.toString());
        assertThrows(SecretSources.SecretSourceException.class, () -> SecretSources.resolve(cfg, env(), new Properties()));
    }

    @Test
    void endToEndTheLibraryConfigSeesTheFileValueNotTheCommandLine() throws Exception {
        // through the real system properties and the library Config the controller reads params with
        String p = "broker_security_secret";
        String old = System.getProperty(p);
        try {
            System.setProperty(p, "from-cmdline");
            Path f = secretFile("broker", "from-file-e2e", "rw-------");
            Map<String, Object> cfg = new HashMap<>();
            cfg.put(p + "_file", f.toString());
            SecretSources.resolve(cfg, env(), System.getProperties());
            assertNull(System.getProperty(p));
            assertEquals("from-file-e2e", new Config(cfg).getStringParam(p));
        } finally {
            if (old == null) System.clearProperty(p); else System.setProperty(p, old);
        }
    }
}
