package io.cresco.agent.db;

import io.cresco.agent.test.TestAgentService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** OUT-03a: controller Derby encryption at rest from an owner-only key file (db_key_file). */
class DerbyAtRestTest {

    private static final String MARKER = "CRESCO-PLAINTEXT-MARKER-7f3a9c";
    private static final String PASSWORD = "correct horse battery staple 42";

    private Path tmp;

    @BeforeEach
    void setUp() throws Exception {
        tmp = Files.createTempDirectory("derby-at-rest");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private Path keyFile(String name, String content, String mode) throws IOException {
        Path f = tmp.resolve(name);
        Files.write(f, content.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(f, PosixFilePermissions.fromString(mode));
        return f;
    }

    private static void shutdownDb(Path db) {
        try {
            DriverManager.getConnection("jdbc:derby:" + db + ";shutdown=true").close();
            fail("Derby reports a database shutdown with an exception");
        } catch (SQLException e) {
            assertEquals("08006", e.getSQLState(), e.getMessage());
        }
    }

    private static boolean filesContain(Path dir, String marker) throws IOException {
        byte[] m = marker.getBytes(StandardCharsets.UTF_8);
        try (Stream<Path> w = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) w.filter(Files::isRegularFile)::iterator) {
                byte[] b = Files.readAllBytes(p);
                outer:
                for (int i = 0; i + m.length <= b.length; i++) {
                    for (int j = 0; j < m.length; j++) if (b[i + j] != m[j]) continue outer;
                    return true;
                }
            }
        }
        return false;
    }

    private static Properties keyProps(DerbyAtRest.Secret s) {
        Properties p = new Properties();
        p.setProperty(s.attribute, s.value);
        return p;
    }

    private static String readMarker(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select v from t")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // ---- key file checks ------------------------------------------------------------------

    @Test
    void anOwnerOnlyKeyFileIsAccepted() throws Exception {
        DerbyAtRest.Secret pw = DerbyAtRest.readKeyFile(keyFile("pw", PASSWORD + "\n", "rw-------"));
        assertEquals("bootPassword", pw.attribute());
        assertEquals(PASSWORD, pw.value, "one trailing newline is stripped");
        assertFalse(pw.toString().contains(PASSWORD), "never printable");
        DerbyAtRest.Secret ro = DerbyAtRest.readKeyFile(keyFile("ro", PASSWORD, "r--------"));
        assertEquals("bootPassword", ro.attribute());
        String hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        assertEquals("encryptionKey", DerbyAtRest.readKeyFile(keyFile("hex", hex, "rw-------")).attribute());
    }

    @Test
    void aKeyFileFailingAnyCheckIsRefused() throws Exception {
        for (String mode : new String[]{"rw-r--r--", "rw-r-----", "rw----r--", "rw-rw----", "rwx------", "-w-------"}) {
            Path f = keyFile("k-" + mode.replace('-', '_'), PASSWORD, mode);
            DerbyAtRest.AtRestException e = assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(f), mode);
            assertTrue(e.getMessage().contains("0600"), e.getMessage());
        }
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(tmp.resolve("missing")));
        Path real = keyFile("real", PASSWORD, "rw-------");
        Path link = Files.createSymbolicLink(tmp.resolve("link"), real);
        assertTrue(assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(link)).getMessage().contains("symbolic link"));
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(tmp), "a directory");
        Path shortPw = keyFile("short", "tooshort", "rw-------");
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(shortPw));
        Path twoLines = keyFile("two", PASSWORD + "\n" + PASSWORD, "rw-------");
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(twoLines));

        Path shared = Files.createDirectory(tmp.resolve("shared"));
        Files.setPosixFilePermissions(shared, PosixFilePermissions.fromString("rwxrwx---"));
        Path inShared = Files.write(shared.resolve("k"), PASSWORD.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(inShared, PosixFilePermissions.fromString("rw-------"));
        assertTrue(assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.readKeyFile(inShared))
                .getMessage().contains("writable"));
    }

    @Test
    void onlyOnDiskDerbyUrlsAreAccepted() {
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.databaseDirectory("jdbc:mysql://h/db"));
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.databaseDirectory("jdbc:derby:memory:x;create=true"));
        assertEquals(tmp.resolve("db").toAbsolutePath(), DerbyAtRest.databaseDirectory("jdbc:derby:" + tmp.resolve("db") + ";create=true"));
        assertEquals(tmp.resolve("db").toAbsolutePath(), DerbyAtRest.databaseDirectory("jdbc:derby:directory:" + tmp.resolve("db")));
    }

    // ---- Derby ------------------------------------------------------------------------------

    @Test
    void aFreshDatabaseIsCreatedEncrypted() throws Exception {
        Path db = tmp.resolve("fresh");
        String url = "jdbc:derby:" + db + ";create=true";
        DerbyAtRest.Secret s = DerbyAtRest.readKeyFile(keyFile("k", PASSWORD, "rw-------"));
        Properties pool = DerbyAtRest.boot(url, s, new Properties(), null);
        assertTrue(DerbyAtRest.isEncrypted(db));
        assertEquals(PASSWORD, pool.getProperty("bootPassword"));
        assertNull(pool.getProperty("dataEncryption"), "later connections only carry the key");
        try (Connection c = DriverManager.getConnection(url, pool); Statement st = c.createStatement()) {
            st.executeUpdate("create table t (v varchar(64))");
            st.executeUpdate("insert into t values ('" + MARKER + "')");
        }
        shutdownDb(db);
        assertFalse(filesContain(db.resolve("seg0"), MARKER), "no plaintext in the data files");
        assertNotNull(assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:" + db)));
        DerbyAtRest.Secret wrong = DerbyAtRest.readKeyFile(keyFile("wrong", "a different password 123", "rw-------"));
        assertThrows(DerbyAtRest.AtRestException.class, () -> DerbyAtRest.boot(url, wrong, new Properties(), null));
        Properties again = DerbyAtRest.boot(url, s, new Properties(), null);
        try (Connection c = DriverManager.getConnection(url, again)) {
            assertEquals(MARKER, readMarker(c));
        }
        shutdownDb(db);
    }

    @Test
    void aFreshDatabaseWithARawKeyIsCreatedEncrypted() throws Exception {
        Path db = tmp.resolve("rawkey");
        String url = "jdbc:derby:" + db + ";create=true";
        DerbyAtRest.Secret s = DerbyAtRest.readKeyFile(keyFile("k",
                "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90", "r--------"));
        Properties pool = DerbyAtRest.boot(url, s, new Properties(), null);
        assertTrue(DerbyAtRest.isEncrypted(db));
        try (Connection c = DriverManager.getConnection(url, pool); Statement st = c.createStatement()) {
            st.executeUpdate("create table t (v varchar(64))");
        }
        shutdownDb(db);
        assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:" + db));
    }

    @Test
    void anExistingPlaintextDatabaseIsEncryptedInPlaceAtBoot() throws Exception {
        Path db = tmp.resolve("existing");
        String url = "jdbc:derby:" + db + ";create=true";
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.executeUpdate("create table t (v varchar(64))");
            st.executeUpdate("insert into t values ('" + MARKER + "')");
        }
        shutdownDb(db);
        assertFalse(DerbyAtRest.isEncrypted(db));
        assertTrue(filesContain(db.resolve("seg0"), MARKER), "control: the plaintext database shows the marker");

        DerbyAtRest.Secret s = DerbyAtRest.readKeyFile(keyFile("k", PASSWORD, "rw-------"));
        Properties pool = DerbyAtRest.boot(url, s, new Properties(), null);
        assertTrue(DerbyAtRest.isEncrypted(db));
        try (Connection c = DriverManager.getConnection(url, pool)) {
            assertEquals(MARKER, readMarker(c), "data survives the in-place encryption");
        }
        shutdownDb(db);
        assertFalse(filesContain(db.resolve("seg0"), MARKER), "no plaintext left in the data files");
        assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:" + db));
        try (Connection c = DriverManager.getConnection("jdbc:derby:" + db, keyProps(s))) {
            assertEquals(MARKER, readMarker(c));
        }
        shutdownDb(db);
    }

    // ---- DBEngine ---------------------------------------------------------------------------

    /** DBEngine.shutdown() stops the whole Derby engine, which deregisters the driver. */
    private static void reloadDriver() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
    }

    private static Map<String, Object> cfg(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    @Test
    void dbEngineIsUnchangedWithoutAKeyFileAndEncryptsWhenOneIsAdded() throws Exception {
        String data = tmp.resolve("agent").toString();
        Path db = tmp.resolve("agent/derbydb-home/cresco-controller-db");

        DBEngine plain = new DBEngine(TestAgentService.plugin(data, cfg()));
        assertTrue(DerbyAtRest.exists(db));
        assertFalse(DerbyAtRest.isEncrypted(db), "no db_key_file: plaintext, as before");
        assertTrue(plain.shutdown());
        reloadDriver();

        Path key = keyFile("controller.key", PASSWORD, "rw-------");
        DBEngine enc = new DBEngine(TestAgentService.plugin(data, cfg("db_key_file", key.toString())));
        assertTrue(DerbyAtRest.isEncrypted(db), "the existing controller database was encrypted at boot");
        assertTrue(enc.shutdown());
        reloadDriver();

        assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:" + db));
        try (Connection c = DriverManager.getConnection("jdbc:derby:" + db, keyProps(DerbyAtRest.readKeyFile(key)));
             Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select tenantname from tenantnode")) {
            assertTrue(rs.next());
            assertEquals("default tenant", rs.getString(1), "the schema and rows made before encryption are intact");
        }
        shutdownDb(db);
    }

    @Test
    void dbEngineRefusesToStartOnABadKeyFile() throws Exception {
        String data = tmp.resolve("agent2").toString();
        Path loose = keyFile("loose.key", PASSWORD, "rw-r--r--");
        assertThrows(DerbyAtRest.AtRestException.class, () -> new DBEngine(TestAgentService.plugin(data, cfg("db_key_file", loose.toString()))));
        assertFalse(Files.exists(tmp.resolve("agent2/derbydb-home/cresco-controller-db")), "nothing was created in plaintext");
        assertThrows(DerbyAtRest.AtRestException.class,
                () -> new DBEngine(TestAgentService.plugin(data, cfg("db_key_file", tmp.resolve("nope.key").toString()))));
    }

    @Test
    void dbEngineFreshWithAKeyFileCreatesAnEncryptedDatabaseWithItsSchema() throws Exception {
        String data = tmp.resolve("agent3").toString();
        Path db = tmp.resolve("agent3/derbydb-home/cresco-controller-db");
        Path key = keyFile("controller.key", PASSWORD, "rw-------");
        DBEngine enc = new DBEngine(TestAgentService.plugin(data, cfg("db_key_file", key.toString())));
        assertTrue(DerbyAtRest.isEncrypted(db));
        assertTrue(enc.shutdown());
        reloadDriver();
        try (Connection c = DriverManager.getConnection("jdbc:derby:" + db, keyProps(DerbyAtRest.readKeyFile(key)));
             Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select tenantname from tenantnode")) {
            assertTrue(rs.next(), "initDB ran on the fresh encrypted database");
        }
        shutdownDb(db);
    }
}
