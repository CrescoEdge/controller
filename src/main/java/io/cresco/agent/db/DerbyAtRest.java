package io.cresco.agent.db;

import io.cresco.agent.core.OwnerOnlyFile;
import io.cresco.library.utilities.CLogger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Derby encryption at rest for the controller database (GaiaKeep OUT-03a). Opt-in: with no
 * {@code db_key_file} the database is opened exactly as before.
 *
 * <p>With {@code db_key_file} set, the file holds the database secret: 64 hex characters are used
 * as a raw 256-bit {@code encryptionKey}, anything else (at least 16 characters) as the
 * {@code bootPassword} from which Derby derives a 256-bit key. The file must be a regular file (not
 * a symlink) owned by the agent's user with mode 0600 or 0400, in a directory no one else can
 * write. The database is booted with {@code dataEncryption=true;encryptionAlgorithm=AES/CBC/NoPadding}:
 * a new database is created encrypted, an existing plaintext one is encrypted in place at this boot,
 * an encrypted one is booted with the key. Any failure (a bad file, a wrong key, a database that did
 * not end up encrypted, a non-file Derby URL) throws {@link AtRestException}; the controller refuses
 * to start rather than run on a plaintext or unreadable database.
 *
 * <p>The secret travels to Derby in the connection {@link Properties}, never in the JDBC URL, so it
 * is not in anything that logs a URL.
 */
public final class DerbyAtRest {

    public static final String ALGORITHM = "AES/CBC/NoPadding";
    public static final int KEY_LENGTH = 256;
    static final int MIN_BOOT_PASSWORD = 16;

    /** A db_key_file is configured but cannot be used: the controller must not start. */
    public static final class AtRestException extends IllegalStateException {
        AtRestException(String msg) { super(msg); }
        AtRestException(String msg, Throwable cause) { super(msg, cause); }
    }

    /** The checked secret from a key file, and which Derby attribute carries it. */
    public static final class Secret {
        final String attribute; // bootPassword or encryptionKey
        final String value;
        Secret(String attribute, String value) { this.attribute = attribute; this.value = value; }
        public String attribute() { return attribute; }
        @Override public String toString() { return attribute + "=[REDACTED]"; }
    }

    private DerbyAtRest() {}

    /** Read the key file after the owner/mode/directory checks ({@link OwnerOnlyFile}); AtRestException on any failure. */
    public static Secret readKeyFile(Path keyFile) {
        String v;
        try {
            v = OwnerOnlyFile.read(keyFile, "db_key_file");
        } catch (OwnerOnlyFile.UnsafeFileException e) {
            throw new AtRestException(e.getMessage(), e);
        }
        Path p = keyFile.toAbsolutePath().normalize();
        if (v.matches("[0-9a-fA-F]{64}")) {
            return new Secret("encryptionKey", v);
        }
        if (v.length() < MIN_BOOT_PASSWORD || v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0) {
            throw new AtRestException("db_key_file " + p + " must hold one line: 64 hex characters (a 256-bit key) or a boot password of at least "
                    + MIN_BOOT_PASSWORD + " characters");
        }
        return new Secret("bootPassword", v);
    }

    /** The database directory a jdbc:derby: URL opens; AtRestException for in-memory/classpath/jar or non-Derby URLs. */
    static Path databaseDirectory(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:derby:")) {
            throw new AtRestException("db_key_file is set but db_jdbc is not an embedded Derby URL; refusing to run an unencrypted database");
        }
        String loc = jdbcUrl.substring("jdbc:derby:".length());
        int semi = loc.indexOf(';');
        if (semi >= 0) loc = loc.substring(0, semi);
        if (loc.startsWith("directory:")) loc = loc.substring("directory:".length());
        if (loc.isEmpty() || loc.startsWith("memory:") || loc.startsWith("classpath:") || loc.startsWith("jar:")) {
            throw new AtRestException("db_key_file is set but the Derby URL does not name an on-disk database");
        }
        Path p = Paths.get(loc);
        if (!p.isAbsolute()) {
            String home = System.getProperty("derby.system.home");
            p = (home != null ? Paths.get(home) : Paths.get("")).resolve(p);
        }
        return p.toAbsolutePath().normalize();
    }

    /** True when the database exists (has a service.properties). */
    static boolean exists(Path dbDir) {
        return Files.isRegularFile(dbDir.resolve("service.properties"));
    }

    /** True when Derby recorded the database as encrypted. */
    public static boolean isEncrypted(Path dbDir) {
        Path sp = dbDir.resolve("service.properties");
        if (!Files.isRegularFile(sp)) return false;
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(sp)) {
            props.load(in);
        } catch (IOException e) {
            return false;
        }
        return "true".equalsIgnoreCase(props.getProperty("dataEncryption"));
    }

    /**
     * Boot the database at {@code jdbcUrl} with the key: create it encrypted, encrypt a plaintext one
     * in place, or open an encrypted one. Must run before anything else opens the database in this
     * JVM (Derby only encrypts at boot). Returns the connection properties for every later
     * connection ({@code base} plus the key); throws AtRestException when the database could not be
     * opened with the key or did not end up encrypted.
     */
    public static Properties boot(String jdbcUrl, Secret secret, Properties base, CLogger logger) {
        Path dbDir = databaseDirectory(jdbcUrl);
        String bootUrl = jdbcUrl.indexOf(';') >= 0 ? jdbcUrl.substring(0, jdbcUrl.indexOf(';')) : jdbcUrl;
        Properties pool = new Properties();
        if (base != null) pool.putAll(base);
        pool.setProperty(secret.attribute, secret.value);

        Properties first = new Properties();
        first.putAll(pool);
        boolean existed = exists(dbDir);
        boolean wasEncrypted = existed && isEncrypted(dbDir);
        if (!wasEncrypted) {
            if (!existed) first.setProperty("create", "true");
            first.setProperty("dataEncryption", "true");
            first.setProperty("encryptionAlgorithm", ALGORITHM);
            if ("bootPassword".equals(secret.attribute)) {
                first.setProperty("encryptionKeyLength", Integer.toString(KEY_LENGTH));
            }
        }
        if (logger != null) {
            logger.info("Derby at rest: " + (!existed ? "creating encrypted database" : wasEncrypted ? "opening encrypted database"
                    : "ENCRYPTING existing plaintext database in place") + " " + dbDir + " (" + ALGORITHM + ", 256-bit, "
                    + secret.attribute + " from db_key_file)");
        }
        try (Connection c = DriverManager.getConnection(bootUrl, first)) {
            c.getMetaData();
        } catch (SQLException e) {
            // never include the properties: they hold the key
            throw new AtRestException("Derby at rest: cannot open " + dbDir + " with the db_key_file secret (SQLState "
                    + e.getSQLState() + "): " + e.getMessage(), e);
        }
        if (!isEncrypted(dbDir)) {
            throw new AtRestException("Derby at rest: " + dbDir + " is not encrypted after boot (was it already open in this JVM?)");
        }
        if (logger != null && existed && !wasEncrypted) {
            logger.info("Derby at rest: " + dbDir + " is now encrypted");
        }
        return pool;
    }
}
