package io.cresco.agent.controller.agentcontroller;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.JarSignatures;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;

/**
 * Every jar that did not come embedded in the agent jar -- uploaded ({@code pluginupload}), fetched from a repository
 * ({@code getjar}, {@code plugindownload}), named by a message ({@code pluginadd} with an absolute path,
 * {@code controllerupdate}) or found in the repository cache -- is installed only from a private copy that verifies:
 * signed, every entry, by a signer chaining to {@code plugin_signing_truststore} ({@link JarSignatures}). The copy is
 * owner-only and nobody else writes it, so the bytes installed are the bytes verified. Hashes a caller supplies are
 * advisory: the receiver computes its own. See docs/caller-authorization.md section 5.
 */
public final class JarIntake {

    /** The largest jar taken in (a plugin is far smaller; this bounds what a caller can make the node store). */
    public static final long MAX_JAR_BYTES = 512L * 1024 * 1024;

    private final PluginBuilder plugin;

    public JarIntake(PluginBuilder plugin) {
        this.plugin = plugin;
    }

    /** The owner-only directory private copies are made in ({@code <agent data dir>/jar-intake}). */
    public Path intakeDir() throws IOException {
        String base = plugin.getAgentService() == null ? null : plugin.getAgentService().getAgentDataDirectory();
        Path dir = Paths.get(base == null ? "cresco-data" : base, "jar-intake").toAbsolutePath();
        Files.createDirectories(dir);
        try {
            Files.setPosixFilePermissions(dir, java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException notPosix) {
            // Windows: the agent data directory's ACL applies
        }
        return dir;
    }

    /** A private copy of {@code source} (a regular file, not a symlink), verified; the caller moves or deletes it. */
    public Path verifiedCopy(Path source) throws IOException, GeneralSecurityException {
        if (source == null || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new JarSignatures.Rejected("not a regular file: " + source);
        }
        if (Files.size(source) > MAX_JAR_BYTES) throw new JarSignatures.Rejected("larger than " + MAX_JAR_BYTES + " bytes: " + source);
        try (InputStream in = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS)) {
            return verifiedFromStream(in);
        }
    }

    /** A private file holding {@code data}, verified; the caller moves or deletes it. */
    public Path verifiedFromBytes(byte[] data) throws IOException, GeneralSecurityException {
        if (data == null || data.length == 0) throw new JarSignatures.Rejected("no jar data");
        if (data.length > MAX_JAR_BYTES) throw new JarSignatures.Rejected("larger than " + MAX_JAR_BYTES + " bytes");
        try (InputStream in = new java.io.ByteArrayInputStream(data)) {
            return verifiedFromStream(in);
        }
    }

    /** A private file holding what {@code in} yields (at most {@link #MAX_JAR_BYTES}), verified. */
    public Path verifiedFromStream(InputStream in) throws IOException, GeneralSecurityException {
        Path copy = Files.createTempFile(intakeDir(), ".intake-", ".jar");      // owner-only on POSIX
        boolean ok = false;
        try {
            long n = 0;
            byte[] buf = new byte[64 * 1024];
            try (java.io.OutputStream out = Files.newOutputStream(copy, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                int r;
                while ((r = in.read(buf)) != -1) {
                    n += r;
                    if (n > MAX_JAR_BYTES) throw new JarSignatures.Rejected("larger than " + MAX_JAR_BYTES + " bytes");
                    out.write(buf, 0, r);
                }
            }
            plugin.verifyJarSignature(copy);
            ok = true;
            return copy;
        } finally {
            if (!ok) {
                try { Files.deleteIfExists(copy); } catch (IOException ignore) { }
            }
        }
    }

    /** Move a verified copy to {@code dir/<its content hash>} (the cache's naming), replacing any file of that name. */
    public Path moveToContentName(Path verified, Path dir) throws Exception {
        String hash = io.cresco.library.crypto.CrescoCrypto.contentHashHex(verified);
        Path target = dir.resolve(hash);
        try {
            Files.move(verified, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException crossDevice) {
            Files.move(verified, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    /** A plain file name: no separators, no "..", not empty, of the characters a jar name uses. */
    public static boolean simpleName(String name) {
        if (name == null || name.isEmpty() || name.length() > 255 || name.equals(".") || name.contains("..")) return false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_';
            if (!ok) return false;
        }
        return true;
    }
}
