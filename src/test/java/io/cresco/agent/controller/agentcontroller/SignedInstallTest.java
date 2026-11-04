package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.core.AgentServiceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.agent.test.SigningTestSupport;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.JarSignatures;
import io.cresco.library.utilities.CLogger;
import io.cresco.agent.test.TestAgentService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Signed installs (docs/caller-authorization.md section 5) and remote file roots (audit D3a): what JarIntake takes in,
 * and what getfiledata/getfileinfo read.
 */
class SignedInstallTest {

    static Path dir;
    static SigningTestSupport keys;
    static Path signed, unsigned, foreign;

    @BeforeAll
    static void jars() throws Exception {
        java.nio.file.Files.createDirectories(Path.of("target"));
        dir = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "signed-install");
        keys = new SigningTestSupport(dir);
        unsigned = keys.plainJar("plain.jar", "test.plugin");
        signed = keys.sign(unsigned, "leaf", "signed.jar");
        foreign = keys.sign(unsigned, "foreign", "foreign.jar");
    }

    /** The controller's signing policy as AgentServiceImpl answers it, over a data directory. */
    static final class Policy implements AgentService {
        final TestAgentService base;
        final KeyStore trust;
        final boolean required;
        Policy(Path data, KeyStore trust, boolean required) {
            this.base = new TestAgentService(data.toString());
            this.trust = trust;
            this.required = required;
        }
        @Override public AgentState getAgentState() { return base.getAgentState(); }
        @Override public DataPlaneService getDataPlaneService() { return null; }
        @Override public CLogger getCLogger(PluginBuilder p, String b, String i, CLogger.Level l) { return base.getCLogger(p, b, i, l); }
        @Override public CLogger getCLogger(PluginBuilder p, String b, String i) { return base.getCLogger(p, b, i); }
        @Override public void msgOut(String id, MsgEvent msg) { }
        @Override public void setLogLevel(String logId, CLogger.Level level) { }
        @Override public String getAgentDataDirectory() { return base.getAgentDataDirectory(); }
        @Override public KeyStore getPluginSigningTrust() { return trust; }
        @Override public boolean isPluginSigningRequired() { return required; }
    }

    private static JarIntake intake(KeyStore trust, boolean required) throws Exception {
        Path data = Files.createTempDirectory(dir, "data");
        PluginBuilder pb = new PluginBuilder(new Policy(data, trust, required), AgentServiceImpl.class.getName(),
                new MockBundleContext(), new HashMap<>());
        return new JarIntake(pb);
    }

    @Test
    void onlyAJarSignedByATrustedSignerIsTakenIn() throws Exception {
        JarIntake in = intake(keys.trust(), true);
        Path copy = in.verifiedCopy(signed);
        assertTrue(Files.isRegularFile(copy));
        assertNotEquals(signed, copy, "installed from a private copy, never the named file");
        assertArrayEquals(Files.readAllBytes(signed), Files.readAllBytes(copy));
        try {
            assertEquals(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE), Files.getPosixFilePermissions(copy));
        } catch (UnsupportedOperationException notPosix) { /* Windows */ }
        assertThrows(JarSignatures.Rejected.class, () -> in.verifiedCopy(unsigned), "an unsigned jar");
        assertThrows(JarSignatures.Rejected.class, () -> in.verifiedCopy(foreign), "a jar signed by an untrusted signer");
        assertThrows(JarSignatures.Rejected.class, () -> in.verifiedFromBytes(Files.readAllBytes(unsigned)));
        assertNotNull(in.verifiedFromBytes(Files.readAllBytes(signed)));
        Path link = dir.resolve("link.jar");
        Files.deleteIfExists(link);
        Files.createSymbolicLink(link, signed);
        assertThrows(JarSignatures.Rejected.class, () -> in.verifiedCopy(link), "a symlink is not taken in");
        try (var listing = Files.list(in.intakeDir())) {
            assertEquals(2, listing.count(), "a refused jar leaves no copy behind");
        }
    }

    @Test
    void noTrustStoreRefusesEverySignedJarAndTheDevelopmentEscapeIsExplicit() throws Exception {
        assertThrows(JarSignatures.Rejected.class, () -> intake(null, true).verifiedCopy(signed),
                "without plugin_signing_truststore nothing arriving over the network is installed");
        assertNotNull(intake(null, false).verifiedCopy(unsigned), "plugin_signing_required=false (development) takes it");
    }

    @Test
    void theCacheNamesAJarByItsOwnHash() throws Exception {
        JarIntake in = intake(keys.trust(), true);
        Path cache = Files.createTempDirectory(dir, "cache");
        Path cached = in.moveToContentName(in.verifiedCopy(signed), cache);
        assertEquals(io.cresco.library.crypto.CrescoCrypto.contentHashHex(signed), cached.getFileName().toString());
        assertTrue(JarIntake.simpleName("executor-1.3.jar"));
        for (String bad : new String[]{"../x.jar", "a/b.jar", "..", "", "x\\y.jar", "x y.jar", null}) {
            assertFalse(JarIntake.simpleName(bad), String.valueOf(bad));
        }
    }

    @Test
    void remoteFileReadsStayInsideTheirRoots() throws Exception {
        Path root = Files.createTempDirectory(dir, "root");
        Path inside = Files.writeString(root.resolve("ok.txt"), "x");
        Path outside = Files.writeString(dir.resolve("secret.txt"), "s");
        Path escape = root.resolve("escape.txt");
        Files.createSymbolicLink(escape, outside);
        List<String> roots = List.of(root.toString());
        assertNull(AgentExecutor.allowedFile(List.of(), inside.toString()), "no file_access_roots: refused");
        assertEquals(inside.toRealPath(), AgentExecutor.allowedFile(roots, inside.toString()));
        assertNull(AgentExecutor.allowedFile(roots, escape.toString()), "a symlink out of the root");
        assertNull(AgentExecutor.allowedFile(roots, root.resolve("../secret.txt").toString()), "a .. out of the root");
        assertNull(AgentExecutor.allowedFile(roots, root.toString()), "a directory");
        assertNull(AgentExecutor.allowedFile(roots, root.resolve("missing").toString()));
        Path linkedRoot = dir.resolve("linked-root");
        Files.deleteIfExists(linkedRoot);
        Files.createSymbolicLink(linkedRoot, root);
        assertEquals(inside.toRealPath(), AgentExecutor.allowedFile(List.of(linkedRoot.toString()), inside.toString()),
                "a root given through a symlink is its real directory");
    }
}
