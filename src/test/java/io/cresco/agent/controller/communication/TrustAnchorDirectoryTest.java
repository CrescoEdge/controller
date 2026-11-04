package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Audit D6: the public-key directory is a trust source only for allowlisted fingerprints, in a private absolute directory. */
class TrustAnchorDirectoryTest {

    private static Path dirWith(X509Certificate... certs) throws Exception {
        Path d = Files.createTempDirectory("pk").resolve("public-keys");
        Files.createDirectories(d, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        int i = 0;
        for (X509Certificate c : certs) {
            Path f = d.resolve("peer" + (i++) + ".cer");
            Files.write(f, c.getEncoded());
            Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("rw-------"));
        }
        return d;
    }

    private static ControllerEngine node(Path pk, String allow) throws Exception {
        return TlsTestSupport.engine("rgA", "n" + System.nanoTime() % 100000, Map.of("public_key_directory", pk.toString(),
                "public_key_trusted_fingerprints", allow));
    }

    @Test
    void onlyAllowlistedCertificatesBecomeAnchors() throws Exception {
        TlsTestSupport.Id listed = TlsTestSupport.selfRooted("listed", "t1", "rgA", "x1"), other = TlsTestSupport.selfRooted("other", "t1", "rgA", "x2");
        Path d = dirWith(listed.chain[1], other.chain[1]);
        String fp = CertificateManager.certFingerprint(listed.chain[1]);
        ControllerEngine e = node(d, fp.toUpperCase().replaceAll("(..)(?!$)", "$1:"));
        List<String> imported = e.getCertificateManager().importPublicKeysFromDirectory();
        assertEquals(List.of(fp), imported, "only the allowlisted fingerprint is imported");
        assertTrue(trusted(e, listed.chain));
        assertFalse(trusted(e, other.chain), "a certificate that was merely placed in the directory became a trust anchor");
    }

    @Test
    void anEmptyAllowlistImportsNothing() throws Exception {
        TlsTestSupport.Id c = TlsTestSupport.selfRooted("c", "t1", "rgA", "x3");
        ControllerEngine e = node(dirWith(c.chain[1]), "");
        assertTrue(e.getCertificateManager().importPublicKeysFromDirectory().isEmpty());
        assertFalse(trusted(e, c.chain));
    }

    @Test
    void aLooseDirectoryOrFileOrARelativePathImportsNothing() throws Exception {
        TlsTestSupport.Id c = TlsTestSupport.selfRooted("c", "t1", "rgA", "x4");
        String fp = CertificateManager.certFingerprint(c.chain[1]);
        Path d = dirWith(c.chain[1]);
        Files.setPosixFilePermissions(d, PosixFilePermissions.fromString("rwxrwxrwx"));
        assertTrue(node(d, fp).getCertificateManager().importPublicKeysFromDirectory().isEmpty(), "a world-writable directory");
        Files.setPosixFilePermissions(d, PosixFilePermissions.fromString("rwx------"));
        try (var s = Files.list(d)) { for (Path f : (Iterable<Path>) s::iterator) Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("rw-rw-rw-")); }
        assertTrue(node(d, fp).getCertificateManager().importPublicKeysFromDirectory().isEmpty(), "a world-writable file");
        assertTrue(node(Path.of("relative-public-keys"), fp).getCertificateManager().importPublicKeysFromDirectory().isEmpty(), "a relative path");
    }

    @Test
    void aSymlinkIsNotFollowed() throws Exception {
        TlsTestSupport.Id c = TlsTestSupport.selfRooted("c", "t1", "rgA", "x5");
        Path d = dirWith();
        Path real = Files.createTempFile("elsewhere", ".cer");
        Files.write(real, c.chain[1].getEncoded());
        Files.createSymbolicLink(d.resolve("link.cer"), real);
        assertTrue(node(d, CertificateManager.certFingerprint(c.chain[1])).getCertificateManager().importPublicKeysFromDirectory().isEmpty());
    }

    private static boolean trusted(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkClientTrusted(chain, "RSA"); return true; }
        catch (Exception ex) { return false; }
    }
}
