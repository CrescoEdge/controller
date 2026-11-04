package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * tools/cresco-pki.sh (OpenSSL 3) makes what a pre-provisioned node loads: a root, a region CA, controllers, agents, an API
 * user, revocations (CRL and deny list). Skipped where OpenSSL 3 is not installed.
 */
class PkiToolTest {

    private static String run(Path pki, String... args) throws Exception {
        String[] cmd = new String[args.length + 2];
        cmd[0] = "bash";
        cmd[1] = Paths.get("tools", "cresco-pki.sh").toAbsolutePath().toString();
        System.arraycopy(args, 0, cmd, 2, args.length);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertTrue(p.waitFor(120, TimeUnit.SECONDS));
        assertEquals(0, p.exitValue(), String.join(" ", args) + ": " + out);
        return out;
    }

    private static boolean openssl3() {
        try {
            Process p = new ProcessBuilder("openssl", "version").redirectErrorStream(true).start();
            String v = new String(p.getInputStream().readAllBytes());
            return p.waitFor(10, TimeUnit.SECONDS) && v.startsWith("OpenSSL 3");
        } catch (Exception e) {
            return false;
        }
    }

    private static ProvisionedIdentity load(Path node, Path pki, ProvisionedIdentity.Revocations rev) throws Exception {
        return ProvisionedIdentity.load(node.resolve("node.p12"), node.resolve("node.p12.pass"), pki.resolve("trust-anchors.pem"), rev, new Date());
    }

    @Test
    void theToolsPkiLoadsAndItsRevocationsHold() throws Exception {
        assumeTrue(openssl3(), "OpenSSL 3 is not installed");
        Path pki = Paths.get("target", "pki-tool-test-" + System.nanoTime()).toAbsolutePath();
        run(pki, "init", pki.toString(), "--days", "30");
        run(pki, "region", pki.toString(), "t1", "rgr", "--days", "30");
        run(pki, "node", pki.toString(), "t1", "glob", "gc", "--controller", "--days", "10");
        run(pki, "node", pki.toString(), "t1", "rgr", "rc", "--controller", "--days", "10");
        run(pki, "node", pki.toString(), "t1", "rgr", "b1", "--days", "10");
        run(pki, "api", pki.toString(), "t1", "alice", "admin", "--days", "10");
        for (String perm : List.of("nodes/rgr_b1/node.p12", "nodes/rgr_b1/node.p12.pass", "root/ca.key")) {
            assertEquals("rw-------", java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(pki.resolve(perm))), perm);
        }
        ProvisionedIdentity gc = load(pki.resolve("nodes/glob_gc"), pki, null), rc = load(pki.resolve("nodes/rgr_rc"), pki, null),
                b1 = load(pki.resolve("nodes/rgr_b1"), pki, null);
        assertTrue(gc.controller && rc.controller && !b1.controller);
        assertEquals("rgr", b1.identity.getRegion());
        assertEquals("b1", b1.identity.getAgent());
        assertEquals(3, b1.chain.length, "an agent's keystore carries leaf, region CA and root");
        // the API user's certificate names no node
        java.security.KeyStore api = java.security.KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(pki.resolve("api/t1_alice/api.p12"))) {
            api.load(in, Files.readAllLines(pki.resolve("api/t1_alice/api.p12.pass")).get(0).trim().toCharArray());
        }
        java.security.cert.X509Certificate alice = (java.security.cert.X509Certificate) api.getCertificate("api");
        assertNull(CrescoNames.identityOf(alice), "an API user's certificate names a node");
        assertTrue(ProvisionedIdentity.sanUris(alice).containsAll(List.of("cresco:api:t1/alice", "cresco:role:admin")), ProvisionedIdentity.sanUris(alice).toString());
        // revoke b1: its CRL entry and its key in the deny list refuse it
        run(pki, "revoke", pki.toString(), pki.resolve("nodes/rgr_b1/node.pem").toString());
        run(pki, "crl", pki.toString());
        ProvisionedIdentity.Revocations rev = ProvisionedIdentity.Revocations.read(pki.resolve("revocations.pem"),
                ProvisionedIdentity.readAnchors(pki.resolve("trust-anchors.pem")));
        assertEquals(2, rev.crls);
        assertNotNull(rev.refusal(b1.chain), "a revoked node passes the operator's revocations");
        assertNull(rev.refusal(rc.chain));
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> load(pki.resolve("nodes/rgr_b1"), pki, rev));
        // only the CRL (without the deny list) still refuses it
        String crlOnly = Files.readString(pki.resolve("revocations.pem")).replaceAll("(?m)^key:.*$", "");
        Path crls = pki.resolve("crl-only.pem");
        Files.writeString(crls, crlOnly);
        PkiTestSupport.ownerOnly(crls);
        ProvisionedIdentity.Revocations byCrl = ProvisionedIdentity.Revocations.read(crls, ProvisionedIdentity.readAnchors(pki.resolve("trust-anchors.pem")));
        assertNotNull(byCrl.refusal(b1.chain), "the CRL alone does not refuse the revoked node");
        deleteTree(pki.toFile());
    }

    private static void deleteTree(File f) {
        File[] c = f.listFiles();
        if (c != null) for (File x : c) deleteTree(x);
        f.delete();
    }
}
