package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.security.CrescoIdentity;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review, round 3, of hardening-identity at 7ba5cd7 (round-2 fixes and pre-provisioned identities),
 * 2026-11-04. Each test asserts the SECURE outcome; a failure is a finding.
 */
class ReviewIdentity3Test {

    private static final long DAY = 24L * 3600 * 1000;

    private static io.cresco.library.plugin.Config cfgOf(Map<String, Object> m) throws Exception {
        return TestAgentService.plugin(Files.createTempDirectory("rvid3-cfg").toString(), m, x -> { }, "rgr", "x").getConfig();
    }

    /** An intermediate CA under {@code issuer} with an arbitrary subject (an operator's PKI is not the tool's). */
    private static PkiTestSupport.Ca intermediate(PkiTestSupport.Ca issuer, String subject) throws Exception {
        KeyPair kp = TlsTestSupport.rsa();
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(X500Name.getInstance(issuer.cert.getSubjectX500Principal().getEncoded()),
                new BigInteger(63, new SecureRandom()).add(BigInteger.ONE), new Date(now - DAY), new Date(now + 1825 * DAY), new X500Name(subject), kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(issuer.kp.getPrivate())));
        X509Certificate[] above = new X509Certificate[issuer.above.length + 1];
        above[0] = issuer.cert;
        System.arraycopy(issuer.above, 0, above, 1, issuer.above.length);
        java.lang.reflect.Constructor<PkiTestSupport.Ca> c = PkiTestSupport.Ca.class.getDeclaredConstructor(KeyPair.class, X509Certificate.class, X509Certificate[].class);
        c.setAccessible(true);
        return c.newInstance(kp, cert, above);
    }

    // ================================================================== CA scope

    /**
     * ProvisionedIdentity.validate scopes a CA only when caScopeOf finds BOTH a valid OU (region) and a valid O (tenant); any
     * other CA on the path -- a per-tenant CA (O=t2 only), an enterprise issuing CA ('O=Acme Corp', a space), a CA with a
     * multi-valued RDN -- is treated like the root: it may issue ANY identity. The documented rule is "a CA naming NEITHER (the
     * fabric root) may issue any". A tenant t2 CA mints the GLOBAL controller of tenant t1 (cresco:role:controller), and every
     * provisioned node accepts it.
     */
    @Test
    void anIntermediateThatNamesNoRegionCannotIssueAnotherTenantsController() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca tenantT2 = intermediate(root, "CN=Tenant t2 CA,O=t2");
        PkiTestSupport.Node forged = PkiTestSupport.node(tenantT2, "t1", "glob", "gc", true, System.currentTimeMillis() - 60_000, 30 * DAY);
        String why = ProvisionedIdentity.validate(forged.chain, List.of(root.cert), new Date());
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.NONE);
        CrescoIdentity id = reg.verifiedIdentity(forged.chain);
        System.out.println("scope: tenant t2's CA issued t1/glob/gc (controller) -> validate=" + (why == null ? "VALID" : why) + "; registry="
                + id + "; bridge peer=" + reg.isBridgePeer(id));
        assertNotNull(why, "a CA that names only tenant t2 issued tenant t1's GLOBAL controller identity, and it validates");
    }

    // ================================================================== revoking a region CA that is a trust anchor

    /**
     * The tool puts every region CA into trust-anchors.pem (and the docs say: "the root, and the region CAs"), and the node
     * validates a peer's chain to the FIRST anchor it reaches. The operator's revocations (deny list key/cert entries and CRL
     * serials) are checked only against the certificates the PEER PRESENTED. Revoking a compromised region CA -- exactly
     * what `cresco-pki.sh revoke <region ca.pem>` + `crl` does (the root's CRL lists its serial, the deny list its key) --
     * therefore does not stop a leaf it issued when the leaf is presented WITHOUT its issuer: the path ends at the region CA
     * as an anchor and nothing presented is revoked.
     */
    @Test
    void aRevokedRegionCaStopsItsLeavesEvenWhenTheyArePresentedAlone() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgr = PkiTestSupport.region(root, "t1", "rgr");
        PkiTestSupport.Node minted = PkiTestSupport.node(rgr, "t1", "rgr", "rc2", true, System.currentTimeMillis() - 60_000, 30 * DAY);
        Path dir = Files.createTempDirectory("rvid3-rev");
        Path rev = dir.resolve("revocations.pem");
        Files.writeString(rev, "key:" + PeerTrustRegistry.fingerprint(rgr.cert.getPublicKey().getEncoded()) + "  # region rgr's CA key was stolen\n"
                + "cert:" + CertificateManager.certFingerprint(rgr.cert) + "\n");
        PkiTestSupport.ownerOnly(rev);
        List<X509Certificate> anchors = List.of(root.cert, rgr.cert);                         // trust-anchors.pem as the tool writes it
        ProvisionedIdentity.Revocations r = ProvisionedIdentity.Revocations.read(rev, anchors);
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(anchors, r);
        CrescoIdentity withIssuer = reg.verifiedIdentity(minted.chain);
        CrescoIdentity alone = reg.verifiedIdentity(new X509Certificate[]{minted.chain[0]});
        System.out.println("anchor revocation: a leaf of the REVOKED region CA, chain presented -> " + withIssuer + "; leaf presented alone -> " + alone);
        assertNull(withIssuer, "precondition: with its issuer in the chain the leaf is refused");
        assertNull(alone, "a leaf issued by a REVOKED region CA is accepted when presented without its issuer (the CA is a trust anchor)");
    }

    // ================================================================== provisioned file checks

    /**
     * checkFile looks at the file's own mode bits only: not its owner, not its directory. trust_anchors and revocation_file
     * are RE-READ every 30 s; whoever can write their directory (a group-writable /etc/cresco, a shared data directory)
     * renames in a file of their own, mode 644 -- and with it the fabric's trust anchors (a root of their own: any identity,
     * any controller) or an empty revocation list. (Round 1's public-key directory check, D6, refused both cases.)
     */
    @Test
    void trustAnchorsInADirectoryOthersCanWriteAreRefused() throws Exception {
        Path dir = Files.createTempDirectory("rvid3-anchors");
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        Path anchors = PkiTestSupport.pem(dir.resolve("trust-anchors.pem"), root.cert);
        Files.setPosixFilePermissions(anchors, PosixFilePermissions.fromString("rw-r--r--"));
        String why;
        try {
            ProvisionedIdentity.checkFile(anchors, false, "trust_anchors");
            why = null;
        } catch (ProvisionedIdentity.ProvisioningException e) {
            why = e.getMessage();
        }
        System.out.println("files: trust_anchors in a world-writable directory -> " + (why == null ? "ACCEPTED" : why));
        assertNotNull(why, "trust_anchors in a directory anyone can write (and so replace it) is accepted and hot-reloaded");
    }

    /**
     * Live connections: the broker's sweep (CrescoAuthorizationBroker.closeInvalidConnections, run at every revocation and
     * every minute) and the outbound record judge a connection by PeerTrustRegistry.revokedReason(leaf) -- the operator's
     * revocation file applied to the LEAF only; the per-region CA check there uses the registry's recorded CAs, which a
     * pre-provisioned node never has. Revoking a region CA in revocation_file therefore closes none of the connections its
     * nodes hold, whatever chain they presented.
     */
    @Test
    void anOperatorRevokedIssuerClosesTheLiveConnectionsOfItsLeaves() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgr = PkiTestSupport.region(root, "t1", "rgr");
        PkiTestSupport.Node b1 = PkiTestSupport.node(rgr, "t1", "rgr", "b1", false, System.currentTimeMillis() - 60_000, 30 * DAY);
        Path rev = Files.createTempDirectory("rvid3-live").resolve("revocations.pem");
        Files.writeString(rev, "key:" + PeerTrustRegistry.fingerprint(rgr.cert.getPublicKey().getEncoded()) + "\n");
        PkiTestSupport.ownerOnly(rev);
        List<X509Certificate> anchors = List.of(root.cert, rgr.cert);
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(anchors, ProvisionedIdentity.Revocations.read(rev, anchors));
        String live = reg.revokedReason(b1.chain[0]);
        System.out.println("live sweep: a connection of rgr/b1 after rgr's CA was revoked in revocation_file -> " + (live == null ? "KEPT" : "closed: " + live)
                + "; a new handshake with its full chain -> " + reg.verifiedIdentity(b1.chain));
        assertNotNull(live, "the live-connection sweep keeps every connection whose ISSUER the operator revoked (it judges the leaf only)");
    }
}
