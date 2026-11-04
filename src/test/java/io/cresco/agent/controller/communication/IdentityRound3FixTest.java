package io.cresco.agent.controller.communication;

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

import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Round-3 identity fixes (P2-P5): the whole path is judged, CA scopes are explicit, the global is a PKI role, D6 file rules. */
class IdentityRound3FixTest {

    private static final long DAY = 24L * 3600 * 1000;

    /** A CA under {@code issuer} (null: self-signed) with {@code subject}, validity and optional SAN URIs. */
    private static PkiTestSupport.Ca ca(PkiTestSupport.Ca issuer, String subject, long from, long to, String... uris) throws Exception {
        TlsTestSupport.provider();
        KeyPair kp = TlsTestSupport.rsa();
        X500Name s = new X500Name(subject);
        X500Name i = issuer == null ? s : X500Name.getInstance(issuer.cert.getSubjectX500Principal().getEncoded());
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(i, new BigInteger(63, new SecureRandom()).add(BigInteger.ONE),
                new Date(from), new Date(to), s, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, issuer == null ? new BasicConstraints(true) : new BasicConstraints(0));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        if (uris.length > 0) {
            GeneralName[] g = new GeneralName[uris.length];
            for (int k = 0; k < uris.length; k++) g[k] = new GeneralName(GeneralName.uniformResourceIdentifier, uris[k]);
            b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(g));
        }
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC")
                .build((issuer == null ? kp : issuer.kp).getPrivate())));
        X509Certificate[] above = issuer == null ? new X509Certificate[0] : new X509Certificate[]{issuer.cert};
        Constructor<PkiTestSupport.Ca> c = PkiTestSupport.Ca.class.getDeclaredConstructor(KeyPair.class, X509Certificate.class, X509Certificate[].class);
        c.setAccessible(true);
        return c.newInstance(kp, cert, above);
    }

    private static long now() { return System.currentTimeMillis(); }

    // ---------------------------------------------------------------- P2: every certificate on the path, the anchor included

    @Test
    void anExpiredAnchorStopsItsLeaves() throws Exception {
        PkiTestSupport.Ca old = ca(null, "CN=Old Root,O=Cresco", now() - 400 * DAY, now() - DAY);
        PkiTestSupport.Node n = PkiTestSupport.node(old, "t1", "rgr", "b1", false, now() - 60_000, 30 * DAY);
        String why = ProvisionedIdentity.validate(n.chain, List.of(old.cert), new Date());
        System.out.println("expired anchor -> " + why);
        assertNotNull(why, "a leaf under an EXPIRED anchor validates");
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(List.of(old.cert), ProvisionedIdentity.Revocations.NONE);
        assertNull(reg.verifiedIdentity(n.chain));
        assertNotNull(reg.revokedReason(n.chain), "the live sweep keeps a connection under an expired anchor");
    }

    @Test
    void withTheRootAsTheOnlyAnchorARegionChainIsAcceptedAndItsRevokedCaClosesIt() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgr = PkiTestSupport.region(root, "t1", "rgr");
        PkiTestSupport.Node b1 = PkiTestSupport.node(rgr, "t1", "rgr", "b1", false, now() - 60_000, 30 * DAY);
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.NONE);
        assertNotNull(reg.verifiedIdentity(b1.chain), "leaf + region CA + root, root the only anchor");
        assertNull(reg.revokedReason(b1.chain));
        PeerTrustRegistry fresh = new PeerTrustRegistry();       // (a registry that validated the leaf's path keeps it for that leaf)
        fresh.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.NONE);
        assertNull(fresh.verifiedIdentity(new X509Certificate[]{b1.chain[0]}), "a leaf without its region CA reaches no anchor");
        Path rev = Files.createTempDirectory("id3-rev").resolve("revocations.pem");
        Files.writeString(rev, "cert:" + CertificateManager.certFingerprint(rgr.cert) + "\n");
        PkiTestSupport.ownerOnly(rev);
        reg.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.read(rev, List.of(root.cert)));
        String live = reg.revokedReason(b1.chain);
        System.out.println("root-only anchors, region CA revoked -> live: " + live);
        assertNotNull(live, "the live sweep keeps a leaf of the revoked region CA");
        assertNull(reg.verifiedIdentity(b1.chain));
    }

    @Test
    void aRegionCaInTheAnchorsIsStillScopedButAnUnscopedIntermediateAnchorIsRefused() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca rgr = PkiTestSupport.region(root, "t1", "rgr");
        PkiTestSupport.Node other = PkiTestSupport.node(rgr, "t1", "rgB", "x", false, now() - 60_000, 30 * DAY);
        assertNotNull(ProvisionedIdentity.validate(new X509Certificate[]{other.chain[0]}, List.of(rgr.cert), new Date()),
                "region rgr's CA, as an anchor, issued a name of region rgB");
        PkiTestSupport.Ca legacy = ca(root, "CN=rgr CA,OU=rgr,O=t1", now() - DAY, now() + 900 * DAY);   // the earlier tool's region CA
        Path ta = PkiTestSupport.pem(Files.createTempDirectory("id3-ta").resolve("trust-anchors.pem"), root.cert, legacy.cert);
        ProvisionedIdentity.ProvisioningException e = assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> ProvisionedIdentity.readAnchors(ta));
        System.out.println("legacy anchors -> " + e.getMessage());
        PkiTestSupport.Node n = PkiTestSupport.node(legacy, "t1", "rgr", "b1", false, now() - 60_000, 30 * DAY);
        assertNotNull(ProvisionedIdentity.validate(n.chain, List.of(root.cert), new Date()), "an unscoped region CA issued an accepted leaf");
    }

    // ---------------------------------------------------------------- P3: explicit CA scopes, fail closed

    @Test
    void caScopesAreExplicitAndEnforcedForTenantRegionAndRole() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        long from = now() - 60_000;
        PkiTestSupport.Ca t1 = ca(root, "CN=t1 CA,O=t1", now() - DAY, now() + 900 * DAY, "cresco:ca:t1/*", "cresco:ca-roles:node,controller");
        assertNull(ProvisionedIdentity.validate(PkiTestSupport.node(t1, "t1", "rgA", "a", true, from, DAY).chain, List.of(root.cert), new Date()),
                "a t1-wide CA issues a t1 controller");
        assertNotNull(ProvisionedIdentity.validate(PkiTestSupport.node(t1, "t2", "rgA", "a", false, from, DAY).chain, List.of(root.cert), new Date()),
                "a t1 CA issued a t2 node");
        assertNotNull(ProvisionedIdentity.validate(PkiTestSupport.global(t1, "t1", "glob", "gc", from, DAY).chain, List.of(root.cert), new Date()),
                "a CA without the global role issued the global");
        PkiTestSupport.Ca nodesOnly = ca(root, "CN=n CA,O=t1", now() - DAY, now() + 900 * DAY, "cresco:ca:t1/rgA", "cresco:ca-roles:node");
        assertNull(ProvisionedIdentity.validate(PkiTestSupport.node(nodesOnly, "t1", "rgA", "a", false, from, DAY).chain, List.of(root.cert), new Date()));
        assertNotNull(ProvisionedIdentity.validate(PkiTestSupport.node(nodesOnly, "t1", "rgA", "c", true, from, DAY).chain, List.of(root.cert), new Date()),
                "a nodes-only CA issued a controller");
        PkiTestSupport.Ca noRoles = ca(root, "CN=r CA,O=t1", now() - DAY, now() + 900 * DAY, "cresco:ca:t1/rgA");
        assertNotNull(ProvisionedIdentity.validate(PkiTestSupport.node(noRoles, "t1", "rgA", "a", false, from, DAY).chain, List.of(root.cert), new Date()),
                "a CA naming no roles issued a node");
        PkiTestSupport.Ca bad = ca(root, "CN=b CA,O=t1", now() - DAY, now() + 900 * DAY, "cresco:ca:t1/rgA", "cresco:ca-roles:node,admin");
        assertNotNull(ProvisionedIdentity.validate(PkiTestSupport.node(bad, "t1", "rgA", "a", false, from, DAY).chain, List.of(root.cert), new Date()),
                "a malformed scope was honoured");
        assertNull(ProvisionedIdentity.validate(PkiTestSupport.global(root, "t1", "glob", "gc", from, DAY).chain, List.of(root.cert), new Date()),
                "the root issues the global");
    }

    // ---------------------------------------------------------------- P5: the D6 rules for the provisioned files

    @Test
    void provisionedFilesUnderADirectoryOthersCanWriteAreRefused() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        Path top = Files.createTempDirectory("id3-dirs");
        Path ok = PkiTestSupport.pem(top.resolve("a/b/trust-anchors.pem"), root.cert);
        Files.setPosixFilePermissions(top.resolve("a"), PosixFilePermissions.fromString("rwx------"));
        Files.setPosixFilePermissions(top.resolve("a/b"), PosixFilePermissions.fromString("rwx------"));
        ProvisionedIdentity.checkFile(ok, false, "trust_anchors");
        Files.setPosixFilePermissions(top.resolve("a"), PosixFilePermissions.fromString("rwxrwx---"));       // a GRANDPARENT group-writable
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> ProvisionedIdentity.checkFile(ok, false, "trust_anchors"));
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> ProvisionedIdentity.readAnchors(ok), "readAnchors skips the check");
        Files.setPosixFilePermissions(top.resolve("a"), PosixFilePermissions.fromString("rwx------"));
        Path link = top.resolve("a/b/link.pem");
        Files.createSymbolicLink(link, ok);
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> ProvisionedIdentity.checkFile(link, false, "trust_anchors"));
    }
}
