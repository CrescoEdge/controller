package io.cresco.agent.controller.communication;

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

import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/** An operator PKI made in the test (what tools/cresco-pki.sh makes with OpenSSL): a root, region CAs, node leaves. */
public final class PkiTestSupport {

    private PkiTestSupport() { }

    static final long DAY = 24L * 3600 * 1000;

    /** A CA: its key and certificate (and the chain above it). */
    public static final class Ca {
        public final KeyPair kp;
        public final X509Certificate cert;
        public final X509Certificate[] above;
        Ca(KeyPair kp, X509Certificate cert, X509Certificate[] above) { this.kp = kp; this.cert = cert; this.above = above; }
    }

    /** A node: its key, chain (leaf first) and the files a node is given. */
    public static final class Node {
        public final KeyPair kp;
        public final X509Certificate[] chain;
        public Path keystore, password;
        Node(KeyPair kp, X509Certificate[] chain) { this.kp = kp; this.chain = chain; }
    }

    public static Ca root(String cn) throws Exception {
        TlsTestSupport.provider();
        KeyPair kp = TlsTestSupport.rsa();
        X500Name n = new X500Name("CN=" + cn + ",O=Cresco");
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(n, serial(), new Date(now - DAY), new Date(now + 3650 * DAY), n, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        return new Ca(kp, sign(b, kp), new X509Certificate[0]);
    }

    /** A region CA under {@code root}: subject O=tenant, OU=region (it may issue only that region's names). */
    public static Ca region(Ca root, String tenant, String region) throws Exception {
        KeyPair kp = TlsTestSupport.rsa();
        long now = System.currentTimeMillis();
        X500Name subject = new X500Name("CN=" + region + " CA,OU=" + region + ",O=" + tenant);
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(X500Name.getInstance(root.cert.getSubjectX500Principal().getEncoded()), serial(),
                new Date(now - DAY), new Date(now + 1825 * DAY), subject, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        return new Ca(kp, sign(b, root.kp), new X509Certificate[]{root.cert});
    }

    /** A node leaf issued by {@code issuer}, valid from {@code from} for {@code validMs}. */
    public static Node node(Ca issuer, String tenant, String region, String agent, boolean controller, long from, long validMs) throws Exception {
        KeyPair kp = TlsTestSupport.rsa();
        return node(issuer, kp, tenant, region, agent, controller, from, validMs);
    }

    public static Node node(Ca issuer, KeyPair kp, String tenant, String region, String agent, boolean controller, long from, long validMs) throws Exception {
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(X500Name.getInstance(issuer.cert.getSubjectX500Principal().getEncoded()), serial(),
                new Date(from), new Date(from + validMs), CrescoNames.x500(CrescoIdentity.of(tenant, region, agent, null)), kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        List<GeneralName> sans = new ArrayList<>();
        sans.add(new GeneralName(GeneralName.uniformResourceIdentifier, "cresco:node:" + tenant + "/" + region + "/" + agent));
        if (controller) sans.add(new GeneralName(GeneralName.uniformResourceIdentifier, "cresco:role:controller"));
        b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(sans.toArray(new GeneralName[0])));
        X509Certificate leaf = sign(b, issuer.kp);
        X509Certificate[] chain = new X509Certificate[1 + issuer.above.length + 1];
        chain[0] = leaf;
        chain[1] = issuer.cert;
        System.arraycopy(issuer.above, 0, chain, 2, issuer.above.length);
        return new Node(kp, trim(chain));
    }

    private static X509Certificate[] trim(X509Certificate[] c) {
        int n = 0;
        while (n < c.length && c[n] != null) n++;
        return java.util.Arrays.copyOf(c, n);
    }

    /** Write {@code node}'s keystore and password file under {@code dir} (owner-only). */
    public static Node write(Node node, Path dir) throws Exception {
        Files.createDirectories(dir);
        String pass = Base64.getEncoder().encodeToString(new SecureRandom().generateSeed(18));
        node.password = dir.resolve("node.p12.pass");
        Files.writeString(node.password, pass + "\n", StandardCharsets.UTF_8);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("node", node.kp.getPrivate(), pass.toCharArray(), node.chain);
        node.keystore = dir.resolve("node.p12");
        Path tmp = dir.resolve("node.p12.tmp");
        try (OutputStream o = Files.newOutputStream(tmp)) { ks.store(o, pass.toCharArray()); }
        Files.move(tmp, node.keystore, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        ownerOnly(node.password);
        ownerOnly(node.keystore);
        return node;
    }

    /** A PEM bundle of {@code certs}. */
    public static Path pem(Path file, X509Certificate... certs) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (X509Certificate c : certs) {
            sb.append("-----BEGIN CERTIFICATE-----\n")
                    .append(Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(c.getEncoded()))
                    .append("\n-----END CERTIFICATE-----\n");
        }
        Files.createDirectories(file.getParent());
        Files.writeString(file, sb.toString());
        ownerOnly(file);
        return file;
    }

    static void ownerOnly(Path p) throws Exception {
        try { Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-------")); } catch (UnsupportedOperationException ignore) { }
    }

    private static BigInteger serial() { return new BigInteger(63, new SecureRandom()).add(BigInteger.ONE); }

    private static X509Certificate sign(JcaX509v3CertificateBuilder b, KeyPair signer) throws Exception {
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(signer.getPrivate())));
    }
}
