package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * A regional Certificate Authority. The regional controller holds one of these (a persistent root +
 * issuing-intermediate keypair) and signs leaf certificates for the agents that enroll into its region.
 * Trust in the fabric then anchors on the <em>region CA certificate</em> — one per region, distributed
 * over the discovery-secret-authenticated channel — instead of every node exchanging its own leaf
 * (O(regions) trust material, not O(nodes²)). Peers validate one another with
 * {@link io.cresco.library.security.CertTrust#verifyChain} against the set of region CA certs they hold.
 *
 * <p>This is the issuance half of the distributed-identity design
 * (see {@code docs/distributed-identity-trust-design.md}, Option C). It is deliberately a small, pure
 * BouncyCastle helper with no ControllerEngine coupling, so it is unit-testable in isolation and reusable
 * by {@code CertificateManager}. Structurally it is the same three-tier chain the manager already builds
 * (root → intermediate → leaf); the difference is that the intermediate's signing key is <b>retained</b>
 * so the region can sign <em>other</em> nodes' leaves, not just its own.
 */
public final class RegionCA {

    private static final String SIG_ALG = "SHA256withRSA";

    private final X509Certificate rootCert;
    private final KeyPair caKeyPair;      // the issuing intermediate — signs enrolling nodes' leaves
    private final X509Certificate caCert;

    private RegionCA(X509Certificate rootCert, KeyPair caKeyPair, X509Certificate caCert) {
        this.rootCert = rootCert;
        this.caKeyPair = caKeyPair;
        this.caCert = caCert;
    }

    /** Generate a fresh region CA (self-signed root + issuing intermediate) for {@code tenant/region}. */
    public static RegionCA generate(String tenant, String region, int keySize, int validityYears) throws Exception {
        KeyPair rootKp = keyPair(keySize);
        X500Name rootName = new X500Name("CN=rootCA-" + region + ", O=" + safe(tenant));
        X509Certificate root = build(rootName, rootName, rootKp.getPublic(), rootKp.getPrivate(),
                validityYears, true, KeyUsage.keyCertSign | KeyUsage.cRLSign);

        KeyPair caKp = keyPair(keySize);
        X500Name caName = new X500Name("CN=regionCA-" + region + ", OU=" + safe(region) + ", O=" + safe(tenant));
        X509Certificate ca = build(caName, rootName, caKp.getPublic(), rootKp.getPrivate(),
                validityYears, true, KeyUsage.keyCertSign | KeyUsage.cRLSign);

        return new RegionCA(root, caKp, ca);
    }

    /**
     * Issue a leaf certificate for an enrolling node: subject = the node's Cresco identity DN, issuer =
     * this region CA, signed by the region CA private key. Returns the full chain [leaf, regionCA, root]
     * that the node installs as its keystore identity.
     */
    public X509Certificate[] issueLeaf(PublicKey subjectPublicKey, CrescoIdentity identity, int validityYears) throws Exception {
        X500Name subject = new X500Name(identity.toX500Name());
        // Take the issuer name directly from the CA certificate so its DER encoding byte-matches the
        // CA's subject (reconstructing it from a string can change the encoding and break PKIX name-chaining).
        X500Name issuer = new JcaX509CertificateHolder(caCert).getSubject();
        X509Certificate leaf = build(subject, issuer, subjectPublicKey, caKeyPair.getPrivate(),
                validityYears, false, KeyUsage.digitalSignature | KeyUsage.keyEncipherment);
        return new X509Certificate[]{leaf, caCert, rootCert};
    }

    /** The region's trust anchors [regionCA, root] — this is what gets distributed to other regions. */
    public X509Certificate[] caChain() {
        return new X509Certificate[]{caCert, rootCert};
    }

    /** The issuing region-CA certificate (the primary trust anchor peers validate chains against). */
    public X509Certificate caCert() {
        return caCert;
    }

    public PrivateKey caPrivateKey() {
        return caKeyPair.getPrivate();
    }

    // --- helpers ---

    private static KeyPair keyPair(int keySize) throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA", "BC");
        kpg.initialize(Math.max(2048, keySize));
        return kpg.generateKeyPair();
    }

    private static X509Certificate build(X500Name subject, X500Name issuer, PublicKey subjectKey,
                                         PrivateKey signingKey, int years, boolean isCa, int keyUsage) throws Exception {
        long now = System.currentTimeMillis();
        Date notBefore = new Date(now - 60_000L); // small backdate for clock skew
        Date notAfter = new Date(now + years * 365L * 24 * 60 * 60 * 1000L);
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer, new BigInteger(64, new SecureRandom()), notBefore, notAfter, subject, subjectKey);
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(keyUsage));
        return new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder(SIG_ALG).setProvider("BC").build(signingKey)));
    }

    private static String safe(String s) {
        return (s == null || s.isEmpty()) ? "default" : s;
    }
}
