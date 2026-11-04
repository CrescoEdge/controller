package io.cresco.agent.controller.agentcontroller;

import com.sun.net.httpserver.HttpServer;
import io.cresco.library.security.JarSignatures;
import jdk.security.jarsigner.JarSigner;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaCertStore;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.cms.jcajce.JcaSimpleSignerInfoGeneratorBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.bouncycastle.tsp.TSPAlgorithms;
import org.bouncycastle.tsp.TimeStampRequest;
import org.bouncycastle.tsp.TimeStampResponseGenerator;
import org.bouncycastle.tsp.TimeStampTokenGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Review item: a signed jar stays installable after its signing certificate expires when its signature carries a trusted
 * RFC 3161 timestamp (JarSignatures). A real time-stamping authority over HTTP, test-only keys under target/.
 */
class JarTimestampTest {

    static Path dir;
    static KeyPair caKey, signerKey, tsaKey, foreignTsaKey;
    static X509Certificate ca, signer, tsa, foreignTsa;
    static long signerNotAfter;
    static HttpServer server;
    static final AtomicLong serial = new AtomicLong(1);

    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp384r1"));
        return g.generateKeyPair();
    }

    static X509Certificate cert(String cn, KeyPair subject, X509Certificate issuer, PrivateKey issuerKey, Date notAfter,
                                boolean isCa, KeyPurposeId eku, boolean ekuCritical) throws Exception {
        X500Name name = new X500Name("CN=" + cn);
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(issuer == null ? name : new X500Name(issuer.getSubjectX500Principal().getName()),
                BigInteger.valueOf(serial.incrementAndGet()), new Date(System.currentTimeMillis() - 60_000), notAfter, name, subject.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
        if (isCa) b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        else b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
        if (eku != null) b.addExtension(Extension.extendedKeyUsage, ekuCritical, new ExtendedKeyUsage(eku));
        return new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA384withECDSA").build(issuerKey == null ? subject.getPrivate() : issuerKey)));
    }

    @BeforeAll
    static void pki() throws Exception {
        Files.createDirectories(Path.of("target"));
        dir = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "jar-timestamp");
        Date year = new Date(System.currentTimeMillis() + 365L * 24 * 3600 * 1000);
        caKey = ec(); signerKey = ec(); tsaKey = ec(); foreignTsaKey = ec();
        ca = cert("Test Signing CA", caKey, null, null, year, true, null, false);
        signerNotAfter = System.currentTimeMillis() + 4_000;                       // expires during the test
        signer = cert("Test Signer", signerKey, ca, caKey.getPrivate(), new Date(signerNotAfter), false, KeyPurposeId.id_kp_codeSigning, false);
        tsa = cert("Test TSA", tsaKey, ca, caKey.getPrivate(), year, false, KeyPurposeId.id_kp_timeStamping, true);
        foreignTsa = cert("Foreign TSA", foreignTsaKey, null, null, year, false, KeyPurposeId.id_kp_timeStamping, true);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/tsa", ex -> respond(ex, tsaKey, tsa));
        server.createContext("/foreign", ex -> respond(ex, foreignTsaKey, foreignTsa));
        server.start();
    }

    @AfterAll
    static void stop() {
        if (server != null) server.stop(0);
    }

    static void respond(com.sun.net.httpserver.HttpExchange ex, KeyPair key, X509Certificate cert) throws java.io.IOException {
        try {
            TimeStampRequest req = new TimeStampRequest(ex.getRequestBody().readAllBytes());
            TimeStampTokenGenerator gen = new TimeStampTokenGenerator(
                    new JcaSimpleSignerInfoGeneratorBuilder().build("SHA384withECDSA", key.getPrivate(), cert),
                    new JcaDigestCalculatorProviderBuilder().build().get(new AlgorithmIdentifier(NISTObjectIdentifiers.id_sha256)),
                    new ASN1ObjectIdentifier("1.2.3.4.1"));
            gen.addCertificates(new JcaCertStore(cert == tsa ? List.of(tsa, ca) : List.of(cert)));
            byte[] body = new TimeStampResponseGenerator(gen, TSPAlgorithms.ALLOWED)
                    .generate(req, BigInteger.valueOf(serial.incrementAndGet()), new Date()).getEncoded();
            ex.getResponseHeaders().add("Content-Type", "application/timestamp-reply");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream o = ex.getResponseBody()) { o.write(body); }
        } catch (Exception e) {
            ex.sendResponseHeaders(500, -1);
        }
    }

    static Path jar(String name) throws Exception {
        Path p = dir.resolve(name);
        Manifest mf = new Manifest();
        mf.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        mf.getMainAttributes().putValue("Bundle-SymbolicName", "test.plugin");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(p), mf)) {
            out.putNextEntry(new JarEntry("data/payload.txt"));
            out.write(("payload " + name).getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return p;
    }

    static Path sign(Path in, String tsaPath, String outName) throws Exception {
        JarSigner.Builder b = new JarSigner.Builder(signerKey.getPrivate(),
                CertificateFactory.getInstance("X.509").generateCertPath(List.of(signer, ca)))
                .digestAlgorithm("SHA-384").signatureAlgorithm("SHA384withECDSA");
        if (tsaPath != null) b.tsa(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + tsaPath));
        Path out = dir.resolve(outName);
        try (ZipFile zf = new ZipFile(in.toFile()); OutputStream os = Files.newOutputStream(out)) {
            b.build().sign(zf, os);
        }
        return out;
    }

    @Test
    void aTimestampedSignatureOutlivesItsSigningCertificate() throws Exception {
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", ca);
        Path stamped = sign(jar("a.jar"), "/tsa", "stamped.jar");
        Path plain = sign(jar("b.jar"), null, "plain.jar");
        Path foreign = sign(jar("c.jar"), "/foreign", "foreign-stamp.jar");
        assertNotNull(JarSignatures.verify(stamped, trust), "valid now, timestamped");
        assertNotNull(JarSignatures.verify(plain, trust), "valid now, no timestamp");

        long wait = signerNotAfter - System.currentTimeMillis() + 1_500;
        if (wait > 0) Thread.sleep(wait);
        assertTrue(System.currentTimeMillis() > signer.getNotAfter().getTime(), "the signing certificate has expired");

        assertNotNull(JarSignatures.verify(stamped, trust), "a trusted timestamp vouches the jar was signed in time");
        assertThrows(JarSignatures.Rejected.class, () -> JarSignatures.verify(plain, trust),
                "without a timestamp an expired signer is refused");
        assertThrows(JarSignatures.Rejected.class, () -> JarSignatures.verify(foreign, trust),
                "a timestamp from an untrusted authority vouches for nothing");
    }
}
