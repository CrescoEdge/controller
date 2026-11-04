package io.cresco.agent.test;

import jdk.security.jarsigner.JarSigner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

/**
 * Test-only code-signing keys (made with keytool under a test directory, never committed): a signing CA, a leaf it
 * issued for code signing, and a foreign self-signed signer; plain and signed jars. The trust store holds the CA only.
 */
public final class SigningTestSupport {

    public static final char[] PW = "test-only-not-a-secret".toCharArray();

    public final Path dir;
    public final Path keys;
    public final Path trustStore;

    public SigningTestSupport(Path dir) throws Exception {
        this.dir = dir;
        this.keys = dir.resolve("signing-keys.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp384r1", "-dname", "CN=Test Signing CA",
                "-ext", "bc:c", "-validity", "2");
        keytool("-genkeypair", "-alias", "leaf", "-keyalg", "EC", "-groupname", "secp384r1", "-dname", "CN=Test Signer",
                "-validity", "2");
        Path csr = dir.resolve("leaf.csr"), crt = dir.resolve("leaf.crt");
        keytool("-certreq", "-alias", "leaf", "-file", csr.toString());
        keytool("-gencert", "-alias", "ca", "-infile", csr.toString(), "-outfile", crt.toString(), "-rfc", "-validity", "2",
                "-ext", "ku:c=digitalSignature", "-ext", "eku=codeSigning");
        keytool("-importcert", "-noprompt", "-alias", "leaf", "-file", crt.toString());
        keytool("-genkeypair", "-alias", "foreign", "-keyalg", "EC", "-groupname", "secp384r1", "-dname", "CN=Foreign",
                "-validity", "2");
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", load().getCertificate("ca"));
        trustStore = dir.resolve("plugin-signing-trust.p12");
        try (OutputStream out = Files.newOutputStream(trustStore)) {
            trust.store(out, PW);
        }
    }

    public KeyStore trust() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(trustStore)) {
            ks.load(in, PW);
        }
        return ks;
    }

    private void keytool(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(Arrays.asList(args));
        cmd.addAll(Arrays.asList("-keystore", keys.toString(), "-storetype", "PKCS12", "-storepass", new String(PW),
                "-keypass", new String(PW)));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("keytool " + String.join(" ", args) + ": " + out);
        }
    }

    private KeyStore load() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keys)) {
            ks.load(in, PW);
        }
        return ks;
    }

    /** A plain (unsigned) jar with a bundle manifest. */
    public Path plainJar(String name, String symbolicName) throws IOException {
        Path p = dir.resolve(name);
        Manifest mf = new Manifest();
        mf.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        mf.getMainAttributes().putValue("Bundle-ManifestVersion", "2");
        mf.getMainAttributes().putValue("Bundle-SymbolicName", symbolicName);
        mf.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(p), mf)) {
            out.putNextEntry(new JarEntry("data/payload.txt"));
            out.write(("payload of " + name).getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return p;
    }

    /** {@code in} signed by {@code alias} ("leaf": trusted through the CA; "foreign": untrusted). */
    public Path sign(Path in, String alias, String outName) throws Exception {
        KeyStore ks = load();
        PrivateKey key = (PrivateKey) ks.getKey(alias, PW);
        Certificate[] chain = ks.getCertificateChain(alias);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        JarSigner signer = new JarSigner.Builder(key, cf.generateCertPath(Arrays.asList(chain)))
                .digestAlgorithm("SHA-384").signatureAlgorithm("SHA384withECDSA").build();
        Path out = dir.resolve(outName);
        try (ZipFile zf = new ZipFile(in.toFile()); OutputStream os = Files.newOutputStream(out)) {
            signer.sign(zf, os);
        }
        return out;
    }
}
