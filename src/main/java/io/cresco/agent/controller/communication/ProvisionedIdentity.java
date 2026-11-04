package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A PRE-PROVISIONED identity: the node's certificate chain and key, issued by the operator's PKI, and the fabric's trust
 * anchors -- loaded, never generated, enrolled or learned on first use ({@code security_profile=production}, or whenever
 * {@code identity_keystore} is set).
 *
 * <ul>
 *   <li>{@code identity_keystore}: a PKCS12 keystore holding ONE private key entry: the node's key and its chain (leaf first;
 *       intermediates may follow). {@code identity_keystore_password_file}: a file holding its password (first line).</li>
 *   <li>{@code trust_anchors}: a PEM bundle of the fabric's CA certificates (its root, and region intermediates when a
 *       verifier should accept them as anchors).</li>
 *   <li>{@code revocation_file} (optional): the operator's revocations -- X.509 CRLs (PEM) issued by the PKI, and/or a deny
 *       list: {@code key:<SHA-256 of a SubjectPublicKeyInfo>}, {@code cert:<SHA-256 of a certificate>},
 *       {@code name:<region>/<agent>}, {@code #} comments.</li>
 * </ul>
 *
 * <p><b>Identity names</b> (what the broker and the registry use): the leaf's subject {@code CN=<agent>, OU=<region>,
 * O=<tenant>} (read strictly, {@link CrescoNames#identityOf}); a SAN URI {@code cresco:node:<tenant>/<region>/<agent>}, when
 * present, must name the same identity. A SAN URI {@code cresco:role:controller} marks a CONTROLLER (a region or global
 * controller: it may bridge, answer discovery, and send revocation lists); without it the node is an agent. A CA whose
 * subject names a region and tenant ({@code OU}, {@code O}) may issue only names of that region and tenant; a CA naming
 * neither (the fabric root) may issue any.
 *
 * <p>The chain must validate (PKIX: signatures, validity, CA basic constraints and path length) to a trust anchor, now.
 */
public final class ProvisionedIdentity {

    public static final String URI_NODE = "cresco:node:", URI_ROLE = "cresco:role:", ROLE_CONTROLLER = "controller";

    final PrivateKey key;
    final X509Certificate[] chain;
    final CrescoIdentity identity;
    final boolean controller;
    final List<X509Certificate> anchors;
    final long keystoreMtime, passwordMtime, anchorsMtime;

    private ProvisionedIdentity(PrivateKey key, X509Certificate[] chain, CrescoIdentity identity, boolean controller,
                                List<X509Certificate> anchors, long keystoreMtime, long passwordMtime, long anchorsMtime) {
        this.key = key; this.chain = chain; this.identity = identity; this.controller = controller; this.anchors = anchors;
        this.keystoreMtime = keystoreMtime; this.passwordMtime = passwordMtime; this.anchorsMtime = anchorsMtime;
    }

    /** A provisioning that cannot be used: the node must not start (or, on a reload, keeps its current identity). */
    public static final class ProvisioningException extends Exception {
        public ProvisioningException(String m) { super(m); }
        public ProvisioningException(String m, Throwable t) { super(m, t); }
    }

    /** Is this node pre-provisioned ({@code security_profile=production}, or {@code identity_keystore} set)? */
    public static boolean enabled(io.cresco.library.plugin.Config config) {
        String ks = config.getStringParam("identity_keystore");
        return (ks != null && !ks.isBlank()) || "production".equalsIgnoreCase(String.valueOf(config.getStringParam("security_profile", "")));
    }

    static Path path(io.cresco.library.plugin.Config config, String key) throws ProvisioningException {
        String v = config.getStringParam(key);
        if (v == null || v.isBlank()) throw new ProvisioningException(key + " is not set (a pre-provisioned node needs identity_keystore,"
                + " identity_keystore_password_file and trust_anchors)");
        return Paths.get(v.trim());
    }

    /** Load and validate this node's provisioned identity from {@code config}; {@code revocations} judges the leaf. */
    public static ProvisionedIdentity load(io.cresco.library.plugin.Config config, Revocations revocations) throws ProvisioningException {
        Path ks = path(config, "identity_keystore"), pw = path(config, "identity_keystore_password_file"), ta = path(config, "trust_anchors");
        return load(ks, pw, ta, revocations, new Date());
    }

    public static ProvisionedIdentity load(Path ks, Path pw, Path ta, Revocations revocations, Date now) throws ProvisioningException {
        checkFile(ks, true, "identity_keystore");
        checkFile(pw, true, "identity_keystore_password_file");
        checkFile(ta, false, "trust_anchors");
        List<X509Certificate> anchors = readAnchors(ta);
        char[] pass;
        try { pass = Files.readAllLines(pw, StandardCharsets.UTF_8).stream().findFirst().orElse("").trim().toCharArray(); }
        catch (IOException e) { throw new ProvisioningException("identity_keystore_password_file " + pw + " is unreadable: " + e.getMessage()); }
        KeyStore store;
        try (InputStream in = Files.newInputStream(ks)) {
            store = KeyStore.getInstance("PKCS12");
            store.load(in, pass);
        } catch (Exception e) {
            throw new ProvisioningException("identity_keystore " + ks + " cannot be opened with its password file (" + e.getMessage() + ")");
        }
        PrivateKey key = null;
        X509Certificate[] chain = null;
        try {
            for (String alias : Collections.list(store.aliases())) {
                if (!store.isKeyEntry(alias)) continue;
                if (key != null) throw new ProvisioningException("identity_keystore " + ks + " holds more than one private key; a node has one identity");
                Key k = store.getKey(alias, pass);
                Certificate[] c = store.getCertificateChain(alias);
                if (!(k instanceof PrivateKey) || c == null || c.length == 0) continue;
                key = (PrivateKey) k;
                chain = new X509Certificate[c.length];
                for (int i = 0; i < c.length; i++) chain[i] = (X509Certificate) c[i];
            }
        } catch (ProvisioningException e) {
            throw e;
        } catch (Exception e) {
            throw new ProvisioningException("identity_keystore " + ks + " is unreadable: " + e.getMessage());
        }
        if (key == null) throw new ProvisioningException("identity_keystore " + ks + " holds no private key with a certificate chain");
        if (!chain[0].getPublicKey().equals(publicKeyOf(key, chain[0]))) {
            throw new ProvisioningException("identity_keystore " + ks + ": the private key does not match its certificate");
        }
        String why = validate(chain, anchors, now);
        if (why != null) throw new ProvisioningException("this node's certificate " + describe(chain[0]) + " " + why);
        CrescoIdentity id = CrescoNames.identityOf(chain[0]);
        String rev = revocations == null ? null : revocations.refusal(chain);
        if (rev != null) throw new ProvisioningException("this node's certificate " + describe(chain[0]) + " is REVOKED by the operator (" + rev + ")");
        try {
            return new ProvisionedIdentity(key, chain, id, isController(chain[0]), anchors, mtime(ks), mtime(pw), mtime(ta));
        } catch (IOException e) {
            throw new ProvisioningException(e.getMessage());
        }
    }

    /** The public key that goes with {@code key} (checked against the leaf by signing a probe). */
    private static java.security.PublicKey publicKeyOf(PrivateKey key, X509Certificate leaf) throws ProvisioningException {
        try {
            String alg = io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(key.getAlgorithm());
            java.security.Signature s = java.security.Signature.getInstance(alg);
            s.initSign(key);
            byte[] probe = "cresco-provisioned-key-check".getBytes(StandardCharsets.UTF_8);
            s.update(probe);
            byte[] sig = s.sign();
            java.security.Signature v = java.security.Signature.getInstance(alg);
            v.initVerify(leaf.getPublicKey());
            v.update(probe);
            return v.verify(sig) ? leaf.getPublicKey() : null;
        } catch (Exception e) {
            throw new ProvisioningException("the identity key cannot be checked against its certificate: " + e.getMessage());
        }
    }

    static long mtime(Path p) throws IOException { return Files.getLastModifiedTime(p).toMillis(); }

    /** Refuse a missing file, a symlink, one others can write, or (for a secret) one others can read. */
    static void checkFile(Path p, boolean secret, String what) throws ProvisioningException {
        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            throw new ProvisioningException(what + " " + p + " is not a regular file (missing, or a symbolic link)");
        }
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(p, LinkOption.NOFOLLOW_LINKS);
            if (perms.contains(PosixFilePermission.GROUP_WRITE) || perms.contains(PosixFilePermission.OTHERS_WRITE)) {
                throw new ProvisioningException(what + " " + p + " is writable by group or others; make it owner-only (chmod 600)");
            }
            if (secret && (perms.contains(PosixFilePermission.GROUP_READ) || perms.contains(PosixFilePermission.OTHERS_READ))) {
                throw new ProvisioningException(what + " " + p + " is readable by group or others; make it owner-only (chmod 600)");
            }
        } catch (UnsupportedOperationException | IOException ignore) {
            // not a POSIX file system: nothing more to check
        }
    }

    /** Every certificate in the PEM (or DER) bundle {@code ta}: CA certificates only. */
    static List<X509Certificate> readAnchors(Path ta) throws ProvisioningException {
        List<X509Certificate> out = new ArrayList<>();
        try (InputStream in = Files.newInputStream(ta)) {
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) out.add((X509Certificate) c);
        } catch (Exception e) {
            throw new ProvisioningException("trust_anchors " + ta + " is unreadable: " + e.getMessage());
        }
        if (out.isEmpty()) throw new ProvisioningException("trust_anchors " + ta + " holds no certificate");
        for (X509Certificate c : out) {
            if (c.getBasicConstraints() < 0) throw new ProvisioningException("trust_anchors " + ta + ": " + describe(c) + " is not a CA certificate");
        }
        return out;
    }

    /**
     * Why {@code chain} (leaf first, as presented) does not validate to {@code anchors} now, or null: a PKIX path (signatures,
     * validity, CA basic constraints, path length, key usage) from the leaf to an anchor, through the presented intermediates;
     * the leaf names a valid Cresco identity (subject, and the SAN node URI when present); and every CA on the path that names
     * a region and tenant names the leaf's.
     */
    public static String validate(X509Certificate[] chain, Collection<X509Certificate> anchors, Date now) {
        if (chain == null || chain.length == 0 || chain[0] == null) return "is missing";
        if (anchors == null || anchors.isEmpty()) return "cannot be checked: no trust anchors";
        X509Certificate leaf = chain[0];
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) return "names no valid Cresco identity (subject CN=<agent>, OU=<region>, O=<tenant>)";
        String san = nodeUri(leaf);
        if (san != null && !san.equals(id.getTenant() + "/" + id.getRegion() + "/" + id.getAgent())) {
            return "names " + id.getTenant() + "/" + id.getRegion() + "/" + id.getAgent() + " in its subject but " + san + " in its SAN";
        }
        List<X509Certificate> path = new ArrayList<>();
        for (X509Certificate c : chain) {
            if (c == null) break;
            if (anchors.contains(c)) break;                 // the presented anchor itself is not part of the path
            path.add(c);
        }
        try {
            Set<TrustAnchor> tas = new HashSet<>();
            for (X509Certificate a : anchors) tas.add(new TrustAnchor(a, null));
            PKIXParameters params = new PKIXParameters(tas);
            params.setRevocationEnabled(false);              // revocation: the operator's file and the signed lists
            params.setDate(now);
            CertPath cp = CertificateFactory.getInstance("X.509").generateCertPath(path);
            java.security.cert.PKIXCertPathValidatorResult r =
                    (java.security.cert.PKIXCertPathValidatorResult) CertPathValidator.getInstance("PKIX").validate(cp, params);
            // scope: a CA naming a region and tenant issues only that region's and tenant's names
            List<X509Certificate> cas = new ArrayList<>(path.subList(1, path.size()));
            if (r.getTrustAnchor().getTrustedCert() != null) cas.add(r.getTrustAnchor().getTrustedCert());
            for (X509Certificate ca : cas) {
                String[] scope = PeerTrustRegistry.caScopeOf(ca);
                if (scope != null && (!scope[0].equals(id.getRegion()) || !scope[1].equals(id.getTenant()))) {
                    return "was issued under " + describe(ca) + ", a CA for " + scope[1] + "/" + scope[0] + ", not for " + id.getTenant() + "/" + id.getRegion();
                }
            }
            return null;
        } catch (java.security.cert.CertPathValidatorException e) {
            return "does not validate to the fabric's trust anchors (" + e.getMessage() + ")";
        } catch (Exception e) {
            return "cannot be validated (" + e.getMessage() + ")";
        }
    }

    /** The SAN {@code cresco:node:} identity ({@code tenant/region/agent}) of {@code c}, or null. */
    static String nodeUri(X509Certificate c) {
        for (String u : sanUris(c)) if (u.startsWith(URI_NODE)) return u.substring(URI_NODE.length());
        return null;
    }

    /** Does {@code c} carry the SAN URI {@code cresco:role:controller}? */
    public static boolean isController(X509Certificate c) {
        return sanUris(c).contains(URI_ROLE + ROLE_CONTROLLER);
    }

    static Set<String> sanUris(X509Certificate c) {
        Set<String> out = new LinkedHashSet<>();
        try {
            Collection<List<?>> sans = c.getSubjectAlternativeNames();
            if (sans != null) for (List<?> e : sans) if (e.size() >= 2 && Integer.valueOf(6).equals(e.get(0))) out.add(String.valueOf(e.get(1)));
        } catch (Exception ignore) { }
        return out;
    }

    static String describe(X509Certificate c) {
        return "'" + c.getSubjectX500Principal().getName() + "' (serial " + c.getSerialNumber().toString(16) + ", valid "
                + c.getNotBefore() + " - " + c.getNotAfter() + ")";
    }

    // ---------------------------------------------------------------- the operator's revocation file

    /**
     * The operator's revocations ({@code revocation_file}): CRLs (their issuers must validate to the anchors) and a deny list.
     * Immutable; re-read (a new instance) when the file changes.
     */
    public static final class Revocations {
        final Set<String> keys = new HashSet<>(), certs = new HashSet<>(), names = new HashSet<>();
        final Set<String> serials = new HashSet<>();               // <issuer DN>|<serial hex>
        final long mtime;
        final int crls;

        private Revocations(long mtime, int crls) { this.mtime = mtime; this.crls = crls; }

        static final Revocations NONE = new Revocations(-1, 0);

        /** Parse {@code file}; CRLs must be signed by a CA in {@code cas}. */
        public static Revocations read(Path file, Collection<X509Certificate> cas) throws ProvisioningException {
            if (file == null) return NONE;
            String text;
            try {
                checkFile(file, false, "revocation_file");
                text = Files.readString(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new ProvisioningException("revocation_file " + file + " is unreadable: " + e.getMessage());
            }
            int crlCount = 0;
            Revocations r;
            try { r = new Revocations(mtime(file), 0); } catch (IOException e) { throw new ProvisioningException(e.getMessage()); }
            StringBuilder deny = new StringBuilder();
            int i = 0;
            while (i < text.length()) {
                int b = text.indexOf("-----BEGIN X509 CRL-----", i);
                if (b < 0) { deny.append(text.substring(i)); break; }
                deny.append(text, i, b);
                int e = text.indexOf("-----END X509 CRL-----", b);
                if (e < 0) throw new ProvisioningException("revocation_file " + file + ": an unterminated CRL");
                e += "-----END X509 CRL-----".length();
                X509CRL crl;
                try {
                    crl = (X509CRL) CertificateFactory.getInstance("X.509").generateCRL(new ByteArrayInputStream(text.substring(b, e).getBytes(StandardCharsets.US_ASCII)));
                } catch (Exception ex) {
                    throw new ProvisioningException("revocation_file " + file + ": an unreadable CRL (" + ex.getMessage() + ")");
                }
                boolean signed = false;
                for (X509Certificate ca : cas) {
                    if (!ca.getSubjectX500Principal().equals(crl.getIssuerX500Principal())) continue;
                    try { crl.verify(ca.getPublicKey()); signed = true; break; } catch (Exception ignore) { }
                }
                if (!signed) throw new ProvisioningException("revocation_file " + file + ": a CRL from '" + crl.getIssuerX500Principal().getName()
                        + "' is not signed by a CA of the fabric's trust anchors");
                if (crl.getRevokedCertificates() != null) {
                    for (X509CRLEntry entry : crl.getRevokedCertificates()) {
                        r.serials.add(crl.getIssuerX500Principal().getName() + "|" + entry.getSerialNumber().toString(16));
                    }
                }
                crlCount++;
                i = e;
            }
            for (String line : deny.toString().split("\n")) {
                String t = line.trim();
                int hash = t.indexOf('#');
                if (hash >= 0) t = t.substring(0, hash).trim();
                if (t.isEmpty()) continue;
                String v = t.toLowerCase(Locale.ROOT);
                if (v.startsWith("key:") && hex64(v.substring(4))) r.keys.add(v.substring(4));
                else if (v.startsWith("cert:") && hex64(v.substring(5))) r.certs.add(v.substring(5));
                else if (hex64(v)) r.keys.add(v);
                else if (t.startsWith("name:") && t.indexOf('/') > 5) r.names.add(t.substring(5).trim());
                else throw new ProvisioningException("revocation_file " + file + ": unknown entry '" + t + "' (key:<sha-256>, cert:<sha-256>,"
                            + " name:<region>/<agent>, or a PEM CRL)");
            }
            Revocations out = new Revocations(r.mtime, crlCount);
            out.keys.addAll(r.keys); out.certs.addAll(r.certs); out.names.addAll(r.names); out.serials.addAll(r.serials);
            return out;
        }

        private static boolean hex64(String s) { return s.matches("[0-9a-f]{64}"); }

        public boolean isEmpty() { return keys.isEmpty() && certs.isEmpty() && names.isEmpty() && serials.isEmpty(); }

        /** Why the operator's file revokes some certificate of {@code chain}, or null. */
        public String refusal(X509Certificate[] chain) {
            if (chain == null || isEmpty()) return null;
            for (int i = 0; i < chain.length; i++) {
                X509Certificate c = chain[i];
                if (c == null) continue;
                String kfp = PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded());
                if (keys.contains(kfp)) return "its key " + PeerIdentity.shortFp(kfp) + " is listed" + (i > 0 ? " (an issuer)" : "");
                try {
                    String cfp = CertificateManager.certFingerprint(c);
                    if (certs.contains(cfp)) return "the certificate " + PeerIdentity.shortFp(cfp) + " is listed" + (i > 0 ? " (an issuer)" : "");
                } catch (Exception ignore) { }
                if (serials.contains(c.getIssuerX500Principal().getName() + "|" + c.getSerialNumber().toString(16))) {
                    return "serial " + c.getSerialNumber().toString(16) + " of '" + c.getIssuerX500Principal().getName() + "' is on its CRL"
                            + (i > 0 ? " (an issuer)" : "");
                }
                if (i == 0) {
                    CrescoIdentity id = CrescoNames.identityOf(c);
                    if (id != null && names.contains(id.getRegion() + "/" + id.getAgent())) return "the name " + id.getRegion() + "/" + id.getAgent() + " is listed";
                }
            }
            return null;
        }

        public String summary() { return keys.size() + " key(s), " + certs.size() + " certificate(s), " + names.size() + " name(s), "
                + serials.size() + " CRL serial(s) from " + crls + " CRL(s)"; }
    }
}
