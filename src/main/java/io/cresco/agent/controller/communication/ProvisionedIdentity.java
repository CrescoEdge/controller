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
 *   <li>{@code trust_anchors}: a PEM bundle of the fabric's ROOT certificate(s) (region CAs are intermediates presented in
 *       each node's chain; an anchor that is not a self-signed root must carry an explicit scope).</li>
 *   <li>{@code revocation_file} (optional): the operator's revocations -- X.509 CRLs (PEM) issued by the PKI, and/or a deny
 *       list: {@code key:<SHA-256 of a SubjectPublicKeyInfo>}, {@code cert:<SHA-256 of a certificate>},
 *       {@code name:<region>/<agent>}, {@code #} comments.</li>
 * </ul>
 *
 * <p><b>Identity names</b> (what the broker and the registry use): the leaf's subject {@code CN=<agent>, OU=<region>,
 * O=<tenant>} (read strictly, {@link CrescoNames#identityOf}); a SAN URI {@code cresco:node:<tenant>/<region>/<agent>}, when
 * present, must name the same identity. A SAN URI {@code cresco:role:controller} marks a CONTROLLER (a region or global
 * controller: it may bridge, answer discovery, and send revocation lists); without it the node is an agent. A SAN URI
 * {@code cresco:role:global} (with the controller role) marks THE global controller. Every CA on a path must allow the leaf's
 * tenant, region and roles by its EXPLICIT scope ({@link #scopeOf}); only an unscoped self-signed anchor (the root) issues any.
 *
 * <p>The chain must validate (PKIX: signatures, CA basic constraints and path length) to a trust anchor, and every
 * certificate on the path, the anchor included, must be valid now and not revoked by the operator.
 */
public final class ProvisionedIdentity {

    public static final String URI_NODE = "cresco:node:", URI_ROLE = "cresco:role:", ROLE_CONTROLLER = "controller";
    /** The fabric's GLOBAL controller (also a controller): only it answers GLOBAL discovery and is a region's parent. */
    public static final String ROLE_GLOBAL = "global", ROLE_NODE = "node";
    /** A CA's explicit scope: {@code cresco:ca:<tenant>/<region>} ({@code <tenant>/*}: every region; {@code *}: any). */
    public static final String URI_CA = "cresco:ca:";
    /** The roles a CA may issue: {@code cresco:ca-roles:<node,controller,global>}. */
    public static final String URI_CA_ROLES = "cresco:ca-roles:";

    final PrivateKey key;
    final X509Certificate[] chain;
    final CrescoIdentity identity;
    final boolean controller, global;
    final List<X509Certificate> anchors;
    final long keystoreMtime, passwordMtime, anchorsMtime;

    private ProvisionedIdentity(PrivateKey key, X509Certificate[] chain, CrescoIdentity identity, boolean controller,
                                List<X509Certificate> anchors, long keystoreMtime, long passwordMtime, long anchorsMtime) {
        this.key = key; this.chain = chain; this.identity = identity; this.controller = controller; this.anchors = anchors;
        this.global = controller && isGlobal(chain[0]);
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
        if (!matches(chain[0], key)) {
            throw new ProvisioningException("identity_keystore " + ks + ": the private key does not match its certificate");
        }
        String rev = revocations == null ? null : revocations.refusal(chain);
        if (rev != null) throw new ProvisioningException("this node's certificate " + describe(chain[0]) + " is REVOKED by the operator (" + rev + ")");
        String why = validate(chain, anchors, now, revocations);
        if (why != null) throw new ProvisioningException("this node's certificate " + describe(chain[0]) + " " + why);
        CrescoIdentity id = CrescoNames.identityOf(chain[0]);
        try {
            return new ProvisionedIdentity(key, chain, id, isController(chain[0]), anchors, mtime(ks), mtime(pw), mtime(ta));
        } catch (IOException e) {
            throw new ProvisioningException(e.getMessage());
        }
    }

    /** Is {@code signer} the private half of {@code leaf}'s key (checked by signing a probe)? */
    private static boolean matches(X509Certificate leaf, PrivateKey signer) throws ProvisioningException {
        try {
            String alg = io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(signer.getAlgorithm());
            java.security.Signature s = java.security.Signature.getInstance(alg);
            s.initSign(signer);
            byte[] probe = "cresco-provisioned-key-check".getBytes(StandardCharsets.UTF_8);
            s.update(probe);
            byte[] sig = s.sign();
            java.security.Signature v = java.security.Signature.getInstance(alg);
            v.initVerify(leaf.getPublicKey());
            v.update(probe);
            return v.verify(sig);
        } catch (Exception e) {
            throw new ProvisioningException("the identity key cannot be checked against its certificate: " + e.getMessage());
        }
    }

    static long mtime(Path p) throws IOException { return Files.getLastModifiedTime(p).toMillis(); }

    /**
     * Refuse (the D6 rules) a missing file, a symbolic link, a file owned by another user than this node's (or root), one
     * others can write or (for a secret) read, and one in a directory -- the parent or any directory above it -- that another
     * user owns or that group or others can write (they could rename another file in its place). A world-writable directory
     * with the sticky bit owned by root (/tmp) is allowed above the parent. Fails closed where POSIX owner and mode cannot be
     * read. Checked at start and at every reload tick.
     */
    static void checkFile(Path p, boolean secret, String what) throws ProvisioningException {
        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            throw new ProvisioningException(what + " " + p + " is not a regular file (missing, or a symbolic link)");
        }
        String me = System.getProperty("user.name");
        try {
            java.nio.file.attribute.PosixFileAttributes a = Files.readAttributes(p, java.nio.file.attribute.PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            String owner = a.owner().getName();
            if (!owner.equals(me) && !owner.equals("root")) {
                throw new ProvisioningException(what + " " + p + " is owned by " + owner + ", not by this node's user " + me + " (or root)");
            }
            Set<PosixFilePermission> perms = a.permissions();
            if (perms.contains(PosixFilePermission.GROUP_WRITE) || perms.contains(PosixFilePermission.OTHERS_WRITE)) {
                throw new ProvisioningException(what + " " + p + " is writable by group or others; make it owner-only (chmod 600)");
            }
            if (secret && (perms.contains(PosixFilePermission.GROUP_READ) || perms.contains(PosixFilePermission.OTHERS_READ))) {
                throw new ProvisioningException(what + " " + p + " is readable by group or others; make it owner-only (chmod 600)");
            }
            Path parent = p.toAbsolutePath().getParent();
            boolean first = true;
            for (Path d = parent == null ? null : parent.toRealPath(); d != null; d = d.getParent(), first = false) {
                java.nio.file.attribute.PosixFileAttributes da = Files.readAttributes(d, java.nio.file.attribute.PosixFileAttributes.class);
                String dOwner = da.owner().getName();
                if (!dOwner.equals(me) && !dOwner.equals("root")) {
                    throw new ProvisioningException(what + " " + p + ": its directory " + d + " is owned by " + dOwner + ", not by " + me + " or root");
                }
                Set<PosixFilePermission> dp = da.permissions();
                if (dp.contains(PosixFilePermission.GROUP_WRITE) || dp.contains(PosixFilePermission.OTHERS_WRITE)) {
                    if (!first && "root".equals(dOwner) && sticky(d)) continue;     // /tmp: no one renames another's entry
                    throw new ProvisioningException(what + " " + p + ": its directory " + d + " is writable by group or others"
                            + " (the file could be replaced); make it writable by its owner only");
                }
            }
        } catch (UnsupportedOperationException e) {
            throw new ProvisioningException(what + " " + p + ": this file system has no POSIX owner and mode, so the file cannot be checked");
        } catch (IOException e) {
            throw new ProvisioningException(what + " " + p + " cannot be checked (" + e.getMessage() + ")");
        }
    }

    private static boolean sticky(Path d) {
        try { return (((Integer) Files.getAttribute(d, "unix:mode")) & 01000) != 0; } catch (Exception e) { return false; }
    }

    /** Every certificate in the PEM (or DER) bundle {@code ta}: CA certificates only. */
    static List<X509Certificate> readAnchors(Path ta) throws ProvisioningException {
        checkFile(ta, false, "trust_anchors");
        List<X509Certificate> out = new ArrayList<>();
        try (InputStream in = Files.newInputStream(ta)) {
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) out.add((X509Certificate) c);
        } catch (Exception e) {
            throw new ProvisioningException("trust_anchors " + ta + " is unreadable: " + e.getMessage());
        }
        if (out.isEmpty()) throw new ProvisioningException("trust_anchors " + ta + " holds no certificate");
        for (X509Certificate c : out) {
            if (c.getBasicConstraints() < 0) throw new ProvisioningException("trust_anchors " + ta + ": " + describe(c) + " is not a CA certificate");
            if (scopeOf(c, true) == null) throw new ProvisioningException("trust_anchors " + ta + ": " + describe(c) + " is neither a root (self-signed,"
                    + " unscoped) nor a CA with an explicit Cresco scope (SAN " + URI_CA + "<tenant>/<region> and " + URI_CA_ROLES + "<roles>): give"
                    + " trust_anchors the fabric's ROOT only; region CAs are presented in each node's chain (a PKI made by cresco-pki.sh before"
                    + " 2026-11-04 must be re-made: see pre-provisioned-identity.md, Migration)");
        }
        return out;
    }

    /** A chain judged against the anchors: the validated path (leaf first, the ANCHOR last), or why there is none. */
    public static final class Validated {
        public final String refusal;
        public final List<X509Certificate> path;
        Validated(String refusal, List<X509Certificate> path) { this.refusal = refusal; this.path = path; }
    }

    /** Why {@code chain} (leaf first, as presented) does not validate to {@code anchors} now, or null (no revocations). */
    public static String validate(X509Certificate[] chain, Collection<X509Certificate> anchors, Date now) {
        return validate(chain, anchors, now, null);
    }

    /**
     * Why {@code chain} (leaf first, as presented) does not validate to {@code anchors} now, or null: {@link #validatePath},
     * then {@link #pathRefusal} over the whole path -- every certificate, the anchor included, valid now and not revoked by
     * {@code rev}.
     */
    public static String validate(X509Certificate[] chain, Collection<X509Certificate> anchors, Date now, Revocations rev) {
        Validated v = validatePath(chain, anchors, now);
        return v.refusal != null ? v.refusal : pathRefusal(v.path, now, rev);
    }

    /**
     * The path of {@code chain} (leaf first, as presented) to one of {@code anchors}: PKIX (signatures, CA basic constraints,
     * path length, key usage) from the leaf to an anchor through the presented intermediates; the leaf names a valid Cresco
     * identity (subject, and the SAN node URI when present); and EVERY CA on the path, the anchor included, may issue that
     * identity and its roles ({@link #scopeOf}: fail closed -- only an unscoped self-signed anchor, the fabric root, issues
     * anything; any other CA issues only what its explicit scope names, and nothing without one). Validity and revocation
     * are {@link #pathRefusal}'s (they change with time and the operator's file; this does not).
     */
    public static Validated validatePath(X509Certificate[] chain, Collection<X509Certificate> anchors, Date now) {
        if (chain == null || chain.length == 0 || chain[0] == null) return new Validated("is missing", null);
        if (anchors == null || anchors.isEmpty()) return new Validated("cannot be checked: no trust anchors", null);
        X509Certificate leaf = chain[0];
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) return new Validated("names no valid Cresco identity (subject CN=<agent>, OU=<region>, O=<tenant>)", null);
        String san = nodeUri(leaf);
        if (san != null && !san.equals(id.getTenant() + "/" + id.getRegion() + "/" + id.getAgent())) {
            return new Validated("names " + id.getTenant() + "/" + id.getRegion() + "/" + id.getAgent() + " in its subject but " + san + " in its SAN", null);
        }
        Set<String> roles = rolesOf(leaf);
        if (roles.contains(ROLE_GLOBAL) && !roles.contains(ROLE_CONTROLLER)) {
            return new Validated("carries " + URI_ROLE + ROLE_GLOBAL + " without " + URI_ROLE + ROLE_CONTROLLER, null);
        }
        List<X509Certificate> path = new ArrayList<>();
        for (X509Certificate c : chain) {
            if (c == null) break;
            if (anchors.contains(c)) break;                 // the presented anchor itself is not part of the PKIX path
            path.add(c);
        }
        try {
            Set<TrustAnchor> tas = new HashSet<>();
            for (X509Certificate a : anchors) tas.add(new TrustAnchor(a, null));
            PKIXParameters params = new PKIXParameters(tas);
            params.setRevocationEnabled(false);              // revocation: the operator's file and the signed lists (pathRefusal)
            params.setDate(now);
            CertPath cp = CertificateFactory.getInstance("X.509").generateCertPath(path);
            java.security.cert.PKIXCertPathValidatorResult r =
                    (java.security.cert.PKIXCertPathValidatorResult) CertPathValidator.getInstance("PKIX").validate(cp, params);
            X509Certificate anchor = r.getTrustAnchor().getTrustedCert();
            if (anchor == null) return new Validated("validates to an anchor without a certificate", null);
            List<X509Certificate> full = new ArrayList<>(path);
            full.add(anchor);
            for (int i = 1; i < full.size(); i++) {
                X509Certificate ca = full.get(i);
                CaScope scope = scopeOf(ca, i == full.size() - 1);
                if (scope == null) {
                    return new Validated("was issued under " + describe(ca) + ", a CA without an explicit Cresco scope (SAN " + URI_CA
                            + "<tenant>/<region> and " + URI_CA_ROLES + "<roles>): such a CA issues nothing this fabric accepts", null);
                }
                String no = scope.refusal(id, roles);
                if (no != null) return new Validated("was issued under " + describe(ca) + ", " + no, null);
            }
            return new Validated(null, java.util.Collections.unmodifiableList(full));
        } catch (java.security.cert.CertPathValidatorException e) {
            return new Validated("does not validate to the fabric's trust anchors (" + e.getMessage() + ")", null);
        } catch (Exception e) {
            return new Validated("cannot be validated (" + e.getMessage() + ")", null);
        }
    }

    /**
     * Why the validated {@code path} (leaf first, anchor last) is refused now, or null: a certificate on it -- the anchor
     * included (PKIX never checks an anchor's own validity) -- is outside its validity, or the operator's {@code rev} lists
     * one of them (a revoked region CA stops every leaf it issued, whether it was presented or reached as an anchor).
     */
    public static String pathRefusal(List<X509Certificate> path, Date now, Revocations rev) {
        if (path == null || path.isEmpty()) return "has no validated path";
        for (int i = 0; i < path.size(); i++) {
            X509Certificate c = path.get(i);
            try { c.checkValidity(now); } catch (Exception e) {
                return i == 0 ? "is not valid now (" + e.getMessage() + ")"
                        : "was issued under " + describe(c) + (i == path.size() - 1 ? ", a trust anchor" : "") + " that is not valid now (" + e.getMessage() + ")";
            }
        }
        String r = rev == null ? null : rev.refusal(path.toArray(new X509Certificate[0]));
        return r == null ? null : "is REVOKED by the operator (" + r + ")";
    }

    /** What a CA may issue: a tenant (null: any), a region (null: any) and roles. */
    static final class CaScope {
        static final Set<String> ALL_ROLES = Set.of(ROLE_NODE, ROLE_CONTROLLER, ROLE_GLOBAL);
        static final CaScope ROOT = new CaScope(null, null, ALL_ROLES);
        final String tenant, region;
        final Set<String> roles;
        CaScope(String tenant, String region, Set<String> roles) { this.tenant = tenant; this.region = region; this.roles = roles; }

        String refusal(CrescoIdentity id, Set<String> leafRoles) {
            if ((tenant != null && !tenant.equals(id.getTenant())) || (region != null && !region.equals(id.getRegion()))) {
                return "a CA for " + this + ", not for " + id.getTenant() + "/" + id.getRegion();
            }
            for (String r : leafRoles) if (!roles.contains(r)) return "a CA for " + this + ", which may not issue the role " + r;
            return null;
        }

        @Override public String toString() {
            return (tenant == null ? "*" : tenant + "/" + (region == null ? "*" : region)) + " (roles " + String.join(",", new java.util.TreeSet<>(roles)) + ")";
        }
    }

    /**
     * What CA certificate {@code ca} may issue, or null (nothing). FAIL CLOSED: a CA carrying explicit Cresco scope SAN URIs
     * -- exactly one {@code cresco:ca:<tenant>/<region>} ({@code <tenant>/*} every region of a tenant, {@code *} any) and
     * exactly one {@code cresco:ca-roles:<node,controller,global>} -- may issue that; an ANCHOR ({@code anchor}) that is
     * self-signed and carries no scope URI is a fabric root and may issue anything; every other CA (an intermediate that
     * names only a tenant, an enterprise CA, a region CA that is not scoped explicitly, a malformed scope) issues nothing.
     */
    static CaScope scopeOf(X509Certificate ca, boolean anchor) {
        if (ca == null || ca.getBasicConstraints() < 0) return null;
        String scope = null, roles = null;
        int scopes = 0, roleLists = 0;
        for (String u : sanUris(ca)) {
            if (u.startsWith(URI_CA)) { scopes++; scope = u.substring(URI_CA.length()); }
            else if (u.startsWith(URI_CA_ROLES)) { roleLists++; roles = u.substring(URI_CA_ROLES.length()); }
        }
        if (scopes == 0 && roleLists == 0) return anchor && selfSigned(ca) ? CaScope.ROOT : null;
        if (scopes != 1 || roleLists != 1 || roles.isEmpty()) return null;
        Set<String> rs = new HashSet<>();
        for (String r : roles.split(",", -1)) {
            if (!CaScope.ALL_ROLES.contains(r)) return null;
            rs.add(r);
        }
        if ("*".equals(scope)) return new CaScope(null, null, Set.copyOf(rs));
        int sl = scope.indexOf('/');
        if (sl <= 0) return null;
        String t = scope.substring(0, sl), r = scope.substring(sl + 1);
        if (!CrescoNames.isValidTenant(t)) return null;
        if ("*".equals(r)) return new CaScope(t, null, Set.copyOf(rs));
        return CrescoNames.isValid(r) ? new CaScope(t, r, Set.copyOf(rs)) : null;
    }

    /** Is {@code c} self-issued and self-signed (a root)? */
    static boolean selfSigned(X509Certificate c) {
        if (!c.getSubjectX500Principal().equals(c.getIssuerX500Principal())) return false;
        try { c.verify(c.getPublicKey()); return true; } catch (Exception e) { return false; }
    }

    /** The roles a node certificate claims: node, and controller / global by its SAN role URIs. */
    static Set<String> rolesOf(X509Certificate leaf) {
        Set<String> out = new HashSet<>();
        out.add(ROLE_NODE);
        Set<String> u = sanUris(leaf);
        if (u.contains(URI_ROLE + ROLE_CONTROLLER)) out.add(ROLE_CONTROLLER);
        if (u.contains(URI_ROLE + ROLE_GLOBAL)) out.add(ROLE_GLOBAL);
        return out;
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

    /** Is {@code c} the fabric's GLOBAL controller (SAN URIs {@code cresco:role:global} and {@code cresco:role:controller})? */
    public static boolean isGlobal(X509Certificate c) {
        Set<String> u = sanUris(c);
        return u.contains(URI_ROLE + ROLE_GLOBAL) && u.contains(URI_ROLE + ROLE_CONTROLLER);
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
        /**
         * Per CRL issuer (its DN): the newest CRL's nextUpdate and CRL number. A CRL past its nextUpdate (plus the grace) makes
         * the revocation status of that issuer's certificates unknown: they are refused (fail closed) until the file is
         * refreshed. CRL numbers only go up ({@link #rollbackRefusal}): an older CRL put back cannot un-revoke anything.
         */
        final java.util.Map<String, Long> nextUpdates = new java.util.HashMap<>();
        final java.util.Map<String, java.math.BigInteger> numbers = new java.util.HashMap<>();
        /** The DNs of every CA this file's CRLs may come from (the anchors, and CA certificates in the file that chain to them). */
        final Set<String> caNames = new HashSet<>();
        final long mtime;
        final int crls;
        final long graceMs;

        private Revocations(long mtime, int crls, long graceMs) { this.mtime = mtime; this.crls = crls; this.graceMs = graceMs; }

        static final Revocations NONE = new Revocations(-1, 0, 0);

        /** Parse {@code file}; CRLs must be signed by a CA in {@code cas}; no grace past a CRL's nextUpdate. */
        public static Revocations read(Path file, Collection<X509Certificate> cas) throws ProvisioningException {
            return read(file, cas, 0);
        }

        /**
         * Parse {@code file}; CRLs must be signed by a CA in {@code cas}, carry a nextUpdate and a CRL number (RFC 5280: both
         * are required of a conforming CRL issuer), and are honoured until {@code graceMs} past their nextUpdate.
         */
        public static Revocations read(Path file, Collection<X509Certificate> cas, long graceMs) throws ProvisioningException {
            if (file == null) return NONE;
            String text;
            try {
                checkFile(file, false, "revocation_file");
                text = Files.readString(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new ProvisioningException("revocation_file " + file + " is unreadable: " + e.getMessage());
            }
            // issuing CAs carried in the file (PEM certificates: the region CAs, which are not anchors) sign their own CRLs; one
            // is taken only when it chains (signature, CA) to the anchors
            List<X509Certificate> issuers = new ArrayList<>(cas);
            List<X509Certificate> carried = new ArrayList<>();
            StringBuilder rest = new StringBuilder();
            for (int i0 = 0; ; ) {
                int b = text.indexOf("-----BEGIN CERTIFICATE-----", i0);
                if (b < 0) { rest.append(text.substring(i0)); break; }
                rest.append(text, i0, b);
                int e = text.indexOf("-----END CERTIFICATE-----", b);
                if (e < 0) throw new ProvisioningException("revocation_file " + file + ": an unterminated certificate");
                e += "-----END CERTIFICATE-----".length();
                try {
                    carried.add((X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(
                            new ByteArrayInputStream(text.substring(b, e).getBytes(StandardCharsets.US_ASCII))));
                } catch (Exception ex) {
                    throw new ProvisioningException("revocation_file " + file + ": an unreadable certificate (" + ex.getMessage() + ")");
                }
                i0 = e;
            }
            text = rest.toString();
            for (boolean grew = true; grew; ) {
                grew = false;
                for (X509Certificate c : carried) {
                    if (issuers.contains(c) || c.getBasicConstraints() < 0) continue;
                    for (X509Certificate up : issuers) {
                        if (!up.getSubjectX500Principal().equals(c.getIssuerX500Principal())) continue;
                        try { c.verify(up.getPublicKey()); issuers.add(c); grew = true; break; } catch (Exception ignore) { }
                    }
                }
            }
            int crlCount = 0;
            Revocations r;
            try { r = new Revocations(mtime(file), 0, graceMs); } catch (IOException e) { throw new ProvisioningException(e.getMessage()); }
            for (X509Certificate ca : issuers) r.caNames.add(ca.getSubjectX500Principal().getName());
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
                for (X509Certificate ca : issuers) {
                    if (!ca.getSubjectX500Principal().equals(crl.getIssuerX500Principal())) continue;
                    try { crl.verify(ca.getPublicKey()); signed = true; break; } catch (Exception ignore) { }
                }
                if (!signed) throw new ProvisioningException("revocation_file " + file + ": a CRL from '" + crl.getIssuerX500Principal().getName()
                        + "' is not signed by a trust anchor, nor by a CA certificate in the file that chains to one");
                String issuerDn = crl.getIssuerX500Principal().getName();
                if (crl.getNextUpdate() == null) {
                    throw new ProvisioningException("revocation_file " + file + ": the CRL from '" + issuerDn + "' has no nextUpdate, so its"
                            + " freshness cannot be judged (an issuer must set it; cresco-pki.sh crl --days N does)");
                }
                java.math.BigInteger number = crlNumber(crl);
                if (number == null) {
                    throw new ProvisioningException("revocation_file " + file + ": the CRL from '" + issuerDn + "' has no CRL number, so an"
                            + " older CRL could not be told from a newer one (an issuer must set it; cresco-pki.sh crl does)");
                }
                java.math.BigInteger had = r.numbers.get(issuerDn);
                if (had == null || number.compareTo(had) > 0) {
                    r.numbers.put(issuerDn, number);
                    r.nextUpdates.put(issuerDn, crl.getNextUpdate().getTime());
                }
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
            Revocations out = new Revocations(r.mtime, crlCount, graceMs);
            out.keys.addAll(r.keys); out.certs.addAll(r.certs); out.names.addAll(r.names); out.serials.addAll(r.serials);
            out.nextUpdates.putAll(r.nextUpdates); out.numbers.putAll(r.numbers); out.caNames.addAll(r.caNames);
            return out;
        }

        /** The CRL number extension (2.5.29.20: an OCTET STRING wrapping a DER INTEGER), or null when absent or unreadable. */
        static java.math.BigInteger crlNumber(X509CRL crl) {
            byte[] ext = crl.getExtensionValue("2.5.29.20");
            if (ext == null) return null;
            try {
                byte[] inner = derContents(ext, 0, (byte) 0x04);
                if (inner == null) return null;
                byte[] integer = derContents(inner, 0, (byte) 0x02);
                return integer == null || integer.length == 0 ? null : new java.math.BigInteger(integer);
            } catch (RuntimeException e) {
                return null;
            }
        }

        /** The contents of the DER element of {@code tag} at {@code off} of {@code b} (definite lengths only), or null. */
        private static byte[] derContents(byte[] b, int off, byte tag) {
            if (b.length < off + 2 || b[off] != tag) return null;
            int len = b[off + 1] & 0xff, p = off + 2;
            if (len > 0x80) {
                int n = len & 0x7f;
                if (n > 4 || b.length < p + n) return null;
                len = 0;
                for (int i = 0; i < n; i++) len = (len << 8) | (b[p++] & 0xff);
            } else if (len == 0x80) {
                return null;
            }
            if (len < 0 || b.length < p + len) return null;
            return java.util.Arrays.copyOfRange(b, p, p + len);
        }

        /** The CRL issuers (DN) whose newest CRL is past its nextUpdate plus the grace at {@code now}: what is refused for it. */
        public java.util.List<String> staleIssuers(long now) {
            java.util.List<String> out = new ArrayList<>();
            for (java.util.Map.Entry<String, Long> e : nextUpdates.entrySet()) if (now > e.getValue() + graceMs) out.add(e.getKey());
            return out;
        }

        /** The earliest nextUpdate of this file's CRLs (ms), or -1 when it holds none. */
        public long earliestNextUpdate() {
            long m = -1;
            for (long v : nextUpdates.values()) if (m < 0 || v < m) m = v;
            return m;
        }

        /** The CRL number per issuer DN (what rollback protection records). */
        public java.util.Map<String, java.math.BigInteger> crlNumbers() { return java.util.Collections.unmodifiableMap(numbers); }

        /**
         * Why this file may NOT replace revocations whose highest CRL numbers were {@code recorded} (issuer DN -> number), or
         * null: a CRL number went down (an older CRL put back), or the CRL of an issuer that is still trusted is gone (that
         * would un-revoke every serial it listed). An issuer no longer trusted at all (retired from the anchors) may go.
         */
        public String rollbackRefusal(java.util.Map<String, java.math.BigInteger> recorded) {
            if (recorded == null) return null;
            for (java.util.Map.Entry<String, java.math.BigInteger> e : recorded.entrySet()) {
                java.math.BigInteger now = numbers.get(e.getKey());
                if (now == null) {
                    if (caNames.contains(e.getKey())) return "the CRL of '" + e.getKey() + "' (number " + e.getValue() + " was in force) is"
                            + " missing from revocation_file: removing it would un-revoke what it lists";
                } else if (now.compareTo(e.getValue()) < 0) {
                    return "the CRL of '" + e.getKey() + "' went BACKWARDS: number " + now + " replaces " + e.getValue() + " (an older CRL"
                            + " put back would un-revoke what the newer one listed)";
                }
            }
            return null;
        }

        private static boolean hex64(String s) { return s.matches("[0-9a-f]{64}"); }

        public boolean isEmpty() { return keys.isEmpty() && certs.isEmpty() && names.isEmpty() && serials.isEmpty(); }

        /** Why the operator's file revokes some certificate of {@code chain}, or null. */
        public String refusal(X509Certificate[] chain) {
            return refusal(chain, System.currentTimeMillis());
        }

        /**
         * As above at {@code now}: also a certificate whose issuer's CRL in this file is out of date (past its nextUpdate plus
         * the grace) -- its revocation status is unknown, so it is refused until the file is refreshed (fail closed).
         */
        public String refusal(X509Certificate[] chain, long now) {
            if (chain == null || (isEmpty() && nextUpdates.isEmpty())) return null;
            for (int i = 0; i < chain.length; i++) {
                X509Certificate c = chain[i];
                if (c == null) continue;
                Long next = nextUpdates.isEmpty() ? null : nextUpdates.get(c.getIssuerX500Principal().getName());
                if (next != null && now > next + graceMs) {
                    return "the CRL of its issuer '" + c.getIssuerX500Principal().getName() + "' is out of date (nextUpdate "
                            + new Date(next) + "): its revocation status is unknown until revocation_file is refreshed"
                            + (i > 0 ? " (an issuer)" : "");
                }
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

        /**
         * As {@link #refusal(X509Certificate[], long)} for one certificate known only by reference (a message signer's, from
         * its attestation or trust-bundle entry): its key ({@code keyFp}), its SHA-256 ({@code certFp}), its issuer's DN and
         * {@code <issuer DN>|<serial hex>}. O(1): a few set lookups.
         */
        public String refusal(String keyFp, String certFp, String issuerDn, String issuerSerial, long now) {
            if (isEmpty() && nextUpdates.isEmpty()) return null;
            Long next = issuerDn == null || nextUpdates.isEmpty() ? null : nextUpdates.get(issuerDn);
            if (next != null && now > next + graceMs) {
                return "the CRL of its issuer '" + issuerDn + "' is out of date (nextUpdate " + new Date(next) + "): its revocation"
                        + " status is unknown until revocation_file is refreshed";
            }
            if (keyFp != null && keys.contains(keyFp)) return "its key " + PeerIdentity.shortFp(keyFp) + " is listed";
            if (certFp != null && certs.contains(certFp)) return "the certificate " + PeerIdentity.shortFp(certFp) + " is listed";
            if (issuerSerial != null && serials.contains(issuerSerial)) {
                return "serial " + issuerSerial.substring(issuerSerial.lastIndexOf('|') + 1) + " of '" + issuerDn + "' is on its CRL";
            }
            return null;
        }

        public String summary() { return keys.size() + " key(s), " + certs.size() + " certificate(s), " + names.size() + " name(s), "
                + serials.size() + " CRL serial(s) from " + crls + " CRL(s)"
                + (nextUpdates.isEmpty() ? "" : ", next CRL update due " + new Date(earliestNextUpdate())); }
    }
}
