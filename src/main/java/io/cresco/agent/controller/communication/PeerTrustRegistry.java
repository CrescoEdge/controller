package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cresco.library.security.CrescoIdentity;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Which key may answer for which Cresco identity, as this node learned it over authenticated discovery or issued it as
 * a regional CA. It is what binds a TLS certificate to an identity, outbound ({@link PeerIdentity}) and inbound
 * ({@link CrescoAuthorizationBroker}):
 *
 * <ul>
 *   <li><b>Identity bindings</b> {@code region/agent -> (tenant, leaf key)}. Per-node trust: the key a peer presented in
 *       a discovery exchange. Regional CA: the key this CA enrolled for the name. A name is bound to ONE key: a second key
 *       for a taken name is refused unless the operator lists the name in {@code peer_identity_rebind} (re-install,
 *       key rotation). A node keeps its key across restarts (CertificateManager persists it), so restarts re-present
 *       the same key.</li>
 *   <li><b>CA scopes</b> {@code region -> (tenant, CA certificate)}. Regional CA: a region CA vouches only for names in its
 *       own region and tenant; this node records its own CA and the CA of each region/global controller it completed a
 *       discovery exchange with. One CA per region; another CA for a recorded region is refused unless the region is
 *       listed in {@code peer_identity_rebind}.</li>
 *   <li><b>Bridge peers</b> region/global controllers that completed a REGION or GLOBAL discovery exchange with this node
 *       (they hold the region/global secret, leaf agents do not); only these may bridge into this broker. Revocable
 *       ({@code peer_bridge_revoke}) and pruned when not seen for {@code peer_bridge_prune_days} (default 30).</li>
 * </ul>
 *
 * <p>A certificate's identity counts ({@link #verifiedIdentity}) only when its leaf key is the one bound to the name it
 * names, or it was issued by the CA recorded for that name's region and tenant. Anything else -- a trusted chain naming
 * an identity nobody bound -- authenticates a TLS session but carries no Cresco identity.
 *
 * <p><b>Persistence.</b> With a file ({@link #open}) every change is written atomically (temp file, fsync, rename) as
 * JSON with an HMAC-SHA-384 over it, keyed by a random per-node key in {@code <file>.key} (owner-only). A file whose MAC
 * does not verify is moved aside and the registry runs <em>sealed</em>: nothing new is bound until an operator looks at it
 * (fail closed: a tampered file must not silently re-open names). The constructor without a file keeps it in memory.
 */
public final class PeerTrustRegistry {

    /** An identity binding or a CA scope. */
    static final class Binding {
        String tenant;
        String key;        // SHA-256 SPKI fingerprint of the bound leaf (identity) or CA
        String cert;       // CA scopes: the CA certificate, base64 DER
        long firstSeen;
        long lastSeen;
        String source;
    }

    static final class State {
        int version = 1;
        Map<String, Binding> identities = new LinkedHashMap<>();
        Map<String, Binding> cas = new LinkedHashMap<>();
        Map<String, Binding> bridges = new LinkedHashMap<>();
    }

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final Object lock = new Object();
    private State state = new State();
    private final Path file;
    private final Set<String> rebind = new HashSet<>();
    private final Set<String> revokedBridges = new HashSet<>();
    private long bridgePruneMs = 30L * 24 * 3600 * 1000;
    private boolean sealed;
    private Consumer<String> log = s -> { };

    /** In memory only (tests, and nodes that opt out of persistence). */
    public PeerTrustRegistry() {
        this.file = null;
    }

    private PeerTrustRegistry(Path file) {
        this.file = file;
    }

    /**
     * Open (or create) the persistent registry at {@code file}.
     *
     * @param rebind      names whose identity binding may be replaced ({@code region/agent}), or regions whose CA may be
     *                    replaced ({@code region}) -- the operator's deliberate re-enroll / rotation
     * @param revoke      bridge peers ({@code region/agent}) to remove and refuse
     * @param pruneDays   bridge peers not seen for this many days are dropped (0 = never)
     */
    public static PeerTrustRegistry open(Path file, Collection<String> rebind, Collection<String> revoke, int pruneDays, Consumer<String> log) {
        PeerTrustRegistry r = new PeerTrustRegistry(file);
        if (log != null) r.log = log;
        if (rebind != null) r.rebind.addAll(rebind);
        if (revoke != null) r.revokedBridges.addAll(revoke);
        r.bridgePruneMs = pruneDays <= 0 ? 0 : pruneDays * 24L * 3600 * 1000;
        r.load();
        synchronized (r.lock) {
            boolean changed = false;
            for (String n : r.revokedBridges) changed |= r.state.bridges.remove(n) != null;
            changed |= r.pruneBridges(System.currentTimeMillis());
            if (changed) r.save();
        }
        return r;
    }

    /** Operator settings for an in-memory registry (tests). */
    public PeerTrustRegistry withRebind(Collection<String> names) {
        synchronized (lock) { rebind.addAll(names); }
        return this;
    }

    public boolean isSealed() {
        synchronized (lock) { return sealed; }
    }

    private static String name(String region, String agent) {
        return region + "/" + agent;
    }

    // ---------------------------------------------------------------- identity bindings

    /** The outcome of {@link #bind}. */
    public static final class BindResult {
        public final boolean ok;
        public final String reason;
        BindResult(boolean ok, String reason) { this.ok = ok; this.reason = reason; }
    }

    /**
     * Bind {@code region/agent} (tenant {@code tenant}) to {@code leafKey}'s key. Allowed when the name is free, already
     * bound to this key (refreshes last-seen), or listed for rebind. A second key for a taken name, or a known name
     * re-appearing under another tenant, is refused.
     */
    public BindResult bind(String tenant, String region, String agent, java.security.PublicKey leafKey, String source) {
        if (!CrescoNames.isValid(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        String fp = fingerprint(leafKey.getEncoded());
        String n = name(region, agent);
        long now = System.currentTimeMillis();
        synchronized (lock) {
            if (sealed) return new BindResult(false, "the peer trust registry failed its integrity check and is sealed");
            Binding b = state.identities.get(n);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) {
                b.lastSeen = now;
                save();
                return new BindResult(true, "known key");
            }
            if (b != null && !rebind.contains(n)) {
                return new BindResult(false, (b.key.equals(fp) ? "tenant " + tenant + " differs from the bound tenant " + b.tenant
                        : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")")
                        + "; to re-enroll it deliberately list " + n + " in peer_identity_rebind");
            }
            if (b != null) log.accept("Identity " + n + " REBOUND from key " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                    + " (listed in peer_identity_rebind)");
            Binding nb = new Binding();
            nb.tenant = tenant; nb.key = fp; nb.firstSeen = now; nb.lastSeen = now; nb.source = source;
            state.identities.put(n, nb);
            save();
            return new BindResult(true, b == null ? "new binding" : "rebound");
        }
    }

    /** Is {@code region/agent} free, or bound to this key? (Checks only; {@link #bind} records.) */
    public BindResult canBind(String tenant, String region, String agent, java.security.PublicKey leafKey) {
        if (!CrescoNames.isValid(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        String fp = fingerprint(leafKey.getEncoded());
        String n = name(region, agent);
        synchronized (lock) {
            if (sealed) return new BindResult(false, "the peer trust registry failed its integrity check and is sealed");
            Binding b = state.identities.get(n);
            if (b == null || (b.key.equals(fp) && b.tenant.equals(tenant)) || rebind.contains(n)) return new BindResult(true, "free");
            return new BindResult(false, (b.key.equals(fp) ? "tenant differs from the bound tenant " + b.tenant
                    : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ")")
                    + "; to re-enroll it deliberately list " + n + " in peer_identity_rebind");
        }
    }

    /**
     * Pin the leaf a peer presented in an authenticated discovery exchange to the identity discovery reported. The leaf
     * must name exactly that identity. A taken name is not re-pinned to another key (see {@link #bind}).
     *
     * @return null when pinned, else why not
     */
    public String pin(String region, String agent, X509Certificate leaf) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return "invalid identity name " + region + "/" + agent;
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null || !region.equals(id.getRegion()) || !agent.equals(id.getAgent())) {
            return "the certificate names " + (id == null ? "no valid Cresco identity" : ("region=" + id.getRegion() + ", agent=" + id.getAgent()))
                    + ", not region=" + region + ", agent=" + agent;
        }
        BindResult r = bind(id.getTenant(), region, agent, leaf.getPublicKey(), "discovery");
        return r.ok ? null : r.reason;
    }

    /** The key fingerprint bound to {@code peer}, or null. */
    public String pinnedKey(PeerIdentity.Expected peer) {
        if (peer == null) return null;
        synchronized (lock) {
            Binding b = state.identities.get(name(peer.region, peer.agent));
            return b == null ? null : b.key;
        }
    }

    // ---------------------------------------------------------------- CA scopes

    /**
     * Record {@code ca} as the CA for {@code region} (tenant {@code tenant}): it may vouch for names in that region and
     * tenant only. The CA certificate must itself name that region (OU) and tenant (O). One CA per region; a different
     * CA for a recorded region is refused unless the region is listed in {@code peer_identity_rebind}.
     *
     * @return null when recorded, else why not
     */
    public String trustCa(X509Certificate ca, String region, String tenant, String source) {
        if (ca == null || !CrescoNames.isValid(region) || !CrescoNames.isValid(tenant)) return "invalid CA scope " + tenant + "/" + region;
        String[] scope = caScopeOf(ca);
        if (scope == null || !region.equals(scope[0]) || !tenant.equals(scope[1])) {
            return "the CA certificate '" + ca.getSubjectX500Principal().getName() + "' does not name region " + region + ", tenant " + tenant;
        }
        String fp = fingerprint(ca.getPublicKey().getEncoded());
        long now = System.currentTimeMillis();
        synchronized (lock) {
            if (sealed) return "the peer trust registry failed its integrity check and is sealed";
            Binding b = state.cas.get(region);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) {
                b.lastSeen = now;
                save();
                return null;
            }
            if (b != null && !rebind.contains(region)) {
                return "region " + region + " already has a recorded CA (" + PeerIdentity.shortFp(b.key) + "); to replace it list "
                        + region + " in peer_identity_rebind";
            }
            if (b != null) log.accept("CA for region " + region + " REBOUND (listed in peer_identity_rebind)");
            Binding nb = new Binding();
            nb.tenant = tenant; nb.key = fp; nb.firstSeen = now; nb.lastSeen = now; nb.source = source;
            try { nb.cert = Base64.getEncoder().encodeToString(ca.getEncoded()); } catch (Exception e) { return "unencodable CA"; }
            state.cas.put(region, nb);
            save();
            return null;
        }
    }

    /** {region, tenant} a CA certificate names (OU, O), when both are valid names. */
    static String[] caScopeOf(X509Certificate ca) {
        try {
            javax.naming.ldap.LdapName ln = new javax.naming.ldap.LdapName(ca.getSubjectX500Principal().getName());
            String ou = null, o = null;
            for (javax.naming.ldap.Rdn rdn : ln.getRdns()) {
                if (rdn.size() != 1 || !(rdn.getValue() instanceof String)) return null;
                if ("OU".equalsIgnoreCase(rdn.getType())) { if (ou != null) return null; ou = (String) rdn.getValue(); }
                else if ("O".equalsIgnoreCase(rdn.getType())) { if (o != null) return null; o = (String) rdn.getValue(); }
            }
            return (CrescoNames.isValid(ou) && CrescoNames.isValid(o)) ? new String[]{ou, o} : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** The CA certificates recorded for regions (to add to a truststore after a restart). */
    public List<X509Certificate> recordedCas() {
        List<X509Certificate> out = new ArrayList<>();
        synchronized (lock) {
            for (Binding b : state.cas.values()) {
                X509Certificate c = decode(b.cert);
                if (c != null) out.add(c);
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- verification

    /**
     * The identity {@code chain} may be trusted to carry (chain already PKIX-validated, leaf first), or null. The leaf
     * must name a valid identity, and either its key is the one bound to that name, or it was issued (signed, issuer DN)
     * by the CA recorded for that name's region and tenant.
     */
    public CrescoIdentity verifiedIdentity(X509Certificate[] chain) {
        return verifiedIdentity(chain, new String[1]);
    }

    /** As {@link #verifiedIdentity(X509Certificate[])}; {@code why[0]} receives the reason when it returns null. */
    public CrescoIdentity verifiedIdentity(X509Certificate[] chain, String[] why) {
        if (chain == null || chain.length == 0 || chain[0] == null) { why[0] = "no certificate"; return null; }
        X509Certificate leaf = chain[0];
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) { why[0] = "the certificate names no valid Cresco identity"; return null; }
        String fp = fingerprint(leaf.getPublicKey().getEncoded());
        Binding ident, ca;
        synchronized (lock) {
            ident = state.identities.get(name(id.getRegion(), id.getAgent()));
            ca = state.cas.get(id.getRegion());
        }
        if (ident != null) {
            if (ident.key.equals(fp) && ident.tenant.equals(id.getTenant())) return id;
            why[0] = "the certificate names " + id.getAgentPath() + " but carries a different key than the one bound to it ("
                    + PeerIdentity.shortFp(ident.key) + ")";
            return null;
        }
        if (ca != null && ca.tenant.equals(id.getTenant())) {
            X509Certificate caCert = decode(ca.cert);
            if (caCert != null && leaf.getIssuerX500Principal().equals(caCert.getSubjectX500Principal())) {
                try {
                    leaf.verify(caCert.getPublicKey());
                    return id;
                } catch (Exception e) {
                    why[0] = "the certificate names " + id.getAgentPath() + " but was not signed by region " + id.getRegion() + "'s CA";
                    return null;
                }
            }
            why[0] = "the certificate names " + id.getAgentPath() + " but was not issued by region " + id.getRegion() + "'s CA";
            return null;
        }
        why[0] = "no key or CA is recorded for " + id.getAgentPath() + " (tenant " + id.getTenant() + ")";
        return null;
    }

    // ---------------------------------------------------------------- bridge peers

    /** Record a controller that may bridge into this node's broker (or refresh its last-seen time). */
    public void allowBridgePeer(String region, String agent) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return;
        String n = name(region, agent);
        long now = System.currentTimeMillis();
        synchronized (lock) {
            if (revokedBridges.contains(n) || sealed) return;
            Binding b = state.bridges.get(n);
            if (b == null) {
                b = new Binding();
                b.firstSeen = now;
                b.source = "discovery";
                state.bridges.put(n, b);
            }
            b.lastSeen = now;
            save();
        }
    }

    /** Stop accepting bridges from {@code region/agent} (until it is allowed again by a discovery exchange). */
    public boolean revokeBridgePeer(String region, String agent) {
        synchronized (lock) {
            boolean removed = state.bridges.remove(name(region, agent)) != null;
            if (removed) save();
            return removed;
        }
    }

    /** Is {@code id} a controller recorded by {@link #allowBridgePeer} (not revoked, not pruned)? */
    public boolean isBridgePeer(CrescoIdentity id) {
        if (id == null || !CrescoNames.isValid(id.getRegion()) || !CrescoNames.isValid(id.getAgent())) return false;
        String n = name(id.getRegion(), id.getAgent());
        synchronized (lock) {
            if (revokedBridges.contains(n)) return false;
            Binding b = state.bridges.get(n);
            if (b == null) return false;
            if (bridgePruneMs > 0 && System.currentTimeMillis() - b.lastSeen > bridgePruneMs) {
                state.bridges.remove(n);
                save();
                return false;
            }
            return true;
        }
    }

    /** Note that a recorded bridge peer is in use (keeps it from being pruned). */
    public void touchBridgePeer(CrescoIdentity id) {
        if (id == null) return;
        synchronized (lock) {
            Binding b = state.bridges.get(name(id.getRegion(), id.getAgent()));
            if (b != null && System.currentTimeMillis() - b.lastSeen > 3_600_000L) {   // at most one write an hour
                b.lastSeen = System.currentTimeMillis();
                save();
            }
        }
    }

    private boolean pruneBridges(long now) {
        if (bridgePruneMs <= 0) return false;
        return state.bridges.values().removeIf(b -> now - b.lastSeen > bridgePruneMs);
    }

    /** Tests: pretend a bridge peer was last seen {@code ms} ago. */
    void backdateBridgePeer(String region, String agent, long ms) {
        synchronized (lock) {
            Binding b = state.bridges.get(name(region, agent));
            if (b != null) { b.lastSeen = System.currentTimeMillis() - ms; save(); }
        }
    }

    int pinCount() { synchronized (lock) { return state.identities.size(); } }

    int bridgePeerCount() { synchronized (lock) { return state.bridges.size(); } }

    // ---------------------------------------------------------------- persistence

    private Path keyFile() { return file.resolveSibling(file.getFileName() + ".key"); }

    private byte[] macKey() throws Exception {
        Path k = keyFile();
        if (Files.exists(k)) return Base64.getDecoder().decode(Files.readString(k, StandardCharsets.US_ASCII).trim());
        byte[] key = new byte[48];
        new SecureRandom().nextBytes(key);
        Files.createDirectories(k.getParent());
        Path tmp = k.resolveSibling(k.getFileName() + ".tmp");
        Files.writeString(tmp, Base64.getEncoder().encodeToString(key), StandardCharsets.US_ASCII);
        ownerOnly(tmp);
        Files.move(tmp, k, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        return key;
    }

    private static void ownerOnly(Path p) {
        try { Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-------")); } catch (Exception ignore) { }
    }

    private static String hmac(byte[] key, String payload) throws Exception {
        Mac m = Mac.getInstance("HmacSHA384");
        m.init(new SecretKeySpec(key, "HmacSHA384"));
        return Base64.getEncoder().encodeToString(m.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class Envelope { String payload; String hmac; }

    private void load() {
        if (file == null) return;
        synchronized (lock) {
            try {
                if (!Files.exists(file)) { macKey(); return; }
                Envelope e = GSON.fromJson(Files.readString(file, StandardCharsets.UTF_8), Envelope.class);
                byte[] key = Files.exists(keyFile()) ? macKey() : null;
                if (e == null || e.payload == null || e.hmac == null || key == null
                        || !MessageDigest.isEqual(Base64.getDecoder().decode(e.hmac), Base64.getDecoder().decode(hmac(key, e.payload)))) {
                    throw new SecurityException("integrity check failed");
                }
                State s = GSON.fromJson(e.payload, State.class);
                if (s == null || s.version != 1) throw new SecurityException("unknown format");
                if (s.identities == null) s.identities = new LinkedHashMap<>();
                if (s.cas == null) s.cas = new LinkedHashMap<>();
                if (s.bridges == null) s.bridges = new LinkedHashMap<>();
                state = s;
            } catch (Exception ex) {
                Path aside = file.resolveSibling(file.getFileName() + ".rejected-" + System.currentTimeMillis());
                try { Files.move(file, aside); } catch (Exception ignore) { }
                sealed = true;
                state = new State();
                log.accept("PEER TRUST REGISTRY REJECTED (" + ex.getMessage() + "): moved to " + aside + "; no new identity, CA or"
                        + " bridge binding is accepted until an operator restores or removes it (fail closed)");
            }
        }
    }

    private void save() {
        if (file == null || sealed) return;
        try {
            String payload = GSON.toJson(state);
            Envelope e = new Envelope();
            e.payload = payload;
            e.hmac = hmac(macKey(), payload);
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ch.write(java.nio.ByteBuffer.wrap(GSON.toJson(e).getBytes(StandardCharsets.UTF_8)));
                ch.force(true);
            }
            ownerOnly(tmp);
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception ex) {
            log.accept("peer trust registry not saved: " + ex.getMessage());
        }
    }

    // ---------------------------------------------------------------- helpers

    static String fingerprint(byte[] spki) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(spki));
        } catch (Exception e) {
            return "unavailable";
        }
    }

    private static X509Certificate decode(String b64) {
        if (b64 == null) return null;
        try {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(b64)));
        } catch (Exception e) {
            return null;
        }
    }
}
