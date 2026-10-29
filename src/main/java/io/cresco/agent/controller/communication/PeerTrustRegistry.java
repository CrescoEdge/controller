package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cresco.library.security.CrescoIdentity;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Which key may answer for which Cresco identity, as this node learned it over authenticated discovery or issued it as
 * a regional CA. It binds TLS certificates to identities, outbound ({@link PeerIdentity}) and inbound
 * ({@link CrescoAuthorizationBroker}).
 *
 * <ul>
 *   <li><b>Identity bindings</b> {@code region/agent -> (tenant, key, role)}. A responder (a controller that agents or
 *       other controllers join) binds each joiner's name to the key it presented, first come: a second key, or another
 *       tenant, for a bound name is refused unless the operator authorises ONE rebind of that name. A client binds the
 *       controller it joined; a controller's key there <em>follows</em> authenticated discovery (a re-installed controller
 *       is accepted, logged), because a joiner cannot tell a re-install from anything else and refusing would cut every
 *       agent off. Names bound as controllers (and this node's own) are reserved: no agent is ever bound to one.</li>
 *   <li><b>CA scopes</b> {@code region -> (tenant, CA)} for the regional CA: a region CA vouches only for its own region and
 *       tenant. One CA per region at a responder (rebind as above); a client follows its region's CA.</li>
 *   <li><b>Bridge peers</b>: controllers that completed a REGION or GLOBAL discovery exchange; revocable, pruned when unseen
 *       for {@code bridgePruneDays}.</li>
 * </ul>
 *
 * <p><b>Rebind</b> is one-shot: each entry ({@code region/agent} or {@code region} for a CA, optionally {@code #tag}) from the
 * {@code peer_identity_rebind} setting or the operator file {@code <file>.rebind} (re-read when it changes, no restart)
 * replaces one binding and is then recorded as consumed; it never applies again. To rebind the same name later, add it
 * with a new tag ({@code rgA/a1#2}).
 *
 * <p><b>Storage.</b> An append-only log of JSON lines, each carrying an HMAC-SHA-384 chained over the previous line's MAC,
 * keyed by a random per-node key in {@code <file>.key}. Appends are fsynced before a bind is acknowledged; a log that grows
 * well past its live state is compacted (temp file, fsync, rename, directory fsync). Reads never take a lock or do I/O;
 * refreshing a known binding writes nothing; last-seen times are persisted in the background at most daily. Files are
 * created owner-only.
 * The MAC detects corruption and edits by anyone who cannot read the key file; it does NOT stop someone who can write
 * the node's data directory (they can read the key beside it).
 *
 * <p><b>Failures.</b> A torn last line (a crash mid-append; never acknowledged) is dropped. Any other line that does not
 * verify, or the log missing while its key says it was initialised, <em>seals</em> the registry: what verified is still
 * served (established peers keep working), nothing new is bound, and the seal is persisted ({@code <file>.sealed}), so a
 * restart does not lift it. Recovery is explicit: the operator writes {@code accept} (keep every record that parses,
 * re-sign them) or {@code reset} (start empty: every name, CA scope and bridge peer must be learned again, first come)
 * into {@code <file>.recover}; it is applied on the next refused bind or restart.
 */
public final class PeerTrustRegistry {

    /** An identity binding, a CA scope or a bridge peer (immutable: replaced, never mutated in place). */
    static final class Binding {
        final String tenant, key, cert, source, role;
        final long firstSeen, lastSeen, savedSeen;     // savedSeen: the last-seen time last written to the log
        Binding(String tenant, String key, String cert, String source, String role, long firstSeen, long lastSeen) {
            this(tenant, key, cert, source, role, firstSeen, lastSeen, lastSeen);
        }
        Binding(String tenant, String key, String cert, String source, String role, long firstSeen, long lastSeen, long savedSeen) {
            this.tenant = tenant; this.key = key; this.cert = cert; this.source = source; this.role = role;
            this.firstSeen = firstSeen; this.lastSeen = lastSeen; this.savedSeen = savedSeen;
        }
        Binding seen(long t) { return new Binding(tenant, key, cert, source, role, firstSeen, t, savedSeen); }
        Binding saved(long t) { return new Binding(tenant, key, cert, source, role, firstSeen, Math.max(lastSeen, t), t); }
    }

    public static final String ROLE_AGENT = "agent", ROLE_CONTROLLER = "controller", ROLE_SELF = "self";

    /** Registry settings. */
    public static final class Options {
        public Collection<String> rebind = List.of();
        public Collection<String> revoke = List.of();
        public int bridgePruneDays = 30;
        public int bindingPruneDays = 90;
    }

    /** One log record. */
    static final class Rec {
        String op;             // init | bind | ca | bridge | unbridge | unbind | seen | consume | noop
        String name, tenant, key, cert, source, role, token, kind;
        long t;
        String mac;
    }

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final FileAttribute<?> OWNER_ONLY = attr("rw-------");
    private static final FileAttribute<?> OWNER_DIR = attr("rwx------");
    private static final long DAY = 24L * 3600 * 1000;

    private final Map<String, Binding> identities = new ConcurrentHashMap<>();
    private final Map<String, Binding> cas = new ConcurrentHashMap<>();
    private final Map<String, Binding> bridges = new ConcurrentHashMap<>();
    private final Set<String> consumed = ConcurrentHashMap.newKeySet();
    private final Set<String> rebindConfig = ConcurrentHashMap.newKeySet();
    private final Set<String> rebindFile = ConcurrentHashMap.newKeySet();
    private final Set<String> revokedBridges = ConcurrentHashMap.newKeySet();

    private final Object writeLock = new Object();
    private final Object syncLock = new Object();
    private FileChannel channel;                                // the log, open for appends (with the write lock)
    private volatile long writtenSeq, syncedSeq;                // group commit: one fsync covers every write before it
    private final Path file;
    private volatile boolean sealed;
    private volatile String sealReason;
    private long bridgePruneMs = 30L * DAY, bindingPruneMs = 90L * DAY;
    private Consumer<String> log = s -> { };
    private byte[] macKey;
    private String lastMac = "";
    private long lines;
    private boolean needsNewline;
    private long rebindFileMtime = -1, recoverCheckedAt;
    private final ExecutorService background;

    /** In memory only (tests, and nodes that opt out of persistence). */
    public PeerTrustRegistry() {
        this.file = null;
        this.background = null;
    }

    private PeerTrustRegistry(Path file) {
        this.file = file;
        this.background = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "peer-trust-writer");
            t.setDaemon(true);
            return t;
        });
    }

    /** As {@link #open(Path, Options, Consumer)} with the bridge prune period and rebind/revoke lists. */
    public static PeerTrustRegistry open(Path file, Collection<String> rebind, Collection<String> revoke, int pruneDays, Consumer<String> log) {
        Options o = new Options();
        o.rebind = rebind == null ? List.of() : rebind;
        o.revoke = revoke == null ? List.of() : revoke;
        o.bridgePruneDays = pruneDays;
        return open(file, o, log);
    }

    /** Open (or create) the persistent registry at {@code file}. */
    public static PeerTrustRegistry open(Path file, Options o, Consumer<String> log) {
        PeerTrustRegistry r = new PeerTrustRegistry(file);
        if (log != null) r.log = log;
        r.rebindConfig.addAll(o.rebind);
        r.revokedBridges.addAll(o.revoke);
        r.bridgePruneMs = o.bridgePruneDays <= 0 ? 0 : o.bridgePruneDays * DAY;
        r.bindingPruneMs = o.bindingPruneDays <= 0 ? 0 : o.bindingPruneDays * DAY;
        synchronized (r.writeLock) {
            r.applyRecoveryIfAsked();
            r.load();
            r.reloadRebindFile();
            if (!r.sealed) {
                long now = System.currentTimeMillis();
                for (String n : r.revokedBridges) if (r.bridges.remove(n) != null) r.append(rec("unbridge", n));
                r.prune(now);
            }
        }
        r.durable();
        return r;
    }

    /** Operator settings for an in-memory registry (tests). */
    public PeerTrustRegistry withRebind(Collection<String> names) {
        rebindConfig.addAll(names);
        return this;
    }

    public boolean isSealed() { return sealed; }

    public String sealReason() { return sealReason; }

    private static String name(String region, String agent) { return region + "/" + agent; }

    // ---------------------------------------------------------------- identity bindings

    /** The outcome of a bind. */
    public static final class BindResult {
        public final boolean ok;
        public final String reason;
        BindResult(boolean ok, String reason) { this.ok = ok; this.reason = reason; }
    }

    /** Bind a joiner's name first come (strict; role agent). See {@link #bind(String, String, String, java.security.PublicKey, String, String, boolean)}. */
    public BindResult bind(String tenant, String region, String agent, java.security.PublicKey leafKey, String source) {
        return bind(tenant, region, agent, leafKey, source, ROLE_AGENT, false);
    }

    /**
     * Bind {@code region/agent} (tenant {@code tenant}) to {@code leafKey}.
     *
     * @param role   {@link #ROLE_AGENT}, {@link #ROLE_CONTROLLER} or {@link #ROLE_SELF}; controller and self names are reserved
     * @param follow true: a different key replaces the binding (a client following the controller it joined); false: first
     *               come, a different key is refused unless an unconsumed rebind for the name exists
     */
    public BindResult bind(String tenant, String region, String agent, java.security.PublicKey leafKey, String source, String role, boolean follow) {
        if (!CrescoNames.isValid(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        String fp = fingerprint(leafKey.getEncoded());
        String n = name(region, agent);
        long now = System.currentTimeMillis();
        Binding b = identities.get(n);
        if (b != null && b.key.equals(fp) && b.tenant.equals(tenant) && (ROLE_AGENT.equals(role) || role.equals(b.role))) {
            touch(identities, n, b, now, "id");                // a refresh: no I/O on this path
            return new BindResult(true, "known key");
        }
        BindResult result;
        synchronized (writeLock) {
            if (sealed && !tryRecover()) return new BindResult(false, sealedText());
            b = identities.get(n);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant) && (ROLE_AGENT.equals(role) || role.equals(b.role))) {
                return new BindResult(true, "known key");
            }
            String token = null;
            if (b != null && !follow) {
                token = rebindToken(n);
                if (token == null) {
                    return new BindResult(false, (b.key.equals(fp) ? "tenant " + tenant + " differs from the bound tenant " + b.tenant
                            : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")")
                            + "; if " + n + " was re-installed, authorise ONE rebind on this node: add the line '" + n + "' to "
                            + rebindFileHint() + " (no restart), or set peer_identity_rebind=" + n);
                }
            }
            String newRole = (b != null && !ROLE_AGENT.equals(b.role) && ROLE_AGENT.equals(role)) ? b.role : role;
            Rec r = rec("bind", n);
            r.tenant = tenant; r.key = fp; r.source = source; r.role = newRole; r.t = now;
            if (!append(r)) return new BindResult(false, "could not record the binding durably");
            if (token != null) {
                Rec c = rec("consume", n);
                c.token = token; c.t = now;
                append(c);
                consumed.add(token);
            }
            identities.put(n, new Binding(tenant, fp, null, source, newRole, b == null || token != null || follow ? now : b.firstSeen, now));
            if (b != null && !b.key.equals(fp)) {
                log.accept("Identity " + n + " REBOUND from key " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                        + (token != null ? " (operator rebind '" + token + "', now consumed)" : " (followed: the controller this node joined re-keyed)"));
            }
            result = new BindResult(true, b == null ? "new binding" : "rebound");
        }
        return durable() ? result : new BindResult(false, "could not record the binding durably");
    }

    /** Is {@code region/agent} free, or bound to this key? (Checks only.) */
    public BindResult canBind(String tenant, String region, String agent, java.security.PublicKey leafKey) {
        if (!CrescoNames.isValid(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        String fp = fingerprint(leafKey.getEncoded());
        String n = name(region, agent);
        Binding b = identities.get(n);
        if (sealed) {
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) return new BindResult(true, "known key (registry sealed)");
            synchronized (writeLock) { if (!tryRecover()) return new BindResult(false, sealedText()); }
            b = identities.get(n);
        }
        if (b == null || (b.key.equals(fp) && b.tenant.equals(tenant))) return new BindResult(true, "free");
        synchronized (writeLock) { if (rebindToken(n) != null) return new BindResult(true, "operator rebind pending"); }
        return new BindResult(false, (b.key.equals(fp) ? "tenant differs from the bound tenant " + b.tenant
                : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")")
                + "; if " + n + " was re-installed, authorise ONE rebind on this node: add the line '" + n + "' to " + rebindFileHint()
                + " (no restart), or set peer_identity_rebind=" + n);
    }

    /** Is {@code region/agent} a controller name (bound as a controller, or this node's own)? Agents are never bound to one. */
    public boolean isReserved(String region, String agent) {
        Binding b = identities.get(name(region, agent));
        return b != null && !ROLE_AGENT.equals(b.role);
    }

    /**
     * Pin the leaf a peer presented in an authenticated discovery exchange to the identity discovery reported (strict).
     *
     * @return null when pinned, else why not
     */
    public String pin(String region, String agent, X509Certificate leaf) {
        return pin(region, agent, leaf, ROLE_AGENT, false);
    }

    /** As {@link #pin(String, String, X509Certificate)} with a role and follow policy. */
    public String pin(String region, String agent, X509Certificate leaf, String role, boolean follow) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return "invalid identity name " + region + "/" + agent;
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null || !region.equals(id.getRegion()) || !agent.equals(id.getAgent())) {
            return "the certificate names " + (id == null ? "no valid Cresco identity" : ("region=" + id.getRegion() + ", agent=" + id.getAgent()))
                    + ", not region=" + region + ", agent=" + agent;
        }
        BindResult r = bind(id.getTenant(), region, agent, leaf.getPublicKey(), "discovery", role, follow);
        return r.ok ? null : r.reason;
    }

    /** The key fingerprint bound to {@code peer}, or null. */
    public String pinnedKey(PeerIdentity.Expected peer) {
        if (peer == null) return null;
        Binding b = identities.get(name(peer.region, peer.agent));
        return b == null ? null : b.key;
    }

    // ---------------------------------------------------------------- CA scopes

    /** Record {@code ca} for {@code region}, first come (a responder). */
    public String trustCa(X509Certificate ca, String region, String tenant, String source) {
        return trustCa(ca, region, tenant, source, false);
    }

    /**
     * Record {@code ca} as the CA for {@code region} (tenant {@code tenant}): it may vouch for names in that region and
     * tenant only. The CA certificate must name that region (OU) and tenant (O).
     *
     * @param follow true: a different CA replaces the recorded one (a client following the region it joined)
     * @return null when recorded, else why not
     */
    public String trustCa(X509Certificate ca, String region, String tenant, String source, boolean follow) {
        if (ca == null || !CrescoNames.isValid(region) || !CrescoNames.isValid(tenant)) return "invalid CA scope " + tenant + "/" + region;
        String[] scope = caScopeOf(ca);
        if (scope == null || !region.equals(scope[0]) || !tenant.equals(scope[1])) {
            return "the CA certificate '" + ca.getSubjectX500Principal().getName() + "' does not name region " + region + ", tenant " + tenant;
        }
        String fp = fingerprint(ca.getPublicKey().getEncoded());
        long now = System.currentTimeMillis();
        Binding b = cas.get(region);
        if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) {
            touch(cas, region, b, now, "ca");
            return null;
        }
        String cert;
        try { cert = Base64.getEncoder().encodeToString(ca.getEncoded()); } catch (Exception e) { return "unencodable CA"; }
        synchronized (writeLock) {
            if (sealed && !tryRecover()) return sealedText();
            b = cas.get(region);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) return null;
            String token = null;
            if (b != null && !follow) {
                token = rebindToken(region);
                if (token == null) {
                    return "region " + region + " already has a recorded CA (" + PeerIdentity.shortFp(b.key) + "); if its controller was"
                            + " re-installed, authorise ONE rebind on this node: add the line '" + region + "' to " + rebindFileHint();
                }
            }
            Rec r = rec("ca", region);
            r.tenant = tenant; r.key = fp; r.cert = cert; r.source = source; r.t = now;
            if (!append(r)) return "could not record the CA durably";
            if (token != null) {
                Rec c = rec("consume", region);
                c.token = token; c.t = now;
                append(c);
                consumed.add(token);
            }
            cas.put(region, new Binding(tenant, fp, cert, source, null, now, now));
            if (b != null) log.accept("CA for region " + region + " REBOUND" + (token != null ? " (operator rebind '" + token + "', now consumed)" : " (followed)"));
        }
        return durable() ? null : "could not record the CA durably";
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
        for (Binding b : cas.values()) {
            X509Certificate c = decode(b.cert);
            if (c != null) out.add(c);
        }
        return out;
    }

    // ---------------------------------------------------------------- verification (lock-free)

    /**
     * The identity {@code chain} may be trusted to carry (chain already PKIX-validated, leaf first), or null: the leaf names
     * a valid identity and either its key is the one bound to that name, or it was issued by the CA recorded for that name's
     * region and tenant.
     */
    public CrescoIdentity verifiedIdentity(X509Certificate[] chain) {
        return verifiedIdentity(chain, new String[1]);
    }

    public CrescoIdentity verifiedIdentity(X509Certificate[] chain, String[] why) {
        if (chain == null || chain.length == 0 || chain[0] == null) { why[0] = "no certificate"; return null; }
        X509Certificate leaf = chain[0];
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) { why[0] = "the certificate names no valid Cresco identity"; return null; }
        String fp = fingerprint(leaf.getPublicKey().getEncoded());
        Binding ident = identities.get(name(id.getRegion(), id.getAgent()));
        Binding ca = cas.get(id.getRegion());
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

    /** Record a controller that may bridge into this node's broker (or refresh it). */
    public void allowBridgePeer(String region, String agent) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return;
        String n = name(region, agent);
        if (revokedBridges.contains(n)) return;
        long now = System.currentTimeMillis();
        Binding b = bridges.get(n);
        if (b != null) { touch(bridges, n, b, now, "bridge"); return; }
        synchronized (writeLock) {
            if (sealed && !tryRecover()) return;
            if (bridges.containsKey(n)) return;
            Rec r = rec("bridge", n);
            r.t = now;
            if (append(r)) bridges.put(n, new Binding(null, null, null, "discovery", null, now, now));
        }
        durable();
    }

    /** Stop accepting bridges from {@code region/agent} (until a discovery exchange allows it again). */
    public boolean revokeBridgePeer(String region, String agent) {
        String n = name(region, agent);
        synchronized (writeLock) {
            if (bridges.remove(n) == null) return false;
            append(rec("unbridge", n));
        }
        durable();
        return true;
    }

    /** Is {@code id} a recorded bridge peer (not revoked, not expired)? Lock-free; no I/O. */
    public boolean isBridgePeer(CrescoIdentity id) {
        if (id == null || !CrescoNames.isValid(id.getRegion()) || !CrescoNames.isValid(id.getAgent())) return false;
        String n = name(id.getRegion(), id.getAgent());
        if (revokedBridges.contains(n)) return false;
        Binding b = bridges.get(n);
        return b != null && (bridgePruneMs <= 0 || System.currentTimeMillis() - b.lastSeen <= bridgePruneMs);
    }

    /** A recorded bridge peer is in use (keeps it from being pruned). In memory; persisted in the background, at most daily. */
    public void touchBridgePeer(CrescoIdentity id) {
        if (id == null) return;
        String n = name(id.getRegion(), id.getAgent());
        Binding b = bridges.get(n);
        if (b != null) touch(bridges, n, b, System.currentTimeMillis(), "bridge");
    }

    /** Tests: pretend a bridge peer was last seen {@code ms} ago. */
    void backdateBridgePeer(String region, String agent, long ms) {
        synchronized (writeLock) {
            String n = name(region, agent);
            Binding b = bridges.get(n);
            if (b == null) return;
            long t = System.currentTimeMillis() - ms;
            bridges.put(n, b.seen(t));
            Rec r = rec("seen", n);
            r.kind = "bridge"; r.t = t;
            append(r);
        }
    }

    int pinCount() { return identities.size(); }

    int bridgePeerCount() { return bridges.size(); }

    // ---------------------------------------------------------------- last-seen, pruning

    /** Refresh last-seen in memory; persist it in the background when the stored value is a day old. */
    private void touch(Map<String, Binding> map, String n, Binding b, long now, String kind) {
        if (now - b.lastSeen < 60_000L) return;
        boolean persist = file != null && background != null && now - b.savedSeen > DAY;
        map.put(n, persist ? b.saved(now) : b.seen(now));
        if (persist) {
            background.execute(() -> {
                synchronized (writeLock) {
                    if (sealed) return;
                    Rec r = rec("seen", n);
                    r.kind = kind; r.t = now;
                    append(r);
                }
            });
        }
    }

    private void prune(long now) {
        if (bridgePruneMs > 0) {
            for (Map.Entry<String, Binding> e : new ArrayList<>(bridges.entrySet())) {
                if (now - e.getValue().lastSeen > bridgePruneMs) { bridges.remove(e.getKey()); append(rec("unbridge", e.getKey())); }
            }
        }
        if (bindingPruneMs > 0) {
            for (Map.Entry<String, Binding> e : new ArrayList<>(identities.entrySet())) {
                if (ROLE_SELF.equals(e.getValue().role)) continue;
                if (now - e.getValue().lastSeen > bindingPruneMs) { identities.remove(e.getKey()); append(rec("unbind", e.getKey())); }
            }
        }
    }

    // ---------------------------------------------------------------- rebind tokens

    private Path rebindPath() { return file == null ? null : file.resolveSibling(file.getFileName() + ".rebind"); }

    private String rebindFileHint() { return file == null ? "peer_identity_rebind" : rebindPath().toString(); }

    /** An unconsumed rebind token for {@code n} (a name, or a region for its CA), or null. */
    private String rebindToken(String n) {
        reloadRebindFile();
        for (Set<String> src : List.of(rebindConfig, rebindFile)) {
            for (String t : src) {
                String bare = t.contains("#") ? t.substring(0, t.indexOf('#')) : t;
                if (bare.equals(n) && !consumed.contains(t)) return t;
            }
        }
        return null;
    }

    private void reloadRebindFile() {
        Path p = rebindPath();
        if (p == null) return;
        try {
            long m = Files.exists(p) ? Files.getLastModifiedTime(p).toMillis() : 0;
            if (m == rebindFileMtime) return;
            rebindFileMtime = m;
            rebindFile.clear();
            if (m != 0) {
                for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    String t = line.trim();
                    if (!t.isEmpty() && !t.startsWith("#")) rebindFile.add(t);
                }
            }
        } catch (IOException e) {
            log.accept("could not read " + p + ": " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------- recovery

    private Path sealPath() { return file.resolveSibling(file.getFileName() + ".sealed"); }

    private Path recoverPath() { return file.resolveSibling(file.getFileName() + ".recover"); }

    private Path keyPath() { return file.resolveSibling(file.getFileName() + ".key"); }

    private String sealedText() {
        return "the peer trust registry is SEALED (" + sealReason + "); established peers keep working, nothing new is bound."
                + (file == null ? "" : " To recover, write 'accept' (keep every record that parses) or 'reset' (start empty: every name"
                + " is learned again, first come) into " + recoverPath());
    }

    /** With the write lock: apply a pending recovery request (polled at most every 5 s). */
    private boolean tryRecover() {
        if (file == null) return false;
        long now = System.currentTimeMillis();
        if (now - recoverCheckedAt < 5_000L) return !sealed;
        recoverCheckedAt = now;
        if (!Files.exists(recoverPath())) return false;
        applyRecoveryIfAsked();
        if (!sealed) return true;
        return false;
    }

    private void applyRecoveryIfAsked() {
        if (file == null || !Files.exists(recoverPath())) return;
        String what;
        try { what = Files.readString(recoverPath(), StandardCharsets.UTF_8).trim().toLowerCase(java.util.Locale.ROOT); }
        catch (IOException e) { log.accept("could not read " + recoverPath() + ": " + e.getMessage()); return; }
        try {
            if (what.equals("reset")) {
                if (channel != null) { try { channel.close(); } catch (IOException ignore) { } channel = null; }
                Path aside = file.resolveSibling(file.getFileName() + ".reset-" + System.currentTimeMillis());
                if (Files.exists(file)) Files.move(file, aside);
                clearState();
                Files.deleteIfExists(sealPath());
                sealed = false;
                sealReason = null;
                macKey = newKey();
                writeKey(macKey, false);
                initLog();
                log.accept("PEER TRUST REGISTRY RESET by the operator (" + recoverPath() + "): every name, CA scope and bridge peer will be"
                        + " learned again, first come; the old log is kept at " + aside);
            } else if (what.equals("accept")) {
                List<Rec> recs = readRecords(false);
                clearState();
                for (Rec r : recs) apply(r);
                Files.deleteIfExists(sealPath());
                sealed = false;
                sealReason = null;
                if (macKey == null) macKey = Files.exists(keyPath()) ? readKey() : newKey();
                compact();
                log.accept("PEER TRUST REGISTRY ACCEPTED by the operator (" + recoverPath() + "): " + recs.size()
                        + " record(s) kept and re-signed");
            } else {
                log.accept("ignoring " + recoverPath() + ": write 'accept' or 'reset'");
                return;
            }
            Files.deleteIfExists(recoverPath());
        } catch (Exception e) {
            log.accept("peer trust registry recovery failed: " + e.getMessage());
        }
    }

    private void clearState() {
        identities.clear(); cas.clear(); bridges.clear(); consumed.clear();
        lastMac = ""; lines = 0; needsNewline = false;
    }

    private void seal(String reason) {
        sealed = true;
        sealReason = reason;
        try {
            if (!Files.exists(sealPath())) writeAtomically(sealPath(), (reason + "\n" + new java.util.Date() + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.accept("could not persist the seal: " + e.getMessage());
        }
        log.accept("PEER TRUST REGISTRY SEALED (" + reason + "): what verified is still served; no new identity, CA or bridge"
                + " binding is accepted until an operator writes 'accept' or 'reset' into " + recoverPath());
    }

    // ---------------------------------------------------------------- the log

    private static Rec rec(String op, String name) {
        Rec r = new Rec();
        r.op = op;
        r.name = name;
        return r;
    }

    private void apply(Rec r) {
        if (r == null || r.op == null) return;
        switch (r.op) {
            case "bind": {
                Binding old = identities.get(r.name);
                identities.put(r.name, new Binding(r.tenant, r.key, null, r.source, r.role == null ? ROLE_AGENT : r.role,
                        old != null && old.key.equals(r.key) ? old.firstSeen : r.t, r.t));
                break;
            }
            case "ca": cas.put(r.name, new Binding(r.tenant, r.key, r.cert, r.source, null, r.t, r.t)); break;
            case "bridge": bridges.put(r.name, new Binding(null, null, null, "discovery", null, r.t, r.t)); break;
            case "unbridge": bridges.remove(r.name); break;
            case "unbind": identities.remove(r.name); break;
            case "consume": if (r.token != null) consumed.add(r.token); break;
            case "seen": {
                Map<String, Binding> m = "ca".equals(r.kind) ? cas : "bridge".equals(r.kind) ? bridges : identities;
                Binding b = m.get(r.name);
                if (b != null) m.put(r.name, new Binding(b.tenant, b.key, b.cert, b.source, b.role, b.firstSeen, r.t, r.t));   // log order: the last record wins
                break;
            }
            default: break;
        }
    }

    private String macOf(Rec r, String prev) throws Exception {
        String mac = r.mac;
        r.mac = null;
        String body = GSON.toJson(r);
        r.mac = mac;
        Mac m = Mac.getInstance("HmacSHA384");
        m.init(new SecretKeySpec(macKey, "HmacSHA384"));
        m.update(prev.getBytes(StandardCharsets.UTF_8));
        m.update((byte) '\n');
        return Base64.getEncoder().encodeToString(m.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    /** Parse the log. strict=true verifies the MAC chain; returns the records read before the first failure. */
    private List<Rec> readRecords(boolean strict) throws IOException {
        List<Rec> out = new ArrayList<>();
        if (!Files.exists(file)) return out;
        byte[] all = Files.readAllBytes(file);
        String text = new String(all, StandardCharsets.UTF_8);
        String prev = "";
        int pos = 0;
        while (pos < text.length()) {
            int nl = text.indexOf('\n', pos);
            boolean terminated = nl >= 0;
            String line = terminated ? text.substring(pos, nl) : text.substring(pos);
            int next = terminated ? nl + 1 : text.length();
            if (line.isEmpty()) { pos = next; continue; }
            Rec r = null;
            try { r = GSON.fromJson(line, Rec.class); } catch (Exception ignore) { }
            boolean ok = r != null && r.op != null;
            if (ok && strict) {
                try { ok = r.mac != null && MessageDigest.isEqual(r.mac.getBytes(StandardCharsets.UTF_8), macOf(r, prev).getBytes(StandardCharsets.UTF_8)); }
                catch (Exception e) { ok = false; }
            }
            if (!ok) {
                if (strict && !terminated && !out.isEmpty()) {
                    // a torn last line: a crash mid-append, never acknowledged -- drop it
                    log.accept("peer trust registry: dropping a torn last line (" + line.length() + " bytes; a crash during a write)");
                    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) { ch.truncate(pos); ch.force(true); }
                    needsNewline = false;
                    break;
                }
                if (strict) throw new BadRecord(out, "line " + (out.size() + 1) + " does not verify");
                pos = next;
                continue;
            }
            out.add(r);
            prev = r.mac == null ? prev : r.mac;
            if (!terminated) needsNewline = true;           // valid but its newline was lost: add it before the next append
            pos = next;
        }
        lastMac = prev;
        lines = out.size();
        return out;
    }

    private static final class BadRecord extends IOException {
        final List<Rec> good;
        BadRecord(List<Rec> good, String m) { super(m); this.good = good; }
    }

    private void load() {
        if (file == null) return;
        try { mkdirs(file.getParent()); } catch (Exception ignore) { /* reported below */ }
        boolean keyExists = Files.exists(keyPath());
        try {
            if (Files.exists(sealPath())) {
                String reason = Files.readAllLines(sealPath(), StandardCharsets.UTF_8).stream().findFirst().orElse("sealed");
                macKey = keyExists ? readKey() : null;
                if (macKey != null) {
                    try { for (Rec r : readRecords(true)) apply(r); }
                    catch (BadRecord br) { for (Rec r : br.good) apply(r); }
                }
                sealed = true;
                sealReason = reason;
                log.accept("PEER TRUST REGISTRY STILL SEALED (" + reason + "); serving " + identities.size() + " verified binding(s); write"
                        + " 'accept' or 'reset' into " + recoverPath() + " to recover");
                return;
            }
            if (!keyExists) {
                if (Files.exists(file)) { seal("the registry exists but its key " + keyPath() + " is missing"); return; }
                macKey = newKey();
                writeKey(macKey, false);
                initLog();
                return;
            }
            macKey = readKey();
            if (!Files.exists(file)) {
                if (keyInitialized()) { seal("the registry file is missing although its key says it was initialised (deleted?)"); return; }
                initLog();
                return;
            }
            try {
                List<Rec> recs = readRecords(true);
                if (recs.isEmpty() || !"init".equals(recs.get(0).op)) { seal("the registry has no valid header"); return; }
                for (Rec r : recs) apply(r);
            } catch (BadRecord br) {
                for (Rec r : br.good) apply(r);
                seal("integrity check failed: " + br.getMessage());
            }
        } catch (Exception ex) {
            seal("unreadable: " + ex.getMessage());
        }
    }

    private void initLog() throws Exception {
        if (channel != null) { try { channel.close(); } catch (IOException ignore) { } channel = null; }
        Rec r = rec("init", null);
        r.t = System.currentTimeMillis();
        r.source = "cresco-peer-trust-v2";
        lastMac = "";
        lines = 0;
        r.mac = macOf(r, lastMac);
        writeAtomically(file, (GSON.toJson(r) + "\n").getBytes(StandardCharsets.UTF_8));
        lastMac = r.mac;
        lines = 1;
        writeKey(macKey, true);
    }

    /** Append one record durably (fsync) and keep the chain. With the write lock. In memory: always true. */
    private boolean append(Rec r) {
        if (file == null) return true;
        if (sealed) return false;
        try {
            r.mac = macOf(r, lastMac);
            byte[] line = ((needsNewline ? "\n" : "") + GSON.toJson(r) + "\n").getBytes(StandardCharsets.UTF_8);
            if (channel == null || !channel.isOpen()) channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            ByteBuffer b = ByteBuffer.wrap(line);
            while (b.hasRemaining()) channel.write(b);
            writtenSeq++;
            needsNewline = false;
            lastMac = r.mac;
            lines++;
            int live = identities.size() + cas.size() + bridges.size() + consumed.size();
            if (lines > 4L * live + 1000) compact();
            return true;
        } catch (Exception ex) {
            log.accept("peer trust registry: could not append (" + ex.getMessage() + ")");
            return false;
        }
    }

    /**
     * Make every write so far durable (group commit: one fsync for all writers waiting). Called after the write lock is
     * released, before a bind is acknowledged. True when durable (or in memory).
     */
    private boolean durable() {
        if (file == null) return true;
        long target = writtenSeq;
        if (syncedSeq >= target) return true;
        synchronized (syncLock) {
            if (syncedSeq >= target) return true;
            long upto = writtenSeq;
            FileChannel c = channel;
            try {
                if (c != null) c.force(false);
                syncedSeq = Math.max(syncedSeq, upto);
                return true;
            } catch (java.nio.channels.ClosedChannelException closed) {
                return syncedSeq >= target;                       // a compaction replaced (and fsynced) the log meanwhile
            } catch (IOException ex) {
                log.accept("peer trust registry: fsync failed (" + ex.getMessage() + ")");
                return false;
            }
        }
    }

    /** Rewrite the log as its live state (temp, fsync, rename, directory fsync). With the write lock. */
    private void compact() throws Exception {
        List<Rec> recs = new ArrayList<>();
        Rec init = rec("init", null);
        init.t = System.currentTimeMillis();
        init.source = "cresco-peer-trust-v2";
        recs.add(init);
        for (Map.Entry<String, Binding> e : identities.entrySet()) {
            Rec r = rec("bind", e.getKey());
            Binding b = e.getValue();
            r.tenant = b.tenant; r.key = b.key; r.source = b.source; r.role = b.role; r.t = b.firstSeen;
            recs.add(r);
            if (b.lastSeen > b.firstSeen) { Rec s = rec("seen", e.getKey()); s.kind = "id"; s.t = b.lastSeen; recs.add(s); }
        }
        for (Map.Entry<String, Binding> e : cas.entrySet()) {
            Rec r = rec("ca", e.getKey());
            Binding b = e.getValue();
            r.tenant = b.tenant; r.key = b.key; r.cert = b.cert; r.source = b.source; r.t = b.lastSeen;
            recs.add(r);
        }
        for (Map.Entry<String, Binding> e : bridges.entrySet()) {
            Rec r = rec("bridge", e.getKey());
            r.t = e.getValue().lastSeen;
            recs.add(r);
        }
        for (String t : consumed) {
            Rec r = rec("consume", t.contains("#") ? t.substring(0, t.indexOf('#')) : t);
            r.token = t;
            recs.add(r);
        }
        StringBuilder sb = new StringBuilder();
        String prev = "";
        for (Rec r : recs) {
            r.mac = macOf(r, prev);
            prev = r.mac;
            sb.append(GSON.toJson(r)).append('\n');
        }
        if (channel != null) { try { channel.close(); } catch (IOException ignore) { } channel = null; }
        writeAtomically(file, sb.toString().getBytes(StandardCharsets.UTF_8));
        lastMac = prev;
        lines = recs.size();
        needsNewline = false;
        syncedSeq = writtenSeq;                                   // the compacted log is durable
    }

    // ---------------------------------------------------------------- key file and atomic writes

    private static final class KeyFile { String key; boolean initialized; }

    private byte[] newKey() {
        byte[] k = new byte[48];
        new SecureRandom().nextBytes(k);
        return k;
    }

    private byte[] readKey() throws IOException {
        String text = Files.readString(keyPath(), StandardCharsets.UTF_8).trim();
        try {
            KeyFile kf = GSON.fromJson(text, KeyFile.class);
            if (kf != null && kf.key != null) return Base64.getDecoder().decode(kf.key);
        } catch (Exception ignore) { }
        return Base64.getDecoder().decode(text);              // the earlier plain format
    }

    private boolean keyInitialized() {
        try {
            KeyFile kf = GSON.fromJson(Files.readString(keyPath(), StandardCharsets.UTF_8).trim(), KeyFile.class);
            return kf == null || kf.initialized;
        } catch (Exception e) {
            return true;                                       // an old-format key: the registry was initialised
        }
    }

    private void writeKey(byte[] key, boolean initialized) throws IOException {
        KeyFile kf = new KeyFile();
        kf.key = Base64.getEncoder().encodeToString(key);
        kf.initialized = initialized;
        writeAtomically(keyPath(), GSON.toJson(kf).getBytes(StandardCharsets.UTF_8));
    }

    /** Write {@code data} to {@code target}: owner-only temp file, fsync, atomic rename, directory fsync. */
    static void writeAtomically(Path target, byte[] data) throws IOException {
        Path dir = target.getParent();
        if (dir != null) mkdirs(dir);
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp-" + Long.toHexString(new SecureRandom().nextLong()));
        try (FileChannel ch = OWNER_ONLY == null
                ? FileChannel.open(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                : FileChannel.open(tmp, Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), OWNER_ONLY)) {
            ByteBuffer b = ByteBuffer.wrap(data);
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(dir);
    }

    /** Create a directory (and parents) owner-only where the platform supports it. */
    static void mkdirs(Path dir) throws IOException {
        if (dir == null || Files.isDirectory(dir)) return;
        if (OWNER_DIR == null) { Files.createDirectories(dir); return; }
        try { Files.createDirectories(dir, OWNER_DIR); } catch (UnsupportedOperationException e) { Files.createDirectories(dir); }
    }

    static void fsyncDir(Path dir) {
        if (dir == null) return;
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (Exception ignore) {
            // not every platform allows opening a directory; the rename is still atomic
        }
    }

    private static FileAttribute<?> attr(String perms) {
        try {
            return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(perms));
        } catch (Exception e) {
            return null;
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

    /** Test helper: a set of names currently bound. */
    Set<String> boundNames() { return new HashSet<>(identities.keySet()); }

}
