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
 *       other controllers join) binds each joiner's name to the key it presented, first come (STRICT): a second key, or
 *       another tenant, for a bound name is refused unless the operator authorises ONE rebind of that name. A client binds
 *       the controller it joined by FOLLOWING: a re-installed controller's new key replaces a binding that was itself
 *       learned by following (logged), because a joiner cannot tell a re-install from anything else. A follow never
 *       replaces a strict binding or this node's own. Names bound as controllers (and this node's own) are reserved: no
 *       agent is ever bound to one.</li>
 *   <li><b>Replaced keys</b>: a key a name was rebound away from stays refused for that name -- for a new strict binding,
 *       and on the CA path, after the binding itself is pruned -- unless the operator rebinds it. Never pruned.</li>
 *   <li><b>CA scopes</b> {@code region -> (tenant, CA)} for the regional CA: a region CA vouches only for its own region and
 *       tenant. Strict or follow as above: one CA per region at a responder; a client follows its region's CA.</li>
 *   <li><b>Bridge peers</b>: controllers that completed a REGION or GLOBAL discovery exchange; revocable, pruned when unseen
 *       for {@code bridgePruneDays}.</li>
 * </ul>
 * Bindings in use are refreshed by every verified broker connection ({@link #verifiedIdentity}) as well as by discovery,
 * in memory; refreshes are persisted in batches by a background writer, so pruning ({@code bindingPruneDays}) removes only
 * peers that were really gone.
 *
 * <p><b>Rebind</b> is one-shot: each entry ({@code region/agent} or {@code region} for a CA, optionally {@code #tag}) from the
 * {@code peer_identity_rebind} setting or the operator file {@code <file>.rebind} (re-read when it changes, no restart)
 * replaces one binding and is then recorded as consumed; it never applies again. To rebind the same name later, add it
 * with a new tag ({@code rgA/a1#2}).
 *
 * <p><b>Storage.</b> An append-only log of JSON lines, each carrying an HMAC-SHA-384 chained over the previous line's MAC,
 * keyed by a random per-node key in {@code <file>.key}. Appends are fsynced (group commit) before a bind is acknowledged,
 * and a HEAD ANCHOR ({@code <file>.head}: record count and last MAC, MAC'd, two alternating slots) is then moved up to
 * cover them, so a log truncated or rolled back to an earlier state is detected. A log that grows well past its live state
 * is compacted from the in-memory state after it includes the record that triggered it (temp file, fsync, rename,
 * directory fsync, new head). Reads never take a lock or do I/O. Files are created owner-only; a group- or world-writable
 * directory is tightened (or reported when another user owns it).
 * The MAC detects corruption and edits by anyone who cannot read the key file; it does NOT stop someone who can read
 * the node's data directory and write it.
 *
 * <p><b>Failures.</b> A torn last line (a crash mid-append; never acknowledged) is dropped. Any other line that does not
 * verify, a log shorter than its head anchor, the log or head missing while the key says they exist, the log, key and
 * head all missing while the node's persistent identity exists ({@link Options#expectExisting}) -- any of these SEALS the
 * registry: what verified is still served (established peers keep working), nothing new is bound, and the seal is
 * persisted ({@code <file>.sealed}), so a restart does not lift it. Recovery is explicit and applies only to a registry
 * that was ALREADY sealed when the request was written (a request found next to an unsealed registry, or one sealed only
 * on this start, is set aside unapplied): the operator writes {@code accept} -- keep every record whose MAC verifies,
 * re-signed (when the key itself was lost, every record that parses, signed with a new key that is saved) -- or
 * {@code reset} (start empty: every name, CA scope and bridge peer is learned again, first come) into
 * {@code <file>.recover}. It is applied on the next refused bind or restart, once: the file is consumed.
 */
public final class PeerTrustRegistry {

    /**
     * An identity binding, a CA scope or a bridge peer (immutable: replaced, never mutated in place). {@code follow}: learned
     * as a CLIENT following the controller it joined (may be replaced by a later follow); otherwise learned strictly (as a
     * responder, an issuing CA, or this node's own) and replaced only by an operator rebind.
     */
    static final class Binding {
        final String tenant, key, cert, source, role;
        final boolean follow;
        final long firstSeen, lastSeen, savedSeen;     // savedSeen: the last-seen time last written to the log
        Binding(String tenant, String key, String cert, String source, String role, boolean follow, long firstSeen, long lastSeen) {
            this(tenant, key, cert, source, role, follow, firstSeen, lastSeen, lastSeen);
        }
        Binding(String tenant, String key, String cert, String source, String role, boolean follow, long firstSeen, long lastSeen, long savedSeen) {
            this.tenant = tenant; this.key = key; this.cert = cert; this.source = source; this.role = role; this.follow = follow;
            this.firstSeen = firstSeen; this.lastSeen = lastSeen; this.savedSeen = savedSeen;
        }
        Binding seen(long t) { return new Binding(tenant, key, cert, source, role, follow, firstSeen, t, savedSeen); }
        Binding saved(long t) { return new Binding(tenant, key, cert, source, role, follow, firstSeen, Math.max(lastSeen, t), t); }
    }

    public static final String ROLE_AGENT = "agent", ROLE_CONTROLLER = "controller", ROLE_SELF = "self";

    /** Registry settings. */
    public static final class Options {
        public Collection<String> rebind = List.of();
        public Collection<String> revoke = List.of();
        public int bridgePruneDays = 30;
        public int bindingPruneDays = 90;
        /**
         * This node's registry must already exist (its persistent node identity does): a registry missing entirely --
         * log, key and head anchor -- is then sealed instead of silently started empty.
         */
        public boolean expectExisting;
    }

    /** One log record. */
    static final class Rec {
        String op;             // init | bind | ca | bridge | unbridge | unbind | seen | consume | revoke | noop
        String name, tenant, key, cert, source, role, token, kind, mode;   // mode: "follow" or absent (strict)
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
    /** name -> key fingerprints that were replaced for it (rebinds); never pruned. */
    private final Map<String, Set<String>> revokedKeys = new ConcurrentHashMap<>();
    /** last-seen refreshes waiting for the background writer (kind|name -> time). */
    private final Map<String, Long> pendingSeen = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicBoolean flushScheduled = new java.util.concurrent.atomic.AtomicBoolean();

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
    private boolean expectExisting;
    private final java.util.concurrent.ScheduledExecutorService background;
    /** Head anchor (truncation / rollback detection): what the last durable write covered. */
    private static final class Head {
        final long generation, lines; final String mac;
        Head(long generation, long lines, String mac) { this.generation = generation; this.lines = lines; this.mac = mac; }
    }
    private volatile Head head = new Head(0, 0, "");
    private long generation;                                    // incremented by every compaction / re-initialisation
    private long headEpoch;                                     // head slot writes so far (slot = epoch % 2)
    private FileChannel headChannel;

    /** In memory only (tests, and nodes that opt out of persistence). */
    public PeerTrustRegistry() {
        this.file = null;
        this.background = null;
    }

    private PeerTrustRegistry(Path file) {
        this.file = file;
        this.background = Executors.newSingleThreadScheduledExecutor(r -> {
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
        r.expectExisting = o.expectExisting;
        synchronized (r.writeLock) {
            secureDirectory(file.getParent(), r.log);
            // a recovery request counts only when the registry was ALREADY sealed when it was written: a request that is
            // present when the registry is not sealed (or becomes sealed only on this start) is set aside, never applied
            boolean sealedBefore = Files.exists(r.sealPath());
            r.load();
            if (Files.exists(r.recoverPath())) {
                if (sealedBefore && r.sealed) r.applyRecoveryIfAsked();
                else r.setAside(r.recoverPath(), "ignored", r.sealed
                        ? "the registry was not sealed when it was written (it was sealed only now); write it again to recover"
                        : "the registry is not sealed");
            }
            r.reloadRebindFile();
            if (!r.sealed) {
                long now = System.currentTimeMillis();
                for (String n : r.revokedBridges) if (r.bridges.remove(n) != null) r.append(rec("unbridge", n));
                r.prune(now);
                r.compactIfDue();
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
     * @param follow true: a client following the controller it joined -- a different key replaces a binding that was itself
     *               learned by following, never one learned strictly (as a responder or issuer) and never this node's own;
     *               false: first come, a different key is refused unless an unconsumed rebind for the name exists
     */
    public BindResult bind(String tenant, String region, String agent, java.security.PublicKey leafKey, String source, String role, boolean follow) {
        if (!CrescoNames.isValidTenant(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
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
            boolean replacesFollowed = b != null && follow && b.follow && !ROLE_SELF.equals(b.role);
            boolean selfOverSelf = b != null && ROLE_SELF.equals(role) && ROLE_SELF.equals(b.role);
            Set<String> revoked = revokedKeys.get(n);
            boolean wasRevoked = revoked != null && revoked.contains(fp);
            String token = null;
            if ((b != null && !replacesFollowed && !selfOverSelf) || (b == null && wasRevoked && !follow)) {
                token = rebindToken(n);
                if (token == null) {
                    String why = b == null ? "this key was replaced (rebound away) for " + n + " and stays refused for it"
                            : b.key.equals(fp) ? "tenant " + tenant + " differs from the bound tenant " + b.tenant
                            : (follow && !b.follow ? "the name was learned strictly (" + (ROLE_SELF.equals(b.role) ? "this node's own" : "as a joiner or issuer")
                                    + ") and is bound to another key (" + PeerIdentity.shortFp(b.key) + "); a client exchange never replaces it"
                            : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")");
                    return new BindResult(false, why + "; if " + n + " was re-installed, authorise ONE rebind on this node: add the line '" + n
                            + "' to " + rebindFileHint() + " (no restart), or set peer_identity_rebind=" + n);
                }
            }
            String newRole = (b != null && !ROLE_AGENT.equals(b.role) && ROLE_AGENT.equals(role)) ? b.role : role;
            boolean newFollow = token == null && (b == null ? follow : (replacesFollowed || (selfOverSelf && b.follow)));
            Rec r = rec("bind", n);
            r.tenant = tenant; r.key = fp; r.source = source; r.role = newRole; r.t = now; r.mode = newFollow ? "follow" : null;
            if (!append(r)) return new BindResult(false, "could not record the binding durably");
            if (token != null) {
                Rec c = rec("consume", n);
                c.token = token; c.t = now;
                append(c);
                consumed.add(token);
            }
            replaced(n, b == null ? null : b.key, fp);
            identities.put(n, new Binding(tenant, fp, null, source, newRole, newFollow,
                    b == null || token != null || !b.key.equals(fp) ? now : b.firstSeen, now));
            if (b != null && !b.key.equals(fp)) {
                log.accept("Identity " + n + " REBOUND from key " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                        + (token != null ? " (operator rebind '" + token + "', now consumed)" : " (followed: the controller this node joined re-keyed)")
                        + "; the old key stays refused for " + n);
            }
            compactIfDue();
            result = new BindResult(true, b == null ? "new binding" : "rebound");
        }
        return durable() ? result : new BindResult(false, "could not record the binding durably");
    }

    /** In memory, with the write lock: {@code oldKey} (when any) is now refused for {@code n}; {@code newKey} is not. */
    private void replaced(String n, String oldKey, String newKey) {
        if (oldKey != null && !oldKey.equals(newKey)) revokedKeys.computeIfAbsent(n, k -> ConcurrentHashMap.newKeySet()).add(oldKey);
        Set<String> rk = revokedKeys.get(n);
        if (rk != null) { rk.remove(newKey); if (rk.isEmpty()) revokedKeys.remove(n); }
    }

    /** Is {@code region/agent} free, or bound to this key? (Checks only.) */
    public BindResult canBind(String tenant, String region, String agent, java.security.PublicKey leafKey) {
        if (!CrescoNames.isValidTenant(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || leafKey == null) {
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
        Set<String> revoked = revokedKeys.get(n);
        boolean wasRevoked = revoked != null && revoked.contains(fp);
        if ((b == null && !wasRevoked) || (b != null && b.key.equals(fp) && b.tenant.equals(tenant))) return new BindResult(true, "free");
        synchronized (writeLock) { if (rebindToken(n) != null) return new BindResult(true, "operator rebind pending"); }
        return new BindResult(false, (b == null ? "this key was replaced (rebound away) for " + n
                : b.key.equals(fp) ? "tenant differs from the bound tenant " + b.tenant
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
     * @param follow true: a different CA replaces one that was itself recorded by following (a client following the region
     *               it joined); a CA recorded strictly (this node's own, or one a joining controller presented) is never
     *               replaced by a follow
     * @return null when recorded, else why not
     */
    public String trustCa(X509Certificate ca, String region, String tenant, String source, boolean follow) {
        if (ca == null || !CrescoNames.isValid(region) || !CrescoNames.isValidTenant(tenant)) return "invalid CA scope " + tenant + "/" + region;
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
            if (b != null && !(follow && b.follow)) {
                token = rebindToken(region);
                if (token == null) {
                    return "region " + region + " already has a " + (b.follow ? "" : "strictly ") + "recorded CA (" + PeerIdentity.shortFp(b.key)
                            + (follow && !b.follow ? "; a client exchange never replaces it" : "") + "); if its controller was"
                            + " re-installed, authorise ONE rebind on this node: add the line '" + region + "' to " + rebindFileHint();
                }
            }
            boolean newFollow = token == null && follow;
            Rec r = rec("ca", region);
            r.tenant = tenant; r.key = fp; r.cert = cert; r.source = source; r.t = now; r.mode = newFollow ? "follow" : null;
            if (!append(r)) return "could not record the CA durably";
            if (token != null) {
                Rec c = rec("consume", region);
                c.token = token; c.t = now;
                append(c);
                consumed.add(token);
            }
            cas.put(region, new Binding(tenant, fp, cert, source, null, newFollow, now, now));
            if (b != null) log.accept("CA for region " + region + " REBOUND from " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                    + (token != null ? " (operator rebind '" + token + "', now consumed)" : " (followed: the region this node joined re-keyed)"));
            compactIfDue();
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
            return (CrescoNames.isValid(ou) && CrescoNames.isValidTenant(o)) ? new String[]{ou, o} : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** What is recorded for {@code region}'s CA now (to restore it if a later step of the same exchange fails). */
    public Object caState(String region) {
        Binding b = cas.get(region);
        return b == null ? "none" : b;
    }

    /** Put {@code region}'s CA back to {@code state} (from {@link #caState}); recorded like any change. */
    public void restoreCa(String region, Object state) {
        synchronized (writeLock) {
            Binding now = cas.get(region);
            if ("none".equals(state)) {
                if (now == null) return;
                cas.remove(region);
                append(rec("unca", region));
            } else if (state instanceof Binding && now != state) {
                Binding b = (Binding) state;
                Rec r = rec("ca", region);
                r.tenant = b.tenant; r.key = b.key; r.cert = b.cert; r.source = b.source; r.t = b.lastSeen; r.mode = b.follow ? "follow" : null;
                if (append(r)) cas.put(region, b);
            }
            log.accept("CA for region " + region + " restored (the exchange that changed it was refused)");
        }
        durable();
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
        String n = name(id.getRegion(), id.getAgent());
        Binding ident = identities.get(n);
        Binding ca = cas.get(id.getRegion());
        if (ident != null) {
            if (ident.key.equals(fp) && ident.tenant.equals(id.getTenant())) {
                touch(identities, n, ident, System.currentTimeMillis(), "id");   // in use: keeps it from being pruned (no I/O here)
                return id;
            }
            why[0] = "the certificate names " + id.getAgentPath() + " but carries a different key than the one bound to it ("
                    + PeerIdentity.shortFp(ident.key) + ")";
            return null;
        }
        Set<String> revoked = revokedKeys.get(n);
        if (revoked != null && revoked.contains(fp)) {
            why[0] = "the certificate names " + id.getAgentPath() + " but its key was replaced (rebound away) for that name";
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

    /**
     * The identity {@code chain}'s leaf names, when its key is the one BOUND to that name (an identity binding; not the CA
     * path) and the leaf is within its validity period; else null. A TLS peer that completes the handshake holds that key,
     * so this is as strong as a pin: it lets the broker accept a bound peer's self-rooted chain after a restart, when the
     * in-memory truststore no longer holds it.
     */
    public CrescoIdentity boundByKey(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return null;
        X509Certificate leaf = chain[0];
        try { leaf.checkValidity(); } catch (Exception e) { return null; }
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) return null;
        Binding ident = identities.get(name(id.getRegion(), id.getAgent()));
        if (ident == null || !ident.tenant.equals(id.getTenant())) return null;
        return ident.key.equals(fingerprint(leaf.getPublicKey().getEncoded())) ? id : null;
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
            if (append(r)) bridges.put(n, new Binding(null, null, null, "discovery", null, false, now, now));
            compactIfDue();
        }
        durable();
    }

    /** Stop accepting bridges from {@code region/agent} (until a discovery exchange allows it again). */
    public boolean revokeBridgePeer(String region, String agent) {
        String n = name(region, agent);
        synchronized (writeLock) {
            if (bridges.remove(n) == null) return false;
            append(rec("unbridge", n));
            compactIfDue();
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
            compactIfDue();
        }
    }

    /** {@code region_agent} paths of the controllers this node joined (bound by following, as a client). */
    public Set<String> joinedControllers() {
        Set<String> out = new HashSet<>();
        for (Map.Entry<String, Binding> e : identities.entrySet()) {
            Binding b = e.getValue();
            if (b.follow && ROLE_CONTROLLER.equals(b.role)) out.add(e.getKey().replace('/', '_'));
        }
        return out;
    }

    /** Tests: pretend an identity binding was last seen {@code ms} ago (in memory only). */
    void backdateBinding(String region, String agent, long ms) {
        String n = name(region, agent);
        Binding b = identities.get(n);
        if (b != null) identities.put(n, new Binding(b.tenant, b.key, b.cert, b.source, b.role, b.follow, b.firstSeen,
                System.currentTimeMillis() - ms, System.currentTimeMillis() - ms));
    }

    /** Tests: when {@code region/agent} was last seen, or -1. */
    long lastSeen(String region, String agent) {
        Binding b = identities.get(name(region, agent));
        return b == null ? -1 : b.lastSeen;
    }

    int pinCount() { return identities.size(); }

    int bridgePeerCount() { return bridges.size(); }

    // ---------------------------------------------------------------- last-seen, pruning

    /**
     * Refresh last-seen in memory (lock-free, no I/O); when the stored value is a day old, queue it for the background
     * writer, which persists every queued refresh in one batch (one lock, one fsync). Replaces the binding only if it is
     * still the one read: a refresh racing a rebind never puts the old key back.
     */
    private void touch(Map<String, Binding> map, String n, Binding b, long now, String kind) {
        if (now - b.lastSeen < 60_000L) return;
        boolean persist = file != null && background != null && now - b.savedSeen > DAY;
        if (!map.replace(n, b, persist ? b.saved(now) : b.seen(now))) return;
        if (persist) {
            pendingSeen.put(kind + "|" + n, now);
            if (flushScheduled.compareAndSet(false, true)) {
                try { background.schedule(this::flushSeen, 5, java.util.concurrent.TimeUnit.SECONDS); }
                catch (Exception e) { flushScheduled.set(false); }
            }
        }
    }

    /** Persist every queued last-seen refresh: one batch under the write lock, then one group-commit fsync. */
    void flushSeen() {
        flushScheduled.set(false);
        if (pendingSeen.isEmpty()) return;
        synchronized (writeLock) {
            if (sealed) { pendingSeen.clear(); return; }
            for (String k : new ArrayList<>(pendingSeen.keySet())) {
                Long t = pendingSeen.remove(k);
                if (t == null) continue;
                int bar = k.indexOf('|');
                Rec r = rec("seen", k.substring(bar + 1));
                r.kind = k.substring(0, bar); r.t = t;
                append(r);
            }
            compactIfDue();
        }
        durable();
    }

    void prune(long now) {
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

    /** With the write lock: apply a pending recovery request (polled at most every 5 s) to a sealed registry. */
    private boolean tryRecover() {
        if (file == null) return false;
        long now = System.currentTimeMillis();
        if (now - recoverCheckedAt < 5_000L) return !sealed;
        recoverCheckedAt = now;
        if (!sealed || !Files.exists(recoverPath())) return !sealed;
        applyRecoveryIfAsked();
        return !sealed;
    }

    /** Move {@code p} aside ({@code <p>.<what>-<time>}) so it is never applied again, and say why. */
    private void setAside(Path p, String what, String why) {
        try {
            Path aside = p.resolveSibling(p.getFileName() + "." + what + "-" + System.currentTimeMillis());
            Files.move(p, aside);
            log.accept(p + " " + what.toUpperCase(java.util.Locale.ROOT) + " (" + why + "); kept as " + aside);
        } catch (IOException e) {
            try { Files.deleteIfExists(p); } catch (IOException ignore) { }
            log.accept(p + " " + what + " (" + why + ") and deleted");
        }
    }

    /**
     * Apply the operator's recovery request to a SEALED registry, once (the request file is consumed either way):
     * {@code reset} starts empty; {@code accept} keeps every record whose MAC verifies (each against the chain as stored)
     * and re-signs them -- or, when the key itself was lost, every record that parses, signed with a new key that is saved.
     */
    private void applyRecoveryIfAsked() {
        if (file == null || !Files.exists(recoverPath())) return;
        String what;
        try { what = Files.readString(recoverPath(), StandardCharsets.UTF_8).trim().toLowerCase(java.util.Locale.ROOT); }
        catch (IOException e) { log.accept("could not read " + recoverPath() + ": " + e.getMessage()); return; }
        if (!sealed) { setAside(recoverPath(), "ignored", "the registry is not sealed"); return; }
        try {
            if (what.equals("reset")) {
                closeChannels();
                Path aside = file.resolveSibling(file.getFileName() + ".reset-" + System.currentTimeMillis());
                if (Files.exists(file)) Files.move(file, aside);
                Files.deleteIfExists(headPath());
                clearState();
                macKey = newKey();
                writeKey(macKey, false);
                initLog();
                Files.deleteIfExists(sealPath());
                sealed = false;
                sealReason = null;
                log.accept("PEER TRUST REGISTRY RESET by the operator (" + recoverPath() + "): every name, CA scope and bridge peer will be"
                        + " learned again, first come; the old log is kept at " + aside);
            } else if (what.equals("accept")) {
                boolean keyLost = macKey == null;
                List<Rec> recs = readRecords(keyLost ? Verify.NONE : Verify.EACH);
                int total = countLines();
                clearState();
                for (Rec r : recs) apply(r);
                if (keyLost) macKey = newKey();
                compact();
                if (keyLost) writeKey(macKey, true);                // saved: the next start verifies with it
                Files.deleteIfExists(sealPath());
                sealed = false;
                sealReason = null;
                log.accept("PEER TRUST REGISTRY ACCEPTED by the operator (" + recoverPath() + "): " + recs.size() + " of " + total
                        + " record(s) kept and re-signed" + (keyLost ? " with a NEW key (the old key was lost, so nothing could be"
                        + " verified: every record that parses was kept on the operator's word)" : "; records that did not verify were dropped"));
            } else {
                setAside(recoverPath(), "ignored", "write 'accept' or 'reset'");
                return;
            }
            Files.deleteIfExists(recoverPath());
        } catch (Exception e) {
            setAside(recoverPath(), "failed", "recovery failed: " + e.getMessage());
        }
    }

    private int countLines() {
        try { return (int) Files.lines(file, StandardCharsets.UTF_8).filter(l -> !l.isBlank()).count(); } catch (Exception e) { return -1; }
    }

    private void clearState() {
        identities.clear(); cas.clear(); bridges.clear(); consumed.clear(); revokedKeys.clear(); pendingSeen.clear();
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
                + " binding is accepted until an operator writes 'accept' or 'reset' into " + recoverPath() + " (it is applied only"
                + " to a registry that is already sealed)");
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
                replaced(r.name, old == null ? null : old.key, r.key);
                identities.put(r.name, new Binding(r.tenant, r.key, null, r.source, r.role == null ? ROLE_AGENT : r.role, "follow".equals(r.mode),
                        old != null && old.key.equals(r.key) ? old.firstSeen : r.t, r.t));
                break;
            }
            case "ca": cas.put(r.name, new Binding(r.tenant, r.key, r.cert, r.source, null, "follow".equals(r.mode), r.t, r.t)); break;
            case "unca": cas.remove(r.name); break;
            case "bridge": bridges.put(r.name, new Binding(null, null, null, "discovery", null, false, r.t, r.t)); break;
            case "unbridge": bridges.remove(r.name); break;
            case "unbind": identities.remove(r.name); break;
            case "consume": if (r.token != null) consumed.add(r.token); break;
            case "revoke": if (r.key != null) revokedKeys.computeIfAbsent(r.name, k -> ConcurrentHashMap.newKeySet()).add(r.key); break;
            case "seen": {
                Map<String, Binding> m = "ca".equals(r.kind) ? cas : "bridge".equals(r.kind) ? bridges : identities;
                Binding b = m.get(r.name);
                if (b != null) m.put(r.name, new Binding(b.tenant, b.key, b.cert, b.source, b.role, b.follow, b.firstSeen, r.t, r.t));   // log order: the last record wins
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
        return hmac(prev + "\n", body);
    }

    private String hmac(String a, String b) throws Exception {
        Mac m = Mac.getInstance("HmacSHA384");
        m.init(new SecretKeySpec(macKey, "HmacSHA384"));
        m.update(a.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(m.doFinal(b.getBytes(StandardCharsets.UTF_8)));
    }

    /** How {@link #readRecords} checks MACs: the whole chain (load), each record against the stored chain (accept), none. */
    private enum Verify { CHAIN, EACH, NONE }

    /**
     * Parse the log. CHAIN verifies the MAC chain and throws {@link BadRecord} (with the records before it) at the first
     * failure; EACH keeps every record whose MAC verifies against the previous line's stored MAC and drops the rest; NONE
     * keeps every record that parses.
     */
    private List<Rec> readRecords(Verify verify) throws IOException {
        List<Rec> out = new ArrayList<>();
        if (!Files.exists(file)) return out;
        byte[] all = Files.readAllBytes(file);
        String text = new String(all, StandardCharsets.UTF_8);
        String prev = "", prevStored = "";
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
            if (ok && verify != Verify.NONE) {
                String chainPrev = verify == Verify.CHAIN ? prev : prevStored;
                try { ok = r.mac != null && MessageDigest.isEqual(r.mac.getBytes(StandardCharsets.UTF_8), macOf(r, chainPrev).getBytes(StandardCharsets.UTF_8)); }
                catch (Exception e) { ok = false; }
            }
            if (r != null && r.mac != null) prevStored = r.mac;
            if (!ok) {
                if (verify == Verify.CHAIN && !terminated && !out.isEmpty()) {
                    // a torn last line: a crash mid-append, never acknowledged -- drop it
                    log.accept("peer trust registry: dropping a torn last line (" + line.length() + " bytes; a crash during a write)");
                    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) { ch.truncate(pos); ch.force(true); }
                    needsNewline = false;
                    break;
                }
                if (verify == Verify.CHAIN) throw new BadRecord(out, "line " + (out.size() + 1) + " does not verify");
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
        boolean keyExists = Files.exists(keyPath()), logExists = Files.exists(file), headExists = Files.exists(headPath());
        headEpoch = headExists ? Math.min(maxHeadEpoch(), Long.MAX_VALUE / 2) : 0;   // slot writes continue above whatever is there
        try {
            if (keyExists) tightenFile(keyPath());
            if (Files.exists(sealPath())) {
                String reason = Files.readAllLines(sealPath(), StandardCharsets.UTF_8).stream().findFirst().orElse("sealed");
                macKey = keyExists ? readKey() : null;
                if (macKey != null) {
                    try { for (Rec r : readRecords(Verify.CHAIN)) apply(r); }
                    catch (BadRecord br) { for (Rec r : br.good) apply(r); }
                }
                sealed = true;
                sealReason = reason;
                log.accept("PEER TRUST REGISTRY STILL SEALED (" + reason + "); serving " + identities.size() + " verified binding(s); write"
                        + " 'accept' or 'reset' into " + recoverPath() + " to recover");
                return;
            }
            if (!keyExists) {
                if (logExists) { seal("the registry exists but its key " + keyPath() + " is missing"); return; }
                if (headExists) { seal("the registry's log and key are missing but its head anchor " + headPath() + " remains (deleted?)"); return; }
                if (expectExisting) {
                    seal("the registry (log, key and head anchor) is missing although this node's persistent identity exists (deleted?)");
                    return;
                }
                macKey = newKey();
                writeKey(macKey, false);
                initLog();
                return;
            }
            macKey = readKey();
            KeyFile kf = keyFile();
            if (!logExists) {
                if (kf == null || kf.initialized) { seal("the registry file is missing although its key says it was initialised (deleted?)"); return; }
                initLog();
                return;
            }
            List<Rec> recs;
            try {
                recs = readRecords(Verify.CHAIN);
            } catch (BadRecord br) {
                for (Rec r : br.good) apply(r);
                seal("integrity check failed: " + br.getMessage());
                return;
            }
            if (recs.isEmpty() || !"init".equals(recs.get(0).op)) { seal("the registry has no valid header"); return; }
            String anchor = checkHead(recs, kf != null && kf.anchored);
            for (Rec r : recs) apply(r);
            if (anchor != null) { seal(anchor); return; }
            if (kf == null || !kf.anchored) { writeHead(new Head(generation, lines, lastMac)); writeKey(macKey, true); }
        } catch (Exception ex) {
            seal("unreadable: " + ex.getMessage());
        }
    }

    private void initLog() throws Exception {
        closeChannels();
        Rec r = rec("init", null);
        r.t = System.currentTimeMillis();
        r.source = "cresco-peer-trust-v2";
        lastMac = "";
        lines = 0;
        r.mac = macOf(r, lastMac);
        writeAtomically(file, (GSON.toJson(r) + "\n").getBytes(StandardCharsets.UTF_8));
        lastMac = r.mac;
        lines = 1;
        generation++;
        writeHead(new Head(generation, lines, lastMac));
        writeKey(macKey, true);
    }

    private void closeChannels() {
        if (channel != null) { try { channel.close(); } catch (IOException ignore) { } channel = null; }
    }

    /** Append one record (written, not yet fsynced: see {@link #durable}) and keep the chain. With the write lock. */
    private boolean append(Rec r) {
        if (file == null) return true;
        if (sealed) return false;
        try {
            r.mac = macOf(r, lastMac);
            byte[] line = ((needsNewline ? "\n" : "") + GSON.toJson(r) + "\n").getBytes(StandardCharsets.UTF_8);
            if (channel == null || !channel.isOpen()) channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            ByteBuffer b = ByteBuffer.wrap(line);
            while (b.hasRemaining()) channel.write(b);
            needsNewline = false;
            lastMac = r.mac;
            lines++;
            head = new Head(generation, lines, lastMac);
            writtenSeq++;
            return true;
        } catch (Exception ex) {
            log.accept("peer trust registry: could not append (" + ex.getMessage() + ")");
            return false;
        }
    }

    private int live() {
        int rk = 0;
        for (Set<String> v : revokedKeys.values()) rk += v.size();
        return identities.size() + cas.size() + bridges.size() + consumed.size() + rk;
    }

    /**
     * With the write lock, AFTER the in-memory state includes every record just appended: rewrite the log when it has grown
     * well past its live state. The rewrite is made from that state, so it contains those records too.
     */
    private void compactIfDue() {
        if (file == null || sealed || lines <= 4L * live() + 1000) return;
        try {
            compact();
        } catch (Exception e) {
            log.accept("peer trust registry: compaction failed (" + e.getMessage() + "); the log is unchanged");
        }
    }

    /**
     * Make every write so far durable (group commit: one fsync for all writers waiting), then move the head anchor up to
     * cover it. Called after the write lock is released, before a bind is acknowledged. True when durable (or in memory).
     */
    private boolean durable() {
        if (file == null) return true;
        long target = writtenSeq;
        if (syncedSeq >= target) return true;
        synchronized (syncLock) {
            if (syncedSeq >= target) return true;
            long upto = writtenSeq;
            Head h = head;
            FileChannel c = channel;
            try {
                if (c != null) c.force(false);
                if (h.generation == generation) writeHead(h);       // a compaction since then wrote a newer head itself
                syncedSeq = Math.max(syncedSeq, upto);
                return true;
            } catch (java.nio.channels.ClosedChannelException closed) {
                return syncedSeq >= target;                       // a compaction replaced (and fsynced) the log meanwhile
            } catch (Exception ex) {
                log.accept("peer trust registry: fsync failed (" + ex.getMessage() + ")");
                return false;
            }
        }
    }

    /** Rewrite the log as its live state (temp, fsync, rename, directory fsync), then its head anchor. With the write lock. */
    private void compact() throws Exception {
        List<Rec> recs = new ArrayList<>();
        Rec init = rec("init", null);
        init.t = System.currentTimeMillis();
        init.source = "cresco-peer-trust-v2";
        recs.add(init);
        for (Map.Entry<String, Binding> e : identities.entrySet()) {
            Rec r = rec("bind", e.getKey());
            Binding b = e.getValue();
            r.tenant = b.tenant; r.key = b.key; r.source = b.source; r.role = b.role; r.t = b.firstSeen; r.mode = b.follow ? "follow" : null;
            recs.add(r);
            if (b.lastSeen > b.firstSeen) { Rec s = rec("seen", e.getKey()); s.kind = "id"; s.t = b.lastSeen; recs.add(s); }
        }
        for (Map.Entry<String, Set<String>> e : revokedKeys.entrySet()) {
            for (String k : e.getValue()) { Rec r = rec("revoke", e.getKey()); r.key = k; recs.add(r); }
        }
        for (Map.Entry<String, Binding> e : cas.entrySet()) {
            Rec r = rec("ca", e.getKey());
            Binding b = e.getValue();
            r.tenant = b.tenant; r.key = b.key; r.cert = b.cert; r.source = b.source; r.t = b.lastSeen; r.mode = b.follow ? "follow" : null;
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
        writeAtomically(file, sb.toString().getBytes(StandardCharsets.UTF_8));
        closeChannels();                                          // the old log is replaced; appends reopen the new one
        lastMac = prev;
        lines = recs.size();
        needsNewline = false;
        synchronized (syncLock) {
            generation++;
            head = new Head(generation, lines, lastMac);
            writeHead(head);
            syncedSeq = writtenSeq;                               // the compacted log (and its head) is durable
        }
    }

    // ---------------------------------------------------------------- head anchor

    private Path headPath() { return file.resolveSibling(file.getFileName() + ".head"); }

    private static final int HEAD_SLOT = 512;

    private static final class HeadRec { long epoch, lines; String mac, h; }

    /**
     * Record that the durable log has at least {@code h.lines} records, the last of them with MAC {@code h.mac}: written
     * in place to alternating fixed slots (a torn write leaves the other slot), fsynced. A log found shorter, or whose
     * record at that position differs, was truncated or rolled back: the registry seals.
     */
    private void writeHead(Head h) throws Exception {
        HeadRec r = new HeadRec();
        r.epoch = ++headEpoch; r.lines = h.lines; r.mac = h.mac;
        r.h = hmac("head|" + r.epoch + "|" + r.lines, r.mac);
        byte[] body = GSON.toJson(r).getBytes(StandardCharsets.UTF_8);
        byte[] slot = new byte[HEAD_SLOT];
        java.util.Arrays.fill(slot, (byte) ' ');
        System.arraycopy(body, 0, slot, 0, Math.min(body.length, HEAD_SLOT - 1));
        slot[HEAD_SLOT - 1] = '\n';
        if (headChannel == null || !headChannel.isOpen()) {
            headChannel = OWNER_ONLY == null
                    ? FileChannel.open(headPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)
                    : FileChannel.open(headPath(), Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ), OWNER_ONLY);
        }
        ByteBuffer b = ByteBuffer.wrap(slot);
        long pos = Math.floorMod(r.epoch, 2L) * HEAD_SLOT;
        while (b.hasRemaining()) pos += headChannel.write(b, pos);
        headChannel.force(false);
    }

    /** The highest epoch in any parseable head slot (verified or not), so new writes always supersede old slots. */
    private long maxHeadEpoch() {
        long max = 0;
        try {
            byte[] all = Files.readAllBytes(headPath());
            for (int i = 0; i < all.length && i < 2 * HEAD_SLOT; i += HEAD_SLOT) {
                try {
                    HeadRec r = GSON.fromJson(new String(all, i, Math.min(HEAD_SLOT, all.length - i), StandardCharsets.UTF_8).trim(), HeadRec.class);
                    if (r != null) max = Math.max(max, r.epoch);
                } catch (Exception ignore) { }
            }
        } catch (IOException ignore) { }
        return max;
    }

    /** Check the log against its head anchor; null when it is consistent, else why it is not. */
    private String checkHead(List<Rec> recs, boolean anchored) {
        if (!Files.exists(headPath())) {
            return anchored ? "the registry's head anchor " + headPath() + " is missing (deleted?)" : null;
        }
        HeadRec best = null;
        try {
            byte[] all = Files.readAllBytes(headPath());
            for (int i = 0; i < all.length && i < 2 * HEAD_SLOT; i += HEAD_SLOT) {
                String slot = new String(all, i, Math.min(HEAD_SLOT, all.length - i), StandardCharsets.UTF_8).trim();
                if (slot.isEmpty()) continue;
                try {
                    HeadRec r = GSON.fromJson(slot, HeadRec.class);
                    if (r == null || r.mac == null || r.h == null) continue;
                    if (!MessageDigest.isEqual(r.h.getBytes(StandardCharsets.UTF_8),
                            hmac("head|" + r.epoch + "|" + r.lines, r.mac).getBytes(StandardCharsets.UTF_8))) continue;
                    if (best == null || r.epoch > best.epoch) best = r;
                } catch (Exception ignore) { /* a torn slot */ }
            }
        } catch (IOException e) {
            return "the head anchor is unreadable: " + e.getMessage();
        }
        if (best == null) return "the registry's head anchor does not verify";
        if (recs.size() < best.lines || !best.mac.equals(recs.get((int) best.lines - 1).mac)) {
            return "the registry log is shorter than its head anchor, or differs from it (" + recs.size() + " records, the anchor"
                    + " covers " + best.lines + "): truncated or rolled back";
        }
        return null;
    }

    // ---------------------------------------------------------------- key file and atomic writes

    private static final class KeyFile { String key; boolean initialized, anchored; }

    /** The key file's flags, or null (an old plain-format key). */
    private KeyFile keyFile() {
        try {
            KeyFile kf = GSON.fromJson(Files.readString(keyPath(), StandardCharsets.UTF_8).trim(), KeyFile.class);
            return (kf == null || kf.key == null) ? null : kf;
        } catch (Exception e) {
            return null;
        }
    }

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

    private void writeKey(byte[] key, boolean initialized) throws IOException {
        KeyFile kf = new KeyFile();
        kf.key = Base64.getEncoder().encodeToString(key);
        kf.initialized = initialized;
        kf.anchored = initialized;                             // an initialised registry always has a head anchor
        writeAtomically(keyPath(), GSON.toJson(kf).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The registry's directory must not be writable by anyone but this node's user: whoever can write it can delete or
     * replace the log, key, head anchor and recovery request. Group/other write permission is removed when this user owns
     * the directory; otherwise it is reported.
     */
    static void secureDirectory(Path dir, Consumer<String> log) {
        if (dir == null || !Files.isDirectory(dir)) return;
        try {
            Set<java.nio.file.attribute.PosixFilePermission> perms = Files.getPosixFilePermissions(dir);
            boolean loose = perms.contains(java.nio.file.attribute.PosixFilePermission.GROUP_WRITE)
                    || perms.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE);
            if (!loose) return;
            String owner = Files.getOwner(dir).getName(), me = System.getProperty("user.name");
            if (owner.equals(me)) {
                Set<java.nio.file.attribute.PosixFilePermission> tight = new java.util.HashSet<>(perms);
                tight.remove(java.nio.file.attribute.PosixFilePermission.GROUP_WRITE);
                tight.remove(java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE);
                Files.setPosixFilePermissions(dir, tight);
                log.accept("Security directory " + dir + " was writable by group/others (" + PosixFilePermissions.toString(perms)
                        + "); removed that write permission (now " + PosixFilePermissions.toString(tight) + ")");
            } else {
                log.accept("SECURITY DIRECTORY " + dir + " is writable by group/others (" + PosixFilePermissions.toString(perms) + ") and owned by "
                        + owner + ", not this node's user " + me + ": anyone who can write it can replace this node's identity and trust"
                        + " records. Make it owner-only (chmod 700).");
            }
        } catch (UnsupportedOperationException | IOException ignore) {
            // not a POSIX file system: nothing to check
        }
    }

    /** Remove group/other access from a secret file this user owns (an older one may predate owner-only creation). */
    private void tightenFile(Path f) {
        try {
            Set<java.nio.file.attribute.PosixFilePermission> perms = Files.getPosixFilePermissions(f);
            Set<java.nio.file.attribute.PosixFilePermission> tight = new java.util.HashSet<>(perms);
            tight.retainAll(Set.of(java.nio.file.attribute.PosixFilePermission.OWNER_READ, java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
            if (!tight.equals(perms) && Files.getOwner(f).getName().equals(System.getProperty("user.name"))) {
                Files.setPosixFilePermissions(f, tight);
                log.accept(f + " was readable or writable by group/others; made it owner-only");
            }
        } catch (UnsupportedOperationException | IOException ignore) { }
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
