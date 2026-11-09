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
 * keyed by a random per-node key in {@code <file>.key}. Every change is written and fsynced under the write lock BEFORE the
 * in-memory state changes and before it is acknowledged; a write or fsync that fails is truncated away (the log is put back
 * to the last durable offset) and nothing changes in memory, so a failed write can never leave a half line that later
 * appends land behind, and a binding that was not made durable is never served. If even that truncation fails, this
 * instance is fenced (it writes nothing more; a restart re-verifies the log). Several records of one change (a bind and the
 * rebind token it consumes) are one write. A HEAD ANCHOR ({@code <file>.head}: record count and last MAC, MAC'd, two
 * alternating slots) is then moved up to cover them, so a log truncated or rolled back to an earlier state is detected. A
 * log that grows well past its live state is compacted from the in-memory state (temp file, fsync, rename, directory fsync,
 * new head); the compacted log's first record names the last {@value #TAIL} MACs of the log it replaced, so a crash between
 * the rename and the new head (the old head then covers more lines than the new log has) is recognised as exactly that and
 * not as a rollback. Reads never take a lock or do I/O. Files are created owner-only; a group- or world-writable
 * directory is tightened (or reported when another user owns it).
 *
 * <p><b>One writer per store.</b> Opening a store fences any instance of the same store this JVM had open (it writes nothing
 * more), and an exclusive file lock ({@code <file>.lock}) keeps another process from writing it: an instance that cannot
 * take the lock serves what it read and records nothing. {@link #shared} returns the instance already open for a store
 * whose files are intact, so re-created components share it.
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
        private volatile X509Certificate decoded;      // the CA certificate, decoded once (a cache; the binding is otherwise immutable)
        X509Certificate caCert() {
            X509Certificate c = decoded;
            if (c == null && cert != null) decoded = c = decode(cert);
            return c;
        }
        Binding seen(long t) { return new Binding(tenant, key, cert, source, role, follow, firstSeen, t, savedSeen); }
        Binding saved(long t) { return new Binding(tenant, key, cert, source, role, follow, firstSeen, Math.max(lastSeen, t), t); }
    }

    /** One revocation (immutable). */
    public static final class Revocation {
        /** The revoked key's fingerprint (SHA-256 of its SubjectPublicKeyInfo, hex). */
        public final String fp;
        /** Null: refused for every name; else refused only for the names of this region. */
        public final String scope;
        /** The name the key was bound to where it was revoked ({@code region/agent}), when known. */
        public final String name;
        public final String source;
        /** Position in this node's list; when; when it can no longer matter (0 = never). */
        public final long seq, t, exp;
        /** Made on this node (its operator): part of its own list, which goes up to its parent. */
        public final boolean own;
        Revocation(String fp, String scope, String name, String source, long seq, long t, long exp, boolean own) {
            this.fp = fp; this.scope = scope; this.name = name; this.source = source; this.seq = seq; this.t = t; this.exp = exp; this.own = own;
        }
        String id() { return scope == null ? fp : fp + "@" + scope; }
    }

    /** A tombstoned name: the key that was revoked for it, and why. */
    static final class Tomb {
        final String key, source; final long t;
        Tomb(String key, String source, long t) { this.key = key; this.source = source; this.t = t; }
    }

    public static final String ROLE_AGENT = "agent", ROLE_CONTROLLER = "controller", ROLE_SELF = "self";

    /** Registry settings. */
    public static final class Options {
        public Collection<String> rebind = List.of();
        public Collection<String> revoke = List.of();
        /** Identities to revoke at open ({@code peer_identity_revoke}): {@code region/agent} names or key fingerprints. */
        public Collection<String> revokeIdentities = List.of();
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
        String op;             // init | bind | ca | bridge | unbridge | unbind | seen | consume | revoke | revokekey | tomb | expire | epoch | noop
        String name, tenant, key, cert, source, role, token, kind, mode;   // mode: "follow" or absent (strict)
        long t;
        String scope;          // revokekey: the region whose names it applies to (absent: every name)
        Long seq, exp;         // revokekey: position in this node's revocation list; when it can no longer matter (absent: never)
        Boolean own;           // revokekey: made on this node (its operator), so it goes up to the parent
        Long prevLines;        // init of a compacted log: the replaced log's record count ...
        List<String> prevTail; // ... and its last MACs (oldest first): a crash before the new head is recognised
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
    /**
     * Revocations by id: a key fingerprint (refused for EVERY name: this node's operator, or its parent's list) or
     * {@code fingerprint@region} (refused for that region's names only: what a child region's list may revoke).
     */
    private final Map<String, Revocation> revoked = new ConcurrentHashMap<>();
    /** The same entries by their position in this node's revocation list (what goes to parent and children, as deltas). */
    private final java.util.concurrent.ConcurrentSkipListMap<Long, Revocation> revokedBySeq = new java.util.concurrent.ConcurrentSkipListMap<>();
    /** Key fingerprint -> how many entries revoke it (any scope). */
    private final Map<String, Integer> revokedAnyScope = new ConcurrentHashMap<>();
    /** Guards the assignment of list positions together with the in-memory application of an entry. */
    private final Object revMemLock = new Object();
    private long revSeq;                                        // with revMemLock: the last position given out
    /** Entries up to this position are durable (only those are served to peers: a position is never reused). */
    private volatile long durableRevSeq;
    /** The highest position of an entry this node made itself (its operator): its own list, which goes up to its parent. */
    private volatile long ownRevSeq;
    /** This registry's epoch: random, made with the registry; a peer that sees it change sends its list again. */
    private volatile String revEpoch;
    private volatile long revVersion;                           // bumped by every change (digest cache)
    private volatile String digestCache;
    private volatile long digestVersion = -1;
    /** Revocations applied in memory whose bindings, name tombstones and durable record are still to be done (seq order). */
    private final java.util.ArrayDeque<Revocation> revPending = new java.util.ArrayDeque<>();   // with revMemLock
    /** Non-null while some revocations are in force in memory but not recorded durably (why). Reported as CRITICAL. */
    private volatile String revDurability;
    private volatile long revRetryMs = 1000;
    private final java.util.concurrent.atomic.AtomicBoolean revFlushScheduled = new java.util.concurrent.atomic.AtomicBoolean();
    /**
     * Names whose bound key was revoked: TOMBSTONED -- refused for every key, at TLS, at the broker and on the CA path, and
     * never bound again (first come) until the operator rebinds the name. Never pruned.
     */
    private final Map<String, Tomb> tombs = new ConcurrentHashMap<>();
    /** Key fingerprint -> the names bound to it (maintained with the write lock; read lock-free). */
    private final Map<String, Set<String>> namesByKey = new ConcurrentHashMap<>();
    /** Notified (outside the write lock) with the key fingerprint when a key is revoked. */
    private volatile Consumer<String> revokedListener;
    /** Notified (outside the write lock) once per batch of newly revoked keys. */
    private volatile Consumer<Collection<String>> revokedBatchListener;
    /** The operator file's mtime once every entry in it is in force AND recorded (a failed write leaves it to be re-read). */
    private volatile long revokeFileMtime = -1;
    /** Lifetime of the leaves this node's region CA issues (0: none): revoking a key it enrolled can expire after that. */
    private volatile long issuedLeafLifetimeMs;
    /** The registry could not take its file lock (another process?): nothing is recorded; reported as CRITICAL. */
    private volatile String lockFailure;
    /** last-seen refreshes waiting for the background writer (kind|name -> time). */
    private final Map<String, Long> pendingSeen = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicBoolean flushScheduled = new java.util.concurrent.atomic.AtomicBoolean();

    private final Object writeLock = new Object();
    private FileChannel channel;                                // the log, open for positional writes (with the write lock)
    private long goodOffset;                                    // the log's length after the last durable write
    private Object logFileKey;                                  // the log file's identity when the channel was opened
    private volatile String fenced;                             // non-null: this instance writes nothing more (why)
    private FileChannel lockChannel;
    private java.nio.channels.FileLock fileLock;
    /** The last {@link #TAIL} MACs of the log, oldest first (named by the first record of a compaction). */
    private final java.util.ArrayDeque<String> recentMacs = new java.util.ArrayDeque<>();
    static final int TAIL = 64;
    /** Notified (outside the write lock) when a name's key is replaced: (name, old key fingerprint). */
    private volatile java.util.function.BiConsumer<String, String> keyReplacedListener;
    /** The instance open for each store in this JVM (one writer per store). */
    private static final Map<Path, PeerTrustRegistry> LIVE = new ConcurrentHashMap<>();
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
        this.revEpoch = newEpochId();
    }

    private static String newEpochId() {
        byte[] b = new byte[12];
        new SecureRandom().nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    /** After a load: a registry from before epochs gets one now (recorded when it can be). */
    private void ensureEpoch() {
        if (revEpoch != null) return;
        revEpoch = newEpochId();
        recordEpoch();
    }

    /** With the write lock: record the current epoch in the log (and the high-water mark). */
    private void recordEpoch() {
        if (file == null || sealed || fenced != null) return;
        Rec r = rec("epoch", null);
        r.token = revEpoch;
        r.t = System.currentTimeMillis();
        if (!append(r)) log.accept("peer trust registry: its revocation epoch could not be recorded (peers resend their lists after a restart)");
        writeHighWater();
    }

    // ---------------------------------------------------------------- revocation positions vs. a restored registry

    /**
     * The revocation high-water mark ({@code <epoch> <position>}): the furthest position of the current epoch ever made
     * durable. Kept OUTSIDE the registry's directory ({@code security/}, which is what an operator restores from a backup),
     * next to it, so a registry restored to an older state -- the same epoch with fewer entries -- is recognised at open and
     * starts a NEW epoch: its next revocations would otherwise take positions its children already hold, and never reach them.
     */
    private Path highWaterPath() {
        if (file == null) return null;
        Path dir = file.toAbsolutePath().getParent();
        Path up = dir == null ? null : dir.getParent();
        return up == null ? null : up.resolve(file.getFileName() + ".hwm");
    }

    private void writeHighWater() {
        Path p = highWaterPath();
        if (p == null || revEpoch == null) return;
        try {
            writeAtomically(p, (revEpoch + " " + durableRevSeq + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.accept("peer trust registry: could not record its revocation high-water mark " + p + " (" + e.getMessage()
                    + "); a registry restored from a backup may not be recognised");
        }
    }

    /** At open: compare the registry with its high-water mark; a registry behind it (or of another epoch) starts a new epoch. */
    private void checkHighWater() {
        Path p = highWaterPath();
        if (p == null || sealed || fenced != null) return;
        try {
            if (!Files.exists(p)) { writeHighWater(); return; }
            String[] hw = Files.readString(p, StandardCharsets.UTF_8).trim().split("\\s+");
            String epoch = hw.length > 0 ? hw[0] : "";
            long seq = hw.length > 1 ? Long.parseLong(hw[1]) : 0;
            if (epoch.equals(revEpoch) && seq <= durableRevSeq) {
                if (seq < durableRevSeq) writeHighWater();
                return;
            }
            newEpoch(epoch.equals(revEpoch)
                    ? "the registry holds revocations up to position " + durableRevSeq + " but already recorded " + seq + " (restored from an older copy?)"
                    : "the registry's revocation epoch " + revEpoch + " is not the last one recorded, " + epoch + " (restored from an older copy?)");
        } catch (Exception e) {
            log.accept("peer trust registry: its revocation high-water mark " + p + " is unreadable (" + e.getMessage() + "); starting a new epoch");
            newEpoch("unreadable high-water mark");
        }
    }

    /** With the write lock: a new revocation epoch (every peer fetches this node's list again, and resends its own). */
    private void newEpoch(String why) {
        String old = revEpoch;
        revEpoch = newEpochId();
        revVersion++;
        log.accept("peer trust registry: NEW revocation epoch " + revEpoch + " (was " + old + "): " + why + "; its parent and children"
                + " exchange their revocation lists again from the start");
        recordEpoch();
    }

    private volatile long lastReactiveEpoch;

    /**
     * A peer reports that this node's list (or its own list as acknowledged) went further, in this epoch, than this node
     * holds: the registry went back in time (a whole data directory restored). Starts a new epoch, at most once an hour (a
     * new epoch is a durable write, and a peer must not be able to make one at every ping). Returns whether it did: when it did
     * not, the caller must not trust what the peer reported for this epoch (it would skip positions this node writes next) --
     * it exchanges the lists from the start instead.
     */
    public boolean rollbackSuspected(String why) {
        long now = System.currentTimeMillis();
        if (now - lastReactiveEpoch < 3_600_000L) return false;
        lastReactiveEpoch = now;
        synchronized (writeLock) { newEpoch(why); }
        return true;
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

    private static Path storeKey(Path file) {
        return file.toAbsolutePath().normalize();
    }

    /**
     * The instance already open in this JVM for the store at {@code file}, when its files are still the ones it opened
     * (re-created components share one writer), with {@code o}'s rebind and revoke entries added; else {@link #open}.
     */
    public static PeerTrustRegistry shared(Path file, Options o, Consumer<String> log) {
        synchronized (LIVE) {
            PeerTrustRegistry live = LIVE.get(storeKey(file));
            if (live != null && live.fenced == null && live.storeIntact()) {
                live.reconfigure(o);
                return live;
            }
            return open(file, o, log);
        }
    }

    /** Open (or create) the persistent registry at {@code file}, fencing any instance of it this JVM had open. */
    public static PeerTrustRegistry open(Path file, Options o, Consumer<String> log) {
        synchronized (LIVE) {
            PeerTrustRegistry old = LIVE.remove(storeKey(file));
            if (old != null) old.supersede();
            PeerTrustRegistry r = openNew(file, o, log);
            LIVE.put(storeKey(file), r);
            return r;
        }
    }

    private static PeerTrustRegistry openNew(Path file, Options o, Consumer<String> log) {
        PeerTrustRegistry r = new PeerTrustRegistry(file);
        if (log != null) r.log = log;
        r.rebindConfig.addAll(o.rebind);
        r.revokedBridges.addAll(o.revoke);
        r.bridgePruneMs = o.bridgePruneDays <= 0 ? 0 : o.bridgePruneDays * DAY;
        r.bindingPruneMs = o.bindingPruneDays <= 0 ? 0 : o.bindingPruneDays * DAY;
        r.expectExisting = o.expectExisting;
        synchronized (r.writeLock) {
            secureDirectory(file.getParent(), r.log);
            r.lock();
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
            r.ensureEpoch();
            r.checkHighWater();
            // revocations are put in force even on a sealed or fenced registry (fail closed): recorded when it can be
            for (String t : o.revokeIdentities) {
                String why = r.revokeLocked(t, "peer_identity_revoke");
                if (why != null) r.log.accept("peer_identity_revoke '" + t + "' not applied: " + why);
            }
            r.reloadRevokeFileLocked();
            r.completeRevocationsLocked();
            if (!r.sealed && r.fenced == null) {
                long now = System.currentTimeMillis();
                r.revokeConfiguredBridges();
                r.prune(now);
                r.compactIfDue();
            }
        }
        if (r.background != null) {
            // the operator revocation file is re-read every 10 s (no restart): a revocation takes effect at once
            r.background.scheduleWithFixedDelay(() -> {
                try { synchronized (r.writeLock) { r.reloadRevokeFileLocked(); } } catch (Exception ignore) { }
                r.fireRevokedPending();                         // close the revoked keys' connections at once
            }, 10, 10, java.util.concurrent.TimeUnit.SECONDS);
            // revocations that can no longer matter (expired) are dropped hourly
            r.background.scheduleWithFixedDelay(() -> {
                try { synchronized (r.writeLock) { if (!r.sealed && r.fenced == null) r.expireRevocations(System.currentTimeMillis()); } }
                catch (Exception ignore) { }
            }, 1, 1, java.util.concurrent.TimeUnit.HOURS);
        }
        r.fireRevokedPending();
        return r;
    }

    /** With the write lock: drop the bridge peers the operator revoked (peer_bridge_revoke); fail-safe: memory first. */
    private void revokeConfiguredBridges() {
        List<Rec> recs = new ArrayList<>();
        for (String n : revokedBridges) if (bridges.remove(n) != null) recs.add(rec("unbridge", n));
        if (!recs.isEmpty() && !append(recs.toArray(new Rec[0]))) {
            log.accept("peer trust registry: the bridge revocations " + recs.size() + " are in effect but could not be recorded");
        }
    }

    /** {@link #shared}: add a re-created component's operator settings to the live instance. */
    private void reconfigure(Options o) {
        synchronized (writeLock) {
            rebindConfig.addAll(o.rebind);
            revokedBridges.addAll(o.revoke);
            bridgePruneMs = o.bridgePruneDays <= 0 ? 0 : o.bridgePruneDays * DAY;
            bindingPruneMs = o.bindingPruneDays <= 0 ? 0 : o.bindingPruneDays * DAY;
            if (!sealed && fenced == null) revokeConfiguredBridges();
        }
    }

    /** Are this instance's files still the ones it opened (not deleted or replaced underneath it)? */
    private boolean storeIntact() {
        if (file == null) return true;
        try {
            if (!Files.exists(file) || !Files.exists(keyPath())) return false;
            Object k = Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
            synchronized (writeLock) {
                return logFileKey == null || k == null || logFileKey.equals(k);
            }
        } catch (IOException e) {
            return false;
        }
    }

    /** Take the store's exclusive file lock; without it this instance is fenced (another process writes the store). */
    private void lock() {
        if (file == null) return;
        try {
            mkdirs(file.getParent());
            Path lp = file.resolveSibling(file.getFileName() + ".lock");
            lockChannel = OWNER_ONLY == null
                    ? FileChannel.open(lp, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
                    : FileChannel.open(lp, Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE), OWNER_ONLY);
            fileLock = lockChannel.tryLock();
            if (fileLock == null) lockFailed("another process holds the registry's lock " + lp + " (two nodes on one data directory?)");
        } catch (java.nio.channels.OverlappingFileLockException e) {
            lockFailed("this process already holds the registry's lock through another instance");
        } catch (IOException e) {
            lockFailed("the registry's lock " + file.getFileName() + ".lock could not be taken (" + e.getMessage()
                    + "; a file system without locks, e.g. some NFS mounts, cannot hold a registry)");
        }
    }

    /** The lock could not be taken: fenced (nothing is recorded), reported loudly (a CRITICAL health check reads it). */
    private void lockFailed(String why) {
        lockFailure = why;
        fence(why);
        log.accept("PEER TRUST REGISTRY CANNOT LOCK ITS STORE: " + why + ". It is FENCED: no new identity, CA or bridge binding is"
                + " accepted on this node (joiners are refused with this reason) until the lock can be taken; revocations are enforced"
                + " in memory only. Health check 'identity' reports CRITICAL.");
    }

    /** Stop writing: what this instance holds is still served, nothing new is recorded by it. */
    private void fence(String why) {
        if (fenced != null) return;
        fenced = why;
        log.accept("PEER TRUST REGISTRY instance FENCED (" + why + "): it records nothing more; what it holds is still served");
    }

    /** Fenced because another instance of the same store was opened in this JVM. */
    private void supersede() {
        synchronized (writeLock) {
            if (fenced == null) fenced = "superseded by a newer instance of the same store";
        }
        close();
    }

    /** Why this instance records nothing (null when it does). */
    public String fencedReason() { return fenced; }

    /**
     * Close the store: persist queued last-seen refreshes, release the files and the lock. The instance keeps serving what
     * it holds but records nothing more.
     */
    public void close() {
        if (file != null && fenced == null) flushSeen();
        synchronized (writeLock) {
            if (fenced == null) fenced = "closed";
            closeChannels();
            if (headChannel != null) { try { headChannel.close(); } catch (IOException ignore) { } headChannel = null; }
            if (fileLock != null) { try { fileLock.release(); } catch (IOException ignore) { } fileLock = null; }
            if (lockChannel != null) { try { lockChannel.close(); } catch (IOException ignore) { } lockChannel = null; }
        }
        if (background != null) background.shutdownNow();
        if (file != null) LIVE.remove(storeKey(file), this);
    }

    /** Called (outside the write lock) with (name, old key fingerprint) whenever a name's key is replaced. */
    public void setKeyReplacedListener(java.util.function.BiConsumer<String, String> l) {
        this.keyReplacedListener = l;
    }

    private void fireReplaced(String n, String oldKey) {
        java.util.function.BiConsumer<String, String> l = keyReplacedListener;
        if (l == null || n == null || oldKey == null) return;
        try { l.accept(n, oldKey); } catch (Exception e) { log.accept("peer trust registry: key-replaced listener failed: " + e.getMessage()); }
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
        return bind(tenant, region, agent, leafKey, source, role, follow, null);
    }

    /** As above; {@code vouched} non-null: the caller verified the new key is vouched for (see {@link #rebindVouched}). */
    private BindResult bind(String tenant, String region, String agent, java.security.PublicKey leafKey, String source, String role, boolean follow,
                            String vouched) {
        String fp = fingerprint(leafKey.getEncoded());
        String n = name(region, agent);
        long now = System.currentTimeMillis();
        if (pki != null && !ROLE_SELF.equals(role)) {
            return new BindResult(false, "this node is pre-provisioned: identities come from the operator's PKI, never bound at a join");
        }
        if (isRevokedFor(fp, region)) {
            return new BindResult(false, "the key " + PeerIdentity.shortFp(fp) + " is " + revokedText(fp, region) + "; a revoked node"
                    + " needs a new key");
        }
        Binding b = identities.get(n);
        if (b != null && b.key.equals(fp) && b.tenant.equals(tenant) && (ROLE_AGENT.equals(role) || role.equals(b.role))) {
            touch(identities, n, b, now, "id");                // a refresh: no I/O on this path
            return new BindResult(true, "known key");
        }
        BindResult result;
        String replacedKey = null;
        synchronized (writeLock) {
            if (sealed && !tryRecover()) return new BindResult(false, sealedText());
            if (fenced != null) return new BindResult(false, fencedText());
            b = identities.get(n);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant) && (ROLE_AGENT.equals(role) || role.equals(b.role))) {
                return new BindResult(true, "known key");
            }
            Tomb tomb = tombs.get(n);
            boolean replacesFollowed = tomb == null && b != null && follow && b.follow && !ROLE_SELF.equals(b.role);
            boolean selfOverSelf = tomb == null && b != null && ROLE_SELF.equals(role) && ROLE_SELF.equals(b.role);
            Set<String> revoked = revokedKeys.get(n);
            boolean wasRevoked = revoked != null && revoked.contains(fp);
            String token = null;
            if (tomb != null || (b != null && !replacesFollowed && !selfOverSelf) || (b == null && wasRevoked && !follow)) {
                token = vouched != null && tomb == null && !wasRevoked ? null : rebindToken(n, fp);
                if (token == null && (vouched == null || tomb != null || wasRevoked)) {
                    if (tomb != null) return new BindResult(false, tombText(n, tomb));
                    String why = b == null ? "this key was replaced (rebound away) for " + n + " and stays refused for it"
                            : b.key.equals(fp) ? "tenant " + tenant + " differs from the bound tenant " + b.tenant
                            : (follow && !b.follow ? "the name was learned strictly (" + (ROLE_SELF.equals(b.role) ? "this node's own" : "as a joiner or issuer")
                                    + ") and is bound to another key (" + PeerIdentity.shortFp(b.key) + "); a client exchange never replaces it"
                            : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")");
                    return new BindResult(false, why + "; if " + n + " was re-installed, authorise ONE rebind on this node: add the line '" + n
                            + "=" + fp + "' (its new key) to " + rebindFileHint() + " (no restart), or set peer_identity_rebind=" + n + "=" + fp);
                }
            }
            String newRole = (b != null && !ROLE_AGENT.equals(b.role) && ROLE_AGENT.equals(role)) ? b.role : role;
            boolean newFollow = token == null && vouched == null && (b == null ? follow : (replacesFollowed || (selfOverSelf && b.follow)));
            Rec r = rec("bind", n);
            r.tenant = tenant; r.key = fp; r.source = source; r.role = newRole; r.t = now; r.mode = newFollow ? "follow" : null;
            Rec c = null;
            if (token != null) {
                c = rec("consume", n);
                c.token = token; c.t = now;
            }
            // the binding and the token it consumes are one durable write; memory changes only after it
            if (!(c == null ? append(r) : append(r, c))) return new BindResult(false, "could not record the binding durably");
            if (token != null) consumed.add(token);
            replaced(n, b == null ? null : b.key, fp);
            if (b != null && !b.key.equals(fp)) replacedKey = b.key;
            putIdentity(n, new Binding(tenant, fp, null, source, newRole, newFollow,
                    b == null || token != null || !b.key.equals(fp) ? now : b.firstSeen, now));
            if (tomb != null) {
                tombs.remove(n);
                log.accept("Identity " + n + " (revoked: key " + PeerIdentity.shortFp(tomb.key) + ") REBOUND to key " + PeerIdentity.shortFp(fp)
                        + " by the operator ('" + token + "', now consumed); the revoked key stays revoked");
            } else if (b != null && !b.key.equals(fp)) {
                log.accept("Identity " + n + " REBOUND from key " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                        + (token != null ? " (operator rebind '" + token + "', now consumed)"
                        : vouched != null ? " (" + vouched + ")" : ROLE_SELF.equals(role) ? " (this node's own identity was replaced)"
                        : " (followed: the controller this node joined re-keyed)")
                        + "; the old key stays refused for " + n);
            }
            compactIfDue();
            result = new BindResult(true, b == null ? "new binding" : "rebound");
        }
        if (replacedKey != null) fireReplaced(n, replacedKey);
        return result;
    }

    /** In memory, with the write lock: {@code oldKey} (when any) is now refused for {@code n}; {@code newKey} is not. */
    private void replaced(String n, String oldKey, String newKey) {
        if (oldKey != null && !oldKey.equals(newKey)) revokedKeys.computeIfAbsent(n, k -> ConcurrentHashMap.newKeySet()).add(oldKey);
        Set<String> rk = revokedKeys.get(n);
        if (rk != null) { rk.remove(newKey); if (rk.isEmpty()) revokedKeys.remove(n); }
    }

    /** With the write lock (or while loading): bind {@code n}, keeping the key -> names index. */
    private void putIdentity(String n, Binding b) {
        Binding old = identities.put(n, b);
        if (old != null && !old.key.equals(b.key)) unindex(old.key, n);
        namesByKey.computeIfAbsent(b.key, k -> ConcurrentHashMap.newKeySet()).add(n);
    }

    /** With the write lock (or while loading): unbind {@code n} if it is still {@code b}. */
    private void removeIdentity(String n, Binding b) {
        if (identities.remove(n, b)) unindex(b.key, n);
    }

    private void unindex(String key, String n) {
        Set<String> s = namesByKey.get(key);
        if (s != null) { s.remove(n); if (s.isEmpty()) namesByKey.remove(key, s); }
    }

    /**
     * A controller this node PINNED re-keyed, and its new leaf is vouched for -- issued by the CA this node recorded for the
     * controller's region ({@link #issuedByRecordedCa}), or matching an operator-configured controller trust anchor (the
     * caller checks which): bind the new key in place of the pinned one, logged, as an operator rebind would. The old key
     * stays refused for the name. Never for a tombstoned (revoked) name, a revoked key, or this node's own name.
     */
    public BindResult rebindVouched(String tenant, String region, String agent, java.security.PublicKey key, String voucher) {
        if (!CrescoNames.isValidTenant(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || key == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        Binding b = identities.get(name(region, agent));
        if (b != null && !ROLE_CONTROLLER.equals(b.role)) return new BindResult(false, name(region, agent) + " is not a controller this node pinned");
        return bind(tenant, region, agent, key, "vouched", ROLE_CONTROLLER, false, voucher == null ? "vouched" : voucher);
    }

    /** Why this instance records nothing, for a refusal. */
    private String fencedText() {
        if (lockFailure != null) {
            return "the peer trust registry is FENCED: " + lockFailure + " -- nothing new is bound or recorded (what it holds is still"
                    + " served); stop the other process using " + (file == null ? "this registry" : file.getParent()) + " or give this node its"
                    + " own data directory, then restart it";
        }
        return "this peer trust registry instance records nothing (" + fenced + ")";
    }

    /** Why the registry could not take its file lock (it is fenced: nothing is recorded), or null. Reported as CRITICAL. */
    public String lockFailure() { return lockFailure; }

    private static boolean validName(String n) {
        int sl = n == null ? -1 : n.indexOf('/');
        return sl > 0 && CrescoNames.isValid(n.substring(0, sl)) && CrescoNames.isValid(n.substring(sl + 1));
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
        if (isRevokedFor(fp, region)) {
            return new BindResult(false, "the key " + PeerIdentity.shortFp(fp) + " is " + revokedText(fp, region) + "; a revoked node needs a new key");
        }
        Tomb tomb = tombs.get(n);
        Set<String> revoked = revokedKeys.get(n);
        boolean wasRevoked = revoked != null && revoked.contains(fp);
        if (tomb == null && ((b == null && !wasRevoked) || (b != null && b.key.equals(fp) && b.tenant.equals(tenant)))) return new BindResult(true, "free");
        synchronized (writeLock) { if (rebindToken(n, fp) != null) return new BindResult(true, "operator rebind pending"); }
        if (tomb != null) return new BindResult(false, tombText(n, tomb));
        return new BindResult(false, (b == null ? "this key was replaced (rebound away) for " + n
                : b.key.equals(fp) ? "tenant differs from the bound tenant " + b.tenant
                : "the name is bound to another key (" + PeerIdentity.shortFp(b.key) + ", since " + new java.util.Date(b.firstSeen) + ")")
                + "; if " + n + " was re-installed, authorise ONE rebind on this node: add the line '" + n + "=" + fp + "' (its new key) to "
                + rebindFileHint() + " (no restart), or set peer_identity_rebind=" + n + "=" + fp);
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
        return pin(region, agent, leaf, role, follow, "discovery");
    }

    /** As above, recording {@code source} (e.g. {@link #SOURCE_LEGACY}) on a new binding. */
    public String pin(String region, String agent, X509Certificate leaf, String role, boolean follow, String source) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return "invalid identity name " + region + "/" + agent;
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null || !region.equals(id.getRegion()) || !agent.equals(id.getAgent())) {
            return "the certificate names " + (id == null ? "no valid Cresco identity" : ("region=" + id.getRegion() + ", agent=" + id.getAgent()))
                    + ", not region=" + region + ", agent=" + agent;
        }
        BindResult r = bind(id.getTenant(), region, agent, leaf.getPublicKey(), source, role, follow);
        return r.ok ? null : r.reason;
    }

    /** The source of a binding made by the legacy (unauthenticated, replayable) discovery exchange of the upgrade window. */
    public static final String SOURCE_LEGACY = "legacy";

    /** Is {@code region/agent} an agent bound by the legacy exchange ({@link #SOURCE_LEGACY})? Lock-free. */
    public boolean isLegacyBinding(String region, String agent) {
        Binding b = identities.get(name(region, agent));
        return b != null && SOURCE_LEGACY.equals(b.source) && ROLE_AGENT.equals(b.role);
    }

    /**
     * The upgrade window: an agent whose name was bound by the legacy exchange (an earlier build, which keeps no key across
     * restarts) proved itself over the authenticated exchange with a new key: bind that key in place of the legacy one,
     * logged; the old key stays refused for the name. Only for an agent binding of {@code tenant} made by the legacy exchange
     * -- never a controller name, a strict binding, a tombstoned name or a revoked key. The caller checks the window is open.
     */
    public BindResult upgradeLegacy(String tenant, String region, String agent, java.security.PublicKey key) {
        if (!CrescoNames.isValidTenant(tenant) || !CrescoNames.isValid(region) || !CrescoNames.isValid(agent) || key == null) {
            return new BindResult(false, "invalid identity " + tenant + "/" + region + "/" + agent);
        }
        Binding b = identities.get(name(region, agent));
        if (b == null || !SOURCE_LEGACY.equals(b.source) || !ROLE_AGENT.equals(b.role)) {
            return new BindResult(false, name(region, agent) + " was not bound by the legacy exchange");
        }
        if (!b.tenant.equals(tenant)) return new BindResult(false, "tenant " + tenant + " differs from the bound tenant " + b.tenant);
        return bind(tenant, region, agent, key, "discovery", ROLE_AGENT, false, "upgraded from the legacy exchange");
    }

    /** The role {@code region/agent} is bound with when its bound key is {@code key}; else null. Lock-free. */
    public String boundRole(String region, String agent, java.security.PublicKey key) {
        if (key == null) return null;
        if (pki != null) {
            Binding own = identities.get(name(region, agent));
            if (own != null && ROLE_SELF.equals(own.role)) return own.key.equals(fingerprint(key.getEncoded())) ? ROLE_SELF : null;
            PkiSeen s = pkiSeenOf(null, region, agent);
            return s != null && s.key.equals(fingerprint(key.getEncoded())) ? s.role : null;
        }
        Binding b = identities.get(name(region, agent));
        return b != null && b.key.equals(fingerprint(key.getEncoded())) ? b.role : null;
    }

    /**
     * The role of {@code region/agent} as THIS chain carries it ({@code chain} as presented -- verified by the caller with
     * {@link #verifiedIdentity}): pre-provisioned, the PKI's role in that very certificate (SAN cresco:role:controller),
     * never a role remembered for the name from another certificate; otherwise the role bound to the chain's key.
     */
    public String boundRole(String region, String agent, X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return null;
        if (pki == null) return boundRole(region, agent, chain[0].getPublicKey());
        Binding own = identities.get(name(region, agent));
        String fp = fingerprint(chain[0].getPublicKey().getEncoded());
        if (own != null && ROLE_SELF.equals(own.role)) return own.key.equals(fp) ? ROLE_SELF : null;
        return pkiRoleOf(region, agent, chain[0]);
    }

    /** Pre-provisioned: the role {@code leaf} itself carries for {@code region/agent} (it names them, is not revoked), or null. */
    private String pkiRoleOf(String region, String agent, X509Certificate leaf) {
        CrescoIdentity id = leaf == null ? null : CrescoNames.identityOf(leaf);
        if (id == null || !id.getRegion().equals(region) || !id.getAgent().equals(agent)) return null;
        if (tombs.containsKey(name(region, agent)) || isRevokedFor(fingerprint(leaf.getPublicKey().getEncoded()), region)) return null;
        return ProvisionedIdentity.isController(leaf) ? ROLE_CONTROLLER : ROLE_AGENT;
    }

    /** The role {@code region/agent} is bound with here (when its tenant is {@code tenant}), or null. Lock-free. */
    public String roleOf(String tenant, String region, String agent) {
        if (pki != null) {
            Binding own = identities.get(name(region, agent));
            if (own != null && ROLE_SELF.equals(own.role)) return ROLE_SELF;
            PkiSeen s = pkiSeenOf(tenant, region, agent);
            return s == null ? null : s.role;
        }
        Binding b = identities.get(name(region, agent));
        return b == null || (tenant != null && !tenant.equals(b.tenant)) ? null : b.role;
    }

    /** The tenant bound to {@code region/agent}, or null. Lock-free. */
    public String boundTenant(String region, String agent) {
        if (pki != null) {
            Binding own = identities.get(name(region, agent));
            if (own != null && ROLE_SELF.equals(own.role)) return own.tenant;
            PkiSeen s = pkiSeenOf(null, region, agent);
            return s == null ? null : s.tenant;
        }
        Binding b = identities.get(name(region, agent));
        return b == null ? null : b.tenant;
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
        if (revoked.containsKey(fp)) return "the CA key " + PeerIdentity.shortFp(fp) + " is " + revokedText(fp, null);
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
            if (fenced != null) return fencedText();
            b = cas.get(region);
            if (b != null && b.key.equals(fp) && b.tenant.equals(tenant)) return null;
            String token = null;
            // a region whose CA key was revoked keeps that (revoked) CA recorded: no other CA is taken for it, not even by a
            // client following its region, until the operator rebinds the region
            if (b != null && (!(follow && b.follow) || revoked.containsKey(b.key))) {
                token = rebindToken(region, fp);
                if (token == null) {
                    return "region " + region + " already has a " + (b.follow ? "" : "strictly ") + "recorded CA (" + PeerIdentity.shortFp(b.key)
                            + (follow && !b.follow ? "; a client exchange never replaces it" : "") + "); if its controller was"
                            + " re-installed, authorise ONE rebind on this node: add the line '" + region + "=" + fp + "' (its new CA key) to "
                            + rebindFileHint();
                }
            }
            boolean newFollow = token == null && follow;
            Rec r = rec("ca", region);
            r.tenant = tenant; r.key = fp; r.cert = cert; r.source = source; r.t = now; r.mode = newFollow ? "follow" : null;
            Rec c = null;
            if (token != null) {
                c = rec("consume", region);
                c.token = token; c.t = now;
            }
            if (!(c == null ? append(r) : append(r, c))) return "could not record the CA durably";
            if (token != null) consumed.add(token);
            cas.put(region, new Binding(tenant, fp, cert, source, null, newFollow, now, now));
            if (b != null) log.accept("CA for region " + region + " REBOUND from " + PeerIdentity.shortFp(b.key) + " to " + PeerIdentity.shortFp(fp)
                    + (token != null ? " (operator rebind '" + token + "', now consumed)" : " (followed: the region this node joined re-keyed)"));
            compactIfDue();
        }
        return null;
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
                cas.remove(region);                               // fail-safe: a refused peer's CA is gone from memory at once
                if (!append(rec("unca", region))) log.accept("CA for region " + region + " removed in memory; the removal could not be recorded");
            } else if (state instanceof Binding && now != state) {
                Binding b = (Binding) state;
                Rec r = rec("ca", region);
                r.tenant = b.tenant; r.key = b.key; r.cert = b.cert; r.source = b.source; r.t = b.lastSeen; r.mode = b.follow ? "follow" : null;
                if (append(r)) cas.put(region, b);
                else { cas.remove(region); log.accept("CA for region " + region + " could not be restored durably; none is trusted for it now"); }
            }
            log.accept("CA for region " + region + " restored (the exchange that changed it was refused)");
        }
    }

    /** The CA certificates recorded for regions (to add to a truststore after a restart); never a revoked CA. */
    public List<X509Certificate> recordedCas() {
        List<X509Certificate> out = new ArrayList<>();
        for (Binding b : cas.values()) {
            if (revoked.containsKey(b.key)) continue;
            X509Certificate c = b.caCert();
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
        if (isRevokedFor(fp, id.getRegion())) { why[0] = "the certificate's key is " + revokedText(fp, id.getRegion()); return null; }
        String issuerWhy = revokedIssuer(chain);
        if (issuerWhy != null) { why[0] = issuerWhy; return null; }
        try { leaf.checkValidity(); } catch (Exception e) { why[0] = "the certificate is not valid now (" + e.getMessage() + ")"; return null; }
        String n = name(id.getRegion(), id.getAgent());
        Tomb tomb = tombs.get(n);
        Pki p = pki;
        if (tomb != null) {
            if (p == null) { why[0] = tombText(n, tomb); return null; }
            // pre-provisioned: an operator rebind lifts the tombstone (a one-shot, recorded act) only for a chain the PKI accepts
            // in full -- judged FIRST, so a chain that would be refused (unanchored, out of scope, revoked, a stale CRL) never
            // consumes the rebind or lifts anything
            String bad = pkiRefusal(p, chain);
            if (bad != null) { why[0] = bad; return null; }
            if (!liftTombByRebind(n, fp)) { why[0] = tombText(n, tomb); return null; }
        }
        if (p != null) {
            // pre-provisioned: the operator's PKI decides (this node's own identity is its own, from the same PKI)
            return pkiVerify(p, chain, id, fp, why);
        }
        Binding ident = identities.get(n);
        Binding ca = cas.get(id.getRegion());
        boolean caGoverned = ca != null && ca.tenant.equals(id.getTenant());
        if (caGoverned && revoked.containsKey(ca.key)) {
            why[0] = "region " + id.getRegion() + "'s CA key is " + revokedText(ca.key, null) + ": nothing of that region is accepted until the"
                    + " operator rebinds its CA";
            return null;
        }
        if (ident != null) {
            if (ident.key.equals(fp) && ident.tenant.equals(id.getTenant())) {
                // under a region CA a name's identity is the CA's to issue: a chain its key holder made itself is refused, so an
                // identity that is no longer re-enrolled (a node gone, refused, or its key stolen) lapses with its last leaf
                if (caGoverned && !signedBy(leaf, ca.caCert())) {
                    why[0] = "the certificate names " + id.getAgentPath() + " with its bound key, but was not issued by region "
                            + id.getRegion() + "'s CA (a self-made chain is refused where a region CA issues identities)";
                    return null;
                }
                touch(identities, n, ident, System.currentTimeMillis(), "id");   // in use: keeps it from being pruned (no I/O here)
                return id;
            }
            why[0] = "the certificate names " + id.getAgentPath() + " but carries a different key than the one bound to it ("
                    + PeerIdentity.shortFp(ident.key) + ")";
            return null;
        }
        Set<String> replacedForName = revokedKeys.get(n);
        if (replacedForName != null && replacedForName.contains(fp)) {
            why[0] = "the certificate names " + id.getAgentPath() + " but its key was replaced (rebound away) for that name";
            return null;
        }
        if (caGoverned) {
            X509Certificate caCert = ca.caCert();
            if (caCert != null && leaf.getIssuerX500Principal().equals(caCert.getSubjectX500Principal())) {
                if (signedBy(leaf, caCert)) return id;
                why[0] = "the certificate names " + id.getAgentPath() + " but was not signed by region " + id.getRegion() + "'s CA";
                return null;
            }
            why[0] = "the certificate names " + id.getAgentPath() + " but was not issued by region " + id.getRegion() + "'s CA";
            return null;
        }
        why[0] = "no key or CA is recorded for " + id.getAgentPath() + " (tenant " + id.getTenant() + ")";
        return null;
    }

    /** Was {@code leaf} issued (named and signed) by {@code caCert}? */
    private static boolean signedBy(X509Certificate leaf, X509Certificate caCert) {
        if (caCert == null || !leaf.getIssuerX500Principal().equals(caCert.getSubjectX500Principal())) return false;
        try {
            leaf.verify(caCert.getPublicKey());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Is {@code chain}'s leaf (valid now, naming a valid identity) issued by the CA recorded here for the region and tenant it
     * names, that CA's key not revoked and the name not tombstoned? What vouches for a re-keyed controller of that region.
     */
    public boolean issuedByRecordedCa(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return false;
        X509Certificate leaf = chain[0];
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) return false;
        try { leaf.checkValidity(); } catch (Exception e) { return false; }
        String fp = fingerprint(leaf.getPublicKey().getEncoded());
        if (isRevokedFor(fp, id.getRegion()) || revokedIssuer(chain) != null || tombs.containsKey(name(id.getRegion(), id.getAgent()))) return false;
        Binding ca = cas.get(id.getRegion());
        return ca != null && ca.tenant.equals(id.getTenant()) && !revoked.containsKey(ca.key) && signedBy(leaf, ca.caCert());
    }

    /**
     * The identity {@code chain}'s leaf names, when its key is the one BOUND to that name (an identity binding; not the CA
     * path) and the leaf is within its validity period; else null. A TLS peer that completes the handshake holds that key,
     * so this is as strong as a pin: it lets the broker accept a bound peer's self-rooted chain after a restart, when the
     * in-memory truststore no longer holds it. Where a region CA is recorded for the name's region the leaf must also be
     * issued by that CA (never a chain the key holder made itself).
     */
    public CrescoIdentity boundByKey(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null || pki != null) return null;   // pre-provisioned: the PKI only
        X509Certificate leaf = chain[0];
        try { leaf.checkValidity(); } catch (Exception e) { return null; }
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        if (id == null) return null;
        String fp = fingerprint(leaf.getPublicKey().getEncoded());
        if (isRevokedFor(fp, id.getRegion()) || revokedIssuer(chain) != null) return null;
        String n = name(id.getRegion(), id.getAgent());
        if (tombs.containsKey(n)) return null;
        Binding ident = identities.get(n);
        if (ident == null || !ident.tenant.equals(id.getTenant()) || !ident.key.equals(fp)) return null;
        Binding ca = cas.get(id.getRegion());
        if (ca != null && ca.tenant.equals(id.getTenant()) && (revoked.containsKey(ca.key) || !signedBy(leaf, ca.caCert()))) return null;
        return id;
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
            if (fenced != null) return;
            if (bridges.containsKey(n)) return;
            Rec r = rec("bridge", n);
            r.t = now;
            if (append(r)) bridges.put(n, new Binding(null, null, null, "discovery", null, false, now, now));
            compactIfDue();
        }
    }

    /** Stop accepting bridges from {@code region/agent} (until a discovery exchange allows it again). */
    public boolean revokeBridgePeer(String region, String agent) {
        String n = name(region, agent);
        synchronized (writeLock) {
            if (bridges.remove(n) == null) return false;            // fail-safe: refused from now on, recorded or not
            if (!append(rec("unbridge", n))) {
                log.accept("Bridge peer " + n + " revoked in memory, but the revocation could not be recorded: it applies until a restart");
                return false;
            }
            compactIfDue();
        }
        return true;
    }

    /** Is {@code id} a recorded bridge peer (not revoked, not expired)? Lock-free; no I/O. */
    /**
     * May the connection of {@code id}, which presented {@code leaf} (verified at connect), bridge? Pre-provisioned, by the
     * role in THAT certificate -- never by a role remembered for the name from another connection's certificate; otherwise as
     * {@link #isBridgePeer(CrescoIdentity)}.
     */
    public boolean isBridgePeer(CrescoIdentity id, X509Certificate leaf) {
        if (pki == null) return isBridgePeer(id);
        if (id == null || leaf == null || !CrescoNames.isValid(id.getRegion()) || !CrescoNames.isValid(id.getAgent())) return false;
        CrescoIdentity named = CrescoNames.identityOf(leaf);
        if (named == null || !named.getTenant().equals(id.getTenant())) return false;
        return ROLE_CONTROLLER.equals(pkiRoleOf(id.getRegion(), id.getAgent(), leaf));
    }

    public boolean isBridgePeer(CrescoIdentity id) {
        if (id == null || !CrescoNames.isValid(id.getRegion()) || !CrescoNames.isValid(id.getAgent())) return false;
        if (pki != null) {
            // pre-provisioned: a controller is whoever the PKI says is one (SAN cresco:role:controller), verified on its connection
            PkiSeen s = pkiSeenOf(id.getTenant(), id.getRegion(), id.getAgent());
            return s != null && ROLE_CONTROLLER.equals(s.role);
        }
        String n = name(id.getRegion(), id.getAgent());
        if (revokedBridges.contains(n)) return false;
        Binding b = bridges.get(n);
        return b != null && (bridgePruneMs <= 0 || System.currentTimeMillis() - b.lastSeen <= bridgePruneMs);
    }

    /**
     * Did {@code region/agent} of {@code tenant} join this node as a controller: a recorded bridge peer (a REGION/GLOBAL
     * discovery exchange with this node), or a name bound here as a controller it joined as (not one this node followed)?
     * A name bound to another tenant never is. Lock-free; no I/O. (The global asks it of a registry update's caller: only
     * a region's own controller speaks for its region.)
     */
    public boolean isJoinedController(String tenant, String region, String agent) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return false;
        if (pki != null) {
            PkiSeen s = pkiSeenOf(tenant, region, agent);
            return s != null && ROLE_CONTROLLER.equals(s.role);
        }
        String n = name(region, agent);
        Binding id = identities.get(n);
        if (id != null && (tenant == null || !tenant.equals(id.tenant))) return false;
        if (!revokedBridges.contains(n)) {
            Binding b = bridges.get(n);
            if (b != null && (bridgePruneMs <= 0 || System.currentTimeMillis() - b.lastSeen <= bridgePruneMs)) return true;
        }
        return id != null && ROLE_CONTROLLER.equals(id.role) && !id.follow;
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

    /**
     * A verified connection for {@code id} is in use: refresh its binding's last-seen (in memory; persisted in the
     * background at most daily), so a peer that stays connected is never pruned. Lock-free; no I/O.
     */
    public void touchIdentity(CrescoIdentity id) {
        if (id == null || id.getRegion() == null || id.getAgent() == null) return;
        String n = name(id.getRegion(), id.getAgent());
        Binding b = identities.get(n);
        if (b != null && b.tenant != null && b.tenant.equals(id.getTenant())) touch(identities, n, b, System.currentTimeMillis(), "id");
    }

    /**
     * Why TLS must refuse {@code chain} even though it may chain to an anchor, or null: its leaf names a Cresco identity
     * whose bound key is another one, or whose key was replaced (rebound away) for that name. Lock-free; no I/O.
     */
    public String refusedKey(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return null;
        String fp = fingerprint(chain[0].getPublicKey().getEncoded());
        CrescoIdentity id = CrescoNames.identityOf(chain[0]);
        String region = id == null ? null : id.getRegion();
        if (isRevokedFor(fp, region)) return "the certificate's key is " + revokedText(fp, region);
        String issuerWhy = revokedIssuer(chain);
        if (issuerWhy != null) return issuerWhy;
        if (id == null) return null;
        String n = name(id.getRegion(), id.getAgent());
        Tomb tomb = tombs.get(n);
        if (tomb != null) return tombText(n, tomb);
        Pki p = pki;
        if (p != null) {
            String r = p.rev.refusal(chain);
            if (r != null) return "the certificate is REVOKED by the operator (" + r + ")";
            String v = pkiCheck(p, chain);
            return v == null ? null : "the certificate " + v;
        }
        Binding b = identities.get(n);
        if (b != null && !b.key.equals(fp)) {
            return "the certificate names " + n + " but carries another key than the one bound to it (" + PeerIdentity.shortFp(b.key) + ")";
        }
        Set<String> replacedForName = revokedKeys.get(n);
        if (replacedForName != null && replacedForName.contains(fp)) return "the certificate names " + n + " but its key was replaced (rebound away) for it";
        Binding ca = cas.get(id.getRegion());
        if (ca != null && ca.tenant.equals(id.getTenant())) {
            if (revoked.containsKey(ca.key)) return "region " + id.getRegion() + "'s CA key is REVOKED";
            if (b != null && !signedBy(chain[0], ca.caCert())) {
                return "the certificate names " + n + " with its bound key, but was not issued by region " + id.getRegion() + "'s CA";
            }
        }
        return null;
    }

    // ---------------------------------------------------------------- revocation

    /** Fingerprints of newly revoked keys, to announce (outside the write lock) once their bindings are dropped. */
    private final java.util.concurrent.ConcurrentLinkedQueue<String> revokedToFire = new java.util.concurrent.ConcurrentLinkedQueue<>();
    /** Revocation records whose durable write failed, retried in order (with the write lock). */
    private final List<Rec> revUndurable = new ArrayList<>();

    private static boolean isFingerprint(String s) {
        return s != null && s.matches("[0-9a-fA-F]{64}");
    }

    /** Is the key {@code fp} revoked for an identity of {@code region} (for every name, or for that region's names)? Lock-free. */
    public boolean isRevokedFor(String fp, String region) {
        if (fp == null) return false;
        return revoked.containsKey(fp) || (region != null && revoked.containsKey(fp + "@" + region));
    }

    private String revokedText(String fp, String region) {
        Revocation r = revoked.get(fp);
        if (r == null && region != null) r = revoked.get(fp + "@" + region);
        return r == null ? "REVOKED" : "REVOKED (" + r.source + (r.scope == null ? "" : "; for the names of region " + r.scope) + ")";
    }

    /** Why a CA in {@code chain} (any certificate above the leaf) is revoked, or null. */
    private String revokedIssuer(X509Certificate[] chain) {
        if (chain == null || revoked.isEmpty()) return null;
        for (int i = 1; i < chain.length; i++) {
            if (chain[i] == null) continue;
            String f = fingerprint(chain[i].getPublicKey().getEncoded());
            Revocation r = revoked.get(f);
            if (r != null) return "the certificate's issuer '" + chain[i].getSubjectX500Principal().getName() + "' has a REVOKED key (" + r.source + ")";
        }
        return null;
    }

    private static String tombText(String n, Tomb t) {
        return "the name " + n + " was REVOKED (" + t.source + ", key " + PeerIdentity.shortFp(t.key) + ") and is reserved: no key is"
                + " accepted for it until the operator rebinds it (add the line '" + n + "=<new key fingerprint>' to the node's"
                + " peer-trust.json.rebind, or set peer_identity_rebind)";
    }

    /** Is {@code fp} this node's own key (an identity of role self) or its own region CA's key? */
    private boolean isOwnKey(String fp) {
        Set<String> names = namesByKey.get(fp);
        if (names != null) {
            for (String n : names) {
                Binding b = identities.get(n);
                if (b != null && ROLE_SELF.equals(b.role) && fp.equals(b.key)) return true;
            }
        }
        for (Binding c : cas.values()) if (fp.equals(c.key) && "own".equals(c.source)) return true;
        return false;
    }

    /** Is {@code fp} the key of a CA recorded for a region? */
    private boolean isCaKey(String fp) {
        for (Binding c : cas.values()) if (fp.equals(c.key)) return true;
        return false;
    }

    /**
     * Revoke an identity: {@code region/agent} (its bound key) or a key fingerprint (SHA-256 of the SubjectPublicKeyInfo,
     * hex). The key is refused from now on for EVERY name -- at TLS, at the broker and on the CA path, as a leaf and as an
     * issuer (a CA key) -- the bindings that hold it are dropped and their names TOMBSTONED (refused for every key until an
     * operator rebind), live connections holding it are closed (listener), and this node's signed revocation list carries
     * it to its parent and children. Permanent: a revoked node gets a new key and its name is rebound by the operator.
     *
     * <p>Fail closed: the revocation is in force in memory BEFORE it is recorded. When it cannot be recorded (full disk, a
     * sealed or fenced registry) it stays in force, the failure is reported (CRITICAL health, {@link #revocationDurability})
     * and the write is retried until it succeeds. This node's own key (and its own region CA) is never revoked.
     *
     * @return null when revoked (or already revoked), else why not
     */
    public String revoke(String target, String source) {
        String why;
        synchronized (writeLock) {
            if (sealed) tryRecover();
            why = revokeLocked(target, source);
            completeRevocationsLocked();
        }
        fireRevokedPending();
        return why;
    }

    /** With the write lock: apply the revocation in memory (its bindings are dropped by {@link #completeRevocationsLocked}). */
    private String revokeLocked(String target, String source) {
        if (target == null || target.isBlank()) return "nothing to revoke";
        target = target.trim();
        String fp, n = null;
        if (isFingerprint(target)) {
            fp = target.toLowerCase(java.util.Locale.ROOT);
            if (isOwnKey(fp)) {
                return "the key " + PeerIdentity.shortFp(fp) + " is this node's own (its identity, or its own region CA): a node never revokes"
                        + " itself -- re-key it, then revoke the old key at its peers";
            }
            Set<String> names = namesByKey.get(fp);
            if (names != null) for (String x : names) { n = x; break; }
        } else {
            int slash = target.indexOf('/');
            if (slash <= 0 || !CrescoNames.isValid(target.substring(0, slash)) || !CrescoNames.isValid(target.substring(slash + 1))) {
                return "'" + target + "' is neither a region/agent name nor a key fingerprint";
            }
            n = target;
            Binding b = identities.get(n);
            if (b == null && pki != null && !tombs.containsKey(n)) {
                // pre-provisioned: names are not bound; the name is reserved (refused for every key), and its last verified key revoked
                PkiSeen seen = pkiSeen.get(n);
                tombstoneLocked(n, seen == null ? null : seen.key, source);
                log.accept("Name " + n + " REVOKED (" + source + "): refused for every key until an operator rebind");
                if (seen == null || revoked.containsKey(seen.key)) return null;
                applyInMemory(seen.key, null, n, source, 0, true);
                return null;
            }
            if (b == null) {
                if (tombs.containsKey(n)) return null;                      // already revoked (and tombstoned)
                return "no key is bound to " + n + " (revoke its key fingerprint instead)";
            }
            if (ROLE_SELF.equals(b.role)) return n + " is this node's own identity";
            fp = b.key;
        }
        if (revoked.containsKey(fp)) return null;
        if (applyInMemory(fp, null, n, source, expiryFor(n), true)) {
            log.accept("Key " + PeerIdentity.shortFp(fp) + (n == null ? "" : " of " + n) + " REVOKED (" + source + "): refused for every"
                    + " name from now on" + (n == null ? "" : "; the name is reserved until an operator rebind"));
        }
        return null;
    }

    /**
     * The longest validity any Cresco region CA has given a leaf: three years (YEARS_VALID) before short-lived leaves (and a
     * day for a leap year). A revocation of a key a region CA enrolled is kept at least this long.
     */
    static final long LONGEST_LEAF_MS = (3 * 365L + 1) * DAY;

    /**
     * When a revocation of the key bound to {@code n} can no longer matter: never (0), unless this node's region CA ENROLLED
     * that key. Then it expires only after the latest notAfter ANY certificate for that key could carry: a leaf this CA
     * issued before short-lived leaves lived three years, and the operator may change {@code identity_leaf_validity_hours};
     * so the longest of those ({@link #LONGEST_LEAF_MS}, or the current leaf lifetime if longer) plus twice the backdate and
     * a day. The CA issues nothing for a revoked key, every verifier requires a CA-issued leaf within its validity for that
     * region's names, and the name stays tombstoned everywhere the revocation went.
     */
    private long expiryFor(String n) {
        if (issuedLeafLifetimeMs <= 0 || n == null) return 0;
        Binding b = identities.get(n);
        if (b == null || !"enrolled".equals(b.source)) return 0;
        return System.currentTimeMillis() + Math.max(issuedLeafLifetimeMs, LONGEST_LEAF_MS) + 2 * CertValidity.backdateMs() + DAY;
    }

    /** The lifetime of the leaves this node's region CA issues (enables expiry of revocations of keys it enrolled). */
    public void setIssuedLeafLifetime(long ms) { this.issuedLeafLifetimeMs = Math.max(0, ms); }

    /**
     * Put a revocation in force in memory (lock-free for readers; no I/O): it is refused from this moment. Its bindings,
     * name tombstones and durable record follow in {@link #completeRevocationsLocked}. False when already in force.
     */
    private boolean applyInMemory(String fp, String scope, String name, String source, long exp, boolean own) {
        synchronized (revMemLock) {
            if (revoked.containsKey(fp) || (scope != null && revoked.containsKey(fp + "@" + scope))) return false;
            Revocation r = new Revocation(fp, scope, name, source == null ? "?" : source, ++revSeq, System.currentTimeMillis(), exp, own);
            putRevocation(r);
            revPending.addLast(r);
            return true;
        }
    }

    /** In memory (with revMemLock, or while loading). */
    private void putRevocation(Revocation r) {
        Revocation old = revoked.put(r.id(), r);
        if (old != null) revokedBySeq.remove(old.seq, old);
        else revokedAnyScope.merge(r.fp, 1, Integer::sum);
        revokedBySeq.put(r.seq, r);
        if (r.own && r.seq > ownRevSeq) ownRevSeq = r.seq;
        revVersion++;
    }

    /** In memory (with revMemLock, or while loading). */
    private void removeRevocation(Revocation r) {
        if (!revoked.remove(r.id(), r)) return;
        revokedBySeq.remove(r.seq, r);
        revokedAnyScope.computeIfPresent(r.fp, (k, v) -> v <= 1 ? null : v - 1);
        revVersion++;
    }

    /**
     * With the write lock: for every revocation put in force since the last call, drop the bindings that hold its key
     * (within its scope) and tombstone their names (and stop accepting bridges from them); then record all of it in ONE
     * durable write. A write that fails is kept and retried (the revocations stay in force; CRITICAL until recorded).
     */
    private void completeRevocationsLocked() {
        List<Revocation> batch;
        synchronized (revMemLock) {
            if (revPending.isEmpty() && revUndurable.isEmpty()) return;
            batch = new ArrayList<>(revPending);
            revPending.clear();
        }
        List<Rec> recs = new ArrayList<>();
        for (Revocation r : batch) {
            Rec rr = rec("revokekey", r.name);
            rr.key = r.fp; rr.source = r.source; rr.t = r.t; rr.scope = r.scope; rr.seq = r.seq;
            rr.exp = r.exp > 0 ? r.exp : null; rr.own = r.own ? Boolean.TRUE : null;
            recs.add(rr);
            Set<String> names = namesByKey.get(r.fp);
            if (names != null) {
                for (String n : new ArrayList<>(names)) {
                    Binding b = identities.get(n);
                    if (b == null || !r.fp.equals(b.key) || ROLE_SELF.equals(b.role)) continue;
                    if (r.scope != null && !n.startsWith(r.scope + "/")) continue;
                    removeIdentity(n, b);
                    tombs.put(n, new Tomb(r.fp, r.source, r.t));
                    Rec tr = rec("tomb", n);
                    tr.key = r.fp; tr.source = r.source; tr.t = r.t;
                    recs.add(tr);
                    if (bridges.remove(n) != null) recs.add(rec("unbridge", n));
                }
            }
            // the name the revocation names is reserved here too, where the revocation applies (not only at the node that bound
            // it): once the entry expires, a leaf that key could still present (a CA-issued one, verified here by the CA path)
            // is still refused -- unless this node binds that name to ANOTHER key (rebound before the entry arrived)
            if (r.name != null && !tombs.containsKey(r.name) && (r.scope == null || r.name.startsWith(r.scope + "/")) && validName(r.name)) {
                Binding b = identities.get(r.name);
                if (b == null) {
                    tombs.put(r.name, new Tomb(r.fp, r.source, r.t));
                    Rec tr = rec("tomb", r.name);
                    tr.key = r.fp; tr.source = r.source; tr.t = r.t;
                    recs.add(tr);
                }
            }
            revokedToFire.add(r.fp);
        }
        revUndurable.addAll(recs);
        writeUndurableLocked();
    }

    /** While loading (with the write lock): a recorded revocation; the bindings holding its key are dropped, names tombstoned. */
    private void loadRevocation(Rec r) {
        String fp = r.key.toLowerCase(java.util.Locale.ROOT);
        String scope = r.scope != null && CrescoNames.isValid(r.scope) ? r.scope : null;
        String source = r.source == null ? "?" : r.source;
        synchronized (revMemLock) {
            if (revoked.containsKey(scope == null ? fp : fp + "@" + scope)) return;
            long seq = r.seq != null && r.seq > 0 ? r.seq : revSeq + 1;
            if (seq > revSeq) revSeq = seq;
            // a record from before revocation lists had positions: made here unless it came from a peer's list
            boolean own = r.seq == null ? !source.startsWith("revocation list") : Boolean.TRUE.equals(r.own);
            putRevocation(new Revocation(fp, scope, r.name, source, seq, r.t, r.exp == null ? 0 : r.exp, own));
            if (seq > durableRevSeq) durableRevSeq = seq;            // what is in the log is durable
        }
        Set<String> names = namesByKey.get(fp);
        if (names == null) return;
        for (String n : new ArrayList<>(names)) {
            Binding b = identities.get(n);
            if (b == null || !fp.equals(b.key) || ROLE_SELF.equals(b.role)) continue;
            if (scope != null && !n.startsWith(scope + "/")) continue;
            removeIdentity(n, b);
            tombs.putIfAbsent(n, new Tomb(fp, source, r.t));
        }
    }

    /** With the write lock: record the revocations still waiting for a durable write (one write), or schedule a retry. */
    private void writeUndurableLocked() {
        if (revUndurable.isEmpty()) return;
        long maxSeq = 0;
        for (Rec r : revUndurable) if (r.seq != null) maxSeq = Math.max(maxSeq, r.seq);
        if (append(revUndurable.toArray(new Rec[0]))) {
            revUndurable.clear();
            if (maxSeq > durableRevSeq) { durableRevSeq = maxSeq; writeHighWater(); }
            if (revDurability != null) log.accept("peer trust registry: the revocations in force are recorded durably again");
            revDurability = null;
            revRetryMs = 1000;
            compactIfDue();
            return;
        }
        String why = sealed ? "the registry is SEALED (" + sealReason + ")" : fenced != null ? "the registry records nothing (" + fenced + ")"
                : "the write failed";
        String text = revUndurable.size() + " revocation record(s) are IN FORCE but NOT recorded durably: " + why
                + "; retrying (a restart before they are recorded re-applies peer_identity_revoke and the operator file, and peers"
                + " send their lists again)";
        if (!text.equals(revDurability)) log.accept("PEER TRUST REGISTRY: " + text);
        revDurability = text;
        scheduleRevocationWork(revRetryMs);
        revRetryMs = Math.min(30_000L, revRetryMs * 2);
    }

    /** Run {@link #completeRevocationsLocked} on the background writer (now, or after {@code delayMs}); inline without one. */
    private void scheduleRevocationWork(long delayMs) {
        if (background == null) {
            if (delayMs == 0) {
                synchronized (writeLock) { completeRevocationsLocked(); }
                fireRevokedPending();
            }
            return;
        }
        if (delayMs == 0 || revFlushScheduled.compareAndSet(false, true)) {
            Runnable task = () -> {
                if (delayMs != 0) revFlushScheduled.set(false);
                try {
                    synchronized (writeLock) { completeRevocationsLocked(); }
                    fireRevokedPending();
                } catch (Exception e) {
                    log.accept("peer trust registry: completing revocations failed: " + e.getMessage());
                }
            };
            try {
                if (delayMs == 0) background.execute(task);
                else background.schedule(task, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                if (delayMs != 0) revFlushScheduled.set(false);
            }
        }
    }

    /** Why some revocations in force are not recorded durably (reported as CRITICAL), or null. */
    public String revocationDurability() { return revDurability; }

    /** Tests: wait until every revocation put in force so far is processed (bindings, tombstones, durable write attempted). */
    void awaitRevocations() {
        if (background == null) return;
        try { background.submit(() -> { }).get(10, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception ignore) { }
    }

    private void fireRevokedPending() {
        List<String> fire = new ArrayList<>();
        for (String f; (f = revokedToFire.poll()) != null; ) fire.add(f);
        if (fire.isEmpty()) return;
        Consumer<String> l = revokedListener;
        if (l != null) {
            for (String fp : fire) {
                try { l.accept(fp); } catch (Exception e) { log.accept("peer trust registry: revocation listener failed: " + e.getMessage()); }
            }
        }
        Consumer<Collection<String>> bl = revokedBatchListener;
        if (bl != null) {
            try { bl.accept(fire); } catch (Exception e) { log.accept("peer trust registry: revocation listener failed: " + e.getMessage()); }
        }
    }

    /** Called (outside the write lock) with each newly revoked key fingerprint. */
    public void setRevokedListener(Consumer<String> l) { this.revokedListener = l; }

    /** Called (outside the write lock) once per batch of newly revoked key fingerprints (after their bindings are dropped). */
    public void setRevokedBatchListener(Consumer<Collection<String>> l) { this.revokedBatchListener = l; }

    /** Is the key with this fingerprint revoked (for every name, or for some region's names)? Lock-free. */
    public boolean isRevokedKey(String fp) { return fp != null && revokedAnyScope.containsKey(fp); }

    // ---------------------------------------------------------------- pre-provisioned PKI (security_profile=production)

    /** The operator's PKI this node trusts instead of bindings, when it is pre-provisioned (null otherwise). */
    private volatile Pki pki;

    private static final class Pki {
        final java.util.List<X509Certificate> anchors;
        final ProvisionedIdentity.Revocations rev;
        /** Leaf certificate SHA-256 -> its validated path (leaf .. anchor) under THESE anchors (signatures and scope hold). */
        final Map<String, java.util.List<X509Certificate>> paths = new ConcurrentHashMap<>();
        Pki(java.util.List<X509Certificate> anchors, ProvisionedIdentity.Revocations rev) { this.anchors = anchors; this.rev = rev; }
    }
    static final int PKI_PATHS_MAX = 200_000;

    /**
     * Why {@code chain} is refused by the PKI now, or null: its validated path to the anchors (cached per leaf certificate
     * for these anchors: signatures and CA scopes do not change) and then, every time, the validity of EVERY certificate on
     * that path, the anchor included, and the operator's revocations of any of them.
     */
    private static String pkiCheck(Pki p, X509Certificate[] chain) {
        String cfp;
        try { cfp = CertificateManager.certFingerprint(chain[0]); } catch (Exception e) { return "is unreadable"; }
        java.util.List<X509Certificate> path = p.paths.get(cfp);
        java.util.Date now = new java.util.Date();
        if (path == null) {
            ProvisionedIdentity.Validated v = ProvisionedIdentity.validatePath(chain, p.anchors, now);
            if (v.refusal != null) return v.refusal;
            path = v.path;
            if (p.paths.size() < PKI_PATHS_MAX) p.paths.put(cfp, path);
        }
        return ProvisionedIdentity.pathRefusal(path, now, p.rev);
    }

    /** What PKI verification learned of a name: its tenant, role (controller or agent) and key, when last verified. */
    private static final class PkiSeen {
        final String tenant, role, key; final long t;
        PkiSeen(String tenant, String role, String key, long t) { this.tenant = tenant; this.role = role; this.key = key; this.t = t; }
    }
    private final Map<String, PkiSeen> pkiSeen = new ConcurrentHashMap<>();
    static final int PKI_SEEN_MAX = 200_000;

    /**
     * Trust the operator's PKI: a certificate is an identity when its chain validates to {@code anchors} now (PKIX, CA scope)
     * and neither the operator's revocations ({@code rev}) nor this node's revocation lists nor a tombstone refuse it. Names
     * are then never bound first come: bindings, rebinds and per-region CAs play no part (this node's own identity excepted).
     */
    public void setPki(java.util.List<X509Certificate> anchors, ProvisionedIdentity.Revocations rev) {
        this.pki = new Pki(java.util.List.copyOf(anchors), rev == null ? ProvisionedIdentity.Revocations.NONE : rev);
        for (String n : rev == null ? java.util.Set.<String>of() : rev.names) {
            if (!tombs.containsKey(n) && validName(n)) {
                synchronized (writeLock) { tombstoneLocked(n, null, "operator revocation_file"); }
            }
        }
    }

    /** Is this node trusting a pre-provisioned PKI? */
    public boolean isPki() { return pki != null; }

    /** With the write lock: reserve {@code n} (its key, when known, was revoked); recorded. */
    private void tombstoneLocked(String n, String key, String source) {
        if (tombs.containsKey(n)) return;
        long t = System.currentTimeMillis();
        tombs.put(n, new Tomb(key, source, t));
        Rec tr = rec("tomb", n);
        tr.key = key; tr.source = source; tr.t = t;
        if (!append(tr)) log.accept("peer trust registry: the tombstone of " + n + " is in force but could not be recorded");
        pkiSeen.remove(n);
    }

    /**
     * Pre-provisioned: an operator rebind token for the tombstoned name {@code n} that allows key {@code fp} lifts the
     * tombstone (consumed, recorded): the PKI issued the name a new certificate. The revoked key stays revoked.
     */
    private boolean liftTombByRebind(String n, String fp) {
        synchronized (writeLock) {
            if (!tombs.containsKey(n)) return true;
            String token = rebindToken(n, fp);
            if (token == null) return false;
            Rec c = rec("consume", n);
            c.token = token; c.t = System.currentTimeMillis();
            Rec u = rec("untomb", n);
            u.key = fp; u.t = c.t;
            if (!append(c, u)) return false;
            consumed.add(token);
            tombs.remove(n);
            log.accept("Name " + n + " (revoked) REBOUND by the operator ('" + token + "', now consumed): its new certificate (key "
                    + PeerIdentity.shortFp(fp) + ") is accepted");
            return true;
        }
    }

    /** PKI verification of {@code chain} (revocations, tombstones and validity were checked by the caller). */
    private CrescoIdentity pkiVerify(Pki p, X509Certificate[] chain, CrescoIdentity id, String fp, String[] why) {
        String bad = pkiRefusal(p, chain);
        if (bad != null) { why[0] = bad; return null; }
        String n = name(id.getRegion(), id.getAgent());
        String role = ProvisionedIdentity.isController(chain[0]) ? ROLE_CONTROLLER : ROLE_AGENT;
        PkiSeen prev = pkiSeen.get(n);
        long now = System.currentTimeMillis();
        if (prev == null || !prev.key.equals(fp) || !prev.role.equals(role) || now - prev.t > 60_000L) {
            if (prev != null || pkiSeen.size() < PKI_SEEN_MAX) pkiSeen.put(n, new PkiSeen(id.getTenant(), role, fp, now));
        }
        return id;
    }

    /** Why the PKI refuses {@code chain} (the operator's revocations, the path to the anchors and its scopes), or null. */
    private String pkiRefusal(Pki p, X509Certificate[] chain) {
        String r = p.rev.refusal(chain);
        if (r != null) return "the certificate is REVOKED by the operator (" + r + ")";
        String v = pkiCheck(p, chain);
        return v == null ? null : "the certificate " + v;
    }

    /** Pre-provisioned: what PKI verification learned of {@code region/agent} (tenant matching), or null. */
    private PkiSeen pkiSeenOf(String tenant, String region, String agent) {
        PkiSeen s = pkiSeen.get(name(region, agent));
        if (s == null || (tenant != null && !tenant.equals(s.tenant))) return null;
        if (tombs.containsKey(name(region, agent)) || isRevokedFor(s.key, region)) return null;
        return s;
    }

    /** Does this node record its OWN CA for {@code region} (it held a region CA)? Lock-free. */
    public boolean ownCaRecorded(String region) {
        Binding ca = region == null ? null : cas.get(region);
        return ca != null && "own".equals(ca.source);
    }

    /** Is the CA recorded for {@code region} revoked (nothing of that region is accepted until an operator rebind)? Lock-free. */
    public boolean isRegionCaRevoked(String region) {
        Binding ca = region == null ? null : cas.get(region);
        return ca != null && revoked.containsKey(ca.key);
    }

    /** Is {@code region/agent} tombstoned (its key was revoked; reserved until an operator rebind)? Lock-free. */
    public boolean isTombstoned(String region, String agent) { return tombs.containsKey(name(region, agent)); }

    // ---- message signers (MsgSigning): the certificates behind a credential, so every revocation reaches its messages

    /**
     * One certificate of a message signer's path, by reference, as message credentials carry it (an agent's attestation, a
     * trust-bundle entry): enough for any node to apply ITS revocations to a certificate it never sees -- this node's revoked
     * keys, and the operator's {@code revocation_file} (key, certificate, a CRL serial of its issuer, that issuer's CRL
     * freshness) -- and the certificate's validity.
     */
    public static final class CertRef {
        /** SHA-256 of the key (SubjectPublicKeyInfo) and of the certificate (DER), hex; the issuer's DN; the serial, hex. */
        public String k, c, i, s;
        /** notBefore, notAfter (ms). */
        public long nb, na;
        /** {@code <issuer DN>|<serial>}, as a CRL lists it (made once). */
        private transient String issuerSerial;

        String issuerSerial() {
            String v = issuerSerial;
            if (v == null) issuerSerial = v = i + "|" + s;
            return v;
        }

        public static CertRef of(X509Certificate cert) {
            CertRef r = new CertRef();
            r.k = fingerprint(cert.getPublicKey().getEncoded());
            try { r.c = CertificateManager.certFingerprint(cert); } catch (Exception e) { r.c = null; }
            r.i = cert.getIssuerX500Principal().getName();
            r.s = cert.getSerialNumber().toString(16);
            r.nb = cert.getNotBefore().getTime();
            r.na = cert.getNotAfter().getTime();
            return r;
        }

        /** Every field present and well formed (what arrived over the wire). */
        public boolean wellFormed() {
            return isFingerprint(k) && isFingerprint(c) && i != null && s != null && !s.isEmpty() && s.length() <= 64 && nb <= na;
        }

        /** The canonical form signatures cover. */
        public String canonical() { return k + "|" + c + "|" + i.length() + ":" + i + "|" + s + "|" + nb + "|" + na; }

        @Override public boolean equals(Object o) {
            if (!(o instanceof CertRef)) return false;
            CertRef x = (CertRef) o;
            return nb == x.nb && na == x.na && java.util.Objects.equals(k, x.k) && java.util.Objects.equals(c, x.c)
                    && java.util.Objects.equals(i, x.i) && java.util.Objects.equals(s, x.s);
        }

        @Override public int hashCode() { return java.util.Objects.hash(k, c, i, s, nb, na); }
    }

    /**
     * The certificates {@code chain}'s leaf stands on, leaf first, by reference: with a PKI its validated path (the anchor
     * included; validated now when not cached yet), else the chain as given. Empty when it cannot be had.
     */
    public List<CertRef> pathRefs(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return List.of();
        List<X509Certificate> path = null;
        Pki p = pki;
        if (p != null) {
            try {
                String cfp = CertificateManager.certFingerprint(chain[0]);
                path = p.paths.get(cfp);
                if (path == null) {
                    ProvisionedIdentity.Validated v = ProvisionedIdentity.validatePath(chain, p.anchors, new java.util.Date());
                    path = v.path;
                }
            } catch (Exception e) {
                path = null;
            }
        }
        List<CertRef> out = new ArrayList<>();
        if (path != null && !path.isEmpty()) {
            for (X509Certificate c : path) out.add(CertRef.of(c));
        } else {
            for (X509Certificate c : chain) if (c != null) out.add(CertRef.of(c));
            // without a PKI: the region CA recorded here that issued the leaf (its key's revocation refuses what it issued)
            CrescoIdentity id = CrescoNames.identityOf(chain[0]);
            Binding ca = id == null ? null : cas.get(id.getRegion());
            X509Certificate caCert = ca == null ? null : ca.caCert();
            if (chain.length == 1 && caCert != null && signedBy(chain[0], caCert)) out.add(CertRef.of(caCert));
        }
        return out;
    }

    /**
     * Why a message signer of {@code region} whose certificates are {@code refs} (leaf first) is refused now, or null: a key
     * revoked here (the leaf's for the names of that region, a CA's for every name), a certificate out of its validity, and
     * with a PKI the operator's revocations of any of them (key, certificate, CRL serial, an out-of-date CRL of its issuer).
     * Lock-free, O(path): a few map lookups per certificate.
     */
    public String refsRefusal(List<CertRef> refs, String region, long now) {
        if (refs == null || refs.isEmpty()) return "no certificate is known for it";
        Pki p = pki;
        for (int n = 0; n < refs.size(); n++) {
            CertRef r = refs.get(n);
            String what = n == 0 ? "its certificate" : "the certificate of its issuer '" + refs.get(n - 1).i + "'";
            if (n == 0 ? isRevokedFor(r.k, region) : revoked.containsKey(r.k)) return what + "'s key is " + revokedText(r.k, n == 0 ? region : null);
            if (now < r.nb || now > r.na) return what + " is not valid now (" + new java.util.Date(r.nb) + " to " + new java.util.Date(r.na) + ")";
            if (p != null) {
                String why = p.rev.refusal(r.k, r.c, r.i, r.issuerSerial(), now);
                if (why != null) return what + " is REVOKED by the operator (" + why + ")";
            }
        }
        return null;
    }

    /** Why messages signed in the name {@code region/agent} are refused (it is tombstoned: revoked, reserved), or null. Lock-free. */
    public String nameRefusal(String region, String agent) {
        String n = name(region, agent);
        Tomb t = tombs.get(n);
        return t == null ? null : tombText(n, t);
    }

    /** The revocations this node holds: id ({@code fp}, or {@code fp@region} for one region's names) -> who revoked it. Sorted. */
    public Map<String, String> revocations() {
        Map<String, String> out = new java.util.TreeMap<>();
        for (Revocation r : revoked.values()) out.put(r.id(), r.source);
        return out;
    }

    /**
     * Why a live connection presenting {@code leaf} must be closed now, or null: its key is revoked for the name it carries,
     * that name is tombstoned, or it was issued by its region's recorded CA and that CA's key is revoked. Lock-free; no
     * signature checks (the broker sweeps its live connections with it).
     */
    public String revokedReason(X509Certificate leaf) {
        return leaf == null ? null : revokedReason(new X509Certificate[]{leaf});
    }

    /**
     * As {@link #revokedReason(X509Certificate)} for the chain a live connection presented (leaf first). Pre-provisioned, the
     * PKI judges the whole path again (validated once per leaf certificate and set of anchors): a certificate on it -- an
     * issuing region CA, the anchor itself -- revoked by the operator or out of its validity, or a path that no longer
     * reaches the anchors (replaced), closes the connection.
     */
    public String revokedReason(X509Certificate[] chain) {
        if (chain == null || chain.length == 0 || chain[0] == null) return null;
        X509Certificate leaf = chain[0];
        String fp = fingerprint(leaf.getPublicKey().getEncoded());
        CrescoIdentity id = CrescoNames.identityOf(leaf);
        String region = id == null ? null : id.getRegion();
        if (isRevokedFor(fp, region)) return "its key was " + revokedText(fp, region);
        if (id == null) return null;
        Tomb t = tombs.get(name(id.getRegion(), id.getAgent()));
        if (t != null) return "its name " + name(id.getRegion(), id.getAgent()) + " was revoked";
        Pki p = pki;
        if (p != null) {
            String r = p.rev.refusal(chain);
            if (r != null) return "it is revoked by the operator (" + r + ")";
            String v = pkiCheck(p, chain);
            if (v != null) return "its certificate " + v;
        }
        Binding ca = cas.get(id.getRegion());
        if (ca != null && revoked.containsKey(ca.key)) {
            X509Certificate c = ca.caCert();
            if (c != null && leaf.getIssuerX500Principal().equals(c.getSubjectX500Principal())) return "its region's CA key was revoked";
        }
        return null;
    }

    /** {@link #revokedReason} for a whole chain: also a revoked key anywhere above the leaf (a revoked CA). */
    public String revocationRefusal(X509Certificate[] chain) {
        if (chain == null || chain.length == 0) return null;
        String r = revokedReason(chain);
        return r != null ? r : revokedIssuer(chain);
    }

    // ---- revocation lists (RevocationSync)

    /** One entry of a signed revocation list: {@code fp|scope|name|exp} (empty fields allowed). */
    public static final class ListEntry {
        public final String fp, scope, name;
        public final long exp;
        public ListEntry(String fp, String scope, String name, long exp) { this.fp = fp; this.scope = scope; this.name = name; this.exp = exp; }
        public String format() {
            return fp + "|" + (scope == null ? "" : scope) + "|" + (name == null ? "" : name) + "|" + (exp > 0 ? Long.toString(exp) : "");
        }
        /** Null when malformed. */
        public static ListEntry parse(String s) {
            if (s == null) return null;
            String[] p = s.split("\\|", -1);
            if (p.length == 0 || !isFingerprint(p[0])) return null;
            String scope = p.length > 1 && !p[1].isEmpty() ? p[1] : null;
            String name = p.length > 2 && !p[2].isEmpty() ? p[2] : null;
            long exp = 0;
            try { if (p.length > 3 && !p[3].isEmpty()) exp = Long.parseLong(p[3]); } catch (NumberFormatException e) { return null; }
            if (scope != null && !CrescoNames.isValid(scope)) return null;
            if (name != null) {
                int sl = name.indexOf('/');
                if (sl <= 0 || !CrescoNames.isValid(name.substring(0, sl)) || !CrescoNames.isValid(name.substring(sl + 1))) return null;
            }
            return new ListEntry(p[0].toLowerCase(java.util.Locale.ROOT), scope, name, exp);
        }
        static ListEntry of(Revocation r) { return new ListEntry(r.fp, r.scope, r.name, r.exp); }
    }

    /** This registry's epoch (changes only when the registry is created anew or reset). */
    public String revocationEpoch() { return revEpoch; }

    /** The highest position of this node's list that is durable (only durable entries are served). */
    public long revocationSeq() { return durableRevSeq; }

    /** The highest position of this node's OWN list (its operator's revocations) that is durable. */
    public long ownRevocationSeq() { return Math.min(ownRevSeq, durableRevSeq); }

    /**
     * At most {@code max} durable entries after position {@code from}, in order ({@code ownOnly}: only this node's own).
     * O(entries returned + entries skipped), never the whole list.
     */
    public List<Revocation> revocationsAfter(long from, int max, boolean ownOnly) {
        List<Revocation> out = new ArrayList<>();
        long limit = durableRevSeq;
        for (Revocation r : revokedBySeq.tailMap(from, false).values()) {
            if (r.seq > limit || out.size() >= max) break;
            if (!ownOnly || r.own) out.add(r);
        }
        return out;
    }

    /** SHA-256 over the sorted ids of the whole list (cached until the list changes): what a v1 peer advertised. */
    public String revocationDigest() {
        long v = revVersion;
        String d = digestCache;
        if (d != null && digestVersion == v) return d;
        List<String> ids = new ArrayList<>(revocations().keySet());
        StringBuilder sb = new StringBuilder();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (String k : ids) { md.update(k.getBytes(StandardCharsets.UTF_8)); md.update((byte) ','); }
            sb.append(java.util.HexFormat.of().formatHex(md.digest()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        d = ids.isEmpty() ? "" : sb.toString();
        digestCache = d;
        digestVersion = v;
        return d;
    }

    /**
     * Merge the revocations a peer's verified, signed list carries (synchronously: in force, bindings dropped, recorded in
     * ONE durable write). See {@link #mergeRevocations(List, String, String, boolean)}.
     *
     * @return how many keys were newly revoked
     */
    public int mergeRevocations(Collection<String> fps, String source, String scopeRegion) {
        List<ListEntry> in = new ArrayList<>(fps.size());
        for (String f : fps) if (isFingerprint(f)) in.add(new ListEntry(f.toLowerCase(java.util.Locale.ROOT), null, null, 0));
        return mergeRevocations(in, source, scopeRegion, false);
    }

    /**
     * Merge a peer's verified list.
     * <ul>
     *   <li>{@code scopeRegion} null: the PARENT's list -- every entry as listed (the parent is the authority for its
     *       subtree: an entry without a scope applies to every name, a scoped one to that region's names).</li>
     *   <li>{@code scopeRegion} non-null: a CHILD region's list -- a POSITIVE scope check: an entry is accepted only when its
     *       key is bound here to a name of that region (and to no other name), or is bound to no name here and the entry names
     *       an identity of that region; it then applies to that region's names ONLY (recorded as {@code fp@region}). A CA key,
     *       a key bound here outside the region, or an entry naming no identity of the region is refused.</li>
     * </ul>
     * This node's own key and its own region CA are never revoked by a list. With {@code async} the entries are in force
     * when this returns and their bindings, tombstones and durable record follow on the background writer (one write): the
     * ping path never waits for a disk.
     *
     * @return how many entries were newly put in force
     */
    public int mergeRevocations(List<ListEntry> entries, String source, String scopeRegion, boolean async) {
        int added = 0, refused = 0;
        String firstRefusal = null;
        for (ListEntry e : entries) {
            if (e == null || !isFingerprint(e.fp)) continue;
            String fp = e.fp.toLowerCase(java.util.Locale.ROOT);
            String why = null, scope, name;
            if (isOwnKey(fp)) {
                why = "it lists THIS node's own key " + PeerIdentity.shortFp(fp);
                scope = name = null;
            } else if (scopeRegion != null) {
                scope = scopeRegion;
                name = null;
                if (isCaKey(fp)) {
                    why = "the key " + PeerIdentity.shortFp(fp) + " is a region CA's (a region's list does not revoke CAs)";
                } else {
                    Set<String> bound = namesByKey.get(fp);
                    String outside = null;
                    if (bound != null) {
                        for (String n : bound) {
                            if (n.startsWith(scopeRegion + "/")) name = n; else outside = n;
                        }
                    }
                    if (outside != null) {
                        why = "the key " + PeerIdentity.shortFp(fp) + " is bound here to " + outside + ", outside region " + scopeRegion;
                    } else if (name == null) {
                        if (e.name != null && e.name.startsWith(scopeRegion + "/")) name = e.name;
                        else why = "the key " + PeerIdentity.shortFp(fp) + " is bound here to no name and the list names no identity of region "
                                + scopeRegion + " for it";
                    }
                }
            } else {
                scope = e.scope;
                name = e.name;
            }
            if (why != null) {
                refused++;
                if (firstRefusal == null) firstRefusal = why;
                continue;
            }
            if (applyInMemory(fp, scope, name, source, e.exp, false)) added++;
        }
        if (refused > 0) {
            log.accept("The " + source + ": " + refused + " entr" + (refused == 1 ? "y" : "ies") + " REFUSED (e.g. "
                    + firstRefusal + ")" + (scopeRegion == null ? "" : "; a region revokes only identities of its own region " + scopeRegion));
        }
        if (added > 0) {
            log.accept("Merged the " + source + ": " + added + " key(s) newly REVOKED"
                    + (scopeRegion == null ? "" : " for the names of region " + scopeRegion));
            if (async) scheduleRevocationWork(0);
            else {
                synchronized (writeLock) { completeRevocationsLocked(); }
                fireRevokedPending();
            }
        }
        return added;
    }

    /**
     * With the write lock: drop revocations that can no longer matter -- entries whose expiry passed (keys a region CA
     * enrolled: every leaf for them has expired, the CA issues no more, and their names stay tombstoned). Recorded.
     */
    void expireRevocations(long now) {
        List<Rec> recs = new ArrayList<>();
        synchronized (revMemLock) {
            for (Revocation r : new ArrayList<>(revoked.values())) {
                if (r.exp > 0 && r.exp < now) {
                    removeRevocation(r);
                    Rec x = rec("expire", r.name);
                    x.key = r.fp; x.scope = r.scope; x.seq = r.seq; x.t = now;
                    recs.add(x);
                }
            }
        }
        if (!recs.isEmpty()) {
            log.accept("peer trust registry: " + recs.size() + " revocation(s) expired (every leaf they could apply to has expired; their names stay reserved)");
            if (!append(recs.toArray(new Rec[0]))) log.accept("peer trust registry: " + recs.size() + " expired revocation(s) could not be recorded (dropped again at the next start)");
        }
    }

    private Path revokePath() { return file == null ? null : file.resolveSibling(file.getFileName() + ".revoke"); }

    /**
     * With the write lock: the operator file {@code <file>.revoke} (one {@code region/agent} or key fingerprint per line,
     * {@code #} comments), re-read when it changes; each entry is revoked once (revocation is idempotent and permanent).
     * The file counts as applied only when every revocation in it is in force AND recorded: after a failed write it is
     * read again at the next check (every 10 s).
     */
    private void reloadRevokeFileLocked() {
        Path p = revokePath();
        if (p == null) return;
        try {
            long m = Files.exists(p) ? Files.getLastModifiedTime(p).toMillis() : 0;
            if (m == revokeFileMtime && revUndurable.isEmpty()) return;
            if (m == 0) { revokeFileMtime = 0; return; }
            for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) continue;
                String why = revokeLocked(t, "operator file " + p.getFileName());
                if (why != null && m != revokeFileMtime) log.accept("Revocation '" + t + "' in " + p + " not applied: " + why);
            }
            completeRevocationsLocked();
            if (revUndurable.isEmpty()) revokeFileMtime = m;
        } catch (IOException e) {
            log.accept("could not read " + p + ": " + e.getMessage());
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
            if (sealed || fenced != null) { pendingSeen.clear(); return; }
            Map<String, Long> batch = new java.util.LinkedHashMap<>();
            for (String k : new ArrayList<>(pendingSeen.keySet())) {
                Long t = pendingSeen.remove(k);
                if (t != null) batch.put(k, t);
            }
            if (batch.isEmpty()) return;
            List<Rec> recs = new ArrayList<>();
            for (Map.Entry<String, Long> e : batch.entrySet()) {
                int bar = e.getKey().indexOf('|');
                Rec r = rec("seen", e.getKey().substring(bar + 1));
                r.kind = e.getKey().substring(0, bar); r.t = e.getValue();
                recs.add(r);
            }
            if (!append(recs.toArray(new Rec[0]))) {
                // not recorded: keep them queued for the next batch (a newer refresh for the same name wins)
                for (Map.Entry<String, Long> e : batch.entrySet()) pendingSeen.merge(e.getKey(), e.getValue(), Math::max);
                return;
            }
            compactIfDue();
        }
    }

    void prune(long now) {
        List<Rec> recs = new ArrayList<>();
        if (bridgePruneMs > 0) {
            for (Map.Entry<String, Binding> e : new ArrayList<>(bridges.entrySet())) {
                if (now - e.getValue().lastSeen > bridgePruneMs) { bridges.remove(e.getKey()); recs.add(rec("unbridge", e.getKey())); }
            }
        }
        if (bindingPruneMs > 0) {
            for (Map.Entry<String, Binding> e : new ArrayList<>(identities.entrySet())) {
                // only AGENT bindings lapse: a controller this node joined (its pin) or that joined it, and this node's own
                // identity, are kept until an operator rebinds or revokes them -- a pin that lapsed would let the next answer
                // to a discovery be trusted on first use again
                if (!ROLE_AGENT.equals(e.getValue().role)) continue;
                if (now - e.getValue().lastSeen > bindingPruneMs) { removeIdentity(e.getKey(), e.getValue()); recs.add(rec("unbind", e.getKey())); }
            }
        }
        // removals take effect in memory either way (the next start prunes them again if they were not recorded)
        if (!recs.isEmpty() && !append(recs.toArray(new Rec[0]))) log.accept("peer trust registry: " + recs.size() + " pruned entries could not be recorded");
        expireRevocations(now);
    }

    // ---------------------------------------------------------------- rebind tokens

    private Path rebindPath() { return file == null ? null : file.resolveSibling(file.getFileName() + ".rebind"); }

    private String rebindFileHint() { return file == null ? "peer_identity_rebind" : rebindPath().toString(); }

    /**
     * An unconsumed rebind token for {@code n} (a name, or a region for its CA) that allows the key {@code fp}, or null. A
     * token is {@code <name>[#tag][=<key fingerprint>]}: with a fingerprint it rebinds the name to THAT key only (the safe
     * form: nobody else holding a discovery secret can take the name meanwhile); without one, to the first key presented.
     */
    private String rebindToken(String n, String fp) {
        reloadRebindFile();
        for (Set<String> src : List.of(rebindConfig, rebindFile)) {
            for (String t : src) {
                if (!tokenName(t).equals(n) || consumed.contains(t)) continue;
                String want = tokenKey(t);
                if (want != null && (fp == null || !want.equals(fp.toLowerCase(java.util.Locale.ROOT)))) continue;
                if (want == null) {
                    log.accept("Rebind token '" + t + "' names no key: it rebinds " + n + " to the FIRST key presented ("
                            + PeerIdentity.shortFp(fp) + "); bind tokens to the expected key: '" + n + "=<key fingerprint>'");
                }
                return t;
            }
        }
        return null;
    }

    /** The name a rebind token is for. */
    static String tokenName(String t) {
        String x = t;
        int eq = x.indexOf('=');
        if (eq >= 0) x = x.substring(0, eq);
        int h = x.indexOf('#');
        return (h >= 0 ? x.substring(0, h) : x).trim();
    }

    /** The key fingerprint a rebind token is bound to (lowercase hex), or null. */
    static String tokenKey(String t) {
        int eq = t.indexOf('=');
        if (eq < 0) return null;
        String k = t.substring(eq + 1).trim().replace(":", "").toLowerCase(java.util.Locale.ROOT);
        return k.isEmpty() ? null : k;
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
        namesByKey.clear(); tombs.clear();
        synchronized (revMemLock) {
            revoked.clear(); revokedBySeq.clear(); revokedAnyScope.clear(); revPending.clear(); revSeq = 0; revVersion++;
        }
        revUndurable.clear(); revokedToFire.clear(); durableRevSeq = 0; ownRevSeq = 0; revEpoch = null; revDurability = null;
        lastMac = ""; lines = 0; needsNewline = false; recentMacs.clear();
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
            case "init": case "epoch": if (r.token != null) revEpoch = r.token; break;
            case "bind": {
                Binding old = identities.get(r.name);
                replaced(r.name, old == null ? null : old.key, r.key);
                putIdentity(r.name, new Binding(r.tenant, r.key, null, r.source, r.role == null ? ROLE_AGENT : r.role, "follow".equals(r.mode),
                        old != null && old.key.equals(r.key) ? old.firstSeen : r.t, r.t));
                tombs.remove(r.name);                     // a binding recorded after a tombstone was an operator rebind
                break;
            }
            case "ca": cas.put(r.name, new Binding(r.tenant, r.key, r.cert, r.source, null, "follow".equals(r.mode), r.t, r.t)); break;
            case "unca": cas.remove(r.name); break;
            case "bridge": bridges.put(r.name, new Binding(null, null, null, "discovery", null, false, r.t, r.t)); break;
            case "unbridge": bridges.remove(r.name); break;
            case "unbind": { Binding old = identities.get(r.name); if (old != null) removeIdentity(r.name, old); break; }
            case "untomb": tombs.remove(r.name); break;
            case "tomb": {
                Binding old = identities.get(r.name);
                if (old != null && old.key.equals(r.key)) removeIdentity(r.name, old);
                tombs.put(r.name, new Tomb(r.key, r.source == null ? "?" : r.source, r.t));
                break;
            }
            case "expire": {
                Revocation x = r.key == null ? null : revoked.get(r.scope == null ? r.key : r.key + "@" + r.scope);
                if (x != null) synchronized (revMemLock) { removeRevocation(x); }
                break;
            }
            case "consume": if (r.token != null) consumed.add(r.token); break;
            case "revoke": if (r.key != null) revokedKeys.computeIfAbsent(r.name, k -> ConcurrentHashMap.newKeySet()).add(r.key); break;
            case "revokekey": if (r.key != null) loadRevocation(r); break;
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
                    if (fenced == null) {
                        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) { ch.truncate(pos); ch.force(true); }
                    }
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
        recentMacs.clear();
        for (int i = Math.max(0, out.size() - TAIL); i < out.size(); i++) if (out.get(i).mac != null) recentMacs.addLast(out.get(i).mac);
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
        if (fenced != null) {
            // another writer owns the store: read what verifies, write nothing
            try {
                if (keyExists && logExists) {
                    macKey = readKey();
                    try { for (Rec r : readRecords(Verify.CHAIN)) apply(r); }
                    catch (BadRecord br) { for (Rec r : br.good) apply(r); }
                }
            } catch (Exception ex) {
                log.accept("peer trust registry: could not read " + file + " (" + ex.getMessage() + ")");
            }
            return;
        }
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
            goodOffset = Files.size(file);
            String anchor = checkHead(recs, kf != null && kf.anchored);
            for (Rec r : recs) apply(r);
            if (anchor != null) { seal(anchor); return; }
            if (compactedPastHead) {
                log.accept("peer trust registry: the log was compacted and the process stopped before its new head anchor was"
                        + " written; the compacted log continues the anchored state exactly, so it is anchored now");
                writeHead(new Head(generation, lines, lastMac));
            }
            if (kf == null || !kf.anchored) { writeHead(new Head(generation, lines, lastMac)); writeKey(macKey, true); }
        } catch (Exception ex) {
            seal("unreadable: " + ex.getMessage());
        }
    }

    /** Set by {@link #checkHead}: the head anchors the log this compacted log replaced (a crash before its new head). */
    private boolean compactedPastHead;

    private void initLog() throws Exception {
        closeChannels();
        Rec r = rec("init", null);
        r.t = System.currentTimeMillis();
        r.source = "cresco-peer-trust-v2";
        if (revEpoch == null) revEpoch = newEpochId();
        r.token = revEpoch;
        lastMac = "";
        lines = 0;
        r.mac = macOf(r, lastMac);
        byte[] first = (GSON.toJson(r) + "\n").getBytes(StandardCharsets.UTF_8);
        writeAtomically(file, first);
        goodOffset = first.length;
        lastMac = r.mac;
        lines = 1;
        recentMacs.clear();
        recentMacs.addLast(r.mac);
        generation++;
        writeHead(new Head(generation, lines, lastMac));
        writeKey(macKey, true);
        writeHighWater();
    }

    private void closeChannels() {
        if (channel != null) { try { channel.close(); } catch (IOException ignore) { } channel = null; }
        logFileKey = null;
    }

    /**
     * Append {@code recs} as ONE durable write and keep the chain, with the write lock: any bytes past the last durable
     * offset (left by a write that failed part-way) are truncated first; the records are written at that offset and
     * fsynced; on any failure the file is truncated back and false is returned (callers change nothing in memory). Then the
     * head anchor is moved up (a failed head write leaves an older, still valid anchor).
     */
    private boolean append(Rec... recs) {
        if (file == null) return true;
        if (sealed || fenced != null || recs.length == 0) return false;
        String prevMac = lastMac;
        long prevLines = lines;
        boolean prevNl = needsNewline;
        try {
            openLogChannel();
            if (sealed) return false;
            long size = channel.size();
            if (size > goodOffset) {
                log.accept("peer trust registry: removing " + (size - goodOffset) + " byte(s) after the last durable record (left by a"
                        + " write that failed part-way)");
                channel.truncate(goodOffset);
                channel.force(true);
            } else if (size < goodOffset) {
                seal("the log is shorter (" + size + " bytes) than this process last made it durable (" + goodOffset + "): truncated underneath it");
                return false;
            }
            StringBuilder sb = new StringBuilder();
            if (needsNewline) sb.append('\n');
            String mac = lastMac;
            List<String> macs = new ArrayList<>(recs.length);
            for (Rec r : recs) {
                r.mac = macOf(r, mac);
                mac = r.mac;
                macs.add(mac);
                sb.append(GSON.toJson(r)).append('\n');
            }
            ByteBuffer b = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
            long pos = goodOffset;
            if (injectedFailures > 0) {                       // tests: a full disk part-way through the write, or at fsync
                injectedFailures--;
                if (!injectAtFsync) {
                    ByteBuffer half = ByteBuffer.wrap(b.array(), 0, Math.max(1, b.remaining() / 2));
                    while (half.hasRemaining()) pos += channel.write(half, pos);
                    throw new IOException("No space left on device (injected)");
                }
                while (b.hasRemaining()) pos += channel.write(b, pos);
                throw new IOException("fsync failed (injected)");
            }
            while (b.hasRemaining()) pos += channel.write(b, pos);
            channel.force(false);
            goodOffset = pos;
            needsNewline = false;
            lastMac = mac;
            lines += recs.length;
            for (String m : macs) remember(m);
        } catch (Exception ex) {
            lastMac = prevMac;
            lines = prevLines;
            needsNewline = prevNl;
            log.accept("peer trust registry: could not record " + recs.length + " record(s) (" + ex.getMessage() + "); nothing was changed");
            try {
                if (channel != null && channel.isOpen()) { channel.truncate(goodOffset); channel.force(true); }
            } catch (Exception again) {
                fence("the log could not be put back after a failed write (" + again.getMessage() + "); a restart re-verifies it");
            }
            return false;
        }
        head = new Head(generation, lines, lastMac);
        try {
            writeHead(head);
        } catch (Exception ex) {
            log.accept("peer trust registry: the records are durable, but the head anchor could not be moved up (" + ex.getMessage() + ")");
        }
        return true;
    }

    /** Open the log for writing when needed, and seal if the file at the path is not the one this instance wrote. */
    private void openLogChannel() throws IOException {
        Object k = null;
        try { k = Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey(); } catch (IOException ignore) { }
        if (channel != null && channel.isOpen()) {
            if (logFileKey != null && k != null && !logFileKey.equals(k)) {
                seal("the log file was replaced underneath this process");
                closeChannels();
            }
            return;
        }
        channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.READ);
        logFileKey = k;
    }

    private volatile int injectedFailures;
    private volatile boolean injectAtFsync;

    /** Tests: the next {@code n} writes fail -- part-way through the bytes, or (atFsync) after all of them, at the fsync. */
    void injectWriteFailures(int n, boolean atFsync) {
        injectAtFsync = atFsync;
        injectedFailures = n;
    }

    private void remember(String mac) {
        recentMacs.addLast(mac);
        while (recentMacs.size() > TAIL) recentMacs.removeFirst();
    }

    private int live() {
        int rk = 0;
        for (Set<String> v : revokedKeys.values()) rk += v.size();
        return identities.size() + cas.size() + bridges.size() + consumed.size() + rk + revoked.size() + tombs.size();
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

    /** Rewrite the log as its live state (temp, fsync, rename, directory fsync), then its head anchor. With the write lock. */
    private void compact() throws Exception {
        List<Rec> recs = new ArrayList<>();
        Rec init = rec("init", null);
        init.t = System.currentTimeMillis();
        init.source = "cresco-peer-trust-v2";
        // the log this one replaces: its record count and last MACs, so a crash between the rename below and the new head
        // (the old head then covers more lines than this log has) is recognised as exactly that
        init.prevLines = lines;
        init.prevTail = new ArrayList<>(recentMacs);
        init.token = revEpoch;
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
        for (Revocation x : revokedBySeq.values()) {
            Rec r = rec("revokekey", x.name);
            r.key = x.fp; r.source = x.source; r.t = x.t; r.scope = x.scope; r.seq = x.seq;
            r.exp = x.exp > 0 ? x.exp : null; r.own = x.own ? Boolean.TRUE : null;
            recs.add(r);
        }
        for (Map.Entry<String, Tomb> e : tombs.entrySet()) {
            Rec r = rec("tomb", e.getKey());
            r.key = e.getValue().key; r.source = e.getValue().source; r.t = e.getValue().t;
            recs.add(r);
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
            Rec r = rec("consume", tokenName(t));
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
        byte[] all = sb.toString().getBytes(StandardCharsets.UTF_8);
        writeAtomically(file, all);
        closeChannels();                                          // the old log is replaced; appends reopen the new one
        logFileKey = null;
        goodOffset = all.length;
        lastMac = prev;
        lines = recs.size();
        needsNewline = false;
        recentMacs.clear();
        for (int i = Math.max(0, recs.size() - TAIL); i < recs.size(); i++) recentMacs.addLast(recs.get(i).mac);
        generation++;
        head = new Head(generation, lines, lastMac);
        writeHead(head);
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
        compactedPastHead = false;
        if (recs.size() < best.lines || best.lines < 1 || !best.mac.equals(recs.get((int) best.lines - 1).mac)) {
            // a compaction whose new head was never written: this log's first record names the replaced log's last MACs, and
            // the anchor is one of them -- the compacted log continues exactly the anchored state (or a later one)
            Rec first = recs.isEmpty() ? null : recs.get(0);
            if (first != null && "init".equals(first.op) && first.prevLines != null && first.prevTail != null && !first.prevTail.isEmpty()) {
                long lastLine = first.prevLines, firstLine = lastLine - first.prevTail.size() + 1;
                if (best.lines >= firstLine && best.lines <= lastLine
                        && best.mac.equals(first.prevTail.get((int) (best.lines - firstLine)))) {
                    compactedPastHead = true;
                    return null;
                }
            }
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

    public static String fingerprint(byte[] spki) {
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
