package io.cresco.agent.controller.communication;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end MsgEvent signatures (audit D4). A broker stamps the caller of each hop; across several brokers (relays,
 * source routes, bridges of other regions) the receiver also needs the ORIGIN's word: every MsgEvent a node originates is
 * signed with that node's identity key (RSA-3072 / SHA-384 today, as CrescoCrypto selects), and a receiver checks the
 * signature before it dispatches the message locally or completes an RPC with it.
 *
 * <p><b>What is signed.</b> The type, the source header and every parameter except those infrastructure rewrites in
 * transit: {@code routepath-*}, {@code linkcost-*}, {@code srcroute*}, {@code ttl}, the broker's caller stamp
 * ({@code cresco_caller*}, including the alias) and the signature itself. The destination header is not signed (the
 * controller tiers, source routes and {@code region_alias} rewrite it), but the RECIPIENT is ({@value #TO}): the node -- and
 * plugin -- the message is for, as the origin's router decided it (after the controller tiers; the last waypoint of a
 * source route); {@code ^<region>/<agent>} for a global function an agent hands its region controller to carry to its
 * global. A receiver refuses a message that does not name it (or its former address, {@code region_alias}): a message
 * signed for one node is never accepted by another. The origin's caller is signed as a copy ({@code cresco_sig_caller*})
 * next to the signer ({@code cresco_sig_by}), its credential ({@code cresco_sig_cred}) and the signing time
 * ({@code cresco_sig_ts}); {@code cresco_tenant} is set when the message is stamped, so it is signed.
 *
 * <p><b>Who signed.</b> A controller (region or global) signs with its leaf certificate as its credential; an agent with
 * an <i>attestation</i> its region controller issued (its tenant, region, name, public key and the certificates its key
 * stands on, by reference; valid {@code msg_signing_attestation_hours}, default 24) once it holds one, and with its
 * certificate before. A receiver resolves a credential, in this order: a certificate that verifies HERE as its name (a
 * peer bound here, or under a recorded region CA, or the operator's PKI); a certificate whose key is the one the <i>trust
 * bundle</i> lists for that controller; an attestation signed by a controller the trust bundle lists for the attested
 * region (or by this node). The trust bundle is the list of region and global controller keys (with the certificates
 * behind them, by reference): the global builds it from the certificates its regions present on their pings, signs it, and
 * sends it down on its pongs; each region re-signs what it verified for its agents. Credentials are verified once and
 * cached (bounded); on EVERY message the signer, the certificates behind it and an attestation's issuer are checked against
 * this node's revocations as they stand: a revoked key, a tombstoned name, and with a PKI the operator's
 * {@code revocation_file} (key, certificate, CRL serial, an out-of-date CRL) refuse it at once. A message costs one hash,
 * a few map lookups and one signature check (or one HMAC): O(1) in the size of the fleet.
 *
 * <p><b>What the receiver does.</b> With {@code broker_security_enabled}: an unsigned message, or one whose signature,
 * credential, recipient, age ({@code msg_signature_max_age_ms}, default 5 min, either way) or freshness (a signature seen
 * before) fails, is refused (an RPC request is answered 401). The signer must be the node of the signed caller (or, for an
 * API user, a node of the user's tenant -- any tenant's for a superuser tenant), and the broker's stamp, when there is one,
 * must be that caller: a bridge that stripped a stamp it could not vouch for no longer cuts the origin's word. A message
 * this node did not check on arrival (it was not addressed to it, or it carries it on as a relay) is checked when it is
 * dispatched here, whatever re-addressed it ({@link #verifyIfDeferred}).
 *
 * <p><b>Direct links.</b> A signature is what carries the origin's word past relays. Where there is no relay -- a message
 * between a node and its own parent or child (an agent and its region or global controller, a region and its global) --
 * the broker the receiver consumes from already attests the origin first hand: it took the message from that node's own
 * verified connection (mutual TLS), or from the verified bridge of that very controller, and says so in a property only it
 * sets ({@link CallerStamps#P_DIRECT}, on the sender's word {@link CallerStamps#P_ORIGIN} that it is the origin). Such a
 * message travels unsigned ({@code msg_signing_direct}, default true): no RSA operation for the bulk of the control plane
 * (keepalives, registrations, region and global replies). The receiver alone decides: it accepts an unsigned message only
 * when ITS broker attests it first hand from the very node whose word the stamp is -- a controller trusts its own broker's
 * attestation; an agent trusts its parent's broker only for the parent's own messages -- so an origin that misjudges a
 * route is refused, never trusted. Each side offers this on the pings ({@link #P_DIRECT_OK}): an origin sends unsigned only
 * to a parent or child that said it accepts it, and only a message whose recipient is that parent or child.
 */
public final class MsgSigning {

    /**
     * On every authenticated message, under its signature or MAC: what its sender reads -- {@code batch} signatures
     * ({@link MsgBatchSigner}), {@code session} keys ({@link MsgSessions}).
     */
    public static final String CAPS = "cresco_sig_caps";
    static final int CAP_BATCH = 1, CAP_SESSION = 2;
    public static final String SIG = "cresco_sig", BY = "cresco_sig_by", TS = "cresco_sig_ts", CRED = "cresco_sig_cred",
            PENDING = "cresco_sig_pending", S_CALLER = "cresco_sig_caller", S_TENANT = "cresco_sig_caller_tenant",
            S_ROLES = "cresco_sig_caller_roles";
    /** The signed recipient: {@code <region>/<agent>/<plugin>} (plugin empty for the node itself), or {@code ^<region>/<agent>}. */
    public static final String TO = "cresco_sig_to";
    /** On a message off the wire this node did not verify on arrival (never on the wire: every receiver removes it). */
    public static final String DEFERRED = "cresco_sig_deferred";

    /** Ping/pong parameters: a child's certificate and what it holds; the parent's attestation and trust bundle. */
    static final String P_CERT = "sig_cert", P_ATT_EXP = "sig_att_exp", P_ATT = "sig_att", P_BUNDLE_HAVE = "sig_bundle_have",
            P_BUNDLE = "sig_bundle";
    /**
     * Beside an attestation on a pong, and back on the agent's pings: the fingerprint of the key that signed it. A parent
     * that comes back with another key sees that its agent holds an attestation of the key before, and attests it anew
     * (otherwise the agent would sign with it until it nears its end, refused everywhere past its parent).
     */
    static final String P_ATT_ISS = "sig_att_iss";
    /** On a ping and a pong: this node accepts unsigned messages its broker attests first hand (direct links). */
    static final String P_DIRECT_OK = "sig_direct_ok";
    /** On a message this node originates and sends unsigned over a direct link: its own path (copied to {@link CallerStamps#P_ORIGIN}). */
    public static final String DIRECT = "cresco_sig_direct";
    /** How long a parent's or child's offer of direct links holds without a new ping (pings come every few seconds). */
    static final long DIRECT_OFFER_MS = 60_000L;
    static final int DIRECT_CHILDREN_MAX = 200_000;

    private static final String VERSION = "cresco-msg-v2";
    private static final Gson GSON = new Gson();

    private final ControllerEngine ce;
    private final boolean required;
    /** {@code msg_signing_direct}: send unsigned over direct links (and accept such messages); default true. */
    private final boolean direct;
    /** The parent this node pinged (region_agent) and until when it accepts direct messages; the children that do. */
    private volatile String directParent;
    private volatile long directParentUntil;
    private final ConcurrentHashMap<String, Long> directChildren = new ConcurrentHashMap<>();
    private final AtomicLong directSent = new AtomicLong(), directAccepted = new AtomicLong();
    /** {@code msg_signing_batch} (default true): batch signatures to the nodes that understand them. */
    private final boolean batch;
    private final int batchThreads;
    private volatile MsgBatchSigner batcher;
    /** What the nodes (region_agent) this node authenticated a message from read ({@link #CAPS}): bounded. */
    private final Cache<String, Integer> peerCaps = CacheBuilder.newBuilder().maximumSize(BATCH_PEERS_MAX).build();
    /** {@code msg_signing_sessions} (default true): end-to-end session keys with the nodes that read them. */
    private final boolean sessionsOn;
    private final MsgSessions sessions;
    private final String ownCaps;
    private volatile boolean sessionsBroken;
    private final AtomicLong macSent = new AtomicLong(), macVerified = new AtomicLong(), sessionsMade = new AtomicLong();
    /** Batch root signatures verified here (digest of signature, key, root and count): one key operation per batch. Bounded LRU. */
    private final Cache<String, Boolean> batchVerified = CacheBuilder.newBuilder().maximumSize(BATCH_VERIFIED_MAX)
            .expireAfterWrite(Duration.ofMinutes(15)).build();
    static final int BATCH_PEERS_MAX = 200_000, BATCH_VERIFIED_MAX = 50_000;
    private final AtomicLong batchedSigned = new AtomicLong();
    private final long maxAgeMs;
    /** How far ahead of this node's clock a signature's time may be ({@code msg_signature_max_future_ms}, default 60 s). */
    private final long maxFutureMs;
    private final long attestationMs;

    // ---- this node's credential
    private volatile X509Certificate ownCertLeaf;
    private volatile String ownCertB64;
    /** This node's own key (X.509 SPKI, base64) and its fingerprint, of {@link #ownCertLeaf}. */
    private volatile String ownKeyB64, ownKeyFp;
    private volatile OwnAttestation ownAttestation;
    /** This node's former region (region_alias), read once: a message for its former address is for it. */
    private volatile String alias;
    private volatile boolean aliasRead;

    // ---- the trust bundle this node holds: region/agent -> entry (key fingerprint, tenant, public key, certificates)
    private final ConcurrentHashMap<String, BundleEntry> bundle = new ConcurrentHashMap<>();
    private volatile String heldBundleDigest = "";
    // ---- at a global: the region controllers' certificates learned from their pings
    private final ConcurrentHashMap<String, BundleEntry> childControllers = new ConcurrentHashMap<>();
    /** Bumped on every change of what this node serves its children: a ping costs O(1) and takes no lock. */
    private final AtomicLong bundleVersion = new AtomicLong(1);
    private final Object servedLock = new Object();
    private volatile Served served;

    // ---- verification state (bounded)
    /** Credentials verified here: digest -> signer. Bounded LRU ({@code msg_signing_credentials_max}); never cleared wholesale but on a revocation. */
    private final Cache<String, Signer> credCache;
    private final ReplayMemory replay;
    private final AtomicLong signedCount = new AtomicLong(), verifiedCount = new AtomicLong(), refusedCount = new AtomicLong();
    private volatile long lastRefusalLog;
    /** CA certificate references, shared by every signer that stands on them (a region's agents share their CAs'). */
    private final Cache<PeerTrustRegistry.CertRef, PeerTrustRegistry.CertRef> refInterner =
            CacheBuilder.newBuilder().maximumSize(10_000).build();

    private final ThreadLocal<MessageDigest> sha256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new IllegalStateException(e); }
    });

    static final class BundleEntry {
        String t, r, a, k, role;            // tenant, region, agent, public key (X.509 SPKI, base64), global|region
        List<PeerTrustRegistry.CertRef> refs; // the certificates the key stands on, leaf first
        transient PublicKey key;
        transient String fp;
        transient long keepUntil;           // a region keeps an entry its global dropped for a grace period
        transient X509Certificate cert;     // at a global: the certificate the region presented
    }

    private static final class SignedBundle {
        int v = 2;
        String issuer, tenant;
        long issued;
        List<BundleEntry> entries;
        String cert, sig;
    }

    private static final class Attestation {
        int v = 2;
        String t, r, a, k, iss;
        long exp;
        List<PeerTrustRegistry.CertRef> refs;
        String sig;
    }

    /**
     * The attestation an agent holds, with what it says: it is this node's credential only while it is still this node's
     * (its name, its key, issued by its current parent; {@link #currentAttestation}).
     */
    private static final class OwnAttestation {
        final String json, issuer, region, agent, key, issuerFp;       // issuerFp: null from a parent that does not name it
        final long exp;
        OwnAttestation(String json, Attestation a, String issuerFp) {
            this.json = json; this.issuer = a.iss; this.region = a.r; this.agent = a.a; this.key = a.k; this.exp = a.exp;
            this.issuerFp = issuerFp;
        }
    }

    /** What this node serves its children: one signed bundle per version of its content. */
    private static final class Served {
        final long version, validUntil;
        final X509Certificate own;
        final String contentKey, json, digest, wire;
        Served(long version, long validUntil, X509Certificate own, String contentKey, String json, String digest, String wire) {
            this.version = version; this.validUntil = validUntil; this.own = own; this.contentKey = contentKey;
            this.json = json; this.digest = digest; this.wire = wire;
        }
    }

    /**
     * Who authenticated a message: the identity its credential verified as, its key, the certificates behind it and (an
     * attestation) its issuer's. What this node re-checks against its revocations on every message.
     */
    static final class Signer {
        final String tenant, region, agent, name;
        final PublicKey key;
        final String fp;
        final List<PeerTrustRegistry.CertRef> refs;
        final String issuer, issuerFp;      // an attestation's issuing controller (region/agent) and its key; else null
        final List<PeerTrustRegistry.CertRef> issuerRefs;
        /** The credential's own end (an attestation's expiry, a certificate's notAfter); a cache entry is re-verified hourly. */
        final long expires;
        /** This node's own key: nothing to re-check. */
        final boolean self;

        Signer(String tenant, String region, String agent, PublicKey key, String fp, List<PeerTrustRegistry.CertRef> refs,
               String issuer, String issuerFp, List<PeerTrustRegistry.CertRef> issuerRefs, long expires, boolean self) {
            this.tenant = tenant; this.region = region; this.agent = agent; this.name = tenant + "/" + region + "/" + agent;
            this.key = key; this.fp = fp; this.refs = refs; this.issuer = issuer; this.issuerFp = issuerFp;
            this.issuerRefs = issuerRefs; this.expires = expires; this.self = self;
        }

        String node() { return region + "_" + agent; }
    }

    public MsgSigning(ControllerEngine ce) {
        this.ce = ce;
        io.cresco.library.plugin.Config c = ce.getPluginBuilder().getConfig();
        this.required = SecuritySettings.production(c)
                || (SecuritySettings.brokerSecurity(c) && c.getBooleanParam("msg_signing_required", true));
        this.maxAgeMs = Math.max(10_000L, c.getLongParam("msg_signature_max_age_ms", 300_000L));
        // a signed time ahead of the clock keeps its entry in the replay memory until that time has passed: the allowance is
        // for clock differences between nodes only, never as long as the age a message may have
        this.maxFutureMs = Math.min(this.maxAgeMs, Math.max(1_000L, c.getLongParam("msg_signature_max_future_ms", 60_000L)));
        this.attestationMs = Math.max(1L, c.getLongParam("msg_signing_attestation_hours", 24L)) * 3_600_000L;
        this.direct = c.getBooleanParam("msg_signing_direct", true);
        this.batch = c.getBooleanParam("msg_signing_batch", true);
        this.batchThreads = Math.max(1, c.getIntegerParam("msg_signing_threads",
                Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() / 2))));
        this.sessionsOn = c.getBooleanParam("msg_signing_sessions", true);
        this.sessions = sessionsOn ? new MsgSessions(Math.max(120L, c.getLongParam("msg_signing_session_seconds", 3600L)) * 1000L,
                (int) Math.min(Integer.MAX_VALUE, Math.max(1L, c.getLongParam("msg_signing_sessions_max", (long) MsgSessions.DEFAULT_MAX_PEERS)))) : null;
        String caps = batch ? "batch" : "";
        if (sessionsOn) caps = caps.isEmpty() ? "session" : caps + ",session";
        this.ownCaps = caps.isEmpty() ? null : caps;
        this.credCache = CacheBuilder.newBuilder()
                .maximumSize(Math.max(1L, c.getLongParam("msg_signing_credentials_max", 100_000L)))
                .expireAfterWrite(Duration.ofHours(1)).build();
        this.replay = new ReplayMemory(maxAgeMs, maxFutureMs, Math.max(4096L, c.getLongParam("msg_replay_memory_max", 4_000_000L)), this::warn);
    }

    /** Messages sent and accepted under a session key, sessions made (diagnostics, tests). */
    public long macSentCount() { return macSent.get(); }
    public long macVerifiedCount() { return macVerified.get(); }
    public long sessionsMadeCount() { return sessionsMade.get(); }

    /** Forget every session key and take a new incarnation (tests: as a restart does). */
    void restartSessions() { if (sessions != null) sessions.restart(); }

    private static int parseCaps(String caps) {
        if (caps == null || caps.isEmpty()) return 0;
        int c = 0;
        for (String k : caps.split(",", -1)) {
            if (k.equals("batch")) c |= CAP_BATCH;
            else if (k.equals("session")) c |= CAP_SESSION;
        }
        return c;
    }

    private boolean peerReads(String node, int cap) {
        Integer c = node == null ? null : peerCaps.getIfPresent(node);
        return c != null && (c & cap) != 0;
    }

    private void sessionsFailed(Throwable e) {
        if (!sessionsBroken) {
            sessionsBroken = true;
            info("session keys OFF on this node (" + e.getClass().getSimpleName() + ": " + e.getMessage() + "): messages stay signed");
        }
    }

    /** Stop the batch signing workers (the controller is stopping). */
    public void close() {
        MsgBatchSigner b = batcher;
        if (b != null) b.close();
    }

    /** Messages signed in a shared batch (diagnostics, tests). */
    public long batchedSignedCount() { return batchedSigned.get(); }

    private MsgBatchSigner batcher() {
        MsgBatchSigner b = batcher;
        if (b == null) {
            synchronized (this) {
                b = batcher;
                if (b == null) {
                    b = new MsgBatchSigner(data -> {
                        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(ownKey().getAlgorithm()));
                        s.initSign(ownKey());
                        s.update(data);
                        return s.sign();
                    }, batchThreads, "cresco-msg-signer");
                    batcher = b;
                }
            }
        }
        return b;
    }

    /** Messages sent unsigned over a direct link, and accepted so (diagnostics, tests). */
    public long directSentCount() { return directSent.get(); }
    public long directAcceptedCount() { return directAccepted.get(); }

    /**
     * Is {@code m}, whose recipient is {@code to}, for this node's parent or one of its children over their direct link, which
     * said it accepts unsigned messages attested first hand by the broker? Only when the recipient is that very node (not a
     * node beyond it, not a source route). O(1): one comparison, one map lookup.
     */
    boolean directTo(MsgEvent m, String to) {
        if (!direct || to == null || to.startsWith("^") || m.getParam(MsgRouter.SRCROUTE) != null
                || m.getDstRegion() == null || m.getDstAgent() == null) return false;
        String dst = m.getDstRegion() + "_" + m.getDstAgent();
        if (!to.startsWith(m.getDstRegion() + "/" + m.getDstAgent() + "/")) return false;
        long now = System.currentTimeMillis();
        if (dst.equals(directParent) && now < directParentUntil) return true;
        Long until = directChildren.get(dst);
        return until != null && now < until;
    }

    boolean directTo(MsgEvent m) { return directTo(m, recipient(m)); }

    /** Signing and verification are on (security on and not turned off with {@code msg_signing_required=false}). */
    public boolean required() { return required; }

    public long signedCount() { return signedCount.get(); }
    public long verifiedCount() { return verifiedCount.get(); }
    public long refusedCount() { return refusedCount.get(); }

    // ================================================================ canonical form

    /** Is {@code key} covered by the signature? Not what infrastructure writes in transit, nor the signature itself. */
    static boolean covered(String key) {
        return key != null && !key.startsWith("routepath-") && !key.startsWith("linkcost-") && !key.startsWith("srcroute")
                && !key.equals("ttl") && !key.equals(SIG) && !key.equals(PENDING) && !key.equals(MsgBatchSigner.TREE)
                && !key.equals(MsgSessions.MAC) && !key.equals(DEFERRED) && !key.equals(DIRECT)
                && !Caller.RESERVED_PARAMS.contains(key);
    }

    /**
     * The bytes a signature covers (length-prefixed, parameters in key order; a parameter without a value is absent): the
     * version, the type and the source header, then the number of covered parameters and each one's key and value -- every
     * string as a big-endian int length (-1 for null) and its UTF-8 bytes. Built on every message signed, MAC'd or checked,
     * so without a sorted map or a stream: the same bytes as before (CanonicalFormTest holds it to the reference).
     */
    static byte[] canonical(MsgEvent m) {
        Map<String, String> params = m.getParams();
        String[] kv = new String[params == null ? 0 : 2 * params.size()];
        int n = 0;
        if (params != null) {
            for (Map.Entry<String, String> e : params.entrySet()) {
                String v = e.getValue();
                if (v != null && covered(e.getKey()) && 2 * n + 1 < kv.length) {
                    kv[2 * n] = e.getKey();
                    kv[2 * n + 1] = v;
                    n++;
                }
            }
        }
        sortPairs(kv, n);
        Bytes o = new Bytes(256 + 64 * n);
        o.str(VERSION);
        o.str(m.getMsgType() == null ? null : m.getMsgType().name());
        o.str(m.getSrcRegion());
        o.str(m.getSrcAgent());
        o.str(m.getSrcPlugin());
        o.writeInt(n);
        for (int i = 0; i < n; i++) {
            o.str(kv[2 * i]);
            o.str(kv[2 * i + 1]);
        }
        return o.toByteArray();
    }

    /** Sort the first {@code n} (key, value) pairs of {@code kv} by key (String order, as a TreeMap); keys are unique. */
    private static void sortPairs(String[] kv, int n) {
        for (int i = 1; i < n; i++) {                        // insertion sort: a message has a few dozen parameters
            String k = kv[2 * i], v = kv[2 * i + 1];
            int j = i - 1;
            while (j >= 0 && kv[2 * j].compareTo(k) > 0) {
                kv[2 * j + 2] = kv[2 * j];
                kv[2 * j + 3] = kv[2 * j + 1];
                j--;
            }
            kv[2 * j + 2] = k;
            kv[2 * j + 3] = v;
        }
    }

    /** A growable byte buffer writing what DataOutputStream would: big-endian ints, length-prefixed UTF-8 strings. */
    private static final class Bytes {
        private byte[] b;
        private int len;

        Bytes(int capacity) { b = new byte[Math.max(64, capacity)]; }

        private void room(int more) {
            if (len + more > b.length) b = java.util.Arrays.copyOf(b, Math.max(b.length * 2, len + more));
        }

        void writeInt(int v) {
            room(4);
            b[len++] = (byte) (v >>> 24);
            b[len++] = (byte) (v >>> 16);
            b[len++] = (byte) (v >>> 8);
            b[len++] = (byte) v;
        }

        void str(String s) {
            if (s == null) { writeInt(-1); return; }
            byte[] u = s.getBytes(StandardCharsets.UTF_8);
            writeInt(u.length);
            room(u.length);
            System.arraycopy(u, 0, b, len, u.length);
            len += u.length;
        }

        byte[] toByteArray() { return java.util.Arrays.copyOf(b, len); }
    }

    // ================================================================ the recipient

    /**
     * Where {@code m} is delivered: {region, agent, plugin (null for the node)} -- the last waypoint of a source route (and
     * the plugin saved for it), else its destination; null when it names none.
     */
    static String[] effectiveDestination(MsgEvent m) {
        String route = m.getParam(MsgRouter.SRCROUTE);
        if (route != null && !route.trim().isEmpty()) {
            String last = null;
            for (String h : route.split(";")) {
                h = h.trim();
                if (!h.isEmpty()) last = h;
            }
            int c = last == null ? -1 : last.indexOf(',');
            if (c <= 0 || c >= last.length() - 1) return null;
            String p = m.getParam(MsgRouter.SRCROUTE_DST_PLUGIN);
            return new String[]{last.substring(0, c), last.substring(c + 1), p == null || p.isEmpty() ? null : p};
        }
        if (m.getDstRegion() == null || m.getDstAgent() == null) return null;
        return new String[]{m.getDstRegion(), m.getDstAgent(), m.getDstPlugin() == null || m.getDstPlugin().isEmpty() ? null : m.getDstPlugin()};
    }

    /**
     * The recipient this node signs on {@code m}, which it originates and sends now (its router has decided the tier and any
     * source route): {@code <region>/<agent>/<plugin>}; for a function of the global that an agent hands its region
     * controller to carry on ({@link ControllerTiers#targetsGlobal}), {@code ^<region>/<agent>}: "the global above this
     * region controller". Null when the message names no destination.
     */
    String recipient(MsgEvent m) {
        String[] d = effectiveDestination(m);
        if (d == null) return null;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        if (cs != null && !cs.isRegionalController() && m.getParam(MsgRouter.SRCROUTE) == null && ControllerTiers.targetsGlobal(m)
                && d[0].equals(cs.getRegionalRegion()) && d[1].equals(cs.getRegionalAgent())) {
            return "^" + d[0] + "/" + d[1];
        }
        return d[0] + "/" + d[1] + "/" + (d[2] == null ? "" : d[2]);
    }

    /** The node (region_agent) of a plain recipient; null for {@code ^...} or a malformed one. */
    static String recipientNode(String to) {
        if (to == null || to.startsWith("^")) return null;
        int a = to.indexOf('/'), b = a < 0 ? -1 : to.indexOf('/', a + 1);
        return a <= 0 || b <= a + 1 ? null : to.substring(0, a) + "_" + to.substring(a + 1, b);
    }

    private String alias() {
        if (!aliasRead) {
            io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
            if (cs == null) return null;
            alias = RegionAlias.of(ce.getPluginBuilder(), cs.isGlobalController());
            aliasRead = true;
        }
        return alias;
    }

    /** Why {@code m}, signed for {@code to}, is not for this node, or null. */
    String recipientRefusal(String to, MsgEvent m) {
        if (to == null) return "a message that names no recipient (" + TO + ")";
        String region = ce.cstate.getRegion(), agent = ce.cstate.getAgent();
        if (to.startsWith("^")) {
            String rest = to.substring(1);
            int s = rest.indexOf('/');
            if (s <= 0 || s >= rest.length() - 1) return "a malformed recipient " + to;
            String r = rest.substring(0, s), a = rest.substring(s + 1);
            if (!ce.cstate.isGlobalController()) return "a message for the global above " + rest + ", and this node is not a global";
            if (!(r.equals(region) && a.equals(agent)) && !childControllers.containsKey(r + "/" + a)) {
                return "a message for the global above " + rest + ", which is not a region controller of this global";
            }
            return null;
        }
        int a1 = to.indexOf('/'), a2 = a1 < 0 ? -1 : to.indexOf('/', a1 + 1);
        if (a1 <= 0 || a2 <= a1 + 1) return "a malformed recipient " + to;
        String r = to.substring(0, a1), a = to.substring(a1 + 1, a2), p = to.substring(a2 + 1);
        if (!a.equals(agent) || !(r.equals(region) || r.equals(alias()))) {
            return "a message signed for " + r + "/" + a + ", not for this node (" + region + "/" + agent + ")";
        }
        String[] d = effectiveDestination(m);
        String delivered = d == null ? null : d[2];
        if (!(p.isEmpty() ? delivered == null : p.equals(delivered))) {
            return "a message signed for " + (p.isEmpty() ? "this node's controller" : "plugin " + p) + ", delivered to "
                    + (delivered == null ? "its controller" : "plugin " + delivered);
        }
        return null;
    }

    /**
     * Does this node only carry {@code m} on (it is addressed here, but its recipient is beyond: a source route's waypoint,
     * or a region controller carrying an agent's global function to its global)? Such a message is not checked on arrival;
     * it is checked wherever it is dispatched ({@link #verifyIfDeferred}).
     */
    public boolean relayOnly(MsgEvent m) {
        if (!required || m == null) return false;
        String to = m.getParam(TO);
        String region = ce.cstate.getRegion(), agent = ce.cstate.getAgent();
        if (to != null && to.startsWith("^") && !ce.cstate.isGlobalController() && to.equals("^" + region + "/" + agent)) return true;
        if (m.getParam(MsgRouter.SRCROUTE) != null) {
            String[] d = effectiveDestination(m);
            return d != null && !(d[1].equals(agent) && (d[0].equals(region) || d[0].equals(alias())));
        }
        return false;
    }

    // ================================================================ origin side

    /**
     * At a stamping point (the controller stamped a message this node originates): drop any signature the message carried
     * (a re-sent message is signed anew), set {@code cresco_tenant} when absent (so it is signed), and mark it for signing
     * when it leaves this node ({@link #signIfPending}). In-JVM deliveries are never signed.
     */
    public void markForSigning(MsgEvent m, String nodeTenant) {
        if (m == null || m.getParams() == null) return;
        clear(m);
        if (!required) return;
        Caller c = Caller.fromMsgEvent(m);
        if (c == null || c.isAnonymous()) return;
        String t = m.getParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM);
        if ((t == null || t.isEmpty()) && nodeTenant != null) m.setParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM, nodeTenant);
        m.setParam(PENDING, "1");
    }

    /** What a node writes to authenticate a message it originates. */
    private static final String[] AUTH_PARAMS = {SIG, BY, TS, CRED, S_CALLER, S_TENANT, S_ROLES, CAPS, TO, MsgBatchSigner.TREE,
            MsgSessions.SID, MsgSessions.MAC, MsgSessions.KX, MsgSessions.KX_ACK, MsgSessions.HELD, MsgSessions.INC};

    /** Remove every signature parameter. */
    public static void clear(MsgEvent m) {
        if (m == null || m.getParams() == null) return;
        for (String k : AUTH_PARAMS) m.removeParam(k);
        m.removeParam(PENDING);
        m.removeParam(DIRECT);
        m.removeParam(DEFERRED);
    }

    /** A message off the wire never asks THIS node to sign it: only this node's own stamping points do. */
    public static void notPending(MsgEvent m) {
        if (m != null && m.getParams() != null) {
            m.removeParam(PENDING);
            m.removeParam(DIRECT);               // a sender's word that it is the origin counts only as its broker attests it
            m.removeParam(DEFERRED);             // what this node checked is its own record, never a sender's
        }
    }

    /** As a message this node originated leaves it (AgentProducer): sign it. Relayed messages carry their origin's signature. */
    public void signIfPending(MsgEvent m) {
        if (m == null || m.getParams() == null) return;
        m.removeParam(DEFERRED);                 // this node's own note: never on the wire
        if (m.getParam(PENDING) == null) return;
        m.removeParam(PENDING);
        if (!required) return;
        Caller c = Caller.fromMsgEvent(m);
        if (c == null || c.isAnonymous()) return;
        String to = recipient(m);
        if (to == null) {
            refusal("a message this node sends names no destination (" + m.getMsgType() + " " + m.getParam("action") + "): it goes unsigned");
            return;
        }
        if (directTo(m, to)) {
            // a direct link: the receiver's broker attests this node first hand (CallerStamps.P_DIRECT); no signature
            m.setParam(DIRECT, ce.cstate.getRegion() + "_" + ce.cstate.getAgent());
            if (directSent.incrementAndGet() == 1) info("direct links ON: messages to this node's parent and children that accept them"
                    + " travel unsigned, attested first hand by the receiving broker (first: " + m.getMsgType() + " " + m.getParam("action")
                    + " to " + m.getDstRegion() + "/" + m.getDstAgent() + ")");
            return;
        }
        try {
            m.setParam(TO, to);
            m.setParam(S_CALLER, c.getPrincipal());
            m.setParam(S_TENANT, c.getTenant());
            m.setParam(S_ROLES, c.getRolesCsv());
            long now = System.currentTimeMillis();
            m.setParam(BY, selfName());
            m.setParam(TS, Long.toString(now));
            String caps = sessionsBroken ? (batch ? "batch" : null) : ownCaps;
            if (caps != null) m.setParam(CAPS, caps);
            // the peer of sessions and batch signatures is the recipient (never a relay, nor the region carrying a ^ message)
            String node = recipientNode(to);
            if (sessions != null && !sessionsBroken) m.setParam(MsgSessions.INC, sessions.incarnation);
            if (sessions != null && !sessionsBroken && node != null) {
                // what this node says to the peer about their session; then a MAC under it -- unless the message carries an
                // offer or an answer, which ride signed messages (their sender's current credential)
                try {
                    MsgSessions.Session ss = sessions.usable(node, now);
                    String held = sessions.heldWith(node);
                    if (held != null) m.setParam(MsgSessions.HELD, held);
                    String ack = sessions.ackFor(node, now, ss != null);
                    if (ack != null) m.setParam(MsgSessions.KX_ACK, ack);
                    String kx = peerReads(node, CAP_SESSION) ? sessions.offerFor(node, now, ss != null) : null;
                    if (kx != null) m.setParam(MsgSessions.KX, kx);
                    if (ss != null && kx == null && ack == null) {
                        m.setParam(MsgSessions.SID, ss.sid);
                        m.setParam(MsgSessions.MAC, Base64.getEncoder().encodeToString(MsgSessions.mac(ss.key, selfName(), canonical(m))));
                        if (macSent.incrementAndGet() == 1) info("session keys ON: messages to the nodes this node holds a session with"
                                + " carry a MAC under it instead of a signature (first: " + m.getMsgType() + " " + m.getParam("action")
                                + " to " + m.getDstRegion() + "/" + m.getDstAgent() + ")");
                        return;
                    }
                } catch (Exception | Error e) {
                    // a provider may refuse (a FIPS module raises Errors): this node stays with signatures
                    if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
                    sessionsFailed(e);
                    for (String k : new String[]{MsgSessions.KX, MsgSessions.KX_ACK, MsgSessions.SID, MsgSessions.MAC}) m.removeParam(k);
                }
            }
            m.setParam(CRED, credential());
            if (batch && node != null && peerReads(node, CAP_BATCH)) {
                // shares the private-key operation with the messages being signed at the same moment (alone: a single one)
                MsgBatchSigner.Signed bs = batcher().sign(canonical(m));
                m.setParam(SIG, bs.sig);
                if (bs.tree != null) {
                    m.setParam(MsgBatchSigner.TREE, bs.tree);
                    batchedSigned.incrementAndGet();
                }
            } else {
                Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(ownKey().getAlgorithm()));
                s.initSign(ownKey());
                s.update(canonical(m));
                m.setParam(SIG, Base64.getEncoder().encodeToString(s.sign()));
            }
            if (signedCount.incrementAndGet() == 1) info("message signatures ON: this node signs what it originates (first: "
                    + m.getMsgType() + " " + m.getParam("action") + " as " + c + " for " + to + ", credential " + m.getParam(CRED).substring(0, 4) + ")");
        } catch (Exception | Error e) {
            if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
            for (String k : AUTH_PARAMS) m.removeParam(k);
            refusal("cannot sign a message this node sends (" + e.getClass().getSimpleName() + ": " + e.getMessage() + "): it goes unsigned and is refused");
        }
    }

    private String selfName() {
        String t = ce.tenant(), r = ce.cstate.getRegion(), a = ce.cstate.getAgent();
        String[] c = selfNameCache;                  // built anew only when the node's name changes
        if (c != null && java.util.Objects.equals(c[0], t) && java.util.Objects.equals(c[1], r) && java.util.Objects.equals(c[2], a)) {
            return c[3];
        }
        String name = t + "/" + r + "/" + a;
        selfNameCache = new String[]{t, r, a, name};
        return name;
    }

    /** tenant, region, agent and the name built of them, as one value. */
    private volatile String[] selfNameCache;

    private PrivateKey ownKey() throws Exception {
        return ce.getCertificateManager().identityPrivateKey();
    }

    private String ownCert() throws Exception {
        X509Certificate leaf = ce.getCertificateManager().getPublicCertificate()[0];
        String b = ownCertB64;
        if (leaf != ownCertLeaf || b == null) {
            b = Base64.getEncoder().encodeToString(leaf.getEncoded());
            ownKeyB64 = Base64.getEncoder().encodeToString(leaf.getPublicKey().getEncoded());
            ownKeyFp = PeerTrustRegistry.fingerprint(leaf.getPublicKey().getEncoded());
            ownCertB64 = b;
            ownCertLeaf = leaf;
        }
        return b;
    }

    /** An agent's attestation while it is its own and valid (a tenth of its life left at least), else this node's certificate. */
    String credential() throws Exception {
        String cert = ownCert();
        OwnAttestation att = currentAttestation();
        if (att != null && System.currentTimeMillis() < att.exp - attestationMs / 10) return "att:" + att.json;
        return "cert:" + cert;
    }

    /**
     * The attestation this node holds while it is still this node's: for its present name and key, and issued by its
     * present parent (when it has one: between a parent's loss and the next join the one before stands). An agent that
     * moved to another region or parent, or took a new key, signs with its certificate until its parent attests it anew
     * (its next ping reports that it holds none). Call after {@link #ownCert()} (which reads this node's key).
     */
    private OwnAttestation currentAttestation() {
        OwnAttestation a = ownAttestation;
        if (a == null) return null;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        if (cs == null || !a.region.equals(cs.getRegion()) || !a.agent.equals(cs.getAgent()) || !a.key.equals(ownKeyB64)) return null;
        String pr = cs.getRegionalRegion(), pa = cs.getRegionalAgent();
        return pr == null || pa == null || a.issuer.equals(pr + "/" + pa) ? a : null;
    }

    // ================================================================ receiving side

    /** The result of {@link #verify}: the signed caller, or why the message is refused. */
    public static final class Verdict {
        public final Caller caller;
        public final String refusal;
        /**
         * Not accepted, but not refused for good: its signer's credential does not verify HERE YET (its controller, or
         * that controller's present key, is not in the trust data this node holds), which newer trust data may cure. Only
         * a caller that said it can hold the message gets this ({@link #verify(MsgEvent, Caller, boolean, CrescoIdentity, boolean)}):
         * nothing was counted or logged; it verifies the message again later, or refuses it ({@link #refusedAfterHold}).
         */
        public final boolean later;
        Verdict(Caller caller, String refusal) { this(caller, refusal, false); }
        Verdict(Caller caller, String refusal, boolean later) { this.caller = caller; this.refusal = refusal; this.later = later; }
        public boolean ok() { return refusal == null; }
    }

    /**
     * Verify {@code m} as it arrived (after its stamp was taken from the broker's properties: {@code fromBroker}, or null).
     * {@code legacyAdmitted}: an unsigned message from a node without signing (stamped {@code agent:} by its broker) is
     * admitted -- the upgrade window.
     */
    public Verdict verify(MsgEvent m, Caller fromBroker, boolean legacyAdmitted) {
        return verify(m, fromBroker, legacyAdmitted, null);
    }

    /** Note on {@code m} (off the wire) that this node did not verify it on arrival: it is verified where it is dispatched. */
    public void deferred(MsgEvent m) {
        if (required && m != null && m.getParams() != null) m.setParam(DEFERRED, "1");
    }

    /**
     * Before {@code m} is dispatched on this node: a message off the wire this node did not verify on arrival (it was not
     * addressed here, or only carried on) is verified now, as addressed to this node -- whatever re-addressed it on the way
     * (the controller tiers, a source route). Null when there was nothing to verify (an in-JVM message, or one verified on
     * arrival); else the verdict (its caller re-stamped when the broker's stamp was stripped on the way).
     */
    public Verdict verifyIfDeferred(MsgEvent m, boolean legacyAdmitted) {
        if (m == null || m.getParams() == null || m.getParam(DEFERRED) == null) return null;
        m.removeParam(DEFERRED);
        if (!required) return null;
        Caller fromBroker = Caller.fromMsgEvent(m);
        if (fromBroker != null && fromBroker.isAnonymous()) fromBroker = null;
        String aliasParam = m.getParam(Caller.PARAM_ALIAS);
        Verdict v = verify(m, fromBroker, legacyAdmitted, null);
        if (v.ok() && v.caller != null && !v.caller.equals(fromBroker)) {
            CallerStamps.stamp(m, v.caller, aliasParam == null || aliasParam.isEmpty() ? null : aliasParam);
        }
        return v;
    }

    /**
     * As above; {@code firstHand}: the node the receiving broker attests delivered this message first hand as its origin
     * ({@link CallerStamps#P_DIRECT}, as the consumer trusts it: its own broker's, or its parent's broker for the parent's
     * own messages), or null. An unsigned message is accepted when that node may vouch for the broker's stamp
     * ({@link #vouchRefusal}): it is the stamp's own node, or (an API user) a node of the user's tenant.
     */
    public Verdict verify(MsgEvent m, Caller fromBroker, boolean legacyAdmitted, CrescoIdentity firstHand) {
        return verify(m, fromBroker, legacyAdmitted, firstHand, false);
    }

    /**
     * As above; {@code hold}: the caller can hold the message and verify it again later, so a credential that does not
     * verify here YET is answered {@link Verdict#later} instead of a refusal.
     */
    public Verdict verify(MsgEvent m, Caller fromBroker, boolean legacyAdmitted, CrescoIdentity firstHand, boolean hold) {
        if (!required) return new Verdict(fromBroker, null);
        String sigB64 = m.getParam(SIG);
        String macB64 = sigB64 == null ? m.getParam(MsgSessions.MAC) : null;
        if (sigB64 == null && macB64 == null) {
            if (legacyAdmitted && fromBroker != null && fromBroker.getKind() == Caller.Kind.AGENT) return new Verdict(fromBroker, null);
            if (direct && firstHand != null && fromBroker != null && !fromBroker.isAnonymous() && vouchRefusal(firstHand, fromBroker,
                    ce.getPluginBuilder() == null ? java.util.Set.of() : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig())) == null) {
                if (directAccepted.incrementAndGet() == 1) info("direct links ON: this node accepts unsigned messages its broker attests"
                        + " first hand (first: " + m.getMsgType() + " " + m.getParam("action") + " from " + fromBroker + ")");
                return new Verdict(fromBroker, null);
            }
            return refuse(m, "an unsigned message (from " + fromBroker + (firstHand == null ? ", not delivered first hand" : ", delivered by "
                    + firstHand) + ")");
        }
        try {
            String by = m.getParam(BY), cred = m.getParam(CRED), ts = m.getParam(TS), to = m.getParam(TO);
            if (by == null || ts == null || (macB64 == null && cred == null)) return refuse(m, "a signature without its signer, credential or time");
            long t = Long.parseLong(ts), now = System.currentTimeMillis();
            if (now - t > maxAgeMs) {
                return refuse(m, "a signature made " + (now - t) / 1000 + " s ago (msg_signature_max_age_ms=" + maxAgeMs + ")");
            }
            if (t - now > maxFutureMs) {
                return refuse(m, "a signature made " + (t - now) / 1000 + " s ahead of this node's clock (msg_signature_max_future_ms="
                        + maxFutureMs + "): the clocks of this node and " + by + " differ");
            }
            // the recipient first: a message signed for another node is refused before any key operation
            String notMine = recipientRefusal(to, m);
            if (notMine != null) return refuse(m, notMine + " (from " + by + ")");
            MsgSessions.Found under = null;
            Signer who;
            if (macB64 != null) {
                String peerNode = nodeOf(by);
                under = sessions == null || peerNode == null ? null : sessions.lookup(peerNode, by, m.getParam(MsgSessions.SID));
                if (under == null) return refuse(m, "a message under a session key this node does not hold (from " + by
                        + ": this node restarted or the session ended; its sender returns to signatures)");
                if (now > under.session.expires) return refuse(m, "a message under an ended session key (" + by + ")");
                who = under.peer.signer;
                if (who == null) return refuse(m, "a message under a session key whose peer no longer verifies (" + by + ")");
            } else {
                String[] why = new String[2];                // [1]: set when newer trust data may cure it
                who = resolve(cred, why);
                if (who == null) {
                    String reason = "the signer's credential does not verify here (" + by + (why[0] == null ? "" : ": " + why[0]) + ")";
                    if (hold && why[1] != null) {
                        wantTrust();                         // newer trust data may cure it: ask the parent now, not at the next ping
                        return new Verdict(null, reason, true);
                    }
                    return refuse(m, reason);
                }
            }
            if (!who.name.equals(by)) return refuse(m, "signed by " + who.name + ", which is not the declared signer " + by);
            String revoked = signerRefusal(who, now);
            if (revoked != null) {
                if (under != null) sessions.dropPeer(under.peer.node);
                return refuse(m, "the signer " + by + " is refused: " + revoked);
            }
            if (to.startsWith("^") && !to.startsWith("^" + who.region + "/")) {
                return refuse(m, "a message for the global above " + to.substring(1) + ", signed by " + by + " of another region");
            }
            byte[] replayKey;
            if (under != null) {
                byte[] mac = Base64.getDecoder().decode(macB64);
                if (!MessageDigest.isEqual(mac, MsgSessions.mac(under.session.key, by, canonical(m)))) {
                    return refuse(m, "the session MAC does not verify (" + by + ")");
                }
                replayKey = mac;
            } else {
                byte[] sig = Base64.getDecoder().decode(sigB64);
                String tree = m.getParam(MsgBatchSigner.TREE);
                replayKey = sig;
                if (tree != null) {
                    // a batch signature: this message's own leaf leads to the signed root; the root's signature once per batch
                    byte[] leaf = MsgBatchSigner.leaf(canonical(m));
                    int[] count = new int[1];
                    byte[] root = MsgBatchSigner.root(leaf, tree, count);
                    if (root == null) return refuse(m, "a malformed batch signature path (" + by + ")");
                    String cacheKey = Base64.getEncoder().encodeToString(sha256.get().digest(sig)) + "|" + who.fp + "|"
                            + Base64.getEncoder().encodeToString(root) + "|" + count[0];
                    if (batchVerified.getIfPresent(cacheKey) == null) {
                        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(who.key.getAlgorithm()));
                        s.initVerify(who.key);
                        s.update(MsgBatchSigner.signedBytes(root, count[0]));
                        if (!s.verify(sig)) return refuse(m, "the batch signature does not verify (" + by + ")");
                        batchVerified.put(cacheKey, Boolean.TRUE);
                    }
                    replayKey = java.nio.ByteBuffer.allocate(sig.length + leaf.length).put(sig).put(leaf).array();
                } else {
                    Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(who.key.getAlgorithm()));
                    s.initVerify(who.key);
                    s.update(canonical(m));
                    if (!s.verify(sig)) return refuse(m, "the signature does not verify (" + by + ")");
                }
            }
            Caller signed = Caller.parse(m.getParam(S_CALLER), m.getParam(S_TENANT), m.getParam(S_ROLES));
            if (signed == null || signed.isAnonymous()) return refuse(m, "no signed caller");
            String vouch = vouchRefusal(CrescoIdentity.of(who.tenant, who.region, who.agent, null), signed,
                    ce.getPluginBuilder() == null ? java.util.Set.of() : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig()));
            if (vouch != null) return refuse(m, vouch);
            if (fromBroker != null && !fromBroker.equals(signed)) {
                return refuse(m, "the broker's caller " + fromBroker + " is not the signed caller " + signed);
            }
            switch (replay.record(ReplayMemory.digest64(sha256.get().digest(replayKey)), t, now, signerMemoryKey(who))) {
                case FRESH: break;
                case REPLAY: return refuse(m, "a signature seen before (a replay) from " + by);

                case TOO_OLD: return refuse(m, "a message signed " + (now - t) / 1000 + " s ago while the replay memory for " + by + " is full");
                case FULL: return refuse(m, "a message while the replay memory for " + by + " (or of this node) is full");
                default: return refuse(m, "a message signed at a time this node's clock went back past (from " + by + ")");
            }
            String peerNode = who.node();
            int caps = parseCaps(m.getParam(CAPS));
            Integer had = peerCaps.getIfPresent(peerNode);
            if (had == null || had != caps) peerCaps.put(peerNode, caps);
            if (sessions != null && !sessionsBroken && !to.startsWith("^")) {
                try {
                    String changed = sessions.received(selfName(), by, peerNode, m.getParam(MsgSessions.INC), who,
                            m.getParam(MsgSessions.KX), m.getParam(MsgSessions.KX_ACK), m.getParam(MsgSessions.HELD),
                            under == null ? null : under.session.sid, under == null, now);
                    if (changed != null && !changed.equals("lost") && sessionsMade.incrementAndGet() == 1) {
                        info("session keys ON: this node agreed a session key with " + by + " (" + changed + ")");
                    }
                } catch (Exception | Error e) {
                    if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
                    sessionsFailed(e);
                }
            }
            if (under != null && macVerified.incrementAndGet() == 1) info("session keys ON: this node accepts messages under a session"
                    + " key it holds (first: " + m.getMsgType() + " " + m.getParam("action") + " from " + by + ")");
            if (verifiedCount.incrementAndGet() == 1) info("message signatures ON: this node checks what it dispatches (first: "
                    + m.getMsgType() + " " + m.getParam("action") + " from " + signed + ", signed by " + by + " for " + to + ")");
            return new Verdict(signed, null);
        } catch (Exception | Error e) {
            if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
            return refuse(m, "an unreadable signature (" + e.getClass().getSimpleName() + ")");
        }
    }

    /** Whose replay memory {@code who}'s messages are remembered in: its key's (64 bits of its fingerprint; never 0). */
    static long signerMemoryKey(Signer who) {
        long h = 1125899906842597L;
        String fp = who.fp;
        for (int i = 0; i < fp.length(); i++) h = 31 * h + fp.charAt(i);
        return h == 0 ? 1 : h;
    }

    /** {@code tenant/region/agent} -> {@code region_agent}, or null. */
    private static String nodeOf(String by) {
        int a = by.indexOf('/'), b = a < 0 ? -1 : by.indexOf('/', a + 1);
        return a <= 0 || b <= a + 1 || b == by.length() - 1 ? null : by.substring(a + 1, b) + "_" + by.substring(b + 1);
    }

    /**
     * Why {@code who} may not sign here NOW, or null: its credential's end passed, its key (or an attestation issuer's) is
     * revoked here, its name (or the issuer's) is tombstoned, or a certificate behind it is out of its validity or revoked
     * by the operator's {@code revocation_file} (with a PKI). Lock-free, O(path).
     */
    String signerRefusal(Signer who, long now) {
        if (who.self) return null;
        if (now > who.expires) return "its credential ended " + new java.util.Date(who.expires);
        PeerTrustRegistry reg = peers();
        if (reg.isRevokedFor(who.fp, who.region)) return "its key is revoked";
        String why = reg.nameRefusal(who.region, who.agent);
        if (why != null) return why;
        why = reg.refsRefusal(who.refs, who.region, now);
        if (why != null) return why;
        if (who.issuer != null) {
            int s = who.issuer.indexOf('/');
            String ir = who.issuer.substring(0, s), ia = who.issuer.substring(s + 1);
            if (ir.equals(ce.cstate.getRegion()) && ia.equals(ce.cstate.getAgent())) return null;      // this node attested it
            if (reg.isRevokedFor(who.issuerFp, ir)) return "its attestation's issuer " + who.issuer + " has a revoked key";
            why = reg.nameRefusal(ir, ia);
            if (why != null) return "its attestation's issuer: " + why;
            why = reg.refsRefusal(who.issuerRefs, ir, now);
            if (why != null) return "its attestation's issuer " + who.issuer + ": " + why;
        }
        return null;
    }

    /** May node {@code signer} sign for {@code c}? Its own controller and plugins; an API user of its tenant (any, superuser). */
    static String vouchRefusal(CrescoIdentity signer, Caller c, java.util.Set<String> superuserTenants) {
        switch (c.getKind()) {
            case CONTROLLER:
            case PLUGIN:
            case AGENT:
                return signer.getTenant().equals(c.getTenant()) && signer.getRegion().equals(c.getRegion()) && signer.getAgent().equals(c.getAgent())
                        ? null : c + " was signed by another node, " + signer;
            case API:
                return signer.getTenant().equals(c.getTenant()) || (superuserTenants != null && superuserTenants.contains(signer.getTenant()))
                        ? null : "a user of tenant " + c.getTenant() + " signed by a node of tenant " + signer.getTenant();
            default:
                return "a " + c.getKind() + " caller is never signed";
        }
    }

    private Verdict refuse(MsgEvent m, String why) {
        refusedCount.incrementAndGet();
        refusal(m, why);
        return new Verdict(null, why);
    }

    /** A message its receiver held ({@link Verdict#later}, or while this node's trust data was arriving) is refused after all. */
    public void refusedAfterHold(MsgEvent m, String why) {
        refuse(m, why);
    }

    // ---- trust data arriving (a node's first moments with a parent)

    private volatile String heardParent;
    private final java.util.concurrent.CopyOnWriteArrayList<Runnable> trustListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Has this node taken a pong of the parent {@code region/agent} (with it: the trust bundle when this node held another,
     * an agent's attestation, the parent's revocations)? Until then it cannot judge any signer but that parent.
     */
    public boolean heardParent(String region, String agent) {
        String h = heardParent;
        return h != null && h.equals(region + "/" + agent);
    }

    /** {@code listener} runs (on the changing thread: keep it short) whenever the trust data this node verifies with changed. */
    public void onTrustChange(Runnable listener) { trustListeners.add(listener); }

    public void removeTrustListener(Runnable listener) { trustListeners.remove(listener); }

    private void trustChanged() {
        trustWantedAt = 0L;                                  // what was asked for may be in: asked again if it is not
        for (Runnable r : trustListeners) {
            try { r.run(); } catch (RuntimeException ignore) { }
        }
    }

    /** The trust data this node judges with is complete now (the parent's whole revocation list is in): held messages are tried again. */
    public void trustDataArrived() { trustChanged(); }

    // ---- trust data on demand (a node that holds a message it cannot judge asks its parent at once)

    /** On a ping: its sender holds a message it cannot judge with the trust data it has (its parent asks its own parent too). */
    static final String P_WANT = "sig_want";
    /** A node asks its parent at most this often, and for this long after the last message it could not judge. */
    static final long WANT_EVERY_MS = 1_000L, WANT_FOR_MS = 5_000L;
    private volatile long trustWantedAt, trustAskedAt;

    /**
     * This node holds a message it cannot judge yet (its signer's controller is not in the trust data held, or the parent's
     * first pong is not in): its health watcher pings the parent at once instead of at the next interval -- the pong brings
     * the trust bundle when the parent holds a newer one. Without it a held message waited for the next ping (5 s), as long
     * as it may be held at all.
     */
    public void wantTrust() {
        if (required) trustWantedAt = System.currentTimeMillis();
    }

    /**
     * For the health watcher (every 250 ms): should it ping the parent now for trust data? True at most once every
     * {@value #WANT_EVERY_MS} ms, for {@value #WANT_FOR_MS} ms after the last message this node could not judge, and not
     * once trust data changed since. One volatile read when nothing is wanted.
     */
    public boolean takeTrustWanted(long now) {
        long at = trustWantedAt;
        if (at == 0L) return false;
        if (now - at > WANT_FOR_MS || now < at) {
            trustWantedAt = 0L;
            return false;
        }
        if (now - trustAskedAt < WANT_EVERY_MS && now >= trustAskedAt) return false;
        trustAskedAt = now;
        return true;
    }

    // ---- direct links end with the link

    /**
     * A consumer of the inbox {@code node} ({@code region_agent}) left this node's broker (the node stopped, moved to
     * another controller, or its link broke): nothing this node sends reaches it first hand any more, so messages to it are
     * signed until its next ping offers the direct link again. The offer alone held for {@value #DIRECT_OFFER_MS} ms: a
     * node that moved away (and answered at its former address through another broker) refused every unsigned message its
     * former parent sent it for a minute.
     */
    public void inboxConsumerLeft(String node) {
        if (node == null || directChildren.remove(node) == null) return;
        // a ping of that node still on its way here must not offer the link again: only a ping after this moment does
        if (inboxLeftAt.size() >= DIRECT_CHILDREN_MAX) inboxLeftAt.clear();
        inboxLeftAt.put(node, System.currentTimeMillis());
    }

    /** Direct children whose inbox consumer left this broker, and when: their pings offer the link again only after {@value #DIRECT_AGAIN_MS} ms. */
    private final ConcurrentHashMap<String, Long> inboxLeftAt = new ConcurrentHashMap<>();
    static final long DIRECT_AGAIN_MS = 5_000L;

    /** May {@code child}'s ping (now) make it a direct child? Not within {@value #DIRECT_AGAIN_MS} ms of its inbox consumer leaving. */
    private boolean directAgain(String child, long now) {
        Long left = inboxLeftAt.get(child);
        if (left == null) return true;
        if (now >= left && now - left < DIRECT_AGAIN_MS) return false;
        inboxLeftAt.remove(child, left);
        return true;
    }

    /**
     * This node's link to its parent is down, was just made anew, or a ping went unanswered: what this node sends its
     * parent is signed until the parent's next pong offers the direct link again. A parent that restarted cannot attest
     * the first messages of a link it has not bound yet; unsigned, they were refused.
     */
    public void parentLinkReset() { directParentUntil = 0L; }

    private void info(String line) {
        try {
            ce.getPluginBuilder().getLogger(MsgSigning.class.getName(), io.cresco.library.utilities.CLogger.Level.Info).info(line);
        } catch (RuntimeException ignore) { }
    }

    private void warn(String line) {
        try {
            ce.getPluginBuilder().getLogger(MsgSigning.class.getName(), io.cresco.library.utilities.CLogger.Level.Info).warn(line);
        } catch (RuntimeException ignore) { }
    }

    /** A refused message's log line names its type and action, which the reason does not. */
    private void refusal(MsgEvent m, String why) {
        refusal(m == null ? why : why + " [" + m.getMsgType() + " " + m.getParam("action") + "]");
    }

    private void refusal(String why) {
        long now = System.currentTimeMillis();
        if (now - lastRefusalLog > 10_000L) {
            lastRefusalLog = now;
            warn("MESSAGE SIGNATURE: REFUSED " + why + " (" + refusedCount.get() + " refused so far; logged at most every 10 s)");
        }
    }

    /** Credentials held verified (diagnostics, tests). */
    long credentialsCached() { credCache.cleanUp(); return credCache.size(); }

    /** Entries in the replay memory (diagnostics, tests). */
    long replayMemorySize() { return replay.size(); }

    /** Record an authenticator in the replay memory as {@link #verify} does (tests). */
    ReplayMemory.Result remember(byte[] authenticator, long signedAt, long now) {
        return replay.record(ReplayMemory.digest64(sha256.get().digest(authenticator)), signedAt, now);
    }

    private PeerTrustRegistry peers() {
        return ce.getCertificateManager().getPeerTrust();
    }

    /** A credential, verified once and cached (bounded) until its own end or an hour; {@code why[0]} when it does not verify. */
    private Signer resolve(String cred, String[] why) throws Exception {
        String k = Base64.getEncoder().encodeToString(sha256.get().digest(cred.getBytes(StandardCharsets.UTF_8)));
        long now = System.currentTimeMillis();
        Signer r = credCache.getIfPresent(k);
        if (r != null && r.expires > now) return r;
        r = cred.startsWith("cert:") ? resolveCert(cred.substring(5), now, why)
                : cred.startsWith("att:") ? resolveAttestation(cred.substring(4), now, why) : null;
        if (r == null) return null;
        credCache.put(k, r);
        return r;
    }

    private Signer resolveCert(String b64, long now, String[] why) throws Exception {
        X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(b64)));
        if (now > leaf.getNotAfter().getTime()) { why[0] = "an expired certificate"; return null; }
        String fp = PeerTrustRegistry.fingerprint(leaf.getPublicKey().getEncoded());
        long until = leaf.getNotAfter().getTime();
        X509Certificate[] own = ce.getCertificateManager().getPublicCertificate();
        if (own != null && own.length > 0 && own[0].getPublicKey().equals(leaf.getPublicKey())) {
            return new Signer(ce.tenant(), ce.cstate.getRegion(), ce.cstate.getAgent(), leaf.getPublicKey(), fp, List.of(), null, null,
                    null, until, true);
        }
        String[] idWhy = new String[1];
        CrescoIdentity id = peers().verifiedIdentity(new X509Certificate[]{leaf}, idWhy);
        if (id != null) {
            return new Signer(id.getTenant(), id.getRegion(), id.getAgent(), leaf.getPublicKey(), fp, intern(peers().pathRefs(new X509Certificate[]{leaf})),
                    null, null, null, until, false);
        }
        CrescoIdentity named = CrescoNames.identityOf(leaf);
        if (named == null) { why[0] = idWhy[0]; return null; }
        // a controller this node cannot verify itself: the trust bundle's word for its key -- never over this node's own
        // revocations, which signerRefusal applies to the certificate presented and to the bundle's, on every message
        BundleEntry e = bundleEntry(named.getRegion(), named.getAgent());
        if (e != null && fp.equals(e.fp) && named.getTenant().equals(e.t)) {
            List<PeerTrustRegistry.CertRef> refs = new ArrayList<>();
            refs.add(PeerTrustRegistry.CertRef.of(leaf));
            if (e.refs != null) refs.addAll(e.refs);
            return new Signer(e.t, e.r, e.a, e.key, fp, intern(refs), null, null, null, until, false);
        }
        why[0] = idWhy[0];
        if (why.length > 1) why[1] = "a controller (or its present key) this node's trust bundle does not list yet";
        return null;
    }

    private Signer resolveAttestation(String json, long now, String[] why) throws Exception {
        Attestation a = GSON.fromJson(json, Attestation.class);
        if (a == null || a.v != 2 || a.t == null || a.r == null || a.a == null || a.k == null || a.iss == null || a.sig == null
                || a.refs == null || a.refs.isEmpty()) { why[0] = "a malformed attestation"; return null; }
        for (PeerTrustRegistry.CertRef r : a.refs) if (r == null || !r.wellFormed()) { why[0] = "a malformed attestation"; return null; }
        if (now > a.exp) { why[0] = "an expired attestation"; return null; }
        String[] iss = a.iss.split("/", -1);
        if (iss.length != 2 || !iss[0].equals(a.r)) { why[0] = "an attestation by another region's controller"; return null; }
        PublicKey issuerKey;
        String issuerTenant;
        List<PeerTrustRegistry.CertRef> issuerRefs;
        if (iss[0].equals(ce.cstate.getRegion()) && iss[1].equals(ce.cstate.getAgent())) {
            issuerKey = ce.getCertificateManager().getPublicCertificate()[0].getPublicKey();
            issuerTenant = ce.tenant();
            issuerRefs = List.of();
        } else {
            BundleEntry e = bundleEntry(iss[0], iss[1]);
            if (e == null) {
                why[0] = "its issuer " + a.iss + " is not in the trust bundle here";
                if (why.length > 1) why[1] = "the trust bundle may list it soon";
                return null;
            }
            issuerKey = e.key;
            issuerTenant = e.t;
            issuerRefs = e.refs == null ? List.of() : e.refs;
        }
        // a region controller attests its own tenant's agents, unless it is a superuser tenant's (a multi-tenant region)
        if (!issuerTenant.equals(a.t) && !SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig()).contains(issuerTenant)) {
            why[0] = "an attestation of tenant " + a.t + " by a controller of tenant " + issuerTenant;
            return null;
        }
        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(issuerKey.getAlgorithm()));
        s.initVerify(issuerKey);
        s.update(attestationBytes(a));
        if (!s.verify(Base64.getDecoder().decode(a.sig))) {
            why[0] = "the attestation's signature does not verify";
            if (why.length > 1) why[1] = "its issuer may have another key than the one this node holds yet";
            return null;
        }
        PublicKey key = KeyFactory.getInstance(keyAlg(a.k)).generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(a.k)));
        String fp = PeerTrustRegistry.fingerprint(key.getEncoded());
        if (!fp.equals(a.refs.get(0).k)) { why[0] = "an attestation whose certificate is not of its key"; return null; }
        return new Signer(a.t, a.r, a.a, key, fp, intern(a.refs), a.iss, PeerTrustRegistry.fingerprint(issuerKey.getEncoded()),
                issuerRefs, a.exp, false);
    }

    /** {@code refs} with its CA certificates (all but the leaf) shared with every other signer standing on them. */
    private List<PeerTrustRegistry.CertRef> intern(List<PeerTrustRegistry.CertRef> refs) {
        if (refs == null || refs.isEmpty()) return List.of();
        PeerTrustRegistry.CertRef[] out = new PeerTrustRegistry.CertRef[refs.size()];
        out[0] = refs.get(0);
        for (int i = 1; i < out.length; i++) {
            PeerTrustRegistry.CertRef r = refs.get(i);
            PeerTrustRegistry.CertRef had = refInterner.asMap().putIfAbsent(r, r);
            out[i] = had == null ? r : had;
        }
        return List.of(out);
    }

    private static String keyAlg(String spkiB64) {
        // RSA keys today; EC accepted for a future curve change (the algorithm is in the SPKI)
        try {
            KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(spkiB64)));
            return "RSA";
        } catch (Exception notRsa) {
            return "EC";
        }
    }

    private static String refsCanonical(List<PeerTrustRegistry.CertRef> refs) {
        StringBuilder b = new StringBuilder();
        b.append(refs == null ? 0 : refs.size());
        if (refs != null) for (PeerTrustRegistry.CertRef r : refs) b.append('\n').append(r == null ? "-" : r.canonical());
        return b.toString();
    }

    private static byte[] attestationBytes(Attestation a) {
        return ("cresco-attestation-v2\n" + a.t + "\n" + a.r + "\n" + a.a + "\n" + a.k + "\n" + a.exp + "\n" + a.iss + "\n" + refsCanonical(a.refs))
                .getBytes(StandardCharsets.UTF_8);
    }

    private BundleEntry bundleEntry(String region, String agent) {
        String n = region + "/" + agent;
        BundleEntry e = bundle.get(n);
        if (e == null) e = childControllers.get(n);                             // at a global: what its regions presented
        if (e == null) return null;
        if (e.keepUntil > 0 && e.keepUntil < System.currentTimeMillis()) return null;
        return e;
    }

    // ================================================================ revocations

    /**
     * A revocation came in force here (an operator's revocation_file reload, a revoked key, a merged revocation list): every
     * credential is verified anew, the sessions of a signer now refused are dropped, a trust-bundle entry whose certificate
     * the PKI or this node's revocations now refuse is dropped (a global's: re-verified; and the bundle travels down without
     * it). What arrives after is checked against the new state in any case ({@link #signerRefusal}, on every message).
     */
    public void onRevocations() {
        if (!required) return;
        long now = System.currentTimeMillis();
        credCache.invalidateAll();
        batchVerified.invalidateAll();
        int sessionsDropped = sessions == null ? 0 : sessions.dropWhere(p -> {
            Signer s = p.signer;
            return s == null || signerRefusal(s, now) != null;
        });
        int dropped = 0;
        PeerTrustRegistry reg = peers();
        for (Map.Entry<String, BundleEntry> en : childControllers.entrySet()) {
            BundleEntry e = en.getValue();
            String[] why = new String[1];
            boolean bad = e.cert == null || reg.verifiedIdentity(new X509Certificate[]{e.cert}, why) == null;
            if (bad && childControllers.remove(en.getKey(), e)) {
                dropped++;
                warn("trust bundle: DROPPED region controller " + en.getKey() + " (" + (why[0] == null ? "no certificate" : why[0]) + ")");
            }
        }
        for (Map.Entry<String, BundleEntry> en : bundle.entrySet()) {
            BundleEntry e = en.getValue();
            String why = reg.isRevokedFor(e.fp, e.r) ? "its key is revoked" : reg.nameRefusal(e.r, e.a);
            if (why == null) why = reg.refsRefusal(e.refs, e.r, now);
            if (why != null && bundle.remove(en.getKey(), e)) {
                dropped++;
                warn("trust bundle: DROPPED controller " + en.getKey() + " (" + why + ")");
            }
        }
        if (dropped > 0) bundleChanged();
        if (sessionsDropped > 0 || dropped > 0) {
            info("revocations in force: " + sessionsDropped + " peer session(s) and " + dropped + " trust-bundle entr"
                    + (dropped == 1 ? "y" : "ies") + " dropped; every credential is verified anew");
        }
    }

    // ================================================================ trust bundle and attestations on the pings

    /** On a child's ping: this node's certificate, what it holds of the bundle and (an agent) when its attestation expires. */
    public void onChildPing(MsgEvent ping) {
        if (!required) return;
        if (direct) ping.setParam(P_DIRECT_OK, "1");
        long wanted = trustWantedAt;
        if (wanted != 0L && System.currentTimeMillis() - wanted <= WANT_FOR_MS) ping.setParam(P_WANT, "1");
        try {
            ping.setParam(P_CERT, ownCert());
            ping.setParam(P_BUNDLE_HAVE, heldBundleDigest);
            if (!ce.cstate.isRegionalController()) {
                OwnAttestation att = currentAttestation();
                ping.setParam(P_ATT_EXP, Long.toString(att == null ? 0 : att.exp));
                if (att != null && att.issuerFp != null) ping.setParam(P_ATT_ISS, att.issuerFp);
            }
        } catch (Exception ignore) { }
    }

    /**
     * In a parent's ping handler (the ping becomes the pong): learn the child's key from the certificate it presents -- it
     * must verify HERE as the ping's verified caller -- then attest an agent child ({@code childIsRegion} false) or record a
     * region child in the bundle (true, at the global), and send the bundle down when the child holds another. O(1) apart
     * from the child's own certificate (one cached verification): the bundle is rebuilt only when it changed.
     */
    public void onPingAtParent(MsgEvent msg, boolean childIsRegion) {
        String certB64 = msg.getParam(P_CERT), have = msg.getParam(P_BUNDLE_HAVE), attExp = msg.getParam(P_ATT_EXP);
        String attIss = msg.getParam(P_ATT_ISS);
        boolean childDirect = "1".equals(msg.getParam(P_DIRECT_OK));
        // a child that cannot judge a message asks for trust data: what this node lacks, its own parent may hold
        if ("1".equals(msg.getParam(P_WANT)) && required && ce.cstate != null && !ce.cstate.isGlobalController()) wantTrust();
        msg.removeParam(P_WANT);
        msg.removeParam(P_CERT);
        msg.removeParam(P_BUNDLE_HAVE);
        msg.removeParam(P_ATT_EXP);
        msg.removeParam(P_ATT_ISS);
        msg.removeParam(P_DIRECT_OK);
        if (!required) return;
        try {
            Caller caller = Caller.fromMsgEvent(msg);
            if (direct) {
                // the child (its verified caller: a controller pinging as itself) accepts direct messages; so does this node
                if (childDirect && caller != null && caller.getKind() == Caller.Kind.CONTROLLER) {
                    String child = caller.getRegion() + "_" + caller.getAgent();
                    long now = System.currentTimeMillis();
                    if (directAgain(child, now) && (directChildren.size() < DIRECT_CHILDREN_MAX || directChildren.containsKey(child))) {
                        directChildren.put(child, now + DIRECT_OFFER_MS);
                    }
                }
                msg.setParam(P_DIRECT_OK, "1");
            }
            if (certB64 != null && caller != null && caller.getKind() == Caller.Kind.CONTROLLER) {
                X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                        .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(certB64)));
                CrescoIdentity id = peers().verifiedIdentity(new X509Certificate[]{leaf});
                boolean same = id != null && id.getTenant().equals(caller.getTenant()) && id.getRegion().equals(caller.getRegion())
                        && id.getAgent().equals(caller.getAgent());
                if (same && childIsRegion) {
                    // the role this very certificate carries (pre-provisioned), never one remembered for the name
                    String role = peers().boundRole(id.getRegion(), id.getAgent(), new X509Certificate[]{leaf});
                    if (PeerTrustRegistry.ROLE_CONTROLLER.equals(role) || ce.speaksForRegion(caller)) {
                        String n = id.getRegion() + "/" + id.getAgent();
                        BundleEntry prev = childControllers.get(n);
                        if (prev == null || prev.cert == null || !prev.cert.equals(leaf)) {
                            BundleEntry e = entry(id.getTenant(), id.getRegion(), id.getAgent(), leaf.getPublicKey(), "region",
                                    intern(peers().pathRefs(new X509Certificate[]{leaf})));
                            e.cert = leaf;
                            childControllers.put(n, e);
                            if (prev == null || !prev.fp.equals(e.fp) || !java.util.Objects.equals(prev.refs, e.refs)) bundleChanged();
                        }
                    }
                } else if (same && id.getRegion().equals(ce.cstate.getRegion())) {
                    long exp = 0;
                    try { exp = attExp == null ? 0 : Long.parseLong(attExp); } catch (NumberFormatException ignore) { }
                    // anew when it nears its end, and when the agent holds one signed by another key than this node's now
                    // (this controller came back re-keyed); an agent that names no key is judged by the end alone
                    ownCert();
                    if (exp < System.currentTimeMillis() + attestationMs / 3 || (attIss != null && !attIss.equals(ownKeyFp))) {
                        msg.setParam(P_ATT, attest(id, leaf.getPublicKey(), peers().pathRefs(new X509Certificate[]{leaf})));
                        msg.setParam(P_ATT_ISS, ownKeyFp);
                    }
                }
            }
            Served s = served();
            if (s != null && !s.digest.equals(have)) msg.setParam(P_BUNDLE, s.wire);
        } catch (Exception e) {
            refusal("ping of " + msg.getSrcRegion() + "/" + msg.getSrcAgent() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private String attest(CrescoIdentity id, PublicKey key, List<PeerTrustRegistry.CertRef> refs) throws Exception {
        Attestation a = new Attestation();
        a.t = id.getTenant();
        a.r = id.getRegion();
        a.a = id.getAgent();
        a.k = Base64.getEncoder().encodeToString(key.getEncoded());
        a.exp = System.currentTimeMillis() + attestationMs;
        a.iss = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
        a.refs = refs;
        a.sig = Base64.getEncoder().encodeToString(ce.getCertificateManager().signWithIdentity(attestationBytes(a)));
        return GSON.toJson(a);
    }

    /** On the parent's pong: keep an attestation of this node, and the parent's bundle when it verifies as the parent's. */
    public void onParentPong(MsgEvent pong, String parentRegion, String parentAgent) {
        if (!required) return;
        if (direct && "1".equals(pong.getParam(P_DIRECT_OK)) && parentRegion != null && parentAgent != null) {
            directParent = parentRegion + "_" + parentAgent;
            directParentUntil = System.currentTimeMillis() + DIRECT_OFFER_MS;
        }
        pong.removeParam(P_DIRECT_OK);
        String attIss = pong.getParam(P_ATT_ISS);
        pong.removeParam(P_ATT_ISS);
        try {
            String att = pong.getParam(P_ATT);
            if (att != null) {
                Attestation a = GSON.fromJson(att, Attestation.class);
                PublicKey own = ce.getCertificateManager().getPublicCertificate()[0].getPublicKey();
                if (a != null && a.v == 2 && a.k != null && a.iss != null && a.iss.equals(parentRegion + "/" + parentAgent)
                        && ce.cstate.getRegion().equals(a.r) && ce.cstate.getAgent().equals(a.a) && a.refs != null && !a.refs.isEmpty()
                        && a.k.equals(Base64.getEncoder().encodeToString(own.getEncoded())) && a.exp > System.currentTimeMillis()) {
                    ownAttestation = new OwnAttestation(att, a, attIss == null || attIss.isEmpty() ? null : attIss);
                }
            }
            String b = pong.getParam(P_BUNDLE) == null ? null : pong.getCompressedParam(P_BUNDLE);
            if (b != null && parentRegion != null && parentAgent != null) acceptBundle(b, parentRegion, parentAgent);
        } catch (Exception e) {
            refusal("pong of " + parentRegion + "/" + parentAgent + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        if (parentRegion != null && parentAgent != null) {
            String p = parentRegion + "/" + parentAgent;
            if (!p.equals(heardParent)) {
                heardParent = p;
                trustChanged();
            }
        }
    }

    /** Adopt a bundle signed by this node's parent (its certificate verifies here as the parent); unknown names are kept a while. */
    void acceptBundle(String json, String parentRegion, String parentAgent) throws Exception {
        SignedBundle sb = GSON.fromJson(json, SignedBundle.class);
        if (sb == null || sb.v != 2 || sb.entries == null || sb.cert == null || sb.sig == null || sb.issuer == null) return;
        if (!sb.issuer.equals(parentRegion + "/" + parentAgent)) return;
        X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(sb.cert)));
        CrescoIdentity id = peers().verifiedIdentity(new X509Certificate[]{leaf});
        if (id == null || !id.getRegion().equals(parentRegion) || !id.getAgent().equals(parentAgent)) {
            refusal("a trust bundle that does not verify as " + parentRegion + "/" + parentAgent);
            return;
        }
        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(leaf.getPublicKey().getAlgorithm()));
        s.initVerify(leaf.getPublicKey());
        s.update(bundleBytes(sb));
        if (!s.verify(Base64.getDecoder().decode(sb.sig))) {
            refusal("a trust bundle whose signature does not verify (" + parentRegion + "/" + parentAgent + ")");
            return;
        }
        long now = System.currentTimeMillis();
        long grace = Math.max(0L, ce.getPluginBuilder().getConfig().getLongParam("msg_signing_bundle_grace_ms", 900_000L));
        Map<String, BundleEntry> next = new LinkedHashMap<>();
        for (BundleEntry e : sb.entries) {
            if (e == null || e.refs == null || e.refs.isEmpty()) continue;
            boolean ok = true;
            for (PeerTrustRegistry.CertRef r : e.refs) ok &= r != null && r.wellFormed();
            if (!ok) continue;
            BundleEntry x = entry(e.t, e.r, e.a, decodeKey(e.k), e.role, intern(e.refs));
            if (x != null && x.fp.equals(e.refs.get(0).k)) next.put(x.r + "/" + x.a, x);
        }
        for (Map.Entry<String, BundleEntry> old : bundle.entrySet()) {
            if (next.containsKey(old.getKey())) continue;
            BundleEntry o = old.getValue();
            if (o.keepUntil == 0) o.keepUntil = now + grace;
            if (o.keepUntil > now) next.put(old.getKey(), o);
        }
        // in place, never emptied: a message checked meanwhile sees the old entry or the new one
        bundle.putAll(next);
        bundle.keySet().removeIf(k -> !next.containsKey(k));
        heldBundleDigest = digest(json);
        bundleChanged();
    }

    private static PublicKey decodeKey(String b64) throws Exception {
        return KeyFactory.getInstance(keyAlg(b64)).generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64)));
    }

    private static BundleEntry entry(String t, String r, String a, PublicKey key, String role, List<PeerTrustRegistry.CertRef> refs) {
        if (t == null || r == null || a == null || key == null) return null;
        BundleEntry e = new BundleEntry();
        e.t = t; e.r = r; e.a = a; e.role = role; e.refs = refs;
        e.k = Base64.getEncoder().encodeToString(key.getEncoded());
        e.key = key;
        e.fp = PeerTrustRegistry.fingerprint(key.getEncoded());
        return e;
    }

    private static byte[] bundleBytes(SignedBundle sb) {
        StringBuilder b = new StringBuilder("cresco-trust-bundle-v2\n").append(sb.issuer).append('\n').append(sb.tenant).append('\n').append(sb.issued);
        for (BundleEntry e : sb.entries) {
            b.append('\n').append(e.t).append('|').append(e.r).append('|').append(e.a).append('|').append(e.k).append('|').append(e.role)
                    .append('|').append(refsCanonical(e.refs).replace('\n', ';'));
        }
        return b.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String digest(String s) {
        return Base64.getEncoder().encodeToString(sha256.get().digest(s.getBytes(StandardCharsets.UTF_8)));
    }

    /** What this node serves its children changed: the next ping rebuilds it (once). */
    void bundleChanged() {
        bundleVersion.incrementAndGet();
        trustChanged();
    }

    /**
     * The bundle this node serves its children, signed by this node: its own entry, what its global sent it (a region), and
     * the region controllers that presented a verified certificate on their pings (a global). The current version is
     * returned with no lock and no work (a volatile read and two comparisons); it is rebuilt -- and re-signed only when its
     * content changed -- after a change ({@link #bundleChanged}), when an entry kept for its grace period ends, or when this
     * node's own certificate changed.
     */
    private Served served() throws Exception {
        long now = System.currentTimeMillis();
        X509Certificate own = ce.getCertificateManager().getPublicCertificate()[0];
        Served s = served;
        if (s != null && s.version == bundleVersion.get() && now < s.validUntil && s.own == own) return s;
        synchronized (servedLock) {
            s = served;
            long v = bundleVersion.get();
            if (s != null && s.version == v && now < s.validUntil && s.own == own) return s;
            TreeMap<String, BundleEntry> all = new TreeMap<>();
            long validUntil = Long.MAX_VALUE;
            for (BundleEntry e : bundle.values()) {
                if (e.keepUntil == 0 || e.keepUntil > now) {
                    all.put(e.r + "/" + e.a, e);
                    if (e.keepUntil > 0) validUntil = Math.min(validUntil, e.keepUntil);
                }
            }
            for (BundleEntry e : childControllers.values()) all.put(e.r + "/" + e.a, e);
            X509Certificate[] chain = ce.getCertificateManager().getPublicCertificate();
            BundleEntry self = entry(ce.tenant(), ce.cstate.getRegion(), ce.cstate.getAgent(), own.getPublicKey(),
                    ce.cstate.isGlobalController() ? "global" : "region", intern(peers().pathRefs(chain)));
            all.put(self.r + "/" + self.a, self);
            StringBuilder kb = new StringBuilder();
            for (BundleEntry e : all.values()) {
                kb.append(e.t).append('/').append(e.r).append('/').append(e.a).append('/').append(e.fp).append('/').append(e.role)
                        .append('/').append(refsCanonical(e.refs)).append(';');
            }
            String key = digest(kb.toString());
            Served next;
            if (s != null && key.equals(s.contentKey)) {
                next = new Served(v, validUntil, own, key, s.json, s.digest, s.wire);         // the same content: not re-signed
            } else {
                SignedBundle sb = new SignedBundle();
                sb.issuer = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
                sb.tenant = ce.tenant();
                sb.issued = now;
                sb.entries = new ArrayList<>(all.values());
                sb.cert = Base64.getEncoder().encodeToString(own.getEncoded());
                sb.sig = Base64.getEncoder().encodeToString(ce.getCertificateManager().signWithIdentity(bundleBytes(sb)));
                String json = GSON.toJson(sb);
                next = new Served(v, validUntil, own, key, json, digest(json), gzipBase64(json));
            }
            served = next;
            return next;
        }
    }

    /** As {@link MsgEvent#setCompressedParam} encodes a value (gzip, base64): made once per version, sent to every child. */
    private static String gzipBase64(String s) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        try (java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(bo)) {
            gz.write(s.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(bo.toByteArray());
    }

    /** The signed bundle this node serves (tests). */
    String servedBundle() throws Exception {
        Served s = served();
        return s == null ? null : s.json;
    }

    /** Trust-bundle size held (tests, diagnostics). */
    public int bundleSize() { return bundle.size() + childControllers.size(); }

    /** This node holds an attestation (tests, diagnostics). */
    public boolean hasAttestation() {
        try { ownCert(); } catch (Exception e) { return false; }
        return currentAttestation() != null;
    }
}
