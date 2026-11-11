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
 * certificate before -- and always for that controller itself: an attestation is its issuer's word to the nodes beyond it,
 * and the issuer knows its agent by the certificate it attested (a controller that came back with another key cannot
 * verify what its former key signed, and the ping that brings the new attestation must reach it). An agent that sees its
 * controller's present key (a message that controller signed, the trust bundle it signed) drops an attestation another key
 * made and asks to be attested anew ({@link #issuerKeySeen}).
 * A receiver resolves a credential, in this order: a certificate that verifies HERE as its name (a
 * peer bound here, or under a recorded region CA, or the operator's PKI); a certificate whose key is the one the <i>trust
 * bundle</i> lists for that controller; an attestation signed by a controller the trust bundle lists for the attested
 * region (or by this node). The trust bundle is the list of region and global controller keys (with the certificates
 * behind them, by reference): the global builds it from the certificates its regions present on their pings, signs it, and
 * sends it down on its pongs; each region re-signs what it verified for its agents. Credentials are verified once and
 * cached (bounded); on EVERY message the signer, the certificates behind it and an attestation's issuer are checked against
 * this node's revocations as they stand: a revoked key, a tombstoned name, and with a PKI the operator's
 * {@code revocation_file} (key, certificate, CRL serial, an out-of-date CRL) refuse it at once. A message costs one hash,
 * a few map lookups and one signature check (or one HMAC): O(1) in the size of the fleet. A name has ONE key: from the
 * moment this node binds another key to a name (an operator's rebind, a controller it follows) or learns one for it (a
 * trust bundle that lists the name with another key; at a global, the certificate a region presents), every credential
 * verified here under another key of that name -- its certificates, the attestations its former key issued -- is no
 * longer served, the sessions agreed with them are dropped and the trust-bundle entry of the former key goes
 * ({@link #keyChanged}): the cost is paid where the binding changes, O(what stands on that name), never per message.
 *
 * <p><b>What the receiver does.</b> With {@code broker_security_enabled}: an unsigned message, or one whose signature,
 * credential, recipient, age ({@code msg_signature_max_age_ms}, default 5 min, either way) or freshness (a signature seen
 * before) fails, is refused (an RPC request is answered 401). The signer must be the node of the signed caller (or, for an
 * API user, a node of the user's tenant -- any tenant's for a superuser tenant), and the broker's stamp, when there is one,
 * must be that caller: a bridge that stripped a stamp it could not vouch for no longer cuts the origin's word. A message
 * this node did not check on arrival (it was not addressed to it, or it carries it on as a relay) is checked when it is
 * dispatched here, whatever re-addressed it ({@link #verifyIfDeferred}). Every refusal names its reason ({@link Refusal}),
 * is counted under it ({@link #refusedCount(Refusal)}; {@code msg.refused.<reason>} in the metric inventory) and is logged
 * under it: a reason's first refusal at once, then at most one line per 10 s per reason.
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
 *
 * <p><b>A lost session.</b> A node that restarted holds none of the sessions its peers still send under. Until now each
 * such message was refused until the restarted node happened to send that peer something (its messages name its new
 * incarnation), or 30 s passed. Now the node that refuses says so at once ({@link #LOST}, see {@link #notHeld}): a
 * signed message that names the session, at most one a second per peer; an RPC request is answered {@code 401} as before
 * and the answer names the session. The sender drops that session on it and signs again; an RPC request refused so is
 * sent again once, signed, by the caller's own node ({@link #afterVerified}), and its caller never sees the refusal.
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
    /**
     * The lost-session notice, on a SIGNED message from a node to a peer: the id of a session that peer sent it a message
     * under and that it does not hold. Written by the signing node alone, as the message is signed ({@link #signIfPending}:
     * every stamping point removes whatever a sender wrote there), and read only under a verified signature -- never under
     * a session MAC, least of all the MAC of the session it speaks of. It rides the {@code 401} that answers an RPC request,
     * or a message of its own ({@value #LOST_ACTION}) when the refused message has no answer.
     */
    public static final String LOST = "cresco_sig_lost";
    /**
     * Beside {@link #LOST}, under the same signature: the signer NAME ({@code tenant/region/agent}) of the message that was
     * refused, as that message declared it. The receiver acts on the notice (drops the session, sends a request again)
     * only when that is its own name: a message this node never signed, refused under whatever name a relay wrote into
     * it, is not this node's business.
     */
    public static final String LOST_BY = "cresco_sig_lost_by";
    /**
     * Beside {@link #LOST}, under the same signature: for how many milliseconds, by its sender's own monotonic clock at the
     * moment it signed, the sender has NOT held that session ({@link MsgSessions#notHeldSince}; an ended session: since it
     * first refused it as ended; 0 when it cannot say). What a request is sent again on ({@link #afterVerified}).
     */
    public static final String LOST_FOR = "cresco_sig_lost_for";
    /**
     * The notice that is a message of its own: an EXEC {@value #LOST_ACTION} shaped as the reply to a call nobody made
     * (the receiver's controller call id {@value #LOST_CALL}). A node of this build takes it where it is verified
     * ({@link #afterVerified}). A node of an earlier build (1.3.378) verifies it like any signed message -- which is where
     * it learns the sender's new incarnation and returns to signatures -- and then hands it to its RPC table as the reply
     * to a call that no longer waits, which drops it without a line: it is never dispatched (an unknown action would be
     * DENIED there), never answered, never counted as a refusal.
     */
    static final String LOST_ACTION = "sessionlost", LOST_CALL = "cresco-sig-lost";
    /** A peer is told of a session this node does not hold at most this often; the node tells {@code msg_signing_lost_notices_per_second} peers a second at most. */
    static final long LOST_NOTICE_EVERY_MS = 1_000L;
    static final int LOST_NOTICE_PEERS_MAX = 4_096;
    /** Requests this node may send again ({@link #afterVerified}): how many, how much text in all, for how long each. */
    static final int LOST_RETRY_MAX = 4_096;
    static final long LOST_RETRY_CHARS_MAX = 8L << 20, LOST_RETRY_MS = 30_000L;
    /**
     * On a message whose caller is an API user, this node's own note of the SPEAKER: the node that vouched for the user
     * ({@code <tenant>/<region>/<agent>}) -- the node whose key signed the message, the node this node's broker attests
     * delivered it first hand, or this node itself for a user its own gateway sends for. Written only here (where a
     * message is verified, and at this node's own stamping point); never on the wire, never signed, and a sender's value
     * is removed on arrival like {@link #DEFERRED}. A controller's or plugin's speaker is its own node and needs no note.
     * Read by {@link DirectionAuthority} (authority follows the tree).
     */
    public static final String SPEAKER = "cresco_sig_speaker";

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

    // ---- lost sessions (LOST): everything here is bounded, and nothing is touched on a message that is neither refused
    // for its session nor an RPC request sent under one
    /** {@code msg_signing_lost_notices_per_second} (default 100): peers this node tells in a second at most; 0: it tells nobody. */
    private final int lostNoticesPerSecond;
    /**
     * When a peer (region_agent) was last sent a notice. An entry is written only for a notice that was sent and lives two
     * seconds: at most twice {@link #lostNoticesPerSecond} entries in practice, {@value #LOST_NOTICE_PEERS_MAX} at the most.
     */
    private final Cache<String, Long> lostNoticeAt = CacheBuilder.newBuilder().maximumSize(LOST_NOTICE_PEERS_MAX)
            .expireAfterWrite(Duration.ofMillis(2 * LOST_NOTICE_EVERY_MS)).build();
    private final Object lostNoticeLock = new Object();
    private long lostNoticeSecond;
    private int lostNoticesThisSecond;
    /**
     * What this node's signing layer itself decided about a message it is about to send, by the message's identity (weak:
     * a message that never reaches {@link #signIfPending} takes its entry with it): the notice it carries ({@link Word}),
     * or {@link #RESEND} for a request sent again, which is signed whatever session this node holds. Nothing a sender can
     * write into a message puts it here. An entry goes when the message is signed, when the message is collected, or
     * after a minute; {@link #ownMarked} counts the entries (0 nearly always: then a message leaving costs one counter read).
     */
    private final java.util.concurrent.atomic.AtomicInteger ownMarked = new java.util.concurrent.atomic.AtomicInteger();
    private final Cache<MsgEvent, Word> ownMarks = CacheBuilder.newBuilder().weakKeys().expireAfterWrite(Duration.ofMinutes(1))
            .removalListener((com.google.common.cache.RemovalNotification<MsgEvent, Word> gone) -> ownMarked.decrementAndGet()).build();

    /**
     * A notice this node is about to sign: the session it does not hold, the signer name of the message it refused for
     * it, and since when it has not held it ({@link System#nanoTime}; {@code known} false: it cannot say, and states 0).
     */
    private static final class Word {
        final String sid, refused;
        final long sinceNanos;
        final boolean known;
        Word(String sid, String refused, long sinceNanos, boolean known) {
            this.sid = sid; this.refused = refused; this.sinceNanos = sinceNanos; this.known = known;
        }
    }

    private static final Word RESEND = new Word(null, null, 0L, false);
    /** When this signing layer was made ({@link System#nanoTime}): a node without session keys has held none since. */
    private final long startedNanos = System.nanoTime();

    /**
     * RPC requests this node sent under a session, by call id, until their reply arrives ({@link #afterVerified}) or
     * {@value #LOST_RETRY_MS} ms pass: at most {@value #LOST_RETRY_MAX} of them and {@value #LOST_RETRY_CHARS_MAX}
     * characters of parameters in all (the least recently sent goes first: such a call is simply not sent again). Null:
     * {@code msg_signing_lost_retry=false}.
     */
    private final Cache<String, Retry> lostRetries;
    private final AtomicLong lostTold = new AtomicLong(), lostWithheld = new AtomicLong(), lostHeard = new AtomicLong(),
            lostIgnored = new AtomicLong(), lostRetried = new AtomicLong(), lostNotRetried = new AtomicLong();
    /** Tests: a request sent again is handed here instead of to the producer (true: sent). */
    volatile java.util.function.Predicate<MsgEvent> resendTap;

    /**
     * An RPC request sent under the session {@code sid} with {@code peerNode}. Nothing is copied when it is sent: the
     * record holds the caller's own message (which the caller holds until its call returns), when it left by this node's
     * monotonic clock, and the MAC it left with -- by which, with the session's key, this node sees at the moment of a
     * retry that the message is still, parameter for parameter, the one it sent.
     */
    private static final class Retry {
        final String peerNode, sid, mac;
        final MsgSessions.Session session;
        final MsgEvent sent;
        final long sentNanos;
        final int weight;
        Retry(String peerNode, MsgSessions.Session session, String mac, MsgEvent sent, long sentNanos, int weight) {
            this.peerNode = peerNode; this.session = session; this.sid = session.sid; this.mac = mac; this.sent = sent;
            this.sentNanos = sentNanos; this.weight = weight;
        }
    }

    /**
     * Times this node told a sender, as the message left signed (a notice of its own, or the 401 of a request); notices not
     * sent for the rate limits; sessions dropped on a peer's word; such words that named no session held; requests sent again.
     */
    public long lostToldCount() { return lostTold.get(); }
    public long lostWithheldCount() { return lostWithheld.get(); }
    public long lostHeardCount() { return lostHeard.get(); }
    public long lostIgnoredCount() { return lostIgnored.get(); }
    public long lostRetriedCount() { return lostRetried.get(); }
    /** Refusals that named a call of this node's and were NOT answered by sending it again: it may have run (its caller sees the 401). */
    public long lostNotRetriedCount() { return lostNotRetried.get(); }
    /** Requests held to be sent again (diagnostics, tests). */
    long lostRetriesHeld() { if (lostRetries == null) return 0; lostRetries.cleanUp(); return lostRetries.size(); }
    /** Peers whose last notice is remembered (tests). */
    long lostNoticePeers() { lostNoticeAt.cleanUp(); return lostNoticeAt.size(); }
    /** The session this node sends to {@code peerNode} under now, or null (tests). */
    MsgSessions.Session sessionWith(String peerNode) { return sessions == null ? null : sessions.usable(peerNode, System.currentTimeMillis()); }
    /** As when this node's clock passes the session's end (tests). */
    void endSession(MsgSessions.Session s) { sessions.ended(s); }
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
    /**
     * At a global: region -> its controller ({@code region/agent}) in {@link #childControllers}. Kept with that map, so
     * an entry goes only when the controller's certificate stops verifying here ({@link #onRevocations}), not when the
     * region leaves: a region that left this global and was not revoked stays BELOW it (authority follows the tree). That
     * is no wider than before directions existed -- BELOW lets its nodes report (UP actions), which RegistryBinding
     * confines to their own records, and nothing more -- and it is listed in the limits of docs/caller-authorization.md
     * section 3a. Do not read it as "joined now".
     */
    private final ConcurrentHashMap<String, String> childRegions = new ConcurrentHashMap<>();
    /** The global(s) this node's present trust bundle names, with the parent that signed it; null: none. */
    private volatile GlobalAbove globalAbove;

    /**
     * Who the trust bundle this node holds says is the global above it: the entries of kind {@code global} in the bundle
     * as its parent last signed it (never an entry only kept for its grace period), with that parent's name.
     */
    static final class GlobalAbove {
        final String parentRegion, parentAgent;
        /** region, agent, region, agent, ... (one global, unless a parent ever names more). */
        final String[] names;
        GlobalAbove(String parentRegion, String parentAgent, String[] names) {
            this.parentRegion = parentRegion; this.parentAgent = parentAgent; this.names = names;
        }
    }

    /**
     * Is {@code region/agent} the global above this node, on the word of the parent {@code parentRegion/parentAgent} it
     * has now: named with kind {@code global} in the trust bundle that parent signed? False for a bundle another (earlier)
     * parent signed, and for a global that parent only keeps for its grace period (it serves that one as
     * {@value #ROLE_FORMER_GLOBAL}). O(1), no lock, nothing allocated.
     */
    public boolean isGlobalAbove(String region, String agent, String parentRegion, String parentAgent) {
        GlobalAbove g = globalAbove;
        if (g == null || region == null || agent == null || !g.parentRegion.equals(parentRegion) || !g.parentAgent.equals(parentAgent)) return false;
        for (int i = 0; i + 1 < g.names.length; i += 2) {
            if (g.names[i].equals(region) && g.names[i + 1].equals(agent)) return true;
        }
        return false;
    }

    /**
     * At a global: did the controller of {@code region} present itself here on its pings, with a certificate that verifies
     * as a controller that joined this global ({@link #onPingAtParent})? O(1), nothing allocated.
     */
    public boolean isChildRegion(String region) {
        return region != null && childRegions.containsKey(region);
    }
    /** Bumped on every change of what this node serves its children: a ping costs O(1) and takes no lock. */
    private final AtomicLong bundleVersion = new AtomicLong(1);
    private final Object servedLock = new Object();
    private volatile Served served;

    // ---- verification state (bounded)
    /** Credentials verified here: digest -> signer. Bounded LRU ({@code msg_signing_credentials_max}); never cleared wholesale but on a revocation. */
    private final Cache<String, Signer> credCache;
    private final ReplayMemory replay;
    /** {@code msg_replay_floor} (default true): what this node accepted before a restart is refused after it ({@link ReplayFloor}). */
    private final boolean replayFloorOn;
    private volatile ReplayFloor replayFloor;
    private volatile boolean replayFloorFailed;

    /** The durable floor, opened at the first signed message this node judges (its certificate manager is there by then); or null. */
    private ReplayFloor floor() {
        ReplayFloor f = replayFloor;
        if (f != null || !replayFloorOn || replayFloorFailed) return f;
        synchronized (this) {
            f = replayFloor;
            if (f == null && !replayFloorFailed) {
                try {
                    // what it restored at start is news, a storage error is a warning
                    f = new ReplayFloor(ce.getCertificateManager().identityDirectory().resolve("replay-floor.bin"), maxAgeMs, maxFutureMs,
                            line -> { if (line.startsWith("REPLAY FLOOR")) warn(line); else info(line); });
                    replayFloor = f;
                } catch (RuntimeException e) {
                    replayFloorFailed = true;
                    warn("REPLAY FLOOR could not be opened (" + e + "): a signed message this node accepted shortly before a restart"
                            + " could be accepted again after it (as before this record existed)");
                }
            }
            return f;
        }
    }

    /** Signed messages refused because they may have been accepted before this node restarted (diagnostics, tests). */
    long replayFloorRefused() { ReplayFloor f = replayFloor; return f == null ? 0 : f.refused(); }
    private final AtomicLong signedCount = new AtomicLong(), verifiedCount = new AtomicLong();
    /** Refusals by reason, and when each reason's line is due ({@link Refusal}). */
    private final KindTally refusals = new KindTally(REFUSALS.length, REFUSAL_LINE_EVERY_MS);
    /** Tests: every refusal line this node logs is handed here as well. */
    volatile java.util.function.Consumer<String> refusalLineTap;
    /** CA certificate references, shared by every signer that stands on them (a region's agents share their CAs'). */
    private final Cache<PeerTrustRegistry.CertRef, PeerTrustRegistry.CertRef> refInterner =
            CacheBuilder.newBuilder().maximumSize(10_000).build();

    private final ThreadLocal<MessageDigest> sha256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new IllegalStateException(e); }
    });

    /** After its first occurrence, one reason of refusals is logged at most this often. */
    static final long REFUSAL_LINE_EVERY_MS = 10_000L;

    /**
     * Why a message is refused: one reason per thing a person would look at. Every refusal names its reason, is counted
     * under it, and is logged under it: the reason's first refusal at once, then at most one line per
     * {@value #REFUSAL_LINE_EVERY_MS} ms per reason with that reason's count -- a reason that refuses a hundred messages a
     * second never hides one that refused a single message.
     */
    public enum Refusal {
        /** No signature and no MAC, and not accepted as delivered first hand over a direct link (or in an upgrade window). */
        UNSIGNED("unsigned"),
        /** Signed for another node, another plugin of this node, or a global this node is not. */
        NOT_FOR_THIS_NODE("not_for_this_node"),
        /** Signed, and names no recipient at all. */
        NO_RECIPIENT("no_recipient"),
        /** Signed longer ago than msg_signature_max_age_ms: it waited that long on the way, or the clocks differ. */
        TOO_OLD("too_old"),
        /** Signed further ahead of this node's clock than msg_signature_max_future_ms (or this node's clock went back). */
        AHEAD_OF_CLOCK("ahead_of_clock"),
        /** A signature or MAC this node accepted before. */
        REPLAY("replay"),
        /** Fresh as far as this node can tell, while the replay memory (its signer's, or the node's) has no room to remember it. */
        REPLAY_MEMORY_FULL("replay_memory_full"),
        /** The signer's credential does not verify here (or not as the signer the message declares). */
        CREDENTIAL("credential"),
        /** The signer verifies, and is refused now: revoked, tombstoned, its credential ended or a certificate behind it not valid. */
        REVOKED("revoked"),
        /** Under a session key this node does not hold (it restarted), that ended, or whose peer no longer verifies. */
        SESSION_KEY("session_key"),
        /** The signature, the batch signature or the session MAC does not verify over the message as it arrived. */
        BAD_SIGNATURE("bad_signature"),
        /** The signer may not speak for the signed caller, or the broker's caller is another than the signed one. */
        CALLER_MISMATCH("caller_mismatch"),
        /** None of the above: a signature that cannot be read (malformed, or a check failed with an error). */
        OTHER("other"),
        /**
         * Sender side: this node could not sign a message it originates (no destination to sign for, or the signing
         * failed). The message leaves unsigned and is refused where it arrives, counted there: not in this node's total.
         */
        CANNOT_SIGN("cannot_sign", "msg.sign.failed"),
        /**
         * Trust data this node did not take: a trust bundle that does not verify as its parent's, the signing data on a
         * ping or a pong that could not be read. No message is refused by it (not in the node's total); the messages it
         * would have let this node judge are, under {@link #CREDENTIAL}.
         */
        TRUST_DATA("trust_data", "msg.trust.refused");

        private final String key, metric;
        private final boolean message;

        Refusal(String key) { this.key = key; this.metric = "msg.refused." + key; this.message = true; }

        Refusal(String key, String metric) { this.key = key; this.metric = metric; this.message = false; }

        /** The reason's stable name: in the log line, and the last part of its metric's name. */
        public String key() { return key; }

        /** The reason's meter in the metric inventory. */
        public String metric() { return metric; }

        /** A message this node received and refused (counted in {@link MsgSigning#refusedCount()}). */
        public boolean ofMessage() { return message; }
    }

    private static final Refusal[] REFUSALS = Refusal.values();

    /**
     * The kinds of a bundle entry. {@value #ROLE_FORMER_GLOBAL}: a global this region no longer has above it, whose key
     * it still serves for the entry's grace period (messages on their way still verify) -- never as {@code global}, which
     * is what an agent takes for the global above it ({@link #isGlobalAbove}).
     */
    static final String ROLE_GLOBAL = "global", ROLE_REGION = "region", ROLE_FORMER_GLOBAL = "former-global";

    static final class BundleEntry {
        String t, r, a, k, role;            // tenant, region, agent, public key (X.509 SPKI, base64), global|region|former-global
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
        final String json, issuer, region, agent, key;
        /** The issuer as a recipient names it ({@code region_agent}): a message for that node is signed with the certificate. */
        final String issuerNode;
        /**
         * The fingerprint of the key that signed it: as the issuer named it on its pong, or null from a parent that does not
         * (an earlier build) until this node has checked the signature against that parent's key ({@link #issuerKeySeen}).
         */
        volatile String issuerFp;
        final Attestation signed;
        final long exp;
        OwnAttestation(String json, Attestation a, String issuerFp) {
            this.json = json; this.issuer = a.iss; this.region = a.r; this.agent = a.a; this.key = a.k; this.exp = a.exp;
            this.issuerNode = a.iss.replace('/', '_');
            this.issuerFp = issuerFp;
            this.signed = a;
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
        /**
         * Set once, before the signer is cached ({@link #standsOn}): the cache key of its credential, and the names it
         * stands on -- its own and (an attestation) its issuer's -- held here so that they live as long as it does.
         */
        String cacheKey;
        Standing ownStanding, issuerStanding;
        /** A name this signer stands on got another key here ({@link #keyChanged}): it is no longer served. */
        volatile boolean superseded;

        Signer(String tenant, String region, String agent, PublicKey key, String fp, List<PeerTrustRegistry.CertRef> refs,
               String issuer, String issuerFp, List<PeerTrustRegistry.CertRef> issuerRefs, long expires, boolean self) {
            this.tenant = tenant; this.region = region; this.agent = agent; this.name = tenant + "/" + region + "/" + agent;
            this.key = key; this.fp = fp; this.refs = refs; this.issuer = issuer; this.issuerFp = issuerFp;
            this.issuerRefs = issuerRefs; this.expires = expires; this.self = self;
        }

        String node() { return region + "_" + agent; }
    }

    /**
     * The signers verified here that stand on one name ({@code region/agent}): those of that name, and those whose
     * attestation that name issued. By identity and weakly: a signer is listed for as long as anything holds it (the
     * credential cache, a session, a message being verified), so whatever could still authenticate a message on that
     * name's former key is found when the name gets another one -- and nothing is kept for a signer that is gone.
     */
    static final class Standing {
        private final java.util.Set<Signer> live = java.util.Collections.newSetFromMap(new java.util.WeakHashMap<>(2));
    }

    /** name -> what stands on it; an entry lives as long as one of its signers (each holds it). */
    private final Cache<String, Standing> standings = CacheBuilder.newBuilder().weakValues().build();
    /** Bumped whenever a name gets another key here: a credential that was being verified meanwhile is verified again. */
    private final AtomicLong trustEpoch = new AtomicLong();
    private final AtomicLong formerKeyCredentials = new AtomicLong(), formerKeySessions = new AtomicLong();

    /** Credentials no longer served, and sessions dropped, because their name got another key (diagnostics, tests). */
    long formerKeyCredentialsDropped() { return formerKeyCredentials.get(); }
    long formerKeySessionsDropped() { return formerKeySessions.get(); }

    private Standing standing(String name) {
        try {
            return standings.get(name, Standing::new);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /** List {@code s}, verified a moment ago, under the names it stands on (before it is cached: once per verification). */
    private void standsOn(Signer s, String cacheKey) {
        s.cacheKey = cacheKey;
        Standing own = standing(s.region + "/" + s.agent);
        s.ownStanding = own;
        synchronized (own) { own.live.add(s); }
        if (s.issuer != null) {
            Standing iss = standing(s.issuer);
            s.issuerStanding = iss;
            synchronized (iss) { iss.live.add(s); }
        }
    }

    /**
     * {@code region/agent} has the key {@code presentFp} here from now on: this node bound it (an operator's rebind, a
     * controller it follows that re-keyed, its own identity replaced: the registry says so,
     * {@link CertificateManager#onKeyReplaced}), its parent's trust bundle lists the name with another key than the bundle
     * before ({@link #acceptBundle}), or -- at a global -- the region controller presented another certificate on its ping
     * ({@link #onPingAtParent}). What its FORMER key could still do here ends now:
     * <ul>
     * <li>the trust-bundle entry this node holds (and serves its children) for the name under another key goes: a
     *     certificate or an attestation of the former key no longer verifies on the bundle's word;</li>
     * <li>every credential verified here on another key of the name -- its certificate, its own attestation, and every
     *     attestation the former key ISSUED (a region's agents) -- leaves the credential cache;</li>
     * <li>the sessions agreed with those signers are dropped (the name's own, and those of the agents its former key
     *     attested): a MAC under them is refused as a session this node does not hold.</li>
     * </ul>
     * O(the signers standing on that name), nothing for any other name, nothing added to a message's checks. The replay
     * memory needs nothing (it only ever refuses), nor do the verified batch roots (reached only through a credential that
     * resolved). What is verified from here on meets the trust data as it stands.
     */
    void keyChanged(String region, String agent, String presentFp) {
        if (!required || region == null || agent == null || presentFp == null) return;
        String name = region + "/" + agent;
        boolean served = false;
        BundleEntry e = childControllers.get(name);
        if (e != null && !presentFp.equals(e.fp) && childControllers.remove(name, e)) {
            childRegions.remove(e.r, name);
            served = true;
        }
        e = bundle.get(name);
        if (e != null && !presentFp.equals(e.fp) && bundle.remove(name, e)) {
            served = true;
            GlobalAbove g = globalAbove;
            if (g != null && ROLE_GLOBAL.equals(e.role) && isGlobalAbove(e.r, e.a, g.parentRegion, g.parentAgent)) globalAbove = null;
        }
        // after the trust data changed, before the signers are looked at: a credential verified across this moment is
        // verified again (resolve), one listed before it is found below
        trustEpoch.incrementAndGet();
        int creds = 0, sess = 0;
        Standing st = standings.getIfPresent(name);
        if (st != null) {
            List<Signer> former = new ArrayList<>();
            synchronized (st) {
                for (Signer s : st.live) {
                    if (s == null || s.superseded) continue;
                    boolean own = s.region.equals(region) && s.agent.equals(agent) && !presentFp.equals(s.fp);
                    if (own || (name.equals(s.issuer) && !presentFp.equals(s.issuerFp))) former.add(s);
                }
            }
            for (Signer s : former) {
                s.superseded = true;                     // before its session is looked at (verify looks after it set one)
                if (s.cacheKey != null && credCache.asMap().remove(s.cacheKey, s)) creds++;
                if (sessions != null && sessions.dropIfSigner(s.node(), s)) sess++;
            }
        }
        formerKeyCredentials.addAndGet(creds);
        formerKeySessions.addAndGet(sess);
        if (served) bundleChanged();
        if (served || creds > 0 || sess > 0) {
            info("ANOTHER KEY for " + name + " (" + PeerIdentity.shortFp(presentFp) + "): what its former key could still do here ends -- "
                    + creds + " cached credential(s) no longer served, " + sess + " session(s) dropped"
                    + (served ? ", its trust-bundle entry under the former key removed" : ""));
        }
    }

    /** The signer cached for {@code cred} ({@code cert:...} / {@code att:...}), or null (tests). */
    Signer cachedSigner(String cred) {
        return credCache.getIfPresent(Base64.getEncoder().encodeToString(sha256.get().digest(cred.getBytes(StandardCharsets.UTF_8))));
    }

    public MsgSigning(ControllerEngine ce) {
        this.ce = ce;
        io.cresco.library.plugin.Config c = ce.getPluginBuilder().getConfig();
        this.required = SecuritySettings.msgSigningRequired(c);
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
        this.attestWait = new TrustWait(Math.max(0L, Math.min(60_000L, c.getLongParam("msg_signing_hold_ms", 5_000L))));
        this.lostNoticesPerSecond = (int) Math.max(0L, Math.min(10_000L, c.getLongParam("msg_signing_lost_notices_per_second", 100L)));
        this.lostRetries = sessionsOn && c.getBooleanParam("msg_signing_lost_retry", true)
                ? CacheBuilder.newBuilder().maximumWeight(LOST_RETRY_CHARS_MAX).weigher((String k, Retry r) -> r.weight)
                        .expireAfterWrite(Duration.ofMillis(LOST_RETRY_MS)).build()
                : null;
        this.watchCalls = sessionsOn && c.getBooleanParam("msg_signing_unanswered_calls", true);
        if (watchCalls) sessions.onTimedOut = peer -> setAside(peer, "a call to it was not answered before its caller stopped waiting");
        String caps = batch ? "batch" : "";
        if (sessionsOn) caps = caps.isEmpty() ? "session" : caps + ",session";
        this.ownCaps = caps.isEmpty() ? null : caps;
        this.credCache = CacheBuilder.newBuilder()
                .maximumSize(Math.max(1L, c.getLongParam("msg_signing_credentials_max", 100_000L)))
                .expireAfterWrite(Duration.ofHours(1)).build();
        this.replay = new ReplayMemory(maxAgeMs, maxFutureMs, Math.max(4096L, c.getLongParam("msg_replay_memory_max", 4_000_000L)), this::warn);
        this.replayFloorOn = required && c.getBooleanParam("msg_replay_floor", true);
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
        ReplayFloor f = replayFloor;
        if (f != null) f.close();
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

    /** Messages this node received and refused so far, whatever the reason. */
    public long refusedCount() {
        long n = 0;
        for (Refusal r : REFUSALS) if (r.message) n += refusals.of(r.ordinal());
        return n;
    }

    /** Refused (or, for a reason that is not a message's, noted) for {@code reason} so far. */
    public long refusedCount(Refusal reason) { return refusals.of(reason.ordinal()); }

    // ================================================================ canonical form

    /** Is {@code key} covered by the signature? Not what infrastructure writes in transit, nor the signature itself. */
    static boolean covered(String key) {
        return key != null && !key.startsWith("routepath-") && !key.startsWith("linkcost-") && !key.startsWith("srcroute")
                && !key.equals("ttl") && !key.equals(SIG) && !key.equals(PENDING) && !key.equals(MsgBatchSigner.TREE)
                && !key.equals(MsgSessions.MAC) && !key.equals(DEFERRED) && !key.equals(DIRECT) && !key.equals(SPEAKER)
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
            MsgSessions.SID, MsgSessions.MAC, MsgSessions.KX, MsgSessions.KX_ACK, MsgSessions.HELD, MsgSessions.INC, LOST,
            LOST_BY, LOST_FOR};

    /** Remove every signature parameter. */
    public static void clear(MsgEvent m) {
        if (m == null || m.getParams() == null) return;
        for (String k : AUTH_PARAMS) m.removeParam(k);
        m.removeParam(PENDING);
        m.removeParam(DIRECT);
        m.removeParam(DEFERRED);
        m.removeParam(SPEAKER);                  // whoever hands this node a message never says whose word it is
    }

    /** A message off the wire never asks THIS node to sign it: only this node's own stamping points do. */
    public static void notPending(MsgEvent m) {
        if (m != null && m.getParams() != null) {
            m.removeParam(PENDING);
            m.removeParam(DIRECT);               // a sender's word that it is the origin counts only as its broker attests it
            m.removeParam(DEFERRED);             // what this node checked is its own record, never a sender's
            m.removeParam(SPEAKER);              // nor whose word the caller is: this node notes it when it has verified it
        }
    }

    /**
     * Note on {@code m}, which this node has just stamped with an API user's caller (its own gateway sends for the user),
     * that this node is the speaker ({@link #SPEAKER}). Call after {@link #markForSigning} (which removes any note).
     */
    public void spokenHere(MsgEvent m) {
        if (m != null && m.getParams() != null && ce.cstate != null) m.setParam(SPEAKER, selfName());
    }

    /** The speaker noted on {@code m} ({@link #SPEAKER}): {@code <tenant>/<region>/<agent>}, or null. */
    public static String speakerOf(MsgEvent m) {
        return m == null || m.getParams() == null ? null : m.getParam(SPEAKER);
    }

    /** As a message this node originated leaves it (AgentProducer): sign it. Relayed messages carry their origin's signature. */
    public void signIfPending(MsgEvent m) {
        if (m == null || m.getParams() == null) return;
        m.removeParam(DEFERRED);                 // this node's own notes: never on the wire
        m.removeParam(SPEAKER);
        // what this node's own signing layer decided about this very message (one counter read when it decided nothing):
        // the session a notice names, or "" for a request sent again
        Word own = ownMarked.get() == 0 ? null : takeMark(m);
        if (m.getParam(PENDING) == null) return;
        m.removeParam(PENDING);
        if (!required) return;
        // a notice is this node's signing layer's word and nobody else's: whatever is in that parameter of a message this
        // node originates (every stamping point removed it already) goes, and the notice is written below
        m.removeParam(LOST);
        m.removeParam(LOST_BY);
        m.removeParam(LOST_FOR);
        Caller c = Caller.fromMsgEvent(m);
        if (c == null || c.isAnonymous()) return;
        String to = recipient(m);
        if (to == null) {
            refusal(Refusal.CANNOT_SIGN, null, "a message this node sends names no destination (" + m.getMsgType() + " " + m.getParam("action") + "): it goes unsigned");
            return;
        }
        // SIGNED, whatever else could authenticate it: a notice must verify under this node's bound key (its receiver
        // reads it under a signature only), and a request sent again must not meet the refusal it is the answer to. Neither
        // ever takes the direct link's unsigned path or a session's MAC; when the signature cannot be made, the message
        // leaves with nothing and is refused where it arrives (below) -- "no session" never becomes "unauthenticated".
        boolean signedOnly = own != null;
        if (!signedOnly && directTo(m, to)) {
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
                    MsgSessions.Session ss = signedOnly ? null : sessions.usable(node, now);
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
                        if ((lostRetries != null || watchCalls) && m.getParam("is_rpc") != null) {
                            // its own try: a failure of this bookkeeping is no failure of the session keys (the catch
                            // below would turn them off for the whole node); the request simply is not sent again
                            try {
                                sentUnderSession(m, node, ss);
                            } catch (RuntimeException notKept) {
                                // nothing to do: its caller sees a refusal, as before
                            }
                        }
                        return;
                    }
                } catch (Exception | Error e) {
                    // a provider may refuse (a FIPS module raises Errors): this node stays with signatures
                    if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
                    sessionsFailed(e);
                    for (String k : new String[]{MsgSessions.KX, MsgSessions.KX_ACK, MsgSessions.SID, MsgSessions.MAC}) m.removeParam(k);
                }
            }
            if (own != null && own != RESEND) {
                // under the signature made below: the session, whose message was refused for it, and for how long this
                // node has not held it AS OF THIS INSTANT, by its monotonic clock (what its receiver measures a request's
                // age against: the later this is signed, the longer -- and it stays true, a session is never held again)
                m.setParam(LOST, own.sid);
                m.setParam(LOST_BY, own.refused);
                m.setParam(LOST_FOR, own.known ? Long.toString(Math.max(0L, (System.nanoTime() - own.sinceNanos) / 1_000_000L)) : "0");
                if (lostTold.incrementAndGet() == 1) info("lost sessions: this node tells the sender of a message under a session it"
                        + " does not hold, on a signed message -- in the 401 that answers a request, else in a notice of its own, at"
                        + " most once a second per peer and " + lostNoticesPerSecond + " times a second in all (first: to " + to + ")");
            }
            m.setParam(CRED, credential(node));
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
            refusal(Refusal.CANNOT_SIGN, null, "cannot sign a message this node sends (" + e.getClass().getSimpleName() + ": " + e.getMessage() + "): it goes unsigned and is refused");
        }
    }

    /** This node's signing layer says of {@code m}, which it sends now: it carries the notice {@code what}; {@link #RESEND}: sign it. */
    private void mark(MsgEvent m, Word what) {
        ownMarked.incrementAndGet();             // before the entry exists: its removal (whatever the cause) takes it back
        ownMarks.put(m, what);
    }

    private Word takeMark(MsgEvent m) {
        return ownMarks.asMap().remove(m);
    }

    /**
     * {@code m}, an RPC request, leaves under the session {@code ss} with {@code node}: remember it, so that it can be
     * sent again, signed, should that node answer that it has not held the session since before this moment
     * ({@link #afterVerified}). Nothing is copied here (see {@link Retry}); a reply or a one-way message is never sent
     * again. Bounded ({@link #lostRetries}); a request with files, or one that would take more than an eighth of the
     * room, is not kept: its caller sees the refusal, as before.
     */
    private void sentUnderSession(MsgEvent m, String node, MsgSessions.Session ss) {
        if (!MsgRouter.isRpcRequest(m)) return;
        String callId = m.getParam("callId-" + m.getSrcRegion() + "-" + m.getSrcAgent() + "-" + m.getSrcPlugin());
        // the call is awaited for as long as its caller says it waits: unanswered, the session is set aside (unanswered calls)
        if (watchCalls) sessions.called(node, ss, callId, RpcExpiry.statedTimeoutMs(m));
        if (lostRetries == null || m.hasFiles() || m.getParam(MsgRouter.SRCROUTE) != null) return;
        long chars = 0;
        for (Map.Entry<String, String> e : m.getParams().entrySet()) chars += e.getKey().length() + (e.getValue() == null ? 0 : e.getValue().length());
        if (chars > LOST_RETRY_CHARS_MAX / 8) return;
        // each entry weighs at least a 4096th of the room: at most LOST_RETRY_MAX of them
        lostRetries.put(callId, new Retry(node, ss, m.getParam(MsgSessions.MAC), m, System.nanoTime(),
                (int) Math.max(chars, LOST_RETRY_CHARS_MAX / LOST_RETRY_MAX)));
    }

    /**
     * The request of {@code r} as it is sent again: what its caller's message holds now, without what authenticated it,
     * marked to be signed -- or null when that message is no longer, parameter for parameter, the one this node sent
     * (its MAC under the session's key no longer matches: the caller has written into it since), in which case nothing
     * is sent again.
     */
    private MsgEvent again(Retry r) throws Exception {
        MsgEvent m = r.sent;
        java.util.HashMap<String, String> params = new java.util.HashMap<>(m.getParams());
        MsgEvent copy = new MsgEvent(m.getMsgType(), m.getSrcRegion(), m.getSrcAgent(), m.getSrcPlugin(), m.getDstRegion(),
                m.getDstAgent(), m.getDstPlugin(), m.isRegional(), m.isGlobal());
        copy.setParams(params);
        String by = params.get(BY);
        if (by == null || !r.mac.equals(params.get(MsgSessions.MAC)) || !MessageDigest.isEqual(Base64.getDecoder().decode(r.mac),
                MsgSessions.mac(r.session.key, by, canonical(copy)))) {
            return null;
        }
        for (String k : AUTH_PARAMS) params.remove(k);
        params.put(PENDING, "1");
        return copy;
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

    /** The credential this node signs with for a node beyond its parent ({@link #credential(String)}). */
    String credential() throws Exception { return credential(null); }

    /**
     * The credential on a message for {@code recipientNode} ({@code region_agent}; null: a global function its region
     * carries on): an agent's attestation while it is its own and valid (a tenth of its life left at least), else this
     * node's certificate -- and always the certificate for the node that issued the attestation. That node verifies its
     * agent's certificate itself (it did, to attest it), whatever became of the key it attested with: signed with the
     * attestation, an agent's ping to a controller that came back with another key was refused there ("the attestation's
     * signature does not verify"), and with the ping the only way to a new attestation.
     */
    String credential(String recipientNode) throws Exception {
        String cert = ownCert();
        OwnAttestation att = currentAttestation();
        if (att != null && !att.issuerNode.equals(recipientNode) && System.currentTimeMillis() < att.exp - attestationMs / 10) {
            return "att:" + att.json;
        }
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

    /**
     * The first moments of a join, as a sender sees them: an agent holds no attestation of its present controller until
     * that controller's first pong, and signs with its certificate until then -- which its own controller verifies, and a
     * node beyond it does not (it knows an agent of another region by its attestation alone). A reply made in those
     * moments was refused where it arrived ({@code the signer's credential does not verify here}); it went unnoticed only
     * because the node had no producer yet and dropped it itself. A message such an attestation would sign waits for it
     * ({@link #awaitsAttestation}), for {@code msg_signing_hold_ms} at most per stretch without one: a controller that
     * never attests does not silence its agent.
     */
    private final TrustWait attestWait;
    /**
     * Is {@code m} a message this node is about to originate that should wait for this node's attestation
     * ({@link #attestationPending} and {@link #needsAttestation})?
     */
    public boolean awaitsAttestation(MsgEvent m) {
        return attestationPending() && needsAttestation(m);
    }

    /**
     * Is this node an AGENT that holds no attestation it could sign with now, and still within the wait of this stretch?
     * False on a controller (it signs with its certificate, which the trust bundle carries) and on an agent with its
     * attestation: the node as it stands is read with no lock (one volatile read: never the state's synchronized getters,
     * which every message this node originates would otherwise enter), a few comparisons, nothing allocated.
     *
     * <p>An agent's inbox consumes from the moment its channels are up, which is BEFORE the node records the controller it
     * joins (ControllerSMHandler: initIOChannels, then setAgentSuccess, then the registration and the first ping): in
     * that window the controller is not known here yet, and the wait holds all the same.
     */
    public boolean attestationPending() {
        if (!required) return false;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        if (st == null || st.regionController || st.region == null) return false;
        long now = System.currentTimeMillis();
        OwnAttestation a = ownAttestation;
        if (a != null) {
            if (ownKeyB64 == null) {
                try {
                    ownCert();                                   // this node's key was not read yet: the comparison below needs it
                } catch (Exception | Error e) {
                    if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
                    return false;
                }
            }
            if (!current(a, st)) a = null;
        }
        return attestWait.waiting(a != null && now < a.exp - attestationMs / 10, now);
    }

    /**
     * Would {@code m}, which this node originates, be signed with a credential that an agent's attestation is: one for a
     * node beyond its controller, not over a direct link, with no session to send it under? What such a message is held
     * with while the attestation is awaited, and what it queues behind ({@link ActiveClient}). False for a message a node
     * only carries on, on a controller, and for what goes to the agent's own controller (which verifies its certificate).
     * While the controller is not recorded yet (see {@link #attestationPending}) nothing is taken for a message to it.
     */
    public boolean needsAttestation(MsgEvent m) {
        if (!required || m == null || m.getParams() == null || m.getParam(PENDING) == null) return false;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        if (st == null || st.regionController || st.region == null) return false;
        Caller c = Caller.fromMsgEvent(m);
        if (c == null || c.isAnonymous()) return false;          // it is not signed at all
        String to = recipient(m);
        if (to == null || directTo(m, to)) return false;
        String node = recipientNode(to), pr = st.parentRegion, pa = st.parentAgent;
        if (node != null && pr != null && pa != null && node.length() == pr.length() + 1 + pa.length() && node.startsWith(pr)
                && node.endsWith(pa) && node.charAt(pr.length()) == '_') return false;     // its own controller verifies its certificate
        return node == null || sessions == null || sessionsBroken || sessions.usable(node, System.currentTimeMillis()) == null;
    }

    /** {@link #currentAttestation} for the node as it stands in {@code st}: no lock, nothing allocated. */
    private boolean current(OwnAttestation a, SpeakerPosition.Standing st) {
        if (!a.region.equals(st.region) || !a.agent.equals(st.agent) || !a.key.equals(ownKeyB64)) return false;
        String pr = st.parentRegion, pa = st.parentAgent, iss = a.issuer;
        return pr == null || pa == null || (iss.length() == pr.length() + 1 + pa.length() && iss.startsWith(pr) && iss.endsWith(pa)
                && iss.charAt(pr.length()) == '/');
    }

    /** Guards the replacement of {@link #ownAttestation}: one that just arrived is never dropped for the one before it. */
    private final Object attestationLock = new Object();
    private final AtomicLong attestationsDropped = new AtomicLong();

    /** Attestations this node dropped because their issuer has another key now (diagnostics, tests). */
    long attestationsDroppedCount() { return attestationsDropped.get(); }

    /**
     * This node has seen, verified, the key {@code region/agent} holds NOW: that controller signed a message with its
     * certificate, or the trust bundle this node took from it. When it is the issuer of the attestation this node holds and
     * another key made that attestation (the controller came back re-keyed), the attestation is dropped: this node signs
     * with its certificate and its watcher pings the parent at once ({@link #takeTrustWanted}; the ping says it holds
     * none, so the parent attests it). Kept, a node beyond the parent that holds the parent's present key refuses
     * everything signed with it until it nears its end. An attestation whose issuer did not name its key (an earlier
     * build) is checked against the key once: its own signature says whether that key made it. A few comparisons when the
     * issuer signed the message; else one.
     */
    private void issuerKeySeen(String region, String agent, PublicKey key, String fp) {
        OwnAttestation a = ownAttestation;
        if (a == null || region == null || agent == null || key == null || fp == null) return;
        String iss = a.issuer;
        int r = region.length();
        if (iss.length() != r + 1 + agent.length() || iss.charAt(r) != '/' || !iss.startsWith(region) || !iss.endsWith(agent)) return;
        String by = a.issuerFp;
        if (by == null) {
            try {
                Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(key.getAlgorithm()));
                s.initVerify(key);
                s.update(attestationBytes(a.signed));
                if (s.verify(Base64.getDecoder().decode(a.signed.sig))) {
                    a.issuerFp = fp;                         // this key made it: nothing to check from here on
                    return;
                }
            } catch (Exception | Error e) {
                if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
                return;                                      // it cannot be told: the attestation stands
            }
        } else if (by.equals(fp)) {
            return;
        }
        synchronized (attestationLock) {
            if (ownAttestation != a) return;                 // replaced meanwhile
            ownAttestation = null;
        }
        attestationsDropped.incrementAndGet();
        attestationWanted = true;                            // one ping now: not at the next interval
        info("attestation DROPPED: " + iss + " has another key now (" + PeerIdentity.shortFp(fp) + ") than the one that attested this node"
                + (by == null ? "" : " (" + PeerIdentity.shortFp(by) + ")") + "; this node signs with its certificate and asks to be attested anew");
    }

    /**
     * A connection this node opened to {@code region/agent} verified with {@code key} (the TLS handshake to its
     * controller's broker: PeerIdentityTrustManager): the key that node holds now, first hand. As {@link #issuerKeySeen}:
     * an attestation of this node that another key of that controller made is dropped NOW. From here until the new
     * attestation arrives (the next pong: the watcher pings at once) this node signs with its certificate, which its
     * controller verifies; what would go to a node beyond it waits for the attestation ({@link #attestationPending},
     * ActiveClient's hold), bounded like the wait of a first join. Nothing when this node holds no attestation, or the
     * key is the one that made it: two field reads.
     */
    public void controllerKeySeen(String region, String agent, PublicKey key) {
        if (!required || ownAttestation == null || region == null || agent == null || key == null) return;
        issuerKeySeen(region, agent, key, PeerTrustRegistry.fingerprint(key.getEncoded()));
    }

    /** This node dropped its attestation and has not pinged its parent since ({@link #takeTrustWanted}). */
    private volatile boolean attestationWanted;
    /** The issuer key (fingerprint) of the attestation this node took last: a change of it is logged. */
    private volatile String attestedBy;

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
        /** Why it is not accepted, as it is counted ({@link #refusal} says it in words); null when it is accepted. */
        public final Refusal reason;
        /**
         * Refused: an RPC request under a session this node does not hold (its id). The {@code 401} that answers it names
         * that session ({@link #answerRefused}), so that its caller's node may send it again, signed. Else null.
         */
        public final String notHeld;
        final Word word;
        /**
         * Accepted, and its signer said under its signature that it does not hold this session ({@link #LOST}) -- with the
         * signer name of the message it refused ({@link #LOST_BY}) and for how long it has not held it ({@link #LOST_FOR},
         * ms); null: it said nothing of the kind. {@code lostNamedCurrent}: it was the session this node sent that signer
         * messages under when the notice was verified. Nothing has been acted on yet: {@link #afterVerified} does that,
         * once the message has passed every refusal.
         */
        public final String lost;
        final String lostName;
        final long lostForMs;
        final boolean lostNamedCurrent;
        /** Accepted under a signature or a MAC: the signer ({@code tenant/region/agent}) and its node ({@code region_agent}); else null. */
        final String signerName, signerNode;
        /** Accepted, as {@code caller}. */
        Verdict(Caller caller) { this(caller, null, null, null, null, 0L, false); }
        Verdict(Caller caller, String signerName, String signerNode, String lost, String lostName, long lostForMs, boolean lostNamedCurrent) {
            this.caller = caller; this.refusal = null; this.reason = null; this.later = false; this.notHeld = null; this.word = null;
            this.signerName = signerName; this.signerNode = signerNode;
            this.lost = lost; this.lostName = lostName; this.lostForMs = lostForMs; this.lostNamedCurrent = lostNamedCurrent;
        }
        /** Not accepted. */
        Verdict(Refusal reason, String refusal, boolean later) { this(reason, refusal, later, null); }
        Verdict(Refusal reason, String refusal, boolean later, Word word) {
            this.caller = null; this.refusal = refusal; this.reason = reason; this.later = later;
            this.word = word; this.notHeld = word == null ? null : word.sid;
            this.signerName = null; this.signerNode = null;
            this.lost = null; this.lostName = null; this.lostForMs = 0L; this.lostNamedCurrent = false;
        }
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
        if (!required) return new Verdict(fromBroker);
        String sigB64 = m.getParam(SIG);
        String macB64 = sigB64 == null ? m.getParam(MsgSessions.MAC) : null;
        if (sigB64 == null && macB64 == null) {
            if (m.getParam(LOST) != null) {         // a notice counts under a signature: an unsigned message carries none on
                m.removeParam(LOST);
                m.removeParam(LOST_BY);
                m.removeParam(LOST_FOR);
            }
            if (legacyAdmitted && fromBroker != null && fromBroker.getKind() == Caller.Kind.AGENT) return new Verdict(fromBroker);
            if (direct && firstHand != null && fromBroker != null && !fromBroker.isAnonymous() && vouchRefusal(firstHand, fromBroker,
                    ce.getPluginBuilder() == null ? java.util.Set.of() : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig())) == null) {
                if (directAccepted.incrementAndGet() == 1) info("direct links ON: this node accepts unsigned messages its broker attests"
                        + " first hand (first: " + m.getMsgType() + " " + m.getParam("action") + " from " + fromBroker + ")");
                // an API user's word is that of the node that delivered it first hand (it vouched: vouchRefusal above)
                if (fromBroker.getKind() == Caller.Kind.API) {
                    m.setParam(SPEAKER, firstHand.getTenant() + "/" + firstHand.getRegion() + "/" + firstHand.getAgent());
                }
                return new Verdict(fromBroker);
            }
            return refuse(m, Refusal.UNSIGNED, "an unsigned message (from " + fromBroker + (firstHand == null ? ", not delivered first hand" : ", delivered by "
                    + firstHand) + ")");
        }
        try {
            String by = m.getParam(BY), cred = m.getParam(CRED), ts = m.getParam(TS), to = m.getParam(TO);
            if (by == null || ts == null || (macB64 == null && cred == null)) return refuse(m, Refusal.OTHER, "a signature without its signer, credential or time");
            long t = Long.parseLong(ts), now = System.currentTimeMillis();
            if (now - t > maxAgeMs) {
                return refuse(m, Refusal.TOO_OLD, "a signature made " + (now - t) / 1000 + " s ago (msg_signature_max_age_ms=" + maxAgeMs + ")");
            }
            if (t - now > maxFutureMs) {
                return refuse(m, Refusal.AHEAD_OF_CLOCK, "a signature made " + (t - now) / 1000 + " s ahead of this node's clock (msg_signature_max_future_ms="
                        + maxFutureMs + "): the clocks of this node and " + by + " differ");
            }
            // the recipient first: a message signed for another node is refused before any key operation
            String notMine = recipientRefusal(to, m);
            if (notMine != null) return refuse(m, to == null ? Refusal.NO_RECIPIENT : Refusal.NOT_FOR_THIS_NODE, notMine + " (from " + by + ")");
            MsgSessions.Found under = null;
            Signer who;
            if (macB64 != null) {
                String peerNode = nodeOf(by), sid = m.getParam(MsgSessions.SID);
                // by the NODE and the session id, never by the name the message declares (MsgSessions.find)
                under = sessions == null || peerNode == null ? null : sessions.find(peerNode, sid);
                if (under == null) {
                    return notHeld(m, fromBroker, by, peerNode, sid, sessions == null ? startedNanos : sessions.notHeldSince(peerNode), now,
                            "a message under a session key this node does not hold (from " + by
                                    + ": this node restarted or the session ended; its sender returns to signatures)");
                }
                if (!by.equals(under.peer.name)) {
                    // This node HOLDS that session, with another signer than the message declares. Refused -- and nothing is
                    // said about the session to anybody: no notice, no word in a 401. A relay that reads a session id off
                    // the wire and writes another tenant's name over the signer's gets a refusal and nothing it can use
                    // (it used to get this node's signed "not held" of a session it held: the true peer dropped it, and
                    // sent a request that had already run a second time).
                    return refuse(m, Refusal.SESSION_KEY, "a message under a session key this node holds with " + under.peer.name
                            + ", not with the signer it declares (" + by + ")");
                }
                if (under.session.endedNanos != 0 || now > under.session.expires) {
                    // KEPT as ended at this instant (its first refusal as ended), not forgotten: from here on nothing is
                    // accepted under it whatever this node's clock does, and this node can say for how long that has been so
                    return notHeld(m, fromBroker, by, peerNode, sid, sessions.ended(under.session), now,
                            "a message under an ended session key (" + by + ")");
                }
                who = under.peer.signer;
                if (who == null) return refuse(m, Refusal.SESSION_KEY, "a message under a session key whose peer no longer verifies (" + by + ")");
            } else {
                String[] why = new String[2];                // [1]: set when newer trust data may cure it
                who = resolve(cred, why);
                if (who == null) {
                    String reason = "the signer's credential does not verify here (" + by + (why[0] == null ? "" : ": " + why[0]) + ")";
                    if (hold && why[1] != null) {
                        wantTrust();                         // newer trust data may cure it: ask the parent now, not at the next ping
                        return new Verdict(Refusal.CREDENTIAL, reason, true);
                    }
                    return refuse(m, Refusal.CREDENTIAL, reason);
                }
            }
            if (!who.name.equals(by)) return refuse(m, Refusal.CREDENTIAL, "signed by " + who.name + ", which is not the declared signer " + by);
            String revoked = signerRefusal(who, now);
            if (revoked != null) {
                if (under != null) sessions.dropPeer(under.peer.node);
                return refuse(m, Refusal.REVOKED, "the signer " + by + " is refused: " + revoked);
            }
            if (to.startsWith("^") && !to.startsWith("^" + who.region + "/")) {
                return refuse(m, Refusal.CALLER_MISMATCH, "a message for the global above " + to.substring(1) + ", signed by " + by + " of another region");
            }
            byte[] replayKey;
            if (under != null) {
                byte[] mac = Base64.getDecoder().decode(macB64);
                if (!MessageDigest.isEqual(mac, MsgSessions.mac(under.session.key, by, canonical(m)))) {
                    return refuse(m, Refusal.BAD_SIGNATURE, "the session MAC does not verify (" + by + ")");
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
                    if (root == null) return refuse(m, Refusal.BAD_SIGNATURE, "a malformed batch signature path (" + by + ")");
                    String cacheKey = Base64.getEncoder().encodeToString(sha256.get().digest(sig)) + "|" + who.fp + "|"
                            + Base64.getEncoder().encodeToString(root) + "|" + count[0];
                    if (batchVerified.getIfPresent(cacheKey) == null) {
                        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(who.key.getAlgorithm()));
                        s.initVerify(who.key);
                        s.update(MsgBatchSigner.signedBytes(root, count[0]));
                        if (!s.verify(sig)) return refuse(m, Refusal.BAD_SIGNATURE, "the batch signature does not verify (" + by + ")");
                        batchVerified.put(cacheKey, Boolean.TRUE);
                    }
                    replayKey = java.nio.ByteBuffer.allocate(sig.length + leaf.length).put(sig).put(leaf).array();
                } else {
                    Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(who.key.getAlgorithm()));
                    s.initVerify(who.key);
                    s.update(canonical(m));
                    if (!s.verify(sig)) return refuse(m, Refusal.BAD_SIGNATURE, "the signature does not verify (" + by + ")");
                }
            }
            Caller signed = Caller.parse(m.getParam(S_CALLER), m.getParam(S_TENANT), m.getParam(S_ROLES));
            if (signed == null || signed.isAnonymous()) return refuse(m, Refusal.CALLER_MISMATCH, "no signed caller");
            String vouch = vouchRefusal(CrescoIdentity.of(who.tenant, who.region, who.agent, null), signed,
                    ce.getPluginBuilder() == null ? java.util.Set.of() : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig()));
            if (vouch != null) return refuse(m, Refusal.CALLER_MISMATCH, vouch);
            if (fromBroker != null && !fromBroker.equals(signed)) {
                return refuse(m, Refusal.CALLER_MISMATCH, "the broker's caller " + fromBroker + " is not the signed caller " + signed);
            }
            // Across a restart (a signature only: a restarted node holds no session, so nothing under a MAC can come again):
            // a message its signer signed at or before the latest one this node accepted from it IN AN EARLIER LIFE may be
            // one it accepted then, and is refused. Two signing times of one signer are compared, nothing else.
            long memory = signerMemoryKey(who);
            ReplayFloor floor = under == null && !who.self ? floor() : null;
            if (floor != null && floor.mayHaveAccepted(memory, t)) {
                return refuse(m, Refusal.REPLAY, "a message " + by + " signed at or before the last one this node accepted from it before"
                        + " this node restarted (it may have accepted it then)");
            }
            switch (replay.record(ReplayMemory.digest64(sha256.get().digest(replayKey)), t, now, memory)) {
                case FRESH:
                    // recorded -- in the operating system's hands -- before the message is accepted: its signing time is
                    // its signer's floor after this node's next restart
                    if (floor != null) floor.accepted(memory, t, now);
                    break;
                case REPLAY: return refuse(m, Refusal.REPLAY, "a signature seen before (a replay) from " + by);

                case TOO_OLD: return refuse(m, Refusal.REPLAY_MEMORY_FULL, "a message signed " + (now - t) / 1000 + " s ago while the replay memory for " + by + " is full");
                case FULL: return refuse(m, Refusal.REPLAY_MEMORY_FULL, "a message while the replay memory for " + by + " (or of this node) is full");
                default: return refuse(m, Refusal.AHEAD_OF_CLOCK, "a message signed at a time this node's clock went back past (from " + by + ")");
            }
            String peerNode = who.node();
            int caps = parseCaps(m.getParam(CAPS));
            Integer had = peerCaps.getIfPresent(peerNode);
            if (had == null || had != caps) peerCaps.put(peerNode, caps);
            // A lost-session notice (LOST) is only READ here, after the signature verified (under == null: never under a
            // session MAC, so never under the session it declares lost), and taken out of the message, so that an answer
            // made from this message never carries it. Nothing is acted on: afterVerified does that, once the message has
            // passed every refusal (the one below for a signer whose key was replaced, and the inbox's own).
            String said = m.getParam(LOST), saidBy = null;
            long saidFor = 0L;
            boolean saidCurrent = false;
            if (said != null) {
                saidBy = m.getParam(LOST_BY);
                String f = m.getParam(LOST_FOR);
                m.removeParam(LOST);
                m.removeParam(LOST_BY);
                m.removeParam(LOST_FOR);
                if (under != null || saidBy == null || to.startsWith("^")) {
                    said = null;
                } else {
                    try {
                        saidFor = f == null ? 0L : Math.max(0L, Long.parseLong(f));
                    } catch (NumberFormatException notANumber) {
                        saidFor = 0L;
                    }
                    // before the session table takes this message's word on the peer's incarnation (which may move the
                    // session aside by itself): was it the session this node sends that peer messages under?
                    saidCurrent = sessions != null && !sessionsBroken && sessions.isCurrent(peerNode, by, said);
                }
            }
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
            // a signature only (never under a MAC: one flag read where a session can be made): a name this signer stands on
            // got another key while this message was being verified. Read AFTER the session took the signer, so that either
            // this sees the flag or keyChanged sees the session
            if (under == null && who.superseded) {
                if (sessions != null) sessions.dropIfSigner(peerNode, who);
                return refuse(m, Refusal.CREDENTIAL, "the signer's credential no longer verifies here (" + by + ": its key, or the key of the"
                        + " controller that attested it, was replaced)");
            }
            if (under != null && macVerified.incrementAndGet() == 1) info("session keys ON: this node accepts messages under a session"
                    + " key it holds (first: " + m.getMsgType() + " " + m.getParam("action") + " from " + by + ")");
            if (verifiedCount.incrementAndGet() == 1) info("message signatures ON: this node checks what it dispatches (first: "
                    + m.getMsgType() + " " + m.getParam("action") + " from " + signed + ", signed by " + by + " for " + to + ")");
            // an API user's word is that of the node whose key vouched for it (the signer, never a relay)
            if (signed.getKind() == Caller.Kind.API) m.setParam(SPEAKER, who.name);
            // a controller that signs with its certificate shows the key it holds now: an attestation of this node that
            // another key of that controller made is dropped (not under a session: its signer is the session's first)
            if (under == null && who.issuer == null && !who.self && ownAttestation != null) issuerKeySeen(who.region, who.agent, who.key, who.fp);
            return new Verdict(signed, by, peerNode, said, saidBy, saidFor, saidCurrent);
        } catch (Exception | Error e) {
            if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
            return refuse(m, Refusal.OTHER, "an unreadable signature (" + e.getClass().getSimpleName() + ")");
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

    /** Refuse {@code m} for {@code reason} ({@code why} says it in words): counted under the reason, logged when its line is due. */
    private Verdict refuse(MsgEvent m, Refusal reason, String why) {
        refusal(reason, m, why);
        return new Verdict(reason, why, false);
    }

    /**
     * A message its receiver was to hold ({@link Verdict#later}) and could not (the hold has no room), or held to the end,
     * is refused after all: counted and logged under the reason of the verdict that was not counted then.
     */
    public void refusedAfterHold(MsgEvent m, Refusal reason, String why) {
        refuse(m, reason == null ? Refusal.OTHER : reason, why);
    }

    // ================================================================ a lost session

    /** The longest session id a notice is made for (a session id is 24 characters of the URL-safe base64 alphabet). */
    private static final int LOST_SID_MAX = 64;

    private static boolean plausibleSid(String sid) {
        if (sid == null || sid.isEmpty() || sid.length() > LOST_SID_MAX) return false;
        for (int i = 0; i < sid.length(); i++) {
            char c = sid.charAt(i);
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_')) return false;
        }
        return true;
    }

    /**
     * Refuse {@code m}: it came under the MAC of a session ({@code sid}) this node does NOT hold with the node it names --
     * no slot of that node's entry holds that id ({@link MsgSessions#find}: by node and id, whatever name the message
     * declares), or the session is kept there as ended -- so it cannot be checked at all. {@code sinceNanos}: since when
     * this node has certainly not held it. Counted and logged like every refusal; and the node {@code by} names is told,
     * so that it stops sending under that session now and not in up to 30 s:
     * <ul>
     * <li>an RPC request is answered {@code 401} as before; the verdict carries the notice, and the answer bears it
     *     ({@link #answerRefused}). One answer per request, as before: nothing is added to the wire but three parameters;</li>
     * <li>anything else has no answer: a notice of its own is sent ({@link #tellLost}), at most one a second per peer.</li>
     * </ul>
     * This runs BEFORE the message is authenticated -- that is the point: it cannot be -- so everything here is decided
     * on an attacker's word, and is written so that it gives one nothing:
     * <ul>
     * <li><b>Never "not held" of a session this node holds.</b> The lookup is by the node and the session id. A message
     *     with a genuine session id is found whatever signer name was written over it: it then fails as a session held
     *     with another signer, or fails its MAC, and in both cases this node says nothing about the session. So a notice
     *     exists only for an id that is in no slot of that node's entry, or is kept there as ended.</li>
     * <li><b>The notice says whose message was refused</b> ({@link #LOST_BY}: the name the refused message declared),
     *     under this node's signature. Its receiver acts only when that is its own name: a notice provoked with another
     *     name (the same region and agent under another tenant, say) is addressed to that node and is none of its business.</li>
     * <li><b>No amplification, and nobody else's budget.</b> A notice of its own is sent only for a message this node's
     *     broker stamped as coming from the very node it names (no stamp, or another node's: no notice, and the sender
     *     recovers as it did before there were notices), so a node can spend only its own second -- one notice a second
     *     per peer -- and the node-wide bound ({@code msg_signing_lost_notices_per_second}) only by being that many nodes.</li>
     * <li><b>No loop.</b> A notice is always signed, never under a session, so it can never come here itself; a message
     *     under a MAC that carries the notice parameter (no node of this build makes one) is refused and not answered
     *     with a notice; a notice is not an RPC request, so its refusal for any other reason is answered by nothing.</li>
     * <li><b>Nothing of the attacker's reaches a destination name or stays in memory.</b> The notice is addressed to the
     *     node the refused message names as its signer: only when tenant, region and agent are names as the controller
     *     issues them (letters, digits, {@code .} and {@code -}, 64 at most: no wildcard, no separator, no list of
     *     destinations), and the session id only when it is one this build could have made. What is remembered is one
     *     entry per notice SENT, never one per message refused.</li>
     * </ul>
     */
    private Verdict notHeld(MsgEvent m, Caller fromBroker, String by, String peerNode, String sid, long sinceNanos, long now, String why) {
        refusal(Refusal.SESSION_KEY, m, why);
        SpeakerPosition.Standing st = ce.cstate == null ? null : ce.cstate.standing();
        if (lostNoticesPerSecond <= 0 || peerNode == null || st == null || st.region == null || m.getParam(LOST) != null || !plausibleSid(sid)) {
            return new Verdict(Refusal.SESSION_KEY, why, false);
        }
        int a = by.indexOf('/'), b = by.indexOf('/', a + 1);         // tenant/region/agent: peerNode != null says it parses
        String tenant = by.substring(0, a), region = by.substring(a + 1, b), agent = by.substring(b + 1);
        if (!CrescoIdentity.isValidTenant(tenant) || !CrescoIdentity.isValidName(region) || !CrescoIdentity.isValidName(agent)
                || (region.equals(st.region) && agent.equals(st.agent))) {
            return new Verdict(Refusal.SESSION_KEY, why, false);
        }
        Word word = new Word(sid, by, sinceNanos, true);
        if (MsgRouter.isRpcRequest(m)) {
            // its 401 goes where the request says it came from (as before); it bears the notice only when that is the
            // node the request names as its signer -- the one node the notice is for, and the one that can send it again
            return region.equals(m.getSrcRegion()) && agent.equals(m.getSrcAgent())
                    ? new Verdict(Refusal.SESSION_KEY, why, false, word) : new Verdict(Refusal.SESSION_KEY, why, false);
        }
        // a notice of its own only for what this node's broker stamped as that very node's: the rate bounds below are
        // then spent by the node they belong to, and by nobody who merely writes its name
        if (fromBroker == null || fromBroker.isAnonymous() || fromBroker.getKind() == Caller.Kind.API
                || !tenant.equals(fromBroker.getTenant()) || !region.equals(fromBroker.getRegion()) || !agent.equals(fromBroker.getAgent())) {
            return new Verdict(Refusal.SESSION_KEY, why, false);
        }
        tellLost(tenant, region, agent, word, now);
        return new Verdict(Refusal.SESSION_KEY, why, false);
    }

    /** As the refusal path does it, for a notice about {@code sid} refused from {@code refused} (tests). */
    void tellLost(String tenant, String region, String agent, String sid, String refused, long now) {
        tellLost(tenant, region, agent, new Word(sid, refused, sessions == null ? startedNanos : sessions.notHeldSince(region + "_" + agent), true), now);
    }

    /**
     * Send the node {@code tenant/region/agent} the notice {@code word}: unless that peer was sent one within the last
     * {@value #LOST_NOTICE_EVERY_MS} ms, or this node sent {@code msg_signing_lost_notices_per_second} of them this
     * second. A peer that gets none goes on as it did before there were notices (its next refused message a second later
     * gets one). No lock is held while the notice is sent.
     */
    private void tellLost(String tenant, String region, String agent, Word word, long now) {
        String peerNode = region + "_" + agent;
        Long last = lostNoticeAt.getIfPresent(peerNode);
        if (last != null && now - last < LOST_NOTICE_EVERY_MS && now >= last) {
            lostWithheld.incrementAndGet();
            return;
        }
        synchronized (lostNoticeLock) {
            long second = now / 1000L;
            if (second != lostNoticeSecond) {
                lostNoticeSecond = second;
                lostNoticesThisSecond = 0;
            }
            if (lostNoticesThisSecond >= lostNoticesPerSecond) {
                lostWithheld.incrementAndGet();
                return;
            }
            lostNoticesThisSecond++;
        }
        // the peer's second is taken by one thread only (two refusals of one peer at the same moment send one notice)
        if (last == null ? lostNoticeAt.asMap().putIfAbsent(peerNode, now) != null : !lostNoticeAt.asMap().replace(peerNode, last, now)) {
            lostWithheld.incrementAndGet();
            return;
        }
        try {
            MsgEvent n = new MsgEvent(MsgEvent.Type.EXEC, ce.cstate.getRegion(), ce.cstate.getAgent(), null, region, agent, null, false, false);
            n.setParam("action", LOST_ACTION);
            // the reply to a call nobody made (see LOST_CALL): the receiver's controller call id, as every controller names it
            n.setParam("is_rpc", "true");
            n.setParam("callId-" + region + "-" + agent + "-" + ce.getPluginBuilder().getPluginID(), LOST_CALL);
            // the namespace the named node reads (a tenant's node travels in its own whatever this says: ControllerEngine.originated)
            n.setParam(io.cresco.library.security.TenantNamespace.TENANT_PARAM, tenant);
            mark(n, word);
            ce.getPluginBuilder().msgOut(n);                       // stamped as this controller, routed, signed as it leaves
        } catch (RuntimeException e) {
            lostWithheld.incrementAndGet();                         // not sent: the peer goes on as before there were notices
        }
    }

    /**
     * Answer {@code rm}, which {@link #verify} refused with {@code v}: an RPC request is answered {@code 401
     * unauthenticated}, anything else is dropped (as {@link MsgRouter#answerUnauthenticated}). When it was refused for a
     * session this node does not hold ({@link Verdict#notHeld}), the answer bears the notice: it is marked here, and the
     * notice is written when the answer is signed ({@link #signIfPending}) -- signed, never under a session. False when
     * the answer could not be sent.
     */
    public boolean answerRefused(MsgEvent rm, Verdict v) {
        if (v != null && v.word != null && MsgRouter.isRpcRequest(rm)) mark(rm, v.word);
        return MsgRouter.answerUnauthenticated(ce.getPluginBuilder(), rm, v == null ? null : v.refusal);
    }

    /**
     * Right after {@link #verify} accepted {@code m} with {@code v}, off the wire and for this node, and after the inbox's
     * own refusals (the tenant rule): the one place a notice is ACTED on. True: the message ends here -- it was a
     * lost-session notice of its own, or the {@code 401} of a call this node has now sent again; it is handed to nobody.
     * False: it goes on as usual.
     *
     * <p><b>Whose notice.</b> Nothing happens unless the notice names THIS node as the signer of the message that was
     * refused ({@link #LOST_BY}). Then, when it names the session this node sends that peer messages under, that session
     * is moved aside and this node signs to the peer again.
     *
     * <p><b>The retry: a request is sent again only when it provably never ran.</b> The peer's {@code 401} names the
     * call and the session, and states for how long (D, by ITS monotonic clock, at the instant it signed) it has not
     * held that session: since it started, since the last time it let go of anything it held with this node, or since it
     * first refused the session as ended. This node knows how long ago (A, by ITS monotonic clock, now) it sent the
     * request. It sends the request again only when {@code A < D} (less a margin for two clocks' rates: D/64 + 5 ms):
     * <pre>
     *   the peer signed at T, and did not hold the session during [T - D, T] -- nor ever after: a session a node does not
     *     hold is never held again (its key came of two ephemeral keys that are gone; an ended one stays ended);
     *   the 401 arrives here at R &gt;= T; the request was sent at R - A &gt; R - D &gt;= T - D;
     *   so the request was sent AFTER the peer stopped holding the session: every copy of it -- the one refused, one a
     *     broker delivers again, one a relay kept -- reaches the peer when it cannot be accepted. It never ran, and
     *     cannot run. The copy sent now, signed, is the only one that can.
     * </pre>
     * No wall clock is read and no two clocks are compared with each other: each side measures a duration on its own. A
     * relay that delays the 401 makes A larger and the retry less likely; it cannot make D larger (it is under the peer's
     * signature, and a second copy of the 401 is refused by the replay memory before this method sees it). A request
     * sent BEFORE the peer stopped holding the session -- before it restarted, say -- may have run there (it ran, the
     * peer crashed before it answered, the broker delivered the unacknowledged copy again): it is NOT sent again, and its
     * caller sees the 401, exactly as on release 1.3.378. So is every request whose peer cannot say (D = 0), and every
     * request of a peer of an earlier build (its 401 bears no notice).
     *
     * <p>One decision per call: the record goes as it is used, with any reply of the peer the request went to, and after
     * {@value #LOST_RETRY_MS} ms; the request sent again is signed, so it is never recorded again. What is sent is this
     * node's own message as it kept it -- checked against the MAC it left with -- never what the refusal carried back.
     *
     * <p><b>How it is sent again: as a first send leaves the router.</b> Nothing is sent for a caller that has stopped
     * waiting (the request's age has reached the {@code rpc_timeout_ms} it states). The copy says what the request said
     * (so the brokers on its way expire it as they would the first, {@link RpcExpiry}), passes the route gate
     * ({@link RouteGate}: a broker with no way to the peer holds it for a route being made anew, or answers its caller
     * {@code route unavailable}), and then goes to the producer, behind whatever this node holds to be sent first. Its
     * caller gets ONE answer: the peer's, or the gate's, or -- when the producer does not take it -- the 401 this method
     * then hands on. A request that carries a source route is never recorded: its way was chosen at its first send and
     * is not chosen again here.
     */
    public boolean afterVerified(MsgEvent m, Verdict v) {
        if (v == null || !v.ok() || m == null || m.getParams() == null) return false;
        if (watchCalls && sessions != null && sessions.callsWatched() != 0) callAnswered(m, v);
        if (v.lost == null) {
            // a reply of the peer a recorded request went to: the call is answered, there is nothing to send again (a
            // message of any OTHER node that names the call id changes nothing)
            if (lostRetries != null && v.signerNode != null && lostRetries.size() != 0) {
                String id = replyCallId(m);
                Retry r = id == null ? null : lostRetries.getIfPresent(id);
                if (r != null && r.peerNode.equals(v.signerNode)) lostRetries.asMap().remove(id, r);
            }
            return false;
        }
        String id = replyCallId(m);
        boolean ownNotice = LOST_CALL.equals(id);                 // a notice of its own: it is handed to nobody either way
        Retry r = ownNotice || id == null || lostRetries == null ? null : lostRetries.getIfPresent(id);
        if (r != null && !r.peerNode.equals(v.signerNode)) r = null;
        if (r != null) lostRetries.asMap().remove(id, r);          // the peer it went to answered: one decision per call
        if (!selfName().equals(v.lostName)) {
            lostIgnored.incrementAndGet();                         // refused under another name than this node's: not its business
            return ownNotice;
        }
        if (v.lostNamedCurrent) {
            if (sessions != null) sessions.lostAtPeer(v.signerNode, v.signerName, v.lost);
            if (lostHeard.incrementAndGet() == 1) info("session with " + v.signerName + " DROPPED: it says it does not hold it (it restarted,"
                    + " or the session ended there); this node signs to it again and agrees a new session (first of this kind)");
        } else {
            lostIgnored.incrementAndGet();                         // not the session this node sends under: nothing changes
        }
        if (ownNotice) return true;
        if (r == null || !"401".equals(m.getParam("status_code")) || !r.sid.equals(v.lost)) return false;
        long ageMs = (System.nanoTime() - r.sentNanos) / 1_000_000L + 1;   // rounded up: this node's own monotonic clock
        if (ageMs + v.lostForMs / 64 + 5 >= v.lostForMs) {
            // sent before the peer stopped holding the session (or the peer cannot say): it may have run. The 401 goes to
            // its caller, as on 1.3.378
            lostNotRetried.incrementAndGet();
            return false;
        }
        boolean sent;
        try {
            MsgEvent again = again(r);
            if (again == null) {
                lostNotRetried.incrementAndGet();
                return false;
            }
            // a caller that has stopped waiting gets nothing sent on its behalf. What the request says of its caller's
            // wait (rpc_timeout_ms) stays as it left the first time -- it may be part of what an application signed --
            // so the brokers on its way keep the copy for that long from now (RpcExpiry): longer than its caller waits
            // by the request's age, which is under LOST_RETRY_MS and within the allowance every expiry has for clocks
            long stated = RpcExpiry.statedTimeoutMs(again);
            if (stated > 0L && ageMs >= stated) {
                lostNotRetried.incrementAndGet();
                return false;
            }
            mark(again, RESEND);                                    // signed, whatever session this node holds by now
            java.util.function.Predicate<MsgEvent> tap = resendTap;
            if (tap != null) {
                sent = tap.test(again);
            } else {
                ce.msgCounts().originated();                        // a message of this node's, as the one refused was
                // as a first send leaves the router: the route gate first (a broker with no way to the peer holds the
                // request for a route being made anew, or answers its caller "route unavailable" -- either way it is
                // the gate's from here, and the 401 is not handed on as a second answer), then the producer
                if (!ce.routeKnown(again, RESEND_HOP, false)) {
                    lostRetried.incrementAndGet();
                    return true;
                }
                sent = ce.getActiveClient().sendMessage(again);
            }
        } catch (Exception | Error e) {
            if (e instanceof VirtualMachineError) throw (VirtualMachineError) e;
            sent = false;
        }
        if (!sent) return false;                                    // counted where it failed: its caller sees the 401, as before
        if (lostRetried.incrementAndGet() == 1) info("lost sessions: an RPC request " + v.signerNode + " refused because it does not hold the"
                + " session it came under, sent after it stopped holding it, was sent again, signed; its caller does not see the"
                + " refusal (first of this kind: " + m.getMsgType() + " " + m.getParam("action") + ")");
        return true;
    }

    // ---- a call a peer does not answer (MsgSessions.called)

    /**
     * {@code msg_signing_unanswered_calls} (default true): a call to a peer that times out at this node, or is answered
     * {@code route unavailable}, ends the use of the session with that peer until it confirms it again.
     */
    private final boolean watchCalls;
    private final AtomicLong ownNoRoute = new AtomicLong();

    /**
     * Calls of this node that a peer did not answer and that set its session aside: the caller's wait ended, a node on
     * the way answered that there is no route, or this node's own broker had none.
     */
    public long callsUnansweredCount() {
        return sessions == null ? 0 : sessions.callsTimedOut() + sessions.callsNoRoute() + ownNoRoute.get();
    }
    /** Calls this node awaits an answer to now, and calls it did not watch for want of room (tests, diagnostics). */
    int callsWatched() { return sessions == null ? 0 : sessions.callsWatched(); }
    long callsNotWatchedCount() { return sessions == null ? 0 : sessions.callsNotWatched(); }

    /**
     * {@code m}, verified, answers a call this node may await ({@link MsgSessions#called}).
     *
     * <p>An answer ends the watch: the call did not time out at its caller. It is found by the node the answer says it
     * comes from (the node the call went to: a controller on the way answers in that node's place, as a refusal of the
     * tier does), else by the node that signed it.
     *
     * <p>One answer does more: {@code route unavailable} made by ANOTHER node than the peer (a controller on the way has
     * no consumer for the peer's inbox, so the peer, or the controller it is reached through, is gone) sets the session
     * aside at once: what this node sends that peer from now on is signed, and a peer that comes back restarted verifies
     * it -- under the session it would have refused it. The peer confirms the session with its next message that names
     * or uses it, if it still holds it.
     *
     * <p>What an attacker gets: nothing but signatures, or nothing at all. The answer must verify (a node of this fabric
     * made it), must be addressed to the caller of a call this node awaits from exactly that peer, and must name its id,
     * which only the nodes on the call's way ever saw -- and those can drop the call anyway. A session set aside drops no
     * key and refuses nothing: this node signs (the stronger credential) until the peer's next word. A watch ended by a
     * false answer leaves the session as it would be on 1.3.378, which watches nothing.
     */
    private void callAnswered(MsgEvent m, Verdict v) {
        String from = m.getSrcRegion() == null || m.getSrcAgent() == null ? null : m.getSrcRegion() + "_" + m.getSrcAgent();
        String peer = from != null && sessions.awaits(from) ? from
                : v.signerNode != null && sessions.awaits(v.signerNode) ? v.signerNode : null;
        if (peer == null) return;
        String id = replyCallId(m);
        if (id == null) return;
        if (m.getParam(MsgRouter.ROUTE_UNAVAILABLE) == null || peer.equals(v.signerNode)) {
            sessions.answered(peer, id);
        } else if (sessions.unanswered(peer, id)) {
            setAside(peer, "a call to it was answered `route unavailable` by " + v.signerNode);
        }
    }

    /**
     * This node's own route gate answered a request of this node {@code route unavailable} (MsgRouter.noRoute): its broker
     * has no way to the node the request is for. The session with that node is not sent under until it confirms it again
     * (as when this node's broker saw the node's consumer leave, {@link #inboxConsumerLeft}). O(1).
     */
    public void noRouteTo(MsgEvent request) {
        if (!watchCalls || sessions == null || request == null) return;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        if (st == null || st.region == null || !st.region.equals(request.getSrcRegion()) || st.agent == null
                || !st.agent.equals(request.getSrcAgent()) || request.getDstRegion() == null || request.getDstAgent() == null) {
            return;                                              // not a request of this node's: its caller's node hears the answer
        }
        String node = request.getDstRegion() + "_" + request.getDstAgent();
        if (sessions.heldWith(node) == null) return;
        sessions.linkDown(node);
        ownNoRoute.incrementAndGet();
        setAside(node, "this node's broker has no route to it");
    }

    private final AtomicLong setAsideLogged = new AtomicLong();

    private void setAside(String peer, String why) {
        if (setAsideLogged.incrementAndGet() == 1) info("session with " + peer + " SET ASIDE: " + why + "; this node signs to it until it"
                + " confirms the session again (first of this kind)");
    }

    /** The hop a request sent again names when the route gate answers it ({@code route_unavailable}). */
    static final String RESEND_HOP = "RESEND";

    /** The call {@code m} answers at the node it is addressed to (a reply's call id is keyed by its destination), or null. */
    private static String replyCallId(MsgEvent m) {
        return m.getParam("is_rpc") == null ? null
                : m.getParam("callId-" + m.getDstRegion() + "-" + m.getDstAgent() + "-" + m.getDstPlugin());
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
     * once trust data changed since; and once after this node dropped its attestation ({@link #issuerKeySeen}), whatever
     * else changed. Two volatile reads when nothing is wanted.
     */
    public boolean takeTrustWanted(long now) {
        if (attestationWanted) {
            attestationWanted = false;
            trustAskedAt = now;
            return true;
        }
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
        if (node == null) return;
        // (1) Its session: this node's broker has no way to that node now, so nothing more is sent under the session it
        // holds with it until that node confirms it again (MsgSessions.linkDown) -- signed, what a broker stores for it
        // verifies when it is back, restarted or not. One peer per event: when a bridge goes, the broker says so for each
        // node whose inbox that bridge carried.
        if (sessions != null) sessions.linkDown(node);
        // (2) When it is this node's own parent (a region's global): nothing reaches it first hand either
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        if (st != null && st.parentRegion != null && node.length() == st.parentRegion.length() + 1 + st.parentAgent.length()
                && node.startsWith(st.parentRegion) && node.endsWith(st.parentAgent) && node.charAt(st.parentRegion.length()) == '_') {
            parentLinkReset();
        }
        if (directChildren.remove(node) == null) return;
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
    public void parentLinkReset() {
        directParentUntil = 0L;
        directWithheldTillPong = true;
    }

    /**
     * An AGENT's connection to its controller's broker was interrupted (AgentConsumer's transport listener says so at
     * once). Every session of an agent ran over that one link: none is sent under until its peer confirms it again
     * (MsgSessions.linkDown), and what goes to the controller is signed until its next pong ({@link #parentLinkReset}).
     * A controller has its own broker, which tells it node by node ({@link #inboxConsumerLeft}): nothing here for it.
     */
    public void parentLinkDown() {
        parentLinkReset();
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        SpeakerPosition.Standing st = cs == null ? null : cs.standing();
        if (sessions != null && st != null && !st.regionController) sessions.linkDown(System.currentTimeMillis());
    }

    /**
     * A ping to the parent went unanswered. As {@link #parentLinkReset}, and for {@value #DIRECT_RETRY_MS} ms this node
     * neither offers the direct link nor takes the parent's offer: the ping or its pong may have been lost BECAUSE it
     * travelled unsigned (carried to its recipient by another controller's bridge, it is not first hand there), and a
     * link that is tried again at once fails again -- every second ping, until the parent is taken for lost. Signed
     * messages are accepted whatever their path.
     */
    public void parentPingUnanswered() {
        directWithheldUntil = System.currentTimeMillis() + DIRECT_RETRY_MS;
        parentLinkReset();
    }

    static final long DIRECT_RETRY_MS = 60_000L;
    /** This node does not offer its parent the direct link: until the next pong (a join), or until a time (a lost ping). */
    private volatile boolean directWithheldTillPong;
    private volatile long directWithheldUntil;

    private boolean directWithheld(long now) {
        long until = directWithheldUntil;
        return directWithheldTillPong || (until != 0L && now < until && now + DIRECT_RETRY_MS >= until);
    }

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

    /**
     * Count one refusal under {@code reason} and log it when the reason's line is due (its first refusal, then at most once
     * per {@value #REFUSAL_LINE_EVERY_MS} ms): {@code MESSAGE SIGNATURE: REFUSED <why> [<type> <action>] (<N> refused so
     * far; <K> of this kind, <reason>; each kind is logged at most every 10 s)} -- N the messages this node refused for
     * any reason, K this reason's count. The line names the message's type and action, which {@code why} does not; it is
     * built only when it is logged.
     */
    private void refusal(Refusal reason, MsgEvent m, String why) {
        if (!refusals.count(reason.ordinal())) return;
        String line = "MESSAGE SIGNATURE: REFUSED " + why + (m == null ? "" : " [" + m.getMsgType() + " " + m.getParam("action") + "]")
                + " (" + refusedCount() + " refused so far; " + refusals.of(reason.ordinal()) + " of this kind, " + reason.key
                + "; each kind is logged at most every " + REFUSAL_LINE_EVERY_MS / 1000L + " s)";
        warn(line);
        java.util.function.Consumer<String> tap = refusalLineTap;
        if (tap != null) tap.accept(line);
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
        for (int attempt = 0; attempt < 3; attempt++) {
            long epoch = trustEpoch.get();
            r = cred.startsWith("cert:") ? resolveCert(cred.substring(5), now, why)
                    : cred.startsWith("att:") ? resolveAttestation(cred.substring(4), now, why) : null;
            if (r == null) return null;
            standsOn(r, k);                              // found from here on by a name that gets another key (keyChanged)
            credCache.put(k, r);
            if (trustEpoch.get() == epoch && !r.superseded) return r;
            credCache.asMap().remove(k, r);              // a name got another key meanwhile: judged again as things stand
        }
        why[0] = "the keys this node holds changed while its credential was verified";
        if (why.length > 1) why[1] = "it is verified again";
        return null;
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
        boolean ownIssued = iss[0].equals(ce.cstate.getRegion()) && iss[1].equals(ce.cstate.getAgent());
        if (ownIssued) {
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
        if (ownIssued) {
            // this node attested that key for that name, and may have bound the name to another key since (an operator's
            // rebind of a re-installed agent): its own word for the former key does not outlive the binding
            String bound = peers().pinnedKey(new PeerIdentity.Expected(a.r, a.a));
            if (bound != null && !bound.equals(fp)) {
                why[0] = "an attestation of a key " + a.r + "/" + a.a + " no longer has here (the name is bound to another key, "
                        + PeerIdentity.shortFp(bound) + ")";
                return null;
            }
        }
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
                childRegions.remove(e.r, en.getKey());
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
                // a global this node no longer takes the word of is no longer above it
                GlobalAbove g = globalAbove;
                if (g != null && ROLE_GLOBAL.equals(e.role) && isGlobalAbove(e.r, e.a, g.parentRegion, g.parentAgent)) globalAbove = null;
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
        // the offer of the direct link: not on the first ping of a join (the parent's first pong then comes signed, whatever
        // its path), nor for a while after a ping went unanswered
        if (direct && !directWithheld(System.currentTimeMillis())) ping.setParam(P_DIRECT_OK, "1");
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
                if (caller != null && caller.getKind() == Caller.Kind.CONTROLLER) {
                    String child = caller.getRegion() + "_" + caller.getAgent();
                    long now = System.currentTimeMillis();
                    if (!childDirect) {
                        // a ping without the offer withdraws it: the child wants signatures now (this pong included)
                        directChildren.remove(child);
                    } else if (directAgain(child, now) && (directChildren.size() < DIRECT_CHILDREN_MAX || directChildren.containsKey(child))) {
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
                            BundleEntry e = entry(id.getTenant(), id.getRegion(), id.getAgent(), leaf.getPublicKey(), ROLE_REGION,
                                    intern(peers().pathRefs(new X509Certificate[]{leaf})));
                            e.cert = leaf;
                            childControllers.put(n, e);
                            childRegions.put(id.getRegion(), n);
                            if (prev == null || !prev.fp.equals(e.fp) || !java.util.Objects.equals(prev.refs, e.refs)) bundleChanged();
                            // the region came back with another key: what its former key signed and attested ends here
                            if (prev != null && !prev.fp.equals(e.fp)) keyChanged(id.getRegion(), id.getAgent(), e.fp);
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
            refusal(Refusal.TRUST_DATA, null, "ping of " + msg.getSrcRegion() + "/" + msg.getSrcAgent() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
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
        long pongAt = System.currentTimeMillis();
        directWithheldTillPong = false;                      // the link answers: the next ping offers the direct link again
        if (direct && "1".equals(pong.getParam(P_DIRECT_OK)) && parentRegion != null && parentAgent != null && !directWithheld(pongAt)) {
            directParent = parentRegion + "_" + parentAgent;
            directParentUntil = pongAt + DIRECT_OFFER_MS;
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
                    OwnAttestation taken = new OwnAttestation(att, a, attIss == null || attIss.isEmpty() ? null : attIss);
                    synchronized (attestationLock) {
                        ownAttestation = taken;
                    }
                    attestationWanted = false;               // it is here
                    ActiveClient client = ce.getActiveClient();  // what waited for it is sent (no lock is held here)
                    if (client != null) {
                        try { client.attested(); } catch (RuntimeException ignore) { }
                    }
                    // one line per issuer key: the first attestation, and each one after its issuer came back re-keyed
                    String madeBy = taken.issuerFp, before = attestedBy;
                    if (madeBy != null && !madeBy.equals(before)) {
                        attestedBy = madeBy;
                        info("attested by " + taken.issuer + " (its key " + PeerIdentity.shortFp(madeBy) + ")" + (before == null ? ""
                                : "; the attestation before was made by its key " + PeerIdentity.shortFp(before)));
                    }
                }
            }
            String b = pong.getParam(P_BUNDLE) == null ? null : pong.getCompressedParam(P_BUNDLE);
            if (b != null && parentRegion != null && parentAgent != null) acceptBundle(b, parentRegion, parentAgent);
        } catch (Exception e) {
            refusal(Refusal.TRUST_DATA, null, "pong of " + parentRegion + "/" + parentAgent + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
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
            refusal(Refusal.TRUST_DATA, null, "a trust bundle that does not verify as " + parentRegion + "/" + parentAgent);
            return;
        }
        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(leaf.getPublicKey().getAlgorithm()));
        s.initVerify(leaf.getPublicKey());
        s.update(bundleBytes(sb));
        if (!s.verify(Base64.getDecoder().decode(sb.sig))) {
            refusal(Refusal.TRUST_DATA, null, "a trust bundle whose signature does not verify (" + parentRegion + "/" + parentAgent + ")");
            return;
        }
        // the parent's certificate, verified here, with the bundle it signed: the key it holds now
        issuerKeySeen(parentRegion, parentAgent, leaf.getPublicKey(), PeerTrustRegistry.fingerprint(leaf.getPublicKey().getEncoded()));
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
        // the names this bundle lists with another key than the one before it did
        List<BundleEntry> reKeyed = null;
        for (Map.Entry<String, BundleEntry> en : next.entrySet()) {
            BundleEntry had = bundle.get(en.getKey()), has = en.getValue();
            if (had != null && had != has && !had.fp.equals(has.fp)) {
                if (reKeyed == null) reKeyed = new ArrayList<>();
                reKeyed.add(has);
            }
        }
        // in place, never emptied: a message checked meanwhile sees the old entry or the new one
        bundle.putAll(next);
        bundle.keySet().removeIf(k -> !next.containsKey(k));
        // a name with another key: what was verified on its former key is no longer served
        if (reKeyed != null) for (BundleEntry e : reKeyed) keyChanged(e.r, e.a, e.fp);
        // who this parent says the global is: as it signed it now, never an entry only kept for its grace period
        List<String> globals = new ArrayList<>();
        for (BundleEntry e : next.values()) {
            if (e.keepUntil == 0 && ROLE_GLOBAL.equals(e.role)) { globals.add(e.r); globals.add(e.a); }
        }
        globalAbove = globals.isEmpty() ? null : new GlobalAbove(parentRegion, parentAgent, globals.toArray(new String[0]));
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
                    // an entry only kept for its grace period is a key this node's children still verify with, and no
                    // longer what this node's parent says: a global this region left is not served as the global (its
                    // agents would hold it as the global above them, with a management word, for the whole grace period)
                    all.put(e.r + "/" + e.a, e.keepUntil > 0 && ROLE_GLOBAL.equals(e.role)
                            ? entry(e.t, e.r, e.a, e.key, ROLE_FORMER_GLOBAL, e.refs) : e);
                    if (e.keepUntil > 0) validUntil = Math.min(validUntil, e.keepUntil);
                }
            }
            for (BundleEntry e : childControllers.values()) all.put(e.r + "/" + e.a, e);
            X509Certificate[] chain = ce.getCertificateManager().getPublicCertificate();
            BundleEntry self = entry(ce.tenant(), ce.cstate.getRegion(), ce.cstate.getAgent(), own.getPublicKey(),
                    ce.cstate.isGlobalController() ? ROLE_GLOBAL : ROLE_REGION, intern(peers().pathRefs(chain)));
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
