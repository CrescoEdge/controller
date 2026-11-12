package io.cresco.agent.controller.communication;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Predicate;

/**
 * End-to-end session keys between two nodes, for the MsgEvents that pass a relay. A signature costs its origin about 2 ms
 * of one core (RSA-3072), so a request and its reply across another region's bridge paid two of them on the critical path:
 * 5.9 ms instead of 1.1 ms. Here two nodes that talk agree, once, on a key only they hold, and then authenticate each
 * message with an HMAC under it (about a microsecond): a relay still cannot forge, alter, replay or re-address what passes
 * through it (the MAC covers the message's canonical form, its recipient included).
 *
 * <p>The agreement rides on SIGNED messages, which carry their sender's current credential: the offerer attaches an
 * ephemeral P-384 key ({@value #KX}); the acceptor its own ephemeral key and the session id ({@value #KX_ACK}). Each derives
 * the key from the ECDH secret (SP 800-56A, ephemeral-ephemeral) with HMAC-SHA-384 (SP 800-56C two-step: extract under a
 * fixed salt, expand over both names and both public keys). One session serves both directions: the MAC covers the sender's
 * name and the message's canonical form. A message carrying an offer or an answer is always signed, never MAC'd, and a
 * receiver acts on an offer or answer only from a signed message: an agreement costs its sender a signature, and each
 * renewal (hourly) refreshes both credentials. While a session is usable, an offer or answer rides at most one message a
 * second ({@value #ATTACH_EVERY_MS} ms); the others stay MAC'd.
 *
 * <p>A node sends under a session only while its peer said, within the last {@value #CONFIRM_MS} ms, that it still holds
 * it: every message to a peer carries the session this node holds with it ({@value #HELD}), and a message from the peer
 * that names another one stops this node's use of its session at once. Every message also names its sender's incarnation
 * ({@value #INC}, random at each start): a peer that restarted (and so lost its sessions) names a new one, and its next
 * message makes this node return to signatures and agree anew; what was sent to it meanwhile under the lost session is
 * refused like any unauthenticated message -- and the node that refuses says so at once, on a signed message that names
 * the session ({@link MsgSigning#LOST}): its sender drops that session ({@link #lostAtPeer}) and signs again within one
 * round trip, whether or not the restarted node had anything else to say to it.
 *
 * <p><b>Renewal never interrupts.</b> An offer from a peer that still holds a confirmed session with this node is answered
 * into a PENDING slot: this node keeps sending under the confirmed session until the peer uses (or names) the new one.
 *
 * <p><b>Bounded.</b> One entry per peer node ({@code msg_signing_sessions_max}, default 200,000; idle ones expire with
 * their sessions): its current, pending and previous sessions (about 0.3 KB each) and who the peer authenticated as. When
 * the table is full a new peer is simply not given a session -- its messages stay signed -- so no session in use is ever
 * evicted. An offer is accepted from a peer at most once every {@value #OFFER_ACCEPT_MIN_MS} ms (a P-384 key generation
 * and an ECDH each), and only under that peer's own lock: a peer cannot make this node spend more, nor block others.
 */
final class MsgSessions {

    static final String KX = "cresco_sig_kx", KX_ACK = "cresco_sig_kxa", HELD = "cresco_sig_held", SID = "cresco_sig_sid",
            MAC = "cresco_sig_mac", INC = "cresco_sig_inc";
    private static final byte[] KDF_SALT_LABEL = "cresco-msg-session-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAC_LABEL = "cresco-msg-mac-v1".getBytes(StandardCharsets.UTF_8);
    static final long CONFIRM_MS = 30_000L, OFFER_MS = 30_000L, OFFER_ACCEPT_MIN_MS = 10_000L, ATTACH_EVERY_MS = 1_000L,
            USABLE_MARGIN_MS = 60_000L;
    static final int DEFAULT_MAX_PEERS = 200_000;

    /** A session with one peer. */
    static final class Session {
        final String sid, offerDigest;
        final SecretKeySpec key;
        final long expires;
        /** When the peer last said it holds this session (this node's clock); 0: not (or no longer) confirmed. */
        volatile long confirmedAt;
        /** The acceptor's answer, attached to its messages to the offerer until the offerer confirms. */
        volatile String ack;
        volatile long ackSentAt;
        /**
         * When this node first treated the session as ended ({@link System#nanoTime}; 0: it has not): from that instant on
         * nothing is accepted or sent under it, whatever a clock does afterwards. It stays in its slot as "ended at".
         */
        volatile long endedNanos;

        Session(String sid, byte[] key, String offerDigest, long expires) {
            this.sid = sid;
            this.key = new SecretKeySpec(key, "HmacSHA384");
            this.offerDigest = offerDigest;
            this.expires = expires;
        }
    }

    private static final class Offer {
        final KeyPair eph;
        final byte[] spki;
        final String digest;
        final long at;
        volatile long sentAt;
        Offer(KeyPair eph, long at) {
            this.eph = eph;
            this.spki = eph.getPublic().getEncoded();
            this.digest = b64(sha256(spki));
            this.at = at;
        }
    }

    /** Everything this node holds with one peer node ({@code region_agent}); changed only while holding it. */
    static final class Peer {
        final String node;
        /** The peer's name ({@code tenant/region/agent}) its sessions are bound to. */
        volatile String name;
        /** Its incarnation, as last heard. */
        volatile String inc;
        /** Who it authenticated as on its last signed message (MsgSigning's signer): what its MACs are checked against. */
        volatile MsgSigning.Signer signer;
        volatile Session current, pending, previous;
        volatile Offer offer;
        long lastOfferAccepted;
        /**
         * When this entry last let go of a session without keeping it (a slot was written over, the peer's sessions were
         * dropped), or was made ({@link System#nanoTime}): since then every session held with this peer is still in one of
         * its three slots, so a session id that is in none of them was not held at any moment since.
         */
        volatile long forgotNanos = System.nanoTime();
        /**
         * The calls this node awaits an answer to from this peer ({@link MsgSessions#called}): call id to the session it
         * left under and when its caller stops waiting. Null: none. Read and changed only while holding this entry.
         */
        java.util.HashMap<String, Call> calls;
        /** How many there are, and the earliest of their ends ({@link System#nanoTime}; it means something only while there are any). */
        volatile int callsOpen;
        volatile long callsDue;
        Peer(String node) { this.node = node; }
    }

    /** A call awaited: the session its request left under, and when its caller stops waiting ({@link System#nanoTime}). */
    static final class Call {
        final Session session;
        final long due;
        Call(Session session, long due) { this.session = session; this.due = due; }
    }

    /** A session a message names, and its peer. */
    static final class Found {
        final Peer peer;
        final Session session;
        Found(Peer peer, Session session) { this.peer = peer; this.session = session; }
    }

    private final long lifetimeMs, renewMs;
    private final int maxPeers;
    /** This node's incarnation: random at each start. */
    volatile String incarnation;
    private final Cache<String, Peer> peers;
    /**
     * Since when this table can speak for itself ({@link System#nanoTime}): its start, and the last time an entry was put
     * out of it while it could still hold a session (the table was full, or it was emptied). An entry that left because
     * it was idle held nothing that was used for {@link #idleNanos}.
     */
    private volatile long startedNanos = System.nanoTime(), evictedNanos = startedNanos;
    private final long idleNanos;
    /** Offers accepted (a key pair and an ECDH each): diagnostics, tests. */
    final java.util.concurrent.atomic.AtomicLong offersAccepted = new java.util.concurrent.atomic.AtomicLong();
    private static final ThreadLocal<Mac> HMAC = ThreadLocal.withInitial(() -> {
        try { return Mac.getInstance("HmacSHA384"); } catch (Exception e) { throw new IllegalStateException(e); }
    });

    MsgSessions(long lifetimeMs) {
        this(lifetimeMs, DEFAULT_MAX_PEERS);
    }

    MsgSessions(long lifetimeMs, int maxPeers) {
        this.lifetimeMs = Math.max(120_000L, lifetimeMs);
        // renewed well before its last minute, in which it is no longer sent under (usable)
        this.renewMs = Math.max(USABLE_MARGIN_MS + 30_000L, Math.min(300_000L, this.lifetimeMs / 10));
        this.maxPeers = Math.max(1, maxPeers);
        this.incarnation = newIncarnation();
        // the hard bound is a little above the admission bound (size() is approximate under concurrency); an idle peer goes
        // with its last session
        this.idleNanos = (this.lifetimeMs + 120_000L) * 1_000_000L;
        this.peers = CacheBuilder.newBuilder().maximumSize(this.maxPeers + this.maxPeers / 8 + 16)
                .expireAfterAccess(Duration.ofMillis(this.lifetimeMs + 120_000L))
                .removalListener((com.google.common.cache.RemovalNotification<String, Peer> gone) -> {
                    if (gone.getCause() != com.google.common.cache.RemovalCause.EXPIRED) evictedNanos = System.nanoTime();
                    Peer p = gone.getValue();
                    if (p != null) {
                        synchronized (p) {                             // the calls awaited from it go with it
                            if (p.calls != null) callsWatched.addAndGet(-p.calls.size());
                            p.calls = null;
                            p.callsOpen = 0;
                        }
                    }
                }).build();
    }

    private Peer peer(String node, boolean create) {
        Peer p = peers.getIfPresent(node);
        if (p != null || !create) return p;
        if (peers.size() >= maxPeers) return null;                    // full: this peer stays with signatures
        try {
            return peers.get(node, () -> new Peer(node));
        } catch (java.util.concurrent.ExecutionException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------ sending

    /** The session this node holds with {@code peerNode} (region_agent), to tell it so; or null. */
    String heldWith(String peerNode) {
        Peer p = peers.getIfPresent(peerNode);
        Session s = p == null ? null : p.current;
        return s == null ? null : s.sid;
    }

    /**
     * This node's answer to {@code peerNode}'s offer while the peer has not confirmed it, or null. While a session is
     * usable ({@code macUsable}) at most once a second: the message carrying it is signed.
     */
    String ackFor(String peerNode, long now, boolean macUsable) {
        Peer p = peers.getIfPresent(peerNode);
        if (p == null) return null;
        Session s = p.pending != null ? p.pending : p.current;
        if (s == null || s.ack == null) return null;
        if (macUsable) {
            if (now - s.ackSentAt < ATTACH_EVERY_MS) return null;
            s.ackSentAt = now;
        }
        return s.ack;
    }

    /**
     * For how long after its peer's last word a session is sent under: {@value #CONFIRM_MS} ms unless set shorter
     * ({@code msg_signing_session_confirm_ms}; never longer, never under a second). What a node sends a peer that has
     * been silent for longer is signed -- so a peer that was away for longer than this, and comes back restarted (or
     * under another name and key, where its notice cannot name the session), is sent nothing it must refuse. The
     * price of a shorter time is a signature on what is sent to a peer that speaks less often than that.
     */
    private volatile long confirmMs = CONFIRM_MS;

    void confirmWithin(long ms) { confirmMs = Math.max(1_000L, Math.min(CONFIRM_MS, ms)); }

    /** The session to send to {@code peerNode} under: the peer confirmed it lately and it is not about to end; or null. */
    Session usable(String peerNode, long now) {
        Peer p = peers.getIfPresent(peerNode);
        // a call to this peer whose caller has stopped waiting: the session it left under is not sent under (timedOut)
        if (p != null && p.callsOpen != 0 && System.nanoTime() - p.callsDue >= 0) timedOut(p);
        Session s = p == null ? null : p.current;
        if (s == null || s.confirmedAt <= 0 || now - s.confirmedAt >= confirmMs || now >= s.expires - USABLE_MARGIN_MS || s.endedNanos != 0) {
            return null;
        }
        // every session of this node ran over a link that went down after the peer last confirmed it (linkDown)
        return s.confirmedAt > unconfirmedUpTo ? s : null;
    }

    // ------------------------------------------------------------------ a link known to be down

    /** Sessions their peers confirmed at or before this moment are not sent under ({@link #linkDown(long)}); 0: none. */
    private volatile long unconfirmedUpTo;

    /**
     * This node knows that its way to {@code peerNode} is gone (its broker lost the consumer of that node's inbox: the
     * node's connection broke, or the bridge that carried it went). The session with THAT peer is not sent under from now
     * on, until the peer confirms it again -- its next message that names or uses it, which it can only send once the way
     * is back. The session is not dropped: a peer that did not restart goes on with it. Until now this node went on
     * sending under it for 30 s, and what a broker stored of that for a peer that restarted was refused when it arrived;
     * a signed message stored so verifies. O(1): one entry.
     */
    void linkDown(String peerNode) {
        Peer p = peerNode == null ? null : peers.getIfPresent(peerNode);
        if (p == null) return;
        synchronized (p) {
            Session s = p.current;
            if (s != null) s.confirmedAt = 0;
        }
    }

    // ------------------------------------------------------------------ a call the peer does not answer

    /**
     * Calls awaited at most from one peer, and from all of them together; a call id longer than {@value #CALL_ID_MAX}
     * characters is not kept. A call beyond a bound is not watched (counted): those that are watched are enough to notice
     * a peer that answers nothing, and a call that is not watched changes nothing -- the session is used as before.
     */
    static final int CALLS_PER_PEER = 16, CALLS_MAX = 8_192, CALL_ID_MAX = 128;
    private final java.util.concurrent.atomic.AtomicInteger callsWatched = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong callsNotWatched = new java.util.concurrent.atomic.AtomicLong(),
            callsTimedOut = new java.util.concurrent.atomic.AtomicLong(), callsNoRoute = new java.util.concurrent.atomic.AtomicLong();

    /** Told, outside every lock, the peer whose session was set aside because a call to it timed out (the log's first line). */
    volatile java.util.function.Consumer<String> onTimedOut;

    /** Calls awaited now; calls not watched for want of room; calls that timed out / were answered that there is no route. */
    int callsWatched() { return callsWatched.get(); }
    long callsNotWatched() { return callsNotWatched.get(); }
    long callsTimedOut() { return callsTimedOut.get(); }
    long callsNoRoute() { return callsNoRoute.get(); }

    /**
     * An RPC request of this node left for {@code peerNode} under the session {@code s}, and its caller waits
     * {@code timeoutMs} for the answer (what the request states: {@code rpc_timeout_ms}). An agent has no broker to tell
     * it that a node beyond its controller went away, but it has this: a call to that node which nothing answers. When
     * the caller's wait ends with no answer of the peer, the session is not sent under ({@link #timedOut}) until the peer
     * confirms it again.
     *
     * <p>O(1): one entry of the peer's own small table (at most {@value #CALLS_PER_PEER}; {@value #CALLS_MAX} in the
     * whole node, about 150 bytes each), under the peer's lock, which is held for no call into a broker.
     */
    void called(String peerNode, Session s, String callId, long timeoutMs) {
        if (callId == null || callId.length() > CALL_ID_MAX || timeoutMs <= 0 || s == null) return;
        Peer p = peerNode == null ? null : peers.getIfPresent(peerNode);
        if (p == null) return;
        long due = System.nanoTime() + Math.min(timeoutMs, lifetimeMs) * 1_000_000L;
        synchronized (p) {
            if (p.current != s) return;
            java.util.HashMap<String, Call> c = p.calls;
            if (c != null && c.containsKey(callId)) return;
            if (c != null && c.size() >= CALLS_PER_PEER) {
                callsNotWatched.incrementAndGet();
                return;
            }
            if (callsWatched.incrementAndGet() > CALLS_MAX) {
                callsWatched.decrementAndGet();
                callsNotWatched.incrementAndGet();
                return;
            }
            if (c == null) p.calls = c = new java.util.HashMap<>(4);
            c.put(callId, new Call(s, due));
            if (c.size() == 1 || due - p.callsDue < 0) p.callsDue = due;
            p.callsOpen = c.size();
        }
    }

    /** Is any call awaited from {@code peerNode}? One read; nothing is allocated. */
    boolean awaits(String peerNode) {
        Peer p = peerNode == null ? null : peers.getIfPresent(peerNode);
        return p != null && p.callsOpen != 0;
    }

    /** {@code peerNode} itself answered the call {@code callId} (whatever it answered): the call is no longer awaited. */
    void answered(String peerNode, String callId) {
        Peer p = peerNode == null || callId == null ? null : peers.getIfPresent(peerNode);
        if (p == null || p.callsOpen == 0) return;
        synchronized (p) {
            take(p, callId);
        }
    }

    /**
     * Another node answered this node's call {@code callId} to {@code peerNode}: there is no route to it
     * ({@code route unavailable}). The session the request left under is not sent under from now on, until the peer
     * confirms it again. True when it was a call this node awaited from that peer -- an answer that names any other call
     * changes nothing, so a node that is not on the way of a call (it never saw its id) cannot make this node sign.
     */
    boolean unanswered(String peerNode, String callId) {
        Peer p = peerNode == null || callId == null ? null : peers.getIfPresent(peerNode);
        if (p == null || p.callsOpen == 0) return false;
        synchronized (p) {
            Call c = take(p, callId);
            if (c == null) return false;
            Session cur = p.current;
            if (cur != null && cur == c.session) cur.confirmedAt = 0;
        }
        callsNoRoute.incrementAndGet();
        return true;
    }

    /** With {@code p}'s lock: the call {@code callId} is no longer awaited; what it was, or null. */
    private Call take(Peer p, String callId) {
        java.util.HashMap<String, Call> c = p.calls;
        Call was = c == null ? null : c.remove(callId);
        if (was == null) return null;
        callsWatched.decrementAndGet();
        if (c.isEmpty()) {
            p.calls = null;
            p.callsOpen = 0;
        } else {
            if (was.due == p.callsDue) p.callsDue = earliest(c);
            p.callsOpen = c.size();
        }
        return was;
    }

    private static long earliest(java.util.HashMap<String, Call> calls) {
        boolean first = true;
        long due = 0;
        for (Call c : calls.values()) {
            if (first || c.due - due < 0) due = c.due;
            first = false;
        }
        return due;
    }

    /**
     * The wait of a call to {@code p} has ended and the peer did not answer it: every such call goes from the table, and
     * the session it left under -- when it is still the one this node sends that peer under -- is not sent under until the
     * peer confirms it again ({@link #received}: its next message that names or uses the session). Seen when this node
     * next sends that peer anything ({@link #usable}), which is the first moment it matters: that message is signed.
     *
     * <p>A peer that is alive and merely slow loses nothing: it still holds the session, its next message says so, and
     * this node sends under it again; meanwhile what this node sends it is signed (a signature here, a verification
     * there, per message -- as it is whenever a peer has been silent for {@link #CONFIRM_MS}). A peer that restarted
     * verifies the signature, where it would have refused the MAC.
     */
    private void timedOut(Peer p) {
        long now = System.nanoTime();
        int n = 0;
        boolean aside = false;
        synchronized (p) {
            java.util.HashMap<String, Call> c = p.calls;
            if (c == null) {
                p.callsOpen = 0;
                return;
            }
            Session cur = p.current;
            for (java.util.Iterator<Call> it = c.values().iterator(); it.hasNext(); ) {
                Call call = it.next();
                if (now - call.due < 0) continue;
                it.remove();
                n++;
                if (cur != null && cur == call.session) {
                    cur.confirmedAt = 0;
                    aside = true;
                }
            }
            if (n > 0) callsWatched.addAndGet(-n);
            if (c.isEmpty()) {
                p.calls = null;
                p.callsOpen = 0;
            } else {
                p.callsDue = earliest(c);
                p.callsOpen = c.size();
            }
        }
        if (n > 0) callsTimedOut.addAndGet(n);
        java.util.function.Consumer<String> told = onTimedOut;
        if (aside && told != null) told.accept(p.node);
    }

    /**
     * The one link EVERY session of this node ran over is down (an agent's connection to its controller's broker): none
     * is sent under until its peer confirms it again after {@code now}. O(1): a time is noted, no session is walked.
     */
    void linkDown(long now) {
        if (now > unconfirmedUpTo) unconfirmedUpTo = now;
    }

    /**
     * An offer to attach for {@code peerNode} when this node holds no session with it, or it is due for renewal, and no
     * agreement is under way; or null. While a session is usable ({@code macUsable}) at most once a second (the message
     * carrying it is signed). The key pair is made outside any lock shared with other peers.
     */
    String offerFor(String peerNode, long now, boolean macUsable) throws Exception {
        Peer p = peer(peerNode, true);
        if (p == null || p.pending != null) return null;
        Session cur = p.current;
        if (cur != null && now < cur.expires - renewMs) return null;
        Offer o = p.offer;
        if (o == null || now - o.at >= OFFER_MS) {
            Offer fresh = new Offer(keyPair(), now);
            synchronized (p) {
                if (p.offer == o) p.offer = fresh;
                o = p.offer;
            }
            if (o == null) return null;
        }
        if (macUsable) {
            if (now - o.sentAt < ATTACH_EVERY_MS) return null;
            o.sentAt = now;
        }
        return b64(o.spki);
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp384r1"));         // the provider's own (approved) random source
        return g.generateKeyPair();
    }

    /** The MAC of a message {@code sender} sends under {@code key}: label, sender, canonical form. */
    static byte[] mac(SecretKeySpec key, String sender, byte[] canonical) throws Exception {
        Mac m = HMAC.get();
        m.init(key);
        m.update(MAC_LABEL);
        m.update((byte) 0);
        m.update(sender.getBytes(StandardCharsets.UTF_8));
        m.update((byte) 0);
        return m.doFinal(canonical);
    }

    // ------------------------------------------------------------------ receiving

    /**
     * The session {@code sid} this node holds with the node {@code peerNode}, or null: found by the NODE and the session
     * id, never by a name the message carries. Whether the message's signer is the name the session is bound to
     * ({@link Peer#name}) is for the caller to judge, and a mismatch there is a refusal of a message under a session this
     * node HOLDS -- it must never be reported as a session it does not hold (a forged signer name with a genuine session
     * id would otherwise make the holder say "not held", and the true peer drop a good session).
     */
    Found find(String peerNode, String sid) {
        if (peerNode == null || sid == null) return null;
        Peer p = peers.getIfPresent(peerNode);
        if (p == null) return null;
        Session s = p.current;
        if (s == null || !s.sid.equals(sid)) s = p.pending;
        if (s == null || !s.sid.equals(sid)) s = p.previous;
        return s == null || !s.sid.equals(sid) ? null : new Found(p, s);
    }

    /** As {@link #find}, and only when the session is bound to the name {@code by} (tests). */
    Found lookup(String peerNode, String by, String sid) {
        Found f = find(peerNode, sid);
        return f != null && by.equals(f.peer.name) ? f : null;
    }

    /**
     * {@code s} has ended (its time passed): from now on nothing is accepted or sent under it. It is KEPT, as "ended at
     * this instant", so that this node can say for how long it has refused it. Returns that instant.
     */
    long ended(Session s) {
        long at = s.endedNanos;
        if (at == 0) {
            synchronized (s) {
                at = s.endedNanos;
                if (at == 0) {
                    at = System.nanoTime();
                    s.endedNanos = at == 0 ? 1 : at;
                }
            }
        }
        return s.endedNanos;
    }

    /**
     * Since when ({@link System#nanoTime}) this node has certainly not held ANY session with {@code peerNode} other than
     * those in its entry now: the latest of this table's start, the last time an entry was put out of it, and the last
     * time that peer's entry let go of a session. A session id of that node that is in none of its slots was not held at
     * any moment since -- which is all this node can truthfully say of a session it has no record of. For a node with no
     * entry at all the answer is at most {@link #idleNanos} ago: an entry that went for being idle is not announced.
     * Monotonic time only; O(1).
     */
    long notHeldSince(String peerNode) {
        long now = System.nanoTime(), since = startedNanos, e = evictedNanos;
        if (e - since > 0) since = e;
        Peer p = peerNode == null ? null : peers.getIfPresent(peerNode);
        if (p != null) {
            long f = p.forgotNanos;
            if (f - since > 0) since = f;
        } else if ((now - idleNanos) - since > 0) {
            since = now - idleNanos;
        }
        return since;
    }

    /** With {@code p}'s lock, after its slots changed: a session that was in one of them and is in none now is forgotten. */
    private static void settle(Peer p, Session c, Session n, Session v) {
        Session c1 = p.current, n1 = p.pending, v1 = p.previous;
        if ((c != null && c != c1 && c != n1 && c != v1) || (n != null && n != c1 && n != n1 && n != v1)
                || (v != null && v != c1 && v != n1 && v != v1)) {
            p.forgotNanos = System.nanoTime();
        }
    }

    /** Forget every session held with {@code peerNode} (its credential no longer verifies, or its key was revoked). */
    void dropPeer(String peerNode) {
        Peer p = peers.getIfPresent(peerNode);
        if (p == null) return;
        synchronized (p) {
            Session c = p.current, n = p.pending, v = p.previous;
            p.current = p.pending = p.previous = null;
            p.signer = null;
            settle(p, c, n, v);
        }
    }

    /**
     * Forget every session held with {@code peerNode} when the signer its MACs are checked against is {@code signer} (that
     * signer's name, or the controller that attested it, got another key): true when it was. O(1).
     */
    boolean dropIfSigner(String peerNode, MsgSigning.Signer signer) {
        Peer p = peerNode == null ? null : peers.getIfPresent(peerNode);
        if (p == null) return false;
        synchronized (p) {
            if (p.signer != signer) return false;
            Session c = p.current, n = p.pending, v = p.previous;
            p.current = p.pending = p.previous = null;
            p.signer = null;
            settle(p, c, n, v);
        }
        return true;
    }

    /**
     * The peer {@code by} (node {@code peerNode}) said, on a message this node authenticated by its SIGNATURE, that it does
     * not hold the session {@code sid}. When that is the session this node sends to it under, it is no longer: it moves to
     * the previous slot (what the peer sent under it before it lost it still verifies) and this node signs to that peer
     * again, offering a new session on its next message. True when it was the current session.
     *
     * <p>Only the CURRENT session, and only by its id: a notice that names any other session -- one this node already
     * replaced (a notice delivered late, or replayed), one it never held, a renewal still pending -- changes nothing. A
     * peer speaks for the sessions it shares with this node and no others: the entry is found by the node the signature
     * verified as, never by a name the message merely carries.
     */
    boolean lostAtPeer(String peerNode, String by, String sid) {
        Peer p = peerNode == null || sid == null ? null : peers.getIfPresent(peerNode);
        if (p == null) return false;
        synchronized (p) {
            Session cur = p.current;
            if (cur == null || !by.equals(p.name) || !cur.sid.equals(sid)) return false;
            Session v = p.previous;
            p.previous = cur;
            p.current = null;
            settle(p, cur, null, v);
            return true;
        }
    }

    /** Is {@code sid} the session this node sends to {@code by} (node {@code peerNode}) under now? Reads only. */
    boolean isCurrent(String peerNode, String by, String sid) {
        Peer p = peerNode == null || sid == null ? null : peers.getIfPresent(peerNode);
        Session cur = p == null ? null : p.current;
        return cur != null && by.equals(p.name) && cur.sid.equals(sid);
    }

    /** Forget the sessions of every peer {@code refused} says no (a revocation came in force); how many. */
    int dropWhere(Predicate<Peer> refused) {
        int n = 0;
        for (Peer p : peers.asMap().values()) {
            if (p.signer != null && refused.test(p)) {
                dropPeer(p.node);
                n++;
            }
        }
        return n;
    }

    /** Forget every session and take a new incarnation (tests: as a restart does). */
    void restart() {
        peers.invalidateAll();
        incarnation = newIncarnation();
        startedNanos = evictedNanos = System.nanoTime();
    }

    private static String newIncarnation() {
        byte[] inc = new byte[12];
        io.cresco.library.crypto.CrescoCrypto.secureRandom().nextBytes(inc);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(inc);
    }

    /** Peers held (diagnostics, tests). */
    long size() { return peers.size(); }

    /**
     * After a message from {@code by} (node {@code peerNode}, incarnation {@code inc}) was authenticated here as
     * {@code signer} -- {@code signed}: by a signature (so carrying its current credential), else under session
     * {@code usedSid} -- take its word on the session it holds with this node, its answer to this node's offer and its
     * offer. Returns what changed, for the log (or null).
     */
    String received(String self, String by, String peerNode, String inc, MsgSigning.Signer signer, String kx, String kxAck,
                    String held, String usedSid, boolean signed, long now) throws Exception {
        Peer p = peer(peerNode, signed && inc != null && (kx != null || kxAck != null));
        if (p == null) return null;
        String changed = null;
        synchronized (p) {
            final Session c0 = p.current, n0 = p.pending, v0 = p.previous;
            if (p.name == null || !p.name.equals(by)) {
                // a node of that address authenticating under another name holds nothing of the one before
                if (p.name != null) { p.current = p.pending = p.previous = null; p.offer = null; }
                p.name = by;
            }
            if (signed && signer != null) {
                MsgSigning.Signer had = p.signer;
                if (had != null && had != signer && !had.fp.equals(signer.fp)) {
                    // the name authenticates with another key: what was agreed with the key before is not this peer's
                    p.current = p.pending = p.previous = null;
                    p.offer = null;
                }
                p.signer = signer;
            }
            if (inc != null && p.inc != null && !inc.equals(p.inc)) {
                // the peer restarted: it holds none of this node's sessions any more (what it sent before still verifies)
                if (p.current != null) p.previous = p.current;
                p.current = null;
                p.pending = null;
                changed = "lost";
            }
            if (inc != null) p.inc = inc;
            if (signed && kxAck != null && inc != null) {
                String c = onAck(p, self, by, kxAck, now);
                if (c != null) changed = c;
            }
            if (signed && kx != null && inc != null) {
                String c = onOffer(p, self, by, kx, held, now);
                if (c != null) changed = c;
            }
            Session pend = p.pending;
            if (pend != null && (pend.sid.equals(held) || pend.sid.equals(usedSid))) {
                // the peer holds the new session (it named it, or sent under it): it replaces the one before
                p.previous = p.current;
                p.current = pend;
                p.pending = null;
                pend.confirmedAt = now;
                pend.ack = null;
            } else {
                Session cur = p.current;
                if (cur != null) {
                    if (cur.sid.equals(held) || cur.sid.equals(usedSid)) {
                        cur.confirmedAt = now;
                        cur.ack = null;
                    } else if (held == null || p.previous == null || !p.previous.sid.equals(held)) {
                        // it does not hold this node's session (it lost it, or never took it): never send under it unconfirmed
                        cur.confirmedAt = 0;
                    }
                }
            }
            settle(p, c0, n0, v0);
        }
        return changed;
    }

    /** With {@code p}'s lock: the peer answered this node's open offer. */
    private String onAck(Peer p, String self, String by, String kxAck, long now) throws Exception {
        String[] parts = kxAck.split(":", -1);
        if (parts.length != 3) return null;
        Offer o = p.offer;
        if (o == null || !o.digest.equals(parts[1])) return null;          // not an answer to this node's open offer
        byte[] theirs = Base64.getDecoder().decode(parts[2]);
        String sid = sid(o.spki, theirs);
        if (!sid.equals(parts[0])) return null;
        byte[] key = derive(agree(o.eph, theirs), self, by, o.spki, theirs);
        // its end counts from the OFFER, not from the answer: the acceptor counts from when it took the offer, which is
        // later, so this node (which sends under it) always stops before the acceptor's end -- however late the answer came
        Session s = new Session(sid, key, o.digest, o.at + lifetimeMs);
        s.confirmedAt = now;                                                // its answer says it holds the session
        if (p.current != null) p.previous = p.current;
        p.current = s;
        p.pending = null;
        p.offer = null;
        return "established";
    }

    /** With {@code p}'s lock: the peer offered a session (its message names {@code held} as the session it holds). */
    private String onOffer(Peer p, String self, String by, String kx, String held, long now) throws Exception {
        byte[] theirs = Base64.getDecoder().decode(kx);
        String digest = b64(sha256(theirs));
        Session cur = p.current, pend = p.pending;
        if ((cur != null && digest.equals(cur.offerDigest)) || (pend != null && digest.equals(pend.offerDigest))) return null;
        if (now - p.lastOfferAccepted < OFFER_ACCEPT_MIN_MS) return null;  // one agreement per peer per interval at most
        Offer mine = p.offer;
        if (mine != null && now - mine.at < OFFER_MS) {
            if (self.compareTo(by) < 0) return null;                        // both offered: the lesser name's offer stands
            p.offer = null;
        }
        KeyPair eph = keyPair();
        byte[] ours = eph.getPublic().getEncoded();
        String sid = sid(theirs, ours);
        byte[] key = derive(agree(eph, theirs), by, self, theirs, ours);
        Session s = new Session(sid, key, digest, now + lifetimeMs);
        s.ack = sid + ":" + digest + ":" + b64(ours);
        p.lastOfferAccepted = now;
        offersAccepted.incrementAndGet();
        if (cur != null && cur.confirmedAt > 0 && cur.sid.equals(held) && now < cur.expires - USABLE_MARGIN_MS) {
            p.pending = s;                                                  // a renewal: keep the confirmed one until it moves
        } else {
            if (cur != null) p.previous = cur;
            p.current = s;
            p.pending = null;
        }
        return "accepted";
    }

    // ------------------------------------------------------------------ the agreement

    private static byte[] agree(KeyPair own, byte[] theirSpki) throws Exception {
        PublicKey theirs = KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(theirSpki));
        if (!(theirs instanceof java.security.interfaces.ECPublicKey)
                || ((java.security.interfaces.ECPublicKey) theirs).getParams().getCurve().getField().getFieldSize() != 384) {
            throw new IllegalArgumentException("not a P-384 key");
        }
        KeyAgreement ka = KeyAgreement.getInstance("ECDH");
        ka.init(own.getPrivate());
        ka.doPhase(theirs, true);
        return ka.generateSecret();
    }

    /** SP 800-56C two-step: PRK = HMAC(salt, Z); key = HMAC(PRK, offerer || 0 || acceptor || 0 || offer key || answer key || 1). */
    static byte[] derive(byte[] z, String offerer, String acceptor, byte[] offerSpki, byte[] answerSpki) throws Exception {
        Mac m = Mac.getInstance("HmacSHA384");
        m.init(new SecretKeySpec(sha384(KDF_SALT_LABEL), "HmacSHA384"));
        byte[] prk = m.doFinal(z);
        java.util.Arrays.fill(z, (byte) 0);
        m.init(new SecretKeySpec(prk, "HmacSHA384"));
        m.update(offerer.getBytes(StandardCharsets.UTF_8));
        m.update((byte) 0);
        m.update(acceptor.getBytes(StandardCharsets.UTF_8));
        m.update((byte) 0);
        m.update(offerSpki);
        m.update(answerSpki);
        m.update((byte) 1);
        return m.doFinal();
    }

    static String sid(byte[] offerSpki, byte[] answerSpki) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(offerSpki);
            byte[] d = md.digest(answerSpki);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(java.util.Arrays.copyOf(d, 18));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] sha256(byte[] b) {
        try { return MessageDigest.getInstance("SHA-256").digest(b); } catch (Exception e) { throw new IllegalStateException(e); }
    }

    private static byte[] sha384(byte[] b) {
        try { return MessageDigest.getInstance("SHA-384").digest(b); } catch (Exception e) { throw new IllegalStateException(e); }
    }

    private static String b64(byte[] b) { return Base64.getEncoder().encodeToString(b); }
}
