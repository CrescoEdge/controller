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
 * refused like any unauthenticated message.
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
        Peer(String node) { this.node = node; }
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
        this.peers = CacheBuilder.newBuilder().maximumSize(this.maxPeers + this.maxPeers / 8 + 16)
                .expireAfterAccess(Duration.ofMillis(this.lifetimeMs + 120_000L)).build();
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

    /** The session to send to {@code peerNode} under: the peer confirmed it lately and it is not about to end; or null. */
    Session usable(String peerNode, long now) {
        Peer p = peers.getIfPresent(peerNode);
        Session s = p == null ? null : p.current;
        return s != null && s.confirmedAt > 0 && now - s.confirmedAt < CONFIRM_MS && now < s.expires - USABLE_MARGIN_MS ? s : null;
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

    /** The session {@code sid} that {@code by} (of node {@code peerNode}) holds with this node, or null. */
    Found lookup(String peerNode, String by, String sid) {
        if (peerNode == null || sid == null) return null;
        Peer p = peers.getIfPresent(peerNode);
        if (p == null || !by.equals(p.name)) return null;
        Session s = p.current;
        if (s == null || !s.sid.equals(sid)) s = p.pending;
        if (s == null || !s.sid.equals(sid)) s = p.previous;
        return s == null || !s.sid.equals(sid) ? null : new Found(p, s);
    }

    /** Forget every session held with {@code peerNode} (its credential no longer verifies, or its key was revoked). */
    void dropPeer(String peerNode) {
        Peer p = peers.getIfPresent(peerNode);
        if (p == null) return;
        synchronized (p) {
            p.current = p.pending = p.previous = null;
            p.signer = null;
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
            p.current = p.pending = p.previous = null;
            p.signer = null;
        }
        return true;
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
        Session s = new Session(sid, key, o.digest, now + lifetimeMs);
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
