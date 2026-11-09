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
import java.util.Base64;
import java.time.Duration;

/**
 * End-to-end session keys between two nodes, for the MsgEvents that pass a relay. A signature costs its origin about 2 ms
 * of one core (RSA-3072), so a request and its reply across another region's bridge paid two of them on the critical path:
 * 5.9 ms instead of 1.1 ms. Here two nodes that talk agree, once, on a key only they hold, and then authenticate each
 * message with an HMAC under it (about a microsecond): a relay still cannot forge, alter or replay what passes through it.
 *
 * <p>The agreement rides on messages already authenticated (signed, or under the previous session): the offerer attaches
 * an ephemeral P-384 key ({@value #KX}); the acceptor its own ephemeral key and the session id ({@value #KX_ACK}). Each
 * derives the key from the ECDH secret (SP 800-56A, ephemeral-ephemeral) with HMAC-SHA-384 (SP 800-56C two-step: extract
 * under a fixed salt, expand over both names and both public keys). One session serves both directions: the MAC covers
 * the sender's name and the message's canonical form.
 *
 * <p>A node sends under a session only while its peer said, within the last {@value #CONFIRM_MS} ms, that it still holds
 * it: every message to a peer carries the session this node holds with it ({@value #HELD}). Every message also names its
 * sender's incarnation ({@value #INC}, random at each start): a peer that restarted (and so lost its sessions) names a new
 * one, and its next message makes this node return to signatures and offer anew; what was sent to it meanwhile under the
 * lost session is refused like any unauthenticated message. Sessions live
 * {@code msg_signing_session_seconds} (default 3600) and are renewed ahead of their end; all state is bounded caches, O(1)
 * per message.
 */
final class MsgSessions {

    static final String KX = "cresco_sig_kx", KX_ACK = "cresco_sig_kxa", HELD = "cresco_sig_held", SID = "cresco_sig_sid",
            MAC = "cresco_sig_mac", INC = "cresco_sig_inc";
    private static final byte[] KDF_SALT_LABEL = "cresco-msg-session-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAC_LABEL = "cresco-msg-mac-v1".getBytes(StandardCharsets.UTF_8);
    static final long CONFIRM_MS = 30_000L, OFFER_MS = 30_000L;
    static final int MAX_PEERS = 200_000;

    /** A session with one peer. */
    static final class Session {
        final String sid, peer, peerNode, peerInc, cred, fp, offerDigest;
        final SecretKeySpec key;
        final long expires;
        /** When the peer last said it holds this session (this node's clock). */
        volatile long confirmedAt;
        /** The acceptor's answer, attached to its messages to the offerer until the offerer confirms. */
        volatile String ack;

        Session(String sid, byte[] key, String peer, String peerNode, String peerInc, String cred, String fp, String offerDigest, long expires) {
            this.sid = sid;
            this.key = new SecretKeySpec(key, "HmacSHA384");
            this.peer = peer;
            this.peerNode = peerNode;
            this.peerInc = peerInc;
            this.cred = cred;
            this.fp = fp;
            this.offerDigest = offerDigest;
            this.expires = expires;
        }
    }

    private static final class Offer {
        final KeyPair eph;
        final byte[] spki;
        final String digest;
        final long at;
        Offer(KeyPair eph, long at) {
            this.eph = eph;
            this.spki = eph.getPublic().getEncoded();
            this.digest = b64(sha256(spki));
            this.at = at;
        }
    }

    private final long lifetimeMs, renewMs;
    /** This node's incarnation: random at each start. */
    volatile String incarnation;
    private final Cache<String, Session> bySid, byPeer;
    private final Cache<String, Offer> offers;
    private static final ThreadLocal<Mac> HMAC = ThreadLocal.withInitial(() -> {
        try { return Mac.getInstance("HmacSHA384"); } catch (Exception e) { throw new IllegalStateException(e); }
    });

    MsgSessions(long lifetimeMs) {
        this.lifetimeMs = Math.max(120_000L, lifetimeMs);
        this.renewMs = Math.min(300_000L, this.lifetimeMs / 10);
        this.incarnation = newIncarnation();
        this.bySid = CacheBuilder.newBuilder().maximumSize(2L * MAX_PEERS).expireAfterWrite(Duration.ofMillis(this.lifetimeMs + 60_000L)).build();
        this.byPeer = CacheBuilder.newBuilder().maximumSize(MAX_PEERS).expireAfterWrite(Duration.ofMillis(this.lifetimeMs)).build();
        this.offers = CacheBuilder.newBuilder().maximumSize(MAX_PEERS).expireAfterWrite(Duration.ofMillis(OFFER_MS)).build();
    }

    // ------------------------------------------------------------------ sending

    /** The session this node holds with {@code peerNode} (region_agent), to tell it so; or null. */
    String heldWith(String peerNode) {
        Session s = byPeer.getIfPresent(peerNode);
        return s == null ? null : s.sid;
    }

    /** This node's answer to {@code peerNode}'s offer, while the peer has not confirmed it; or null. */
    String ackFor(String peerNode) {
        Session s = byPeer.getIfPresent(peerNode);
        return s == null ? null : s.ack;
    }

    /** The session to send to {@code peerNode} under: the peer confirmed it lately and it is not about to end; or null. */
    Session usable(String peerNode, long now) {
        Session s = byPeer.getIfPresent(peerNode);
        return s != null && s.confirmedAt > 0 && now - s.confirmedAt < CONFIRM_MS && now < s.expires - 60_000L ? s : null;
    }

    /** An offer to attach for {@code peerNode} when this node holds no session with it or it is due for renewal; or null. */
    String offerFor(String peerNode, long now) throws Exception {
        Session s = byPeer.getIfPresent(peerNode);
        if (s != null && now < s.expires - renewMs) return null;
        Offer o = offers.getIfPresent(peerNode);
        if (o == null || now - o.at >= OFFER_MS) {
            synchronized (this) {                                         // one open offer per peer
                o = offers.getIfPresent(peerNode);
                if (o == null || now - o.at >= OFFER_MS) {
                    KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
                    g.initialize(new ECGenParameterSpec("secp384r1"));  // the provider's own (approved) random source
                    o = new Offer(g.generateKeyPair(), now);
                    offers.put(peerNode, o);
                }
            }
        }
        return b64(o.spki);
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

    /** The session a message names, or null. */
    Session bySid(String sid) { return sid == null ? null : bySid.getIfPresent(sid); }

    /** Forget a session (its peer's credential no longer verifies, or its key was revoked). */
    void drop(Session s) {
        bySid.invalidate(s.sid);
        byPeer.asMap().remove(s.peerNode, s);
    }

    /** Forget every session and take a new incarnation (tests: as a restart does). */
    void restart() {
        bySid.invalidateAll();
        byPeer.invalidateAll();
        offers.invalidateAll();
        incarnation = newIncarnation();
    }

    private static String newIncarnation() {
        byte[] inc = new byte[12];
        io.cresco.library.crypto.CrescoCrypto.secureRandom().nextBytes(inc);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(inc);
    }

    long size() { return bySid.size(); }

    /**
     * After a message from {@code peer} ({@code peerNode}, incarnation {@code inc}, credential {@code cred} of key
     * {@code fp}) was authenticated here: take its word on the session it holds with this node, its answer to this node's
     * offer, and its offer. Returns what changed, for the log (or null).
     */
    String received(String self, String peer, String peerNode, String inc, String cred, String fp, String kx, String kxAck,
                    String held, long now) throws Exception {
        String changed = null;
        Session s = byPeer.getIfPresent(peerNode);
        if (s != null && inc != null && !inc.equals(s.peerInc)) {
            // the peer restarted: it holds none of this node's sessions any more (what it sent before still verifies)
            byPeer.asMap().remove(peerNode, s);
            changed = "lost";
        }
        if (kxAck != null && inc != null) {
            String c = onAck(self, peer, peerNode, inc, cred, fp, kxAck, now);
            if (c != null) changed = c;
        }
        if (kx != null && inc != null) {
            String c = onOffer(self, peer, peerNode, inc, cred, fp, kx, now);
            if (c != null) changed = c;
        }
        s = byPeer.getIfPresent(peerNode);
        if (s != null && s.sid.equals(held)) {
            s.confirmedAt = now;
            s.ack = null;
        }
        return changed;
    }

    private synchronized String onAck(String self, String peer, String peerNode, String inc, String cred, String fp, String kxAck, long now)
            throws Exception {
        String[] p = kxAck.split(":", -1);
        if (p.length != 3) return null;
        Offer o = offers.getIfPresent(peerNode);
        if (o == null || !o.digest.equals(p[1])) return null;              // not an answer to this node's open offer
        byte[] theirs = Base64.getDecoder().decode(p[2]);
        String sid = sid(o.spki, theirs);
        if (!sid.equals(p[0])) return null;
        byte[] key = derive(agree(o.eph, theirs), self, peer, o.spki, theirs);
        Session s = new Session(sid, key, peer, peerNode, inc, cred, fp, o.digest, now + lifetimeMs);
        s.confirmedAt = now;                                                // its answer says it holds the session
        bySid.put(sid, s);
        byPeer.put(peerNode, s);
        offers.invalidate(peerNode);
        return "established";
    }

    private synchronized String onOffer(String self, String peer, String peerNode, String inc, String cred, String fp, String kx, long now)
            throws Exception {
        byte[] theirs = Base64.getDecoder().decode(kx);
        String digest = b64(sha256(theirs));
        Session prev = byPeer.getIfPresent(peerNode);
        if (prev != null && digest.equals(prev.offerDigest)) return null;  // answered already
        Offer mine = offers.getIfPresent(peerNode);
        if (mine != null && now - mine.at < OFFER_MS) {
            if (self.compareTo(peer) < 0) return null;                      // both offered: the lesser name's offer stands
            offers.invalidate(peerNode);
        }
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp384r1"));
        KeyPair eph = g.generateKeyPair();
        byte[] ours = eph.getPublic().getEncoded();
        String sid = sid(theirs, ours);
        byte[] key = derive(agree(eph, theirs), peer, self, theirs, ours);
        Session s = new Session(sid, key, peer, peerNode, inc, cred, fp, digest, now + lifetimeMs);
        s.ack = sid + ":" + digest + ":" + b64(ours);
        if (prev != null && prev.confirmedAt == 0) bySid.invalidate(prev.sid); // never used: no message can be under it
        bySid.put(sid, s);
        byPeer.put(peerNode, s);
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
