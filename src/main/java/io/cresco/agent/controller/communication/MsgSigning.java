package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * ({@code cresco_caller*}, including the alias) and the signature itself. The destination header is not signed
 * ({@code region_alias} and the controller tiers rewrite it). The origin's caller is signed as a copy
 * ({@code cresco_sig_caller*}) next to the signer ({@code cresco_sig_by}), its credential ({@code cresco_sig_cred}) and
 * the signing time ({@code cresco_sig_ts}); {@code cresco_tenant} is set when the message is stamped, so it is signed.
 *
 * <p><b>Who signed.</b> A controller (region or global) signs with its leaf certificate as its credential; an agent with
 * an <i>attestation</i> its region controller issued (its tenant, region, name and public key, valid
 * {@code msg_signing_attestation_hours}, default 24) once it holds one, and with its certificate before. A receiver
 * resolves a credential, in this order: a certificate that verifies HERE as its name (a peer bound here, or under a
 * recorded region CA); a certificate whose key is the one the <i>trust bundle</i> lists for that controller; an
 * attestation signed by a controller the trust bundle lists for the attested region (or by this node). The trust bundle
 * is the list of region and global controller keys: the global builds it from the certificates its regions present on
 * their pings, signs it, and sends it down on its pongs; each region re-signs what it verified for its agents, as
 * revocation lists travel (RevocationSync). A revoked key never verifies. Credentials are verified once and cached, so a
 * message costs one hash, one map lookup and one signature check: O(1) in the size of the fleet.
 *
 * <p><b>What the receiver does.</b> With {@code broker_security_enabled}: an unsigned message, or one whose signature,
 * credential, age ({@code msg_signature_max_age_ms}, default 5 min, either way) or freshness (a signature seen before)
 * fails, is refused (an RPC request is answered 401). The signer must be the node of the signed caller (or, for an API
 * user, a node of the user's tenant -- any tenant's for a superuser tenant), and the broker's stamp, when there is one,
 * must be that caller: a bridge that stripped a stamp it could not vouch for no longer cuts the origin's word.
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
 * to a parent or child that said it accepts it (a node of an earlier hardened build is always sent signatures).
 */
public final class MsgSigning {

    public static final String SIG = "cresco_sig", BY = "cresco_sig_by", TS = "cresco_sig_ts", CRED = "cresco_sig_cred",
            PENDING = "cresco_sig_pending", S_CALLER = "cresco_sig_caller", S_TENANT = "cresco_sig_caller_tenant",
            S_ROLES = "cresco_sig_caller_roles";

    /** Ping/pong parameters: a child's certificate and what it holds; the parent's attestation and trust bundle. */
    static final String P_CERT = "sig_cert", P_ATT_EXP = "sig_att_exp", P_ATT = "sig_att", P_BUNDLE_HAVE = "sig_bundle_have",
            P_BUNDLE = "sig_bundle";
    /** On a ping and a pong: this node accepts unsigned messages its broker attests first hand (direct links). */
    static final String P_DIRECT_OK = "sig_direct_ok";
    /** On a message this node originates and sends unsigned over a direct link: its own path (copied to {@link CallerStamps#P_ORIGIN}). */
    public static final String DIRECT = "cresco_sig_direct";
    /** How long a parent's or child's offer of direct links holds without a new ping (pings come every few seconds). */
    static final long DIRECT_OFFER_MS = 60_000L;
    static final int DIRECT_CHILDREN_MAX = 200_000;

    private static final String VERSION = "cresco-msg-v1";
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
    private final long maxAgeMs;
    private final long attestationMs;

    // ---- this node's credential
    private volatile String ownCertB64;
    private volatile String ownAttestation;
    private volatile long ownAttestationExp;

    // ---- the trust bundle this node holds: region/agent -> entry (key fingerprint, tenant, public key)
    private final ConcurrentHashMap<String, BundleEntry> bundle = new ConcurrentHashMap<>();
    private volatile String heldBundleDigest = "";
    // ---- at a global: the region controllers' certificates learned from their pings
    private final ConcurrentHashMap<String, BundleEntry> childControllers = new ConcurrentHashMap<>();
    private volatile String servedBundle, servedDigest = "";
    private volatile String servedVersionKey = "";

    // ---- verification caches (bounded)
    private final ConcurrentHashMap<String, Resolved> credCache = new ConcurrentHashMap<>();
    /**
     * The signatures seen, in three generations of {@code msg_signature_max_age_ms} each: a signature is remembered for two
     * to three max-ages after it arrived, longer than it can be accepted (its signing time is within one max-age of the
     * receiver's clock either way). Rotating a generation drops a whole map: no scan, O(1) per message.
     */
    private volatile Set<String> seenNow = ConcurrentHashMap.newKeySet(), seenBefore = ConcurrentHashMap.newKeySet(),
            seenEarlier = ConcurrentHashMap.newKeySet();
    private volatile long seenSince = System.currentTimeMillis();
    /** At most this many signatures per generation; a busier generation is rotated early (logged: the window shortens). */
    static final int SEEN_GENERATION_MAX = 1_000_000;
    private final AtomicLong signedCount = new AtomicLong(), verifiedCount = new AtomicLong(), refusedCount = new AtomicLong();
    private volatile long lastRefusalLog, lastEarlyRotationLog;

    private final ThreadLocal<MessageDigest> sha256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new IllegalStateException(e); }
    });

    static final class BundleEntry {
        String t, r, a, k, role;            // tenant, region, agent, public key (X.509 SPKI, base64), global|region
        transient PublicKey key;
        transient String fp;
        transient long keepUntil;           // a region keeps an entry its global dropped for a grace period
    }

    private static final class SignedBundle {
        int v = 1;
        String issuer, tenant;
        long issued;
        List<BundleEntry> entries;
        String cert, sig;
    }

    private static final class Attestation {
        int v = 1;
        String t, r, a, k, iss;
        long exp;
        String sig;
    }

    private static final class Resolved {
        final CrescoIdentity id;
        final PublicKey key;
        final String fp;
        final long until;
        Resolved(CrescoIdentity id, PublicKey key, String fp, long until) { this.id = id; this.key = key; this.fp = fp; this.until = until; }
    }

    public MsgSigning(ControllerEngine ce) {
        this.ce = ce;
        io.cresco.library.plugin.Config c = ce.getPluginBuilder().getConfig();
        this.required = SecuritySettings.production(c)
                || (SecuritySettings.brokerSecurity(c) && c.getBooleanParam("msg_signing_required", true));
        this.maxAgeMs = Math.max(10_000L, c.getLongParam("msg_signature_max_age_ms", 300_000L));
        this.attestationMs = Math.max(1L, c.getLongParam("msg_signing_attestation_hours", 24L)) * 3_600_000L;
        this.direct = c.getBooleanParam("msg_signing_direct", true);
    }

    /** Messages sent unsigned over a direct link, and accepted so (diagnostics, tests). */
    public long directSentCount() { return directSent.get(); }
    public long directAcceptedCount() { return directAccepted.get(); }

    /**
     * Is {@code m} addressed to this node's parent or one of its children over their direct link, which said it accepts
     * unsigned messages attested first hand by the broker? (O(1): one comparison, one map lookup.)
     */
    boolean directTo(MsgEvent m) {
        if (!direct || m.getDstRegion() == null || m.getDstAgent() == null) return false;
        String dst = m.getDstRegion() + "_" + m.getDstAgent();
        long now = System.currentTimeMillis();
        if (dst.equals(directParent) && now < directParentUntil) return true;
        Long until = directChildren.get(dst);
        return until != null && now < until;
    }

    /** Signing and verification are on (security on and not turned off with {@code msg_signing_required=false}). */
    public boolean required() { return required; }

    public long signedCount() { return signedCount.get(); }
    public long verifiedCount() { return verifiedCount.get(); }
    public long refusedCount() { return refusedCount.get(); }

    // ================================================================ canonical form

    /** Is {@code key} covered by the signature? Not what infrastructure writes in transit, nor the signature itself. */
    static boolean covered(String key) {
        return key != null && !key.startsWith("routepath-") && !key.startsWith("linkcost-") && !key.startsWith("srcroute")
                && !key.equals("ttl") && !key.equals(SIG) && !key.equals(PENDING) && !Caller.RESERVED_PARAMS.contains(key);
    }

    /** The bytes a signature covers (length-prefixed, parameters in key order; a parameter without a value is absent). */
    static byte[] canonical(MsgEvent m) {
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream(512);
            DataOutputStream o = new DataOutputStream(bo);
            str(o, VERSION);
            str(o, m.getMsgType() == null ? null : m.getMsgType().name());
            str(o, m.getSrcRegion());
            str(o, m.getSrcAgent());
            str(o, m.getSrcPlugin());
            TreeMap<String, String> p = new TreeMap<>();
            if (m.getParams() != null) {
                for (Map.Entry<String, String> e : m.getParams().entrySet()) {
                    if (e.getValue() != null && covered(e.getKey())) p.put(e.getKey(), e.getValue());
                }
            }
            o.writeInt(p.size());
            for (Map.Entry<String, String> e : p.entrySet()) {
                str(o, e.getKey());
                str(o, e.getValue());
            }
            o.flush();
            return bo.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void str(DataOutputStream o, String s) throws IOException {
        if (s == null) { o.writeInt(-1); return; }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        o.writeInt(b.length);
        o.write(b);
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

    /** Remove every signature parameter. */
    public static void clear(MsgEvent m) {
        if (m == null || m.getParams() == null) return;
        for (String k : new String[]{SIG, BY, TS, CRED, PENDING, S_CALLER, S_TENANT, S_ROLES, DIRECT}) m.removeParam(k);
    }

    /** A message off the wire never asks THIS node to sign it: only this node's own stamping points do. */
    public static void notPending(MsgEvent m) {
        if (m != null && m.getParams() != null) {
            m.removeParam(PENDING);
            m.removeParam(DIRECT);               // a sender's word that it is the origin counts only as its broker attests it
        }
    }

    /** As a message this node originated leaves it (AgentProducer): sign it. Relayed messages carry their origin's signature. */
    public void signIfPending(MsgEvent m) {
        if (m == null || m.getParams() == null || m.getParam(PENDING) == null) return;
        m.removeParam(PENDING);
        if (!required) return;
        Caller c = Caller.fromMsgEvent(m);
        if (c == null || c.isAnonymous()) return;
        if (directTo(m)) {
            // a direct link: the receiver's broker attests this node first hand (CallerStamps.P_DIRECT); no signature
            m.setParam(DIRECT, ce.cstate.getRegion() + "_" + ce.cstate.getAgent());
            if (directSent.incrementAndGet() == 1) info("direct links ON: messages to this node's parent and children that accept them"
                    + " travel unsigned, attested first hand by the receiving broker (first: " + m.getMsgType() + " " + m.getParam("action")
                    + " to " + m.getDstRegion() + "/" + m.getDstAgent() + ")");
            return;
        }
        try {
            m.setParam(S_CALLER, c.getPrincipal());
            m.setParam(S_TENANT, c.getTenant());
            m.setParam(S_ROLES, c.getRolesCsv());
            m.setParam(BY, selfName());
            m.setParam(TS, Long.toString(System.currentTimeMillis()));
            m.setParam(CRED, credential());
            Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(ownKey().getAlgorithm()));
            s.initSign(ownKey());
            s.update(canonical(m));
            m.setParam(SIG, Base64.getEncoder().encodeToString(s.sign()));
            if (signedCount.incrementAndGet() == 1) info("message signatures ON: this node signs what it originates (first: "
                    + m.getMsgType() + " " + m.getParam("action") + " as " + c + ", credential " + m.getParam(CRED).substring(0, 4) + ")");
        } catch (Exception e) {
            for (String k : new String[]{SIG, BY, TS, CRED, S_CALLER, S_TENANT, S_ROLES}) m.removeParam(k);
            refusal("cannot sign a message this node sends (" + e.getClass().getSimpleName() + ": " + e.getMessage() + "): it goes unsigned and is refused");
        }
    }

    private String selfName() {
        return ce.tenant() + "/" + ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
    }

    private PrivateKey ownKey() throws Exception {
        return ce.getCertificateManager().identityPrivateKey();
    }

    private String ownCert() throws Exception {
        X509Certificate leaf = ce.getCertificateManager().getPublicCertificate()[0];
        String b = Base64.getEncoder().encodeToString(leaf.getEncoded());
        ownCertB64 = b;
        return b;
    }

    /** An agent's attestation while it is valid (a tenth of its life left at least), else this node's certificate. */
    String credential() throws Exception {
        String att = ownAttestation;
        if (att != null && System.currentTimeMillis() < ownAttestationExp - attestationMs / 10) return "att:" + att;
        return "cert:" + ownCert();
    }

    // ================================================================ receiving side

    /** The result of {@link #verify}: the signed caller, or why the message is refused. */
    public static final class Verdict {
        public final Caller caller;
        public final String refusal;
        Verdict(Caller caller, String refusal) { this.caller = caller; this.refusal = refusal; }
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

    /**
     * As above; {@code firstHand}: the node the receiving broker attests delivered this message first hand as its origin
     * ({@link CallerStamps#P_DIRECT}, as the consumer trusts it: its own broker's, or its parent's broker for the parent's
     * own messages), or null. An unsigned message is accepted when that node may vouch for the broker's stamp
     * ({@link #vouchRefusal}): it is the stamp's own node, or (an API user) a node of the user's tenant.
     */
    public Verdict verify(MsgEvent m, Caller fromBroker, boolean legacyAdmitted, CrescoIdentity firstHand) {
        if (!required) return new Verdict(fromBroker, null);
        String sigB64 = m.getParam(SIG);
        if (sigB64 == null) {
            if (legacyAdmitted && fromBroker != null && fromBroker.getKind() == Caller.Kind.AGENT) return new Verdict(fromBroker, null);
            if (direct && firstHand != null && fromBroker != null && !fromBroker.isAnonymous() && vouchRefusal(firstHand, fromBroker,
                    ce.getPluginBuilder() == null ? java.util.Set.of() : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig())) == null) {
                if (directAccepted.incrementAndGet() == 1) info("direct links ON: this node accepts unsigned messages its broker attests"
                        + " first hand (first: " + m.getMsgType() + " " + m.getParam("action") + " from " + fromBroker + ")");
                return new Verdict(fromBroker, null);
            }
            return refuse("an unsigned message (from " + fromBroker + (firstHand == null ? ", not delivered first hand" : ", delivered by "
                    + firstHand) + ")");
        }
        try {
            String by = m.getParam(BY), cred = m.getParam(CRED), ts = m.getParam(TS);
            if (by == null || cred == null || ts == null) return refuse("a signature without its signer, credential or time");
            long t = Long.parseLong(ts), now = System.currentTimeMillis();
            if (Math.abs(now - t) > maxAgeMs) {
                return refuse("a signature made " + (now - t) / 1000 + " s from now (msg_signature_max_age_ms=" + maxAgeMs + ")");
            }
            Resolved who = resolve(cred);
            if (who == null) return refuse("the signer's credential does not verify here (" + by + ")");
            String name = who.id.getTenant() + "/" + who.id.getRegion() + "/" + who.id.getAgent();
            if (!name.equals(by)) return refuse("signed by " + name + ", which is not the declared signer " + by);
            if (peers().isRevokedKey(who.fp)) return refuse("the signer's key is revoked (" + by + ")");
            byte[] sig = Base64.getDecoder().decode(sigB64);
            Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(who.key.getAlgorithm()));
            s.initVerify(who.key);
            s.update(canonical(m));
            if (!s.verify(sig)) return refuse("the signature does not verify (" + by + ")");
            Caller signed = Caller.parse(m.getParam(S_CALLER), m.getParam(S_TENANT), m.getParam(S_ROLES));
            if (signed == null || signed.isAnonymous()) return refuse("no signed caller");
            String vouch = vouchRefusal(who.id, signed, ce.getPluginBuilder() == null ? java.util.Set.of()
                    : SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig()));
            if (vouch != null) return refuse(vouch);
            if (fromBroker != null && !fromBroker.equals(signed)) {
                return refuse("the broker's caller " + fromBroker + " is not the signed caller " + signed);
            }
            if (!fresh(sig, now)) return refuse("a signature seen before (a replay) from " + by);
            if (verifiedCount.incrementAndGet() == 1) info("message signatures ON: this node checks what it dispatches (first: "
                    + m.getMsgType() + " " + m.getParam("action") + " from " + signed + ", signed by " + by + ")");
            return new Verdict(signed, null);
        } catch (Exception e) {
            return refuse("an unreadable signature (" + e.getClass().getSimpleName() + ")");
        }
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

    private Verdict refuse(String why) {
        refusedCount.incrementAndGet();
        refusal(why);
        return new Verdict(null, why);
    }

    private void info(String line) {
        try {
            ce.getPluginBuilder().getLogger(MsgSigning.class.getName(), io.cresco.library.utilities.CLogger.Level.Info).info(line);
        } catch (RuntimeException ignore) { }
    }

    private void refusal(String why) {
        long now = System.currentTimeMillis();
        if (now - lastRefusalLog > 10_000L) {
            lastRefusalLog = now;
            ce.getPluginBuilder().getLogger(MsgSigning.class.getName(), io.cresco.library.utilities.CLogger.Level.Info)
                    .warn("MESSAGE SIGNATURE: REFUSED " + why + " (" + refusedCount.get() + " refused so far; logged at most every 10 s)");
        }
    }

    /** Has this signature not been seen while it could be accepted? O(1): three set lookups, one insert. */
    boolean fresh(byte[] sig, long now) {
        String d = Base64.getEncoder().encodeToString(sha256.get().digest(sig));
        if (now - seenSince >= maxAgeMs || now < seenSince || seenNow.size() >= SEEN_GENERATION_MAX) rotateSeen(now);
        if (seenBefore.contains(d) || seenEarlier.contains(d)) return false;
        return seenNow.add(d);
    }

    private synchronized void rotateSeen(long now) {
        boolean due = now - seenSince >= maxAgeMs || now < seenSince;
        if (!due && seenNow.size() < SEEN_GENERATION_MAX) return;            // another thread rotated meanwhile
        if (!due && now - lastEarlyRotationLog > 60_000L) {
            lastEarlyRotationLog = now;
            ce.getPluginBuilder().getLogger(MsgSigning.class.getName(), io.cresco.library.utilities.CLogger.Level.Info)
                    .warn("MESSAGE SIGNATURE: more than " + SEEN_GENERATION_MAX + " signed messages in " + (now - seenSince) / 1000
                            + " s: the replay memory rotates early (a replay older than that is caught only by its age)");
        }
        seenEarlier = seenBefore;
        seenBefore = seenNow;
        seenNow = ConcurrentHashMap.newKeySet();
        seenSince = now;
    }

    private PeerTrustRegistry peers() {
        return ce.getCertificateManager().getPeerTrust();
    }

    /** A credential, verified once and cached (bounded) until its own expiry or an hour. */
    private Resolved resolve(String cred) throws Exception {
        String k = Base64.getEncoder().encodeToString(sha256.get().digest(cred.getBytes(StandardCharsets.UTF_8)));
        long now = System.currentTimeMillis();
        Resolved r = credCache.get(k);
        if (r != null && r.until > now) return r;
        r = cred.startsWith("cert:") ? resolveCert(cred.substring(5), now) : cred.startsWith("att:") ? resolveAttestation(cred.substring(4), now) : null;
        if (r == null) return null;
        if (credCache.size() > 20_000) credCache.clear();
        credCache.put(k, r);
        return r;
    }

    private Resolved resolveCert(String b64, long now) throws Exception {
        X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(b64)));
        if (now > leaf.getNotAfter().getTime()) return null;
        String fp = PeerTrustRegistry.fingerprint(leaf.getPublicKey().getEncoded());
        long until = Math.min(leaf.getNotAfter().getTime(), now + 3_600_000L);
        X509Certificate[] own = ce.getCertificateManager().getPublicCertificate();
        if (own != null && own.length > 0 && own[0].getPublicKey().equals(leaf.getPublicKey())) {
            return new Resolved(CrescoIdentity.of(ce.tenant(), ce.cstate.getRegion(), ce.cstate.getAgent(), null), leaf.getPublicKey(), fp, until);
        }
        CrescoIdentity id = peers().verifiedIdentity(new X509Certificate[]{leaf});
        if (id != null) return new Resolved(id, leaf.getPublicKey(), fp, until);
        CrescoIdentity named = CrescoNames.identityOf(leaf);
        if (named == null) return null;
        BundleEntry e = bundleEntry(named.getRegion(), named.getAgent());
        if (e != null && fp.equals(e.fp) && named.getTenant().equals(e.t)) {
            return new Resolved(CrescoIdentity.of(e.t, e.r, e.a, null), e.key, fp, until);
        }
        return null;
    }

    private Resolved resolveAttestation(String json, long now) throws Exception {
        Attestation a = GSON.fromJson(json, Attestation.class);
        if (a == null || a.v != 1 || a.t == null || a.r == null || a.a == null || a.k == null || a.iss == null || a.sig == null) return null;
        if (now > a.exp) return null;
        String[] iss = a.iss.split("/", -1);
        if (iss.length != 2 || !iss[0].equals(a.r)) return null;              // a region's controller attests its own agents only
        PublicKey issuerKey;
        String issuerTenant;
        if (iss[0].equals(ce.cstate.getRegion()) && iss[1].equals(ce.cstate.getAgent())) {
            issuerKey = ce.getCertificateManager().getPublicCertificate()[0].getPublicKey();
            issuerTenant = ce.tenant();
        } else {
            BundleEntry e = bundleEntry(iss[0], iss[1]);
            if (e == null) return null;
            issuerKey = e.key;
            issuerTenant = e.t;
        }
        if (peers().isRevokedKey(PeerTrustRegistry.fingerprint(issuerKey.getEncoded()))) return null;
        // a region controller attests its own tenant's agents, unless it is a superuser tenant's (a multi-tenant region)
        if (!issuerTenant.equals(a.t) && !SecuritySettings.superuserTenants(ce.getPluginBuilder().getConfig()).contains(issuerTenant)) return null;
        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(issuerKey.getAlgorithm()));
        s.initVerify(issuerKey);
        s.update(attestationBytes(a));
        if (!s.verify(Base64.getDecoder().decode(a.sig))) return null;
        PublicKey key = KeyFactory.getInstance(keyAlg(a.k)).generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(a.k)));
        return new Resolved(CrescoIdentity.of(a.t, a.r, a.a, null), key, PeerTrustRegistry.fingerprint(key.getEncoded()),
                Math.min(a.exp, now + 3_600_000L));
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

    private static byte[] attestationBytes(Attestation a) {
        return ("cresco-attestation-v1\n" + a.t + "\n" + a.r + "\n" + a.a + "\n" + a.k + "\n" + a.exp + "\n" + a.iss)
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

    // ================================================================ trust bundle and attestations on the pings

    /** On a child's ping: this node's certificate, what it holds of the bundle and (an agent) when its attestation expires. */
    public void onChildPing(MsgEvent ping) {
        if (!required) return;
        if (direct) ping.setParam(P_DIRECT_OK, "1");
        try {
            ping.setParam(P_CERT, ownCert());
            ping.setParam(P_BUNDLE_HAVE, heldBundleDigest);
            if (!ce.cstate.isRegionalController()) ping.setParam(P_ATT_EXP, Long.toString(ownAttestation == null ? 0 : ownAttestationExp));
        } catch (Exception ignore) { }
    }

    /**
     * In a parent's ping handler (the ping becomes the pong): learn the child's key from the certificate it presents -- it
     * must verify HERE as the ping's verified caller -- then attest an agent child ({@code childIsRegion} false) or record a
     * region child in the bundle (true, at the global), and send the bundle down when the child holds another.
     */
    public void onPingAtParent(MsgEvent msg, boolean childIsRegion) {
        String certB64 = msg.getParam(P_CERT), have = msg.getParam(P_BUNDLE_HAVE), attExp = msg.getParam(P_ATT_EXP);
        boolean childDirect = "1".equals(msg.getParam(P_DIRECT_OK));
        msg.removeParam(P_CERT);
        msg.removeParam(P_BUNDLE_HAVE);
        msg.removeParam(P_ATT_EXP);
        msg.removeParam(P_DIRECT_OK);
        if (!required) return;
        try {
            Caller caller = Caller.fromMsgEvent(msg);
            if (direct) {
                // the child (its verified caller: a controller pinging as itself) accepts direct messages; so does this node
                if (childDirect && caller != null && caller.getKind() == Caller.Kind.CONTROLLER) {
                    String child = caller.getRegion() + "_" + caller.getAgent();
                    if (directChildren.size() < DIRECT_CHILDREN_MAX || directChildren.containsKey(child)) {
                        directChildren.put(child, System.currentTimeMillis() + DIRECT_OFFER_MS);
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
                        BundleEntry e = entry(id.getTenant(), id.getRegion(), id.getAgent(), leaf.getPublicKey(), "region");
                        BundleEntry prev = childControllers.put(e.r + "/" + e.a, e);
                        if (prev == null || !prev.fp.equals(e.fp)) servedVersionKey = "";     // the bundle changed
                    }
                } else if (same && id.getRegion().equals(ce.cstate.getRegion())) {
                    long exp = 0;
                    try { exp = attExp == null ? 0 : Long.parseLong(attExp); } catch (NumberFormatException ignore) { }
                    if (exp < System.currentTimeMillis() + attestationMs / 3) msg.setParam(P_ATT, attest(id, leaf.getPublicKey()));
                }
            }
            String served = servedBundle();
            if (served != null && !servedDigest.equals(have)) msg.setCompressedParam(P_BUNDLE, served);
        } catch (Exception e) {
            refusal("ping of " + msg.getSrcRegion() + "/" + msg.getSrcAgent() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private String attest(CrescoIdentity id, PublicKey key) throws Exception {
        Attestation a = new Attestation();
        a.t = id.getTenant();
        a.r = id.getRegion();
        a.a = id.getAgent();
        a.k = Base64.getEncoder().encodeToString(key.getEncoded());
        a.exp = System.currentTimeMillis() + attestationMs;
        a.iss = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
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
        try {
            String att = pong.getParam(P_ATT);
            if (att != null) {
                Attestation a = GSON.fromJson(att, Attestation.class);
                PublicKey own = ce.getCertificateManager().getPublicCertificate()[0].getPublicKey();
                if (a != null && a.k != null && a.iss != null && a.iss.equals(parentRegion + "/" + parentAgent)
                        && a.r.equals(ce.cstate.getRegion()) && a.a.equals(ce.cstate.getAgent())
                        && a.k.equals(Base64.getEncoder().encodeToString(own.getEncoded())) && a.exp > System.currentTimeMillis()) {
                    ownAttestation = att;
                    ownAttestationExp = a.exp;
                }
            }
            String b = pong.getParam(P_BUNDLE) == null ? null : pong.getCompressedParam(P_BUNDLE);
            if (b != null && parentRegion != null && parentAgent != null) acceptBundle(b, parentRegion, parentAgent);
        } catch (Exception e) {
            refusal("pong of " + parentRegion + "/" + parentAgent + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** Adopt a bundle signed by this node's parent (its certificate verifies here as the parent); unknown names are kept a while. */
    void acceptBundle(String json, String parentRegion, String parentAgent) throws Exception {
        SignedBundle sb = GSON.fromJson(json, SignedBundle.class);
        if (sb == null || sb.v != 1 || sb.entries == null || sb.cert == null || sb.sig == null || sb.issuer == null) return;
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
            BundleEntry x = entry(e.t, e.r, e.a, decodeKey(e.k), e.role);
            if (x != null) next.put(x.r + "/" + x.a, x);
        }
        for (Map.Entry<String, BundleEntry> old : bundle.entrySet()) {
            if (next.containsKey(old.getKey())) continue;
            BundleEntry o = old.getValue();
            if (o.keepUntil == 0) o.keepUntil = now + grace;
            if (o.keepUntil > now) next.put(old.getKey(), o);
        }
        bundle.clear();
        bundle.putAll(next);
        heldBundleDigest = digest(json);
        servedVersionKey = "";
    }

    private static PublicKey decodeKey(String b64) throws Exception {
        return KeyFactory.getInstance(keyAlg(b64)).generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64)));
    }

    private static BundleEntry entry(String t, String r, String a, PublicKey key, String role) {
        if (t == null || r == null || a == null || key == null) return null;
        BundleEntry e = new BundleEntry();
        e.t = t; e.r = r; e.a = a; e.role = role;
        e.k = Base64.getEncoder().encodeToString(key.getEncoded());
        e.key = key;
        e.fp = PeerTrustRegistry.fingerprint(key.getEncoded());
        return e;
    }

    private static byte[] bundleBytes(SignedBundle sb) {
        StringBuilder b = new StringBuilder("cresco-trust-bundle-v1\n").append(sb.issuer).append('\n').append(sb.tenant).append('\n').append(sb.issued);
        for (BundleEntry e : sb.entries) b.append('\n').append(e.t).append('|').append(e.r).append('|').append(e.a).append('|').append(e.k).append('|').append(e.role);
        return b.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String digest(String s) {
        return Base64.getEncoder().encodeToString(sha256.get().digest(s.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * The bundle this node serves its children, signed by this node: its own entry, what its global sent it (a region), and
     * the region controllers that presented a verified certificate on their pings (a global). Re-signed only when it changes.
     */
    synchronized String servedBundle() throws Exception {
        TreeMap<String, BundleEntry> all = new TreeMap<>();
        long now = System.currentTimeMillis();
        for (BundleEntry e : bundle.values()) if (e.keepUntil == 0 || e.keepUntil > now) all.put(e.r + "/" + e.a, e);
        for (BundleEntry e : childControllers.values()) all.put(e.r + "/" + e.a, e);
        X509Certificate[] own = ce.getCertificateManager().getPublicCertificate();
        BundleEntry self = entry(ce.tenant(), ce.cstate.getRegion(), ce.cstate.getAgent(), own[0].getPublicKey(),
                ce.cstate.isGlobalController() ? "global" : "region");
        all.put(self.r + "/" + self.a, self);
        StringBuilder kb = new StringBuilder();
        for (BundleEntry e : all.values()) kb.append(e.t).append('/').append(e.r).append('/').append(e.a).append('/').append(e.fp).append(';');
        String key = digest(kb.toString());
        if (key.equals(servedVersionKey) && servedBundle != null) return servedBundle;
        SignedBundle sb = new SignedBundle();
        sb.issuer = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
        sb.tenant = ce.tenant();
        sb.issued = System.currentTimeMillis();
        sb.entries = new ArrayList<>(all.values());
        sb.cert = Base64.getEncoder().encodeToString(own[0].getEncoded());
        sb.sig = Base64.getEncoder().encodeToString(ce.getCertificateManager().signWithIdentity(bundleBytes(sb)));
        String json = GSON.toJson(sb);
        servedBundle = json;
        servedDigest = digest(json);
        servedVersionKey = key;
        return json;
    }

    /** Trust-bundle size held (tests, diagnostics). */
    public int bundleSize() { return bundle.size() + childControllers.size(); }

    /** This node holds an attestation (tests, diagnostics). */
    public boolean hasAttestation() { return ownAttestation != null; }
}
