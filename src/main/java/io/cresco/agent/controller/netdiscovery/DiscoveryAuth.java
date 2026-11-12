package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.CrescoNames;
import io.cresco.library.security.CrescoIdentity;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;

/**
 * Discovery v2: a mutually authenticated, replay-proof certificate exchange keyed by the discovery secret.
 *
 * <pre>
 *   client                                                     responder (region/global controller)
 *   HELLO     type, Nc, client region/agent, client chain   -&gt;
 *                                                          &lt;-  CHALLENGE  Ns, responder region/agent, responder
 *                                                                          chain, broker port, MAC_K("S1", T)
 *   (client checks MAC "S1": the responder knows the secret, and it is fresh -- it covers Nc)
 *   PROOF     MAC_K("C", T), SIG_client("pop-C", P)          -&gt;
 *                                    (responder checks MAC "C": the client knows the secret, and it is fresh -- it
 *                                     covers Ns; and the signature: the client holds the key of the leaf it sent;
 *                                     only now does it record anything or issue a certificate)
 *                                                          &lt;-  RESULT     ok, reason, result chain, MAC_K("S2", T, H(result)),
 *                                                                          SIG_responder("pop-S2", P, H(result))
 *   (client checks MAC "S2" and the signature: the responder holds the key of the leaf it sent; then records/installs)
 * </pre>
 *
 * The secret proves membership only (every member of a kind holds it), so the MACs alone never said that the sender of a
 * certificate holds its key. The signatures do: see "proof of possession" below ({@link #possessionTranscript}, P) for
 * what they cover, how a build says that it proves ({@link #PROOF_NONCE}), and what a peer of a release before the
 * proof (1.3.378: MACs only) is still given.
 *
 * T is the transcript: protocol label, discovery type, both nonces, both identities (region, agent), the SHA-384
 * fingerprints of both leaf certificates' keys, the responder's broker port, and the SHA-384 over the responder's WHOLE
 * presented chain (every certificate's DER, in order), each length-prefixed. MAC "C" and MAC "S2" also cover the SHA-384
 * over the client's whole presented chain: the client vouches for exactly the chain it sent, and the responder records a
 * chain (and any CA in it) only after that MAC verifies; the client records the responder's chain or the RESULT's chain
 * only after S1/S2 cover them. So a man in the middle cannot re-issue either side's leaf (same key, same subject) under a
 * CA of its own: the exchange fails before anything is recorded. K is derived
 * from the discovery secret of that type with PBKDF2-HMAC-SHA-384 (210,000 iterations, a per-type salt) and cached;
 * MACs are HMAC-SHA-384 and compared in constant time; nonces are 32 bytes from SecureRandom. All FIPS 140 approved
 * primitives at CNSA-1.0 sizes. A replayed HELLO/PROOF fails (fresh Ns), a man in the middle that changes an identity,
 * a certificate or the port fails (the transcript), and neither side records a name, pin, bridge peer or enrollment
 * before the other side has proven itself. The leaf each side presents must name the identity it claims.
 *
 * <p>Under {@code security_profile=production} the same exchange runs on the operator's PKI instead of a secret
 * ({@link #AUTH_PKI}): signatures by both sides' provisioned identity keys, chains verified to the trust anchors.
 *
 * <p>Modes ({@code discovery_auth}): {@code v2} (default) speaks and accepts only this exchange for certificate trust;
 * {@code compat} also accepts and falls back to the legacy single-message exchange (constant validator, replayable)
 * for peers that are not upgraded -- for a rolling upgrade only.
 */
public final class DiscoveryAuth {

    public static final String PARAM = "discovery_v2";
    public static final String MODE_KEY = "discovery_auth";
    static final String LABEL = "cresco-discovery-v2";
    static final int ITERATIONS = 210_000;

    private DiscoveryAuth() { }

    public enum Mode {
        V2, COMPAT;

        /**
         * This node's mode: {@code compat} only while its upgrade window is open ({@code discovery_compat_until}, see
         * SecuritySettings.compatOpen); otherwise {@code v2}, whatever {@code discovery_auth} says.
         */
        public static Mode of(io.cresco.library.plugin.Config config) {
            return io.cresco.agent.controller.communication.SecuritySettings.compatOpen(config, System.currentTimeMillis()) ? COMPAT : V2;
        }

        public static Mode parse(String v) {
            return v != null && v.trim().toLowerCase(Locale.ROOT).equals("compat") ? COMPAT : V2;
        }
    }

    /** One message of the exchange (JSON in the MsgEvent parameter {@value #PARAM}). */
    public static final class Msg {
        public int v = 2;
        public String step;              // hello | challenge | proof | result | busy
        public String type;              // AGENT | REGION | GLOBAL
        public String nc, ns;
        public String creg, cagt, ccert;
        public String sreg, sagt, scert;
        public int sport;
        public String mac;
        public boolean ok;
        public String reason;
        public String rcert;
        /** CHALLENGE, optional: an operator-signed controller rebind statement (RebindStatement; signed on its own, not MAC'd). */
        public String rebind;
        /** {@link #AUTH_PKI}: the PKI exchange (security_profile=production), {@code mac} carries signatures; absent: the secret's MACs. */
        public String auth;
        /**
         * The secret's exchange, PROOF and RESULT: the sender's signature by the key of the leaf it presented, next to the MAC
         * (proof of possession: {@link #possessionTranscript}). Absent from a build of before the proof (release 1.3.378).
         */
        public String pop;
        /**
         * BUSY: after how many milliseconds the responder asks to be tried again (a hint; it is not authenticated). Absent
         * from every other message (null is not written): a message of an exchange is as it was.
         */
        public Long retry;
    }

    // ---------------------------------------------------------------- proof of possession (the secret's exchange)
    //
    // The discovery secret proves membership: every region controller holds the global's, every agent its region's. The MACs
    // therefore say "a member sent this certificate", never "the sender holds this certificate's key" -- and a member that
    // presented another node's certificate (public) was recorded under it. So each side also SIGNS the exchange with the key
    // of the leaf it presents: the client in its PROOF (label pop-C), the responder in its RESULT (label pop-S2, which also
    // covers the result; it signs only for a client whose MAC verified, so a stranger cannot make it sign). What is signed:
    // a label of its own, the exchange type, both nonces, and for the joiner and the responder each its role in this type
    // of exchange, region, agent, the tenant its leaf states, its leaf key and its whole chain, the responder's broker port
    // and the rebind statement it served -- so a signature made for one exchange, one responder or one type of exchange
    // verifies in no other.
    //
    // How a build says what it is: a build that proves possession starts its NONCE with PROOF_NONCE. An earlier build's nonce
    // is plain base64 (no '.'), and both nonces are transcript fields that every MAC of the exchange covers on every build:
    // the mark cannot be removed or added on the way without the secret. A peer whose nonce carries the mark and whose
    // message carries no valid signature is refused (no downgrade); a peer without the mark is a build of before the proof
    // (or a holder of the secret that says so) and is given only what such a build may have (PeerTrustRegistry.join).

    static final String PROOF_NONCE = "p1.";
    static final String POP_CLIENT = "pop-C", POP_RESULT = "pop-S2";
    static final String POP_LABEL = "cresco-discovery-pop-v1";

    /** A nonce of this build: {@link #PROOF_NONCE} and 32 random bytes. */
    public static String proofNonce() { return PROOF_NONCE + nonce(); }

    /** Does the sender of {@code nonce} say it proves possession of its key? */
    public static boolean claimsProof(String nonce) { return nonce != null && nonce.startsWith(PROOF_NONCE); }

    static String joinerRole(String type) { return "AGENT".equals(type) ? "agent" : "controller"; }

    static String responderRole(String type) { return "GLOBAL".equals(type) ? "global" : "controller"; }

    private static String tenantOf(X509Certificate leaf) {
        CrescoIdentity id = leaf == null ? null : CrescoNames.claimOf(leaf);
        return id == null || id.getTenant() == null ? "" : id.getTenant();
    }

    /**
     * The signature algorithm of a proof of possession: FIXED per key type, like the key derivation -- never the node's crypto
     * policy ({@code crypto_allow_sig}), which a joiner does not hold yet and two nodes need not share: both sides must sign
     * and verify alike before anything else is agreed. RSA: SHA384withRSA; EC: SHA384withECDSA (FIPS 186, CNSA 1.0). Another
     * key type has none (null): its holder cannot prove it in this exchange, and is told so by name.
     */
    public static String proofAlgorithm(String keyType) {
        if ("RSA".equalsIgnoreCase(keyType)) return "SHA384withRSA";
        if ("EC".equalsIgnoreCase(keyType) || "ECDSA".equalsIgnoreCase(keyType)) return "SHA384withECDSA";
        return null;
    }

    /** A proof of possession: {@code key}'s signature over {@link #authBytes} with {@link #proofAlgorithm}, base64; null for a key type that has none. */
    static String proofSign(java.security.PrivateKey key, String label, byte[] transcript, String... extra) {
        String alg = key == null ? null : proofAlgorithm(key.getAlgorithm());
        if (alg == null) return null;
        try {
            java.security.Signature s = java.security.Signature.getInstance(alg);
            s.initSign(key);
            s.update(authBytes(label, transcript, extra));
            return Base64.getEncoder().encodeToString(s.sign());
        } catch (Exception e) {
            throw new IllegalStateException("cannot sign the discovery exchange: " + e.getMessage(), e);
        }
    }

    /** Does {@code sigB64} verify under {@code key} as a proof of possession ({@link #proofAlgorithm})? */
    static boolean proofVerify(java.security.PublicKey key, String sigB64, String label, byte[] transcript, String... extra) {
        String alg = key == null || sigB64 == null ? null : proofAlgorithm(key.getAlgorithm());
        if (alg == null) return false;
        try {
            java.security.Signature s = java.security.Signature.getInstance(alg);
            s.initVerify(key);
            s.update(authBytes(label, transcript, extra));
            return s.verify(Base64.getDecoder().decode(sigB64));
        } catch (Exception e) {
            return false;
        }
    }

    /** What a proof of possession signs (see above); each field length-prefixed. */
    static byte[] possessionTranscript(Msg hello, Msg challenge, X509Certificate cleaf, X509Certificate sleaf) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String f : new String[]{POP_LABEL, hello.type, hello.nc, challenge.ns,
                "joiner", joinerRole(hello.type), hello.creg, hello.cagt, tenantOf(cleaf), fp(cleaf), chainFp(hello.ccert),
                "responder", responderRole(hello.type), challenge.sreg, challenge.sagt, tenantOf(sleaf), fp(sleaf), chainFp(challenge.scert),
                Integer.toString(challenge.sport), "rebind", chainFp(challenge.rebind)}) {
            byte[] b = (f == null ? "" : f).getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(4).putInt(b.length).array());
            out.writeBytes(b);
        }
        return out.toByteArray();
    }

    /** The message parameter of a busy answer for a peer that sent a locator (it reads no {@link Msg}): the same milliseconds. */
    public static final String BUSY_PARAM = "discovery_busy";

    /**
     * The PKI exchange (security_profile=production). The same four messages and the same transcript, but no discovery
     * secret at all: S1, C and S2 are signatures, each by the sender's provisioned identity key (the key its leaf carries),
     * under labels of their own ({@code pki-S1}, {@code pki-C}, {@code pki-S2}), and each side's whole chain must verify to
     * the operator's trust anchors (role, name, validity, revocation) before its signature is checked. Fresh nonces in the
     * transcript make every signature single-use; nothing is recorded before the peer's signature verifies.
     */
    public static final String AUTH_PKI = "pki";
    /** The locator validator of the PKI exchange: locators carry no secret (they only locate; the exchange authenticates). */
    public static final String PKI_LOCATOR = "pki";

    /** What a MAC or a PKI signature covers: the label, a zero byte, the transcript, then each extra length-prefixed. */
    static byte[] authBytes(String label, byte[] transcript, String... extra) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(transcript.length + 64);
        out.writeBytes(label.getBytes(StandardCharsets.UTF_8));
        out.write(0);
        out.writeBytes(transcript);
        for (String e : extra) {
            byte[] b = (e == null ? "" : e).getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(4).putInt(b.length).array());
            out.writeBytes(b);
        }
        return out.toByteArray();
    }

    /** The PKI exchange: {@code key}'s signature over {@link #authBytes}, base64. */
    static String sign(java.security.PrivateKey key, String label, byte[] transcript, String... extra) {
        try {
            java.security.Signature s = java.security.Signature.getInstance(
                    io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(key.getAlgorithm()));
            s.initSign(key);
            s.update(authBytes(label, transcript, extra));
            return Base64.getEncoder().encodeToString(s.sign());
        } catch (Exception e) {
            throw new IllegalStateException("cannot sign the discovery exchange: " + e.getMessage(), e);
        }
    }

    /** The PKI exchange: does {@code sigB64} verify under {@code key} over {@link #authBytes}? */
    static boolean verify(java.security.PublicKey key, String sigB64, String label, byte[] transcript, String... extra) {
        if (key == null || sigB64 == null) return false;
        try {
            java.security.Signature s = java.security.Signature.getInstance(
                    io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(key.getAlgorithm()));
            s.initVerify(key);
            s.update(authBytes(label, transcript, extra));
            return s.verify(Base64.getDecoder().decode(sigB64));
        } catch (Exception e) {
            return false;
        }
    }

    /** Derived keys, by (purpose, secret digest): bounded, so secrets that come and go cannot grow it without limit. */
    static final int KEY_CACHE = 64;
    private static final Map<String, byte[]> KEYS = java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(16, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, byte[]> e) { return size() > KEY_CACHE; }
    });

    static int cachedKeys() { return KEYS.size(); }

    /** K for {@code type}, derived from {@code secret} (cached: PBKDF2 is deliberately slow). */
    static byte[] key(String secret, String purpose) {
        if (secret == null) throw new IllegalArgumentException("no discovery secret");
        String cacheKey;
        try {
            cacheKey = purpose + "|" + Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-384").digest(secret.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return KEYS.computeIfAbsent(cacheKey, k -> {
            try {
                SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA384");
                PBEKeySpec spec = new PBEKeySpec(secret.toCharArray(), (LABEL + "|" + purpose).getBytes(StandardCharsets.UTF_8), ITERATIONS, 384);
                return f.generateSecret(spec).getEncoded();
            } catch (Exception e) {
                throw new IllegalStateException("PBKDF2WithHmacSHA384 unavailable", e);
            }
        });
    }

    public static String nonce() {
        byte[] n = new byte[32];
        new SecureRandom().nextBytes(n);
        return Base64.getEncoder().encodeToString(n);
    }

    /** SHA-384 over a certificate's SubjectPublicKeyInfo, hex ("" when absent). */
    static String fp(X509Certificate c) {
        if (c == null) return "";
        try {
            StringBuilder b = new StringBuilder();
            for (byte x : MessageDigest.getInstance("SHA-384").digest(c.getPublicKey().getEncoded())) b.append(String.format("%02x", x));
            return b.toString();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * SHA-384 over a chain exactly as a message carries it (its JSON text, which is what the receiver parses and records),
     * hex; "" when absent.
     */
    static String chainFp(String chainJson) {
        if (chainJson == null) return "";
        try {
            StringBuilder b = new StringBuilder();
            for (byte x : MessageDigest.getInstance("SHA-384").digest(chainJson.getBytes(StandardCharsets.UTF_8))) b.append(String.format("%02x", x));
            return b.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** The transcript MAC "C" and MAC "S2" cover: both whole chains as the messages carry them. */
    static byte[] transcript(Msg hello, Msg challenge, X509Certificate cleaf, X509Certificate sleaf) {
        return transcript(hello, challenge, cleaf, sleaf, true);
    }

    /**
     * The transcript. Always the responder's whole chain ({@code challenge.scert}); with {@code clientChain} also the client's
     * ({@code hello.ccert}), for MAC "C" and "S2". MAC "S1" binds only the client's leaf key, so a client whose chain was
     * altered on the way still learns that the responder is genuine, then fails at the responder, which records nothing.
     */
    static byte[] transcript(Msg hello, Msg challenge, X509Certificate cleaf, X509Certificate sleaf, boolean clientChain) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String f : new String[]{LABEL, hello.type, hello.nc, challenge.ns, hello.creg, hello.cagt, fp(cleaf),
                challenge.sreg, challenge.sagt, fp(sleaf), Integer.toString(challenge.sport), "schain", chainFp(challenge.scert),
                "cchain", clientChain ? chainFp(hello.ccert) : "-"}) {
            byte[] b = (f == null ? "" : f).getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(4).putInt(b.length).array());
            out.writeBytes(b);
        }
        return out.toByteArray();
    }

    static String mac(byte[] key, String label, byte[] transcript, String... extra) {
        try {
            Mac m = Mac.getInstance("HmacSHA384");
            m.init(new SecretKeySpec(key, "HmacSHA384"));
            return Base64.getEncoder().encodeToString(m.doFinal(authBytes(label, transcript, extra)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static boolean macEquals(String a, String b) {
        if (a == null || b == null) return false;
        try {
            return MessageDigest.isEqual(Base64.getDecoder().decode(a), Base64.getDecoder().decode(b));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Hash of a RESULT's content, bound into MAC "S2". */
    static String resultDigest(Msg r) {
        return r.ok + "|" + (r.reason == null ? "" : r.reason) + "|" + (r.rcert == null ? "" : r.rcert);
    }

    /** Does the leaf name exactly region/agent (strict grammar)? */
    static boolean leafNames(X509Certificate leaf, String region, String agent) {
        if (!CrescoNames.isValid(region) || !CrescoNames.isValid(agent)) return false;
        CrescoIdentity id = CrescoNames.claimOf(leaf);
        return id != null && region.equals(id.getRegion()) && agent.equals(id.getAgent());
    }
}
