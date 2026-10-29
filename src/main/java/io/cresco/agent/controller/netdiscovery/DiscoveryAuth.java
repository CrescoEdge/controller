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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Discovery v2: a mutually authenticated, replay-proof certificate exchange keyed by the discovery secret.
 *
 * <pre>
 *   client                                                     responder (region/global controller)
 *   HELLO     type, Nc, client region/agent, client chain   -&gt;
 *                                                          &lt;-  CHALLENGE  Ns, responder region/agent, responder
 *                                                                          chain, broker port, MAC_K("S1", T)
 *   (client checks MAC "S1": the responder knows the secret, and it is fresh -- it covers Nc)
 *   PROOF     MAC_K("C", T)                                  -&gt;
 *                                    (responder checks MAC "C": the client knows the secret, and it is fresh -- it
 *                                     covers Ns; only now does it record anything or issue a certificate)
 *                                                          &lt;-  RESULT     ok, reason, result chain, MAC_K("S2", T, H(result))
 *   (client checks MAC "S2", then records/installs)
 * </pre>
 *
 * T is the transcript: protocol label, discovery type, both nonces, both identities (region, agent), the SHA-384
 * fingerprints of both leaf certificates' keys and the responder's broker port, each length-prefixed. K is derived
 * from the discovery secret of that type with PBKDF2-HMAC-SHA-384 (210,000 iterations, a per-type salt) and cached;
 * MACs are HMAC-SHA-384 and compared in constant time; nonces are 32 bytes from SecureRandom. All FIPS 140 approved
 * primitives at CNSA-1.0 sizes. A replayed HELLO/PROOF fails (fresh Ns), a man in the middle that changes an identity,
 * a certificate or the port fails (the transcript), and neither side records a name, pin, bridge peer or enrollment
 * before the other side has proven itself. The leaf each side presents must name the identity it claims.
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

        public static Mode parse(String v) {
            return v != null && v.trim().toLowerCase(Locale.ROOT).equals("compat") ? COMPAT : V2;
        }
    }

    /** One message of the exchange (JSON in the MsgEvent parameter {@value #PARAM}). */
    public static final class Msg {
        public int v = 2;
        public String step;              // hello | challenge | proof | result
        public String type;              // AGENT | REGION | GLOBAL
        public String nc, ns;
        public String creg, cagt, ccert;
        public String sreg, sagt, scert;
        public int sport;
        public String mac;
        public boolean ok;
        public String reason;
        public String rcert;
    }

    private static final Map<String, byte[]> KEYS = new ConcurrentHashMap<>();

    /** K for {@code type}, derived from {@code secret} (cached: PBKDF2 is deliberately slow). */
    static byte[] key(String secret, String purpose) {
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

    static byte[] transcript(Msg hello, Msg challenge, X509Certificate cleaf, X509Certificate sleaf) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String f : new String[]{LABEL, hello.type, hello.nc, challenge.ns, hello.creg, hello.cagt, fp(cleaf),
                challenge.sreg, challenge.sagt, fp(sleaf), Integer.toString(challenge.sport)}) {
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
            m.update(label.getBytes(StandardCharsets.UTF_8));
            m.update((byte) 0);
            m.update(transcript);
            for (String e : extra) {
                byte[] b = (e == null ? "" : e).getBytes(StandardCharsets.UTF_8);
                m.update(ByteBuffer.allocate(4).putInt(b.length).array());
                m.update(b);
            }
            return Base64.getEncoder().encodeToString(m.doFinal());
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
