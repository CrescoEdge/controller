package io.cresco.agent.controller.communication;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Locale;

/**
 * An operator-signed CONTROLLER REBIND STATEMENT: "the controller {@code <region>/<agent>} now holds the key
 * {@code <fingerprint>}", signed with an operator key. A controller that was re-installed with a new key (its
 * {@code security/} directory lost) serves the statement found in {@code <data>/security/controller-rebind.json} in its
 * discovery CHALLENGE; a joiner that pinned the old key, and lists the operator key's fingerprint in
 * {@code controller_rebind_signers}, verifies it and follows the controller to the new key. Recovery is then ONE operator
 * action at the controller instead of one rebind per joiner.
 *
 * <p>A statement vouches for exactly one name and one key, for at most {@link #MAX_LIFETIME_MS} (30 days); it never
 * applies to a revoked (tombstoned) name or a revoked key, and only joiners that configured the signer accept it. The
 * signed bytes are {@code cresco-controller-rebind-v1|<name>|<key>|<issued>|<expires>} (milliseconds), signed
 * SHA384withRSA or SHA384withECDSA; the signer is the operator's public key (X.509 SubjectPublicKeyInfo, base64), and
 * its fingerprint is SHA-256 over that DER (what {@code controller_rebind_signers} lists, hex).
 *
 * <p>Make one with the JDK alone:
 * {@code java -cp controller-<v>.jar io.cresco.agent.controller.communication.RebindStatement <operator-key.pem>
 * <operator-pub.pem> <region>/<agent> <new key fingerprint> [days]} (PKCS#8 and SubjectPublicKeyInfo PEM, e.g. from
 * {@code openssl genpkey}/{@code openssl pkey -pubout}); the controller's new key fingerprint is logged at its start
 * ("this node's key ...") and is SHA-256 over its certificate's SubjectPublicKeyInfo.
 */
public final class RebindStatement {

    private RebindStatement() { }

    static final String LABEL = "cresco-controller-rebind-v1";
    public static final long MAX_LIFETIME_MS = 30L * 24 * 3600 * 1000;

    /** The statement as stored and sent (JSON). */
    static final class Body {
        int v = 1;
        String name, key, signer, sig;
        long issued, expires;
    }

    static byte[] bytes(String name, String key, long issued, long expires) {
        return (LABEL + "|" + name + "|" + key + "|" + issued + "|" + expires).getBytes(StandardCharsets.UTF_8);
    }

    private static String algorithm(PublicKey k) {
        return "EC".equalsIgnoreCase(k.getAlgorithm()) ? "SHA384withECDSA" : "SHA384withRSA";
    }

    static PublicKey publicKey(byte[] spki) throws Exception {
        for (String alg : new String[]{"RSA", "EC"}) {
            try { return KeyFactory.getInstance(alg).generatePublic(new X509EncodedKeySpec(spki)); } catch (Exception ignore) { }
        }
        throw new IllegalArgumentException("the signer is neither an RSA nor an EC public key");
    }

    /** A signed statement (JSON) that {@code name} holds the key {@code keyFp}, valid from {@code issued} to {@code expires}. */
    public static String sign(PrivateKey operatorKey, PublicKey operatorPublic, String name, String keyFp, long issued, long expires) throws Exception {
        String key = keyFp.replace(":", "").toLowerCase(Locale.ROOT);
        Signature s = Signature.getInstance(algorithm(operatorPublic));
        s.initSign(operatorKey);
        s.update(bytes(name, key, issued, expires));
        String signer = Base64.getEncoder().encodeToString(operatorPublic.getEncoded());
        String sig = Base64.getEncoder().encodeToString(s.sign());
        return "{\"v\":1,\"name\":\"" + name + "\",\"key\":\"" + key + "\",\"issued\":" + issued + ",\"expires\":" + expires
                + ",\"signer\":\"" + signer + "\",\"sig\":\"" + sig + "\"}";
    }

    /**
     * Why {@code json} does NOT vouch that {@code region/agent} holds the key {@code keyFp} now, for a joiner trusting the
     * signer fingerprints {@code signers}; null when it does.
     */
    public static String refusal(String json, String region, String agent, String keyFp, Collection<String> signers, long now) {
        if (json == null || json.isBlank()) return "no rebind statement";
        if (signers == null || signers.isEmpty()) return "no controller_rebind_signers are configured";
        try {
            Body b = new com.google.gson.Gson().fromJson(json, Body.class);
            if (b == null || b.v != 1 || b.name == null || b.key == null || b.signer == null || b.sig == null) return "a malformed statement";
            String name = region + "/" + agent;
            if (!name.equals(b.name)) return "the statement is for " + b.name + ", not " + name;
            if (keyFp == null || !b.key.equalsIgnoreCase(keyFp)) return "the statement names another key (" + PeerIdentity.shortFp(b.key) + ")";
            if (b.expires <= b.issued || b.expires - b.issued > MAX_LIFETIME_MS) return "the statement's validity is not 1 ms to 30 days";
            if (now < b.issued - CertValidity.backdateMs() || now >= b.expires) return "the statement is not valid now (" + new java.util.Date(b.issued)
                    + " - " + new java.util.Date(b.expires) + ")";
            byte[] spki = Base64.getDecoder().decode(b.signer);
            String signerFp = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(spki));
            boolean listed = false;
            for (String s : signers) if (s.replace(":", "").trim().equalsIgnoreCase(signerFp)) listed = true;
            if (!listed) return "its signer " + PeerIdentity.shortFp(signerFp) + " is not in controller_rebind_signers";
            PublicKey pk = publicKey(spki);
            Signature s = Signature.getInstance(algorithm(pk));
            s.initVerify(pk);
            s.update(bytes(b.name, b.key.toLowerCase(Locale.ROOT), b.issued, b.expires));
            if (!s.verify(Base64.getDecoder().decode(b.sig))) return "the signature does not verify";
            return null;
        } catch (Exception e) {
            return "an unreadable statement (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
        }
    }

    private static byte[] pem(Path p) throws Exception {
        StringBuilder b64 = new StringBuilder();
        for (String line : Files.readAllLines(p, StandardCharsets.US_ASCII)) {
            if (line.startsWith("-----")) continue;
            b64.append(line.trim());
        }
        return Base64.getDecoder().decode(b64.toString());
    }

    /** Operator tool: print a signed statement (see the class comment). */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: RebindStatement <operator-key.pem (PKCS#8)> <operator-pub.pem> <region>/<agent> <new key fingerprint> [days, max 30]");
            System.exit(2);
        }
        PublicKey pub = publicKey(pem(Paths.get(args[1])));
        PrivateKey key = KeyFactory.getInstance(pub.getAlgorithm()).generatePrivate(new PKCS8EncodedKeySpec(pem(Paths.get(args[0]))));
        long days = args.length > 4 ? Math.min(30, Math.max(1, Long.parseLong(args[4]))) : 7;
        long now = System.currentTimeMillis();
        System.out.println(sign(key, pub, args[2], args[3], now, now + days * 24 * 3600 * 1000 - 1));
    }
}
