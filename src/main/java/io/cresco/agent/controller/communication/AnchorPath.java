package io.cresco.agent.controller.communication;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Validates that a presented chain's LEAF reaches a listed trust anchor ({@code controller_trust_anchors}): a path built
 * from the leaf upward through the presented certificates only, each link checked -- the issuer's subject is the child's
 * issuer, the issuer is a CA (basic constraints), the child's signature verifies with the issuer's key, and every
 * certificate on the path is within its validity -- until a certificate whose SHA-256 (of its DER encoding) or whose key's
 * SHA-256 (of its SubjectPublicKeyInfo) is listed. Certificates that are not on that path are ignored: appending a
 * listed certificate (they are public: presented in every handshake) proves nothing. A listed LEAF matches at once.
 */
public final class AnchorPath {

    private AnchorPath() { }

    static final int MAX_DEPTH = 8;

    /** The path from the leaf to the first listed certificate (leaf first), or null when the leaf reaches none. */
    public static List<X509Certificate> path(X509Certificate[] chain, Collection<String> anchors, Date now) {
        if (chain == null || chain.length == 0 || chain[0] == null || anchors == null || anchors.isEmpty()) return null;
        List<X509Certificate> path = new ArrayList<>();
        X509Certificate cur = chain[0];
        if (!validAt(cur, now)) return null;
        path.add(cur);
        while (path.size() <= MAX_DEPTH) {
            if (listed(cur, anchors)) return path;
            X509Certificate issuer = null;
            for (X509Certificate c : chain) {
                if (c == null || path.contains(c)) continue;
                if (!c.getSubjectX500Principal().equals(cur.getIssuerX500Principal())) continue;
                if (c.getBasicConstraints() < 0) continue;                         // not a CA
                if (!validAt(c, now)) continue;
                try { cur.verify(c.getPublicKey()); } catch (Exception e) { continue; }
                issuer = c;
                break;
            }
            if (issuer == null) return null;
            path.add(issuer);
            cur = issuer;
        }
        return null;
    }

    static boolean listed(X509Certificate c, Collection<String> anchors) {
        try {
            return anchors.contains(CertificateManager.certFingerprint(c))
                    || anchors.contains(PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()));
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean validAt(X509Certificate c, Date now) {
        try { c.checkValidity(now); return true; } catch (Exception e) { return false; }
    }
}
