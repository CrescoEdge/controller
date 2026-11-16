package io.cresco.agent.controller.communication;

import javax.security.auth.x500.X500Principal;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The certificates of a node's truststore, indexed so that neither recording one nor judging a chain walks the store.
 *
 * <p>Before, a controller that recorded a joiner's certificates (per-node trust) rebuilt its broker's trust from the whole
 * truststore and re-initialised the broker's TLS context with it, at every join: a join cost more the more had joined,
 * and a connection that arrived during the rebuild was judged by the trust of a moment before. Here every entry is
 * indexed when it is added (by its subject name, by the prefix of its alias and by its key), and a TLS trust decision
 * ({@link LivePkixTrustManager}) asks for the entries that can anchor the chain it was shown: those whose subject is the
 * subject or the issuer of one of the chain's certificates. PKIX can use no other entry for that chain, so the answer is
 * the one a walk of the whole store gives, and an entry is used from the moment {@link #put} returns.
 *
 * <p><b>One subject, few certificates.</b> A subject's name is whatever the certificate's maker wrote, and a joiner makes
 * its own: entries of ONE subject are all handed to PKIX for a chain that names that subject (it tries each key), so a
 * thousand of them made one decision cost a third of a second -- with the whole store as with this index. At most
 * {@link #MAX_PER_SUBJECT} DIFFERENT certificates of one subject are indexed; a further one is not recorded (its holder
 * is still trusted by its bound key, as after a restart). The same certificate under many aliases counts once. This is
 * the one place where the index is stricter than a walk of the store: a subject with more certificates than that.
 *
 * <p>All changes are made by {@link CertificateManager} under its truststore lock, together with the same change of the
 * {@code KeyStore} (which stays the form that is saved); reads take no lock.
 */
final class TrustAnchorIndex {

    /** Different certificates of one subject that are indexed at most (a CA renewed under its name: two or three). */
    static final int MAX_PER_SUBJECT = 8;

    private static final class Entry {
        final X509Certificate cert;
        final String certFp, keyFp;
        Entry(X509Certificate cert, String certFp, String keyFp) { this.cert = cert; this.certFp = certFp; this.keyFp = keyFp; }
    }

    /** One certificate of a subject and the aliases it is recorded under. */
    private static final class Held {
        final X509Certificate cert;
        final Set<String> aliases = ConcurrentHashMap.newKeySet();
        Held(X509Certificate cert) { this.cert = cert; }
    }

    private final Map<String, Entry> byAlias = new ConcurrentHashMap<>();
    private final Map<X500Principal, Map<String, Held>> bySubject = new ConcurrentHashMap<>();   // subject -> certificate fingerprint
    private final Map<String, Set<String>> byPrefix = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> byKey = new ConcurrentHashMap<>();
    private final AtomicLong lookups = new AtomicLong(), changes = new AtomicLong(), refused = new AtomicLong();

    /** The part of an alias that names whose entry it is: up to and with its first {@code #}, lower case; "" without one. */
    static String prefixOf(String alias) {
        int h = alias.indexOf('#');
        return h < 0 ? "" : alias.substring(0, h + 1).toLowerCase(Locale.ROOT);
    }

    private static String certFp(X509Certificate c) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(c.getEncoded()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Would {@link #put} record {@code cert}? Not when its subject already has {@link #MAX_PER_SUBJECT} other certificates. */
    boolean accepts(Certificate cert) {
        if (!(cert instanceof X509Certificate)) return true;                 // (nothing to index: it anchors nothing)
        X509Certificate c = (X509Certificate) cert;
        Map<String, Held> of = bySubject.get(c.getSubjectX500Principal());
        return of == null || of.size() < MAX_PER_SUBJECT || of.containsKey(certFp(c));
    }

    /**
     * Record {@code cert} under {@code alias} (replacing what the alias held). Only X.509 certificates anchor anything.
     *
     * @return false when it was NOT recorded: its subject already has {@link #MAX_PER_SUBJECT} different certificates
     */
    boolean put(String rawAlias, Certificate cert) {
        final String alias = norm(rawAlias);
        if (!accepts(cert)) {
            refused.incrementAndGet();
            return false;
        }
        remove(alias);
        if (!(cert instanceof X509Certificate)) return true;
        X509Certificate c = (X509Certificate) cert;
        Entry e = new Entry(c, certFp(c), PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()));
        byAlias.put(alias, e);
        bySubject.computeIfAbsent(c.getSubjectX500Principal(), k -> new ConcurrentHashMap<>())
                .computeIfAbsent(e.certFp, k -> new Held(c)).aliases.add(alias);
        byPrefix.computeIfAbsent(prefixOf(alias), k -> ConcurrentHashMap.newKeySet()).add(alias);
        byKey.computeIfAbsent(e.keyFp, k -> ConcurrentHashMap.newKeySet()).add(alias);
        changes.incrementAndGet();
        return true;
    }

    /** Forget the entry {@code alias}. */
    void remove(String rawAlias) {
        final String alias = norm(rawAlias);
        Entry e = byAlias.remove(alias);
        if (e == null) return;
        bySubject.computeIfPresent(e.cert.getSubjectX500Principal(), (k, m) -> {
            Held h = m.get(e.certFp);
            if (h != null) {
                h.aliases.remove(alias);
                if (h.aliases.isEmpty()) m.remove(e.certFp);
            }
            return m.isEmpty() ? null : m;
        });
        byPrefix.computeIfPresent(prefixOf(alias), (k, s) -> { s.remove(alias); return s.isEmpty() ? null : s; });
        byKey.computeIfPresent(e.keyFp, (k, s) -> { s.remove(alias); return s.isEmpty() ? null : s; });
        changes.incrementAndGet();
    }

    /**
     * Start again from what {@code store} holds (one walk: when a store is created or loaded).
     *
     * @return the entries of the store that are NOT indexed (more than {@link #MAX_PER_SUBJECT} certificates of one subject)
     */
    int rebuild(KeyStore store) throws java.security.KeyStoreException {
        byAlias.clear(); bySubject.clear(); byPrefix.clear(); byKey.clear();
        int left = 0;
        if (store == null) return 0;
        for (String alias : Collections.list(store.aliases())) {
            Certificate c = store.getCertificate(alias);
            if (c != null && !put(alias, c)) left++;
        }
        return left;
    }

    /** The aliases whose prefix (see {@link #prefixOf}) is {@code prefix}: a copy. */
    List<String> aliasesOfPrefix(String prefix) {
        Set<String> s = byPrefix.get(prefix);
        return s == null ? List.of() : new ArrayList<>(s);
    }

    /** The aliases of the entries that carry the key {@code keyFp} (SHA-256 of its SubjectPublicKeyInfo): a copy. */
    List<String> aliasesOfKey(String keyFp) {
        Set<String> s = keyFp == null ? null : byKey.get(keyFp);
        return s == null ? List.of() : new ArrayList<>(s);
    }

    X509Certificate get(String alias) {
        Entry e = byAlias.get(norm(alias));
        return e == null ? null : e.cert;
    }

    boolean contains(String alias) { return byAlias.containsKey(norm(alias)); }

    /** A keystore's aliases do not tell upper from lower case: one spelling here too. */
    private static String norm(String alias) { return alias == null ? "" : alias.toLowerCase(Locale.ROOT); }

    int size() { return byAlias.size(); }

    /**
     * The entries that can anchor {@code chain}: those whose subject is the subject or the issuer of one of its
     * certificates. A PKIX path for the chain ends at an entry that IS one of its certificates (same subject) or that
     * issued one of them (subject = that certificate's issuer); no other entry can take part. At most
     * {@link #MAX_PER_SUBJECT} per name the chain carries.
     */
    List<X509Certificate> anchorsFor(X509Certificate[] chain) {
        lookups.incrementAndGet();
        Set<X509Certificate> out = new LinkedHashSet<>();
        if (chain != null) {
            for (X509Certificate c : chain) {
                if (c == null) continue;
                collect(out, c.getSubjectX500Principal());
                collect(out, c.getIssuerX500Principal());
            }
        }
        return new ArrayList<>(out);
    }

    private void collect(Set<X509Certificate> out, X500Principal subject) {
        Map<String, Held> of = bySubject.get(subject);
        if (of != null) for (Held h : of.values()) out.add(h.cert);
    }

    /** Every entry (a walk: for a listing, never for a trust decision). */
    X509Certificate[] all() {
        Set<X509Certificate> out = new LinkedHashSet<>();
        for (Entry e : byAlias.values()) out.add(e.cert);
        return out.toArray(new X509Certificate[0]);
    }

    /** Trust decisions answered from the index so far. */
    long lookups() { return lookups.get(); }

    /** Entries added or removed so far. */
    long changes() { return changes.get(); }

    /** Certificates not recorded because their subject had {@link #MAX_PER_SUBJECT} already. */
    long refused() { return refused.get(); }
}
