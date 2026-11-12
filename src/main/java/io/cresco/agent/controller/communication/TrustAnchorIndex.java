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
 * <p>All changes are made by {@link CertificateManager} under its truststore lock, together with the same change of the
 * {@code KeyStore} (which stays the form that is saved); reads take no lock.
 */
final class TrustAnchorIndex {

    private final Map<String, X509Certificate> byAlias = new ConcurrentHashMap<>();
    private final Map<X500Principal, Map<String, X509Certificate>> bySubject = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> byPrefix = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> byKey = new ConcurrentHashMap<>();
    private final AtomicLong lookups = new AtomicLong(), changes = new AtomicLong();

    /** The part of an alias that names whose entry it is: up to and with its first {@code #}, lower case; "" without one. */
    static String prefixOf(String alias) {
        int h = alias.indexOf('#');
        return h < 0 ? "" : alias.substring(0, h + 1).toLowerCase(Locale.ROOT);
    }

    /** Record {@code cert} under {@code alias} (replacing what the alias held). Only X.509 certificates anchor anything. */
    void put(String rawAlias, Certificate cert) {
        final String alias = norm(rawAlias);
        remove(alias);
        if (!(cert instanceof X509Certificate)) return;
        X509Certificate c = (X509Certificate) cert;
        byAlias.put(alias, c);
        bySubject.computeIfAbsent(c.getSubjectX500Principal(), k -> new ConcurrentHashMap<>()).put(alias, c);
        byPrefix.computeIfAbsent(prefixOf(alias), k -> ConcurrentHashMap.newKeySet()).add(alias);
        byKey.computeIfAbsent(PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()), k -> ConcurrentHashMap.newKeySet()).add(alias);
        changes.incrementAndGet();
    }

    /** Forget the entry {@code alias}. */
    void remove(String rawAlias) {
        final String alias = norm(rawAlias);
        X509Certificate c = byAlias.remove(alias);
        if (c == null) return;
        bySubject.computeIfPresent(c.getSubjectX500Principal(), (k, m) -> { m.remove(alias); return m.isEmpty() ? null : m; });
        byPrefix.computeIfPresent(prefixOf(alias), (k, s) -> { s.remove(alias); return s.isEmpty() ? null : s; });
        byKey.computeIfPresent(PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()), (k, s) -> { s.remove(alias); return s.isEmpty() ? null : s; });
        changes.incrementAndGet();
    }

    /** Start again from what {@code store} holds (one walk: when a store is created or loaded). */
    void rebuild(KeyStore store) throws java.security.KeyStoreException {
        byAlias.clear(); bySubject.clear(); byPrefix.clear(); byKey.clear();
        if (store == null) return;
        for (String alias : Collections.list(store.aliases())) {
            Certificate c = store.getCertificate(alias);
            if (c != null) put(alias, c);
        }
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

    X509Certificate get(String alias) { return byAlias.get(norm(alias)); }

    boolean contains(String alias) { return byAlias.containsKey(norm(alias)); }

    /** A keystore's aliases do not tell upper from lower case: one spelling here too. */
    private static String norm(String alias) { return alias == null ? "" : alias.toLowerCase(Locale.ROOT); }

    int size() { return byAlias.size(); }

    /**
     * The entries that can anchor {@code chain}: those whose subject is the subject or the issuer of one of its
     * certificates. A PKIX path for the chain ends at an entry that IS one of its certificates (same subject) or that
     * issued one of them (subject = that certificate's issuer); no other entry can take part.
     */
    List<X509Certificate> anchorsFor(X509Certificate[] chain) {
        lookups.incrementAndGet();
        Set<X509Certificate> out = new LinkedHashSet<>();
        if (chain != null) {
            for (X509Certificate c : chain) {
                if (c == null) continue;
                Map<String, X509Certificate> a = bySubject.get(c.getSubjectX500Principal());
                if (a != null) out.addAll(a.values());
                a = bySubject.get(c.getIssuerX500Principal());
                if (a != null) out.addAll(a.values());
            }
        }
        return new ArrayList<>(out);
    }

    /** Every entry (a walk: for a listing, never for a trust decision). */
    X509Certificate[] all() {
        return new LinkedHashSet<>(byAlias.values()).toArray(new X509Certificate[0]);
    }

    /** Trust decisions answered from the index so far. */
    long lookups() { return lookups.get(); }

    /** Entries added or removed so far. */
    long changes() { return changes.get(); }
}
