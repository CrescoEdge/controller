package io.cresco.agent.controller.communication;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

/**
 * This node's TLS key manager: the JDK's choice, or else the node's own identity, for a client certificate.
 *
 * <p>Why: a server that requires client certificates names the CAs it accepts (from its truststore), and the JDK key
 * manager presents a certificate only when its chain names one of them. Under per-node trust a peer's self-rooted chain
 * is in the server's truststore only while the server remembers it; after the server restarts, its truststore no longer
 * names the agent's (or the bridging region's) issuer, so the client presented NO certificate and the handshake failed
 * -- although the server would trust the chain by its registry-bound key ({@link BoundKeyTrustManager}). A Cresco node
 * has exactly one identity, so it always presents it; the server decides (PKIX, or the key bound to the name).
 * Server-side selection is unchanged.
 */
final class NodeIdentityKeyManager extends X509ExtendedKeyManager {

    private final X509KeyManager d;
    private final Supplier<String> ownAlias;

    /** The node's CURRENT chain (renewed or re-enrolled since the delegate was made), or null to use the delegate's. */
    private volatile Supplier<X509Certificate[]> currentChain;

    /** Present the chain {@code current} supplies for this node's own alias (the key is unchanged by renewal). */
    NodeIdentityKeyManager withCurrentChain(Supplier<X509Certificate[]> current) {
        this.currentChain = current;
        return this;
    }

    /** The node's CURRENT private key (a provisioned keystore replaced at run time), or null to use the delegate's. */
    private volatile Supplier<PrivateKey> currentKey;

    /** Present the key {@code current} supplies for this node's own alias (when it supplies one). */
    NodeIdentityKeyManager withCurrentKey(Supplier<PrivateKey> current) {
        this.currentKey = current;
        return this;
    }

    /**
     * This node's identity as presented in one handshake: its chain and its key, published together (one volatile write
     * where it is made), with a generation that names that pair.
     */
    static final class Identity {
        final X509Certificate[] chain;
        final PrivateKey key;           // null: the delegate's key for the node's own alias (a key that renewal keeps)
        final long gen;
        Identity(X509Certificate[] chain, PrivateKey key, long gen) { this.chain = chain; this.key = key; this.gen = gen; }
    }

    /** Separates this node's alias from the generation of the identity a handshake was given. */
    static final String GEN = "#cresco-gen-";
    private static final int GENERATIONS_KEPT = 8;

    private volatile Supplier<Identity> currentIdentity;
    /** The delegate's own form of this node's alias, as it last chose it (the key of a self-made identity is under it). */
    private volatile String delegateOwnAlias;
    /** The identities handed to handshakes, by generation (the last {@value #GENERATIONS_KEPT}): a handshake's pair stays whole. */
    private final java.util.concurrent.ConcurrentSkipListMap<Long, Identity> handedOut = new java.util.concurrent.ConcurrentSkipListMap<>();

    /**
     * Present the identity {@code current} supplies for this node's own alias: the alias chosen for a handshake names the
     * generation in force at that moment, so the chain and the key the handshake then asks for are always one pair -- a hot
     * reload between the two lookups (a new key) can never pair a new chain with an old key or the reverse.
     */
    NodeIdentityKeyManager withCurrentIdentity(Supplier<Identity> current) {
        this.currentIdentity = current;
        return this;
    }

    /**
     * Does {@code alias} name this node's own entry: its alias, or the JDK key manager's form of it
     * ({@code <builder>.<alias>.<uid>}, what the PKIX key manager returns)?
     */
    boolean isOwn(String alias) {
        String own = ownAlias.get();
        if (alias == null || own == null) return false;
        if (alias.equalsIgnoreCase(own)) return true;                  // a PKCS12 store reports its aliases in lower case
        int a = alias.indexOf('.'), b = alias.lastIndexOf('.');
        return a > 0 && b > a && alias.substring(a + 1, b).equalsIgnoreCase(own);
    }

    /** {@code alias} bound to the identity in force now, when it names this node's own entry. */
    private String bind(String alias) {
        Supplier<Identity> s = currentIdentity;
        if (s == null || !isOwn(alias)) return alias;
        delegateOwnAlias = alias;
        Identity id = s.get();
        if (id == null || id.chain == null || id.chain.length == 0 || id.chain[0] == null) return alias;
        handedOut.putIfAbsent(id.gen, id);
        while (handedOut.size() > GENERATIONS_KEPT) handedOut.pollFirstEntry();
        return ownAlias.get() + GEN + id.gen;
    }

    /** The identity a generation-bound alias names, or null. */
    private Identity boundTo(String alias) {
        int at = alias == null ? -1 : alias.lastIndexOf(GEN);
        if (at < 0 || !alias.substring(0, at).equals(ownAlias.get())) return null;
        try {
            return handedOut.get(Long.parseLong(alias.substring(at + GEN.length())));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String plain(String alias) {
        int at = alias == null ? -1 : alias.lastIndexOf(GEN);
        return at < 0 ? alias : alias.substring(0, at);
    }

    private static X509Certificate[] trimmed(X509Certificate[] c) {
        int n = 0;
        while (n < c.length && c[n] != null) n++;
        return java.util.Arrays.copyOf(c, n);
    }

    NodeIdentityKeyManager(X509KeyManager delegate, Supplier<String> ownAlias) {
        this.d = delegate;
        this.ownAlias = ownAlias;
    }

    /** The JDK's choice when it made one, else the node's own identity when its key fits one of the requested types. */
    private String orOwn(String chosen, String[] keyTypes) {
        if (chosen != null) return chosen;
        String a = ownAlias.get();
        if (a == null) return null;
        PrivateKey k = getPrivateKey(a);
        X509Certificate[] c = getCertificateChain(a);
        if (k == null || c == null || c.length == 0) return null;
        if (keyTypes == null) return a;
        for (String t : keyTypes) {
            if (t == null) continue;
            int u = t.indexOf('_');                       // "EC_EC" / "RSA_EC" (TLS 1.2 forms): the key's own type first
            String base = u > 0 ? t.substring(0, u) : t;
            if (base.equalsIgnoreCase(k.getAlgorithm())) return a;
        }
        return null;
    }

    @Override
    public String chooseClientAlias(String[] keyTypes, Principal[] issuers, Socket socket) {
        return bind(orOwn(d.chooseClientAlias(keyTypes, issuers, socket), keyTypes));
    }

    @Override
    public String chooseEngineClientAlias(String[] keyTypes, Principal[] issuers, SSLEngine engine) {
        String chosen = d instanceof X509ExtendedKeyManager
                ? ((X509ExtendedKeyManager) d).chooseEngineClientAlias(keyTypes, issuers, engine)
                : d.chooseClientAlias(keyTypes, issuers, null);
        return bind(orOwn(chosen, keyTypes));
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) { return d.getClientAliases(keyType, issuers); }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) { return d.getServerAliases(keyType, issuers); }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) { return bind(d.chooseServerAlias(keyType, issuers, socket)); }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        return bind(d instanceof X509ExtendedKeyManager
                ? ((X509ExtendedKeyManager) d).chooseEngineServerAlias(keyType, issuers, engine)
                : d.chooseServerAlias(keyType, issuers, null));
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        Identity bound = boundTo(alias);
        if (bound != null) return trimmed(bound.chain);
        alias = plain(alias);
        Supplier<Identity> ci = currentIdentity;
        if (ci != null && alias != null && isOwn(alias)) {
            Identity id = ci.get();
            if (id != null && id.chain != null && id.chain.length > 0 && id.chain[0] != null) return trimmed(id.chain);
        }
        Supplier<X509Certificate[]> cur = currentChain;
        if (cur != null && alias != null && isOwn(alias)) {
            X509Certificate[] c = cur.get();
            if (c != null && c.length > 0 && c[0] != null) {
                int n = 0;
                while (n < c.length && c[n] != null) n++;
                return java.util.Arrays.copyOf(c, n);
            }
        }
        return d.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        Identity bound = boundTo(alias);
        if (bound != null && bound.key != null) return bound.key;
        if (bound != null && delegateOwnAlias != null) alias = delegateOwnAlias;   // the keystore's key for this node's entry
        alias = plain(alias);
        Supplier<Identity> ci = currentIdentity;
        if (bound == null && ci != null && alias != null && isOwn(alias)) {
            Identity id = ci.get();
            if (id != null && id.key != null) return id.key;
        }
        Supplier<PrivateKey> cur = currentKey;
        if (cur != null && alias != null && isOwn(alias)) {
            PrivateKey k = cur.get();
            if (k != null) return k;
        }
        return d.getPrivateKey(alias);
    }
}
