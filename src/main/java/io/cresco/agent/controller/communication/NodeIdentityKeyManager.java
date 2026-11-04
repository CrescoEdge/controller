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
        return orOwn(d.chooseClientAlias(keyTypes, issuers, socket), keyTypes);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyTypes, Principal[] issuers, SSLEngine engine) {
        String chosen = d instanceof X509ExtendedKeyManager
                ? ((X509ExtendedKeyManager) d).chooseEngineClientAlias(keyTypes, issuers, engine)
                : d.chooseClientAlias(keyTypes, issuers, null);
        return orOwn(chosen, keyTypes);
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) { return d.getClientAliases(keyType, issuers); }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) { return d.getServerAliases(keyType, issuers); }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) { return d.chooseServerAlias(keyType, issuers, socket); }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        return d instanceof X509ExtendedKeyManager
                ? ((X509ExtendedKeyManager) d).chooseEngineServerAlias(keyType, issuers, engine)
                : d.chooseServerAlias(keyType, issuers, null);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        Supplier<X509Certificate[]> cur = currentChain;
        if (cur != null && alias != null && alias.equals(ownAlias.get())) {
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
        Supplier<PrivateKey> cur = currentKey;
        if (cur != null && alias != null && alias.equals(ownAlias.get())) {
            PrivateKey k = cur.get();
            if (k != null) return k;
        }
        return d.getPrivateKey(alias);
    }
}
