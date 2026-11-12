package io.cresco.agent.controller.communication;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * PKIX over the node's truststore AS IT IS at each handshake, without building anything from the whole store.
 *
 * <p>One instance serves every TLS context of the node for its whole life: a context is never re-initialised because the
 * truststore changed. Each decision takes from the {@link TrustAnchorIndex} the entries that can anchor the chain it was
 * shown and hands the chain to the platform's own PKIX trust manager made over exactly those entries -- the same
 * algorithm, provider, constraints and checks as before, on the anchors PKIX could have used anyway. So a certificate
 * recorded a moment ago is trusted by the next handshake, one removed a moment ago is not, and the cost of a decision
 * does not grow with the number of entries.
 */
final class LivePkixTrustManager extends X509ExtendedTrustManager {

    private final java.util.function.Supplier<TrustAnchorIndex> index;

    /** @param index the truststore's index now (the truststore, and its index with it, may be replaced as a whole) */
    LivePkixTrustManager(java.util.function.Supplier<TrustAnchorIndex> index) {
        this.index = index;
    }

    private X509ExtendedTrustManager pkixFor(X509Certificate[] chain) throws CertificateException {
        if (chain == null || chain.length == 0) throw new IllegalArgumentException("null or zero-length certificate chain");
        List<X509Certificate> anchors = index.get().anchorsFor(chain);
        if (anchors.isEmpty()) {
            throw new CertificateException("no entry of this node's truststore is, or issued, a certificate of the presented chain ("
                    + (chain[0] == null ? "?" : chain[0].getSubjectX500Principal().getName()) + ")");
        }
        try {
            KeyStore ks = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
            ks.load(null, null);
            int i = 0;
            for (X509Certificate a : anchors) ks.setCertificateEntry("a" + (i++), a);
            TrustManagerFactory f = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            f.init(ks);
            for (TrustManager tm : f.getTrustManagers()) {
                if (tm instanceof X509ExtendedTrustManager) return (X509ExtendedTrustManager) tm;
            }
        } catch (Exception e) {
            throw new CertificateException("the trust decision could not be made: " + e, e);
        }
        throw new CertificateException("the platform offers no X.509 trust manager");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        pkixFor(chain).checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        pkixFor(chain).checkServerTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        pkixFor(chain).checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        pkixFor(chain).checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        pkixFor(chain).checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        pkixFor(chain).checkServerTrusted(chain, authType, engine);
    }

    /** Every certificate of the truststore (a walk; asked for a listing or a CA hint, never for a trust decision). */
    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return index.get().all();
    }
}
