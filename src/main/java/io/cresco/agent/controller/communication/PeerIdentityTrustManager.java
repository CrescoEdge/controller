package io.cresco.agent.controller.communication;

import io.cresco.library.utilities.CLogger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

/**
 * The trust manager of one outbound broker connection (agent to its regional/global broker, or one end of a
 * broker-to-broker bridge): the node's PKIX trust (its current truststore), then {@link PeerIdentity} binding
 * to the peer the connection was opened to. A failed binding throws {@link PeerIdentityException} under
 * {@link PeerIdentity.Mode#ENFORCE}, which aborts the TLS handshake, and is logged once per handshake as an
 * ERROR naming the endpoint, the expected identity and what the certificate presented.
 *
 * <p>The PKIX trust manager is fetched from {@code pkix} at every handshake, so certificates added to the
 * truststore later (discovery exchanges, regional-CA enrollment) count without rebuilding the connection's
 * SSL context.
 */
public final class PeerIdentityTrustManager extends X509ExtendedTrustManager {

    /** A failed identity binding; a {@link CertificateException} so JSSE turns it into a handshake failure. */
    public static final class PeerIdentityException extends CertificateException {
        public PeerIdentityException(String msg) { super(msg); }
    }

    private final Supplier<X509TrustManager> pkix;
    private final PeerIdentity.Expected expected;
    private final PeerTrustRegistry registry;
    private final PeerIdentity.Mode mode;
    private final CLogger logger;
    private final String endpoint;

    /**
     * @param pkix     the node's current PKIX trust manager (called per handshake)
     * @param expected the peer this connection is meant to reach (null = unknown: refused under ENFORCE)
     * @param registry pinned keys learned in discovery (may be null)
     * @param endpoint a label for logs, e.g. "agent->broker 10.0.0.1:32010"
     */
    public PeerIdentityTrustManager(Supplier<X509TrustManager> pkix, PeerIdentity.Expected expected,
                                    PeerTrustRegistry registry, PeerIdentity.Mode mode, CLogger logger, String endpoint) {
        this.pkix = pkix;
        this.expected = expected;
        this.registry = registry;
        this.mode = (mode == null) ? PeerIdentity.Mode.ENFORCE : mode;
        this.logger = logger;
        this.endpoint = endpoint;
    }

    public PeerIdentity.Expected expected() { return expected; }

    private X509TrustManager delegate() throws CertificateException {
        X509TrustManager tm = pkix.get();
        if (tm == null) throw new CertificateException("no trust manager available (truststore not loaded)");
        return tm;
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate().checkServerTrusted(chain, authType);
        bind(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        X509TrustManager d = delegate();
        if (d instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) d).checkServerTrusted(chain, authType, socket);
        else d.checkServerTrusted(chain, authType);
        bind(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        X509TrustManager d = delegate();
        if (d instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) d).checkServerTrusted(chain, authType, engine);
        else d.checkServerTrusted(chain, authType);
        bind(chain);
    }

    // Client-side checks: these contexts only dial out, but keep PKIX semantics if one is ever used to accept.
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        X509TrustManager d = delegate();
        if (d instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) d).checkClientTrusted(chain, authType, socket);
        else d.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        X509TrustManager d = delegate();
        if (d instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) d).checkClientTrusted(chain, authType, engine);
        else d.checkClientTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        try {
            return delegate().getAcceptedIssuers();
        } catch (CertificateException e) {
            return new X509Certificate[0];
        }
    }

    private void bind(X509Certificate[] chain) throws CertificateException {
        if (mode == PeerIdentity.Mode.OFF) return;
        PeerIdentity.Verdict v = PeerIdentity.verify(expected, chain, registry);
        String want = (expected == null) ? "an unrecorded peer" : expected.toString();
        if (v.ok) {
            if (logger != null) logger.info("Peer identity verified: " + endpoint + " is " + want + " (" + v.reason + ")");
            return;
        }
        String msg = "PEER IDENTITY MISMATCH on " + endpoint + ": the connection was opened to " + want
                + " but " + v.reason;
        if (mode == PeerIdentity.Mode.WARN) {
            if (logger != null) logger.warn(msg + " -- ACCEPTED because " + PeerIdentity.MODE_KEY + "=warn");
            return;
        }
        if (logger != null) logger.error(msg + " -- connection REFUSED (" + PeerIdentity.MODE_KEY + "=enforce)");
        throw new PeerIdentityException(msg);
    }

    /** True if {@code t} or one of its causes is a refused identity binding. */
    public static boolean isIdentityRefusal(Throwable t) {
        for (int i = 0; t != null && i < 16; i++, t = t.getCause()) {
            if (t instanceof PeerIdentityException) return true;
        }
        return false;
    }
}
