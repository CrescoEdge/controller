package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.utilities.CLogger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This node's TLS trust decision for broker connections, both directions: PKIX over the node's truststore, or else a
 * chain whose leaf carries the key BOUND to the identity it names in the peer trust registry
 * ({@link PeerTrustRegistry#boundByKey}).
 *
 * <p>Why: under per-node trust a peer's self-rooted chain is added to the truststore when discovery binds it, and the
 * truststore is in memory. After a restart the binding persists (the registry) but the anchor is gone, so with client
 * certificates required every agent's (and every bridge's) reconnect failed the handshake until it ran discovery again.
 * Accepting the bound key is exactly as strong as the pin: the handshake proves possession of that key, and the broker
 * still binds the connection to the name only when the key is the one bound to it ({@link CrescoAuthorizationBroker}).
 */
final class BoundKeyTrustManager extends X509ExtendedTrustManager {

    private final X509TrustManager pkix;
    private final PeerTrustRegistry registry;
    private final CLogger logger;
    private static final Set<String> announced = ConcurrentHashMap.newKeySet();

    BoundKeyTrustManager(X509TrustManager pkix, PeerTrustRegistry registry, CLogger logger) {
        this.pkix = pkix;
        this.registry = registry;
        this.logger = logger;
    }

    private interface Check { void run() throws CertificateException; }

    private void check(X509Certificate[] chain, String direction, Check pkixCheck) throws CertificateException {
        try {
            pkixCheck.run();
            // PKIX accepted it, but the registry still decides for a Cresco identity: a key that is not (or no longer) the
            // one bound to the name the certificate carries is refused at once -- its chain may still be an anchor until the
            // truststore is rebuilt, and that must not keep a rebound-away key in
            String refused = registry == null ? null : registry.refusedKey(chain);
            if (refused != null) {
                if (logger != null) logger.warn("TLS " + direction + " certificate REFUSED: " + refused);
                throw new CertificateException(refused);
            }
        } catch (CertificateException e) {
            if (registry != null && registry.refusedKey(chain) != null) throw e;
            CrescoIdentity bound = registry == null ? null : registry.boundByKey(chain);
            if (bound == null) throw e;
            String fp = PeerTrustRegistry.fingerprint(chain[0].getPublicKey().getEncoded());
            if (logger != null && announced.size() < 10_000 && announced.add(fp)) {
                logger.info("TLS " + direction + " " + bound + " trusted by its bound key " + PeerIdentity.shortFp(fp)
                        + " (its chain is not in the truststore, e.g. after a restart)");
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        check(chain, "client", () -> pkix.checkClientTrusted(chain, authType));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        check(chain, "server", () -> pkix.checkServerTrusted(chain, authType));
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        check(chain, "client", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkClientTrusted(chain, authType, socket);
            else pkix.checkClientTrusted(chain, authType);
        });
        notResumable(handshakeSession(socket));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        check(chain, "server", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkServerTrusted(chain, authType, socket);
            else pkix.checkServerTrusted(chain, authType);
        });
        notResumable(handshakeSession(socket));
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        check(chain, "client", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkClientTrusted(chain, authType, engine);
            else pkix.checkClientTrusted(chain, authType);
        });
        notResumable(handshakeSession(engine));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        check(chain, "server", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkServerTrusted(chain, authType, engine);
            else pkix.checkServerTrusted(chain, authType);
        });
        notResumable(handshakeSession(engine));
    }

    /**
     * The session of a handshake whose peer this trust has just ACCEPTED is invalidated: it is never resumed. A resumed
     * session skips the trust manager, so a peer revoked here, or whose anchors were removed and whose key is not bound
     * here, got back in by resuming -- for the 24 h a session lives, and also after a revocation that came while the
     * handshake that made the session was in flight (whatever is looked for at a revocation, such a session is cached
     * after it). Invalidated here, inside the handshake that judged it, the session is resumable at no moment: a JDK
     * server issues no ticket for it and caches nothing (TLS 1.3 and 1.2: the session is not rejoinable when its ticket or
     * cache entry would be made), so the peer's next connection is a full handshake, judged by this trust as it is then.
     * {@code SSLSession.invalidate} is the platform's contract ("future connections will not be able to resume or join
     * this session"), whatever the TLS provider; a provider that does not let a handshake session be invalidated gets its
     * handshake refused (fail safe), never a resumable session. The client side of the node's own links and bridges does
     * not depend on the peer: each connection there has a TLS context of its own ({@link FreshSslContext}).
     *
     * <p>TLS 1.2 included (security over speed): a peer that negotiates it makes a full handshake at each connection.
     * Cresco's own nodes negotiate TLS 1.3 with each other (SunJSSE offers it by default and is TLS 1.3 only in the
     * production profile; BCJSSE, crypto_fips_tls=bcjsse, offers 1.3 and 1.2 and negotiates 1.3 with any node of this
     * family): TLS 1.2 is negotiated only with a peer that does not offer 1.3.
     */
    static void notResumable(javax.net.ssl.SSLSession handshakeSession) throws CertificateException {
        if (handshakeSession == null) return;           // (no handshake in progress: nothing is made that could be resumed)
        try {
            handshakeSession.invalidate();
        } catch (RuntimeException e) {
            throw new CertificateException("this TLS provider does not let the session be made non-resumable ("
                    + e.getClass().getSimpleName() + "): a session whose peer is judged here is never resumable, so the handshake is refused");
        }
    }

    /** The session of the handshake in progress on {@code socket} / {@code engine} (null when there is none). */
    static javax.net.ssl.SSLSession handshakeSession(Socket socket) {
        return socket instanceof javax.net.ssl.SSLSocket ? ((javax.net.ssl.SSLSocket) socket).getHandshakeSession() : null;
    }

    static javax.net.ssl.SSLSession handshakeSession(SSLEngine engine) {
        return engine == null ? null : engine.getHandshakeSession();
    }

    /**
     * No CA hints. A server's CertificateRequest names the issuers it accepts; a JDK key manager then presents a certificate
     * only if its issuer is among them. After a restart a bound peer's self-rooted chain is no longer in the truststore (it is
     * trusted by its bound key instead), so a node still running an older release -- with the JDK key manager, not
     * {@link NodeIdentityKeyManager} -- presented NO certificate and was refused. An empty list means "any issuer": every
     * client presents its certificate and is judged here. The hints carry no security; they only steer the client's choice.
     */
    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    /** The CA certificates the underlying PKIX trust accepts (the hints the server no longer sends). */
    X509Certificate[] trustedIssuers() {
        return pkix.getAcceptedIssuers();
    }
}
