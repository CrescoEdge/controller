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
        } catch (CertificateException e) {
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
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        check(chain, "server", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkServerTrusted(chain, authType, socket);
            else pkix.checkServerTrusted(chain, authType);
        });
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        check(chain, "client", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkClientTrusted(chain, authType, engine);
            else pkix.checkClientTrusted(chain, authType);
        });
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        check(chain, "server", () -> {
            if (pkix instanceof X509ExtendedTrustManager) ((X509ExtendedTrustManager) pkix).checkServerTrusted(chain, authType, engine);
            else pkix.checkServerTrusted(chain, authType);
        });
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return pkix.getAcceptedIssuers();
    }
}
