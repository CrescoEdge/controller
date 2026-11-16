package io.cresco.agent.controller.communication;

import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.JMSExceptionSupport;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.security.SecureRandom;

/**
 * The connection factory of this node's own links to a broker (an agent's to its region controller's broker): what
 * {@link ActiveMQSslConnectionFactory} does with key and trust managers -- a TLS context of its own for each connection,
 * which a failover transport keeps for its reconnects -- with that context registered at the node's
 * {@link CertificateManager} ({@link CertificateManager#registerTlsContext}). A failover reconnect may resume the
 * link's TLS session, which skips the trust manager; registered, a session whose broker this node's trust refuses now
 * (its key revoked here, its anchors gone) is invalidated and not resumed.
 *
 * <p>The managers are kept here, not in the parent's fields: with those set the parent makes the context itself, where
 * nobody can reach it. With them unset it uses the context set for the calling thread, which is this one.
 */
final class RegisteredTlsConnectionFactory extends ActiveMQSslConnectionFactory {

    private final CertificateManager registry;
    private volatile KeyManager[] keyManagers;
    private volatile TrustManager[] trustManagers;
    private volatile SecureRandom random;

    RegisteredTlsConnectionFactory(String brokerURL, CertificateManager registry) {
        super(brokerURL);
        this.registry = registry;
    }

    /** The key and trust managers of every connection made from now on (as {@code setKeyAndTrustManagers}). */
    void setTls(KeyManager[] km, TrustManager[] tm, SecureRandom random) {
        this.keyManagers = km;
        this.trustManagers = tm;
        this.random = random;
    }

    @Override
    protected Transport createTransport() throws JMSException {
        if (keyManagers == null && trustManagers == null) return super.createTransport();
        SslContext existing = SslContext.getCurrentSslContext();
        try {
            SslContext ctx = new SslContext(keyManagers, trustManagers, random);
            if (registry != null) registry.registerTlsContext(ctx.getSSLContext());
            SslContext.setCurrentSslContext(ctx);
            return super.createTransport();
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        } finally {
            SslContext.setCurrentSslContext(existing);
        }
    }
}
