package io.cresco.agent.controller.communication;

import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.JMSExceptionSupport;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.security.SecureRandom;

/**
 * The connection factory of this node's own links to a broker (an agent's to its region controller's broker): what
 * {@link ActiveMQSslConnectionFactory} does with key and trust managers, with two differences.
 * <ul>
 *   <li>Each connection gets a {@link FreshSslContext}, which a failover transport keeps for its reconnects: every
 *       connection attempt makes its own JSSE context, so a reconnect never resumes a TLS session -- whose broker would
 *       not be judged again -- and is a full handshake, judged by this node's trust as it is then.</li>
 *   <li>It knows the peer its connections are bound to ({@link #boundPeer}): the one recorded for its URI when it was made
 *       ({@link ActiveClient#expectPeer}), which its trust manager holds every TLS connection it makes to
 *       ({@link PeerIdentityTrustManager}). A later {@code expectPeer} for the same URI makes a new factory for new
 *       connections and does not re-label the connections of this one.</li>
 * </ul>
 * The managers are kept here, not in the parent's fields: with those set the parent makes the context itself.
 */
final class LinkTlsConnectionFactory extends ActiveMQSslConnectionFactory {

    private final PeerIdentity.Expected boundPeer;
    private volatile KeyManager[] keyManagers;
    private volatile TrustManager[] trustManagers;
    private volatile SecureRandom random;

    LinkTlsConnectionFactory(String brokerURL, PeerIdentity.Expected boundPeer) {
        super(brokerURL);
        this.boundPeer = boundPeer;
    }

    /** The peer every TLS connection of this factory is bound to (null: none was recorded for its URI). */
    PeerIdentity.Expected boundPeer() { return boundPeer; }

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
            SslContext.setCurrentSslContext(new FreshSslContext(keyManagers, trustManagers, random, null));
            return super.createTransport();
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        } finally {
            SslContext.setCurrentSslContext(existing);
        }
    }

    /** The connections of this factory carry the peer they are bound to ({@link ActiveClient#boundPeer}). */
    @Override
    protected ActiveMQConnection createActiveMQConnection(Transport transport, org.apache.activemq.management.JMSStatsImpl stats) throws Exception {
        ActiveMQConnection c = super.createActiveMQConnection(transport, stats);
        BoundPeers.put(c, boundPeer);
        return c;
    }

    /** The peer each live connection of these factories is bound to (weak: a connection that is gone goes). */
    static final class BoundPeers {
        private static final java.util.Map<ActiveMQConnection, PeerIdentity.Expected> PEERS =
                java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());
        private BoundPeers() { }
        static void put(ActiveMQConnection c, PeerIdentity.Expected peer) { if (c != null && peer != null) PEERS.put(c, peer); }
        static PeerIdentity.Expected of(ActiveMQConnection c) { return c == null ? null : PEERS.get(c); }
    }
}
