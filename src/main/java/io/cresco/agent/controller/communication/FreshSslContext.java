package io.cresco.agent.controller.communication;

import org.apache.activemq.broker.SslContext;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

/**
 * The TLS context of a connection this node DIALS (its own link to a broker, a bridge): a JSSE context made anew each time
 * ActiveMQ asks for one -- each connection and each reconnect of a failover transport -- so no connection of the node
 * ever offers to resume a session another one made.
 *
 * <p><b>Why.</b> A resumed session skips the trust manager: a broker revoked here, or whose anchors were removed and
 * whose key is not bound here, was accepted again when a reconnect resumed the session it had before (TLS 1.3 tickets
 * and TLS 1.2 session ids alike), and a session made by a handshake judged a moment before a revocation could be resumed
 * after it whatever was swept at the revocation. A JSSE client keeps what it resumes with in its context's session
 * cache; a context used for one connection has nothing to resume with. That holds for every TLS provider (SunJSSE,
 * BCJSSE), and whatever the broker hands out. (The server side: {@link BoundKeyTrustManager} invalidates every session
 * whose peer it judged, so this node's broker hands out nothing to resume with either.)
 *
 * <p><b>Cost.</b> A context is made per connection ({@link #made} counts them); what is lost is the abbreviated handshake
 * of a reconnect -- a full handshake instead. A node's first connection to a broker was a full handshake in any case
 * (ActiveMQ made a context per connection already); its reconnects to a broker that stayed up are what pay.
 */
final class FreshSslContext extends SslContext {

    private static final AtomicLong made = new AtomicLong();
    private final UnaryOperator<SSLContext> wrap;

    /**
     * @param wrap applied to each context made (a bridge's records its sockets: {@link RecordingSslContext}); null: none
     */
    FreshSslContext(KeyManager[] km, TrustManager[] tm, SecureRandom random, UnaryOperator<SSLContext> wrap) {
        super(km, tm, random);
        this.wrap = wrap;
    }

    /** A new JSSE context, initialised with the key and trust managers of this one as they are now. */
    @Override
    public SSLContext getSSLContext() throws NoSuchProviderException, NoSuchAlgorithmException, KeyManagementException {
        SSLContext c = provider == null ? SSLContext.getInstance(protocol) : SSLContext.getInstance(protocol, provider);
        c.init(getKeyManagersAsArray(), getTrustManagersAsArray(), getSecureRandom());
        made.incrementAndGet();
        return wrap == null ? c : wrap.apply(c);
    }

    /** There is no one context to replace: a new identity's key managers go in {@link #setKeyManagers}. */
    @Override
    public synchronized void setSSLContext(SSLContext sslContext) {
        throw new UnsupportedOperationException("a FreshSslContext makes a JSSE context per connection: set its key and trust managers");
    }

    /** The key managers of every context made from now on (a node whose identity was re-issued presents the new one). */
    void useKeyManagers(KeyManager[] km) {
        setKeyManagers(km == null ? new java.util.ArrayList<>() : new java.util.ArrayList<>(Arrays.asList(km)));
    }

    /** JSSE contexts made so far, by every instance (tests, diagnostics). */
    static long made() { return made.get(); }
}
