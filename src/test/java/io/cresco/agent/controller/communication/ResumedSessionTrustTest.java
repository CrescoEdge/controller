package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.InetAddress;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A resumed TLS session skips the trust manager. No session whose peer this node's trust judged is ever resumed, in
 * either direction, with real handshakes:
 * <ul>
 *   <li>the node as a SERVER (its broker): its trust invalidates the session of every peer it accepts, inside the
 *       handshake that judged it, so nothing resumable exists at any moment -- not after a revocation that came while that
 *       handshake was in flight, not with TLS 1.2, not with BCJSSE -- and the broker's session cache stays empty however
 *       many peers connect;</li>
 *   <li>the node as a CLIENT (its own links, its bridges): every connection, reconnects included, has a TLS context of
 *       its own ({@link FreshSslContext}), so nothing is resumed whatever the broker hands out (a broker of an earlier
 *       release still issues tickets).</li>
 * </ul>
 * Several cases are the review's probes (rv-scale2): an in-flight handshake at a revocation on both sides, TLS 1.2, the
 * cache, and the production contexts (the agent's link through ActiveClient, a bridge's). Each fails before the change
 * (sessions were resumed, or swept only at the revocation, after which an in-flight handshake cached a ticket).
 */
class ResumedSessionTrustTest {

    private static final SecureRandom RND = new SecureRandom();

    /** Delegates to a trust manager, counts its decisions; when armed, after an ACCEPT, signals and waits to be released. */
    static final class Gated extends X509ExtendedTrustManager {
        final X509ExtendedTrustManager d;
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean armed;
        final CountDownLatch checked = new CountDownLatch(1), release = new CountDownLatch(1);
        Gated(X509ExtendedTrustManager d) { this.d = d; }
        private void gate() {
            if (!armed) return;
            armed = false;
            checked.countDown();
            try { release.await(20, TimeUnit.SECONDS); } catch (InterruptedException ignore) { }
        }
        public void checkClientTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a); gate(); }
        public void checkServerTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a); gate(); }
        public void checkClientTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, s); gate(); }
        public void checkServerTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, s); gate(); }
        public void checkClientTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, e); gate(); }
        public void checkServerTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, e); gate(); }
        public X509Certificate[] getAcceptedIssuers() { return d.getAcceptedIssuers(); }
    }

    private static Gated nodeTrust(CertificateManager cm) {
        for (TrustManager t : cm.getTrustManagers()) if (t instanceof X509ExtendedTrustManager) return new Gated((X509ExtendedTrustManager) t);
        throw new AssertionError("no extended trust manager");
    }

    /** The JDK's own trust over {@code anchors} (a broker of an earlier release: it hands out session tickets). */
    private static Gated jdkTrust(X509Certificate... anchors) throws Exception {
        return new Gated((X509ExtendedTrustManager) TlsTestSupport.trustManagers(anchors)[0]);
    }

    private static SSLContext context(String proto, Provider p, javax.net.ssl.KeyManager[] km, TrustManager[] tm) throws Exception {
        SSLContext c = p == null ? SSLContext.getInstance(proto) : SSLContext.getInstance(proto, p);
        c.init(km, tm, RND);
        return c;
    }

    private static SSLContext context(javax.net.ssl.KeyManager[] km, TrustManager[] tm) throws Exception {
        return context("TLS", null, km, tm);
    }

    /** One connection; the protocol both sides completed and exchanged a byte over, or null when either side refused. */
    private static String connect(SSLServerSocket ss, SSLContext client) throws Exception {
        CompletableFuture<Boolean> server = CompletableFuture.supplyAsync(() -> {
            try (SSLSocket s = (SSLSocket) ss.accept()) {
                s.setSoTimeout(15000);
                s.startHandshake();
                if (s.getInputStream().read() != 1) return false;
                s.getOutputStream().write(7);
                s.getOutputStream().flush();
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        String protocol = null;
        // by host name, as ActiveMQ dials: a client finds the session to resume by the peer's host and port (BCJSSE, by name only)
        try (SSLSocket c = (SSLSocket) client.getSocketFactory().createSocket("127.0.0.1", ss.getLocalPort())) {
            c.setSoTimeout(15000);
            c.startHandshake();
            c.getOutputStream().write(1);
            c.getOutputStream().flush();
            if (c.getInputStream().read() == 7) protocol = c.getSession().getProtocol();
        } catch (Exception refused) {
            protocol = null;
        }
        return server.get(30, TimeUnit.SECONDS) && protocol != null ? protocol : null;
    }

    private static boolean ok(SSLServerSocket ss, SSLContext client) throws Exception { return connect(ss, client) != null; }

    private static String fp(TlsTestSupport.Id id) { return PeerTrustRegistry.fingerprint(id.chain[0].getPublicKey().getEncoded()); }

    private static int cached(javax.net.ssl.SSLSessionContext c) { return Collections.list(c.getIds()).size(); }

    private static SSLServerSocket listen(SSLContext server, boolean needClientAuth) throws Exception {
        SSLServerSocket ss = (SSLServerSocket) server.getServerSocketFactory().createServerSocket(0, 200, InetAddress.getLoopbackAddress());
        ss.setNeedClientAuth(needClientAuth);
        return ss;
    }

    // ------------------------------------------------------------------ the node as a server (its broker)

    @Test
    void aClientTheBrokersTrustJudgedNeverResumesAndTheBrokerCachesNothingForIt() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        CertificateManager cm = e.getCertificateManager();
        Gated tm = nodeTrust(cm);
        SSLContext broker = context(cm.getKeyManagers(), new TrustManager[]{tm});           // as ActiveBroker makes its context
        TlsTestSupport.Id a5 = TlsTestSupport.selfRooted("a5", "t1", "rgA", "a5");
        assertTrue(cm.replaceTrustStoreEntries("rgA_a5", a5.chain));
        X509Certificate[] sc = cm.getPublicCertificate();
        int n = 40;
        try (SSLServerSocket ss = listen(broker, true)) {
            SSLContext held = context(a5.keyManagers(), TlsTestSupport.trustManagers(sc)); // one client context, reused
            for (int i = 0; i < n; i++) assertEquals("TLSv1.3", connect(ss, held));
            assertEquals(n, tm.calls.get(), "a connection of a client the broker's trust judged before was resumed (no trust decision)");
            // each with a context of its own, as a node's link makes them: nothing is cached for any (finding 6 of rv-scale2: 2 per handshake)
            for (int i = 0; i < n; i++) assertTrue(ok(ss, context(a5.keyManagers(), TlsTestSupport.trustManagers(sc))));
            assertEquals(0, cached(broker.getServerSessionContext()), "the broker cached sessions of judged clients");
        }
    }

    @Test
    void aClientRevokedWhileItsHandshakeIsInFlightDoesNotResumeAfterwards() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        Gated tm = nodeTrust(cm);
        SSLContext broker = context(cm.getKeyManagers(), new TrustManager[]{tm});
        X509Certificate[] sc = cm.getPublicCertificate();
        try (SSLServerSocket ss = listen(broker, true)) {
            TlsTestSupport.Id a4 = TlsTestSupport.selfRooted("a4", "t1", "rgA", "a4");
            assertTrue(reg.bind("t1", "rgA", "a4", a4.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_a4", a4.chain));
            SSLContext held4 = context(a4.keyManagers(), TlsTestSupport.trustManagers(sc));
            tm.armed = true;                                         // the broker's trust accepts a4, then the key is revoked
            CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(() -> {
                try { return ok(ss, held4); } catch (Exception ex) { return false; }
            });
            assertTrue(tm.checked.await(10, TimeUnit.SECONDS), "the handshake never reached the trust decision");
            assertNull(reg.revoke(fp(a4), "test"));
            reg.awaitRevocations();
            tm.release.countDown();
            assertTrue(first.get(30, TimeUnit.SECONDS), "precondition: the in-flight handshake (judged before the revocation) completed");
            assertEquals(0, cached(broker.getServerSessionContext()));
            int decisions = tm.calls.get();
            assertFalse(ok(ss, held4), "a client revoked while its handshake was in flight resumed afterwards");
            assertEquals(decisions + 1, tm.calls.get(), "its next connection was not judged");
        }
    }

    @Test
    void tls12IsNotResumedEither() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        Gated tm = nodeTrust(cm);
        SSLContext broker = context(cm.getKeyManagers(), new TrustManager[]{tm});
        try (SSLServerSocket ss = listen(broker, true)) {
            TlsTestSupport.Id a4 = TlsTestSupport.selfRooted("a4", "t1", "rgA", "a4");
            assertTrue(reg.bind("t1", "rgA", "a4", a4.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_a4", a4.chain));
            SSLContext c12 = context("TLSv1.2", null, a4.keyManagers(), TlsTestSupport.trustManagers(cm.getPublicCertificate()));
            assertEquals("TLSv1.2", connect(ss, c12));
            assertEquals("TLSv1.2", connect(ss, c12));
            assertEquals(2, tm.calls.get(), "a TLS 1.2 client of the broker resumed (no trust decision)");
            assertNull(reg.revoke(fp(a4), "test"));
            reg.awaitRevocations();
            assertFalse(ok(ss, c12), "TLS 1.2: a revoked client got in");
        }
    }

    // ------------------------------------------------------------------ the node as a client (its links, its bridges)

    /** A broker of an earlier release: the JDK's trust and its session tickets. */
    private static final class TicketBroker implements AutoCloseable {
        final TlsTestSupport.Id id = TlsTestSupport.selfRooted("rc", "t1", "rgA", "rc");
        final Gated tm;
        final SSLServerSocket ss;
        TicketBroker(CertificateManager node) throws Exception {
            tm = jdkTrust(node.getPublicCertificate());
            ss = listen(context(id.keyManagers(), new TrustManager[]{tm}), true);
        }
        public void close() throws Exception { ss.close(); }
    }

    @Test
    void theNodesLinkNeverResumesAndOneRevokedWhileItsHandshakeIsInFlightIsRefusedAtTheNext() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        try (TicketBroker rc = new TicketBroker(cm)) {
            assertTrue(reg.bind("t1", "rgA", "rc", rc.id.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_rc", rc.id.chain));
            Gated tm = nodeTrust(cm);
            // the TLS context of a link: what LinkTlsConnectionFactory hands a failover transport, asked at each connection
            SslContext link = new FreshSslContext(cm.getKeyManagers(), new TrustManager[]{tm}, RND, null);
            assertTrue(ok(rc.ss, link.getSSLContext()));
            assertTrue(ok(rc.ss, link.getSSLContext()));
            assertEquals(2, rc.tm.calls.get(), "the link's reconnect resumed a session (the broker made no trust decision)");

            tm.armed = true;                                         // the node's trust accepts rc, then rc's key is revoked here
            CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(() -> {
                try { return ok(rc.ss, link.getSSLContext()); } catch (Exception ex) { return false; }
            });
            assertTrue(tm.checked.await(10, TimeUnit.SECONDS));
            assertNull(reg.revoke(fp(rc.id), "test"));
            reg.awaitRevocations();
            tm.release.countDown();
            first.get(30, TimeUnit.SECONDS);
            assertFalse(ok(rc.ss, link.getSSLContext()), "the node's link reconnected to a broker revoked while its handshake was in flight");
        }
    }

    @Test
    void aBridgesReconnectNeverResumes() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        try (TicketBroker rc = new TicketBroker(cm)) {
            assertNull(cm.getPeerTrust().pin("rgA", "rc", rc.id.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, true));
            assertTrue(cm.replaceTrustStoreEntries("rgA_rc", rc.id.chain));
            SslContext bridge = ActiveBroker.bridgeSslContext(cm, new PeerIdentity.Expected("rgA", "rc"), "bridge -> rc");
            for (int i = 0; i < 3; i++) assertTrue(ok(rc.ss, bridge.getSSLContext()), "the bridge's connection " + i);
            assertEquals(3, rc.tm.calls.get(), "a bridge's reconnect resumed a session (the broker made no trust decision)");
        }
    }

    /** The agent's real link (ActiveClient, its factory, a failover transport) to a broker that hands out tickets. */
    @Test
    void theAgentsRealLinkMakesAFullHandshakeWhenItsFailoverTransportReconnects() throws Exception {
        ControllerEngine a1 = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = a1.getCertificateManager();
        TlsTestSupport.Id rcId = TlsTestSupport.selfRooted("rc", "t1", "rgA", "rc");
        assertNull(cm.getPeerTrust().pin("rgA", "rc", rcId.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertTrue(cm.replaceTrustStoreEntries("rgA_rc", rcId.chain));
        Gated brokerTrust = jdkTrust(cm.getPublicCertificate());
        SslBrokerService b = new SslBrokerService();
        b.setBrokerName("rgA_rc");
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setSslContext(new SslContext(rcId.keyManagers(), new TrustManager[]{brokerTrust}, RND));
        TransportConnector tc = b.addConnector("ssl://127.0.0.1:0?needClientAuth=true");
        b.start();
        b.waitUntilStarted();
        try {
            String uri = "failover:(ssl://127.0.0.1:" + tc.getConnectUri().getPort() + "?verifyHostName=false)"
                    + "?maxReconnectAttempts=20&initialReconnectDelay=100&useExponentialBackOff=false";
            a1.getActiveClient().expectPeer(uri, "rgA", "rc");
            org.apache.activemq.ActiveMQSession s = a1.getActiveClient().createDedicatedSession(uri, false, jakarta.jms.Session.AUTO_ACKNOWLEDGE, true);
            assertNotNull(s, "the link did not connect");
            assertEquals(new PeerIdentity.Expected("rgA", "rc"), a1.getActiveClient().boundPeer(s.getConnection()));
            int before = brokerTrust.calls.get();
            assertTrue(before >= 1);
            // the broker drops the connection: the failover transport reconnects with the context it was made with
            java.util.Set<TransportConnection> old = new java.util.HashSet<>(tc.getConnections());
            assertEquals(1, old.size());
            for (TransportConnection c : old) c.stop();
            boolean back = false;
            for (int i = 0; i < 400 && !back; i++) {
                Thread.sleep(25);
                for (TransportConnection c : tc.getConnections()) if (!old.contains(c)) back = true;
            }
            assertTrue(back, "the link did not reconnect");
            assertEquals(before + 1, brokerTrust.calls.get(), "the link's reconnect resumed its session: the broker made no trust decision");
            s.getConnection().close();
        } finally {
            a1.getActiveClient().shutdown();
            b.stop();
        }
    }

    // ------------------------------------------------------------------ BCJSSE (crypto_fips_tls=bcjsse)

    private static Provider bcjsse() throws Exception {
        Provider fips = (Provider) Class.forName("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider").getConstructor().newInstance();
        return (Provider) Class.forName("org.bouncycastle.jsse.provider.BouncyCastleJsseProvider").getConstructor(Provider.class).newInstance(fips);
    }

    @Test
    void withBcjsseAJudgedClientNeverResumesAndANodeOfTheFamilyNegotiatesTls13() throws Exception {
        Provider bc = bcjsse();
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        Gated tm = nodeTrust(cm);
        SSLContext broker = context("TLS", bc, cm.getKeyManagers(), new TrustManager[]{tm});
        TlsTestSupport.Id a4 = TlsTestSupport.selfRooted("a4", "t1", "rgA", "a4");
        assertTrue(reg.bind("t1", "rgA", "a4", a4.kp.getPublic(), "discovery").ok);
        assertTrue(cm.replaceTrustStoreEntries("rgA_a4", a4.chain));
        try (SSLServerSocket ss = listen(broker, true)) {
            SSLContext bcClient = context("TLS", bc, a4.keyManagers(), TlsTestSupport.trustManagers(cm.getPublicCertificate()));
            assertEquals("TLSv1.3", connect(ss, bcClient), "BCJSSE to BCJSSE");
            assertEquals("TLSv1.3", connect(ss, bcClient));
            SSLContext jdkClient = context(a4.keyManagers(), TlsTestSupport.trustManagers(cm.getPublicCertificate()));
            assertEquals("TLSv1.3", connect(ss, jdkClient), "SunJSSE to BCJSSE");
            assertEquals(3, tm.calls.get(), "a BCJSSE broker resumed a judged client's session");
            SSLContext bc12 = context("TLSv1.2", bc, a4.keyManagers(), TlsTestSupport.trustManagers(cm.getPublicCertificate()));
            assertEquals("TLSv1.2", connect(ss, bc12));
            assertEquals("TLSv1.2", connect(ss, bc12));
            assertEquals(5, tm.calls.get(), "a BCJSSE broker resumed a judged TLS 1.2 client's session");
            assertNull(reg.revoke(fp(a4), "test"));
            reg.awaitRevocations();
            assertFalse(ok(ss, bcClient), "BCJSSE: a revoked client got in");
            assertFalse(ok(ss, bc12), "BCJSSE, TLS 1.2: a revoked client got in");
        }
    }
}
