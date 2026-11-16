package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A resumed TLS session skips the trust manager. With real handshakes, both ways: a peer whose key was revoked here, or
 * whose anchors were removed and whose key is not bound here, does not get back in by resuming its session -- a session
 * of this node's TLS contexts (its broker's, where the peers are clients; its own links' and bridges', where they are
 * servers) is judged again when the trust changes and invalidated when it is refused, so the peer's next connection makes
 * a full handshake and is refused there. A peer the trust still takes resumes as before.
 *
 * <p>Fails before the change: in each case the same client context resumed its session and was accepted (the JDK's
 * server kept nothing to take back -- its tickets were sealed -- and nothing invalidated a client's cache).
 */
class ResumedSessionTrustTest {

    private static final SecureRandom RND = new SecureRandom();

    /** Counts the decisions a TLS context asks of the node's trust manager. */
    private static final class Counting extends X509ExtendedTrustManager {
        final X509ExtendedTrustManager d;
        final AtomicInteger calls = new AtomicInteger();
        Counting(X509ExtendedTrustManager d) { this.d = d; }
        public void checkClientTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a); }
        public void checkServerTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a); }
        public void checkClientTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, s); }
        public void checkServerTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, s); }
        public void checkClientTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, e); }
        public void checkServerTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, e); }
        public X509Certificate[] getAcceptedIssuers() { return d.getAcceptedIssuers(); }
    }

    private static Counting nodeTrust(CertificateManager cm) {
        for (TrustManager t : cm.getTrustManagers()) if (t instanceof X509ExtendedTrustManager) return new Counting((X509ExtendedTrustManager) t);
        throw new AssertionError("no extended trust manager");
    }

    /** One connection; true when BOTH sides completed the handshake and exchanged a byte. */
    private static boolean connect(SSLServerSocket ss, SSLContext client) throws Exception {
        CompletableFuture<Boolean> server = CompletableFuture.supplyAsync(() -> {
            try (SSLSocket s = (SSLSocket) ss.accept()) {
                s.setSoTimeout(5000);
                s.startHandshake();
                if (s.getInputStream().read() != 1) return false;
                s.getOutputStream().write(7);
                s.getOutputStream().flush();
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        boolean clientOk;
        try (SSLSocket c = (SSLSocket) client.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), ss.getLocalPort())) {
            c.setSoTimeout(5000);
            c.startHandshake();
            c.getOutputStream().write(1);
            c.getOutputStream().flush();
            clientOk = c.getInputStream().read() == 7;
        } catch (Exception refused) {
            clientOk = false;
        }
        return server.get(10, TimeUnit.SECONDS) && clientOk;
    }

    private static SSLContext context(javax.net.ssl.KeyManager[] km, TrustManager[] tm) throws Exception {
        SSLContext c = SSLContext.getInstance("TLS");
        c.init(km, tm, RND);
        return c;
    }

    /** {@code client} connects twice: accepted both times, the second time WITHOUT a trust decision (resumed). */
    private static void resumes(SSLServerSocket ss, SSLContext client, Counting tm, String who) throws Exception {
        assertTrue(connect(ss, client), who + ": the first connection was refused");
        int decisions = tm.calls.get();
        assertTrue(connect(ss, client), who + ": the second connection was refused");
        assertEquals(decisions, tm.calls.get(), "precondition: " + who + "'s second connection resumed its session (no trust decision)");
    }

    @Test
    void aClientThisNodesBrokerNoLongerTrustsDoesNotResume() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        Counting tm = nodeTrust(cm);
        SSLContext broker = context(cm.getKeyManagers(), new TrustManager[]{tm});       // as ActiveBroker makes its context
        cm.registerTlsContext(broker);
        X509Certificate[] sc = cm.getPublicCertificate();
        try (SSLServerSocket ss = (SSLServerSocket) broker.getServerSocketFactory().createServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            ss.setNeedClientAuth(true);

            // (1) its anchors are removed, its key is bound nowhere: a full handshake refuses it -- so does its resumption
            TlsTestSupport.Id a3 = TlsTestSupport.selfRooted("a3", "t1", "rgA", "a3");
            assertTrue(cm.replaceTrustStoreEntries("rgA_a3", a3.chain));
            SSLContext held3 = context(a3.keyManagers(), TlsTestSupport.trustManagers(sc));
            resumes(ss, held3, tm, "a3");
            assertTrue(cm.replaceTrustStoreEntries("rgA_a3", new Certificate[0]));
            assertFalse(connect(ss, context(a3.keyManagers(), TlsTestSupport.trustManagers(sc))), "precondition: a full handshake of a3 is refused");
            assertFalse(connect(ss, held3), "a peer whose anchors were removed got back in by resuming its session");

            // (2) its key is revoked
            TlsTestSupport.Id a4 = TlsTestSupport.selfRooted("a4", "t1", "rgA", "a4");
            assertTrue(reg.bind("t1", "rgA", "a4", a4.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_a4", a4.chain));
            SSLContext held4 = context(a4.keyManagers(), TlsTestSupport.trustManagers(sc));
            resumes(ss, held4, tm, "a4");
            assertNull(reg.revoke(PeerTrustRegistry.fingerprint(a4.chain[0].getPublicKey().getEncoded()), "test"));
            assertFalse(connect(ss, held4), "a revoked peer got back in by resuming its session");

            // (3) its anchors are removed, its key is BOUND here: a full handshake still takes it, and so does its resumption
            TlsTestSupport.Id a5 = TlsTestSupport.selfRooted("a5", "t1", "rgA", "a5");
            assertTrue(reg.bind("t1", "rgA", "a5", a5.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_a5", a5.chain));
            SSLContext held5 = context(a5.keyManagers(), TlsTestSupport.trustManagers(sc));
            resumes(ss, held5, tm, "a5");
            assertTrue(cm.replaceTrustStoreEntries("rgA_a5", new Certificate[0]));
            int decisions = tm.calls.get();
            assertTrue(connect(ss, held5), "a peer the trust still takes (its bound key) was refused");
            assertEquals(decisions, tm.calls.get(), "a peer the trust still takes lost its session");
        }
    }

    @Test
    void aBrokerThisNodeNoLongerTrustsIsNotResumedTo() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        // the peer's broker: a plain JDK context (its tickets sealed, as any JDK server's), no client certificate asked
        TlsTestSupport.Id rc = TlsTestSupport.selfRooted("rc", "t1", "rgA", "rc");
        SSLContext peerBroker = context(rc.keyManagers(), TlsTestSupport.trustManagers(cm.getPublicCertificate()));
        Counting tm = nodeTrust(cm);
        SSLContext link = context(cm.getKeyManagers(), new TrustManager[]{tm});          // this node's link: one context, reused
        cm.registerTlsContext(link);
        try (SSLServerSocket ss = (SSLServerSocket) peerBroker.getServerSocketFactory().createServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            // (1) the broker's anchors are removed here, its key bound nowhere
            assertTrue(cm.replaceTrustStoreEntries("rgA_rc", rc.chain));
            resumes(ss, link, tm, "the link to rc");
            assertTrue(cm.replaceTrustStoreEntries("rgA_rc", new Certificate[0]));
            assertFalse(connect(ss, link), "this node resumed its session with a broker whose anchors it removed");

            // (2) the broker's key is revoked here (bound and anchored before)
            assertTrue(reg.bind("t1", "rgA", "rc", rc.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_rc", rc.chain));
            resumes(ss, link, tm, "the link to rc, bound");
            assertNull(reg.revoke(PeerTrustRegistry.fingerprint(rc.chain[0].getPublicKey().getEncoded()), "test"));
            assertFalse(connect(ss, link), "this node resumed its session with a broker whose key it revoked");
        }
    }
}
