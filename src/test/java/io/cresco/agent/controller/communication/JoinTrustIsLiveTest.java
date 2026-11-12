package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The trust a broker connection is judged by is the truststore as it is at that handshake: a certificate recorded a moment
 * ago is trusted, one removed a moment ago is not, without a TLS context being rebuilt -- and the decision is the one PKIX
 * over the whole truststore gives. Before, a controller rebuilt its broker's trust from the whole truststore and
 * re-initialised the broker's TLS context at every join: a join cost more the more had joined (DGX job 268125: 3.3 ms of
 * the 4.2 ms a join took the global's discovery responder with 750 agents joined).
 */
class JoinTrustIsLiveTest {

    private static X509TrustManager held(CertificateManager cm) {
        for (TrustManager tm : cm.getTrustManagers()) if (tm instanceof X509TrustManager) return (X509TrustManager) tm;
        throw new AssertionError("no X.509 trust manager");
    }

    private static boolean accepts(X509TrustManager tm, X509Certificate[] chain, boolean client) {
        try {
            if (client) tm.checkClientTrusted(chain, "RSA"); else tm.checkServerTrusted(chain, "RSA");
            return true;
        } catch (CertificateException refused) {
            return false;
        }
    }

    /** Fails before the change: the trust managers a TLS context holds were a copy of the truststore made when they were built. */
    @Test
    void aCertificateRecordedAMomentAgoIsTrustedByTheTrustATlsContextAlreadyHolds() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        X509TrustManager held = held(cm);                    // what the broker's TLS context was given when it started
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertFalse(accepts(held, a1.chain, true), "a chain nobody recorded was trusted");

        long[] before = cm.trustIndexCounts();
        assertTrue(cm.replaceTrustStoreEntries("rgA_a1", a1.chain), "the joiner's chain was not recorded");   // what a join does
        assertTrue(accepts(held, a1.chain, true), "the joiner's chain, recorded a moment ago, is not trusted by the trust the"
                + " broker already holds: its TLS context would have to be rebuilt at every join");
        assertEquals(before[2] + a1.chain.length, cm.trustIndexCounts()[2], "entries indexed");

        // the joiner comes back with another chain (re-installed): its former chain is no anchor any more, at once
        TlsTestSupport.Id again = TlsTestSupport.selfRooted("a1-again", "t1", "rgA", "a1");
        assertTrue(cm.replaceTrustStoreEntries("rgA_a1", again.chain));
        assertTrue(accepts(held, again.chain, true));
        assertFalse(accepts(held, a1.chain, true), "a chain that was replaced is still trusted");
        assertEquals(before[2] + again.chain.length, cm.trustIndexCounts()[2], "the former chain's entries were not removed");
    }

    @Test
    void theDecisionIsTheOnePkixOverTheWholeTruststoreGives() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        List<X509Certificate> anchors = new ArrayList<>(List.of(cm.getPublicCertificate()));
        List<TlsTestSupport.Id> recorded = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            TlsTestSupport.Id id = TlsTestSupport.selfRooted("n" + i, "t1", "rgA", "n" + i);
            recorded.add(id);
            cm.replaceTrustStoreEntries("rgA_n" + i, id.chain);
            anchors.addAll(List.of(id.chain));
        }
        // the oracle: the platform's PKIX trust manager over every entry (what getTrustManagers built before, by a walk)
        X509TrustManager whole = (X509TrustManager) TlsTestSupport.trustManagers(anchors.toArray(new X509Certificate[0]))[0];
        X509TrustManager live = new LivePkixTrustManagerProbe(cm).pkix();

        List<X509Certificate[]> shown = new ArrayList<>();
        for (TlsTestSupport.Id id : recorded) {
            shown.add(id.chain);                                              // leaf and its root
            shown.add(new X509Certificate[]{id.chain[0]});                    // the leaf alone: its issuer is an entry
            shown.add(new X509Certificate[]{id.chain[1]});                    // an entry itself
        }
        TlsTestSupport.Id stranger = TlsTestSupport.selfRooted("stranger", "t1", "rgA", "x1");
        shown.add(stranger.chain);                                            // nobody recorded it
        shown.add(new X509Certificate[]{stranger.chain[0]});
        // a root with the SUBJECT of a recorded root and another key: the name finds the entry, the signature does not verify
        TlsTestSupport.Id impostor = TlsTestSupport.selfRooted("n0", "t1", "rgA", "n0");
        shown.add(impostor.chain);
        shown.add(new X509Certificate[]{impostor.chain[0]});
        shown.add(new X509Certificate[]{impostor.chain[0], recorded.get(0).chain[1]});   // its leaf under the real root's certificate
        shown.add(new X509Certificate[]{recorded.get(1).chain[0], recorded.get(2).chain[1]});   // a leaf with another node's root

        int accepted = 0;
        for (X509Certificate[] chain : shown) {
            for (boolean client : new boolean[]{true, false}) {
                boolean want = accepts(whole, chain, client), got = accepts(live, chain, client);
                assertEquals(want, got, "chain of " + chain[0].getSubjectX500Principal() + " (" + chain.length + " certificate(s), "
                        + (client ? "client" : "server") + "): PKIX over the whole truststore says " + want);
                if (got) accepted++;
            }
        }
        assertTrue(accepted >= 6 * 2 * 2, "the recorded chains were not accepted: " + accepted);
        System.out.println("JoinTrustIsLiveTest: " + shown.size() * 2 + " decisions equal to PKIX over the whole truststore, " + accepted + " accepted");
    }

    /** What a decision reads does not grow with the truststore: only the entries named by the chain it is shown. */
    @Test
    void aDecisionReadsOnlyTheEntriesThatCanAnchorTheChain() throws Exception {
        TrustAnchorIndex index = new TrustAnchorIndex();
        TlsTestSupport.Id one = TlsTestSupport.selfRooted("one", "t1", "rgA", "one"), two = TlsTestSupport.selfRooted("two", "t1", "rgA", "two");
        for (int i = 0; i < 5000; i++) index.put("rgA_filler" + i + "#" + i, two.chain[1]);        // 5,000 entries of other names
        index.put("rgA_one#leaf", one.chain[0]);
        index.put("rgA_one#root", one.chain[1]);
        assertEquals(5002, index.size());
        assertEquals(2, index.anchorsFor(one.chain).size(), "the entries that can anchor a chain are its own two");
        assertEquals(List.of("rga_one#leaf", "rga_one#root"), index.aliasesOfPrefix("rga_one#").stream().sorted().toList());
        assertEquals(1, index.aliasesOfKey(PeerTrustRegistry.fingerprint(one.chain[1].getPublicKey().getEncoded())).size());
        index.remove("RGA_one#ROOT");                                                               // aliases do not tell case
        assertEquals(List.of(one.chain[0]), index.anchorsFor(one.chain), "a removed entry still anchors");
        assertEquals(5001, index.size());
    }

    // ------------------------------------------------------------------ real handshakes

    private static final java.security.SecureRandom RND = new java.security.SecureRandom();

    /** Counts the decisions a server's TLS context asks of the node's trust managers. */
    private static final class Counting extends javax.net.ssl.X509ExtendedTrustManager {
        final javax.net.ssl.X509ExtendedTrustManager d;
        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        Counting(javax.net.ssl.X509ExtendedTrustManager d) { this.d = d; }
        public void checkClientTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a); }
        public void checkServerTrusted(X509Certificate[] c, String a) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a); }
        public void checkClientTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, s); }
        public void checkServerTrusted(X509Certificate[] c, String a, java.net.Socket s) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, s); }
        public void checkClientTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkClientTrusted(c, a, e); }
        public void checkServerTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) throws CertificateException { calls.incrementAndGet(); d.checkServerTrusted(c, a, e); }
        public X509Certificate[] getAcceptedIssuers() { return d.getAcceptedIssuers(); }
    }

    private static javax.net.ssl.SSLContext clientCtx(TlsTestSupport.Id id, X509Certificate[] serverChain) throws Exception {
        javax.net.ssl.SSLContext c = javax.net.ssl.SSLContext.getInstance("TLS");
        c.init(id.keyManagers(), TlsTestSupport.trustManagers(serverChain), RND);
        return c;
    }

    /**
     * One connection with a client certificate required. The chain the SERVER holds for the client when the handshake
     * completed and the client's byte arrived; null when the server refused it.
     */
    private static X509Certificate[] connect(javax.net.ssl.SSLServerSocket ss, javax.net.ssl.SSLContext client) throws Exception {
        java.util.concurrent.CompletableFuture<X509Certificate[]> server = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try (javax.net.ssl.SSLSocket s = (javax.net.ssl.SSLSocket) ss.accept()) {
                s.setSoTimeout(5000);
                s.startHandshake();
                if (s.getInputStream().read() != 1) return null;
                s.getOutputStream().write(7);
                s.getOutputStream().flush();
                java.security.cert.Certificate[] peer = s.getSession().getPeerCertificates();
                X509Certificate[] x = new X509Certificate[peer.length];
                for (int i = 0; i < peer.length; i++) x[i] = (X509Certificate) peer[i];
                return x;
            } catch (Exception e) {
                return null;
            }
        });
        try (javax.net.ssl.SSLSocket c = (javax.net.ssl.SSLSocket) client.getSocketFactory().createSocket(java.net.InetAddress.getLoopbackAddress(), ss.getLocalPort())) {
            c.setSoTimeout(5000);
            c.startHandshake();
            c.getOutputStream().write(1);
            c.getOutputStream().flush();
            c.getInputStream().read();
        } catch (Exception ignore) {
            // judged by the server's side
        }
        return server.get(10, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Real handshakes against ONE TLS context, made once as a broker's is at its start: a chain recorded a moment ago
     * completes a handshake, a replaced chain does not, a revoked key does not -- and no context is rebuilt in between.
     * Fails before: the context had to be re-initialised for the first of these.
     */
    @Test
    void realHandshakesFollowTheTruststoreAndTheRegistryWithoutAContextRebuild() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        Counting tm = null;
        for (TrustManager t : cm.getTrustManagers()) if (t instanceof javax.net.ssl.X509ExtendedTrustManager) tm = new Counting((javax.net.ssl.X509ExtendedTrustManager) t);
        assertNotNull(tm);
        javax.net.ssl.SSLContext server = javax.net.ssl.SSLContext.getInstance("TLS");
        server.init(cm.getKeyManagers(), new TrustManager[]{tm}, RND);
        X509Certificate[] sc = cm.getPublicCertificate();
        try (javax.net.ssl.SSLServerSocket ss = (javax.net.ssl.SSLServerSocket) server.getServerSocketFactory().createServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            ss.setNeedClientAuth(true);
            TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
            assertNull(connect(ss, clientCtx(a1, sc)), "a chain nobody recorded completed a handshake");
            assertTrue(cm.replaceTrustStoreEntries("rgA_a1", a1.chain));
            assertNotNull(connect(ss, clientCtx(a1, sc)), "a chain recorded a moment ago is refused at a real handshake");

            // the same name comes back with another chain: the former one is refused at once, the new one accepted
            TlsTestSupport.Id a1b = TlsTestSupport.selfRooted("a1b", "t1", "rgA", "a1");
            assertTrue(cm.replaceTrustStoreEntries("rgA_a1", a1b.chain));
            assertNull(connect(ss, clientCtx(a1, sc)), "a replaced chain still completes a handshake");
            assertNotNull(connect(ss, clientCtx(a1b, sc)));

            // its key is revoked: a new connection is refused by the live trust at its handshake, at once
            assertNull(cm.getPeerTrust().revoke(PeerTrustRegistry.fingerprint(a1b.chain[0].getPublicKey().getEncoded()), "test"));
            assertNull(connect(ss, clientCtx(a1b, sc)), "a revoked key completes a handshake");
        }
    }

    /**
     * A resumed TLS session skips the trust manager (older than this change). What a revoked node cannot get through a
     * resumed session is an IDENTITY: the broker binds a connection to one only by the registry's word for the chain of
     * its session (CrescoAuthorizationBroker.resolve: verifiedIdentity), which refuses a revoked key whether or not the
     * session was resumed -- asserted here. What it CAN still do, measured here, is complete the TLS handshake: at a
     * revocation the broker invalidates the sessions its TLS context caches (ActiveBroker.invalidateRefusedSessions), but
     * the JDK's server keeps none for a client that resumes by a session ticket (its default), so nothing is there to
     * invalidate and the ticket still resumes. Such a connection is to the broker one without a verified identity
     * (quarantined: it receives nothing and what it sends is discarded).
     */
    @Test
    void aRevokedNodeHoldsNoIdentityThroughAResumedSession() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        PeerTrustRegistry reg = cm.getPeerTrust();
        Counting tm = null;
        for (TrustManager t : cm.getTrustManagers()) if (t instanceof javax.net.ssl.X509ExtendedTrustManager) tm = new Counting((javax.net.ssl.X509ExtendedTrustManager) t);
        javax.net.ssl.SSLContext server = javax.net.ssl.SSLContext.getInstance("TLS");
        server.init(cm.getKeyManagers(), new TrustManager[]{tm}, RND);
        X509Certificate[] sc = cm.getPublicCertificate();
        try (javax.net.ssl.SSLServerSocket ss = (javax.net.ssl.SSLServerSocket) server.getServerSocketFactory().createServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            ss.setNeedClientAuth(true);
            TlsTestSupport.Id a3 = TlsTestSupport.selfRooted("a3", "t1", "rgA", "a3");
            assertTrue(reg.bind("t1", "rgA", "a3", a3.kp.getPublic(), "discovery").ok);
            assertTrue(cm.replaceTrustStoreEntries("rgA_a3", a3.chain));
            javax.net.ssl.SSLContext held = clientCtx(a3, sc);                 // one client context: its later connections resume
            X509Certificate[] first = connect(ss, held);
            assertNotNull(first);
            assertNotNull(reg.verifiedIdentity(first, new String[1]), "precondition: the broker would bind this connection to rgA/a3");
            int decisions = tm.calls.get();
            X509Certificate[] again = connect(ss, held);
            boolean resumes = again != null && tm.calls.get() == decisions;
            System.out.println("JoinTrustIsLiveTest: a second connection of the same client context "
                    + (resumes ? "resumed its session (no trust decision)" : "made a full handshake (" + (tm.calls.get() - decisions) + " trust decision(s))"));

            assertNull(reg.revoke(PeerTrustRegistry.fingerprint(a3.chain[0].getPublicKey().getEncoded()), "test"));
            // (1) whatever TLS does with the session it still holds, the registry gives its chain no identity
            decisions = tm.calls.get();
            X509Certificate[] afterRevocation = connect(ss, held);
            String[] why = new String[1];
            if (afterRevocation != null) {
                assertNull(reg.verifiedIdentity(afterRevocation, why), "a revoked node's resumed connection would be bound to its identity");
                assertEquals(decisions, tm.calls.get(), "the connection was accepted by a full handshake, not a resumed one");
            }
            System.out.println("JoinTrustIsLiveTest: after the revocation, the same client context again: TLS "
                    + (afterRevocation == null ? "refused it" : "accepted it (resumed, no trust decision); the registry gives it no identity: " + why[0]));
            // (2) what the broker does at a revocation: the sessions of peers refused now are invalidated
            int invalidated = ActiveBroker.invalidateRefusedSessions(server.getServerSessionContext(), reg);
            X509Certificate[] afterInvalidation = connect(ss, held);
            System.out.println("JoinTrustIsLiveTest: " + invalidated + " cached session(s) of the revoked peer invalidated; the same client context again: TLS "
                    + (afterInvalidation == null ? "refused it (a full handshake, refused by the trust manager)" : "accepted it"));
            if (afterInvalidation != null) assertNull(reg.verifiedIdentity(afterInvalidation, why), "bound to its identity after its sessions were invalidated");
            // a client context of its own (no session to resume) is refused at its handshake
            assertNull(connect(ss, clientCtx(a3, sc)), "a revoked key completed a full handshake");
        }
    }

    // ------------------------------------------------------------------ joins beside handshakes

    /** A joiner's chain [leaf, root] with names of its own; one key for all of them (making keys is not what is measured). */
    private static X509Certificate[] chainOf(java.security.KeyPair kp, org.bouncycastle.operator.ContentSigner signer, String name) throws Exception {
        long now = System.currentTimeMillis();
        org.bouncycastle.asn1.x500.X500Name rn = new org.bouncycastle.asn1.x500.X500Name("CN=rootCA-" + name),
                ln = new org.bouncycastle.asn1.x500.X500Name("CN=" + name + ",OU=rgA,O=default");
        org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder rb = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(rn,
                new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L), rn, kp.getPublic());
        rb.addExtension(org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new org.bouncycastle.asn1.x509.BasicConstraints(true));
        org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder lb = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(rn,
                new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L), ln, kp.getPublic());
        lb.addExtension(org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new org.bouncycastle.asn1.x509.BasicConstraints(false));
        org.bouncycastle.cert.jcajce.JcaX509CertificateConverter c = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter();
        return new X509Certificate[]{ c.getCertificate(lb.build(signer)), c.getCertificate(rb.build(signer)) };
    }

    /**
     * Joins are not starved by handshakes. Every outbound connection (a bridge's, an agent's) asks for the node's trust at
     * its handshake; that built trust managers from the whole truststore under the truststore's lock, the lock a join
     * needs. With 4,300 entries and 4 threads handshaking, the code before this branch recorded 304 joins in 1.5 s; this
     * one 173,949 (the reviewer's measurement, same test). Asserted here: at least 5,000, and at least a twentieth of what
     * the same loop records with no handshake beside it. (This replaces TrustStoreWhileJoiningTest, which guarded the
     * walk that is gone.)
     */
    @Test
    void joinsAreNotStarvedByHandshakes() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        java.security.KeyPairGenerator g = java.security.KeyPairGenerator.getInstance("RSA");
        g.initialize(2048, RND);
        java.security.KeyPair kp = g.generateKeyPair();
        org.bouncycastle.operator.ContentSigner signer = new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA384withRSA").build(kp.getPrivate());
        final int names = 150;
        X509Certificate[][][] variant = new X509Certificate[2][names][];
        for (int v = 0; v < 2; v++) for (int i = 0; i < names; i++) variant[v][i] = chainOf(kp, signer, "j" + i + "v" + v);
        X509Certificate[] known = chainOf(kp, signer, "known");
        cm.replaceTrustStoreEntries("rgA_known", known);
        for (int i = 0; i < 2000; i++) cm.replaceTrustStoreEntries("rgA_f" + i, chainOf(kp, signer, "f" + i));
        for (int i = 0; i < names; i++) cm.replaceTrustStoreEntries("rgA_j" + i, variant[0][i]);
        long entries = cm.trustIndexCounts()[2];
        assertTrue(entries >= 4300, "entries " + entries);
        long[] joins = new long[2];
        long decisions = 0;
        for (int phase = 0; phase < 2; phase++) {
            java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            java.util.concurrent.atomic.AtomicLong done = new java.util.concurrent.atomic.AtomicLong(), failed = new java.util.concurrent.atomic.AtomicLong();
            Thread[] hs = new Thread[phase == 0 ? 0 : 4];
            for (int h = 0; h < hs.length; h++) {
                hs[h] = new Thread(() -> {
                    while (!stop.get()) {
                        // what a connection does at its handshake: the node's trust of the moment, and one decision
                        try { cm.currentPkixTrustManager().checkClientTrusted(known, "RSA"); done.incrementAndGet(); }
                        catch (Exception ex) { failed.incrementAndGet(); }
                    }
                });
                hs[h].start();
            }
            long end = System.nanoTime() + 1_500_000_000L, n = 0;
            while (System.nanoTime() < end) {
                int i = (int) (n % names);
                cm.replaceTrustStoreEntries("rgA_j" + i, variant[(int) ((n / names + 1) % 2)][i]);     // a join: this name's chain replaced
                n++;
            }
            stop.set(true);
            for (Thread h : hs) h.join();
            joins[phase] = n;
            decisions = done.get();
            assertEquals(0, failed.get(), "a handshake was left without a trust decision while joins recorded their chains");
        }
        System.out.println("JoinTrustIsLiveTest: " + entries + " entries: joins in 1.5 s alone " + joins[0] + ", beside 4 handshake threads "
                + joins[1] + " (" + decisions + " trust decisions meanwhile)");
        assertTrue(decisions > 0);
        assertTrue(joins[1] >= 5_000, "joins beside handshakes: " + joins[1] + " in 1.5 s (the code before this branch: 304)");
        assertTrue(joins[1] * 20 >= joins[0], "joins beside handshakes " + joins[1] + ", alone " + joins[0]);
    }

    // ------------------------------------------------------------------ one subject, many certificates

    /**
     * Entries of ONE subject are all tried for a chain that names that subject, and a joiner writes its own subjects: a
     * holder of the discovery secret could plant two per join. A thousand of them cost 362 ms per decision for a stranger's
     * leaf naming that issuer (the reviewer's measurement; the same with the whole store). At most
     * {@link TrustAnchorIndex#MAX_PER_SUBJECT} different certificates of one subject are recorded; a node whose
     * certificate is not recorded for that is still trusted by its bound key. Fails before: all thousand were recorded.
     */
    @Test
    void oneSubjectHoldsOnlyAFewCertificatesAndADecisionForItStaysCheap() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        CertificateManager cm = e.getCertificateManager();
        java.security.KeyPairGenerator ec = java.security.KeyPairGenerator.getInstance("EC");
        ec.initialize(256, RND);
        org.bouncycastle.asn1.x500.X500Name same = new org.bouncycastle.asn1.x500.X500Name("CN=rootCA-same");
        long now = System.currentTimeMillis();
        org.bouncycastle.cert.jcajce.JcaX509CertificateConverter conv = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter();
        long before = cm.trustIndexCounts()[2];
        final int k = 1000;
        java.security.KeyPair lastRootKey = null;
        X509Certificate lastRoot = null;
        for (int i = 0; i < k; i++) {
            lastRootKey = ec.generateKeyPair();
            org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder rb = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(same,
                    new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L), same, lastRootKey.getPublic());
            rb.addExtension(org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new org.bouncycastle.asn1.x509.BasicConstraints(true));
            lastRoot = conv.getCertificate(rb.build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA256withECDSA").build(lastRootKey.getPrivate())));
            cm.replaceTrustStoreEntries("rgA_s" + i, new java.security.cert.Certificate[]{lastRoot});       // what a join records
        }
        assertEquals(TrustAnchorIndex.MAX_PER_SUBJECT, cm.trustIndexCounts()[2] - before, "certificates of ONE subject recorded");
        // a stranger's leaf that names that subject as its issuer: refused, and quickly
        java.security.KeyPair stranger = ec.generateKeyPair();
        org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder lb = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(same,
                new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L),
                new org.bouncycastle.asn1.x500.X500Name("CN=x"), stranger.getPublic());
        X509Certificate forged = conv.getCertificate(lb.build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA256withECDSA").build(stranger.getPrivate())));
        X509TrustManager held = held(cm);
        for (int i = 0; i < 5; i++) assertFalse(accepts(held, new X509Certificate[]{forged}, true));     // (warm)
        long t0 = System.nanoTime();
        for (int i = 0; i < 20; i++) assertFalse(accepts(held, new X509Certificate[]{forged}, true), "a stranger's leaf was trusted");
        double ms = (System.nanoTime() - t0) / 1e6 / 20;
        System.out.printf("JoinTrustIsLiveTest: %d joiners recorded a root of ONE subject, %d are entries; a decision for a stranger's leaf naming"
                + " that issuer takes %.2f ms%n", k, TrustAnchorIndex.MAX_PER_SUBJECT, ms);
        assertTrue(ms < 40, "a decision for a subject with many certificates takes " + ms + " ms (with all 1,000 recorded: 362 ms)");
        // the last joiner's root is not an entry: the node it belongs to is trusted by its bound key, as after a restart
        org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder own = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(same,
                new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L),
                new org.bouncycastle.asn1.x500.X500Name(io.cresco.library.security.CrescoIdentity.of("t1", "rgA", "s999", null).toX500Name()), stranger.getPublic());
        X509Certificate leaf = conv.getCertificate(own.build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA256withECDSA").build(lastRootKey.getPrivate())));
        X509Certificate[] chain = {leaf, lastRoot};
        assertFalse(accepts(held, chain, true), "precondition: its root is not an entry and its key is not bound");
        assertTrue(cm.getPeerTrust().bind("t1", "rgA", "s999", stranger.getPublic(), "discovery").ok);
        assertTrue(accepts(held, chain, true), "a bound node whose root was not recorded is not trusted by its bound key");
    }

    /** The refusal's text ends up in logs and the peer is not authenticated: nothing the peer wrote is in it. */
    @Test
    void theRefusalOfAnUnknownChainCarriesNothingThePeerWrote() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", null);
        java.security.KeyPairGenerator ec = java.security.KeyPairGenerator.getInstance("EC");
        ec.initialize(256, RND);
        java.security.KeyPair kp = ec.generateKeyPair();
        long now = System.currentTimeMillis();
        org.bouncycastle.asn1.x500.X500Name evil = new org.bouncycastle.asn1.x500.X500Name(new org.bouncycastle.asn1.x500.RDN[]{
                new org.bouncycastle.asn1.x500.RDN(org.bouncycastle.asn1.x500.style.BCStyle.CN,
                        new org.bouncycastle.asn1.DERUTF8String("x\n11 Nov 2026 00:00:00,000 ERROR forged line\u001b[31m"))});
        org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder b = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(evil,
                new java.math.BigInteger(64, RND), new java.util.Date(now - 60_000L), new java.util.Date(now + 86_400_000L), evil, kp.getPublic());
        X509Certificate c = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(
                b.build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA256withECDSA").build(kp.getPrivate())));
        CertificateException refusal = assertThrows(CertificateException.class,
                () -> e.getCertificateManager().livePkixForTests().checkClientTrusted(new X509Certificate[]{c}, "EC"));
        String text = refusal.getMessage();
        assertFalse(text.contains("forged") || text.contains("\n") || text.contains("\u001b"), "the peer's text is in the refusal: " + text);
        assertTrue(text.matches("[\\x20-\\x7e]{1,200}"), "not plain, short text: " + text);
        assertTrue(text.contains(PeerIdentity.shortFp(PeerIdentity.spkiFingerprint(c))), "the refusal does not say which certificate: " + text);
    }

    // ------------------------------------------------------------------ stores that are replaced or kept in files

    /**
     * File-backed stores (keystorefile, truststorefile): the removal of a replaced chain's entries is written before the
     * call returns. Fails before: a peer's change was saved up to a second later, and a crash in that second brought the
     * replaced chain's anchors back from the file.
     */
    @Test
    void theRemovalOfAReplacedChainIsWrittenToTheTruststoreFileAtOnce() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("tsfile");
        java.util.Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put("keystorefile", dir.resolve("key.p12").toString());
        cfg.put("truststorefile", dir.resolve("trust.p12").toString());
        cfg.put("keystorepwd", "test-only-key");
        cfg.put("truststorepwd", "test-only-trust");
        ControllerEngine e = TlsTestSupport.engine("rgA", "gc", cfg);
        CertificateManager cm = e.getCertificateManager();
        TlsTestSupport.Id first = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1"), second = TlsTestSupport.selfRooted("a1-again", "t1", "rgA", "a1");
        assertTrue(cm.replaceTrustStoreEntries("rgA_a1", first.chain));
        long end = System.currentTimeMillis() + 5000;
        while (!inFile(dir.resolve("trust.p12"), first.chain[1]) && System.currentTimeMillis() < end) Thread.sleep(50);
        assertTrue(inFile(dir.resolve("trust.p12"), first.chain[1]), "a recorded peer's chain was not saved within its second");
        assertTrue(cm.replaceTrustStoreEntries("rgA_a1", second.chain));                   // re-installed: its chain replaced
        assertFalse(inFile(dir.resolve("trust.p12"), first.chain[1]) || inFile(dir.resolve("trust.p12"), first.chain[0]),
                "the replaced chain's anchors are still in the truststore file when the replacement has returned");
        assertTrue(inFile(dir.resolve("trust.p12"), second.chain[1]));
    }

    private static boolean inFile(java.nio.file.Path file, X509Certificate cert) throws Exception {
        if (!java.nio.file.Files.exists(file)) return false;
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        try (java.io.InputStream in = java.nio.file.Files.newInputStream(file)) {
            ks.load(in, "test-only-trust".toCharArray());
        } catch (java.io.IOException beingWritten) {
            return false;
        }
        return ks.getCertificateAlias(cert) != null;
    }

    /**
     * A pre-provisioned node whose operator CHANGES its trust anchors while it runs (not only their first load): the same
     * trust manager follows -- an added anchor's chains are trusted from the reload on, a removed one's refused.
     */
    @Test
    void aPreProvisionedNodesTrustFollowsItsAnchorsWhenTheyAreReloaded() throws Exception {
        final long day = 24L * 3600 * 1000, from = System.currentTimeMillis() - 60_000;
        java.nio.file.Path pki = java.nio.file.Files.createTempDirectory("pkireload"), dir = java.nio.file.Files.createTempDirectory("pkireload-a1");
        PkiTestSupport.Ca root = PkiTestSupport.root("Fabric Root"), other = PkiTestSupport.root("A Second Root");
        PkiTestSupport.Ca rgA = PkiTestSupport.region(root, "t1", "rgA"), rgAofOther = PkiTestSupport.region(other, "t1", "rgA");
        PkiTestSupport.Node a1 = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, from, 30 * day), pki.resolve("rgA_a1"));
        PkiTestSupport.Node rc = PkiTestSupport.node(rgA, "t1", "rgA", "rc", true, from, 30 * day);
        PkiTestSupport.Node x = PkiTestSupport.node(rgAofOther, "t1", "rgA", "a9", false, from, 30 * day);
        java.nio.file.Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        java.util.Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("security_profile", "production");
        cfg.put("identity_keystore", a1.keystore.toString());
        cfg.put("identity_keystore_password_file", a1.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        io.cresco.library.plugin.PluginBuilder pb = io.cresco.agent.test.TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "a1");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, "rgA", "a1");
        CertificateManager cm = new CertificateManager(e);
        e.setCertificateManager(cm);
        assertTrue(cm.isProvisioned());
        X509TrustManager live = cm.livePkixForTests();                         // one trust manager, held throughout
        assertTrue(accepts(live, rc.chain, false), "its controller, of the operator's PKI, is not trusted");
        assertFalse(accepts(live, x.chain, true), "a chain of a root the operator did not list is trusted");

        PkiTestSupport.pem(anchors, root.cert, other.cert);                    // the operator adds an anchor
        java.nio.file.Files.setLastModifiedTime(anchors, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
        assertTrue(cm.reloadProvisionedIfChanged(), "the changed anchors were not loaded");
        assertTrue(accepts(live, x.chain, true), "a chain of the anchor the operator added is refused after the reload");
        assertTrue(accepts(live, rc.chain, false));

        PkiTestSupport.pem(anchors, root.cert);                                // ... and removes it again
        java.nio.file.Files.setLastModifiedTime(anchors, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        assertTrue(cm.reloadProvisionedIfChanged());
        assertFalse(accepts(live, x.chain, true), "a chain of an anchor the operator removed is still trusted after the reload");
        assertTrue(accepts(live, rc.chain, false));
    }

    /** The node's PKIX trust without the registry's part (BoundKeyTrustManager wraps it for TLS). */
    private static final class LivePkixTrustManagerProbe {
        private final CertificateManager cm;
        LivePkixTrustManagerProbe(CertificateManager cm) { this.cm = cm; }
        X509TrustManager pkix() { return cm.livePkixForTests(); }
    }
}
