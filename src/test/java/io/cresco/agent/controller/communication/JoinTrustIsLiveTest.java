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

    /** The node's PKIX trust without the registry's part (BoundKeyTrustManager wraps it for TLS). */
    private static final class LivePkixTrustManagerProbe {
        private final CertificateManager cm;
        LivePkixTrustManagerProbe(CertificateManager cm) { this.cm = cm; }
        X509TrustManager pkix() { return cm.livePkixForTests(); }
    }
}
