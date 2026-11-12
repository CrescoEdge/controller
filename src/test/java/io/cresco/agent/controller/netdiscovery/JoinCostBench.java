package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.core.ControllerEngine;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * What one join costs a controller against the joins already made, in one JVM (not a test: run it on purpose,
 * {@code mvn test -Dtest=JoinCostBench -Dcresco.bench=true}; the DGX gate's scale phase measures a live fabric). One line
 * per number of joiners already present, each with certificates of its own: (1) recording a joiner's chain as a
 * controller does at a per-node-trust join and making the broker's trust include it -- with
 * {@code -Dcresco.bench.rebuild=true} by rebuilding the trust managers and re-initialising a TLS context, as a join did
 * before the trust was read live; (2) one trust decision for the joiner's chain (a TLS handshake's); (3) binding the
 * joiner's name to its key in the peer trust registry (a durable write: two disk syncs).
 */
@EnabledIfSystemProperty(named = "cresco.bench", matches = "true")
class JoinCostBench {

    private static final SecureRandom RND = new SecureRandom();

    /** A joiner's chain [leaf, root] with names of its own (one key for all of them: making keys is not what is measured). */
    private static X509Certificate[] chain(KeyPair kp, ContentSigner signer, String name) throws Exception {
        long now = System.currentTimeMillis();
        X500Name rn = new X500Name("CN=rootCA-" + name), ln = new X500Name("CN=" + name + ",OU=rgA,O=default");
        JcaX509v3CertificateBuilder rb = new JcaX509v3CertificateBuilder(rn, new BigInteger(64, RND), new Date(now - 60_000L),
                new Date(now + 86_400_000L), rn, kp.getPublic());
        rb.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        JcaX509v3CertificateBuilder lb = new JcaX509v3CertificateBuilder(rn, new BigInteger(64, RND), new Date(now - 60_000L),
                new Date(now + 86_400_000L), ln, kp.getPublic());
        lb.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        JcaX509CertificateConverter c = new JcaX509CertificateConverter();
        return new X509Certificate[]{ c.getCertificate(lb.build(signer)), c.getCertificate(rb.build(signer)) };
    }

    @Test
    void costOfOneJoinAgainstJoinsAlreadyMade() throws Exception {
        boolean rebuild = Boolean.getBoolean("cresco.bench.rebuild");
        ControllerEngine e = DiscoveryDosTest.engine("rgA", "gc", true, null);
        CertificateManager cm = new CertificateManager(e);
        e.setCertificateManager(cm);
        KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
        g.initialize(2048, RND);
        KeyPair kp = g.generateKeyPair();
        ContentSigner signer = new JcaContentSignerBuilder("SHA384withRSA").build(kp.getPrivate());
        SSLContext ctx = SSLContext.getInstance("TLS");
        TrustManager[] held = cm.getTrustManagers();
        ctx.init(cm.getKeyManagers(), held, RND);
        PeerTrustRegistry reg = cm.getPeerTrust();
        int joined = 0;
        final int n = 40;
        for (int target : new int[]{0, 250, 500, 1000, 2000, 4000, 8000}) {
            for (; joined < target; joined++) {
                cm.replaceTrustStoreEntries("rgA_w" + joined, chain(kp, signer, "w" + joined));
                reg.bind("default", "rgA", "w" + joined, kp.getPublic(), "bench");
            }
            X509Certificate[][] fresh = new X509Certificate[n][];
            for (int i = 0; i < n; i++) fresh[i] = chain(kp, signer, "m" + target + "x" + i);
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                cm.replaceTrustStoreEntries("rgA_m" + target + "x" + i, fresh[i]);
                if (rebuild) {                                     // ActiveBroker.updateTrustManager, as at every join before
                    held = cm.getTrustManagers();
                    ctx.init(cm.getKeyManagers(), held, RND);
                }
            }
            long record = System.nanoTime() - t0;
            X509TrustManager tm = (X509TrustManager) held[0];
            t0 = System.nanoTime();
            for (int i = 0; i < n; i++) tm.checkClientTrusted(fresh[i], "RSA");
            long decide = System.nanoTime() - t0;
            t0 = System.nanoTime();
            for (int i = 0; i < n; i++) reg.bind("default", "rgA", "m" + target + "x" + i, kp.getPublic(), "bench");
            long bind = System.nanoTime() - t0;
            System.out.printf("BENCH join with %5d joined: chain recorded and trusted by the broker %8.3f ms; one trust decision %7.3f ms;"
                    + " name bound durably %7.3f ms%n", target, record / 1e6 / n, decide / 1e6 / n, bind / 1e6 / n);
        }
    }
}
