package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** A peer's certificate can never replace a trust anchor by copying its subject (third review, finding 12). */
class TrustStoreAliasTest {

    @Test
    void aCertificateWithACopiedSubjectDoesNotEvictTheRegionCa() throws Exception {
        ControllerEngine rc = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        CertificateManager cm = rc.getCertificateManager();
        cm.ensureRegionCA();
        X509Certificate ca = cm.getPublicCertificate()[1];
        // a leaf the region CA issued (not bound: only PKIX can trust it)
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("regionCA");
        f.setAccessible(true);
        TlsTestSupport.Id leaf = TlsTestSupport.issued((RegionCA) f.get(cm), "t1", "rgA", "unbound");
        cm.currentPkixTrustManager().checkClientTrusted(leaf.chain, "RSA");          // precondition

        KeyPair kp = TlsTestSupport.rsa();
        X500Name same = X500Name.getInstance(ca.getSubjectX500Principal().getEncoded());
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(same, BigInteger.TEN, new Date(now - 60_000), new Date(now + 86_400_000),
                same, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        X509Certificate forged = new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(kp.getPrivate())));
        assertEquals(1, cm.addCertificatesToTrustStore("rgB_peer", new X509Certificate[]{forged}).size());
        assertEquals(0, cm.addCertificatesToTrustStore("rgB_peer", new X509Certificate[]{forged}).size(), "the same certificate twice");
        assertDoesNotThrow(() -> cm.currentPkixTrustManager().checkClientTrusted(leaf.chain, "RSA"),
                "a peer certificate that copied the region CA's subject evicted the CA from the truststore");
    }
}
