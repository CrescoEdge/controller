package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.*;

/** The certificate chain a node makes for itself at its first start, and the keys it makes for it. */
class NodeChainKeysTest {

    private static boolean accepts(X509TrustManager tm, X509Certificate[] chain) {
        try {
            tm.checkClientTrusted(chain, "RSA");
            return true;
        } catch (CertificateException refused) {
            return false;
        }
    }

    /**
     * The chain a node makes for itself: one key pair for its two CA certificates, thrown away as before, and a key of its
     * own for the leaf -- a valid path to its root. Fails before: three key pairs were made (two thirds of the work of a
     * node's first start, for two keys that nothing ever used again).
     */
    @Test
    void aNodeMakesOneThrowawayCaKeyAndItsOwnLeafKey() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "a7", null);
        X509Certificate[] chain = e.getCertificateManager().getPublicCertificate();
        assertEquals(3, chain.length, "the chain's shape is as it was: leaf, intermediate, root");
        assertEquals(chain[1].getPublicKey(), chain[2].getPublicKey(), "two CA key pairs were made");
        assertNotEquals(chain[0].getPublicKey(), chain[1].getPublicKey(), "the leaf shares the CA key: the node could issue under its own root");
        chain[2].verify(chain[2].getPublicKey());
        chain[1].verify(chain[2].getPublicKey());
        chain[0].verify(chain[1].getPublicKey());
        // PKIX, with only its root trusted and with only its intermediate trusted (as a peer that recorded either)
        ((X509TrustManager) TlsTestSupport.trustManagers(chain[2])[0]).checkClientTrusted(chain, "RSA");
        ((X509TrustManager) TlsTestSupport.trustManagers(chain[1])[0]).checkServerTrusted(new X509Certificate[]{chain[0], chain[1]}, "ECDHE_RSA");
        // and by the node's own trust once a peer recorded the chain
        ControllerEngine peer = TlsTestSupport.engine("rgA", "gc", null);
        assertTrue(peer.getCertificateManager().replaceTrustStoreEntries("rgA_a7", chain));
        assertTrue(accepts(peer.getCertificateManager().livePkixForTests(), chain));
    }

}
