package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pure decisions behind peer identity binding. */
class PeerIdentityPolicyTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id rc, a9;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        rc = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a9 = TlsTestSupport.issued(ca, "t1", "rgA", "a9");
    }

    private static final PeerIdentity.Expected RC = new PeerIdentity.Expected("rgA", "rc");

    @Test
    void theNamedPeerIsAccepted() {
        PeerIdentity.Verdict v = PeerIdentity.verify(RC, rc.chain, null);
        assertTrue(v.ok, v.reason);
        assertEquals("rc", v.presented.getAgent());
    }

    @Test
    void anotherAgentOrRegionIsRefused() throws Exception {
        assertFalse(PeerIdentity.verify(RC, a9.chain, null).ok);
        TlsTestSupport.Id otherRegion = TlsTestSupport.issued(ca, "t1", "rgB", "rc");   // same agent name, other region
        PeerIdentity.Verdict v = PeerIdentity.verify(RC, otherRegion.chain, null);
        assertFalse(v.ok);
        assertTrue(v.reason.contains("region=rgB"), v.reason);
    }

    @Test
    void aCertificateWithoutACrescoIdentityIsRefused() throws Exception {
        // the old pre-identity leaf shape: CN=endUserCert-<uuid>, no OU/O
        TlsTestSupport.Id legacy = TlsTestSupport.selfRooted("x", null, null, "endUserCert-1234");
        PeerIdentity.Verdict v = PeerIdentity.verify(RC, legacy.chain, null);
        assertFalse(v.ok);
        assertTrue(v.reason.contains("names no valid Cresco region/agent"), v.reason);
    }

    @Test
    void noExpectationOrNoCertificateIsRefused() {
        assertFalse(PeerIdentity.verify(null, rc.chain, null).ok);
        assertFalse(PeerIdentity.verify(new PeerIdentity.Expected("rgA", null), rc.chain, null).ok);
        assertFalse(PeerIdentity.verify(RC, new X509Certificate[0], null).ok);
        assertFalse(PeerIdentity.verify(RC, null, null).ok);
    }

    @Test
    void aPinnedKeyMustMatch() throws Exception {
        PeerTrustRegistry pins = new PeerTrustRegistry();
        assertNull(pins.pin("rgA", "rc", rc.chain[0]));
        assertTrue(PeerIdentity.verify(RC, rc.chain, pins).ok);
        // same name, minted under another root whose key the node trusts (legacy per-node trust)
        TlsTestSupport.Id minted = TlsTestSupport.selfRooted("evil", "t1", "rgA", "rc");
        PeerIdentity.Verdict v = PeerIdentity.verify(RC, minted.chain, pins);
        assertFalse(v.ok);
        assertTrue(v.reason.contains("different key"), v.reason);
    }

    @Test
    void aPinIsOnlyRecordedForTheIdentityTheLeafNames() {
        PeerTrustRegistry pins = new PeerTrustRegistry();
        assertNotNull(pins.pin("rgA", "rc", a9.chain[0]));   // discovery said rgA/rc, the leaf says rgA/a9
        assertNull(pins.pinnedKey(RC));
        assertEquals(0, pins.pinCount());
    }

    @Test
    void bridgePeersAreExactIdentities() {
        PeerTrustRegistry r = new PeerTrustRegistry();
        r.allowBridgePeer("rgB", "rc");
        assertTrue(r.isBridgePeer(CrescoIdentity.of("t9", "rgB", "rc", null)));
        assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", "rgB", "a1", null)));
        assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", "rgB_rc", null, null)));
        assertFalse(r.isBridgePeer(null));
    }

    @Test
    void modeParsingFailsClosed() {
        assertEquals(PeerIdentity.Mode.ENFORCE, PeerIdentity.Mode.parse(null));
        assertEquals(PeerIdentity.Mode.ENFORCE, PeerIdentity.Mode.parse("enforce"));
        assertEquals(PeerIdentity.Mode.ENFORCE, PeerIdentity.Mode.parse("typo"));
        assertEquals(PeerIdentity.Mode.WARN, PeerIdentity.Mode.parse(" WARN "));
        assertEquals(PeerIdentity.Mode.OFF, PeerIdentity.Mode.parse("off"));
        assertEquals(PeerIdentity.Mode.OFF, PeerIdentity.Mode.parse("false"));
    }
}
