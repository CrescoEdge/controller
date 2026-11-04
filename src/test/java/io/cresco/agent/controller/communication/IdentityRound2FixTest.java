package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The fixes for the identity re-review (round 2, 2026-11-04) beyond the review's own tests: the region CA that cannot be
 * loaded (P-a), lists served only to verified child controllers (P-c), short leaves (N6), registry rollback seen by a peer
 * (N4), and a revoked peer's resumed reconnects (N5).
 */
class IdentityRound2FixTest {

    private static final long DAY = 24L * 3600 * 1000;
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() {
        for (BrokerService b : brokers) try { b.stop(); } catch (Exception ignore) { }
    }

    private static String randomFp() {
        byte[] b = new byte[32];
        new SecureRandom().nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    private static ControllerEngine regionCaNodeAt(Path dir, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("security_regional_ca", "true");
        cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "rc");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, "rgA", "rc");
        st.regional = true;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    // ------------------------------------------------------------------ P-a: the region CA that existed

    /** An unreadable region-ca.p12 never silently becomes a new CA: no CA, CRITICAL, joiners refused, until it is restored. */
    @Test
    void anUnreadableRegionCaIsNeverReplacedSilently() throws Exception {
        Path dir = Files.createTempDirectory("r2-regionca");
        ControllerEngine first = regionCaNodeAt(dir, Map.of());
        first.getCertificateManager().ensureRegionCA();
        assertTrue(first.getCertificateManager().isRegionCA(), "precondition: the region CA exists");
        X509Certificate original = first.getCertificateManager().getPublicCertificate()[1];
        first.getCertificateManager().close();
        first.getCertificateManager().getPeerTrust().close();
        Path store = dir.resolve("security").resolve("region-ca.p12");
        byte[] good = Files.readAllBytes(store);
        Files.write(store, "not a keystore".getBytes());                     // a damaged disk, a bad restore
        ControllerEngine again = regionCaNodeAt(dir, Map.of());
        CertificateManager cm = again.getCertificateManager();
        cm.ensureRegionCA();
        assertFalse(cm.isRegionCA(), "a new region CA was generated in place of one that could not be read");
        assertNotNull(cm.regionCaFailure());
        String[] h = cm.identityHealth();
        assertEquals("CRITICAL", h[0], h[1]);
        assertTrue(h[1].contains("REGION CA UNAVAILABLE"), h[1]);
        ControllerEngine joiner = node("rgA", "a1", false);
        assertNull(cm.issueEnrollment(joiner.getCertificateManager().getJsonFromCerts(joiner.getCertificateManager().getPublicCertificate())),
                "an identity was issued without the region's CA");
        // restored from the backup: picked up again (the retry is at most once a minute; a new manager tries at once)
        Files.write(store, good);
        cm.close();
        cm.getPeerTrust().close();
        ControllerEngine restored = regionCaNodeAt(dir, Map.of());
        restored.getCertificateManager().ensureRegionCA();
        assertTrue(restored.getCertificateManager().isRegionCA());
        assertEquals(original.getPublicKey(), restored.getCertificateManager().getPublicCertificate()[1].getPublicKey(),
                "the restored region CA is not the original");
    }

    /** A missing region-ca.p12 while the registry records this region's own CA is the same failure; region_ca_allow_new decides. */
    @Test
    void aMissingRegionCaThatExistedNeedsAnExplicitNewCa() throws Exception {
        Path dir = Files.createTempDirectory("r2-regionca2");
        ControllerEngine first = regionCaNodeAt(dir, Map.of());
        first.getCertificateManager().ensureRegionCA();
        X509Certificate original = first.getCertificateManager().getPublicCertificate()[1];
        first.getCertificateManager().close();
        first.getCertificateManager().getPeerTrust().close();
        Files.delete(dir.resolve("security").resolve("region-ca.p12"));
        Files.delete(dir.resolve("security").resolve("region-ca.p12.pass"));
        ControllerEngine again = regionCaNodeAt(dir, Map.of());
        again.getCertificateManager().ensureRegionCA();
        assertFalse(again.getCertificateManager().isRegionCA(), "a deleted region CA was silently replaced");
        assertTrue(again.getCertificateManager().regionCaFailure().contains("missing"), again.getCertificateManager().regionCaFailure());
        again.getCertificateManager().close();
        again.getCertificateManager().getPeerTrust().close();
        ControllerEngine allowed = regionCaNodeAt(dir, Map.of("region_ca_allow_new", "true"));
        allowed.getCertificateManager().ensureRegionCA();
        assertTrue(allowed.getCertificateManager().isRegionCA(), "region_ca_allow_new did not make a new CA");
        assertNotEquals(original.getPublicKey(), allowed.getCertificateManager().getPublicCertificate()[1].getPublicKey());
    }

    // ------------------------------------------------------------------ P-c: who gets the global's list

    /** The global serves its list only to a verified child controller (a stamped caller bound here as a controller). */
    @Test
    void theGlobalServesItsListOnlyToVerifiedChildControllers() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), region = node("rgB", "rc", true);
        PeerTrustRegistry atGlobal = global.getCertificateManager().getPeerTrust();
        assertNull(atGlobal.pin("rgB", "rc", region.getCertificateManager().getPublicCertificate()[0], PeerTrustRegistry.ROLE_CONTROLLER, false));
        assertTrue(atGlobal.bind("t1", "rgG", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);     // an agent of the global's region
        assertNull(atGlobal.revoke(randomFp(), "operator"));
        String[][] callers = {{null, "no"}, {"agent:t1/rgG/a1", "no"}, {"controller:t1/rgX/rc", "no"}, {"agent:t1/rgB/rc", "no"},
                {"controller:t1/rgB/rc", "yes"}};
        for (String[] c : callers) {
            MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc", null, "rgG", "gc", null, true, true);
            p.setParam("action", "ping");
            if (c[0] != null) p.setParam("cresco_caller", c[0]);
            RevocationSync.onPingAtParent(global, p, true);
            assertEquals("yes".equals(c[1]), p.getParam(RevocationSync.PARENT_LIST) != null, "caller " + c[0]);
        }
    }

    // ------------------------------------------------------------------ N6: short leaves

    /** Leaves of an hour and of twelve hours are not due at issue; they are due once their remaining time is a fraction of it. */
    @Test
    void shortLeavesRenewAtAFractionOfTheirIntendedLife() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        long now = System.currentTimeMillis();
        for (long hours : new long[]{1, 12, 168}) {
            X509Certificate[] leaf = ca.enrollFor(TlsTestSupport.rsa().getPublic(), "t1", "rgA", "a1", hours * 3600_000L);
            assertFalse(CertificateManager.chainNeedsRenewal(leaf, now, 30 * DAY), hours + " h leaf due at issue");
            long life = hours * 3600_000L;
            assertFalse(CertificateManager.chainNeedsRenewal(leaf, leaf[0].getNotAfter().getTime() - life / 2 - 60_000L, 30 * DAY),
                    hours + " h leaf due before half its life");
            assertTrue(CertificateManager.chainNeedsRenewal(leaf, leaf[0].getNotAfter().getTime() - life / 2 + 60_000L, 30 * DAY),
                    hours + " h leaf not due after half its life");
        }
    }

    // ------------------------------------------------------------------ N4: a rollback seen by a verified peer

    /** A verified child that holds more of this epoch than this node does makes it start a new epoch (and get the whole list). */
    @Test
    void aVerifiedChildAheadOfThisRegistryStartsANewEpoch() throws Exception {
        ControllerEngine region = node("rgA", "rc", true), agent = node("rgA", "a1", false);
        PeerTrustRegistry reg = region.getCertificateManager().getPeerTrust();
        assertTrue(reg.bind("t1", "rgA", "a1", agent.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "discovery").ok);
        assertNull(reg.revoke(randomFp(), "operator"));
        String epoch = reg.revocationEpoch();
        // an unverified pinger cannot do it
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        p.setParam(RevocationSync.HAVE, epoch + ".99");
        RevocationSync.onPingAtParent(region, p, false);
        assertEquals(epoch, reg.revocationEpoch(), "an unverified ping changed the epoch");
        MsgEvent q = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        q.setParam(RevocationSync.HAVE, epoch + ".99");
        q.setParam("cresco_caller", "agent:t1/rgA/a1");
        RevocationSync.onPingAtParent(region, q, false);
        assertNotEquals(epoch, reg.revocationEpoch(), "a verified child ahead of this registry did not start a new epoch");
        assertEquals(reg.revocationEpoch(), q.getParam(RevocationSync.EPOCH));
        assertNotNull(q.getParam(RevocationSync.PARENT_LIST), "the child was not sent the whole list");
    }

    // ------------------------------------------------------------------ N5: a revoked peer's reconnects

    /** After its region CA is revoked, a bridge's reconnects (resumed TLS sessions included) are closed at their handshake. */
    @Test
    void aRevokedPeersReconnectsAreClosedAtTheirHandshake() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id rc = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        ControllerEngine global = TlsTestSupport.engine("rgG", "gc", null);
        CertificateManager cm = global.getCertificateManager();
        assertNull(cm.getPeerTrust().trustCa(caA.caCert(), "rgA", "t1", "federated"));
        cm.addCertificatesToTrustStore("peerCA-rgA", new X509Certificate[]{caA.caCert()});
        assertTrue(cm.getPeerTrust().bind("t1", "rgA", "rc", rc.kp.getPublic(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        BrokerService region = TlsTestSupport.broker("rgA_rc", rc, false, null, null, caA.caCert());
        brokers.add(region);
        BrokerService local = new BrokerService();
        local.setBrokerName("rgG_gc-" + System.nanoTime());
        local.setPersistent(false);
        local.setUseJmx(false);
        local.setUseShutdownHook(false);
        local.start();
        brokers.add(0, local);
        int port = TlsTestSupport.port(region);
        NetworkConnector nc = local.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + port
                + "?verifyHostName=false&socketBufferSize=0)?maxReconnectAttempts=-1&initialReconnectDelay=200&useExponentialBackOff=false"));
        nc.setName("bridge-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(ActiveBroker.bridgeSslContext(cm, new PeerIdentity.Expected("rgA", "rc"), "bridge -> 127.0.0.1:" + port));
        nc.start();
        assertTrue(bridgeUp(region, 10_000), "precondition: the bridge is up");
        assertNull(cm.getPeerTrust().revoke(PeerTrustRegistry.fingerprint(caA.caCert().getPublicKey().getEncoded()), "operator"));
        // for 6 s the bridge keeps reconnecting (resuming its session): no connection may stay up longer than a moment
        long end = System.currentTimeMillis() + 6000, upSince = -1, longest = 0;
        while (System.currentTimeMillis() < end) {
            boolean up = bridgeUp(region, 0);
            long now = System.currentTimeMillis();
            if (up && upSince < 0) upSince = now;
            if (!up) upSince = -1;
            if (upSince >= 0) longest = Math.max(longest, now - upSince);
            Thread.sleep(50);
        }
        System.out.println("N5: after the CA revocation the bridge's reconnects stayed up at most " + longest + " ms");
        assertTrue(longest < 2000, "a reconnect to a revoked region stayed up " + longest + " ms");
    }

    private static boolean bridgeUp(BrokerService accepting, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        do {
            for (org.apache.activemq.broker.Connection c : accepting.getRegionBroker().getClients()) if (c.isNetworkConnection()) return true;
            if (ms > 0) Thread.sleep(50);
        } while (System.currentTimeMillis() < end);
        return false;
    }
}
