package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Second adversarial review (registry). Each test asserts the SECURE outcome; a failure is a finding. */
class SecondReviewRegistryTest {

    private static PeerTrustRegistry open(Path f, List<String> rebind) {
        return PeerTrustRegistry.open(f, rebind, List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    private static String json(ControllerEngine e) {
        return e.getCertificateManager().getJsonFromCerts(e.getCertificateManager().getPublicCertificate());
    }

    /** The seal lasts one boot: the tampered file was moved aside, so the next boot opens an empty, unsealed registry. */
    @Test
    void aTamperedRegistryStaysSealedAcrossTheNextRestart() throws Exception {
        Path f = Files.createTempDirectory("r2").resolve("security").resolve("peer-trust.json");
        KeyPair legit = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgA", "a1", legit.getPublic(), "test").ok);
        Files.writeString(f, Files.readString(f).replace("rgA/a1", "rgA/a2"));                // any edit without the MAC key
        PeerTrustRegistry boot1 = open(f, List.of());
        assertTrue(boot1.isSealed(), "precondition: the first boot after tampering is sealed");
        PeerTrustRegistry boot2 = open(f, List.of());                                          // e.g. a crash loop / restart
        System.out.println("boot2 sealed=" + boot2.isSealed() + " pinCount=" + boot2.pinCount());
        assertTrue(boot2.isSealed() || !boot2.bind("t1", "rgA", "a1", attacker.getPublic(), "attacker").ok,
                "after one more restart the tampered registry is forgotten: rgA/a1 was bound to the attacker's key");
    }

    /** Deleting the file (the MAC key next to it untouched) re-opens every name, with no seal and no log line. */
    @Test
    void aDeletedRegistryIsNotSilentlyReplacedByAnEmptyOne() throws Exception {
        Path f = Files.createTempDirectory("r2").resolve("security").resolve("peer-trust.json");
        KeyPair legit = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgA", "a1", legit.getPublic(), "test").ok);
        assertTrue(Files.exists(f.resolveSibling("peer-trust.json.key")));
        Files.delete(f);
        PeerTrustRegistry after = open(f, List.of());
        assertTrue(after.isSealed() || !after.bind("t1", "rgA", "a1", attacker.getPublic(), "attacker").ok,
                "a deleted registry (MAC key still present) re-opened rgA/a1 to a new key");
    }

    /** A name listed in peer_identity_rebind can be re-taken by ANY key, any number of times, for as long as it is listed. */
    @Test
    void peerIdentityRebindIsOneShotNotAPermanentOpenDoor() throws Exception {
        Path f = Files.createTempDirectory("r2").resolve("security").resolve("peer-trust.json");
        KeyPair old = TlsTestSupport.rsa(), reinstalled = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgG", "gc", old.getPublic(), "test").ok);
        PeerTrustRegistry r = open(f, List.of("rgG/gc"));                          // operator re-enrolls the wiped global
        assertTrue(r.bind("t1", "rgG", "gc", reinstalled.getPublic(), "discovery").ok);
        PeerTrustRegistry.BindResult hijack = r.bind("t1", "rgG", "gc", attacker.getPublic(), "discovery");
        System.out.println("second rebind while listed: ok=" + hijack.ok + " (" + hijack.reason + ")");
        assertFalse(hijack.ok, "after the operator's rebind completed, a third key took rgG/gc too");
    }

    /**
     * Regional CA: one corrupted peer-trust.json takes the region down on the next boot (its own CA and every agent
     * binding are gone), and the boot after that re-issues an existing agent's name to a new key.
     */
    @Test
    void aCorruptedRegistryNeitherIsolatesTheRegionNorReopensItsNames() throws Exception {
        Path dir = Files.createTempDirectory("node2");
        Map<String, Object> cfg = Map.of("security_regional_ca", "true", "node_identity_dir", dir.toString());
        ControllerEngine first = TlsTestSupport.engine("rgA", "rc", cfg);
        first.getCertificateManager().ensureRegionCA();
        ControllerEngine a1 = TlsTestSupport.engine("rgA", "a1", null);
        String issued = first.getCertificateManager().issueEnrollment(json(a1), DiscoveryType.AGENT);
        assertNotNull(issued);
        X509Certificate[] a1Chain = first.getCertificateManager().getCertsfromJson(issued);
        assertNotNull(first.getCertificateManager().getPeerTrust().verifiedIdentity(a1Chain));

        Path reg = dir.resolve("peer-trust.json");
        byte[] b = Files.readAllBytes(reg);
        Files.write(reg, java.util.Arrays.copyOf(b, b.length - 1));                // a torn/corrupted file

        ControllerEngine boot1 = TlsTestSupport.engine("rgA", "rc", cfg);
        boot1.getCertificateManager().ensureRegionCA();
        PeerTrustRegistry r1 = boot1.getCertificateManager().getPeerTrust();
        CrescoIdentity stillA1 = r1.verifiedIdentity(a1Chain);
        String reEnroll = boot1.getCertificateManager().issueEnrollment(json(a1), DiscoveryType.AGENT);
        System.out.println("boot1: sealed=" + r1.isSealed() + " a1 verified=" + stillA1 + " a1 re-enrolled=" + (reEnroll != null)
                + " own identity verified=" + r1.verifiedIdentity(boot1.getCertificateManager().getPublicCertificate()));

        ControllerEngine boot2 = TlsTestSupport.engine("rgA", "rc", cfg);
        boot2.getCertificateManager().ensureRegionCA();
        ControllerEngine impostor = TlsTestSupport.engine("rgA", "a1", null);       // another key claiming a1
        String hijack = boot2.getCertificateManager().issueEnrollment(json(impostor), DiscoveryType.AGENT);
        System.out.println("boot2: sealed=" + boot2.getCertificateManager().getPeerTrust().isSealed()
                + " impostor issued rgA/a1=" + (hijack != null));

        assertNotNull(stillA1, "boot1: the region no longer recognises its enrolled agent a1 (one bad file = region outage)");
        assertNull(hijack, "boot2: the region CA issued the enrolled name rgA/a1 to a second key");
    }

    /**
     * Scale: every bind (including a refresh of a known key) rewrites and fsyncs the WHOLE registry under the lock that
     * every bridge send/dispatch takes (isBridgePeer / touchBridgePeer). Measures the stall.
     */
    @Test
    void registryWritesDoNotStallBridgeDispatch() throws Exception {
        Path f = Files.createTempDirectory("r2").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry r = open(f, List.of());
        java.security.PublicKey k = TlsTestSupport.rsa().getPublic();
        int n = Integer.getInteger("r2.n", 3000);
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) r.bind("t1", "rgA", "agent-" + i, k, "test");
        long buildMs = (System.nanoTime() - t0) / 1_000_000;
        r.allowBridgePeer("rgB", "rc");
        CrescoIdentity bridge = CrescoIdentity.of("t1", "rgB", "rc", null);
        java.util.concurrent.atomic.AtomicBoolean run = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicLong maxNs = new java.util.concurrent.atomic.AtomicLong();
        Thread dispatch = new Thread(() -> {
            while (run.get()) {
                long s = System.nanoTime();
                r.isBridgePeer(bridge);
                long d = System.nanoTime() - s;
                maxNs.accumulateAndGet(d, Math::max);
            }
        });
        dispatch.start();
        long s = System.nanoTime();
        int refreshes = 50;
        for (int i = 0; i < refreshes; i++) r.bind("t1", "rgA", "agent-" + i, k, "test");    // known keys: "refresh"
        long perRefreshMs = (System.nanoTime() - s) / 1_000_000 / refreshes;
        run.set(false);
        dispatch.join();
        System.out.println("registry: " + n + " bindings built in " + buildMs + " ms; file " + Files.size(f) + " bytes; "
                + "one known-key refresh = " + perRefreshMs + " ms; worst isBridgePeer (per-message bridge check) = "
                + (maxNs.get() / 1_000_000) + " ms");
        assertTrue(maxNs.get() / 1_000_000 < 5,
                "a per-message bridge check waited " + (maxNs.get() / 1_000_000) + " ms behind a whole-registry rewrite");
    }
}
