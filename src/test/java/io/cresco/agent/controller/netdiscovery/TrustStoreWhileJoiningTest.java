package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import java.security.cert.Certificate;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Trust managers are built for a connection while other joins add their certificates to the truststore. A KeyStore must
 * not be walked while it is added to: on the DGX gate of agent release 1.3.378 one of 350 concurrent joins failed there
 * ("getTrustManagers Error : null", a ConcurrentModificationException) and its connection was left without a trust
 * manager.
 */
class TrustStoreWhileJoiningTest {

    @Test
    void trustManagersAreBuiltWhileJoinsAddTheirCertificates() throws Exception {
        ControllerEngine e = DiscoveryDosTest.engine("rgA", "gc", true, null);
        CertificateManager cm = new CertificateManager(e);
        e.setCertificateManager(cm);
        Certificate[] chain = cm.getPublicCertificate();
        for (int i = 0; i < 200; i++) cm.addCertificatesToTrustStore("seed" + i, chain);       // a store worth walking

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger added = new AtomicInteger(), built = new AtomicInteger(), failed = new AtomicInteger();
        final int joinsWanted = 400, buildsWanted = 40;
        long t0 = System.nanoTime();
        Thread joins = new Thread(() -> {
            // joins go on until the builders have had their turn too: the two must overlap, however fast a join is (on a
            // fast machine 400 joins took 15 ms and ended before a builder thread had started; the test then showed nothing)
            long deadline = System.nanoTime() + 120_000_000_000L;
            for (int i = 0; (i < joinsWanted || built.get() < buildsWanted) && failed.get() == 0 && System.nanoTime() < deadline; i++) {
                cm.addCertificatesToTrustStore("join" + i, chain);                             // a new alias each time
                added.incrementAndGet();
            }
            stop.set(true);
        }, "joins");
        Thread[] builders = new Thread[4];
        for (int b = 0; b < builders.length; b++) {
            builders[b] = new Thread(() -> {
                while (!stop.get()) {
                    TrustManager[] tm = cm.getTrustManagers();
                    if (tm == null || tm.length == 0) failed.incrementAndGet(); else built.incrementAndGet();
                    // a connection now and then, not four threads that do nothing else: the truststore's lock is not
                    // fair, and builders in a tight loop starve the joins (on a two-core machine 259 joins in 120 s beside
                    // 745,594 builds). That a handshake does not walk the store under a lock at all is another change.
                    java.util.concurrent.locks.LockSupport.parkNanos(500_000L);
                }
            }, "builder-" + b);
        }
        joins.start();
        for (Thread t : builders) t.start();
        joins.join(240_000);
        stop.set(true);
        for (Thread t : builders) t.join(30_000);
        System.out.println("TrustStoreWhileJoiningTest: " + added.get() + " joins added in " + (System.nanoTime() - t0) / 1_000_000
                + " ms while " + built.get() + " trust managers were built, " + failed.get() + " failed");
        assertTrue(added.get() >= joinsWanted, "the joins added their certificates: " + added.get());
        assertTrue(built.get() >= buildsWanted || failed.get() > 0, "trust managers were built while the joins ran: " + built.get());
        assertEquals(0, failed.get(), "a connection was left without trust managers (" + built.get() + " built)");
    }
}
