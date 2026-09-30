package io.cresco.agent.controller.health;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StallDiagnosticsTest {

    @Test
    void namesTheParkedPingThreadAndTheLockOwner() throws Exception {
        Object lock = new Object();
        CountDownLatch held = new CountDownLatch(1), release = new CountDownLatch(1);
        Thread owner = new Thread(() -> { synchronized (lock) { held.countDown(); try { release.await(); } catch (InterruptedException ignore) { } } },
                "net-autotuner");
        owner.start(); held.await();
        Thread ping = new Thread(() -> { synchronized (lock) { } }, "AgentActivePingTimer");
        ping.start();
        while (ping.getState() != Thread.State.BLOCKED) Thread.sleep(5);
        try {
            String d = StallDiagnostics.dump("test");
            assertTrue(d.contains("\"AgentActivePingTimer\" BLOCKED"), d);
            assertTrue(d.contains("owned by \"net-autotuner\""), d);
            assertTrue(d.contains("- locked"), d);
        } finally {
            release.countDown(); owner.join(); ping.join();
        }
    }

    @Test
    void atMostOncePerInterval() {
        String first = StallDiagnostics.dumpIfDue("a", 60_000);
        String second = StallDiagnostics.dumpIfDue("b", 60_000);
        // the first call may be throttled by the other test's dumps only if they used dumpIfDue; they did not
        assertNotNull(first);
        assertNull(second);
    }
}
