package io.cresco.agent.controller.health;

import org.apache.felix.hc.api.Result;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** controller#22: per-session dataplane queue depth, and a listener that stops draining, in the controller health. */
class DataPlaneQueueDepthTest {

    private static Map<String, Object[]> queues(Object[]... rows) {
        Map<String, Object[]> m = new TreeMap<>();
        for (Object[] r : rows) m.put((String) r[0], new Object[]{r[1], r[2], r[3]});
        return m;
    }

    @Test
    void aBusyListenerThatKeepsDrainingIsOkAndItsDepthIsReported() {
        Map<String, long[]> p = new HashMap<>();
        assertEquals(Result.Status.OK, DataPlaneHealthCheck.evaluate(queues(new Object[]{"l1", "shard-0", 100, 5L}), p, 0, 30_000).getStatus());
        Result r = DataPlaneHealthCheck.evaluate(queues(new Object[]{"l1", "shard-0", 100, 900L}), p, 60_000, 30_000);
        assertEquals(Result.Status.OK, r.getStatus(), "at the prefetch limit but delivering: busy, not stalled");
        assertTrue(r.toString().contains("shard-0=100"), r.toString());
    }

    @Test
    void aListenerHoldingMessagesWithoutTakingOneWarnsAfterTheStallTime() {
        Map<String, long[]> p = new HashMap<>();
        Object[] stuck = {"listener-zombie", "shard-2", 7, 42L};
        Object[] fine = {"listener-ok", "pooled", 0, 3L};
        assertEquals(Result.Status.OK, DataPlaneHealthCheck.evaluate(queues(stuck, fine), p, 0, 30_000).getStatus());
        assertEquals(Result.Status.OK, DataPlaneHealthCheck.evaluate(queues(stuck, fine), p, 29_000, 30_000).getStatus());
        Result r = DataPlaneHealthCheck.evaluate(queues(stuck, fine), p, 31_000, 30_000);
        assertEquals(Result.Status.WARN, r.getStatus());
        assertTrue(r.toString().contains("shard-2 listener listener"), r.toString());
        assertFalse(r.toString().contains("pooled listener"), r.toString());
        Object[] moved = {"listener-zombie", "shard-2", 7, 43L};
        assertEquals(Result.Status.OK, DataPlaneHealthCheck.evaluate(queues(moved, fine), p, 32_000, 30_000).getStatus(), "it took one: clears");
    }

    @Test
    void anIdleListenerWithAnEmptyQueueNeverWarns() {
        Map<String, long[]> p = new HashMap<>();
        for (long t = 0; t < 600_000; t += 10_000)
            assertEquals(Result.Status.OK, DataPlaneHealthCheck.evaluate(queues(new Object[]{"l", "pooled", 0, 1L}), p, t, 30_000).getStatus());
    }
}
