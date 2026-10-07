package io.cresco.agent.controller.health;

import org.apache.felix.hc.api.Result;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** controller#22: per-session dataplane queue depth in the controller health. */
class DataPlaneQueueDepthTest {

    @Test
    void depthsBelowTheThresholdAreOkAndReported() {
        Map<String, Integer> d = new TreeMap<>(Map.of("pooled", 0, "shard-0", 12, "shard-3", 4999));
        Result r = DataPlaneHealthCheck.depthResult(d, 5000);
        assertEquals(Result.Status.OK, r.getStatus());
        assertTrue(r.toString().contains("shard-3=4999"), r.toString());
    }

    @Test
    void aSessionThatStopsDrainingWarnsAndIsNamed() {
        Map<String, Integer> d = new TreeMap<>(Map.of("pooled", 3, "shard-2", 7000));
        Result r = DataPlaneHealthCheck.depthResult(d, 5000);
        assertEquals(Result.Status.WARN, r.getStatus());
        assertTrue(r.toString().contains("shard-2=7000"), r.toString());
        assertFalse(r.toString().contains("not draining (queue depth >= 5000): pooled"), r.toString());
    }
}
