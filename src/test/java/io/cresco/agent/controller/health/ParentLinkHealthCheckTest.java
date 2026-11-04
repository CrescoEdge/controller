package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.felix.hc.api.Result;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Audit item: a region must declare its global lost when the global stops answering, bridge of its own or not. */
class ParentLinkHealthCheckTest {

    private static ControllerEngine region(Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        // no thread dump: StallDiagnostics' throttle is JVM-wide, and StallDiagnosticsTest asserts on it
        cfg.put("health_link_stale_threaddump", "false");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("plh").toString(), cfg, m -> { }, "rgA", "rc");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = new io.cresco.agent.core.ControllerStateImp(pb, null) {
            @Override public ControllerMode getControllerState() { return ControllerMode.REGION_GLOBAL; }
            @Override public boolean isRegionalController() { return true; }
            @Override public String getRegion() { return "rgA"; }
            @Override public String getAgent() { return "rc"; }
        };
        return e;
    }

    private static ParentLinkHealthCheck check(ControllerEngine e, long pongAgeMs, long pingIntervalMs) {
        return new ParentLinkHealthCheck(e) {
            @Override protected long[] globalPong() { return new long[]{System.currentTimeMillis() - pongAgeMs, pingIntervalMs}; }
        };
    }

    @Test
    void aRegionWhoseGlobalStoppedAnsweringGoesNonOkWithinTheBound() throws Exception {
        ControllerEngine e = region(Map.of());
        Result fresh = check(e, 1_000, 5_000).execute();
        assertEquals(Result.Status.OK, fresh.getStatus(), "a fresh pong: " + fresh);
        long bound = ParentLinkHealthCheck.regionStaleThresholdMs(e.getPluginBuilder(), 5_000);
        assertEquals(30_000, bound, "default: three ping intervals, at least 30 s");
        Result stale = check(e, bound + 1, 5_000).execute();
        assertEquals(Result.Status.TEMPORARILY_UNAVAILABLE, stale.getStatus(), "a region never declared its global lost: " + stale);
        assertTrue(ParentLinkHealthCheck.regionStaleThresholdMs(e.getPluginBuilder(), 20_000) == 60_000, "scales with the ping");
    }

    @Test
    void theRegionThresholdIsConfigurableAndTheOptOutIsExplicit() throws Exception {
        ControllerEngine tuned = region(Map.of("health_link_region_stale_ms", "12000"));
        assertEquals(Result.Status.TEMPORARILY_UNAVAILABLE, check(tuned, 12_001, 5_000).execute().getStatus());
        ControllerEngine optOut = region(Map.of("health_link_region_ping", "false"));
        Result r = check(optOut, 10 * 60_000, 5_000).execute();
        assertEquals(Result.Status.OK, r.getStatus());
        assertTrue(r.toString().contains("health_link_region_ping=false"), r.toString());
    }
}
