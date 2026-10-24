package io.cresco.agent.core;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * GaiaKeep gfs#26: a regional controller that lost its global keeps its own broker, its sessions and its agents, so
 * its dataplane stays usable while it joins the global again. Before, every dataplane send of the region's controller
 * (a regional wsapi gateway's streams) waited dataplane_session_wait_sec and was then dropped for the whole outage.
 */
class DataPlaneUsableTest {

    private ControllerStateImp in(ControllerMode mode) {
        PluginBuilder pb;
        try { pb = TestAgentService.plugin(Files.createTempDirectory("dpu").toString(), Map.of()); }
        catch (java.io.IOException e) { throw new RuntimeException(e); }
        return new ControllerStateImp(pb, null) {
            @Override public synchronized ControllerMode getControllerState() { return mode; }
            @Override public boolean isActive() {
                return mode == ControllerMode.AGENT || mode == ControllerMode.REGION || mode == ControllerMode.REGION_GLOBAL
                        || mode == ControllerMode.GLOBAL || mode == ControllerMode.STANDALONE;
            }
        };
    }

    @Test
    void aRegionThatLostItsGlobalKeepsItsDataplane() {
        assertTrue(in(ControllerMode.REGION_GLOBAL_FAILED).isDataPlaneUsable(), "global lost: the region's own broker still runs");
        assertTrue(in(ControllerMode.REGION_GLOBAL_INIT).isDataPlaneUsable(), "joining the global again");
    }

    @Test
    void activeModesAreUsableAndFailedOnesAreNot() {
        for (ControllerMode m : new ControllerMode[]{ControllerMode.AGENT, ControllerMode.REGION, ControllerMode.REGION_GLOBAL, ControllerMode.GLOBAL})
            assertTrue(in(m).isDataPlaneUsable(), m.name());
        for (ControllerMode m : new ControllerMode[]{ControllerMode.AGENT_FAILED, ControllerMode.AGENT_INIT, ControllerMode.REGION_FAILED,
                ControllerMode.REGION_INIT, ControllerMode.PRE_INIT})
            assertFalse(in(m).isDataPlaneUsable(), m.name());
    }
}
