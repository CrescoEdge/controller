package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.communication.BrokerStatusType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region joining its global builds its OWN bridge to it unless it already has one: a live route over the global's
 * bridge to the region is not an uplink (the global tears that bridge down when its monitor fails the group, and the
 * region then stayed detached -- live restart drill after a region restart).
 */
class RegionUplinkTest {

    @Test
    void aRouteOverTheGlobalsBridgeAloneIsNotAnUplink() {
        assertTrue(ControllerSMHandler.needsOwnUplink(true, null), "reachable only over the global's bridge: no own bridge was built");
        assertTrue(ControllerSMHandler.needsOwnUplink(true, BrokerStatusType.FAILED), "own bridge FAILED: not rebuilt");
        assertTrue(ControllerSMHandler.needsOwnUplink(true, BrokerStatusType.STOPPED), "own bridge STOPPED: not rebuilt");
    }

    @Test
    void anOwnBridgeUpOrBeingBuiltIsNotDuplicated() {
        assertFalse(ControllerSMHandler.needsOwnUplink(true, BrokerStatusType.ACTIVE));
        assertFalse(ControllerSMHandler.needsOwnUplink(true, BrokerStatusType.INIT));
        assertFalse(ControllerSMHandler.needsOwnUplink(true, BrokerStatusType.STARTING));
    }

    @Test
    void notReachableAlwaysSubmitsTheGlobal() {
        for (BrokerStatusType s : BrokerStatusType.values()) assertTrue(ControllerSMHandler.needsOwnUplink(false, s));
        assertTrue(ControllerSMHandler.needsOwnUplink(false, null));
    }
}
