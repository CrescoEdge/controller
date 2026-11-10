package io.cresco.agent.controller.regionalcontroller;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The peer cost probe's via-global path (see communication.SourceRouteOffTest for the refusals it caused). */
class PeerProbeRouteTest {

    @Test
    void noViaGlobalProbeWithoutSourceRouting() {
        // net_source_routing off: the global does not relay it, so the probe would only be refused there
        assertNull(RegionHealthWatcher.viaGlobalRoute(false, "r2", "rc2", "g", "gc", "r3", "rc3"));
        assertEquals("g,gc;r3,rc3", RegionHealthWatcher.viaGlobalRoute(true, "r2", "rc2", "g", "gc", "r3", "rc3"));
    }

    @Test
    void theGlobalAndThePeersInItsRegionHaveNoViaGlobalPath() {
        assertNull(RegionHealthWatcher.viaGlobalRoute(true, "g", "gc", "g", "gc", "r3", "rc3"));
        assertNull(RegionHealthWatcher.viaGlobalRoute(true, "r2", "rc2", "g", "gc", "g", "x"));
        assertNull(RegionHealthWatcher.viaGlobalRoute(true, "r2", "rc2", null, null, "r3", "rc3"));
    }

    @Test
    void onlyAPongIsARoundTrip() {
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "r3", "rc3", null, "r2", "rc2", null, false, false);
        ping.setParam("action", "ping");
        ping.setParam("status_code", "401");                // a refused probe comes back as its request, turned around
        assertFalse(RegionHealthWatcher.isPong(ping));
        assertFalse(RegionHealthWatcher.isPong(null));
        ping.setParam("action", "pong");
        assertTrue(RegionHealthWatcher.isPong(ping));
    }
}
