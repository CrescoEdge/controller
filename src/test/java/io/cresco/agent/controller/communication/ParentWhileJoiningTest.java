package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.Fixture;
import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.StubState;
import io.cresco.library.agent.ControllerMode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region's broker knows its global as its parent from the moment the region starts to join it, not from the moment the
 * join is recorded. The bridge to the global is up about a second before that, and what it brought in that second -- an
 * advert of another region that the global relays -- was stripped of its caller as if a peer region had brought it
 * ({@code a node of region r1 over the bridge of region glob (outside its subtree)}; fabric matrix, after a restart of the
 * global: rc3 15:13:14,866, the join recorded at 15:13:15,072).
 */
class ParentWhileJoiningTest {

    @Test
    void theGlobalARegionIsJoiningIsItsParentBeforeTheJoinIsRecorded() {
        Fixture f = new Fixture(ControllerMode.REGION_GLOBAL_INIT, Map.of());
        StubState region = f.state;
        assertNull(ActiveBroker.parentOf(region, f.engine.joiningGlobal()), "a region that has no global and joins none has no parent");

        f.engine.setJoiningGlobal("glob", "gc");                             // discovery authenticated it; the bridge is being built
        PeerIdentity.Expected parent = ActiveBroker.parentOf(region, f.engine.joiningGlobal());
        assertNotNull(parent, "until the join is recorded the global's bridge was a peer's: what it relayed was stripped");
        assertEquals("glob", parent.region);
        assertEquals("gc", parent.agent);

        f.engine.setJoiningGlobal(null, null);                               // the join failed
        assertNull(ActiveBroker.parentOf(region, f.engine.joiningGlobal()), "a global that was not joined stays the parent");
    }

    @Test
    void theGlobalARegionHasJoinedIsItsParentWhateverItTriesNext() {
        Fixture f = new Fixture(ControllerMode.REGION_GLOBAL, Map.of());
        f.state.globalRegion = "glob";
        f.state.globalAgent = "gc";
        PeerIdentity.Expected parent = ActiveBroker.parentOf(f.state, new PeerIdentity.Expected("other", "gc"));
        assertEquals("glob", parent.region, "the recorded global is the parent");
    }

    @Test
    void theGlobalItselfHasNoParent() {
        Fixture f = new Fixture(ControllerMode.GLOBAL, Map.of());
        assertNull(ActiveBroker.parentOf(f.state, new PeerIdentity.Expected("glob", "gc")));
        assertNull(ActiveBroker.parentOf(null, null));
    }
}
