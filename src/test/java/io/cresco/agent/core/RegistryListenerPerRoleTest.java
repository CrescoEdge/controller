package io.cresco.agent.core;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.MessageListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The registry listeners (the state exports of the nodes below a controller) are one per role: a region takes its
 * agents' exports with or without a global, the global also takes the regions', and a listener is removed when its
 * role is left. They used to be added on every entry to GLOBAL and REGION_GLOBAL and never removed (the id kept was
 * always null): a global held two of each from boot (globalInit enters GLOBAL twice) and applied every region export
 * twice, a region gained one per global rejoin, and a region with no global had none.
 */
class RegistryListenerPerRoleTest {

    /** The persistence under test, its dataplane subscriptions recorded instead of made. */
    private static class Recording extends ControllerStatePersistance {
        final List<String> live = new ArrayList<>();      // the selectors of the subscriptions held now
        int added, removed;

        Recording() { super(plugin, dbe); }

        @Override String addRegistryListener(MessageListener ml, String selector) {
            added++;
            live.add(selector);
            return "listener-" + added + "|" + selector;
        }

        @Override void removeRegistryListener(String listenerId) {
            removed++;
            assertTrue(live.remove(listenerId.substring(listenerId.indexOf('|') + 1)), "removed a listener that was not held: " + listenerId);
        }

        @Override boolean holdsRegistryListener(String listenerId) {
            return live.contains(listenerId.substring(listenerId.indexOf('|') + 1));
        }

        long agentExports() { return live.stream().filter(s -> s.contains("'AGENT'")).count(); }
        long regionExports() { return live.stream().filter(s -> s.contains("'REGION'")).count(); }

        void enter(ControllerMode mode) { setControllerState(mode, "test", null, null, null, null, "r1", "a1"); }
    }

    private static PluginBuilder plugin;
    private static DBEngine dbe;

    @BeforeAll
    static void database() throws Exception {
        Path tmp = Files.createTempDirectory("registry-listeners");
        plugin = TestAgentService.plugin(tmp.toString(), new HashMap<>());
        dbe = new DBEngine(plugin);
    }

    @AfterAll
    static void close() {
        try { dbe.shutdown(); } catch (Exception ignored) { }
    }

    private static Recording persistence() {
        return new Recording();
    }

    @Test
    void onlyAGlobalOfASuperuserTenantTakesEveryTenantsRegionExports() {
        java.util.Set<String> su = java.util.Set.of("cresco-system");
        assertTrue(ControllerStatePersistance.takesEveryTenantsRegionExports(true, "cresco-system", su),
                "the global the tenants' regions join listened in its own tenant's namespace only: no tenant's region registered");
        assertFalse(ControllerStatePersistance.takesEveryTenantsRegionExports(true, "ta", su), "a tenant's global takes its own tenant's regions only");
        assertFalse(ControllerStatePersistance.takesEveryTenantsRegionExports(false, "cresco-system", su), "without namespacing there is one topic");
        assertFalse(ControllerStatePersistance.takesEveryTenantsRegionExports(true, null, su));
        assertFalse(ControllerStatePersistance.takesEveryTenantsRegionExports(true, "ta", java.util.Set.of()));
    }

    @Test
    void aGlobalHoldsOneListenerOfEachKindHoweverOftenItEntersTheRole() {
        Recording p = persistence();
        p.enter(ControllerMode.REGION);
        p.enter(ControllerMode.GLOBAL_INIT);
        p.enter(ControllerMode.GLOBAL);
        p.enter(ControllerMode.GLOBAL);                   // globalInit sets GLOBAL twice
        assertEquals(1, p.agentExports(), "a global applied every agent export once per entry to GLOBAL");
        assertEquals(1, p.regionExports(), "a global applied every region export once per entry to GLOBAL");
    }

    @Test
    void aRegionKeepsItsOneListenerThroughEveryLossAndRejoinOfTheGlobal() {
        Recording p = persistence();
        p.enter(ControllerMode.REGION);
        for (int rejoin = 0; rejoin < 5; rejoin++) {
            p.enter(ControllerMode.REGION_GLOBAL_INIT);
            assertEquals(1, p.agentExports(), "joining a global: the region still takes its agents' exports");
            p.enter(ControllerMode.REGION_GLOBAL);
            p.enter(ControllerMode.REGION_GLOBAL_FAILED);
            assertEquals(1, p.agentExports(), "global lost: the region still takes its agents' exports");
        }
        p.enter(ControllerMode.REGION_GLOBAL);
        assertEquals(1, p.agentExports(), "a region gained one agent-export listener per global rejoin");
        assertEquals(0, p.regionExports(), "only the global takes region exports");
        assertEquals(1, p.added);
        assertEquals(0, p.removed);
    }

    @Test
    void aRegionWithoutAGlobalTakesItsAgentsExports() {
        Recording p = persistence();
        p.enter(ControllerMode.REGION_INIT);
        assertEquals(0, p.agentExports(), "no broker yet");
        p.enter(ControllerMode.REGION);
        assertEquals(1, p.agentExports(), "a region with no global ignored its agents' exports");
        assertTrue(p.live.get(0).contains("region_id = 'r1'"), "its own region's agents only: " + p.live);
    }

    @Test
    void theListenersAreRemovedWhenTheRoleIsLeft() {
        Recording p = persistence();
        p.enter(ControllerMode.REGION);
        p.enter(ControllerMode.GLOBAL);
        p.enter(ControllerMode.GLOBAL_SHUTDOWN);
        assertTrue(p.live.isEmpty(), "still subscribed after the role was left: " + p.live);
        assertEquals(2, p.removed);

        p.enter(ControllerMode.REGION);
        p.enter(ControllerMode.REGION_GLOBAL);
        assertEquals(1, p.agentExports());
        p.enter(ControllerMode.REGION_SHUTDOWN);
        assertTrue(p.live.isEmpty(), "still subscribed after the region stopped: " + p.live);
    }

    @Test
    void aSubscriptionThatCouldNotBeMadeIsTriedAgainOnTheNextEntry() {
        boolean[] dataplaneUp = {false};
        Recording p = new Recording() {
            @Override String addRegistryListener(MessageListener ml, String selector) {
                return dataplaneUp[0] ? super.addRegistryListener(ml, selector) : null;
            }
        };
        p.enter(ControllerMode.REGION);
        assertEquals(0, p.agentExports());
        dataplaneUp[0] = true;
        p.enter(ControllerMode.REGION_GLOBAL);
        assertEquals(1, p.agentExports(), "a listener the dataplane refused once is registered at the next role entry");
    }

    @Test
    void aListenerTheDataplaneLostIsRegisteredAgainOnceOnTheNextStateChange() {
        Recording p = persistence();
        p.enter(ControllerMode.REGION);
        p.enter(ControllerMode.GLOBAL);
        assertEquals(2, p.added);
        p.live.clear();                                   // a dataplane reconnect could not re-add them (updateConnections)
        p.enter(ControllerMode.GLOBAL);
        assertEquals(1, p.agentExports(), "the id of a listener the dataplane no longer holds was kept: no agent export is taken again");
        assertEquals(1, p.regionExports(), "the id of a listener the dataplane no longer holds was kept: no region export is taken again");
        p.enter(ControllerMode.GLOBAL);
        assertEquals(4, p.added, "registered again exactly once each");
        assertEquals(0, p.removed, "a listener the dataplane does not hold is not removed from it");

        Recording region = persistence();
        region.enter(ControllerMode.REGION_GLOBAL);
        region.live.clear();
        region.enter(ControllerMode.REGION_GLOBAL_FAILED);
        assertEquals(1, region.agentExports(), "a region that lost its listener and then its global took no agent export again");
        assertEquals(2, region.added);
    }

    @Test
    void whichRolesTakeWhichExports() {
        for (ControllerMode m : ControllerMode.values()) {
            boolean region = m == ControllerMode.REGION || m == ControllerMode.REGION_GLOBAL_INIT || m == ControllerMode.REGION_GLOBAL_FAILED
                    || m == ControllerMode.REGION_GLOBAL || m == ControllerMode.GLOBAL_INIT || m == ControllerMode.GLOBAL;
            assertEquals(region, ControllerStatePersistance.takesAgentExports(m), m.name());
            assertEquals(m == ControllerMode.GLOBAL, ControllerStatePersistance.takesRegionExports(m), m.name());
        }
    }
}
