package io.cresco.agent.controller.statemachine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The state-export diff-gate: unchanged sections are not re-sent, except on the periodic full tick and when a
 * (re-)registered node asks for a full export now. Before requestFull existed a node whose parent had marked it
 * LOST sent nothing that restored its plugins until the periodic full tick (up to 4 x 15 s).
 */
class StateExportGateTest {

    /** One tick: which of the three sections go out. */
    private static String tick(StateExportGate g, String region, String agent, String plugins) {
        g.beginTick();
        StringBuilder sb = new StringBuilder();
        if (g.changed("regionconfigs", region) != null) sb.append('R');
        if (g.changed("agentconfigs", agent) != null) sb.append('A');
        if (g.changed("pluginconfigs", plugins) != null) sb.append('P');
        g.sent();
        return sb.toString();
    }

    @Test
    void unchanged_sections_go_out_only_on_the_periodic_full_tick() {
        StateExportGate g = new StateExportGate(4);
        assertEquals("RAP", tick(g, "r", "a", "p"));     // tick 1: first export
        assertEquals("", tick(g, "r", "a", "p"));        // 2
        assertEquals("P", tick(g, "r", "a", "p2"));      // 3: a change goes out at once
        assertEquals("RAP", tick(g, "r", "a", "p2"));    // 4: periodic full
        assertEquals("", tick(g, "r", "a", "p2"));       // 5
    }

    @Test
    void a_requested_full_export_goes_out_on_the_next_tick_and_only_once() {
        StateExportGate g = new StateExportGate(1000);
        assertEquals("RAP", tick(g, "r", "a", "p"));
        assertEquals("", tick(g, "r", "a", "p"));
        // the node re-registered after its parent marked it LOST: nothing changed locally
        g.requestFull();
        g.requestFull();                                  // repeated requests coalesce
        assertTrue(g.fullPending());
        assertTrue(g.beginTick(), "this tick serves the request");
        assertEquals("r", g.changed("regionconfigs", "r"));
        assertEquals("a", g.changed("agentconfigs", "a"));
        assertEquals("p", g.changed("pluginconfigs", "p"));
        g.sent();
        assertFalse(g.fullPending());
        assertEquals("", tick(g, "r", "a", "p"), "served once, then diff-gated again");
    }

    @Test
    void a_request_made_while_a_tick_runs_stays_pending_for_the_next_tick() {
        StateExportGate g = new StateExportGate(1000);
        tick(g, "r", "a", "p");
        assertFalse(g.beginTick());
        assertNull(g.changed("pluginconfigs", "p"));
        g.requestFull();                                  // e.g. registration completes mid-tick
        g.sent();
        assertTrue(g.fullPending(), "the request postdates this tick's export");
        assertEquals("RAP", tick(g, "r", "a", "p"));
        assertFalse(g.fullPending());
    }

    @Test
    void a_failed_send_keeps_the_request_and_resends_its_sections() {
        StateExportGate g = new StateExportGate(1000);
        tick(g, "r", "a", "p");
        g.requestFull();
        assertTrue(g.beginTick());
        assertEquals("p", g.changed("pluginconfigs", "p"));
        g.sendFailed();                                   // the dataplane refused the message
        assertTrue(g.fullPending());
        assertEquals("RAP", tick(g, "r", "a", "p"));
        assertFalse(g.fullPending());
    }

    @Test
    void absent_sections_are_never_sent() {
        StateExportGate g = new StateExportGate(1);
        assertEquals("AP", tick(g, null, "a", "p"));
        assertEquals("AP", tick(g, null, "a", "p"));      // every tick full when fullEveryTicks = 1
    }
}
