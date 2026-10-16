package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.MsgRouter.Hop;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Pins {@link MsgRouter}'s routing decision for every header combination against the route table of controller
 * 1.3 191d895 ({@code msgrouter-table-191d895.txt}, extracted from that version's {@code switch(routePath)})
 * plus that version's destination-resolved catch-all, re-implemented here independently.
 *
 * <p>A cell is: the node's role (agent / regional controller / global controller), the message's
 * isRegional/isGlobal flags, each of the six header fields (dst/src region, agent, plugin) absent, naming
 * another node or naming this node, and the node's plugin id (the controller's is null in production). Any
 * cell that decides differently from 191d895 must be listed in {@link #CHANGED}; nothing else may move.
 */
class MsgRouterTableTest {

    private static final String SELF_REGION = "rs";
    private static final String SELF_AGENT = "as";
    private static final String[] PLUGIN_IDS = {null, "plugin/0"};

    /**
     * The cells this branch deliberately changes against 191d895, keyed "rp=<code> <flags>", valued
     * "<before> -> <after>".
     */
    static final Map<String, String> CHANGED = new TreeMap<>();
    static {
        // a regional-controller message from another region addressed to this regional (non-global) controller:
        // from a plugin/wsapi there (20847) or from a controller there (20815). Global-flagged: unchanged.
        CHANGED.put("rp=20815 regional", "LOCAL_AGENT -> LOCAL_REGIONAL_CONTROLLER");
        CHANGED.put("rp=20847 regional", "LOCAL_AGENT -> LOCAL_REGIONAL_CONTROLLER");
    }

    static Map<Integer, Hop> pinnedTable() throws Exception {
        Map<Integer, Hop> table = new TreeMap<>();
        try (InputStream in = MsgRouterTableTest.class.getResourceAsStream("msgrouter-table-191d895.txt")) {
            assertNotNull(in, "pinned table resource missing");
            BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] f = line.split("\\s+");
                table.put(Integer.parseInt(f[0]), Hop.valueOf(f[1]));
            }
        }
        return table;
    }

    /** The 191d895 decision: the pinned table, else its catch-all (re-implemented, not MsgRouter's). */
    static Hop pinnedDecision(Map<Integer, Hop> table, int rp, MsgEvent m, String region, String agent, String pluginId) {
        Hop h = table.get(rp);
        if (h != null) return h;
        boolean forThisNode = region.equals(m.getDstRegion()) && agent.equals(m.getDstAgent());
        if (forThisNode && (m.getDstPlugin() == null || m.getDstPlugin().equals(pluginId))) return Hop.LOCAL_AGENT;
        if (forThisNode) return Hop.LOCAL_PLUGIN;
        return Hop.REMOTE_REGION;
    }

    static String flags(MsgEvent m) {
        return (m.isRegional() ? "regional" : "") + (m.isGlobal() ? "+global" : "") + (!m.isRegional() && !m.isGlobal() ? "agent/plugin" : "");
    }

    @Test
    void everyTableCodeDecidesAsPinned() throws Exception {
        Map<Integer, Hop> table = pinnedTable();
        assertEquals(72, table.size(), "191d895 has 72 table codes");
        for (boolean reg : new boolean[]{false, true}) {
            for (boolean glob : new boolean[]{false, true}) {
                MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "x", "y", null, "x", "y", null, reg, glob);
                for (Map.Entry<Integer, Hop> e : table.entrySet()) {
                    assertEquals(e.getValue(), MsgRouter.decide(e.getKey(), m, SELF_REGION, SELF_AGENT, null),
                            "table code " + e.getKey());
                }
            }
        }
    }

    @Test
    void everyCellDecidesAsPinnedExceptTheChangedOnes() throws Exception {
        Map<Integer, Hop> table = pinnedTable();
        Map<String, String> changed = new TreeMap<>();
        TreeSet<Integer> codes = new TreeSet<>();
        int cells = 0;
        boolean[][] roles = {{false, false}, {true, false}, {true, true}};       // {regional, global} controller
        boolean[][] msgFlags = {{false, false}, {true, false}, {true, true}, {false, true}};
        for (String pluginId : PLUGIN_IDS) {
            String[] regions = {null, "ro", SELF_REGION};
            String[] agents = {null, "ao", SELF_AGENT};
            String[] plugins = pluginId == null ? new String[]{null, "plugin/9"} : new String[]{null, "plugin/9", pluginId};
            for (boolean[] role : roles)
            for (boolean[] mf : msgFlags)
            for (String dr : regions) for (String da : agents) for (String dp : plugins)
            for (String sr : regions) for (String sa : agents) for (String sp : plugins) {
                MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, sr, sa, sp, dr, da, dp, mf[0], mf[1]);
                int rp = MsgRouter.routePath(m, role[0], role[1], SELF_REGION, SELF_AGENT, pluginId);
                codes.add(rp);
                cells++;
                Hop before = pinnedDecision(table, rp, m, SELF_REGION, SELF_AGENT, pluginId);
                Hop after = MsgRouter.decide(rp, m, SELF_REGION, SELF_AGENT, pluginId);
                if (before != after) {
                    changed.merge("rp=" + rp + " " + flags(m), before + " -> " + after,
                            (a, b) -> a.equals(b) ? a : a + " | " + b);
                }
            }
        }
        assertFalse(codes.contains(-1));
        System.out.println("MsgRouterTableTest: " + cells + " cells, " + codes.size() + " distinct route codes, changed: " + changed);
        assertEquals(CHANGED, changed, "cells that decide differently from 191d895");
    }
}
