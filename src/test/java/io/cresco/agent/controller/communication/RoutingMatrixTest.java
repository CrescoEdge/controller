package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.MsgRouter.Hop;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end routing map over a small fabric, driven by {@link MsgRouter#routePath} / {@link MsgRouter#decide}
 * at every hop: where does a message of each class, built the way the wsapi builds it, end up for every
 * (origin, destination)?
 *
 * <pre>
 *   G  (rg/gc)  global + regional controller of region rg     AG (rg/ag) agent under G
 *   R1 (r1/rc1) regional controller                            A1 (r1/a1) agent under R1
 *   R2 (r2/rc2) regional controller                            A2 (r2/a2) agent under R2
 * </pre>
 *
 * A relay hop ({@code forwardTo*Region/Global}) is the broker network delivering to the queue named by the
 * message's forward destination, i.e. the next hop is that node. Each hop first applies the controller-tier step
 * ({@link MsgRouter#tierStep}, trail entry "(tier)"), then the route table. The regional-controller hop models
 * {@code RegionalExecutor.dispatch}: a message for the global ({@link ControllerTiers#targetsGlobal}) runs on the
 * global or, at a non-global region, goes on to the global ({@code remoteGlobalSend}); a regional-only function
 * with the global flag is refused; any other message runs on this node's RegionalExecutor when it is addressed
 * to it. Controllers have a null plugin id, the wsapi is {@code plugin/1}.
 */
class RoutingMatrixTest {

    static final class Node {
        final String name, region, agent;
        final boolean rc, gc;
        Node regional;
        Node(String name, String region, String agent, boolean rc, boolean gc) {
            this.name = name; this.region = region; this.agent = agent; this.rc = rc; this.gc = gc;
        }
    }

    static final Node G = new Node("G", "rg", "gc", true, true);
    static final Node R1 = new Node("R1", "r1", "rc1", true, false);
    static final Node R2 = new Node("R2", "r2", "rc2", true, false);
    static final Node AG = new Node("AG", "rg", "ag", false, false);
    static final Node A1 = new Node("A1", "r1", "a1", false, false);
    static final Node A2 = new Node("A2", "r2", "a2", false, false);
    static final Node[] NODES = {G, R1, R2, AG, A1, A2};
    static {
        G.regional = G; R1.regional = R1; R2.regional = R2; AG.regional = G; A1.regional = R1; A2.regional = R2;
    }

    static final String WSAPI = "plugin/1";

    static Node owner(String region, String agent) {
        for (Node n : NODES) if (n.region.equals(region) && n.agent.equals(agent)) return n;
        return null;
    }

    /** Route {@code m} from node {@code at} until it is consumed; returns the hop trail and the outcome. */
    static String trace(Node at, MsgEvent m) {
        List<String> trail = new ArrayList<>();
        for (int hops = 0; hops < 12; hops++) {
            // MsgRouter.route: the controller-tier step first (it may rewrite the address), then the route table
            MsgRouter.TierStep step = MsgRouter.tierStep(m, at.rc, at.region, at.agent);
            if (step == MsgRouter.TierStep.REFUSE) {
                trail.add(at.name + "(tier)");
                return String.join(">", trail) + " => REFUSED@" + at.name;
            }
            if (step == MsgRouter.TierStep.GLOBAL_VIA_THIS_CONTROLLER) {
                m.setForwardDst(at.region, at.agent, null);
            }
            int rp = MsgRouter.routePath(m, at.rc, at.gc, at.region, at.agent, null);
            Hop hop = step.hop != null ? step.hop : MsgRouter.decide(rp, m, at.region, at.agent, null);
            trail.add(at.name + (step.hop != null ? "(tier)" : "(" + rp + ")"));
            String path = String.join(">", trail);
            switch (hop) {
                case LOCAL_AGENT:
                    return path + " => AgentExecutor@" + at.name;
                case LOCAL_PLUGIN:
                    return path + " => " + m.getDstPlugin() + "@" + at.name;
                case LOCAL_REGIONAL_CONTROLLER:
                    if (!at.rc) return path + " => DROP(no regional controller)@" + at.name;
                    // RegionalExecutor.dispatch
                    if (ControllerTiers.refusal(m) != null) return path + " => REFUSED@" + at.name;
                    if (ControllerTiers.targetsGlobal(m)) {
                        if (at.gc) return path + " => GlobalExecutor@" + at.name;
                        m.setForwardDst(G.region, G.agent, null);      // remoteGlobalSend
                        at = G;
                        continue;
                    }
                    if (at.region.equals(m.getDstRegion()) && at.agent.equals(m.getDstAgent()) && m.getDstPlugin() == null) {
                        return path + " => RegionalExecutor@" + at.name;
                    }
                    return path + " => DROP(regional controller: not destined for it)@" + at.name;
                case REMOTE_REGIONAL_CONTROLLER:
                    m.setForwardDst(at.regional.region, at.regional.agent, null);
                    at = at.regional;
                    continue;
                default: {                                                // every relay: the broker network
                    Node next = owner(m.getDstRegion(), m.getDstAgent());
                    if (next == null) return path + " => DROP(no consumer for " + m.getForwardDst() + ")";
                    at = next;
                }
            }
        }
        return String.join(">", trail) + " => LOOP";
    }

    /** A wsapi-built message from {@code origin}'s wsapi (pluginbuilder getMsgEvent semantics). */
    static MsgEvent wsapi(Node origin, String cls, Node dst) {
        switch (cls) {
            case "regional": return new MsgEvent(MsgEvent.Type.CONFIG, origin.region, origin.agent, WSAPI, dst.region, dst.agent, null, true, false);
            case "global":   return new MsgEvent(MsgEvent.Type.CONFIG, origin.region, origin.agent, WSAPI, dst.region, dst.agent, null, true, true);
            case "agent":    return new MsgEvent(MsgEvent.Type.CONFIG, origin.region, origin.agent, WSAPI, dst.region, dst.agent, null, false, false);
            case "plugin":   return new MsgEvent(MsgEvent.Type.CONFIG, origin.region, origin.agent, WSAPI, dst.region, dst.agent, "plugin/7", false, false);
            // a regional-controller message built by a CONTROLLER (null plugin id), e.g. an executor's own send
            case "regional(ctrl)": return new MsgEvent(MsgEvent.Type.CONFIG, origin.region, origin.agent, null, dst.region, dst.agent, null, true, false);
            default: throw new IllegalArgumentException(cls);
        }
    }

    static Map<String, String> matrix() {
        Map<String, String> out = new LinkedHashMap<>();
        Object[][] cases = {
                // origin, destination label, destination
                {G, "global(self)", G}, {G, "remote region ctrl", R1}, {G, "agent under remote region", A1}, {G, "agent under global", AG},
                {R1, "global", G}, {R1, "own region ctrl(self)", R1}, {R1, "remote region ctrl", R2}, {R1, "agent under remote region", A2},
                {A1, "global", G}, {A1, "own region ctrl", R1}, {A1, "remote region ctrl", R2}, {A1, "agent under remote region", A2},
        };
        for (String cls : new String[]{"regional", "regional(ctrl)", "global", "agent", "plugin"}) {
            for (Object[] c : cases) {
                Node origin = (Node) c[0];
                out.put(cls + " | " + origin.name + (cls.endsWith("(ctrl)") ? " controller" : " wsapi") + " -> " + c[1], trace(origin, wsapi(origin, cls, (Node) c[2])));
            }
        }
        return out;
    }

    /**
     * The map; "191d895:" marks the cells 0e06721 changed, "1d466ba:" the cells the controller-tier step changed
     * (every global controller message now runs on the real global, whatever node it is addressed to; before, one
     * addressed to another region or an agent ran on that node's AgentExecutor).
     */
    static final Map<String, String> EXPECTED = new LinkedHashMap<>();
    static {
        EXPECTED.put("regional | G wsapi -> global(self)",
                "G(29679) => RegionalExecutor@G");
        EXPECTED.put("regional | G wsapi -> remote region ctrl",
                "G(29359)>R1(20847) => RegionalExecutor@R1"); // 191d895: AgentExecutor
        EXPECTED.put("regional | G wsapi -> agent under remote region",
                "G(29359)>A1(16751) => AgentExecutor@A1");
        EXPECTED.put("regional | G wsapi -> agent under global",
                "G(29423)>AG(16879) => AgentExecutor@AG");
        EXPECTED.put("regional | R1 wsapi -> global",
                "R1(21167)>G(29039) => RegionalExecutor@G");
        EXPECTED.put("regional | R1 wsapi -> own region ctrl(self)",
                "R1(21487) => RegionalExecutor@R1");
        EXPECTED.put("regional | R1 wsapi -> remote region ctrl",
                "R1(21167)>R2(20847) => RegionalExecutor@R2"); // 191d895: AgentExecutor
        EXPECTED.put("regional | R1 wsapi -> agent under remote region",
                "R1(21167)>A2(16751) => AgentExecutor@A2");
        EXPECTED.put("regional | A1 wsapi -> global",
                "A1(17071)>G(29039) => RegionalExecutor@G");
        EXPECTED.put("regional | A1 wsapi -> own region ctrl",
                "A1(17135)>R1(20975) => RegionalExecutor@R1");
        EXPECTED.put("regional | A1 wsapi -> remote region ctrl",
                "A1(17071)>R2(20847) => RegionalExecutor@R2"); // 191d895: AgentExecutor
        EXPECTED.put("regional | A1 wsapi -> agent under remote region",
                "A1(17071)>A2(16751) => AgentExecutor@A2");
        EXPECTED.put("regional(ctrl) | G controller -> global(self)",
                "G(29647) => RegionalExecutor@G");
        EXPECTED.put("regional(ctrl) | G controller -> remote region ctrl",
                "G(29327)>R1(20815) => RegionalExecutor@R1"); // 191d895: AgentExecutor
        EXPECTED.put("regional(ctrl) | G controller -> agent under remote region",
                "G(29327)>A1(16719) => AgentExecutor@A1");
        EXPECTED.put("regional(ctrl) | G controller -> agent under global",
                "G(29391)>AG(16847) => AgentExecutor@AG");
        EXPECTED.put("regional(ctrl) | R1 controller -> global",
                "R1(21135)>G(29007) => RegionalExecutor@G");
        EXPECTED.put("regional(ctrl) | R1 controller -> own region ctrl(self)",
                "R1(21455) => RegionalExecutor@R1");
        EXPECTED.put("regional(ctrl) | R1 controller -> remote region ctrl",
                "R1(21135)>R2(20815) => RegionalExecutor@R2"); // 191d895: AgentExecutor
        EXPECTED.put("regional(ctrl) | R1 controller -> agent under remote region",
                "R1(21135)>A2(16719) => AgentExecutor@A2");
        EXPECTED.put("regional(ctrl) | A1 controller -> global",
                "A1(17039)>G(29007) => RegionalExecutor@G");
        EXPECTED.put("regional(ctrl) | A1 controller -> own region ctrl",
                "A1(17103)>R1(20943) => RegionalExecutor@R1");
        EXPECTED.put("regional(ctrl) | A1 controller -> remote region ctrl",
                "A1(17039)>R2(20815) => RegionalExecutor@R2"); // 191d895: AgentExecutor
        EXPECTED.put("regional(ctrl) | A1 controller -> agent under remote region",
                "A1(17039)>A2(16719) => AgentExecutor@A2");
        EXPECTED.put("global | G wsapi -> global(self)",
                "G(tier) => GlobalExecutor@G"); // 1d466ba: G(29679) => GlobalExecutor@G
        EXPECTED.put("global | G wsapi -> remote region ctrl",
                "G(tier) => GlobalExecutor@G"); // 1d466ba: G(29359)>R1(20847) => AgentExecutor@R1
        EXPECTED.put("global | G wsapi -> agent under remote region",
                "G(tier) => GlobalExecutor@G"); // 1d466ba: G(29359)>A1(16751) => AgentExecutor@A1
        EXPECTED.put("global | G wsapi -> agent under global",
                "G(tier) => GlobalExecutor@G"); // 1d466ba: G(29423)>AG(16879) => AgentExecutor@AG
        EXPECTED.put("global | R1 wsapi -> global",
                "R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: R1(21167)>G(29039) => GlobalExecutor@G
        EXPECTED.put("global | R1 wsapi -> own region ctrl(self)",
                "R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: R1(21487)>G(29039) => GlobalExecutor@G
        EXPECTED.put("global | R1 wsapi -> remote region ctrl",
                "R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: R1(21167)>R2(20847) => AgentExecutor@R2
        EXPECTED.put("global | R1 wsapi -> agent under remote region",
                "R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: R1(21167)>A2(16751) => AgentExecutor@A2
        EXPECTED.put("global | A1 wsapi -> global",
                "A1(tier)>R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: A1(17071)>G(29039) => GlobalExecutor@G
        EXPECTED.put("global | A1 wsapi -> own region ctrl",
                "A1(tier)>R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: A1(17135)>R1(20975)>G(29039) => GlobalExecutor@G
        EXPECTED.put("global | A1 wsapi -> remote region ctrl",
                "A1(tier)>R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: A1(17071)>R2(20847) => AgentExecutor@R2
        EXPECTED.put("global | A1 wsapi -> agent under remote region",
                "A1(tier)>R1(tier)>G(tier) => GlobalExecutor@G"); // 1d466ba: A1(17071)>A2(16751) => AgentExecutor@A2
        EXPECTED.put("agent | G wsapi -> global(self)",
                "G(13295) => AgentExecutor@G");
        EXPECTED.put("agent | G wsapi -> remote region ctrl",
                "G(12975)>R1(4463) => RegionalExecutor@R1");
        EXPECTED.put("agent | G wsapi -> agent under remote region",
                "G(12975)>A1(367) => AgentExecutor@A1");
        EXPECTED.put("agent | G wsapi -> agent under global",
                "G(13039)>AG(495) => AgentExecutor@AG");
        EXPECTED.put("agent | R1 wsapi -> global",
                "R1(4783)>G(12655) => AgentExecutor@G");
        EXPECTED.put("agent | R1 wsapi -> own region ctrl(self)",
                "R1(5103) => AgentExecutor@R1");
        EXPECTED.put("agent | R1 wsapi -> remote region ctrl",
                "R1(4783)>R2(4463) => RegionalExecutor@R2");
        EXPECTED.put("agent | R1 wsapi -> agent under remote region",
                "R1(4783)>A2(367) => AgentExecutor@A2");
        EXPECTED.put("agent | A1 wsapi -> global",
                "A1(687)>G(12655) => AgentExecutor@G");
        EXPECTED.put("agent | A1 wsapi -> own region ctrl",
                "A1(751)>R1(4591) => AgentExecutor@R1");
        EXPECTED.put("agent | A1 wsapi -> remote region ctrl",
                "A1(687)>R2(4463) => RegionalExecutor@R2");
        EXPECTED.put("agent | A1 wsapi -> agent under remote region",
                "A1(687)>A2(367) => AgentExecutor@A2");
        EXPECTED.put("plugin | G wsapi -> global(self)",
                "G(13311) => plugin/7@G");
        EXPECTED.put("plugin | G wsapi -> remote region ctrl",
                "G(12991)>R1(4479) => plugin/7@R1");
        EXPECTED.put("plugin | G wsapi -> agent under remote region",
                "G(12991)>A1(383) => plugin/7@A1");
        EXPECTED.put("plugin | G wsapi -> agent under global",
                "G(13055)>AG(511) => plugin/7@AG");
        EXPECTED.put("plugin | R1 wsapi -> global",
                "R1(4799)>G(12671) => plugin/7@G");
        EXPECTED.put("plugin | R1 wsapi -> own region ctrl(self)",
                "R1(5119) => plugin/7@R1");
        EXPECTED.put("plugin | R1 wsapi -> remote region ctrl",
                "R1(4799)>R2(4479) => plugin/7@R2");
        EXPECTED.put("plugin | R1 wsapi -> agent under remote region",
                "R1(4799)>A2(383) => plugin/7@A2");
        EXPECTED.put("plugin | A1 wsapi -> global",
                "A1(703)>G(12671) => plugin/7@G");
        EXPECTED.put("plugin | A1 wsapi -> own region ctrl",
                "A1(767)>R1(4607) => plugin/7@R1");
        EXPECTED.put("plugin | A1 wsapi -> remote region ctrl",
                "A1(703)>R2(4479) => plugin/7@R2");
        EXPECTED.put("plugin | A1 wsapi -> agent under remote region",
                "A1(703)>A2(383) => plugin/7@A2");
    }

    @Test
    void aRegionalRpcAtTheGlobalForARemoteRegionRunsThereAndItsReplyReturnsToTheWsapi() {
        MsgEvent req = wsapi(G, "regional", R1);
        req.setParam("action", "rpipelinesubmit");
        assertEquals("G(29359)>R1(20847) => RegionalExecutor@R1", trace(G, req));
        req.setReturn();                                     // sendRegionalMsg: reply from R1's controller
        assertEquals("R1(21151)>G(29023) => plugin/1@G", trace(R1, req));
    }

    @Test
    void theRoutingMapIsPinned() {
        Map<String, String> m = matrix();
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> sb.append(String.format("%-60s %s%n", k, v)));
        System.out.println("RoutingMatrixTest map:\n" + sb);
        assertEquals(EXPECTED, m);
    }

    // --- functions: the tier of the ACTION decides (ControllerTiers), the address only picks the node ---------

    /** The node whose controller serves a REGIONAL function addressed to {@code dst}: the regional controller of its region. */
    static Node regionalControllerOf(Node dst) {
        return dst.regional;
    }

    static MsgEvent controllerMsg(Node origin, boolean global, String action, Node dst) {
        MsgEvent m = new MsgEvent(action.equals("agent_enable") || action.equals("rpipelinesubmit")
                ? MsgEvent.Type.CONFIG : MsgEvent.Type.EXEC,
                origin.region, origin.agent, WSAPI, dst.region, dst.agent, null, true, global);
        m.setParam("action", action);
        return m;
    }

    @Test
    void everyControllerFunctionRunsOnTheTierThatServesIt() {
        Node[] origins = {G, R1, A1, AG};
        Node[] dsts = {G, R1, R2, AG, A1, A2};
        Map<String, String> out = new LinkedHashMap<>();
        StringBuilder bad = new StringBuilder();
        for (String action : new String[]{"listplugins", "gpipelinesubmit", "agent_enable", "rpipelinesubmit", "getmetricinventory", "listagents"}) {
            for (boolean global : new boolean[]{true, false}) {
                for (Node origin : origins) {
                    for (Node dst : dsts) {
                        MsgEvent m = controllerMsg(origin, global, action, dst);
                        if (action.equals("gpipelinesubmit")) m.setMsgType(MsgEvent.Type.CONFIG);
                        ControllerTiers.Tier tier = ControllerTiers.tierOf(m);
                        String got = trace(origin, m);
                        String want;
                        if (tier == ControllerTiers.Tier.GLOBAL) {
                            want = "=> GlobalExecutor@G";                                  // either flag, any address
                        } else if (tier == ControllerTiers.Tier.REGIONAL) {
                            want = global ? "=> REFUSED@" + origin.name                    // refused at the first hop
                                    : "=> RegionalExecutor@" + regionalControllerOf(dst).name;
                        } else {                                                           // EITHER: the flag picks the tier
                            // regional flag: the addressed controller; a plain agent answers for itself, except
                            // that an agent's own (default-addressed) controller message means its region's controller
                            want = global ? "=> GlobalExecutor@G"
                                    : dst.rc ? "=> RegionalExecutor@" + dst.name
                                    : dst == origin ? "=> RegionalExecutor@" + dst.regional.name
                                    : "=> AgentExecutor@" + dst.name;
                        }
                        String key = action + (global ? " [global]" : " [regional]") + " | " + origin.name + " -> " + dst.name;
                        out.put(key, got);
                        if (!got.endsWith(want)) bad.append(key).append(": ").append(got).append(" (want ").append(want).append(")\n");
                    }
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        out.forEach((k, v) -> sb.append(String.format("%-55s %s%n", k, v)));
        System.out.println("RoutingMatrixTest functions:\n" + sb);
        assertEquals("", bad.toString());
        assertEquals(6 * 2 * 4 * 6, out.size());
    }

    @Test
    void aGlobalFunctionAddressedToARemoteRegionRunsOnTheGlobalAndItsReplyReturnsToTheWsapi() {
        // global_controller_msgevent(listplugins, region_id=r2, agent_id=rc2) at R1's wsapi: the owner's question
        MsgEvent req = controllerMsg(R1, true, "listplugins", R2);
        assertEquals("R1(tier)>G(tier) => GlobalExecutor@G", trace(R1, req));
        assertEquals("rg", req.getDstRegion());                // re-addressed to the global, which answers it
        req.setReturn();                                     // the global's reply: back to the caller's wsapi
        String back = trace(G, req);
        assertTrue(back.startsWith("G(") && back.endsWith(">R1(" + MsgRouter.routePath(req, true, false, "r1", "rc1", null)
                + ") => plugin/1@R1"), back);
        assertTrue(!back.contains("tier"), "a reply is never re-classified: " + back);
    }

    @Test
    void aRegionalFunctionAddressedToAnAgentRunsOnItsRegionsController() {
        // regional_controller_msgevent(agent_enable) addressed to a plain agent: that agent has no regional
        // controller; the function runs on the controller of the region it was addressed to
        assertEquals("G(29423)>AG(tier)>G(29679) => RegionalExecutor@G", trace(G, controllerMsg(G, false, "agent_enable", AG)));
        assertTrue(trace(G, controllerMsg(G, false, "agent_enable", A1)).endsWith("A1(tier)>R1(20847) => RegionalExecutor@R1"));
    }

    @Test
    void aRegionalFunctionWithTheGlobalFlagIsRefusedAtOnce() {
        assertEquals("R1(tier) => REFUSED@R1", trace(R1, controllerMsg(R1, true, "agent_enable", R1)));
        assertEquals("G(tier) => REFUSED@G", trace(G, controllerMsg(G, true, "rpipelinesubmit", G)));
    }
}
