package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Trust data from the parent is there early: the first ping goes when a node becomes active under its parent (the trust
 * bundle, an agent's attestation and the parent's revocation list come on its pong), and a node can tell whether it holds
 * its parent's whole revocation list since it joined ({@link RevocationSync#parentListComplete}).
 */
class ParentTrustReadyTest {

    /** A node's controller state whose mode a test sets. */
    static final class ModeState extends ControllerStateImp {
        final String region, agent;
        volatile ControllerMode mode;
        volatile String parentRegion, parentAgent;
        ModeState(PluginBuilder pb, String region, String agent, ControllerMode mode) { super(pb, null); this.region = region; this.agent = agent; this.mode = mode; }
        @Override public ControllerMode getControllerState() { return mode; }
        @Override public boolean isActive() {
            return mode == ControllerMode.AGENT || mode == ControllerMode.REGION || mode == ControllerMode.REGION_GLOBAL || mode == ControllerMode.GLOBAL;
        }
        @Override public boolean isFailed() { return false; }
        @Override public boolean isRegionalController() { return mode.name().startsWith("REGION") || isGlobalController(); }
        @Override public boolean isGlobalController() { return mode.name().startsWith("GLOBAL"); }
        @Override public String getRegion() { return region; }
        @Override public String getAgent() { return agent; }
        @Override public String getAgentPath() { return region + "_" + agent; }
        @Override public String getRegionalRegion() { return parentRegion; }
        @Override public String getRegionalAgent() { return parentAgent; }
        @Override public String getGlobalRegion() { return parentRegion; }
        @Override public String getGlobalAgent() { return parentAgent; }
        @Override public String getGlobalControllerPath() { return parentRegion + "_" + parentAgent; }
        @Override public String getRegionalControllerPath() { return parentRegion + "_" + parentAgent; }
    }

    private static ControllerEngine node(String region, String agent, ControllerMode mode, List<MsgEvent> sent, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("trustready-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { if (sent != null) sent.add(m); }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = new ModeState(pb, region, agent, mode);
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static void joined(ControllerEngine child, ControllerEngine parent, boolean childIsController) {
        X509Certificate c = child.getCertificateManager().getPublicCertificate()[0], p = parent.getCertificateManager().getPublicCertificate()[0];
        assertNull(child.getCertificateManager().getPeerTrust().pin(parent.cstate.getRegion(), parent.cstate.getAgent(), p,
                PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(parent.getCertificateManager().getPeerTrust().pin(child.cstate.getRegion(), child.cstate.getAgent(), c,
                childIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        ((ModeState) child.cstate).parentRegion = parent.cstate.getRegion();
        ((ModeState) child.cstate).parentAgent = parent.cstate.getAgent();
    }

    /** One ping and its pong, as the health watchers and executors run them; returns the pong. */
    private static MsgEvent pingPong(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, true);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(child, p);
        RevocationSync.onPingAtParent(parent, p, childIsRegion);
        RevocationSync.onParentPong(child, p, parent.cstate.getRegion(), parent.cstate.getAgent());
        return p;
    }

    private static List<String> randomFingerprints(int n) {
        SecureRandom r = new SecureRandom();
        List<String> out = new ArrayList<>();
        byte[] b = new byte[32];
        for (int i = 0; i < n; i++) { r.nextBytes(b); out.add(java.util.HexFormat.of().formatHex(b)); }
        return out;
    }

    private static long pings(List<MsgEvent> sent) {
        return sent.stream().filter(m -> "ping".equals(m.getParam("action"))).count();
    }

    private static void await(java.util.function.BooleanSupplier ok, long ms, String what) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (!ok.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < end, what);
            Thread.sleep(20);
        }
    }

    // ------------------------------------------------------------------ the first ping

    @Test
    void anAgentPingsItsRegionAtOnceWhenItBecomesActive() throws Exception {
        List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        ControllerEngine a = node("rgA", "a1", ControllerMode.AGENT, sent, Map.of("agent_ping_interval", "60000", "agent_ping_timeout", "200"));
        ((ModeState) a.cstate).parentRegion = "rgA";
        ((ModeState) a.cstate).parentAgent = "rc";
        AgentHealthWatcher w = new AgentHealthWatcher(a);                           // created once the agent is active (ControllerSMHandler)
        try {
            await(() -> pings(sent) >= 1, 3000, "no ping within 3 s of activation: the first ping waits one interval (60 s here)");
            assertNotNull(sent.stream().filter(m -> "ping".equals(m.getParam("action"))).findFirst().get().getParam(RevocationSync.HAVE),
                    "the first ping does not ask for the region's revocation list");
        } finally {
            w.shutdown();
        }
    }

    @Test
    void aRegionPingsItsGlobalAtOnceEachTimeItJoins() throws Exception {
        List<MsgEvent> sent = new CopyOnWriteArrayList<>();
        ControllerEngine r = node("rgA", "rc", ControllerMode.REGION, sent, Map.of("region_ping_interval", "60000", "region_ping_timeout", "100",
                "region_ping_retries", "0"));
        ModeState st = (ModeState) r.cstate;
        st.parentRegion = "rgG";
        st.parentAgent = "gc";
        RegionHealthWatcher w = new RegionHealthWatcher(r);                          // a region's watcher is up before it joins a global
        try {
            Thread.sleep(600);
            assertEquals(0, pings(sent), "a region without a global pinged");
            st.mode = ControllerMode.REGION_GLOBAL;
            await(() -> pings(sent) >= 1, 3000, "no ping within 3 s of joining the global: the first ping waits one interval (60 s here)");
            st.mode = ControllerMode.REGION_GLOBAL_FAILED;                           // the global is lost, then joined again
            Thread.sleep(600);
            long before = pings(sent);
            st.mode = ControllerMode.REGION_GLOBAL;
            await(() -> pings(sent) > before, 3000, "no ping within 3 s of joining the global again");
        } finally {
            w.shutdown();
        }
    }

    // ------------------------------------------------------------------ the parent's whole list

    @Test
    void aNodeKnowsWhenItHoldsItsParentsWholeListSinceItJoined() throws Exception {
        ControllerEngine region = node("rgA", "rc", ControllerMode.REGION, null, Map.of("revocation_sync_max_entries", "16"));
        ControllerEngine agent = node("rgA", "a1", ControllerMode.AGENT_INIT, null, Map.of());
        joined(agent, region, false);
        region.getCertificateManager().getPeerTrust().mergeRevocations(randomFingerprints(40), "test", null);
        ModeState st = (ModeState) agent.cstate;
        assertFalse(RevocationSync.parentListComplete(agent), "an agent that has not joined holds its region's list");
        st.mode = ControllerMode.AGENT;
        assertFalse(RevocationSync.parentListComplete(agent));
        assertTrue(RevocationSync.onParentLink(agent, true), "the watcher's first look at an active agent is a join");
        // 40 entries, 16 per pong: whole with the third
        for (int i = 1; i <= 2; i++) {
            MsgEvent pong = pingPong(agent, region, false);
            assertEquals("40", pong.getParam(RevocationSync.SEQ));
            assertFalse(RevocationSync.parentListComplete(agent), "complete after " + i + " of 3 parts of the region's list");
        }
        pingPong(agent, region, false);
        assertTrue(RevocationSync.parentListComplete(agent), "the agent holds all 40 entries and does not know it");
        assertEquals(40, agent.getCertificateManager().getPeerTrust().revocations().size());
        // the region revokes more later: the next pong brings it; the answer stays
        region.getCertificateManager().getPeerTrust().mergeRevocations(randomFingerprints(3), "test", null);
        assertFalse(RevocationSync.onParentLink(agent, true));
        assertTrue(RevocationSync.parentListComplete(agent));
        pingPong(agent, region, false);
        assertEquals(43, agent.getCertificateManager().getPeerTrust().revocations().size());
        // the region is lost: the answer of that join stands; joined again: false until a pong of the new join shows it
        st.mode = ControllerMode.AGENT_FAILED;
        RevocationSync.onParentLink(agent, false);
        assertTrue(RevocationSync.parentListComplete(agent));
        st.mode = ControllerMode.AGENT;
        assertFalse(RevocationSync.parentListComplete(agent), "an agent that joined again holds the list before any pong of this join");
        assertTrue(RevocationSync.onParentLink(agent, true));
        assertFalse(RevocationSync.parentListComplete(agent));
        MsgEvent pong = pingPong(agent, region, false);
        assertNull(pong.getParam(RevocationSync.PARENT_LIST), "nothing new: the pong carries no list");
        assertTrue(RevocationSync.parentListComplete(agent));
    }

    @Test
    void aParentThatStatesNoPositionNeverCompletesTheList() throws Exception {
        ControllerEngine region = node("rgA", "rc", ControllerMode.REGION, null, Map.of());
        ControllerEngine agent = node("rgA", "a1", ControllerMode.AGENT, null, Map.of());
        joined(agent, region, false);
        RevocationSync.onParentLink(agent, true);
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, true);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(agent, p);
        RevocationSync.onPingAtParent(region, p, false);
        p.removeParam(RevocationSync.SEQ);                                           // a region of an earlier build
        RevocationSync.onParentPong(agent, p, "rgA", "rc");
        assertFalse(RevocationSync.parentListComplete(agent));
        pingPong(agent, region, false);                                              // an empty list, stated: whole
        assertTrue(RevocationSync.parentListComplete(agent));
    }

    @Test
    void aNodeWithoutAParentHoldsEverything() throws Exception {
        assertTrue(RevocationSync.parentListComplete(node("rgG", "gc", ControllerMode.GLOBAL, null, Map.of())));
        assertTrue(RevocationSync.parentListComplete(node("rgA", "rc", ControllerMode.REGION, null, Map.of())), "a region that joined no global");
        assertTrue(RevocationSync.parentListComplete(node("rgA", "s1", ControllerMode.STANDALONE, null, Map.of())));
        ControllerEngine joining = node("rgA", "rc", ControllerMode.REGION_GLOBAL_INIT, null, Map.of());
        assertFalse(RevocationSync.parentListComplete(joining), "a region joining its first global holds that global's list");
        ((ModeState) joining.cstate).mode = ControllerMode.REGION_GLOBAL;
        assertFalse(RevocationSync.parentListComplete(joining));
    }
}
