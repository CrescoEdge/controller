package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The findings of the independent review of the first revoke-by-name change (2026-11-11), one test each: what the
 * reviewer's throwaway test showed to be possible must not be. Every test here FAILS on that first change (d3a5e23): it
 * uses only what that commit and this one share, and real nodes -- a lying region is a real controller whose registry
 * holds the lie.
 */
class RevocationByNameReviewTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean global, String tenant) throws Exception {
        Path dir = Files.createTempDirectory("byname-rv-" + region);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", tenant);
        cfg.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        if (global) cfg.put("is_global", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = true;
        st.global = global;
        if (!global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static ControllerEngine node(String region, String agent, boolean global) throws Exception { return node(region, agent, global, "t1"); }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static Path fileOf(ControllerEngine e) {
        return Path.of(e.getPluginBuilder().getConfig().getStringParam("peer_trust_file"));
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    /** As discovery leaves it: each binds the other's key as a controller's; the joiner is a bridge peer of the global. */
    private static void join(ControllerEngine global, ControllerEngine region) {
        assertNull(reg(global).pin(region.cstate.getRegion(), region.cstate.getAgent(), leaf(region), PeerTrustRegistry.ROLE_CONTROLLER, false));
        reg(global).allowBridgePeer(region.cstate.getRegion(), region.cstate.getAgent());
        assertNull(reg(region).pin(global.cstate.getRegion(), global.cstate.getAgent(), leaf(global), PeerTrustRegistry.ROLE_CONTROLLER, true));
    }

    private static MsgEvent ping(ControllerEngine region) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, region.cstate.getRegion(), region.cstate.getAgent(), null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    private static String tenantOf(ControllerEngine e) { return CrescoNames.identityOf(leaf(e)).getTenant(); }

    /** The pong the global makes for a real ping of {@code region}'s controller, NOT delivered to it. */
    private static MsgEvent pongFor(ControllerEngine global, ControllerEngine region, boolean thisBuild) {
        MsgEvent p = ping(region);
        RevocationSync.onChildPing(region, p);
        if (!thisBuild) for (String k : new String[]{"rev_names_fresh", "rev_cap", "rev_name_report"}) p.removeParam(k);
        p.setParam("cresco_caller", "controller:" + tenantOf(region) + "/" + region.cstate.getRegion() + "/" + region.cstate.getAgent());
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
        return p;
    }

    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global) { return exchange(region, global, true); }

    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global, boolean thisBuild) {
        MsgEvent p = pongFor(global, region, thisBuild);
        if (thisBuild) {
            RevocationSync.onParentPong(region, p, "rgG", "gc");
        } else {
            MsgEvent q = ping(region);
            for (String k : new String[]{"rev_epoch", "rev_seq", "rev_ack", "rev_parent_list"}) if (p.getParam(k) != null) q.setParam(k, p.getParam(k));
            RevocationSync.onParentPong(region, q, "rgG", "gc");
        }
        reg(region).awaitRevocations();
        return p;
    }

    private static Map<String, String> requests(MsgEvent pong) {
        Map<String, String> out = new HashMap<>();
        if (pong.getParam("rev_names") == null) return out;
        JsonObject o = GSON.fromJson(pong.getCompressedParam("rev_names"), JsonObject.class);
        o.getAsJsonArray("items").forEach(i -> { String[] p = i.getAsString().split("\\|", -1); out.put(p[2], p[1]); });
        return out;
    }

    private static String[] health(ControllerEngine e) { return e.getCertificateManager().identityHealth(); }

    /** An agent (this build or an earlier one) of {@code region} pings its controller and reads the pong. */
    private static void agentExchange(ControllerEngine agent, ControllerEngine region, boolean thisBuild) {
        MsgEvent ap = new MsgEvent(MsgEvent.Type.EXEC, agent.cstate.getRegion(), agent.cstate.getAgent(), null, region.cstate.getRegion(),
                region.cstate.getAgent(), null, true, false);
        RevocationSync.onChildPing(agent, ap);
        if (!thisBuild) for (String k : new String[]{"rev_names_fresh", "rev_cap"}) ap.removeParam(k);
        RevocationSync.onPingAtParent(region, ap, false);
        RevocationSync.onParentPong(agent, ap, region.cstate.getRegion(), region.cstate.getAgent());
        reg(agent).awaitRevocations();
    }

    private static ControllerEngine agentOf(ControllerEngine region, String name) throws Exception {
        ControllerEngine a = TlsTestSupport.engine(region.cstate.getRegion(), name, Map.of("broker_security_enabled", "true"));
        assertTrue(reg(region).bind("t1", region.cstate.getRegion(), name, leaf(a).getPublicKey(), "discovery").ok);
        assertNull(reg(a).pin(region.cstate.getRegion(), region.cstate.getAgent(), leaf(region), PeerTrustRegistry.ROLE_CONTROLLER, true));
        return a;
    }

    // ------------------------------------------------------------------ a region's word revokes nothing outside it

    /**
     * Review F1 (critical). A lying region names the key of ANOTHER region's agent -- bound only at that agent's own
     * controller, the normal case -- as the key of its revoked name. Nobody refuses that key for it: not the global, not
     * another region, not that region's agents (each asks isRevokedFor(key, region) of a signer).
     */
    @Test
    void aLyingRegionHasNoOtherRegionsAgentRefusedAnywhere() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), liar = node("rgB", "rc", false), regionC = node("rgC", "rc", false),
                regionD = node("rgD", "rc", false);
        join(global, liar);
        join(global, regionC);
        join(global, regionD);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(liar), atC = reg(regionC), atD = reg(regionD);
        KeyPair victim = TlsTestSupport.rsa();
        assertTrue(atC.bind("t1", "rgC", "c1", victim.getPublic(), "discovery").ok);
        // the lie: at the lying controller, "rgB/b1" is bound to the victim's key -- what it then revokes and reports
        assertTrue(atB.bind("t1", "rgB", "b1", victim.getPublic(), "discovery").ok);
        Thread.sleep(40);
        String vk = fp(victim.getPublic());
        ControllerEngine d1 = agentOf(regionD, "d1");
        exchange(regionC, global);
        exchange(regionD, global);

        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(liar, global);
        exchange(liar, global);
        exchange(liar, global);
        assertTrue(atB.isRevokedKey(vk), "(the lying controller did not report: the test shows nothing)");
        exchange(regionC, global);
        exchange(regionD, global);
        agentExchange(d1, regionD, true);

        assertFalse(atGlobal.isRevokedFor(vk, "rgC"), "the global refuses rgC/c1's key on region rgB's word");
        assertFalse(atD.isRevokedFor(vk, "rgC"), "region rgD refuses rgC/c1's key on region rgB's word");
        assertFalse(reg(d1).isRevokedFor(vk, "rgC"), "region rgD's agents refuse rgC/c1's key on region rgB's word");
        assertFalse(atC.isRevokedFor(vk, "rgC"));
        assertTrue(atC.bind("t1", "rgC", "c1", victim.getPublic(), "discovery").ok, "the victim lost its binding at its own region");
        assertTrue(atGlobal.revocations().isEmpty() && atC.revocations().isEmpty() && atD.revocations().isEmpty(),
                "a key is on a list on a region's word");
    }

    // ------------------------------------------------------------------ one controller answers for a region

    /**
     * Review F2. A second controller bound at the global under the region's name (a registry written before the
     * one-controller-per-region rule: discovery refuses it now) is NOT the region's controller: while more than one is
     * recorded under the region nobody is asked and nobody's answer is read; the operator is told why. With the impostor
     * revoked, the region's controller is asked.
     */
    @Test
    void aSecondControllerBoundUnderTheRegionsNameIsNeitherAskedNorHeard() throws Exception {
        secondController("t1");
    }

    /** Review F2b: the same when the second controller is of ANOTHER tenant. */
    @Test
    void aSecondControllerOfAnotherTenantIsNeitherAskedNorHeard() throws Exception {
        secondController("t2");
    }

    private static void secondController(String tenant) throws Exception {
        ControllerEngine global = node("rgG", "gc", true), real = node("rgB", "rc", false), second = node("rgB", "zz", false, tenant);
        join(global, real);
        assertEquals(tenant, tenantOf(second));
        join(global, second);                                                          // (pin: what a registry of before the rule holds)
        PeerTrustRegistry atGlobal = reg(global), atB = reg(real);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        for (int i = 0; i < 3; i++) {
            assertTrue(requests(exchange(second, global)).isEmpty(), "the second controller was sent the region's request");
            assertTrue(requests(exchange(real, global)).isEmpty(), "a request was sent while two controllers are recorded under the region");
        }
        String[] h = health(global);
        // two controllers recorded for one region are CRITICAL by the one-controller-per-region rule; the outstanding
        // revocation is said with it
        assertEquals("CRITICAL", h[0], "the revocation is reported as done: " + h[1]);
        assertTrue(h[1].contains("CONTROLLERS are recorded here for region rgB"), "the one-controller rule's conflict is not said: " + h[1]);
        assertTrue(h[1].contains("rgB/b1 (not answered") && h[1].contains("rgB/rc") && h[1].contains("rgB/zz"), "the operator is not told why nobody is asked: " + h[1]);
        assertTrue(atGlobal.revocations().isEmpty(), "a key was revoked at the global");
        assertFalse(atB.isRevokedKey(fp(b1.getPublic())));
        // the operator revokes the name that is not the region's controller: the remaining one is asked, and answers
        assertNull(atGlobal.revoke("rgB/zz", "operator"));
        atGlobal.awaitRevocations();
        assertNotNull(requests(exchange(real, global)).get("rgB/b1"), "the region's one controller was not asked");
        exchange(real, global);
        assertTrue(atB.isRevokedKey(fp(b1.getPublic())) && atB.isTombstoned("rgB", "b1"), "the region's controller did not apply the request");
        assertEquals("OK", health(global)[0], health(global)[1]);
    }

    /**
     * A node has ONE global (the rule of the discovery that records it): where a region's registry records its global, a
     * request signed by another controller -- even one the region's state takes as its parent -- is not taken.
     */
    @Test
    void onlyTheGlobalARegionRecordsAsksIt() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), other = node("rgH", "gh", true), region = node("rgB", "rc", false);
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = "rgG"; j.agent = "gc"; j.chain = global.getCertificateManager().getPublicCertificate();
        j.role = PeerTrustRegistry.ROLE_CONTROLLER; j.proved = true; j.client = true; j.global = true;
        assertTrue(reg(region).join(j).ok, "the region did not record its global");
        assertEquals("rgG/gc", reg(region).globalRecorded());
        join(other, region);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(reg(region).bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        assertNull(reg(other).revoke("rgB/b1", "operator"));
        MsgEvent p = pongFor(other, region, true);
        assertNotNull(requests(p).get("rgB/b1"), "(the other global did not ask: the test shows nothing)");
        RevocationSync.onParentPong(region, p, "rgH", "gh");
        reg(region).awaitRevocations();
        assertFalse(reg(region).isRevokedKey(fp(b1.getPublic())) || reg(region).isTombstoned("rgB", "b1"), "a controller that is not the region's global revoked one of its names");
        assertTrue(String.valueOf(RevocationSync.lastWarning(region)).contains("global is recorded here as rgG/gc"), String.valueOf(RevocationSync.lastWarning(region)));
    }

    // ------------------------------------------------------------------ the operator's own word reaches everybody

    /**
     * Review F3. The fingerprint the operator revokes at the global after a region reported it is the global's own word,
     * for all: it reaches an earlier-build agent below another region like any revocation (nothing marks or withholds it).
     */
    @Test
    void theOperatorsOwnRevocationOfAReportedKeyReachesAnEarlierBuildBelowAnotherRegion() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB), atC = reg(regionC);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        String k = fp(b1.getPublic());
        ControllerEngine oldAgent = agentOf(regionC, "c5");

        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global);
        exchange(regionB, global);
        exchange(regionC, global);
        agentExchange(oldAgent, regionC, false);
        assertTrue(atB.isRevokedKey(k), "(the region did not revoke the key: the test shows nothing)");

        assertNull(atGlobal.revoke(k, "operator"));                                 // the operator's own word, for all
        exchange(regionC, global);
        exchange(regionC, global);
        agentExchange(oldAgent, regionC, false);
        agentExchange(oldAgent, regionC, false);
        assertTrue(reg(oldAgent).isRevokedKey(k), "the earlier-build agent did not get the operator's own revocation");
        List<PeerTrustRegistry.Revocation> all = atC.revocationsAfter(0, 10, false);
        assertEquals(1, all.size());
        assertEquals(4, PeerTrustRegistry.ListEntry.of(all.get(0)).format().split("\\|", -1).length, "the entry at rgC carries a mark");
    }

    // ------------------------------------------------------------------ a request is of its time

    /**
     * Review F4. The operator, told by the reminder to "revoke the name at that controller", does so and rebinds the
     * replaced node there; when the global's request then reaches the region (it was on an earlier build, or away), the
     * binding made AFTER the operator's act is left alone, nothing is revoked anywhere, and the global is told.
     */
    @Test
    void aRequestThatArrivesAfterALocalRebindLeavesTheNewKeyAlone() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        KeyPair b1 = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global, false);                                           // the region is on an earlier build: not asked
        // the operator does what the reminder says, at the region, and replaces the node there
        assertNull(atB.revoke("rgB/b1", "operator at the region"));
        Files.writeString(Path.of(fileOf(regionB) + ".rebind"), "rgB/b1=" + fp(fresh.getPublic()) + "\n");
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        // the region is upgraded: the request of BEFORE the rebind arrives
        assertNotNull(requests(exchange(regionB, global)).get("rgB/b1"), "(the upgraded region was not asked: the test shows nothing)");
        exchange(regionB, global);
        exchange(regionB, global);
        assertFalse(atB.isRevokedKey(fp(fresh.getPublic())), "the replaced node's new key is revoked at its region");
        assertFalse(atB.isTombstoned("rgB", "b1"), "the rebound name is reserved again at its region");
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        assertFalse(atGlobal.isRevokedKey(fp(fresh.getPublic())), "the new key is revoked at the global");
        assertTrue(requests(exchange(regionB, global)).isEmpty(), "the request is asked again after it was answered");
    }

    /**
     * Review F7. An old signed request -- held back on its way, or kept and delivered again -- that arrives after the
     * request was withdrawn (the name rebound at the global) and never applied revokes nothing: it is not fresh, the
     * region remembers every request it answered whatever the answer, and the binding is later than the operator's act.
     */
    @Test
    void anOldSignedRequestDeliveredAgainAfterItWasWithdrawnRevokesNothing() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        KeyPair b1 = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        MsgEvent old = pongFor(global, regionB, true);                              // on the wire, not delivered (held back on the path)
        assertNotNull(requests(old).get("rgB/b1"), "(the region was not asked: the test shows nothing)");
        // the node is replaced at its region, and rebound at the global to the same new key
        assertNull(atB.revoke("rgB/b1", "operator at the region"));
        Files.writeString(Path.of(fileOf(regionB) + ".rebind"), "rgB/b1=" + fp(fresh.getPublic()) + "\n");
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        Files.writeString(Path.of(fileOf(global) + ".rebind"), "rgB/b1=" + fp(fresh.getPublic()) + "\n");
        atGlobal.checkOperatorFiles();
        assertFalse(atGlobal.isTombstoned("rgB", "b1"));
        exchange(regionB, global);
        exchange(regionB, global);
        exchange(regionB, global);
        assertNull(atGlobal.nameRevocation("rgB", "b1"), "the withdrawn request is not settled at the global");
        assertFalse(atB.isRevokedKey(fp(fresh.getPublic())));
        // the old pong, at last
        RevocationSync.onParentPong(regionB, old, "rgG", "gc");
        atB.awaitRevocations();
        assertFalse(atB.isRevokedKey(fp(fresh.getPublic())), "the old request revoked the key bound after the rebind");
        assertFalse(atB.isTombstoned("rgB", "b1"), "the old request reserved the rebound name");
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
    }

    // ------------------------------------------------------------------ bounds

    /**
     * Review F5. One silent region does not use up what other regions need, and a name is reserved at the global whether
     * or not its region can be asked; a rebind its region never confirms stops counting after a day, and says so.
     */
    @Test
    void oneSilentRegionCannotKeepOtherRegionsNamesFromBeingReservedAndAsked() throws Exception {
        PeerTrustRegistry g = new PeerTrustRegistry();
        g.setGlobal(true);
        for (int i = 0; i < PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX; i++) g.revoke("rgGone/n" + i, "operator");
        for (int i = 0; i < PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX; i += 97) {
            assertTrue(g.isTombstoned("rgGone", "n" + i), "rgGone/n" + i + " is not reserved at the global");
        }
        assertTrue(g.isTombstoned("rgGone", "n" + (PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX - 1)));
        assertNull(g.revoke("rgB/b1", "operator"), "a name of another region is not asked about");
        assertTrue(g.isTombstoned("rgB", "b1"), "a name of another region is not reserved at the global");

        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("byname-rv").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.global = true;
        o.revokeIdentities = List.of("rgGone/n0");
        o.rebind = List.of("rgGone/n0");
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry h = PeerTrustRegistry.open(f, o, log::add);
        assertFalse(h.isTombstoned("rgGone", "n0"));                                 // rebound by the operator ...
        assertEquals(1, h.nameRevocationsOpen());                                    // ... and its region is still to be told
        assertNull(h.nameRevocationsOutstanding());
        int before = log.size();
        h.remindNameRevocations(System.currentTimeMillis() + 25 * 3_600_000L);
        assertEquals(0, h.nameRevocationsOpen(), "a rebind its region never confirms is counted for good");
        assertTrue(log.size() > before && log.get(log.size() - 1).contains("rgGone/n0") && log.get(log.size() - 1).contains("did not confirm"),
                "the global did not say that the rebind was never confirmed: " + log.subList(before, log.size()));
    }

    // ------------------------------------------------------------------ the node's own name

    /** Review F6. The global's own name is not reserved by a revocation by name -- also when its identity is bound nowhere in its registry. */
    @Test
    void theGlobalsOwnNameIsNotReservedByARevocationByName() throws Exception {
        ControllerEngine global = node("rgG", "gc", true);
        PeerTrustRegistry atGlobal = reg(global);
        assertNotNull(atGlobal.revoke("rgG/gc", "operator"), "the global revoked its own name");
        assertFalse(atGlobal.isTombstoned("rgG", "gc"), "the global reserved its own name");
        assertNull(atGlobal.nameRefusal("rgG", "gc"));
        assertEquals("OK", health(global)[0], health(global)[1]);
    }

    // ------------------------------------------------------------------ nothing depends on a ping parameter surviving

    /**
     * Review F8. A child whose ping once arrives without what it says of itself misses nothing for good: what it holds of
     * the global's list is what the global holds, after a revocation by name and after the operator's revocation of the key.
     */
    @Test
    void aChildWhosePingOnceLostAParameterHoldsWhatTheGlobalHolds() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB), atC = reg(regionC);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        String k = fp(b1.getPublic());
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global);
        exchange(regionB, global);
        assertTrue(atB.isRevokedKey(k), "(the region did not revoke the key: the test shows nothing)");
        // one ping of rgC loses what it says of itself on the way; rgC reads the pong as this build does
        MsgEvent p = pongFor(global, regionC, false);
        RevocationSync.onParentPong(regionC, p, "rgG", "gc");
        atC.awaitRevocations();
        for (int i = 0; i < 3; i++) exchange(regionC, global);
        assertEquals(atGlobal.revocations().keySet(), atC.revocations().keySet(), "the child does not hold what the global's list holds");
        assertNull(atGlobal.revoke(k, "operator"));
        exchange(regionC, global);
        assertEquals(atGlobal.revocations().keySet(), atC.revocations().keySet());
        assertTrue(atC.isRevokedKey(k));
    }
}
