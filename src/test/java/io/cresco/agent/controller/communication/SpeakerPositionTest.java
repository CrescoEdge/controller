package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.ActionAuthorizer.Position;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.cresco.library.security.ActionAuthorizer.Position.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a speaker stands, seen from an agent, a region controller and the global: first as the rule alone (a node that
 * knows exactly what the test says it knows), then on real engines with the trust data message signing gave them -- the
 * global an agent's bundle names, the regions that joined a global.
 */
class SpeakerPositionTest {

    // ------------------------------------------------------------------ the rule

    /** A node that knows what it is told. */
    static class Knows implements SpeakerPosition.Node {
        String tenant = "t1", region, agent, parentRegion, parentAgent;
        boolean regionController, global;
        /** region/agent the parent's bundle names as the global (an agent). */
        String globalAbove;
        /** Regions whose controller joined (the global), and controllers known to have joined before they presented themselves. */
        final Set<String> joinedRegions = new HashSet<>(), joinedControllers = new HashSet<>();
        int standingRead;

        Knows(String region, String agent) { this.region = region; this.agent = agent; }

        @Override public String tenant() { return tenant; }
        @Override public SpeakerPosition.Standing standing() {
            standingRead++;
            return new SpeakerPosition.Standing(region, agent, regionController, global, parentRegion, parentAgent);
        }
        @Override public boolean globalAbove(String r, String a, String pr, String pa) {
            return pr.equals(parentRegion) && pa.equals(parentAgent) && (r + "/" + a).equals(globalAbove);
        }
        @Override public boolean ofJoinedRegion(String t, String r, String a) {
            return joinedRegions.contains(r) || joinedControllers.contains(t + "/" + r + "/" + a);
        }
    }

    private static Knows agent() {
        Knows n = new Knows("rgA", "a1");
        n.parentRegion = "rgA";
        n.parentAgent = "rc";
        n.globalAbove = "rgG/gc";
        return n;
    }

    private static Knows region() {
        Knows n = new Knows("rgA", "rc");
        n.regionController = true;
        n.parentRegion = "rgG";
        n.parentAgent = "gc";
        return n;
    }

    private static Knows global() {
        Knows n = new Knows("rgG", "gc");
        n.regionController = true;
        n.global = true;
        n.joinedRegions.add("rgA");
        n.joinedRegions.add("rgB");
        return n;
    }

    private static Position at(Knows n, String region, String agent) { return SpeakerPosition.of("t1", region, agent, n); }

    @Test
    void seenFromAnAgent() {
        Knows n = agent();
        assertEquals(SELF, at(n, "rgA", "a1"), "the agent itself");
        assertEquals(ABOVE, at(n, "rgA", "rc"), "its region controller");
        assertEquals(ABOVE, at(n, "rgG", "gc"), "the global above its region, as its parent's bundle names it");
        assertEquals(ELSEWHERE, at(n, "rgA", "a2"), "a sibling agent");
        assertEquals(ELSEWHERE, at(n, "rgB", "rc2"), "another region's controller");
        assertEquals(ELSEWHERE, at(n, "rgB", "b1"), "another region's agent");
        assertEquals(ELSEWHERE, at(n, "rgG", "ga"), "an agent of the global's own region");
        assertEquals(ELSEWHERE, at(n, "rgA", "gc"), "the global's agent name in this region");
        assertEquals(ELSEWHERE, at(n, "rgG", "rc"), "the parent's agent name in the global's region");
        for (Position p : new Position[]{at(n, "rgA", "a2"), at(n, "rgB", "b1"), at(n, "rgA", "rc"), at(n, "rgG", "gc")}) {
            assertNotEquals(BELOW, p, "nothing is below an agent");
        }
    }

    @Test
    void anAgentKnowsTheGlobalOnlyOnItsPresentParentsWord() {
        Knows n = agent();
        n.globalAbove = null;
        assertEquals(ELSEWHERE, at(n, "rgG", "gc"), "a bundle that names no global (a region that joined none)");
        n.globalAbove = "rgG/gc";
        n.parentRegion = null;
        n.parentAgent = null;
        assertEquals(ELSEWHERE, at(n, "rgG", "gc"), "an agent with no parent has nobody above it");
        assertEquals(ELSEWHERE, at(n, "rgA", "rc"), "its former region controller");
        assertEquals(SELF, at(n, "rgA", "a1"));
        // an agent of the global's own region: its parent is the global
        Knows ga = new Knows("rgG", "ga");
        ga.parentRegion = "rgG";
        ga.parentAgent = "gc";
        ga.globalAbove = "rgG/gc";
        assertEquals(ABOVE, at(ga, "rgG", "gc"));
        assertEquals(ELSEWHERE, at(ga, "rgA", "rc"), "a region controller is not above an agent of the global's region");
    }

    @Test
    void seenFromARegionController() {
        Knows n = region();
        assertEquals(SELF, at(n, "rgA", "rc"));
        assertEquals(ABOVE, at(n, "rgG", "gc"), "the global it joined");
        assertEquals(BELOW, at(n, "rgA", "a1"), "an agent of its region");
        assertEquals(BELOW, at(n, "rgA", "a2"));
        assertEquals(ELSEWHERE, at(n, "rgB", "rc2"), "another region's controller");
        assertEquals(ELSEWHERE, at(n, "rgB", "b1"), "another region's agent");
        assertEquals(ELSEWHERE, at(n, "rgG", "ga"), "an agent of the global's own region");
        n.globalAbove = "rgB/rc2";                 // a region controller takes its parent from its own join, never from a bundle
        assertEquals(ELSEWHERE, at(n, "rgB", "rc2"));
        n.parentRegion = null;
        n.parentAgent = null;
        assertEquals(ELSEWHERE, at(n, "rgG", "gc"), "the global, seen from a region that has not joined it");
        assertEquals(BELOW, at(n, "rgA", "a1"), "a region with no global still has its agents below it");
    }

    @Test
    void theNodesStateIsReadOncePerSpeakerSoAChangeMeanwhileNeverGivesAMixOfTwoStates() {
        for (Knows n : new Knows[]{agent(), region(), global()}) {
            for (String[] speaker : new String[][]{{"rgA", "a1"}, {"rgA", "rc"}, {"rgG", "gc"}, {"rgA", "a2"}, {"rgB", "b1"}}) {
                int before = n.standingRead;
                at(n, speaker[0], speaker[1]);
                assertEquals(before + 1, n.standingRead, "placing " + speaker[0] + "/" + speaker[1] + " read the node's state more than once");
            }
        }
        // a standing is whole: a parent is both names or none, and the global has none
        SpeakerPosition.Standing half = new SpeakerPosition.Standing("rgA", "a1", false, false, "rgA", null);
        assertNull(half.parentRegion);
        assertNull(half.parentAgent);
        SpeakerPosition.Standing top = new SpeakerPosition.Standing("rgG", "gc", true, true, "rgG", "gc");
        assertNull(top.parentRegion, "the global named itself as its parent");
        assertTrue(new SpeakerPosition.Standing("rgG", "gc", false, true, null, null).regionController, "the global is a region controller too");
        assertEquals(UNKNOWN, SpeakerPosition.of("t1", "rgA", "a1", new Knows("rgA", "a1") {
            @Override public SpeakerPosition.Standing standing() { return null; }
        }));
        assertEquals(UNKNOWN, SpeakerPosition.of("t1", "rgA", "a1", new Knows("rgA", "a1") {
            @Override public SpeakerPosition.Standing standing() { return SpeakerPosition.Standing.NONE; }
        }));
    }

    @Test
    void seenFromTheGlobal() {
        Knows n = global();
        n.parentRegion = "rgG";                    // a global's state names itself as its global: never taken as a parent
        n.parentAgent = "gc";
        assertEquals(SELF, at(n, "rgG", "gc"));
        assertEquals(BELOW, at(n, "rgG", "ga"), "an agent of its own region");
        assertEquals(BELOW, at(n, "rgA", "rc"), "the controller of a region that joined");
        assertEquals(BELOW, at(n, "rgA", "a1"), "an agent of a region that joined");
        assertEquals(BELOW, at(n, "rgB", "b1"));
        assertEquals(ELSEWHERE, at(n, "rgX", "rcx"), "the controller of a region that has not joined this global");
        assertEquals(ELSEWHERE, at(n, "rgX", "x1"), "a node of a region that has not joined this global");
        n.joinedControllers.add("t1/rgX/rcx");     // it joined (discovery), and has not presented itself on a ping yet
        assertEquals(BELOW, at(n, "rgX", "rcx"));
        assertEquals(ELSEWHERE, at(n, "rgX", "x1"), "its agents are below once the controller presented itself");
        for (String[] s : new String[][]{{"rgA", "rc"}, {"rgA", "a1"}, {"rgG", "ga"}, {"rgX", "x1"}}) {
            assertNotEquals(ABOVE, at(n, s[0], s[1]), "nothing is above the global");
        }
        assertNull(n.standing().parentRegion, "the global has a parent");
    }

    @Test
    void aSpeakerThatCannotBePlacedIsUnknownAndThisNodesNameUnderAnotherTenantIsNotThisNode() {
        Knows n = agent();
        assertEquals(UNKNOWN, SpeakerPosition.of("t1", null, "a1", n));
        assertEquals(UNKNOWN, SpeakerPosition.of("t1", "rgA", null, n));
        assertEquals(UNKNOWN, SpeakerPosition.of("t1", "rgA", "a1", null));
        assertEquals(UNKNOWN, SpeakerPosition.of(null, n), "no speaker noted");
        for (String malformed : new String[]{"", "rgA/a1", "t1/rgA", "t1//a1", "/rgA/a1", "t1/rgA/", "t1/rgA/a1/x", "t1"}) {
            assertEquals(UNKNOWN, SpeakerPosition.of(malformed, n), "'" + malformed + "'");
        }
        assertEquals(ABOVE, SpeakerPosition.of("t1/rgA/rc", n));
        assertEquals(SELF, SpeakerPosition.of("t1/rgA/a1", n));
        assertEquals(ELSEWHERE, SpeakerPosition.of("t2/rgA/a1", n), "this node's name under another tenant");
        assertEquals(SELF, SpeakerPosition.of(null, "rgA", "a1", n), "a name verified without a tenant is the name");
        Knows unnamed = new Knows(null, null);
        assertEquals(UNKNOWN, at(unnamed, "rgA", "a1"), "a node with no name yet places nobody");
    }

    // ------------------------------------------------------------------ real engines, real trust data

    private static AuthorityFabric f;

    @BeforeAll
    static void fabric() throws Exception { f = new AuthorityFabric("enforce"); }

    private static Position node(ControllerEngine at, ControllerEngine speaker) {
        MsgEvent m = AuthorityFabric.request(MsgEvent.Type.EXEC, "x", speaker, at, false, false);
        Caller c = AuthorityFabric.controller(speaker);
        c.stamp(m);
        Position asController = at.authority().positionOf(m, c);
        Caller p = AuthorityFabric.plugin(speaker);
        p.stamp(m);
        assertEquals(asController, at.authority().positionOf(m, p), "a plugin speaks from its own node, as its controller does");
        return asController;
    }

    @Test
    void anAgentPlacesItsRegionControllerAndTheGlobalItsBundleNames() {
        assertEquals(SELF, node(f.a1, f.a1));
        assertEquals(ABOVE, node(f.a1, f.rA), "its region controller");
        assertEquals(ABOVE, node(f.a1, f.g), "the global: its entry in the bundle the region controller signed is of kind global");
        assertEquals(ELSEWHERE, node(f.a1, f.a2), "a sibling agent");
        assertEquals(ELSEWHERE, node(f.a1, f.rB), "another region's controller (in the same bundle, of kind region)");
        assertEquals(ELSEWHERE, node(f.a1, f.b1));
        assertEquals(ELSEWHERE, node(f.a1, f.ga));
        assertEquals(ELSEWHERE, node(f.a1, f.rX));
        assertEquals(ABOVE, node(f.ga, f.g), "an agent of the global's own region");
        assertEquals(ELSEWHERE, node(f.ga, f.rA));
        // an agent of a region that joined no global: its bundle names none
        assertEquals(ABOVE, node(f.x1, f.rX));
        assertEquals(ELSEWHERE, node(f.x1, f.g), "the global, seen from an agent of a region that has not joined it");
    }

    @Test
    void anAgentThatLostItsParentNoLongerTakesTheGlobalFromThatParentsBundle() throws Exception {
        AuthorityFabric own = new AuthorityFabric("enforce");
        assertEquals(ABOVE, node(own.a1, own.g));
        TlsTestSupport.State s = (TlsTestSupport.State) own.a1.cstate;
        s.parentRegion = null;
        s.parentAgent = null;
        assertEquals(ELSEWHERE, node(own.a1, own.g), "the bundle of a parent this agent no longer has still named the global");
        assertEquals(ELSEWHERE, node(own.a1, own.rA));
        // it joins another region: the bundle it holds was signed by the parent before
        s.parentRegion = "rgB";
        s.parentAgent = "rc2";
        assertEquals(ELSEWHERE, node(own.a1, own.g), "the global was taken on the word of a controller that is not this agent's parent");
        assertEquals(ABOVE, node(own.a1, own.rB));
    }

    @Test
    void whenARegionChangesGlobalsItsAgentsPlaceTheOldGlobalElsewhereAtOnce() throws Exception {
        AuthorityFabric own = new AuthorityFabric("enforce");
        assertEquals(ABOVE, node(own.a1, own.g), "premise: the global the region joined first");
        // the region joins another global: discovery, then that global's bundle on its pongs (the old global's entry is
        // then only kept for its grace period, msg_signing_bundle_grace_ms: its messages on the way still verify)
        ControllerEngine g2 = TlsTestSupport.engine("rgH", "gc2", java.util.Map.of("broker_security_enabled", "true", "authority_direction", "enforce"));
        ((TlsTestSupport.State) g2.cstate).regional = true;
        ((TlsTestSupport.State) g2.cstate).global = true;
        assertNull(own.rA.getCertificateManager().getPeerTrust().pin("rgH", "gc2", g2.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(g2.getCertificateManager().getPeerTrust().pin("rgA", "rc", own.rA.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_CONTROLLER, false));
        g2.getCertificateManager().getPeerTrust().allowBridgePeer("rgA", "rc");
        TlsTestSupport.State region = (TlsTestSupport.State) own.rA.cstate;
        region.globalRegion = "rgH";
        region.globalAgent = "gc2";
        for (int i = 0; i < 2; i++) AuthorityFabric.ping(own.rA, g2, true);
        assertEquals(ABOVE, node(own.rA, g2), "the region's own parent is the global it joined now");
        assertEquals(ELSEWHERE, node(own.rA, own.g));
        // the agent's next pong brings the region's bundle as it stands now
        AuthorityFabric.ping(own.a1, own.rA, false);
        assertEquals(ABOVE, node(own.a1, g2), "the global the region joined now");
        assertEquals(ELSEWHERE, node(own.a1, own.g), "the global the region left was served as the global for its grace period:"
                + " the agent still takes its management word");
        // the old global's key is still served (a message of it on the way verifies), only not as the global
        assertTrue(own.rA.msgSigning().servedBundle().contains("\"role\":\"" + MsgSigning.ROLE_FORMER_GLOBAL + "\""), "the entry kept for its grace period");
        MsgEvent late = AuthorityFabric.deliver(own.g, AuthorityFabric.controller(own.g),
                AuthorityFabric.request(MsgEvent.Type.CONFIG, "killjvm", own.g, own.a1, false, false), own.a1);
        assertNotNull(late, "premise: the old global's message still verifies at the agent during the grace period");
        assertFalse(AuthorityFabric.admittedByAgentTier(own.a1, late), "the global the region left stopped the agent");
        // an agent that only holds the bundle from before (no pong yet) is covered by its parent's word once it comes
        assertEquals(ABOVE, node(own.a2, own.g), "a2 has not heard its region since: it still holds the bundle from before");
        AuthorityFabric.ping(own.a2, own.rA, false);
        assertEquals(ELSEWHERE, node(own.a2, own.g));
    }

    @Test
    void aRegionControllerPlacesItsGlobalAndItsAgents() {
        assertEquals(SELF, node(f.rA, f.rA));
        assertEquals(ABOVE, node(f.rA, f.g));
        assertEquals(BELOW, node(f.rA, f.a1));
        assertEquals(BELOW, node(f.rA, f.a2));
        assertEquals(ELSEWHERE, node(f.rA, f.rB));
        assertEquals(ELSEWHERE, node(f.rA, f.b1));
        assertEquals(ELSEWHERE, node(f.rA, f.ga));
        assertEquals(ELSEWHERE, node(f.rX, f.g), "the global, seen from a region that has not joined it");
        assertEquals(BELOW, node(f.rX, f.x1));
    }

    @Test
    void theGlobalPlacesItsOwnAgentsAndTheNodesOfTheRegionsThatJoined() {
        assertEquals(SELF, node(f.g, f.g));
        assertEquals(BELOW, node(f.g, f.ga));
        assertEquals(BELOW, node(f.g, f.rA));
        assertEquals(BELOW, node(f.g, f.rB));
        assertEquals(BELOW, node(f.g, f.a1), "an agent of a region whose controller presented itself here");
        assertEquals(BELOW, node(f.g, f.b1));
        assertEquals(ELSEWHERE, node(f.g, f.rX), "the controller of a region that has not joined");
        assertEquals(ELSEWHERE, node(f.g, f.x1), "a node of a region that has not joined");
    }

    @Test
    void aRegionThatJoinedIsBelowTheGlobalBeforeItsFirstPing() throws Exception {
        // discovery recorded the join (a controller's binding); nothing travelled on a ping yet
        ControllerEngine g2 = TlsTestSupport.engine("rgG", "gc", java.util.Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) g2.cstate).regional = true;
        ((TlsTestSupport.State) g2.cstate).global = true;
        assertEquals(ELSEWHERE, node(g2, f.rA), "a region nothing recorded");
        assertNull(g2.getCertificateManager().getPeerTrust().pin("rgA", "rc", f.rA.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_CONTROLLER, false));
        assertEquals(BELOW, node(g2, f.rA), "the controller that joined");
        assertEquals(ELSEWHERE, node(g2, f.a1), "its agents: not before their controller presented itself");
        assertFalse(g2.msgSigning().isChildRegion("rgA"));
        // an agent bound here is no controller, whatever region it names
        assertNull(g2.getCertificateManager().getPeerTrust().pin("rgB", "b1", f.b1.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_AGENT, false));
        assertEquals(ELSEWHERE, node(g2, f.b1));
    }

    // ------------------------------------------------------------------ an API user: the node that vouched for it

    /** {@code vouchedBy}'s gateway sends for an admin; the message as {@code at} holds it after its inbox verified it. */
    private static Position apiOverTheWire(ControllerEngine at, ControllerEngine vouchedBy) throws Exception {
        Caller admin = AuthorityFabric.admin();
        MsgEvent m = AuthorityFabric.deliver(vouchedBy, admin, AuthorityFabric.request(MsgEvent.Type.EXEC, "x", vouchedBy, at, false, false), at);
        assertNotNull(m, "the message of an admin vouched by " + vouchedBy.cstate.getAgentPath() + " did not verify at " + at.cstate.getAgentPath());
        assertEquals("t1/" + vouchedBy.cstate.getRegion() + "/" + vouchedBy.cstate.getAgent(), MsgSigning.speakerOf(m), "the speaker noted");
        return at.authority().positionOf(m, Caller.fromMsgEvent(m));
    }

    @Test
    void anApiUserStandsWhereTheNodeThatVouchedForItStands() throws Exception {
        // at an agent
        assertEquals(ABOVE, apiOverTheWire(f.a1, f.g), "vouched by the global");
        assertEquals(ELSEWHERE, apiOverTheWire(f.a1, f.a2), "vouched by a sibling agent");
        assertEquals(ELSEWHERE, apiOverTheWire(f.a1, f.rB), "vouched by another region's controller");
        assertEquals(ELSEWHERE, apiOverTheWire(f.a1, f.b1), "vouched by another region's agent");
        // at a region controller
        assertEquals(ELSEWHERE, apiOverTheWire(f.rA, f.rB));
        assertEquals(ELSEWHERE, apiOverTheWire(f.rA, f.b1));
        // at the global
        assertEquals(BELOW, apiOverTheWire(f.g, f.a1), "vouched by an agent of a region that joined");
        assertEquals(BELOW, apiOverTheWire(f.g, f.b1));
        // through its own gateway: the node itself (nothing crosses the wire)
        MsgEvent own = AuthorityFabric.request(MsgEvent.Type.EXEC, "x", f.a1, f.a1, false, false);
        CallerStamps.stamp(own, AuthorityFabric.admin());
        f.a1.markForSigning(own);
        f.a1.msgSigning().spokenHere(own);
        assertEquals(SELF, f.a1.authority().positionOf(own, AuthorityFabric.admin()));
    }

    @Test
    void anApiUsersMessageOverADirectLinkIsTheWordOfTheNodeTheBrokerAttests() throws Exception {
        Caller admin = AuthorityFabric.admin();
        // the region controller to its own agent, and an agent to its region controller: unsigned, attested first hand
        for (ControllerEngine[] link : new ControllerEngine[][]{{f.rA, f.a1}, {f.a1, f.rA}, {f.rA, f.g}, {f.g, f.rA}}) {
            ControllerEngine from = link[0], to = link[1];
            MsgEvent wire = AuthorityFabric.send(from, admin, AuthorityFabric.request(MsgEvent.Type.EXEC, "x", from, to, false, false));
            assertNull(wire.getParam(MsgSigning.SIG), "premise: a direct link, so unsigned");
            assertNull(AuthorityFabric.arrive(to, AuthorityFabric.wire(wire), admin, null), "an unsigned message nobody attests was accepted");
            MsgEvent m = AuthorityFabric.arrive(to, wire, admin, from);
            assertNotNull(m, "a direct-link message its broker attests was refused");
            assertEquals("t1/" + from.cstate.getRegion() + "/" + from.cstate.getAgent(), MsgSigning.speakerOf(m));
            Position expected = (from == f.rA && to == f.a1) || (from == f.g && to == f.rA) ? ABOVE : BELOW;
            assertEquals(expected, to.authority().positionOf(m, admin), from.cstate.getAgentPath() + " -> " + to.cstate.getAgentPath());
        }
    }

    @Test
    void anApiUsersMessageWithoutANotedSpeakerCannotBePlaced() {
        MsgEvent m = AuthorityFabric.request(MsgEvent.Type.EXEC, "x", f.rA, f.a1, false, false);
        Caller admin = AuthorityFabric.admin();
        admin.stamp(m);
        assertEquals(UNKNOWN, f.a1.authority().positionOf(m, admin));
        assertEquals(UNKNOWN, f.a1.authority().positionOf(m, null));
        assertEquals(UNKNOWN, f.a1.authority().positionOf(m, Caller.anonymous()));
        // the legacy stamp of a node without signing (the upgrade window) is that node's own word
        Caller legacy = Caller.of("agent:t1/rgA/a2", "t1", List.of("user"));
        assertEquals(ELSEWHERE, f.a1.authority().positionOf(m, legacy));
        assertEquals(BELOW, f.rA.authority().positionOf(m, legacy));
    }
}
