package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.cresco.agent.controller.communication.AuthorityFabric.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Authority follows the tree, between real controller engines with {@code authority_direction=enforce}: a leaf's
 * controller holds {@code infra} for its tenant, and an API admin it vouches for holds {@code admin} -- neither manages
 * the leaf's region controller, a sibling or the global any more. Every message is stamped, signed, sent and verified as
 * in production ({@link AuthorityFabric}); what is asked is each tier's own admission, and its 403.
 */
class PeerAuthorityTest {

    /** The management actions of the agent tier: {type, action}. */
    static final String[][] DOWN = {
            {"CONFIG", "pluginadd"}, {"CONFIG", "pluginremove"}, {"CONFIG", "pluginupload"}, {"CONFIG", "pluginrepopull"},
            {"CONFIG", "controllerupdate"}, {"CONFIG", "stopcontroller"}, {"CONFIG", "restartcontroller"}, {"CONFIG", "restartframework"},
            {"CONFIG", "killjvm"}, {"CONFIG", "setloglevel"}, {"CONFIG", "setlogdp"}, {"CONFIG", "getislogdp"}, {"CONFIG", "getagentinfo"},
            {"EXEC", "getlog"}, {"EXEC", "getfileinfo"}, {"EXEC", "getfiledata"}, {"EXEC", "getbroadcastdiscovery"},
            {"EXEC", "getnetworkstate"}, {"EXEC", "getcoordinators"}};

    private static final String DOWN_403 = " is honoured only from the node itself or a controller above it";
    private static final String UP_403 = " is honoured only from the node itself or a node below it";

    private static AuthorityFabric f;

    @BeforeAll
    static void fabric() throws Exception { f = new AuthorityFabric("enforce"); }

    @BeforeEach
    void quiet() { f.clearSent(); }

    private static MsgEvent.Type type(String t) { return MsgEvent.Type.valueOf(t); }

    /** Is a direct link between the two (a node and its own parent)? Then the message travels unsigned, attested by the broker. */
    private static boolean direct(ControllerEngine from, ControllerEngine to) {
        TlsTestSupport.State a = (TlsTestSupport.State) from.cstate, b = (TlsTestSupport.State) to.cstate;
        return parentOf(a, to) || parentOf(b, from);
    }

    private static boolean parentOf(TlsTestSupport.State child, ControllerEngine parent) {
        String pr = child.regional ? child.globalRegion : child.parentRegion, pa = child.regional ? child.globalAgent : child.parentAgent;
        return parent.cstate.getRegion().equals(pr) && parent.cstate.getAgent().equals(pa);
    }

    /** {@code from} sends {@code type}/{@code action} as {@code caller} to {@code to}; the message as {@code to} holds it, verified. */
    private static MsgEvent received(ControllerEngine from, Caller caller, String type, String action, ControllerEngine to) throws Exception {
        MsgEvent wire = send(from, caller, request(type(type), action, from, to, false, false));
        assertNull(wire.getParam(MsgSigning.SPEAKER), "the speaker note left its node");
        MsgEvent m = arrive(to, wire, caller, direct(from, to) ? from : null);
        assertNotNull(m, type + " " + action + " from " + from.cstate.getAgentPath() + " did not verify at " + to.cstate.getAgentPath()
                + ": " + AuthorityFabric.lastRefusal);
        return m;
    }

    /** The one answer {@code at} sent since the last {@link #quiet}: a 403 that says from where the action is honoured. */
    private static void refused403(ControllerEngine at, String action, String how, String what) {
        List<MsgEvent> out = f.sent(at);
        assertEquals(1, out.size(), what + ": one answer");
        MsgEvent answer = out.get(0);
        assertEquals("403", answer.getParam("status_code"), what);
        assertTrue(answer.getParam("status_desc").startsWith("unauthorized: " + action + how), what + ": " + answer.getParam("status_desc"));
        out.clear();
    }

    /** Both tables a node may meet the action on: its agent tier, and (a controller) its regional tier. */
    private static void refusedOnEveryTier(ControllerEngine from, Caller caller, String type, String action, ControllerEngine to,
                                           String how) throws Exception {
        String what = type + " " + action + " as " + caller + " from " + from.cstate.getAgentPath() + " at " + to.cstate.getAgentPath();
        assertFalse(admittedByAgentTier(to, received(from, caller, type, action, to)), what + " (agent tier)");
        refused403(to, action, how, what + " (agent tier)");
        if (((TlsTestSupport.State) to.cstate).regional) {
            assertFalse(admittedByControllerTier(to, received(from, caller, type, action, to), false), what + " (regional tier)");
            refused403(to, action, how, what + " (regional tier)");
        }
    }

    private static void allowedOnEveryTier(ControllerEngine from, Caller caller, String type, String action, ControllerEngine to) throws Exception {
        String what = type + " " + action + " as " + caller + " from " + from.cstate.getAgentPath() + " at " + to.cstate.getAgentPath();
        assertTrue(admittedByAgentTier(to, received(from, caller, type, action, to)), what + " (agent tier)");
        if (((TlsTestSupport.State) to.cstate).regional) {
            assertTrue(admittedByControllerTier(to, received(from, caller, type, action, to), false), what + " (regional tier)");
        }
        assertTrue(f.sent(to).isEmpty(), what + ": an allowed call was answered by the admission");
    }

    // ------------------------------------------------------------------ a node's own controller (infra)

    @Test
    void aLeafSendingEachManagementActionToItsRegionControllerASiblingAndTheGlobalIsRefused403() throws Exception {
        for (String[] a : DOWN) {
            for (ControllerEngine to : new ControllerEngine[]{f.rA, f.a2, f.g}) {
                refusedOnEveryTier(f.a1, controller(f.a1), a[0], a[1], to, DOWN_403);
            }
        }
        // the same word, from a plugin of the leaf: its role refuses it first where a role is needed, and its place
        // refuses it where its role would do (getagentinfo admits a plugin)
        assertFalse(admittedByAgentTier(f.a2, received(f.a1, plugin(f.a1), "CONFIG", "getagentinfo", f.a2)));
        refused403(f.a2, "getagentinfo", DOWN_403, "a sibling's plugin");
        assertFalse(admittedByAgentTier(f.a2, received(f.a1, plugin(f.a1), "CONFIG", "killjvm", f.a2)));
        assertTrue(f.sent(f.a2).get(0).getParam("status_desc").startsWith("unauthorized: killjvm needs "), "a role refusal keeps its text");
    }

    @Test
    void aRegionControllerIsRefusedByAnotherRegionAndByTheGlobal() throws Exception {
        for (String[] a : DOWN) {
            refusedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.rB, DOWN_403);
            refusedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.b1, DOWN_403);
            refusedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.g, DOWN_403);
            refusedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.ga, DOWN_403);
        }
    }

    @Test
    void theRegionControllerManagesItsOwnAgentsAndTheGlobalManagesEveryNode() throws Exception {
        for (String[] a : DOWN) {
            allowedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.a1);
            allowedOnEveryTier(f.rA, controller(f.rA), a[0], a[1], f.a2);
            for (ControllerEngine to : new ControllerEngine[]{f.ga, f.rA, f.rB, f.a1, f.b1}) {
                allowedOnEveryTier(f.g, controller(f.g), a[0], a[1], to);
            }
        }
    }

    @Test
    void aNodeManagesItself() {
        // the controller to itself, a plugin to its own controller (getagentinfo admits a plugin): nothing crosses the wire
        for (ControllerEngine e : new ControllerEngine[]{f.a1, f.rA, f.g}) {
            for (String[] a : DOWN) {
                MsgEvent m = request(type(a[0]), a[1], e, e, false, false);
                e.stampSelf(m);
                assertTrue(admittedByAgentTier(e, m), a[1] + " by " + e.cstate.getAgentPath() + " itself");
            }
            MsgEvent own = request(MsgEvent.Type.CONFIG, "getagentinfo", e, e, false, false);
            CallerStamps.stamp(own, plugin(e));
            e.markForSigning(own);
            assertTrue(admittedByAgentTier(e, own), "a plugin to its own controller");
        }
    }

    @Test
    void aGlobalARegionNeverJoinedDoesNotManageIt() throws Exception {
        // its word does not even verify there: the region's nodes hold no trust data that names it
        MsgEvent wire = send(f.g, controller(f.g), request(MsgEvent.Type.CONFIG, "killjvm", f.g, f.x1, false, false));
        assertNull(arrive(f.x1, wire, controller(f.g), null));
        wire = send(f.g, controller(f.g), request(MsgEvent.Type.CONFIG, "killjvm", f.g, f.rX, false, false));
        assertNull(arrive(f.rX, wire, controller(f.g), null));
    }

    // ------------------------------------------------------------------ an API admin: the node that vouched for it

    /** An admin through {@code gateway}'s own API, for {@code gateway} itself: stamped there, never on the wire. */
    private static MsgEvent throughOwnGateway(ControllerEngine gateway, String type, String action) {
        MsgEvent m = request(type(type), action, gateway, gateway, false, false);
        CallerStamps.stamp(m, admin());
        gateway.markForSigning(m);
        gateway.msgSigning().spokenHere(m);
        return m;
    }

    @Test
    void anAdminVouchedByALeafIsHonouredAtThatLeafOnly() throws Exception {
        for (String[] a : DOWN) {
            assertTrue(admittedByAgentTier(f.a1, throughOwnGateway(f.a1, a[0], a[1])), a[1] + " at the leaf whose gateway the admin uses");
            for (ControllerEngine to : new ControllerEngine[]{f.rA, f.a2, f.g, f.b1}) {
                refusedOnEveryTier(f.a1, admin(), a[0], a[1], to, DOWN_403);
            }
        }
    }

    @Test
    void anAdminVouchedByARegionControllerIsHonouredInItsRegionOnly() throws Exception {
        for (String[] a : DOWN) {
            assertTrue(admittedByAgentTier(f.rA, throughOwnGateway(f.rA, a[0], a[1])), a[1] + " at the region controller itself");
            allowedOnEveryTier(f.rA, admin(), a[0], a[1], f.a1);
            allowedOnEveryTier(f.rA, admin(), a[0], a[1], f.a2);
            for (ControllerEngine to : new ControllerEngine[]{f.g, f.ga, f.rB, f.b1}) {
                refusedOnEveryTier(f.rA, admin(), a[0], a[1], to, DOWN_403);
            }
        }
    }

    @Test
    void anAdminVouchedByTheGlobalIsHonouredEverywhere() throws Exception {
        for (String[] a : DOWN) {
            assertTrue(admittedByAgentTier(f.g, throughOwnGateway(f.g, a[0], a[1])), a[1] + " at the global itself");
            for (ControllerEngine to : new ControllerEngine[]{f.ga, f.rA, f.a1, f.a2, f.rB, f.b1}) {
                allowedOnEveryTier(f.g, admin(), a[0], a[1], to);
            }
        }
    }

    @Test
    void aLeafCannotClaimAnotherSpeaker() throws Exception {
        Caller admin = admin();
        // over relays (signed): the note is not under the signature, so a sender can add any -- and it is removed on arrival
        for (String claimed : new String[]{"t1/rgA/rc", "t1/rgG/gc", "t1/rgA/a2"}) {
            MsgEvent wire = send(f.a1, admin, request(MsgEvent.Type.CONFIG, "killjvm", f.a1, f.a2, false, false));
            wire.setParam(MsgSigning.SPEAKER, claimed);
            MsgEvent m = arrive(f.a2, wire, admin, null);
            assertNotNull(m, "a note a sender added broke its own signature: it would then never be tested here");
            assertEquals("t1/rgA/a1", MsgSigning.speakerOf(m), "the speaker is the signer, whatever the sender wrote");
            assertFalse(admittedByAgentTier(f.a2, m), "a leaf claimed to speak as " + claimed);
            refused403(f.a2, "killjvm", DOWN_403, "claimed " + claimed);
        }
        // over the direct link to its region controller: the speaker is the node the broker attests
        MsgEvent wire = send(f.a1, admin, request(MsgEvent.Type.CONFIG, "killjvm", f.a1, f.rA, false, false));
        wire.setParam(MsgSigning.SPEAKER, "t1/rgG/gc");
        MsgEvent m = arrive(f.rA, wire, admin, f.a1);
        assertNotNull(m);
        assertEquals("t1/rgA/a1", MsgSigning.speakerOf(m));
        assertFalse(admittedByAgentTier(f.rA, m));
        refused403(f.rA, "killjvm", DOWN_403, "over the direct link");
        // as its own controller: there is no note to forge at all, the stamp is the node
        wire = send(f.a1, controller(f.a1), request(MsgEvent.Type.CONFIG, "killjvm", f.a1, f.a2, false, false));
        wire.setParam(MsgSigning.SPEAKER, "t1/rgG/gc");
        m = arrive(f.a2, wire, controller(f.a1), null);
        assertNotNull(m);
        assertNull(MsgSigning.speakerOf(m), "a sender's note stayed on a message off the wire");
        assertFalse(admittedByAgentTier(f.a2, m));
        // in the JVM: whatever a plugin writes before it hands a message to its controller is removed at the stamping point
        MsgEvent local = request(MsgEvent.Type.CONFIG, "getagentinfo", f.a1, f.a1, false, false);
        local.setParam(MsgSigning.SPEAKER, "t1/rgG/gc");
        CallerStamps.stamp(local, plugin(f.a1));
        f.a1.markForSigning(local);
        assertNull(MsgSigning.speakerOf(local));
        // and a stamp another node's connection could not declare never arrives: the signer may sign only for itself
        MsgEvent forged = request(MsgEvent.Type.CONFIG, "killjvm", f.rA, f.a2, false, false);
        CallerStamps.stamp(forged, controller(f.rA));
        f.a1.markForSigning(forged);
        f.a1.msgSigning().signIfPending(forged);
        assertNull(arrive(f.a2, wire(forged), controller(f.rA), null), "a leaf signed as its region controller");
    }

    @Test
    void theSpeakerNoteIsNeverSignedAndNeverLeavesItsNode() {
        assertFalse(MsgSigning.covered(MsgSigning.SPEAKER));
        MsgEvent m = request(MsgEvent.Type.CONFIG, "killjvm", f.g, f.a1, false, false);
        CallerStamps.stamp(m, admin());
        f.g.markForSigning(m);
        f.g.msgSigning().spokenHere(m);
        assertEquals("t1/rgG/gc", MsgSigning.speakerOf(m), "the gateway's own note");
        f.g.msgSigning().signIfPending(m);
        assertNull(MsgSigning.speakerOf(m), "the note was still on the message as it left");
        m.setParam(MsgSigning.SPEAKER, "x");
        MsgSigning.notPending(m);
        assertNull(MsgSigning.speakerOf(m));
        m.setParam(MsgSigning.SPEAKER, "x");
        MsgSigning.clear(m);
        assertNull(MsgSigning.speakerOf(m));
    }

    // ------------------------------------------------------------------ reports go up

    private static MsgEvent report(ControllerEngine from, String type, String action, ControllerEngine to, boolean global) throws Exception {
        Caller caller = controller(from);
        MsgEvent wire = send(from, caller, request(type(type), action, from, to, true, global));
        MsgEvent m = arrive(to, wire, caller, direct(from, to) ? from : null);
        assertNotNull(m, type + " " + action + " from " + from.cstate.getAgentPath() + " did not verify at " + to.cstate.getAgentPath());
        return m;
    }

    @Test
    void aReportIsHonouredFromBelowAndRefusedFromAnywhereElse() throws Exception {
        String[][] atRegion = {{"CONFIG", "agent_enable"}, {"CONFIG", "agent_disable"}, {"WATCHDOG", null}, {"KPI", null}};
        for (String[] a : atRegion) {
            String what = a[0] + " " + a[1];
            assertTrue(admittedByControllerTier(f.rA, report(f.a1, a[0], a[1], f.rA, false), false), what + " from an agent of the region");
            assertTrue(f.sent(f.rA).isEmpty(), what);
            // another region's agent, another region's controller, and the global above: none of them is below
            for (ControllerEngine from : new ControllerEngine[]{f.b1, f.rB, f.g}) {
                assertFalse(admittedByControllerTier(f.rA, report(from, a[0], a[1], f.rA, false), false),
                        what + " from " + from.cstate.getAgentPath());
                refused403(f.rA, a[1] == null ? "(no action)" : a[1], UP_403, what + " from " + from.cstate.getAgentPath());
            }
        }
        String[][] atGlobal = {{"CONFIG", "region_enable"}, {"CONFIG", "region_disable"}, {"CONFIG", "regionalimport"}, {"WATCHDOG", null}, {"KPI", null}};
        // a controller whose key this global holds and that never joined it as a controller
        ControllerEngine stranger = TlsTestSupport.engine("rgY", "rcy", Map.of("broker_security_enabled", "true", "authority_direction", "enforce"));
        ((TlsTestSupport.State) stranger.cstate).regional = true;
        assertNull(f.g.getCertificateManager().getPeerTrust().pin("rgY", "rcy", stranger.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_AGENT, false));
        for (String[] a : atGlobal) {
            String what = a[0] + " " + a[1];
            assertTrue(admittedByControllerTier(f.g, report(f.rA, a[0], a[1], f.g, true), true), what + " from a region that joined");
            assertTrue(f.sent(f.g).isEmpty(), what);
            assertFalse(admittedByControllerTier(f.g, report(stranger, a[0], a[1], f.g, true), true), what + " from a controller that never joined");
            refused403(f.g, a[1] == null ? "(no action)" : a[1], UP_403, what + " from a controller that never joined");
        }
        // a node reports to itself (the global is its own region's controller)
        MsgEvent own = request(MsgEvent.Type.KPI, null, f.g, f.g, true, false);
        f.g.stampSelf(own);
        assertTrue(admittedByControllerTier(f.g, own, false));
    }

    // ------------------------------------------------------------------ the global's own functions

    /** An agent's function of the global, carried on by its region controller: verified only where it is dispatched. */
    private static MsgEvent carriedToTheGlobal(ControllerEngine from, Caller caller, String type, String action) throws Exception {
        ControllerEngine region = from == f.b1 ? f.rB : f.rA;
        MsgEvent wire = send(from, caller, request(type(type), action, from, region, true, true));
        assertEquals("^" + region.cstate.getRegion() + "/" + region.cstate.getAgent(), wire.getParam(MsgSigning.TO), "premise: signed for the global above");
        assertTrue(region.msgSigning().relayOnly(wire), "premise: its region only carries it on");
        // at the global's inbox: addressed to the region controller, so not checked on arrival
        MsgEvent m = wire(wire);
        org.apache.activemq.command.ActiveMQTextMessage jms = new org.apache.activemq.command.ActiveMQTextMessage();
        CallerStamps.toJms(caller, jms);
        m.setParams(new java.util.HashMap<>(m.getParams()));
        CallerStamps.fromJms(m, jms, true);
        f.g.msgSigning().deferred(m);
        assertNull(MsgSigning.speakerOf(m));
        assertTrue(f.g.admitDeferred(m), "it verifies where it is dispatched");
        return m;
    }

    @Test
    void schedulingAndTheRepositoryAreManagedAtTheGlobalItself() throws Exception {
        String[] manage = {"addplugin", "removeplugin", "gpipelinesubmit", "gpipelineremove", "plugindownload", "savetorepo", "setinodestatus"};
        for (String action : manage) {
            // a leaf's controller, an admin its gateway vouches for, and its region controller's own word: all from below
            MsgEvent m = carriedToTheGlobal(f.a1, controller(f.a1), "CONFIG", action);
            assertFalse(admittedByControllerTier(f.g, m, true), action + " from a leaf's controller");
            refused403(f.g, action, DOWN_403, action + " from a leaf's controller");
            m = carriedToTheGlobal(f.a1, admin(), "CONFIG", action);
            assertEquals("t1/rgA/a1", MsgSigning.speakerOf(m), "the speaker is noted where the message is verified, also when that is late");
            assertFalse(admittedByControllerTier(f.g, m, true), action + " from an admin a leaf vouches for");
            refused403(f.g, action, DOWN_403, action + " from an admin a leaf vouches for");
            MsgEvent fromRegion = arrive(f.g, send(f.rA, controller(f.rA), request(MsgEvent.Type.CONFIG, action, f.rA, f.g, true, true)),
                    controller(f.rA), f.rA);
            assertNotNull(fromRegion);
            assertFalse(admittedByControllerTier(f.g, fromRegion, true), action + " from a region controller");
            refused403(f.g, action, DOWN_403, action + " from a region controller");
            // at the global's own gateway, and by the global itself
            MsgEvent own = request(MsgEvent.Type.CONFIG, action, f.g, f.g, true, true);
            CallerStamps.stamp(own, admin());
            f.g.markForSigning(own);
            f.g.msgSigning().spokenHere(own);
            assertTrue(admittedByControllerTier(f.g, own, true), action + " at the global's own gateway");
            MsgEvent self = request(MsgEvent.Type.CONFIG, action, f.g, f.g, true, true);
            f.g.stampSelf(self);
            assertTrue(admittedByControllerTier(f.g, self, true), action + " by the global itself");
        }
        // the registry is read from anywhere, as before
        MsgEvent read = carriedToTheGlobal(f.a1, controller(f.a1), "EXEC", "listregions");
        assertTrue(admittedByControllerTier(f.g, read, true));
        // a pipeline for a region: at the region, or from its global; not from one of its agents or another region
        assertTrue(admittedByControllerTier(f.rA, received(f.g, controller(f.g), "CONFIG", "rpipelinesubmit", f.rA), false));
        assertFalse(admittedByControllerTier(f.rA, received(f.a1, controller(f.a1), "CONFIG", "rpipelinesubmit", f.rA), false));
        refused403(f.rA, "rpipelinesubmit", DOWN_403, "from an agent of the region");
        assertFalse(admittedByControllerTier(f.rA, received(f.rB, controller(f.rB), "CONFIG", "rpipelinesubmit", f.rA), false));
    }

    // ------------------------------------------------------------------ what stays as it was

    @Test
    void whatIsOpenOrOnlyReadIsHonouredFromAnywhere() throws Exception {
        String[][] any = {{"EXEC", "ping"}, {"EXEC", "getcontrollerstatus"}, {"EXEC", "iscontrolleractive"}, {"EXEC", "getmetricinventory"},
                {"EXEC", "gethealthinventory"}, {"EXEC", "getcapabilities"}, {"EXEC", "listagents"}, {"EXEC", "indirectprobe"},
                {"CONFIG", "pluginlist"}, {"CONFIG", "pluginstatus"}, {"CONFIG", "cepadd"}, {"INFO", null}};
        for (String[] a : any) {
            for (ControllerEngine to : new ControllerEngine[]{f.rA, f.a2, f.g, f.b1}) {
                assertTrue(admittedByAgentTier(to, received(f.a1, controller(f.a1), a[0], a[1], to)),
                        a[0] + " " + a[1] + " from a leaf at " + to.cstate.getAgentPath());
            }
        }
    }

    @Test
    void theWholeInboundPathRefusesASiblingAndRunsTheParentsCall() throws Exception {
        // through the router and the controller's message processor, as the inbox hands a verified message on
        MsgEvent fromSibling = received(f.a1, controller(f.a1), "CONFIG", "getagentinfo", f.a2);
        f.a2.msgIn(fromSibling, true);
        await(() -> !f.sent(f.a2).isEmpty());
        refused403(f.a2, "getagentinfo", DOWN_403, "routed");
        MsgEvent fromParent = received(f.rA, controller(f.rA), "CONFIG", "getagentinfo", f.a2);
        f.a2.msgIn(fromParent, true);
        await(() -> !f.sent(f.a2).isEmpty());
        assertEquals(1, f.sent(f.a2).size());
        MsgEvent answer = f.sent(f.a2).get(0);
        assertNull(answer.getParam("status_code"), "the region controller's call was refused: " + answer.getParam("status_desc"));
        assertNotNull(answer.getParam("agent-data"), "the action did not run");
    }

    private static void await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long end = System.currentTimeMillis() + 10_000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(10);
    }
}
