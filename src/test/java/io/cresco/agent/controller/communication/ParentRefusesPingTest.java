package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.agentcontroller.AgentHealthWatcher;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.health.ParentLinkHealthCheck;
import io.cresco.agent.controller.health.ParentPing;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.apache.felix.hc.api.Result;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The answer to a child's ping is a pong only when its parent's ping handler answered. The parent's refusal (401
 * unauthenticated: the ping did not verify there) is the child's own ping coming back with a status; the watchers took any
 * answer for a pong, so a refused child read its own ping as its parent's word -- the parent heard, the direct link offered
 * (its own offer, echoed) -- and looked healthy while its parent accepted nothing it sent.
 *
 * <p>Now a refusal counts for liveness only (the parent's broker answers: the link is not torn down and joined again in a
 * loop), is reported as WARN by link:parent with one WARN line per interval that names the cure, and advances nothing on
 * the trust side. A real pong clears it.
 */
class ParentRefusesPingTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional, List<MsgEvent> out) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_sessions", "false");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg, out::add);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static Caller controller(ControllerEngine e) { return Caller.controller("t1", e.cstate.getRegion(), e.cstate.getAgent()); }

    /** The ping as the agent's watcher sends it (an RPC), as it reaches the parent. */
    private static MsgEvent ping(ControllerEngine a1, ControllerEngine rc) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        p.setParam("action", "ping");
        p.setParam("desc", "agent-ping-request");
        p.setParam("is_rpc", "true");
        p.setParam("callId-rgA-a1-null", "call-1");
        a1.msgSigning().onChildPing(p);
        CallerStamps.stamp(p, controller(a1));
        a1.markForSigning(p);
        a1.msgSigning().signIfPending(p);
        return wire(p);
    }

    /** The parent's 401 to {@code ping}, as its inbox answers a message that did not verify (MsgRouter.answerUnauthenticated). */
    private static MsgEvent refusal(ControllerEngine rc, List<MsgEvent> rcOut, MsgEvent ping) {
        rcOut.clear();
        assertTrue(MsgRouter.answerUnauthenticated(rc.getPluginBuilder(), ping, "the signer's credential does not verify here (t1/rgA/a1)"));
        assertEquals(1, rcOut.size(), "an RPC request that fails its authentication is answered 401");
        MsgEvent r = wire(rcOut.get(0));
        assertEquals("401", r.getParam("status_code"));
        assertEquals("ping", r.getParam("action"), "the refusal is the ping itself, with a status");
        return r;
    }

    /** The parent's pong to {@code ping}, as its ping handler makes it (RegionalExecutor). */
    private static MsgEvent pong(ControllerEngine rc, MsgEvent ping) {
        MsgSigning.notPending(ping);
        ping.setParam("action", "pong");
        ping.setParam("remote_ts", String.valueOf(System.currentTimeMillis()));
        rc.msgSigning().onPingAtParent(ping, false);
        ping.setReturn();
        return wire(ping);
    }

    /** Is what {@code a1} sends its parent now unsigned, over the direct link? */
    private static boolean directLinkOn(ControllerEngine a1) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", "p1", "rgA", "rc", null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, Caller.plugin("t1", "rgA", "a1", "p1"));
        a1.markForSigning(m);
        a1.msgSigning().signIfPending(m);
        boolean direct = m.getParam(MsgSigning.DIRECT) != null;
        assertEquals(direct, m.getParam(MsgSigning.SIG) == null);
        return direct;
    }

    private static final class Fabric {
        final List<MsgEvent> rcOut = new ArrayList<>();
        final ControllerEngine rc, a1;
        final AgentHealthWatcher watcher;

        Fabric() throws Exception {
            rc = node("rgA", "rc", true, rcOut);
            a1 = node("rgA", "a1", false, new ArrayList<>());
            ((TlsTestSupport.State) a1.cstate).parentRegion = "rgA";
            ((TlsTestSupport.State) a1.cstate).parentAgent = "rc";
            assertNull(a1.getCertificateManager().getPeerTrust().pin("rgA", "rc", leaf(rc), PeerTrustRegistry.ROLE_CONTROLLER, true));
            assertNull(rc.getCertificateManager().getPeerTrust().pin("rgA", "a1", leaf(a1), PeerTrustRegistry.ROLE_AGENT, false));
            watcher = new AgentHealthWatcher(a1);
            watcher.shutdown();                                  // its timers: the test hands it the answers itself
            a1.setAgentHealthWatcher(watcher);
            a1.msgSigning().parentLinkReset();                   // a join: the first ping goes signed and offers no direct link
        }

        Result link() { return new ParentLinkHealthCheck(a1).execute(); }
    }

    @Test
    void aRefusalIsNotAPongTheParentIsNotHeardAndTheLinkIsNeitherHealthyNorTornDown() throws Exception {
        Fabric f = new Fabric();
        MsgSigning s = f.a1.msgSigning();
        long before = System.currentTimeMillis();

        assertEquals(ParentPing.Answer.REFUSED, ParentPing.answer(refusal(f.rc, f.rcOut, ping(f.a1, f.rc))));
        f.watcher.pingAnswered(refusal(f.rc, f.rcOut, ping(f.a1, f.rc)), System.nanoTime());

        // trust: nothing advanced
        assertFalse(s.heardParent("rgA", "rc"), "a parent that refused this agent's ping counts as heard");
        assertFalse(s.hasAttestation());
        assertFalse(directLinkOn(f.a1), "the direct link came on with a refusal");
        // liveness: the parent's broker answers -- the link is not on its way to being torn down, and it is not healthy
        assertTrue(f.watcher.getLastParentPongTs() >= before);
        Result link = f.link();
        assertEquals(Result.Status.WARN, link.getStatus(), "link:parent with a parent that refuses this node: " + link);
        assertTrue(link.toString().contains("refuses this node's pings"), link.toString());

        // it says so once per interval, however often it is refused
        assertEquals(1, f.watcher.parentPing().lines());
        for (int i = 0; i < 3; i++) f.watcher.pingAnswered(refusal(f.rc, f.rcOut, ping(f.a1, f.rc)), System.nanoTime());
        assertEquals(4, f.watcher.parentPing().refusedInARow());
        assertEquals(1, f.watcher.parentPing().lines(), "one line per refusal");
        assertFalse(s.heardParent("rgA", "rc"));
        assertEquals(Result.Status.WARN, f.link().getStatus());

        // a real pong: the parent is heard, this agent is attested, the refusal is over
        MsgEvent ping = ping(f.a1, f.rc);
        assertNotNull(ping.getParam(MsgSigning.SIG), "after a refusal the ping goes signed");
        MsgSigning.Verdict v = f.rc.msgSigning().verify(ping, controller(f.a1), false);
        assertTrue(v.ok(), v.refusal);
        MsgEvent pong = pong(f.rc, ping);
        assertEquals(ParentPing.Answer.PONG, ParentPing.answer(pong));
        f.watcher.pingAnswered(pong, System.nanoTime());
        assertTrue(s.heardParent("rgA", "rc"), "a pong does not mark the parent heard");
        assertTrue(s.hasAttestation());
        assertEquals(0, f.watcher.parentPing().refusedInARow());
        assertNull(f.watcher.parentPing().standing());
        assertEquals(Result.Status.OK, f.link().getStatus(), f.link().toString());
    }

    @Test
    void aRefusalThatEchoesThisNodesOfferDoesNotBringTheDirectLinkBackAndWhatItSendsStaysSigned() throws Exception {
        Fabric f = new Fabric();
        // joined and heard: two pongs, the second answers a ping that offered the direct link
        for (int i = 0; i < 2; i++) {
            MsgEvent ping = ping(f.a1, f.rc);
            f.watcher.pingAnswered(pong(f.rc, ping), System.nanoTime());
        }
        assertTrue(directLinkOn(f.a1), "the direct link is not on after its parent's pongs");

        // its next ping offers the direct link (sig_direct_ok) and is refused: the 401 carries that offer back
        MsgEvent ping = ping(f.a1, f.rc);
        assertEquals("1", ping.getParam(MsgSigning.P_DIRECT_OK));
        MsgEvent refusal = refusal(f.rc, f.rcOut, ping);
        assertEquals("1", refusal.getParam(MsgSigning.P_DIRECT_OK), "the refusal echoes the ping's own offer");
        f.watcher.pingAnswered(refusal, System.nanoTime());
        assertFalse(directLinkOn(f.a1), "this node took its own offer, echoed by a refusal, for its parent's");
        MsgEvent next = ping(f.a1, f.rc);
        assertNotNull(next.getParam(MsgSigning.SIG), "the ping after a refusal goes unsigned");
        assertNull(next.getParam(MsgSigning.P_DIRECT_OK), "and offers the direct link");
        assertEquals(Result.Status.WARN, f.link().getStatus());
    }

    @Test
    void theLineIsDueAtOnceThenOncePerIntervalAndAnythingElseThatComesBackIsAMiss() {
        ParentPing p = new ParentPing();
        long t = 1_000_000L;
        String first = p.refused("Regional Controller [rgA_rc]", "unauthenticated: its credential does not verify", t);
        assertNotNull(first);
        assertTrue(first.contains("rgA_rc") && first.contains("its credential does not verify") && first.contains("Cure"), first);
        assertNull(p.refused("Regional Controller [rgA_rc]", "x", t + 5_000L));
        assertNull(p.refused("Regional Controller [rgA_rc]", "x", t + ParentPing.LINE_EVERY_MS - 1));
        String second = p.refused("Regional Controller [rgA_rc]", "x", t + ParentPing.LINE_EVERY_MS);
        assertNotNull(second, "no line an interval later, while its parent still refuses it");
        assertTrue(second.contains("4 in a row"), second);
        assertNotNull(p.pong("Regional Controller [rgA_rc]", t + 70_000L));
        assertNull(p.standing());
        assertNull(p.pong("Regional Controller [rgA_rc]", t + 75_000L), "a pong after a pong is nothing to report");
        assertNotNull(p.refused("Regional Controller [rgA_rc]", "x", t + 80_000L), "a new run of refusals is reported at once");

        assertEquals(ParentPing.Answer.MISS, ParentPing.answer(null));
        MsgEvent routeError = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "rc", null, "rgA", "a1", null, true, false);
        routeError.setParam("action", "ping");
        routeError.setParam(MsgRouter.ROUTE_UNAVAILABLE, "1");
        assertEquals(ParentPing.Answer.MISS, ParentPing.answer(routeError));
        MsgEvent echo = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "rc", null, "rgA", "a1", null, true, false);
        echo.setParam("action", "ping");
        assertEquals(ParentPing.Answer.MISS, ParentPing.answer(echo), "a ping that came back without the parent's handler having answered");
    }
}
