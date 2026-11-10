package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An agent signs with the attestation its region controller issued only while that attestation is still its own. When the
 * controller comes back with another key, the agent is attested anew on its next ping; when the agent has another parent
 * (or name, or key) it signs with its certificate until its parent attests it. Before, the attestation was replaced only
 * near its end: after a controller's re-key the agent signed with the old one for up to two thirds of its life (16 h by
 * default), refused by every node past its parent.
 */
class AttestationRefreshTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static void bound(ControllerEngine agent, ControllerEngine controller) {
        assertNull(controller.getCertificateManager().getPeerTrust().pin(agent.cstate.getRegion(), agent.cstate.getAgent(), leaf(agent),
                PeerTrustRegistry.ROLE_AGENT, false));
    }

    /** One ping/pong as the health watchers and executors run it; true when the pong carried an attestation. */
    private static boolean ping(ControllerEngine child, ControllerEngine parent, boolean childNamesItsIssuer) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        if (!childNamesItsIssuer) p.removeParam(MsgSigning.P_ATT_ISS);           // an agent of a build before this
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, false);
        p.setReturn();
        MsgEvent pong = wire(p);
        boolean attested = pong.getParam(MsgSigning.P_ATT) != null;
        child.msgSigning().onParentPong(pong, parent.cstate.getRegion(), parent.cstate.getAgent());
        return attested;
    }

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    @Test
    void anAgentIsAttestedAnewWhenItsControllerComesBackWithAnotherKey() throws Exception {
        ControllerEngine rcOld = node("rgA", "rc", true), a1 = node("rgA", "a1", false), a2 = node("rgA", "a2", false);
        bound(a1, rcOld);
        assertTrue(ping(a1, rcOld, true), "the first ping is attested");
        assertFalse(ping(a1, rcOld, true), "an attestation its controller's present key signed is kept");
        String before = a1.msgSigning().credential();
        assertTrue(before.startsWith("att:"));

        // the controller comes back under its name with a new key (a rotation, a rebuild after a compromise)
        ControllerEngine rcNew = node("rgA", "rc", true);
        assertNotEquals(leaf(rcOld).getPublicKey(), leaf(rcNew).getPublicKey());
        bound(a1, rcNew);
        bound(a2, rcNew);
        // a2 joins the re-keyed controller: it holds the controller's present key
        assertNull(a2.getCertificateManager().getPeerTrust().pin("rgA", "rc", leaf(rcNew), PeerTrustRegistry.ROLE_CONTROLLER, true));
        ping(a2, rcNew, true);
        ping(a2, rcNew, true);

        assertTrue(ping(a1, rcNew, true), "the re-keyed controller did not attest an agent holding its former key's attestation");
        String after = a1.msgSigning().credential();
        assertTrue(after.startsWith("att:"));
        assertNotEquals(before, after, "the agent still signs with the attestation of the key before");
        MsgSigning.Verdict v = a2.msgSigning().verify(sent(a1, a2), plugin(a1), false);
        assertTrue(v.ok(), "a node that trusts the controller's present key refuses its agent: " + v.refusal);
        assertFalse(ping(a1, rcNew, true), "attested again although it holds the present one");
    }

    @Test
    void anAgentOfAnEarlierBuildIsJudgedByItsAttestationsEndAlone() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        bound(a1, rc);
        assertTrue(ping(a1, rc, false));
        // it names no issuer key on its pings: no attestation on every ping (one signature each) for a fleet of them
        for (int i = 0; i < 5; i++) assertFalse(ping(a1, rc, false), "attested on a ping although its attestation is far from its end");
    }

    @Test
    void anAttestationIsUsedOnlyWhileItIsTheAgentsOwn() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        TlsTestSupport.State st = (TlsTestSupport.State) a1.cstate;
        bound(a1, rc);
        st.parentRegion = "rgA";
        st.parentAgent = "rc";
        assertTrue(ping(a1, rc, true));
        assertTrue(a1.msgSigning().hasAttestation());
        assertTrue(a1.msgSigning().credential().startsWith("att:"));

        // its parent is lost (unknown until the next join): the attestation stands
        st.parentRegion = null;
        st.parentAgent = null;
        assertTrue(a1.msgSigning().credential().startsWith("att:"), "dropped while its parent is being re-joined");

        // it joins another controller: the attestation of the one before is no longer its credential
        st.parentRegion = "rgA";
        st.parentAgent = "rc9";
        assertFalse(a1.msgSigning().hasAttestation());
        assertTrue(a1.msgSigning().credential().startsWith("cert:"), "signs with another controller's attestation");
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc9", null, true, false);
        a1.msgSigning().onChildPing(p);
        assertEquals("0", p.getParam(MsgSigning.P_ATT_EXP), "its ping must say it holds no attestation, so the new parent attests it");
        assertNull(p.getParam(MsgSigning.P_ATT_ISS));

        // back with its controller: its own again
        st.parentAgent = "rc";
        assertTrue(a1.msgSigning().credential().startsWith("att:"));

        // it moves to another region (its name changes): not its own
        st.region = "rgB";
        assertTrue(a1.msgSigning().credential().startsWith("cert:"), "signs with an attestation of its former name");
    }
}
