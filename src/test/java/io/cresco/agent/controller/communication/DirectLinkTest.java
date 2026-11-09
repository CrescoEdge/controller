package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct links (MsgSigning): a message between a node and its own parent or child travels unsigned, attested first hand
 * by the broker the receiver consumes from (cresco_direct, which only that broker writes, on the sender's word cresco_origin
 * that it is the origin). The receiver alone decides, and accepts an unsigned message only so; each side offers it on the
 * pings, so a node that did not is always sent signatures.
 */
class DirectLinkTest {

    private static final Gson GSON = new Gson();

    // ------------------------------------------------------------------ the broker's attestation

    private static RegionCA ca;
    private static TlsTestSupport.Id brokerId, a1, a2;
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService s : brokers) s.stop();
    }

    private BrokerService securedBroker() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("direct").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
        return b;
    }

    private Session client(BrokerService b, TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private int rounds;

    /** a1 sends one message with {@code caller}, {@code origin} and a pre-set {@code direct}; what does a2 receive? */
    private Message send(BrokerService b, String caller, String origin, String presetDirect) throws Exception {
        String queue = "t1.direct." + (++rounds);
        Session rx = client(b, a2);
        MessageConsumer c = rx.createConsumer(rx.createQueue(queue));
        Session tx = client(b, a1);
        MessageProducer p = tx.createProducer(tx.createQueue(queue));
        TextMessage m = tx.createTextMessage("hello");
        if (caller != null) {
            m.setStringProperty("cresco_caller", caller);
            m.setStringProperty("cresco_caller_tenant", "t1");
            m.setStringProperty("cresco_caller_roles", caller.startsWith("plugin:") ? "plugin" : "infra");
        }
        if (origin != null) m.setStringProperty(CallerStamps.P_ORIGIN, origin);
        if (presetDirect != null) m.setStringProperty(CallerStamps.P_DIRECT, presetDirect);
        p.send(m);
        Message got = c.receive(5000);
        assertNotNull(got, "nothing delivered");
        return got;
    }

    @Test
    void onlyTheBrokerAttestsAndOnlyANodesOwnMessages() throws Exception {
        BrokerService b = securedBroker();
        CrescoIdentity a1id = CrescoIdentity.of("t1", "rgA", "a1", null);
        assertEquals(a1id, CallerStamps.directOf(send(b, "controller:t1/rgA/a1", "rgA_a1", null)), "a node's own message is attested");
        assertEquals(a1id, CallerStamps.directOf(send(b, "plugin:t1/rgA/a1/p1", "rgA_a1", null)), "and its plugins'");
        assertNull(CallerStamps.directOf(send(b, "controller:t1/rgA/a1", null, null)), "no origin claimed: no attestation");
        assertNull(CallerStamps.directOf(send(b, "controller:t1/rgA/a1", "rgA_a2", null)), "an origin it is not");
        assertNull(CallerStamps.directOf(send(b, "controller:t1/rgA/a1", null, "t1/rgA/rc")), "a sender's own cresco_direct survived");
        assertNull(CallerStamps.directOf(send(b, "controller:t1/rgA/rc", "rgA_rc", "t1/rgA/rc")), "a forged stamp (stripped) was attested");
        assertNull(CallerStamps.directOf(send(b, null, "rgA_a1", null)), "a sender without stamping (the window's) was attested");
    }

    // ------------------------------------------------------------------ the receiver decides

    private static ControllerEngine node(String region, String agent, boolean regional, String... extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static MsgEvent originated(ControllerEngine from, Caller caller, String toRegion, String toAgent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), null, toRegion, toAgent, null, false, false);
        m.setParam("action", "getstatus");
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return m;
    }

    @Test
    void theReceiverAcceptsAnUnsignedMessageOnlyFirstHandFromTheStampsNode() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true);
        Caller a1c = Caller.controller("t1", "rgA", "a1");
        MsgEvent unsigned = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, false, false);
        unsigned.setParam("action", "getstatus");
        CrescoIdentity a1id = CrescoIdentity.of("t1", "rgA", "a1", null), a2id = CrescoIdentity.of("t1", "rgA", "a2", null);
        assertTrue(rc.msgSigning().verify(wire(unsigned), a1c, false, a1id).ok(), "first hand from the stamp's own node");
        assertFalse(rc.msgSigning().verify(wire(unsigned), a1c, false, null).ok(), "unsigned and not first hand");
        assertFalse(rc.msgSigning().verify(wire(unsigned), a1c, false, a2id).ok(), "delivered first hand by another node");
        assertTrue(rc.msgSigning().verify(wire(unsigned), Caller.plugin("t1", "rgA", "a1", "p1"), false, a1id).ok(), "its plugin's");
        Caller user = Caller.api("t1", "alice", List.of("user"));
        assertTrue(rc.msgSigning().verify(wire(unsigned), user, false, a1id).ok(), "a user of its tenant, vouched by the node");
        assertFalse(rc.msgSigning().verify(wire(unsigned), Caller.api("t2", "eve", List.of("admin")), false, a1id).ok(),
                "a user of another tenant");
        // turned off: signatures only
        ControllerEngine strict = node("rgA", "rc", true, "msg_signing_direct", "false");
        assertFalse(strict.msgSigning().verify(wire(unsigned), a1c, false, a1id).ok(), "msg_signing_direct=false accepted an unsigned message");
        // an agent trusts its parent's broker only for the parent's own messages
        assertEquals(CrescoIdentity.of("t1", "rgA", "rc", null),
                AgentConsumer.trustedFirstHand(CrescoIdentity.of("t1", "rgA", "rc", null), false, "rgA", "rc"));
        assertNull(AgentConsumer.trustedFirstHand(a2id, false, "rgA", "rc"), "an agent took its parent broker's word for another node");
        assertEquals(a2id, AgentConsumer.trustedFirstHand(a2id, true, "rgA", "rc"), "a controller trusts its own broker");
    }

    /** One ping/pong between child and parent, as the health watchers and executors run it. */
    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    @Test
    void anOriginSendsUnsignedOnlyToAParentOrChildThatOfferedIt() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1n = node("rgA", "a1", false), old = node("rgA", "a9", false, "msg_signing_direct", "false");
        Caller rcC = Caller.controller("t1", "rgA", "rc"), a1C = Caller.controller("t1", "rgA", "a1");
        // before any ping: signed both ways
        assertNotNull(originated(a1n, a1C, "rgA", "rc").getParam(MsgSigning.SIG));
        assertNotNull(originated(rc, rcC, "rgA", "a1").getParam(MsgSigning.SIG));
        ping(a1n, rc, false);
        ping(old, rc, false);
        MsgEvent up = originated(a1n, a1C, "rgA", "rc"), down = originated(rc, rcC, "rgA", "a1");
        assertNull(up.getParam(MsgSigning.SIG), "to its parent, after the parent offered direct links: unsigned");
        assertEquals("rgA_a1", up.getParam(MsgSigning.DIRECT));
        assertNull(down.getParam(MsgSigning.SIG), "to a child that offered direct links: unsigned");
        assertNotNull(originated(rc, rcC, "rgA", "a9").getParam(MsgSigning.SIG), "a child that did not offer them is sent a signature");
        assertNotNull(originated(a1n, a1C, "rgA", "a2").getParam(MsgSigning.SIG), "another agent (through a relay) is sent a signature");
        assertNotNull(originated(a1n, a1C, "glob", "gc").getParam(MsgSigning.SIG), "a node beyond the parent is sent a signature");
        assertEquals(2, a1n.msgSigning().directSentCount() + rc.msgSigning().directSentCount() - 0, "direct sends counted");
        // the copy of the origin's word on the wire never survives into the receiver's MsgEvent
        MsgEvent got = wire(up);
        MsgSigning.notPending(got);
        assertNull(got.getParam(MsgSigning.DIRECT));
    }
}
