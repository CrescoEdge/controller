package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A direct link ends with the link. A node sends its parent and its children unsigned messages, which the receiving broker
 * attests first hand; the offer made on a ping held for 60 s whatever became of the link. So a node that moved to another
 * controller (and still answered at its former address through that controller's broker) refused every message its former
 * parent sent it for a minute -- unsigned, and no longer first hand --, and a parent that restarted refused the first ping
 * of each child, which it could not attest yet. Now the parent stops when the child's inbox consumer leaves its broker, and
 * the child when its link goes down, is made anew, or a ping goes unanswered; the next ping and pong offer the link again.
 */
class DirectLinkEndsTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
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

    /** One ping/pong between child and parent, as the health watchers and executors run it. */
    private static void ping(ControllerEngine child, ControllerEngine parent) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, false);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    private static boolean unsigned(MsgEvent m) { return m.getParam(MsgSigning.SIG) == null && m.getParam(MsgSigning.DIRECT) != null; }

    @Test
    void aChildWhoseInboxConsumerLeftIsSentSignaturesUntilItPingsAgain() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false), a2 = node("rgA", "a2", false);
        Caller rcC = Caller.controller("t1", "rgA", "rc");
        ping(a1, rc);
        ping(a2, rc);
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a1")), "a child that offered the direct link is not sent unsigned messages");

        // a1 stopped, or moved to another controller: its inbox consumer left this broker
        rc.msgSigning().inboxConsumerLeft("rgA_a1");
        MsgEvent m = originated(rc, rcC, "rgA", "a1");
        assertNotNull(m.getParam(MsgSigning.SIG), "an unsigned message to a child that is gone: refused wherever it now answers");
        assertNull(m.getParam(MsgSigning.DIRECT));
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a2")), "another child lost its direct link");

        // a ping it sent before it left, handled after: the link is not offered again on that
        ping(a1, rc);
        assertNotNull(originated(rc, rcC, "rgA", "a1").getParam(MsgSigning.SIG), "a ping from before the consumer left re-opened the link");

        // it is back and pings (later than a ping on its way could be): direct again
        rc.msgSigning().inboxConsumerLeft("rgA_a2");
        Thread.sleep(MsgSigning.DIRECT_AGAIN_MS + 200);
        ping(a1, rc);
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a1")), "a child that came back never gets its direct link again");
        ping(a2, rc);
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a2")));
    }

    @Test
    void aNodeThatWasNoDirectChildLeavesNothingBehind() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a3 = node("rgA", "a3", false);
        Caller rcC = Caller.controller("t1", "rgA", "rc");
        // consumers of other queues come and go (another node's inbox carried over a bridge, a plugin's queue)
        rc.msgSigning().inboxConsumerLeft("rgA_a3");
        rc.msgSigning().inboxConsumerLeft("some.queue");
        rc.msgSigning().inboxConsumerLeft(null);
        ping(a3, rc);
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a3")), "a node's first ping was held off by a consumer that left before it was a child");
    }

    @Test
    void aChildSendsItsParentSignaturesWhenTheLinkWentDownUntilTheNextPong() throws Exception {
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        Caller a1C = Caller.controller("t1", "rgA", "a1");
        ping(a1, rc);
        assertTrue(unsigned(originated(a1, a1C, "rgA", "rc")));
        // the link went down, was made anew, or a ping went unanswered (the parent may have restarted)
        a1.msgSigning().parentLinkReset();
        MsgEvent m = originated(a1, a1C, "rgA", "rc");
        assertNotNull(m.getParam(MsgSigning.SIG), "unsigned to a parent that may not be able to attest this link yet");
        assertNull(m.getParam(MsgSigning.DIRECT));
        // the first ping after it carries no offer of the direct link: the parent signs (its pong too, whatever its path)
        Caller rcC = Caller.controller("t1", "rgA", "rc");
        ping(a1, rc);
        assertTrue(unsigned(originated(a1, a1C, "rgA", "rc")), "the parent's pong did not offer the direct link again");
        assertNotNull(originated(rc, rcC, "rgA", "a1").getParam(MsgSigning.SIG), "the parent sends unsigned to a child that withdrew its offer");
        ping(a1, rc);
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a1")), "the child's next ping did not offer the direct link again");
    }

    @Test
    void afterAnUnansweredPingBothSidesSignForAWhile() throws Exception {
        // the ping or its pong may have been lost because it travelled unsigned by another controller's bridge: tried
        // again at once, the direct link fails again, every second ping, until the parent is taken for lost
        ControllerEngine rc = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        Caller a1C = Caller.controller("t1", "rgA", "a1"), rcC = Caller.controller("t1", "rgA", "rc");
        ping(a1, rc);
        ping(a1, rc);
        assertTrue(unsigned(originated(a1, a1C, "rgA", "rc")));
        assertTrue(unsigned(originated(rc, rcC, "rgA", "a1")));
        a1.msgSigning().parentPingUnanswered();
        for (int i = 0; i < 3; i++) {
            ping(a1, rc);
            assertNotNull(originated(a1, a1C, "rgA", "rc").getParam(MsgSigning.SIG), "ping " + i + " after a lost one: the child goes unsigned again");
            assertNotNull(originated(rc, rcC, "rgA", "a1").getParam(MsgSigning.SIG), "ping " + i + " after a lost one: the parent goes unsigned again");
        }
        assertTrue(MsgSigning.DIRECT_RETRY_MS >= 30_000L, "the direct link is tried again within a few pings");
    }

    // ------------------------------------------------------------------ the broker says which inbox a consumer left

    @Test
    void theInboxAQueueIs() {
        assertEquals("rgA_a1", CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("T.t1.rgA_a1")));
        assertEquals("rgA_rc", CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("T.*.rgA_rc")), "a controller's own inbox");
        assertEquals("rgA_a1", CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("rgA_a1")), "without namespacing");
        assertNull(CrescoAuthorizationBroker.inboxNode(new ActiveMQTopic("T.t1.rgA_a1")), "a topic is no inbox");
        assertNull(CrescoAuthorizationBroker.inboxNode(new ActiveMQTempQueue("ID:x:1")));
        assertNull(CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("T.t1.")));
        assertNull(CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("T.t1")));
        assertNull(CrescoAuthorizationBroker.inboxNode(new ActiveMQQueue("T.t1.a,T.t1.b")), "a composite destination");
        assertNull(CrescoAuthorizationBroker.inboxNode(null));
    }

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService s : brokers) s.stop();
    }

    private static void await(java.util.function.BooleanSupplier ok, String what) throws Exception {
        long end = System.currentTimeMillis() + 10_000L;
        while (!ok.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < end, what);
            Thread.sleep(20);
        }
    }

    @Test
    void theBrokerTellsWhenANodesInboxConsumerLeaves() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc"), a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("directends").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        List<String> left = new CopyOnWriteArrayList<>();
        authz.setInboxLeftListener(left::add);
        BrokerService b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);

        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(a1.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        // the consumer is closed (the agent's inbox was shut down)
        Connection c1 = f.createConnection();
        connections.add(c1);
        c1.start();
        Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer inbox = s1.createConsumer(s1.createQueue("T.t1.rgA_a1"));
        assertTrue(left.isEmpty(), "told before any consumer left: " + left);
        inbox.close();
        await(() -> left.contains("rgA_a1"), "a closed inbox consumer was not announced");

        // the connection ends with its consumer open (the agent was killed, or its link broke)
        left.clear();
        Connection c2 = f.createConnection();
        c2.start();
        Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        s2.createConsumer(s2.createQueue("T.t1.rgA_a1"));
        s2.createConsumer(s2.createTopic("T.t1.agent.event"));
        c2.close();
        await(() -> left.contains("rgA_a1"), "the inbox consumer of a connection that ended was not announced");
        assertEquals(List.of("rgA_a1"), new ArrayList<>(left), "a topic's consumer was announced as an inbox");
    }
}
