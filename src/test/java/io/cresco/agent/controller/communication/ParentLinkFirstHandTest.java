package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An agent takes its parent broker's first-hand word for the parent's own unsigned message from the moment its link to
 * that parent exists -- not only while its state names the parent. On a real secured broker with real mutual TLS: the
 * region rgA's controller rc (its broker, its own in-JVM connection), the agent a1 with its real inbox consumer on a
 * failover link bound to rgA/rc, and a sibling a2.
 *
 * <p>An agent's state names its parent from the end of its join (after its inbox is up) until it declares that parent
 * lost (before its inbox is closed). What the parent's broker delivered outside that span -- the answer to a ping of the
 * agent's previous link, still on the inbox queue when the agent joined again a moment later -- was refused, "an unsigned
 * message (from controller:t1/rgr/rc[infra], not delivered first hand) [EXEC pong]": on one host, a region controller
 * with 30 light agents stopped for 40 s, 13 of them logged it (release 1.3.379: 24), the line the DGX scale run counted
 * at its agents with the region controller at a whole core. Fails before the change: the first case is refused, and in
 * the second the state's word was taken for a node the link was not made to.
 */
class ParentLinkFirstHandTest {

    private static final Gson GSON = new Gson();
    private static final Set<String> SUPERUSERS = Set.of("cresco-system");
    private static RegionCA ca;
    private static TlsTestSupport.Id brokerId, a2;
    private final List<Connection> connections = new ArrayList<>();
    private final List<ControllerEngine> agents = new ArrayList<>();
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("parentlink").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
    }

    @AfterEach
    void stop() throws Exception {
        for (ControllerEngine e : agents) try { e.getActiveClient().shutdown(); } catch (Exception ignore) { }
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        b.stop();
    }

    /** The region controller's own connection to its broker, as its ActiveClient opens it. */
    private Session controller() throws Exception {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false");
        f.setUserName(CrescoAuthorizationBroker.LOCAL_CONTROLLER_PREFIX + "t1|rgA|rc");
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /** A sibling agent's own TLS connection. */
    private Session sibling() throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(a2.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /** The agent a1 joining rgA/rc: its link to the broker is up (bound to rgA/rc), its state names no parent yet. */
    private ControllerEngine joiningAgent() throws Exception {
        return joiningAgent(0);
    }

    /** As above; {@code holdMs}: msg_signing_hold_ms (0: the inbox holds nothing back). */
    private ControllerEngine joiningAgent(long holdMs) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_hold_ms", String.valueOf(holdMs));
        ControllerEngine a1 = TlsTestSupport.engine("rgA", "a1", cfg);
        TlsTestSupport.enroll(a1, ca, "t1");
        agents.add(a1);
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(b));
        a1.getActiveClient().expectPeer(uri, "rgA", "rc");            // what initIOChannels records before the link is made
        assertTrue(a1.getActiveClient().initActiveAgentConsumer(MsgRouter.inboxQueue("rgA_a1", false, true, "t1", SUPERUSERS), uri));
        return a1;
    }

    /**
     * {@code from} sends a1, unsigned over the direct link, the answer to a call a1 no longer waits for (as a late pong is),
     * stamped as {@code who} and claiming to be its origin ({@code origin}); then waits until a1 has judged it.
     */
    private static void deliverUnsigned(Session from, ControllerEngine a1, Caller who, String origin) throws Exception {
        long before = judged(a1);
        send(from, a1, who, origin);
        for (int i = 0; i < 200 && judged(a1) == before; i++) Thread.sleep(25);
        assertEquals(before + 1, judged(a1), "a1 did not judge the message");
    }

    /** As {@link #deliverUnsigned}, without waiting: the message is on a1's inbox queue. */
    private static void send(Session from, ControllerEngine a1, Caller who, String origin) throws Exception {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, who.getRegion(), who.getAgent(), null, "rgA", "a1", null, false, false);
        m.setParam("action", "ping");
        PluginBuilder pb = a1.getPluginBuilder();
        m.setParam("callId-" + pb.getRegion() + "-" + pb.getAgent() + "-" + pb.getPluginID(), "a-call-nobody-waits-for");
        m.setParam(MsgSigning.DIRECT, origin);
        MessageProducer p = from.createProducer(from.createQueue("T.t1.rgA_a1"));
        TextMessage t = from.createTextMessage(GSON.toJson(m));
        CallerStamps.toJms(who, t);
        t.setStringProperty(CallerStamps.P_ORIGIN, origin);
        p.send(t);
        p.close();
    }

    private static long judged(ControllerEngine a1) {
        return a1.msgSigning().directAcceptedCount() + a1.msgSigning().refusedCount();
    }

    private static long refusedUnsigned(ControllerEngine a1) {
        return a1.msgSigning().refusedCount(MsgSigning.Refusal.UNSIGNED);
    }

    @Test
    void theParentsOwnMessageIsTakenFirstHandWhileTheJoinIsNotComplete() throws Exception {
        ControllerEngine a1 = joiningAgent();
        TlsTestSupport.State state = (TlsTestSupport.State) a1.cstate;
        assertNull(state.getRegionalRegion(), "precondition: the agent's state names no parent yet");
        Session rc = controller(), sib = sibling();
        Caller rcC = Caller.controller("t1", "rgA", "rc");

        // (1) the link is up, the join is not complete: the parent's own message, attested first hand by its broker
        deliverUnsigned(rc, a1, rcC, "rgA_rc");
        assertEquals(0, refusedUnsigned(a1), "the parent's own message on the link made to it was refused before the join completed");
        assertEquals(1, a1.msgSigning().directAcceptedCount());

        // (2) a sibling's message on the same broker, attested as the sibling's: refused, as it always was (the parent's
        // broker is trusted for the parent's own messages only)
        deliverUnsigned(sib, a1, Caller.controller("t1", "rgA", "a2"), "rgA_a2");
        assertEquals(1, refusedUnsigned(a1), "an agent took its parent's broker's word for another node");

        // (3) joined: the state names the parent -- accepted, as before
        state.parentRegion = "rgA";
        state.parentAgent = "rc";
        deliverUnsigned(rc, a1, rcC, "rgA_rc");
        assertEquals(1, refusedUnsigned(a1));
        assertEquals(2, a1.msgSigning().directAcceptedCount());

        // (4) the parent declared lost (the state names none again) while the inbox is being closed: still its own word
        state.parentRegion = null;
        state.parentAgent = null;
        deliverUnsigned(rc, a1, rcC, "rgA_rc");
        assertEquals(1, refusedUnsigned(a1), "the parent's own message was refused while the link to it was being closed");
        assertEquals(3, a1.msgSigning().directAcceptedCount());
    }

    @Test
    void aLinkToAnotherControllerDoesNotMakeItsBrokersWordTheParents() throws Exception {
        ControllerEngine a1 = joiningAgent();
        // the agent's state names another parent than the controller its link was made to: what that broker attests as
        // the sibling's is refused, and so is what it attests as the controller the state names
        TlsTestSupport.State state = (TlsTestSupport.State) a1.cstate;
        state.parentRegion = "rgA";
        state.parentAgent = "a2";
        deliverUnsigned(sibling(), a1, Caller.controller("t1", "rgA", "a2"), "rgA_a2");
        assertEquals(1, refusedUnsigned(a1), "a node the link was not made to was taken for the parent");
        assertEquals(0, a1.msgSigning().directAcceptedCount());
    }

    /**
     * The parent is the controller this connection is BOUND to, not the peer recorded for its URI now: a later attempt to
     * join recorded another node (a2) for the same address while the old connection, bound to rgA/rc, is still up (it goes
     * only when a new consumer is made). a2 is a client of rc's broker, which attests its message as a2's: refused.
     * (Review rv-scale2, finding 1: ef703b9 read the URI's present entry, and took it for the parent's.)
     */
    @Test
    void aLaterExpectPeerForTheSameUriDoesNotMakeAnotherNodeTheParentOfTheLink() throws Exception {
        ControllerEngine a1 = joiningAgent();
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(b));
        a1.getActiveClient().expectPeer(uri, "rgA", "a2");
        assertNull(((TlsTestSupport.State) a1.cstate).getRegionalAgent());
        deliverUnsigned(sibling(), a1, Caller.controller("t1", "rgA", "a2"), "rgA_a2");
        assertEquals(1, refusedUnsigned(a1), "a sibling's unsigned message was taken for the parent's on a link bound to rgA/rc");
        assertEquals(0, a1.msgSigning().directAcceptedCount());
        // and the parent the link IS bound to is still taken
        deliverUnsigned(controller(), a1, Caller.controller("t1", "rgA", "rc"), "rgA_rc");
        assertEquals(1, a1.msgSigning().directAcceptedCount());
    }

    /**
     * While the node waits for its parent's trust data (its state names a parent it has not heard yet), the inbox holds
     * back what it cannot judge -- except the messages of that parent, the one the state names, whose pong brings the trust
     * data. The controller a connection is bound to is not exempt for that reason: here the state names rgA/rc2 (a new
     * parent) while the old connection to rgA/rc is still up; rc's message is held, and judged when the hold ends. (Review
     * rv-scale2, finding 2: ef703b9 also exempted the link's controller, which nothing tested.)
     */
    @Test
    void whileTrustIsAwaitedTheHoldExemptsTheParentTheStateNamesOnly() throws Exception {
        ControllerEngine a1 = joiningAgent(1_000);
        TlsTestSupport.State state = (TlsTestSupport.State) a1.cstate;
        state.parentRegion = "rgA";
        state.parentAgent = "rc2";
        assertFalse(a1.trustReady(), "precondition: a1 waits for its parent's trust data");
        AgentConsumer inbox = a1.getActiveClient().agentConsumer();
        long before = judged(a1);
        send(controller(), a1, Caller.controller("t1", "rgA", "rc"), "rgA_rc");
        for (int i = 0; i < 100 && inbox.heldForTrust() == 0; i++) Thread.sleep(10);
        assertEquals(1, inbox.heldForTrust(), "the link's controller, which the state does not name, was not held back");
        assertEquals(before, judged(a1), "judged before the hold ended");
        for (int i = 0; i < 400 && judged(a1) == before; i++) Thread.sleep(25);
        assertEquals(1, a1.msgSigning().directAcceptedCount(), "judged when the hold ended: the link's controller's own message");

        // the state names rc: its messages are never held back
        state.parentAgent = "rc";
        assertFalse(a1.trustReady());
        deliverUnsigned(controller(), a1, Caller.controller("t1", "rgA", "rc"), "rgA_rc");
        assertEquals(1, inbox.heldForTrust(), "the parent the state names was held back");
        assertEquals(2, a1.msgSigning().directAcceptedCount());
    }
}
