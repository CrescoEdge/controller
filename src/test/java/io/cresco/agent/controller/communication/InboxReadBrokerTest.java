package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
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
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node reads its own inbox and no other queue of its tenant (docs/tenant-isolation-design.md section 12a), on a real
 * secured broker with real mutual TLS: the region rgA's controller rc (its broker; its own connection is in-JVM), its
 * leaves a1 and a2 of tenant t1, and op, a node of the superuser tenant (of a region rgS). Before, a1 -- any node holding a certificate of
 * the tenant -- opened a consumer on its controller's inbox, or on {@code T.t1.>}, and took the control messages sent there.
 */
class InboxReadBrokerTest {

    private static final String CONTROLLER_INBOX = "T.t1.rgA_rc";       // where a node of t1 writes to its controller
    private static final Set<String> SUPERUSERS = Set.of("cresco-system");

    private static RegionCA ca, caB, caS;
    private static TlsTestSupport.Id brokerId, a1, a2, op;
    private final List<Connection> connections = new ArrayList<>();
    private CrescoAuthorizationBroker authz;
    private PeerTrustRegistry peers;
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        caB = RegionCA.generate("t1", "rgB", 2048, 1);
        caS = RegionCA.generate("cresco-system", "rgS", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
        op = TlsTestSupport.issued(caS, "cresco-system", "rgS", "op");
    }

    @BeforeEach
    void start() throws Exception {
        peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        assertNull(peers.trustCa(caS.caCert(), "rgS", "cresco-system", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");                  // the secure defaults: tenant namespacing with it
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("inbox").toString(), cfg);
        authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert(), caB.caCert(), caS.caCert());
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        b.stop();
    }

    private ActiveMQConnection connect(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
        connections.add(c);
        c.start();
        return c;
    }

    private Session client(TlsTestSupport.Id id) throws Exception {
        return connect(id).createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /** The controller's own connection to its broker, as ActiveClient opens it. */
    private Session controller() throws Exception {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false");
        f.setUserName(CrescoAuthorizationBroker.LOCAL_CONTROLLER_PREFIX + "t1|rgA|rc");
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /** The controller's inbox consumer, on the queue ControllerSMHandler gives a controller on its own broker. */
    private MessageConsumer controllerInbox(Session controller) throws Exception {
        String queue = MsgRouter.inboxQueue("rgA_rc", true, true, "t1", SUPERUSERS);
        assertEquals("T.*.rgA_rc", queue);
        return controller.createConsumer(controller.createQueue(queue));
    }

    private static void send(Session from, String queue, int count, String tag) throws Exception {
        MessageProducer p = from.createProducer(from.createQueue(queue));
        for (int i = 0; i < count; i++) p.send(from.createTextMessage(tag + i));
        p.close();
    }

    private static Set<String> drain(MessageConsumer c, int expected, long firstWait) throws Exception {
        Set<String> got = new TreeSet<>();
        Message m = c.receive(firstWait);
        while (m != null) {
            got.add(((TextMessage) m).getText());
            m = c.receive(got.size() >= expected ? 300 : 3000);
        }
        return got;
    }

    private ConnectionContext contextOf(ActiveMQConnection c) throws Exception {
        for (TransportConnection t : b.getTransportConnectorByName("tls").getConnections()) {
            try {
                org.apache.activemq.broker.TransportConnectionState state = t.lookupConnectionState(c.getConnectionInfo().getConnectionId());
                if (state.getInfo().getConnectionId().equals(c.getConnectionInfo().getConnectionId())) return state.getContext();
            } catch (IllegalStateException notThisOne) {
                // another client's connection
            }
        }
        throw new AssertionError("the broker does not hold the connection");
    }

    private static final List<String> NOT_ITS_OWN = List.of(
            CONTROLLER_INBOX,                                    // its controller's inbox
            CONTROLLER_INBOX + "?consumer.exclusive=true",       // ... for itself alone
            "T.t1.rgA_a2",                                       // a sibling's
            "T.t1.old_a2",                                       // a sibling's former address
            "T.t1.old_rc",
            "T.t1.rgA_rc_plugin-0",                              // a plugin-suffixed form
            "T.t1.>", "T.t1.*", "T.t1.*.>",                      // patterns over every inbox of the tenant
            "T.*.rgA_a1",                                        // what only a superuser tenant's node reads
            "T.t1.rgA_a1," + CONTROLLER_INBOX,                   // a composite with its own inbox
            "T.t1.work");                                        // a queue that is no inbox

    @Test
    void aLeafOpensNoConsumerAndNoBrowserOnAnotherNodesInboxOrOnAPattern() throws Exception {
        Session leaf = client(a1);
        for (String queue : NOT_ITS_OWN) {
            assertThrows(JMSException.class, () -> leaf.createConsumer(leaf.createQueue(queue)), "a1 opened a consumer on the queue " + queue);
            assertThrows(JMSException.class, () -> leaf.createBrowser(leaf.createQueue(queue)).getEnumeration(),
                    "a1 opened a browser on the queue " + queue);
        }
        // and it is still a working connection
        MessageConsumer own = leaf.createConsumer(leaf.createQueue("T.t1.rgA_a1"));
        send(client(a2), "T.t1.rgA_a1", 1, "after-");
        assertEquals(Set.of("after-0"), drain(own, 1, 5000));
    }

    @Test
    void whatIsSentToAControllersInboxReachesTheControllerAndNoOtherNode() throws Exception {
        MessageConsumer rightful = controllerInbox(controller());
        Session leaf = client(a1);
        List<MessageConsumer> taken = new ArrayList<>();
        for (String queue : NOT_ITS_OWN) {
            try {
                taken.add(leaf.createConsumer(leaf.createQueue(queue)));
            } catch (JMSException refused) {
                // as it must be
            }
        }
        send(client(a2), CONTROLLER_INBOX, 40, "m");
        Set<String> got = drain(rightful, 40, 5000);
        int stolen = 0;
        for (MessageConsumer c : taken) while (c.receive(200) != null) stolen++;
        assertEquals(0, stolen, "a1 received messages sent to its controller's inbox");
        assertEquals(40, got.size(), "the controller did not receive all that was sent to its inbox: " + got);
        assertTrue(taken.isEmpty(), "a1 holds " + taken.size() + " subscriptions on queues that are not its inbox");
    }

    @Test
    void aSubscriptionTheRuleNeverJudgedIsGivenNothing() throws Exception {
        // defence in depth: a subscription of a1's connection on its controller's inbox, put on the queue past the
        // authorization (straight at the region broker), as if some path had let it through
        MessageConsumer rightful = controllerInbox(controller());
        ActiveMQConnection leaf = connect(a1);
        ConsumerInfo info = new ConsumerInfo(new ConsumerId(new SessionId(leaf.getConnectionInfo().getConnectionId(), 4242), 1));
        info.setDestination(new ActiveMQQueue(CONTROLLER_INBOX));
        info.setPrefetchSize(1000);
        Subscription rogue = b.getRegionBroker().addConsumer(contextOf(leaf), info);
        long withheldBefore = authz.withheldCount();
        send(client(a2), CONTROLLER_INBOX, 40, "m");
        Set<String> got = drain(rightful, 40, 5000);
        assertEquals(0, rogue.getDispatchedCounter(), "messages of the controller's inbox were dispatched to a1's connection");
        assertEquals(40, got.size(), "the controller did not receive all that was sent to its inbox: " + got);
        assertTrue(authz.withheldCount() > withheldBefore, "what was withheld from a1 was not counted");
    }

    @Test
    void perMessageANodeIsGivenItsOwnInboxOnly() throws Exception {
        ConnectionContext leaf = contextOf(connect(a1));
        assertFalse(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue(CONTROLLER_INBOX))), "its controller's inbox");
        assertFalse(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue("T.t1.rgA_a2"))), "a sibling's inbox");
        assertFalse(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue("T.t1.old_a2"))), "a sibling's former address");
        assertTrue(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue("T.t1.rgA_a1"))), "its own inbox");
        assertTrue(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue("T.t1.old_a1"))), "its former address");
        assertTrue(authz.isAllowedToConsume(leaf, on(new ActiveMQQueue("rgA_a1"))), "its raw inbox (an earlier build's node)");
        assertTrue(authz.isAllowedToConsume(leaf, on(new ActiveMQTopic("T.t1.agent.event"))), "a topic of its tenant: as before");
        assertTrue(authz.isAllowedToConsume(leaf, on(new ActiveMQTopic("T.t1.rgA_rc"))), "a topic is not a queue");
        // a superuser tenant's node reads its inbox in every tenant's namespace, and whatever else it subscribed to
        ConnectionContext operator = contextOf(connect(op));
        assertTrue(authz.isAllowedToConsume(operator, on(new ActiveMQQueue("T.t1.rgS_op"))));
        assertTrue(authz.isAllowedToConsume(operator, on(new ActiveMQQueue(CONTROLLER_INBOX))));
        assertTrue(authz.isAllowedToConsume(operator, on(new ActiveMQQueue("T.cresco-system.rgA_rc"))));
    }

    private static ActiveMQTextMessage on(org.apache.activemq.command.ActiveMQDestination d) {
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setDestination(d);
        return m;
    }

    @Test
    void anAdvisorysCopyOfAnInboxMessageReachesNoOtherNode() throws Exception {
        // an advisory that carries a copy of a message (expired, discarded, sent to the dead-letter queue) is a topic that
        // holds a message addressed to one node: the copy is read as the queue it was on
        ConnectionContext leaf = contextOf(connect(a1));
        ActiveMQMessage aboutController = new ActiveMQMessage();
        aboutController.setDestination(AdvisorySupport.getExpiredMessageTopic(new ActiveMQQueue(CONTROLLER_INBOX)));
        aboutController.setDataStructure(on(new ActiveMQQueue(CONTROLLER_INBOX)));
        assertFalse(authz.isAllowedToConsume(leaf, aboutController), "a1 was given the copy of a message of its controller's inbox");
        ActiveMQMessage aboutItself = new ActiveMQMessage();
        aboutItself.setDestination(AdvisorySupport.getExpiredMessageTopic(new ActiveMQQueue("T.t1.rgA_a1")));
        aboutItself.setDataStructure(on(new ActiveMQQueue("T.t1.rgA_a1")));
        assertTrue(authz.isAllowedToConsume(leaf, aboutItself), "a1 is no longer given the copy of a message of its own inbox");
        ActiveMQMessage aboutATopic = new ActiveMQMessage();
        aboutATopic.setDestination(AdvisorySupport.getExpiredMessageTopic(new ActiveMQTopic("T.t1.agent.event")));
        aboutATopic.setDataStructure(on(new ActiveMQTopic("T.t1.agent.event")));
        assertTrue(authz.isAllowedToConsume(leaf, aboutATopic), "the copy of a message of its tenant's topic: as before");
    }

    @Test
    void theAdvisoryAboutAMessageThatExpiredInAControllersInboxReachesNoOtherNode() throws Exception {
        // live: a message to the controller's inbox expires there; the broker's advisory carries the message's properties
        Session controller = controller();
        MessageConsumer forController = controller.createConsumer(controller.createTopic("ActiveMQ.Advisory.Expired.Queue.>"));
        Session spy = client(a1);
        MessageConsumer forSpy = spy.createConsumer(spy.createTopic("ActiveMQ.Advisory.Expired.Queue.>"));
        Session sender = client(a2);
        MessageProducer p = sender.createProducer(sender.createQueue(CONTROLLER_INBOX));
        TextMessage m = sender.createTextMessage("for the controller");
        m.setStringProperty("what", "for-the-controller-only");
        p.send(m, DeliveryMode.NON_PERSISTENT, 4, 150);
        Thread.sleep(600);
        MessageConsumer inbox = controllerInbox(controller);          // the queue looks at the message now: expired
        assertNull(inbox.receive(500), "an expired message was delivered");
        Message seenByController = forController.receive(5000);
        assertNotNull(seenByController, "the broker sent no advisory about the expired message (the test proves nothing)");
        assertTrue(((ActiveMQMessage) seenByController).getDataStructure() instanceof org.apache.activemq.command.Message,
                "the advisory carries no copy of the message");
        assertNull(forSpy.receive(1500), "a1 was sent the advisory's copy of a message of its controller's inbox");
    }

    @Test
    void aLeafRemovesNoInboxButItsOwn() throws Exception {
        send(client(a2), CONTROLLER_INBOX, 1, "kept-");               // waits there: its controller is not up yet
        ActiveMQConnection leaf = connect(a1);
        assertThrows(JMSException.class, () -> leaf.destroyDestination(new ActiveMQQueue(CONTROLLER_INBOX)),
                "a1 removed its controller's inbox queue, and what waited in it");
        assertEquals(Set.of("kept-0"), drain(controllerInbox(controller()), 1, 5000), "what waited in the controller's inbox is gone");
        send(client(a2), "T.t1.rgA_a1", 1, "x");
        leaf.destroyDestination(new ActiveMQQueue("T.t1.rgA_a1"));    // its own: as before
    }

    @Test
    void aNodeOfTheReleasedBuildStillWorks() throws Exception {
        // what AgentConsumer of a network node opens: its inbox, the inbox of its former address (region_alias), and --
        // an earlier build without tenant namespacing -- its raw inbox; each named as the code names it
        String own = MsgRouter.inboxQueue("rgA_a1", false, true, "t1", SUPERUSERS);
        String alias = RegionAlias.aliasQueue(own, RegionAlias.of("old", "rgA"), "rgA", "a1");
        String raw = "rgA_a1";
        assertEquals("T.t1.rgA_a1", own);
        assertEquals("T.t1.old_a1", alias);
        Session leaf = client(a1);
        MessageConsumer inbox = leaf.createConsumer(leaf.createQueue(own));
        MessageConsumer aliasInbox = leaf.createConsumer(leaf.createQueue(alias));
        MessageConsumer rawInbox = leaf.createConsumer(leaf.createQueue(raw));
        Session controller = controller();
        MessageConsumer controllerInbox = controllerInbox(controller);
        send(controller, own, 5, "own-");
        send(client(a2), own, 5, "peer-");
        send(controller, alias, 5, "alias-");
        send(controller, raw, 5, "raw-");
        assertEquals(10, drain(inbox, 10, 5000).size(), "its own inbox");
        assertEquals(5, drain(aliasInbox, 5, 5000).size(), "the inbox of its former address");
        assertEquals(5, drain(rawInbox, 5, 5000).size(), "its raw inbox");
        send(leaf, CONTROLLER_INBOX, 5, "up-");
        assertEquals(5, drain(controllerInbox, 5, 5000).size(), "what it sends its controller");
        assertTrue(leaf.createBrowser(leaf.createQueue(own)).getEnumeration() != null, "a browser on its own inbox");
        // its dataplane: the topics of its tenant, as before
        MessageConsumer events = leaf.createConsumer(leaf.createTopic("T.t1.agent.event"));
        Session peer = client(a2);
        peer.createProducer(peer.createTopic("T.t1.agent.event")).send(peer.createTextMessage("event"));
        assertNotNull(events.receive(5000), "a topic of its tenant");
    }

    @Test
    void theFormerAddressOfANodeIsNeverTheAddressOfAControllerThisBrokerKnows() throws Exception {
        // the alias form of an inbox (<another region>_<the node's own agent name>) is the node's own statement. It is never
        // the inbox of a controller that is there: this broker's own (rgA/rc), its parent (the global rgG/gc), one that
        // joined it (rgB/rc2) -- or a node that holds the same agent name in another region took that controller's messages
        authz.setParent(() -> new PeerIdentity.Expected("rgG", "gc"));
        assertNull(peers.allowBridgePeer("t1", "rgB", "rc2"));
        ActiveMQConnection likeTheParent = connect(TlsTestSupport.issued(ca, "t1", "rgA", "gc"));
        Session s = likeTheParent.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSException.class, () -> s.createConsumer(s.createQueue("T.t1.rgG_gc")), "rgA/gc reads the inbox of the global rgG/gc");
        assertThrows(JMSException.class, () -> s.createBrowser(s.createQueue("T.t1.rgG_gc")).getEnumeration());
        assertThrows(JMSException.class, () -> likeTheParent.destroyDestination(new ActiveMQQueue("T.t1.rgG_gc")),
                "rgA/gc removes the inbox of the global rgG/gc");
        s.createConsumer(s.createQueue("T.t1.rgA_gc")).close();       // its own inbox
        s.createConsumer(s.createQueue("T.t1.old_gc")).close();       // a former address that is no controller's
        Session likeAPeer = client(TlsTestSupport.issued(ca, "t1", "rgA", "rc2"));
        assertThrows(JMSException.class, () -> likeAPeer.createConsumer(likeAPeer.createQueue("T.t1.rgB_rc2")),
                "rgA/rc2 reads the inbox of the joined controller rgB/rc2");
        Session likeThisController = client(TlsTestSupport.issued(caB, "t1", "rgB", "rc"));
        assertThrows(JMSException.class, () -> likeThisController.createConsumer(likeThisController.createQueue(CONTROLLER_INBOX)),
                "rgB/rc reads the inbox of this broker's controller rgA/rc");
        likeThisController.createConsumer(likeThisController.createQueue("T.t1.rgB_rc")).close();   // its own inbox
    }

    @Test
    void aSuperuserTenantsNodeReadsItsInboxInEveryTenantsNamespace() throws Exception {
        String inbox = MsgRouter.inboxQueue("rgS_op", false, true, "cresco-system", SUPERUSERS);
        assertEquals("T.*.rgS_op", inbox);
        Session operator = client(op);
        MessageConsumer c = operator.createConsumer(operator.createQueue(inbox));
        send(client(a1), "T.t1.rgS_op", 3, "answer-");                // a tenant's node answers it in the tenant's namespace
        assertEquals(3, drain(c, 3, 5000).size(), "a superuser tenant's node no longer reads its inbox in a tenant's namespace");
        // and it reads what it read before: any queue, a pattern
        operator.createConsumer(operator.createQueue("T.t1.>")).close();
        operator.createBrowser(operator.createQueue(CONTROLLER_INBOX)).getEnumeration();
    }
}
