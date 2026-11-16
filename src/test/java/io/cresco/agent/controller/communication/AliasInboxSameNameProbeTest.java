package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
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
 * F-17 probe (review of the inbox fix of 1.3.379). A node may read the inbox of its former address (region_alias): the
 * queue {@code T.<tenant>.<another region>_<its own agent name>}. The region in that name was the node's own statement, so
 * a node that joined a region under the agent name of a node of ANOTHER region read that node's inbox.
 *
 * <p>On the secured broker of region rgB, with real mutual TLS: rgB/a1 holds a valid certificate of tenant t1 under the
 * agent name of rgA/a1 (in the standard profile an agent's name is its own claim, bound first come in its region). A node
 * of rgB writes to rgA/a1, as rgA/a1's peers do; the message waits on this broker for a consumer. Before the fix, rgB/a1
 * opened a consumer on {@code T.t1.rgA_a1}, said nothing else, and took every message addressed to rgA/a1 (and could
 * browse that queue or remove it, with what waited in it). The broker here is configured as the controller configures its
 * own: nothing about any move is recorded for rgB/a1.
 */
class AliasInboxSameNameProbeTest {

    private static final String VICTIM_INBOX = "T.t1.rgA_a1";       // the inbox of rgA/a1, a node of another region

    private static RegionCA caB;
    private static TlsTestSupport.Id brokerId, sameName, peer;
    private final List<Connection> connections = new ArrayList<>();
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        caB = RegionCA.generate("t1", "rgB", 2048, 1);
        brokerId = TlsTestSupport.issued(caB, "t1", "rgB", "rc");
        sameName = TlsTestSupport.issued(caB, "t1", "rgB", "a1");     // the same agent name as rgA/a1
        peer = TlsTestSupport.issued(caB, "t1", "rgB", "b2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("alias-probe").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgB_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, caB.caCert());
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        b.stop();
    }

    private ActiveMQConnection connect(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(caB.caCert()), new SecureRandom());
        ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
        connections.add(c);
        c.start();
        return c;
    }

    private static void send(Session from, String queue, int count, String tag) throws Exception {
        MessageProducer p = from.createProducer(from.createQueue(queue));
        for (int i = 0; i < count; i++) p.send(from.createTextMessage(tag + i));
        p.close();
    }

    @Test
    void aNodeOfAnotherRegionWithTheSameAgentNameDoesNotReadThatNodesInbox() throws Exception {
        Session writer = connect(peer).createSession(false, Session.AUTO_ACKNOWLEDGE);
        send(writer, VICTIM_INBOX, 5, "for-rgA-a1-");
        ActiveMQConnection thief = connect(sameName);
        Session s = thief.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer c = null;
        try {
            c = s.createConsumer(s.createQueue(VICTIM_INBOX));
        } catch (JMSException refused) {
            // the fix: refused at the subscription
        }
        Set<String> taken = new TreeSet<>();
        if (c != null) {
            for (Message m = c.receive(3000); m != null; m = c.receive(500)) taken.add(((TextMessage) m).getText());
        }
        assertTrue(taken.isEmpty(), "rgB/a1 took " + taken.size() + " message(s) addressed to rgA/a1: " + taken);
        assertNull(c, "the broker gave rgB/a1 a consumer on the inbox of rgA/a1");

        int browsed = -1;
        try {
            QueueBrowser qb = s.createBrowser(s.createQueue(VICTIM_INBOX));
            browsed = java.util.Collections.list(qb.getEnumeration()).size();
        } catch (JMSException refused) {
            // the fix
        }
        assertEquals(-1, browsed, "rgB/a1 browsed the inbox of rgA/a1 (" + browsed + " message(s))");
        assertThrows(JMSException.class, () -> thief.destroyDestination(new ActiveMQQueue(VICTIM_INBOX)),
                "rgB/a1 removed the inbox of rgA/a1 and what waited in it");
    }
}
