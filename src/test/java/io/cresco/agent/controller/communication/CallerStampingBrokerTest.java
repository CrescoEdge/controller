package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The broker stamps every message with a caller the producing connection may declare (docs/caller-authorization.md
 * section 2): a node declares itself, its own plugins and its tenant's users; anything else is stripped; a sender
 * without stamping is that node with minimal rights. And no subscription but the executor's sees stdin. Real broker,
 * real mutual TLS.
 */
class CallerStampingBrokerTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id brokerId, a1, a2;
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private CrescoAuthorizationBroker authz;
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("stamp").toString(), cfg);
        authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService s : brokers) s.stop();
    }

    private Session client(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private int rounds;

    /** a1 sends one message declaring {@code caller} (null: no properties at all); what does a2 receive? */
    private Message roundTrip(String caller, String tenant, String roles) throws Exception {
        String queue = "t1.stamps." + (++rounds);                     // a queue of its own: one consumer, one message
        Session rx = client(a2);
        MessageConsumer c = rx.createConsumer(rx.createQueue(queue));
        Session tx = client(a1);
        MessageProducer p = tx.createProducer(tx.createQueue(queue));
        TextMessage m = tx.createTextMessage("hello");
        if (caller != null) {
            m.setStringProperty("cresco_caller", caller);
            m.setStringProperty("cresco_caller_tenant", tenant);
            m.setStringProperty("cresco_caller_roles", roles);
        }
        p.send(m);
        Message got = c.receive(5000);
        assertNotNull(got, "nothing delivered");
        return got;
    }

    private void kept(String caller, String tenant, String roles) throws Exception {
        Message got = roundTrip(caller, tenant, roles);
        assertEquals(caller, got.getStringProperty("cresco_caller"), caller + " was not kept");
        assertEquals(tenant, got.getStringProperty("cresco_caller_tenant"));
    }

    private void stripped(String caller, String tenant, String roles) throws Exception {
        long before = authz.forgedStampCount();
        Message got = roundTrip(caller, tenant, roles);
        assertEquals("", got.getStringProperty("cresco_caller"), caller + " was not stripped");
        assertEquals("", got.getStringProperty("cresco_caller_roles"));
        assertEquals(before + 1, authz.forgedStampCount());
    }

    @Test
    void aNodeDeclaresItselfItsPluginsAndItsTenantsUsers() throws Exception {
        kept("controller:t1/rgA/a1", "t1", "infra");
        kept("plugin:t1/rgA/a1/p1", "t1", "plugin");
        kept("api:t1/alice", "t1", "user");
    }

    @Test
    void anythingElseIsStripped() throws Exception {
        stripped("controller:t1/rgA/rc", "t1", "infra");               // another node's controller
        stripped("plugin:t1/rgA/a2/p1", "t1", "plugin");               // another node's plugin
        stripped("api:t2/eve", "t2", "admin");                         // another tenant's admin
        stripped("api:cresco-system/op", "cresco-system", "admin");    // a superuser admin, from a tenant's node
        stripped("anonymous", "", "infra,admin,plugin,user");
        stripped("agent:t1/rgA/a1", "t1", "user");                     // agent: is the broker's to give
        stripped("controller:t1/rgA/a1", "t2", "infra");               // malformed: tenant does not match
        stripped("plugin:t1/rgA/a1/p1", "t1", "infra");                // well formed, but: forged roles count as declared
    }

    @Test
    void aSenderWithoutStampingIsThatNodeWithMinimalRights() throws Exception {
        long before = authz.legacyStampCount();
        Message got = roundTrip(null, null, null);
        assertEquals("agent:t1/rgA/a1", got.getStringProperty("cresco_caller"));
        assertEquals("user", got.getStringProperty("cresco_caller_roles"));
        assertEquals(before + 1, authz.legacyStampCount());
        Message none = roundTrip("", "", "");
        assertEquals("", none.getStringProperty("cresco_caller"), "an explicitly unstamped message stays unstamped");
    }

    @Test
    void aNodeNamesOnlyItsTenantsUsersAsDataplaneWriters() throws Exception {
        for (String[] w : new String[][]{{"api:t1/alice", "user", "api:t1/alice"}, {"api:t2/eve", "admin", null},
                {"controller:t1/rgA/rc", "infra", null}}) {
            Session rx = client(a2);
            String q = "t1.writer." + (++rounds);
            MessageConsumer c = rx.createConsumer(rx.createQueue(q));
            Session tx = client(a1);
            TextMessage m = tx.createTextMessage("line");
            m.setStringProperty("cresco_writer", w[0]);
            m.setStringProperty("cresco_writer_roles", w[1]);
            tx.createProducer(tx.createQueue(q)).send(m);
            Message got = c.receive(5000);
            assertNotNull(got);
            assertEquals(w[2], got.getStringProperty("cresco_writer"), w[0]);
        }
    }

    @Test
    void noSubscriptionButTheExecutorsSeesStdin() throws Exception {
        Session spy = client(a2);
        JMSException e = assertThrows(JMSException.class,
                () -> spy.createConsumer(spy.createTopic("global.event"), "type LIKE 'input-%'"));
        assertTrue(String.valueOf(e.getMessage()).contains("stdin"), e.getMessage());
        MessageConsumer wide = spy.createConsumer(spy.createTopic("global.event"));
        Session tx = client(a1);
        MessageProducer p = tx.createProducer(tx.createTopic("global.event"));
        TextMessage stdin = tx.createTextMessage("secret typed into a process");
        stdin.setStringProperty("type", "input-0a1b2c");
        p.send(stdin);
        TextMessage event = tx.createTextMessage("an ordinary event");
        event.setStringProperty("type", "event");
        p.send(event);
        TextMessage got = (TextMessage) wide.receive(5000);
        assertNotNull(got);
        assertEquals("an ordinary event", got.getText(), "a subscriber without a stdin selector received stdin");
        assertNull(wide.receive(500));
    }
}
