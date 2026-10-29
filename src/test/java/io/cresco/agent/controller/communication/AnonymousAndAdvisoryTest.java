package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Third review, findings 7 and 10: a connection without a client certificate has no identity and no role whatever username
 * it types; consumer/producer/connection advisories reach a tenant only for its own destinations.
 */
class AnonymousAndAdvisoryTest {

    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();
    private RegionCA caA, caB, caS;

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private BrokerService broker(boolean needClientAuth) throws Exception {
        TlsTestSupport.provider();
        caA = RegionCA.generate("t1", "rgA", 2048, 1);
        caB = RegionCA.generate("t2", "rgB", 2048, 1);
        caS = RegionCA.generate("cresco-system", "rgS", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("anon").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t2", "test"));
        assertNull(peers.trustCa(caS.caCert(), "rgS", "cresco-system", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_gc", TlsTestSupport.issued(caA, "t1", "rgA", "gc"),
                needClientAuth ? "?needClientAuth=true" : "?wantClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert(), caB.caCert(), caS.caCert());
        brokers.add(b);
        return b;
    }

    private Session client(BrokerService b, TlsTestSupport.Id id, String user) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id == null ? null : id.keyManagers(), TlsTestSupport.trustManagers(caA.caCert()), new SecureRandom());
        if (user != null) { f.setUserName(user); f.setPassword("x"); }
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static List<String> drain(MessageConsumer c, long wait) throws Exception {
        List<String> got = new ArrayList<>();
        Message m = c.receive(wait);
        while (m != null) { got.add(m instanceof TextMessage ? ((TextMessage) m).getText() : String.valueOf(m)); m = c.receive(300); }
        return got;
    }

    @Test
    void aConnectionWithoutACertificateReadsNothingWritesNothingAndHoldsNothing() throws Exception {
        BrokerService b = broker(false);
        Session owner = client(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), null);
        Session spy = client(b, null, "cresco-system|rgS|admin");
        MessageConsumer c = spy.createConsumer(spy.createQueue("T.t1.inbox"));
        owner.createProducer(owner.createQueue("T.t1.inbox")).send(owner.createTextMessage("t1-secret"));
        assertTrue(drain(c, 1500).isEmpty(), "a certificate-less connection that typed a superuser username received");
        spy.createProducer(spy.createQueue("T.t1.inbox")).send(spy.createTextMessage("injected"));
        MessageConsumer real = owner.createConsumer(owner.createQueue("T.t1.inbox"));
        assertEquals(List.of("t1-secret"), drain(real, 2000), "the owner lost its message, or received the anonymous one");
        assertThrows(JMSException.class, () -> spy.createDurableSubscriber(spy.createTopic("T.t1.events"), "keep-everything"),
                "a certificate-less connection was allowed a durable subscription");
        // an exclusive consumer request cannot take a queue from its owner
        Session spy2 = client(b, null, "t1|rgA|x");
        spy2.createConsumer(spy2.createQueue("T.t1.work?consumer.exclusive=true"));
        Thread.sleep(200);
        MessageConsumer worker = owner.createConsumer(owner.createQueue("T.t1.work"));
        owner.createProducer(owner.createQueue("T.t1.work")).send(owner.createTextMessage("job"));
        assertEquals(List.of("job"), drain(worker, 3000), "a certificate-less exclusive consumer starved the queue");
    }

    @Test
    void advisoriesReachATenantOnlyForItsOwnDestinations() throws Exception {
        BrokerService b = broker(true);
        Session t2 = client(b, TlsTestSupport.issued(caB, "t2", "rgB", "watcher"), null);
        Session su = client(b, TlsTestSupport.issued(caS, "cresco-system", "rgS", "admin"), null);
        MessageConsumer t2Foreign = t2.createConsumer(t2.createTopic("ActiveMQ.Advisory.Consumer.Queue.T.t1.>"));
        MessageConsumer t2Own = t2.createConsumer(t2.createTopic("ActiveMQ.Advisory.Consumer.Queue.T.t2.>"));
        MessageConsumer t2Conn = t2.createConsumer(t2.createTopic("ActiveMQ.Advisory.Connection"));
        MessageConsumer suAll = su.createConsumer(su.createTopic("ActiveMQ.Advisory.Consumer.Queue.T.t1.>"));
        Session t1 = client(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), null);
        t1.createConsumer(t1.createQueue("T.t1.payroll"), "stream='q3-salaries'");
        t2.createConsumer(t2.createQueue("T.t2.mine"));
        assertNull(t2Foreign.receive(1500), "tenant t2 saw tenant t1's consumer and selector in an advisory");
        Message own = t2Own.receive(3000);
        assertNotNull(own, "tenant t2 no longer sees advisories about its own destinations");
        assertTrue(((ActiveMQMessage) own).getDataStructure() instanceof ConsumerInfo);
        assertNull(t2Conn.receive(500), "tenant t2 saw another connection's advisory (username, client id)");
        assertNotNull(suAll.receive(3000), "a superuser no longer sees every tenant's advisories");
    }
}
