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

/** Third adversarial review (broker). Each test asserts the SECURE outcome; a failure is a finding. */
class ThirdReviewBrokerTest {

    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private BrokerService broker(RegionCA ca, boolean needClientAuth) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz3").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_gc3", TlsTestSupport.issued(ca, "t1", "rgA", "gc3"), needClientAuth,
                new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
        return b;
    }

    private Session client(BrokerService b, RegionCA ca, String user) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(null, TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        f.setUserName(user);
        f.setPassword("anything");
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static List<String> drain(MessageConsumer c) throws Exception {
        List<String> got = new ArrayList<>();
        Message m = c.receive(3000);
        while (m != null) { got.add(m instanceof TextMessage ? ((TextMessage) m).getText() : String.valueOf(m)); m = c.receive(500); }
        return got;
    }

    /** broker_require_client_auth=false (the documented migration setting): a typed username still earns SUPERUSER. */
    @Test
    void aTypedUsernameIsNeverASuperuser() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService b = broker(ca, false);
        Session spy = client(b, ca, "cresco-system|rgA|nobody");          // no certificate, no secret, no password check
        MessageConsumer c = spy.createConsumer(spy.createQueue("T.t2.private-inbox"));
        Session t2 = client(b, ca, "t2|rgA|owner");
        t2.createProducer(t2.createQueue("T.t2.private-inbox")).send(t2.createTextMessage("tenant-t2-secret"));
        List<String> got = drain(c);
        System.out.println("certificate-less client typing 'cresco-system|rgA|nobody' read tenant t2's queue: " + got);
        assertTrue(got.isEmpty(), "a typed username in a superuser tenant read another tenant's queue (SUPERUSER from an unproven username)");
    }

    /** The name grammar allows '.', and the tenant namespace is a string prefix: tenant 't1' reads tenant 't1.x'. */
    @Test
    void aTenantCannotReadAnotherTenantWhoseNameItPrefixes() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService b = broker(ca, false);
        Session t1 = client(b, ca, "t1|rgA|a");
        MessageConsumer c = t1.createConsumer(t1.createQueue("T.t1.x.inbox"));
        Session t1x = client(b, ca, "t1.x|rgA|b");
        t1x.createProducer(t1x.createQueue("T.t1.x.inbox")).send(t1x.createTextMessage("tenant-t1.x-secret"));
        List<String> got = drain(c);
        System.out.println("tenant t1 read tenant t1.x's namespaced queue: " + got + " ; TenantNamespace.tenantOf(T.t1.x.inbox)="
                + io.cresco.library.security.TenantNamespace.tenantOf("T.t1.x.inbox"));
        assertTrue(got.isEmpty(), "tenant t1 read tenant t1.x's queue (T.<tenant>. prefix with a '.'-bearing tenant name)");
    }

    /** Advisories that carry no Message (ConsumerInfo, ConnectionInfo...) reach every tenant, selectors and identities included. */
    @Test
    void anotherTenantsConsumersAreNotVisibleInAdvisories() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService b = broker(ca, false);
        Session t2 = client(b, ca, "t2|rgB|watcher");
        MessageConsumer adv = t2.createConsumer(t2.createTopic("ActiveMQ.Advisory.Consumer.Queue.T.t1.>"));
        Session t1 = client(b, ca, "t1|rgA|a1");
        t1.createConsumer(t1.createQueue("T.t1.payroll"), "gfs_to='rgA_a1' AND stream='q3-salaries'");
        Message m = adv.receive(3000);
        String seen = null;
        if (m instanceof ActiveMQMessage && ((ActiveMQMessage) m).getDataStructure() instanceof ConsumerInfo) {
            ConsumerInfo ci = (ConsumerInfo) ((ActiveMQMessage) m).getDataStructure();
            seen = ci.getDestination() + " selector=" + ci.getSelector();
        }
        System.out.println("tenant t2 sees tenant t1's consumer via advisory: " + seen);
        assertNull(seen, "tenant t2 learned tenant t1's destination and selector from a ConsumerInfo advisory");
    }
}
