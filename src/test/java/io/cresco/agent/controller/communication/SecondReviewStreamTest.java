package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
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
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Second adversarial review (stream recipient binding). broker_security_enabled=true with the DEFAULTS:
 * broker_stream_recipient_binding=true and broker_require_client_auth=false. Each test asserts the SECURE outcome.
 */
class SecondReviewStreamTest {

    private static final String TOPIC = "global.event";
    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private BrokerService broker(RegionCA ca, TlsTestSupport.Id id, boolean needClientAuth) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");                     // nothing else: defaults
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz2").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_gc2", id, needClientAuth, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
        return b;
    }

    /** A TLS client with NO certificate (server-auth only, as with the default broker_require_client_auth=false). */
    private Session anonymous(BrokerService b, RegionCA ca, String user) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(null, TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        f.setUserName(user);
        f.setPassword("anything");                                       // never checked
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static List<String> drain(MessageConsumer c) throws Exception {
        List<String> got = new ArrayList<>();
        Message m = c.receive(3000);
        while (m != null) { got.add(((TextMessage) m).getText()); m = c.receive(500); }
        return got;
    }

    @Test
    void withDefaultsAStrangerCannotReceiveAnAgentsRecipientBoundStream() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService g = broker(ca, TlsTestSupport.issued(ca, "t1", "rgA", "gc"), false);
        // no fabric certificate, no discovery secret: just reaches the broker port and types the victim's name
        Session stranger = anonymous(g, ca, "t1|rgA|a1");
        MessageConsumer spy = stranger.createConsumer(stranger.createTopic(TOPIC));
        Session sender = anonymous(g, ca, "t1|rgA|s1");
        MessageProducer p = sender.createProducer(sender.createTopic(TOPIC));
        TextMessage m = sender.createTextMessage("secret-for-a1");
        m.setStringProperty(StreamRecipientPolicy.TO, "rgA_a1");
        p.send(m);
        List<String> got = drain(spy);
        System.out.println("certificate-less client asserting t1|rgA|a1 received: " + got);
        assertTrue(got.isEmpty(), "a certificate-less client that merely asserted the username t1|rgA|a1 received a1's recipient-bound stream");
    }

    @Test
    void anInJvmConnectionCannotNameItselfAnyIdentity() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService g = broker(ca, TlsTestSupport.issued(ca, "t1", "rgA", "gc"), true);
        org.apache.activemq.ActiveMQConnectionFactory f = new org.apache.activemq.ActiveMQConnectionFactory("vm://" + g.getBrokerName() + "?create=false");
        f.setUserName(CrescoAuthorizationBroker.LOCAL_CONTROLLER_PREFIX + "t1|rgZ|someone-else");   // not this controller
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer spy = s.createConsumer(s.createTopic(TOPIC), StreamRecipientPolicy.TO + "='rgZ_someone-else'");
        MessageProducer p = s.createProducer(s.createTopic(TOPIC));
        TextMessage m = s.createTextMessage("for-rgZ");
        m.setStringProperty(StreamRecipientPolicy.TO, "rgZ_someone-else");
        p.send(m);
        List<String> got = drain(spy);
        System.out.println("vm:// connection claiming cresco-local|t1|rgZ|someone-else received: " + got);
        assertTrue(got.isEmpty(), "an in-JVM connection took an identity that is not its controller's (rgZ/someone-else)");
    }

    /**
     * broker_discard_accounting (default true) turns on advisoryForDiscardingMessages: when the recipient is slow, a
     * copy of each discarded recipient-bound message (properties intact, body cleared) is published on an advisory
     * topic that TenantPolicy opens to every client, of every tenant.
     */
    @Test
    void aDiscardedRecipientBoundMessageDoesNotReachOtherTenantsViaAdvisories() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        RegionCA caOther = RegionCA.generate("t2", "rgC", 2048, 1);
        TlsTestSupport.Id gc = TlsTestSupport.issued(ca, "t1", "rgA", "gc"), a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1"),
                s1 = TlsTestSupport.issued(ca, "t1", "rgA", "s1"), other = TlsTestSupport.issued(caOther, "t2", "rgC", "x9");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz3").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caOther.caCert(), "rgC", "t2", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        org.apache.activemq.broker.SslBrokerService b = new org.apache.activemq.broker.SslBrokerService();
        b.setBrokerName("rgA_gc3"); b.setPersistent(false); b.setUseJmx(false); b.setUseShutdownHook(false);
        b.setSslContext(new org.apache.activemq.broker.SslContext(gc.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caOther.caCert()), new SecureRandom()));
        b.setPlugins(new BrokerPlugin[]{authz});
        b.setMessageAuthorizationPolicy(authz);
        org.apache.activemq.broker.region.policy.PolicyEntry e = new org.apache.activemq.broker.region.policy.PolicyEntry();
        e.setTopic(">");
        e.setAdvisoryForDiscardingMessages(true);                         // what broker_discard_accounting=true sets
        org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy lim = new org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy();
        lim.setLimit(1);
        e.setPendingMessageLimitStrategy(lim);
        org.apache.activemq.broker.region.policy.PolicyMap pm = new org.apache.activemq.broker.region.policy.PolicyMap();
        pm.setDefaultEntry(e);
        b.setDestinationPolicy(pm);
        b.addConnector("nio+ssl://127.0.0.1:0?needClientAuth=true").setName("tls");
        b.start(); b.waitUntilStarted();
        brokers.add(b);
        String url = "nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false&jms.prefetchPolicy.topicPrefetch=1";
        java.util.function.Function<TlsTestSupport.Id, Session> client = id -> {
            try {
                ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory(url);
                f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert(), caOther.caCert()), new SecureRandom());
                Connection c = f.createConnection(); connections.add(c); c.start();
                return c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            } catch (Exception ex) { throw new RuntimeException(ex); }
        };
        Session spy = client.apply(other);                                // tenant t2, region rgC
        MessageConsumer adv = spy.createConsumer(spy.createTopic("ActiveMQ.Advisory.MessageDiscarded.Topic." + TOPIC));
        Session slow = client.apply(a1);
        slow.createConsumer(slow.createTopic(TOPIC));                     // the recipient, momentarily slow (never reads)
        Session tx = client.apply(s1);
        MessageProducer p = tx.createProducer(tx.createTopic(TOPIC));
        for (int i = 0; i < 10; i++) {
            TextMessage m = tx.createTextMessage("body-" + i);
            m.setStringProperty(StreamRecipientPolicy.TO, "rgA_a1");
            m.setStringProperty("stream_name", "gkt-private-stream");
            m.setStringProperty("gfs_file", "patient-0042.dcm");
            p.send(m);
        }
        List<String> leaked = new ArrayList<>();
        Message m = adv.receive(3000);
        while (m != null) {
            Object ds = ((org.apache.activemq.command.ActiveMQMessage) m).getDataStructure();
            if (ds instanceof org.apache.activemq.command.Message) {
                org.apache.activemq.command.Message orig = (org.apache.activemq.command.Message) ds;
                leaked.add(orig.getProperty(StreamRecipientPolicy.TO) + "/" + orig.getProperty("stream_name") + "/" + orig.getProperty("gfs_file"));
            }
            m = adv.receive(500);
        }
        System.out.println("tenant t2 client received " + leaked.size() + " discard advisories carrying: " + (leaked.isEmpty() ? "-" : leaked.get(0)));
        assertTrue(leaked.isEmpty(), "another tenant read the properties of messages bound to rgA_a1 from MessageDiscarded advisories");
    }

    /** Docs: peer_identity_mode=warn is "log and accept". Inbound, an unbound certificate loses its identity instead. */
    @Test
    void warnModeAcceptsAnUnboundClientIdentityInbound() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        RegionCA caD = RegionCA.generate("t1", "rgD", 2048, 1);              // PKIX-trusted, not (yet) recorded
        TlsTestSupport.Id gc = TlsTestSupport.issued(ca, "t1", "rgA", "gc"), a5 = TlsTestSupport.issued(caD, "t1", "rgD", "a5");
        Map<String, String> verdict = new java.util.LinkedHashMap<>();
        for (String mode : new String[]{"off", "warn"}) {
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("broker_security_enabled", "true");
            cfg.put(PeerIdentity.MODE_KEY, mode);
            PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz4").toString(), cfg);
            PeerTrustRegistry peers = new PeerTrustRegistry();
            assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
            CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
            BrokerService b = TlsTestSupport.broker("rgA_gc4" + mode, gc, true, new BrokerPlugin[]{authz}, authz, ca.caCert(), caD.caCert());
            brokers.add(b);
            ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
            f.setKeyAndTrustManagers(a5.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
            Connection c = f.createConnection(); connections.add(c); c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try { s.createConsumer(s.createQueue("rgD_a5")); verdict.put(mode, "allowed"); }
            catch (Exception e) { verdict.put(mode, "DENIED: " + e.getMessage()); }
        }
        System.out.println("own inbox rgD_a5 for an unbound but PKIX-trusted client: " + verdict);
        assertEquals("allowed", verdict.get("warn"), "peer_identity_mode=warn refused a client that off accepts (warn is not log-and-accept inbound)");
    }
}
