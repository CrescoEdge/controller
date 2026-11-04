package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent review of the identity phases 0-2 (2026-11-04), the broker findings fixed in phase 3: composite
 * destinations and the broker's own destinations. Copied from ReviewIdentityTest on review/identity-20261104.
 */
class ReviewIdentityBrokerTest {

    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) try { b.stop(); } catch (Exception ignore) { }
    }

    private Connection connect(BrokerService b, TlsTestSupport.Id id, X509Certificate... anchors) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c;
    }

    /**
     * H2. enforce() (addConsumer / addProducer / send) judges a COMPOSITE destination by its whole physical name
     * ("T.t1.x,T.t2.global.event" starts with T.t1.), and addDestination accepts each component the identity may READ OR
     * WRITE. With the documented GFS sink '*->T.*.global.event:write' (WRITE only: "a rule never grants READ unless it says
     * so"), tenant t1 subscribes to a composite whose second half is tenant t2's dataplane, and reads it.
     */
    @Test
    void aCompositeDestinationCannotReadAnotherTenantsDataplane() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1), caB = RegionCA.generate("t2", "rgB", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t2", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        cfg.put("broker_cross_tenant_sinks", "*->T.*.global.event:write");
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("rvid-h2").toString(), cfg), peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert(), caB.caCert());
        brokers.add(b);
        Connection thief = connect(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), caA.caCert());
        Connection victim = connect(b, TlsTestSupport.issued(caB, "t2", "rgB", "b1"), caA.caCert());
        Session direct = thief.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSException.class, () -> direct.createConsumer(new ActiveMQTopic("T.t2.global.event")),
                "precondition: t1 may not read t2's dataplane directly");
        Session ts = thief.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c;
        try {
            c = ts.createConsumer(new ActiveMQTopic("T.t1.x,T.t2.global.event"));
        } catch (JMSException refused) {
            System.out.println("H2 composite: refused (" + refused.getMessage() + ")");
            return;                                                                  // secure: the composite was refused
        }
        Session vs = victim.createSession(false, Session.AUTO_ACKNOWLEDGE);
        vs.createProducer(new ActiveMQTopic("T.t2.global.event")).send(vs.createTextMessage("tenant t2 data"));
        Message got = c.receive(3000);
        System.out.println("H2 composite: tenant t1's consumer on 'T.t1.x,T.t2.global.event' received " + got);
        assertNull(got, "tenant t1 read tenant t2's dataplane through a composite destination");
    }

    /**
     * TenantPolicy's legacy rule (dest == tenant or starts with tenant + ".") still applies with namespacing on, and tenant
     * names are not checked against the broker's own namespaces. A federating region whose tenant is named "ActiveMQ"
     * (valid: letters only; security_federated_tenants defaults to any) may READ, WRITE and administer ActiveMQ.DLQ -- the
     * broker's shared dead-letter queue, holding every tenant's undeliverable persistent messages.
     */
    @Test
    void aTenantNameCannotClaimTheBrokersOwnDestinations() {
        assertTrue(CrescoNames.isValidTenant("ActiveMQ"), "precondition: 'ActiveMQ' is a valid tenant name");
        CrescoIdentity id = CrescoIdentity.of("ActiveMQ", "rgX", "x1", null);
        TenantPolicy.Decision d = TenantPolicy.check(id, "ActiveMQ.DLQ", TenantPolicy.Access.READ, Set.of("agent.event", "region.event"),
                TenantPolicy.Role.TENANT, List.of(), n -> null);
        System.out.println("tenant 'ActiveMQ' READ ActiveMQ.DLQ -> " + d.allowed + " (" + d.reason + ")");
        assertFalse(d.allowed, "a tenant named 'ActiveMQ' may read the broker's shared dead-letter queue: " + d.reason);
    }

    /**
     * H2, default configuration (tenant_namespacing is OFF by default at ea6a98e). TenantPolicy: a same-region peer inbox
     * "may [be sent] to -- never read". A composite whose FIRST component passes enforce() (a fabric-shared name such as
     * agent.event.x, or the tenant's own legacy prefix) and whose second is a peer's inbox passes addDestination (WRITE to
     * the peer inbox is allowed, D2b) -- and the consumer then competes on that inbox: any agent takes another agent's
     * control messages (RPC requests and replies included) off its queue.
     */
    @Test
    void aCompositeDestinationCannotConsumeAPeersInbox() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        TlsTestSupport.Id a1 = TlsTestSupport.issued(caA, "t1", "rgA", "a1"), a2 = TlsTestSupport.issued(caA, "t1", "rgA", "a2"),
                a3 = TlsTestSupport.issued(caA, "t1", "rgA", "a3");
        for (TlsTestSupport.Id id : List.of(a1, a2, a3)) {
            CrescoIdentity n = CrescoNames.identityOf(id.chain[0]);
            assertTrue(peers.bind("t1", "rgA", n.getAgent(), id.kp.getPublic(), "enrolled").ok);
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("rvid-h2b").toString(), cfg), peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert());
        brokers.add(b);
        Connection thief = connect(b, a1, caA.caCert()), sender = connect(b, a3, caA.caCert());
        Session direct = thief.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSException.class, () -> direct.createConsumer(new org.apache.activemq.command.ActiveMQQueue("rgA_a2")),
                "precondition: a peer inbox may not be read directly");
        Session ts = thief.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c;
        try {
            c = ts.createConsumer(new org.apache.activemq.command.ActiveMQQueue("agent.event.x,rgA_a2"));
        } catch (JMSException refused) {
            System.out.println("H2 composite inbox: refused (" + refused.getMessage() + ")");
            return;
        }
        Session ss = sender.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ss.createProducer(new org.apache.activemq.command.ActiveMQQueue("rgA_a2")).send(ss.createTextMessage("for a2 only"));
        Message got = c.receive(3000);
        System.out.println("H2 composite inbox: a1's consumer on 'agent.event.x,rgA_a2' received " + (got == null ? null : ((jakarta.jms.TextMessage) got).getText()));
        assertNull(got, "agent a1 consumed a message from peer a2's inbox through a composite destination");
    }

    @Test
    void onlyTheBrokerWritesAdvisories() {
        CrescoIdentity id = CrescoIdentity.of("t1", "rgA", "a1", null);
        assertTrue(TenantPolicy.check(id, "ActiveMQ.Advisory.Consumer.Queue.rgA_a1", TenantPolicy.Access.READ, Set.of(),
                TenantPolicy.Role.TENANT, List.of(), n -> null).allowed);
        assertFalse(TenantPolicy.check(id, "ActiveMQ.Advisory.Consumer.Queue.rgA_a1", TenantPolicy.Access.WRITE, Set.of(),
                TenantPolicy.Role.TENANT, List.of(), n -> null).allowed, "a tenant forged an advisory");
    }
}
