package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * OUT-28 A-HOSTNAME: an agent's connection to its regional broker must reach the controller discovery returned,
 * not merely any holder of a certificate the agent trusts. Real agent code path: CertificateManager + ActiveClient
 * (the factory, failover URI and trust material the agent uses), real TLS, a real broker.
 *
 * <p>Before the fix the first test FAILED: the connection to a broker presenting another fabric node's valid
 * certificate (issued by the same regional CA) was accepted, because the trust managers only checked the chain.
 */
class PeerImpersonationTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id regionalController;  // rgA/rc: the broker the agent meant to reach
    private static TlsTestSupport.Id otherNode;           // rgA/a9: another node with a valid certificate from the same CA

    private final List<BrokerService> brokers = new ArrayList<>();

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        regionalController = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        otherNode = TlsTestSupport.issued(ca, "t1", "rgA", "a9");
    }

    @AfterEach
    void stop() throws Exception {
        for (BrokerService b : brokers) b.stop();
    }

    private ActiveMQSession connect(ControllerEngine agent, BrokerService broker) throws Exception {
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(broker));
        // what ControllerSMHandler.initIOChannels records for the controller discovery certified
        agent.getActiveClient().expectPeer(uri, "rgA", "rc");
        return agent.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true);
    }

    @Test
    void agentRefusesABrokerPresentingAnotherNodesValidCertificate() throws Exception {
        BrokerService impostor = TlsTestSupport.broker("impostor", otherNode, false, null, null, ca.caCert());
        brokers.add(impostor);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        TlsTestSupport.enroll(agent, ca, "t1");

        ActiveMQSession s = connect(agent, impostor);
        assertNull(s, "the agent connected to a broker whose certificate names rgA/a9 while it meant to reach rgA/rc");
    }

    @Test
    void agentConnectsToTheBrokerItMeantToReach() throws Exception {
        BrokerService real = TlsTestSupport.broker("rgA_rc", regionalController, false, null, null, ca.caCert());
        brokers.add(real);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        TlsTestSupport.enroll(agent, ca, "t1");

        ActiveMQSession s = connect(agent, real);
        assertNotNull(s, "the agent could not reach its own regional broker");
        Queue q = s.createQueue("rgA_a1");
        MessageConsumer c = s.createConsumer(q);
        s.createProducer(q).send(s.createTextMessage("hello"));
        TextMessage m = (TextMessage) c.receive(5000);
        assertNotNull(m);
        assertEquals("hello", m.getText());
        s.getConnection().close();
    }
}
