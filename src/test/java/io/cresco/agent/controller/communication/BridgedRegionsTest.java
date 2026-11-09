package io.cresco.agent.controller.communication;

import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * listregions on a global with a directly attached agent hung 30 s: the "bridged regions" were read from every consumed
 * queue split on '_', so the agent's tenant-namespaced inbox T.default.glob_a1 became a region "T.default.glob" with a
 * controller "a1", the global asked it for its agents, the agent could only bounce the request (it is not a relay; its
 * broker stripped the global's stamp it carried) and no answer ever came. A bridged region is a controller whose inbox a
 * BRIDGE consumes here (a network subscription), named past its tenant prefix.
 */
class BridgedRegionsTest {

    private BrokerService broker;
    private ActiveMQConnection c;

    @BeforeEach
    void start() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("glob_gc");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.start();
        broker.waitUntilStarted();
        c = (ActiveMQConnection) new ActiveMQConnectionFactory("vm://glob_gc?create=false").createConnection();
        c.start();
    }

    @AfterEach
    void stop() throws Exception {
        try { c.close(); } catch (Exception ignore) { }
        broker.stop();
        broker.waitUntilStopped();
    }

    /** A consumer on {@code queue} as a broker bridge makes one (a network subscription carrying remote demand). */
    private void bridgeConsumer(org.apache.activemq.command.SessionId sid, String queue, long id) throws Exception {
        ConsumerInfo info = new ConsumerInfo(new ConsumerId(sid, id));
        info.setDestination(new ActiveMQQueue(queue));
        info.setNetworkSubscription(true);
        info.setPrefetchSize(1);
        c.syncSendPacket(info);
    }

    @Test
    void aDirectlyAttachedAgentIsNotARegionAndANamespacedInboxIsReadPastItsPrefix() throws Exception {
        ActiveMQSession s = (ActiveMQSession) c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        s.createConsumer(s.createQueue("T.default.glob_a1"));          // the attached agent's own inbox
        s.createConsumer(s.createQueue("glob_a2"));                    // an earlier build's raw inbox
        assertEquals(List.of(), ActiveBroker.bridgedRegionsOf(broker.getBroker()), "an attached agent was listed as a bridged region");

        org.apache.activemq.command.SessionId sid = new org.apache.activemq.command.SessionId(c.getConnectionInfo().getConnectionId(), 4242);
        c.syncSendPacket(new org.apache.activemq.command.SessionInfo(sid));
        bridgeConsumer(sid, "T.default.rgr_rc", 1001);                  // a region's controller, behind a bridge
        bridgeConsumer(sid, "rgs_rc2", 1002);                           // raw, as an earlier build bridges
        bridgeConsumer(sid, "T.default.not_a_valid_node", 1003);        // three parts: no node
        List<Map<String, String>> got = ActiveBroker.bridgedRegionsOf(broker.getBroker());
        assertEquals(2, got.size(), String.valueOf(got));
        assertTrue(got.contains(Map.of("region_id", "rgr", "agent_id", "rc")), String.valueOf(got));
        assertTrue(got.contains(Map.of("region_id", "rgs", "agent_id", "rc2")), String.valueOf(got));
    }

    @Test
    void inboxNamesAreReadStrictly() {
        assertEquals("rgr_rc", ActiveBroker.bridgedInboxPath("T.default.rgr_rc"));
        assertEquals("rgr_rc", ActiveBroker.bridgedInboxPath("rgr_rc"));
        assertEquals("glob.x_a1", ActiveBroker.bridgedInboxPath("T.default.glob.x_a1"), "a region name may have a '.'");
        assertNull(ActiveBroker.bridgedInboxPath("T.default.a_b_c"), "three parts are no node");
        assertNull(ActiveBroker.bridgedInboxPath("T.*.glob_gc"), "the wildcard inbox");
        assertNull(ActiveBroker.bridgedInboxPath("global.event"));
        assertNull(ActiveBroker.bridgedInboxPath(null));
    }
}
