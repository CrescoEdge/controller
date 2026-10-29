package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A peer that restarted on a new dynamic broker port is dialled on the new port. */
class BrokeredAgentRefreshTest {

    private static DiscoveryNode node(String ip, int brokerPort) {
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.REGION);
        n.discovered_region = "rgB";
        n.discovered_agent = "rc";
        n.discovered_ip = ip;
        n.discovered_broker_port = brokerPort;
        return n;
    }

    @Test
    void aRediscoveredPeersNewBrokerPortIsUsed() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "rc", null);
        BrokeredAgent ba = new BrokeredAgent(e, node("10.0.0.5", 32020));
        ba.refresh(node("10.0.0.6", 32041));
        assertEquals(32041, ba.getBrokerPort(), "the stale broker port was kept after the peer restarted on a new one");
        assertEquals("10.0.0.6", ba.getActiveAddress());
        ba.refresh(node("10.0.0.7", -1));                    // a peer that advertises no port keeps the default behaviour
        assertEquals(-1, ba.getBrokerPort());
    }
}
