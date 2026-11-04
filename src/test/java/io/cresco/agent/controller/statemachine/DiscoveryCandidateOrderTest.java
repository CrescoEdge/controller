package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Responders are never ordered by what they say about themselves (third review, finding 2; tightened by the fourth review,
 * M5). The third review's version of this test preferred the lowest self-reported agent count and a responder claiming the
 * joined controller's name; the fourth review showed an impostor then always goes first by claiming 0 or that name. Now:
 * arrival order, except an address this node already authenticated is tried first.
 */
class DiscoveryCandidateOrderTest {

    private static DiscoveryNode node(String ip, String region, String agent, int count) {
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.AGENT);
        n.discovered_ip = ip; n.discovered_region = region; n.discovered_agent = agent; n.discovered_agent_count = count;
        return n;
    }

    @Test
    void selfReportedCountsAndNamesDoNotOrderResponders() {
        DiscoveryNode liar = node("10.0.0.9", "rgA", "evil", -1), huge = node("10.0.0.8", "rgA", "big", Integer.MAX_VALUE - 1);
        DiscoveryNode busy = node("10.0.0.2", "rgA", "rc2", 50), quiet = node("10.0.0.3", "rgA", "rc3", 0), joined = node("10.0.0.1", "rgA", "rc", 90);
        List<DiscoveryNode> arrival = List.of(busy, liar, joined, huge, quiet);
        for (int i = 0; i < 20; i++) {
            assertEquals(arrival, ControllerSMHandler.orderCandidates(arrival, Set.of()), "responders were reordered by what they reported");
            assertEquals(arrival, ControllerSMHandler.orderCandidates(arrival, Set.of("rgA_rc")), "a claimed name reordered responders");
        }
    }

    @Test
    void anAddressAuthenticatedBeforeIsTriedFirst() {
        DiscoveryNode impostor = node("10.6.6.6", "rgA", "rc", 0), joined = node("10.0.0.1", "rgA", "rc", 90);
        List<DiscoveryNode> o = ControllerSMHandler.orderCandidates(List.of(impostor, joined), Set.of("10.0.0.1"));
        assertEquals(joined, o.get(0), "the controller this node authenticated before is not tried first");
        assertEquals(impostor, o.get(1));
    }
}
