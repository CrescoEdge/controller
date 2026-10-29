package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Responders are not chosen by an unauthenticated, self-reported agent count (third review, finding 2). */
class DiscoveryCandidateOrderTest {

    private static DiscoveryNode node(String region, String agent, int count) {
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.AGENT);
        n.discovered_region = region; n.discovered_agent = agent; n.discovered_agent_count = count;
        return n;
    }

    @Test
    void aNegativeOrAbsurdCountIsTriedLastAndTheJoinedControllerFirst() {
        DiscoveryNode liar = node("rgA", "evil", -1), huge = node("rgA", "big", Integer.MAX_VALUE - 1);
        DiscoveryNode busy = node("rgA", "rc2", 50), quiet = node("rgA", "rc3", 3), joined = node("rgA", "rc", 90);
        for (int i = 0; i < 20; i++) {
            List<DiscoveryNode> o = ControllerSMHandler.orderCandidates(List.of(liar, busy, huge, quiet, joined), Set.of());
            assertEquals(quiet, o.get(0), "a lower plausible count is preferred");
            assertTrue(o.indexOf(liar) >= 3 && o.indexOf(huge) >= 3, "a negative or absurd count was preferred: " + o.indexOf(liar));
            List<DiscoveryNode> j = ControllerSMHandler.orderCandidates(List.of(liar, busy, huge, quiet, joined), Set.of("rgA_rc"));
            assertEquals(joined, j.get(0), "the controller this node joined before is not tried first");
        }
    }
}
