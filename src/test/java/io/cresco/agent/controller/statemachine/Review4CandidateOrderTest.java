package io.cresco.agent.controller.statemachine;

import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fourth review (local only). The locator reply (region, agent, agent count) is unauthenticated and replayable. Round 3's
 * fix moved a NEGATIVE count last; the ordering is still decided by what the responder says about itself.
 */
class Review4CandidateOrderTest {

    private static DiscoveryNode node(String ip, String region, String agent, int count) {
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.AGENT);
        n.discovered_ip = ip; n.discovered_region = region; n.discovered_agent = agent; n.discovered_agent_count = count;
        return n;
    }

    /** First join: a member that answers with count 0 is tried before every real controller that has agents. */
    @Test
    void anUnauthenticatedCountOfZeroDoesNotPutAnImpostorFirst() {
        DiscoveryNode real = node("10.0.0.1", "rgA", "rc", 12), impostor = node("10.6.6.6", "rgA", "rc9", 0);
        int first = 0;
        for (int i = 0; i < 50; i++) if (ControllerSMHandler.orderCandidates(List.of(real, impostor), Set.of()).get(0) == impostor) first++;
        System.out.println("first join: impostor reporting 0 agents tried first in " + first + "/50 orderings");
        assertEquals(0, first, "a responder that claims 0 agents is still tried before the real controller");
    }

    /** Rejoin: the impostor's locator reply CLAIMS the joined controller's name (unauthenticated) and wins on its count. */
    @Test
    void claimingTheJoinedControllersNameInALocatorReplyDoesNotWin() {
        DiscoveryNode real = node("10.0.0.1", "rgA", "rc", 12), impostor = node("10.6.6.6", "rgA", "rc", 0);
        int first = 0;
        for (int i = 0; i < 50; i++) if (ControllerSMHandler.orderCandidates(List.of(real, impostor), Set.of("rgA_rc")).get(0) == impostor) first++;
        System.out.println("rejoin: impostor claiming rgA_rc with 0 agents tried first in " + first + "/50 orderings");
        assertEquals(0, first, "an unauthenticated locator reply naming the joined controller is tried before the real one");
    }
}
