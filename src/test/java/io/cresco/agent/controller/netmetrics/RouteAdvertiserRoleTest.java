package io.cresco.agent.controller.netmetrics;

import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Link state is advertised by controllers only. With every agent publishing every 5 s on the fabric-wide topic and
 * subscribed to every other node's, deliveries grew with the square of the fleet; an agent's one link (to its region
 * controller) is that controller's to advertise.
 */
class RouteAdvertiserRoleTest {

    @Test
    void anAgentNeitherPublishesNorSubscribesAndAControllerDoes() throws Exception {
        ControllerEngine agent = TlsTestSupport.engine("r1", "a1", new HashMap<>());
        RouteAdvertiser a = new RouteAdvertiser(agent, new RouteView(20_000L), 5_000L);
        assertFalse(a.advertises(), "an agent advertises link state");
        a.tick();                                                        // returns before it touches the dataplane

        ControllerEngine region = TlsTestSupport.engine("r1", "rc", new HashMap<>());
        ((TlsTestSupport.State) region.cstate).regional = true;
        assertTrue(new RouteAdvertiser(region, new RouteView(20_000L), 5_000L).advertises(), "a region controller does not advertise");
    }
}
