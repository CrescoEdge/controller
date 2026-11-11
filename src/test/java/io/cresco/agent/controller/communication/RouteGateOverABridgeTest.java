package io.cresco.agent.controller.communication;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The route gate fed by real brokers over a real duplex bridge ({@link BridgedFabric}), not by hand: a bridge's demand
 * reaches the gate on both of its sides (the side that opened the bridge takes it through an in-JVM connection, the other
 * through the network connection), it leaves the gate when the bridge goes, and the broker's own account
 * ({@link ActiveBroker#queueHasConsumer}) says the same.
 */
class RouteGateOverABridgeTest {

    private final BridgedFabric f = new BridgedFabric();

    @BeforeAll
    static void pki() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) BridgedFabric.ca("t1", r);
    }

    @AfterEach
    void stop() {
        f.close();
    }

    private boolean route(String region, String queue) {
        return f.gates.get(region).noRouteFrom(queue, System.currentTimeMillis()) == RouteGate.ROUTE;
    }

    @Test
    void aBridgesDemandReachesTheGateOnBothSidesAndLeavesItWithTheBridge() throws Exception {
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        NetworkConnector bridge = f.bridge("rgA", "rgG");                    // rgA opens it; rgG accepts it
        Session a = f.node("rgA"), g = f.node("rgG");
        MessageConsumer leafA = a.createConsumer(a.createQueue("T.t1.rgA_n1"));
        MessageConsumer leafG = g.createConsumer(g.createQueue("T.t1.rgG_n1"));
        // rgA's controller reads its inbox in every tenant's namespace, on its own broker
        Connection own = new ActiveMQConnectionFactory("vm://rgA_rc?create=false").createConnection();
        f.connections.add(own);
        own.start();
        Session vm = own.createSession(false, Session.AUTO_ACKNOWLEDGE);
        vm.createConsumer(vm.createQueue("T.*.rgA_rc"));
        assertNotNull(leafA);
        assertNotNull(leafG);
        BridgedFabric.await(() -> route("rgG", "T.t1.rgA_n1") && route("rgA", "T.t1.rgG_n1") && route("rgG", "T.t1.rgA_rc"), 8000,
                "the demand of a bridge did not reach the gate: at the global for rgA's leaf " + route("rgG", "T.t1.rgA_n1")
                        + ", for rgA's controller " + route("rgG", "T.t1.rgA_rc") + "; at the region for the global's leaf " + route("rgA", "T.t1.rgG_n1"));
        assertTrue(route("rgG", "T.t9.rgA_rc"), "a controller's inbox is consumed in every tenant's namespace, across the bridge too");
        assertFalse(route("rgG", "T.t1.rgA_n2"), "a node that consumes nothing has a route");
        assertTrue(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t1.rgA_n1"), "the broker's own account");
        assertTrue(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t1.rgA_rc"), "a queue made for the question is given the controller's subscription");
        assertFalse(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t1.rgA_n2"));
        assertEquals(0, f.gates.get("rgG").unreadPatterns());

        long stopped = System.currentTimeMillis();
        bridge.stop();
        BridgedFabric.await(() -> !route("rgG", "T.t1.rgA_n1") && !route("rgA", "T.t1.rgG_n1") && !route("rgG", "T.t1.rgA_rc"), 8000,
                "the bridge is gone and its demand is still a route: at the global " + route("rgG", "T.t1.rgA_n1")
                        + ", at the region " + route("rgA", "T.t1.rgG_n1"));
        long known = f.gates.get("rgG").noRouteFrom("T.t1.rgA_n1", System.currentTimeMillis());
        assertTrue(known >= stopped + 1500 && known <= System.currentTimeMillis() + 1500, "the grace counts from the loss the gate saw: " + (known - stopped));
        assertFalse(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t1.rgA_n1"), "the broker's own account after the bridge went");
        assertTrue(route("rgA", "T.t1.rgA_n1"), "the leaf still consumes at its own region");
    }

    @Test
    void aTenantsPatternConsumerStopsTheGateForItsOwnTenantsInboxesOnlyAndIsSaid() throws Exception {
        // a tenant's node may consume `T.<its tenant>.>` today (that it may is another defect); the gate cannot count what
        // such a consumer takes, so it does not judge the queues the pattern can match -- those only -- and says so
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        f.bridge("rgA", "rgG");
        assertFalse(route("rgA", "T.t1.rgZ_n9"));
        Session a = f.node("rgA");
        a.createConsumer(a.createQueue("T.t1.>"));
        BridgedFabric.await(() -> f.gates.get("rgA").unreadPatterns() == 1 && f.gates.get("rgG").unreadPatterns() == 1, 8000,
                "the pattern consumer (and its demand at the global) is not in the gauge: region " + f.gates.get("rgA").unreadPatterns()
                        + ", global " + f.gates.get("rgG").unreadPatterns());
        for (String region : new String[]{"rgA", "rgG"}) {
            assertTrue(route(region, "T.t1.rgZ_n9"), "at " + region + ": a request the pattern consumer may take was judged");
            assertFalse(route(region, "T.t2.rgZ_n9"), "at " + region + ": tenant t1's pattern switched the gate off for another tenant's inboxes");
            assertFalse(route(region, "rgZ_n9"), "at " + region + ": ... and for inboxes outside any tenant's namespace");
            assertTrue(f.warnings.get(region).stream().anyMatch(w -> w.contains("queue pattern 'T.t1.>'")), "no WARN at " + region + ": " + f.warnings.get(region));
        }
        // the broker's own account agrees: the queue, once it exists, has the pattern's subscription
        assertTrue(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t1.rgZ_n9"));
        assertFalse(ActiveBroker.queueHasConsumer(f.byRegion.get("rgG"), "T.t2.rgZ_n9"));
    }
}
