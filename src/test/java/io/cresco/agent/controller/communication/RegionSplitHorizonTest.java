package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.cresco.agent.controller.communication.BridgedFabric.send;
import static io.cresco.agent.controller.communication.BridgedFabric.stampOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Which broker relays between bridges (split horizon, {@code broker_bridge_transit}), over real duplex bridges between
 * secured brokers ({@link BridgedFabric}).
 *
 * <p>Regions bridged to each other as well as to the global (a mesh) had every topic message arrive once over each path
 * of bridges, and a queue message take the way over another region whenever its own bridge was away. The broker at the
 * end strips the caller from what a region's bridge brings for another region's node, so those copies arrived without a
 * caller (fabric matrix, cell mesh: 150 to 300 stripped stamps a run; after a restart of the global the regions' watchdogs
 * went over another region and were refused as unsigned). A region of a fabric with a global does not relay; a fabric
 * without a global still does, as it must to carry anything past a neighbour.
 *
 * <p>The fabric: a global rgG/gc and the regions rgA, rgB, rgC (controller rc, a node n1 each) of tenant t1; for the last
 * test a global sgG of the superuser tenant with regions of two tenants.
 */
class RegionSplitHorizonTest {

    private final BridgedFabric f = new BridgedFabric();

    @BeforeAll
    static void pki() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) BridgedFabric.ca("t1", r);
        BridgedFabric.ca("cresco-system", "sgG");
        BridgedFabric.ca("t1", "taA");
        BridgedFabric.ca("t1", "taB");
        BridgedFabric.ca("t2", "tbC");
    }

    @AfterEach
    void stop() {
        f.close();
    }

    // ---- a fabric with a global: its regions relay nothing ----

    @Test
    void inAMeshEveryTopicMessageArrivesOnceWithItsCallerAndNoBridgeBringsAnotherRegionsStamp() throws Exception {
        f.start("rgG", null);
        for (String r : new String[]{"rgA", "rgB", "rgC"}) f.startUnder(r, "rgG", null);
        for (String r : new String[]{"rgA", "rgB", "rgC"}) f.bridge(r, "rgG");
        f.bridge("rgA", "rgB");
        f.bridge("rgA", "rgC");
        f.bridge("rgB", "rgC");
        Map<String, MessageConsumer> listeners = new HashMap<>();
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) {
            Session s = f.node(r);
            listeners.put(r, s.createConsumer(s.createTopic("t1.global.event")));
        }
        Thread.sleep(3000);                                                      // every bridge has its demand
        Session a = f.node("rgA");
        int sent = 20;
        for (int i = 0; i < sent; i++) send(a, a.createTopic("t1.global.event"), "advert-" + i, stampOf("rgA"));
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) {
            List<String> all = new ArrayList<>();
            Map<String, String> got = BridgedFabric.drain(listeners.get(r), sent, 1500L, all);
            assertEquals(sent, got.size(), "the listener of " + r + " was not given every message");
            assertEquals(sent, all.size(), "the listener of " + r + " was given a message twice");
            Set<String> callers = new HashSet<>(got.values());
            assertEquals(Set.of(stampOf("rgA")), callers, "at " + r + " a message arrived without its sender's stamp");
        }
        assertEquals(0, f.stripped(), "a bridge brought a stamp of another region's node (a second hop through a region)");
        assertEquals(0, f.authz.get("rgG").transitWithheldCount(), "the global relays");
        // rgB had each of rgA's messages twice over a bridge: from rgA itself (asked for by the bridges to rgG and to rgC) and
        // the copy the global relays (asked for by the bridge to rgC). Each copy that arrived is counted once -- 40 -- not once
        // per bridge it was kept from, which would be 60
        assertEquals(2 * sent, f.authz.get("rgB").transitWithheldCount(), "messages are counted, not the bridges they were kept from");
        assertEquals(2 * sent, f.authz.get("rgC").transitWithheldCount());
    }

    @Test
    void aQueueMessageWaitsForItsOwnBridgeInsteadOfCrossingAnotherRegion() throws Exception {
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        f.startUnder("rgB", "rgG", null);
        f.bridge("rgB", "rgG");                                                  // rgA's bridge to the global is not back yet
        f.bridge("rgA", "rgB");
        Session g = f.node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(3000);                                                      // the demand has gone as far as it goes
        assertTrue(f.byRegion.get("rgA").getDestination(new ActiveMQQueue("t1.rgG_gc")).getConsumers().isEmpty(),
                "rgB told rgA that it is a way to the global's inbox");
        assertTrue(f.authz.get("rgB").transitDemandWithheldCount() > 0, "the demand of the global's bridge was not kept from rgA's");
        Session a = f.node("rgA");
        send(a, a.createQueue("t1.rgG_gc"), "watchdog", stampOf("rgA"));
        assertNull(inbox.receive(2500), "rgA's message for the global went over rgB's bridge");
        f.bridge("rgA", "rgG");
        Message got = inbox.receive(10_000);
        assertNotNull(got, "the message did not arrive once rgA's own bridge was there");
        assertEquals(stampOf("rgA"), got.getStringProperty("cresco_caller"));
        assertEquals(0, f.stripped());
    }

    @Test
    void theGlobalStillRelaysBetweenItsRegions() throws Exception {
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        f.startUnder("rgB", "rgG", null);
        f.bridge("rgA", "rgG");
        f.bridge("rgB", "rgG");
        Session b = f.node("rgB");
        MessageConsumer inbox = b.createConsumer(b.createQueue("t1.rgB_n1"));
        MessageConsumer listener = f.node("rgB").createConsumer(b.createTopic("t1.global.event"));
        Thread.sleep(3000);
        Session a = f.node("rgA");
        send(a, a.createQueue("t1.rgB_n1"), "request", stampOf("rgA"));
        send(a, a.createTopic("t1.global.event"), "advert", stampOf("rgA"));
        Message q = inbox.receive(8000), t = listener.receive(8000);
        assertNotNull(q, "a queue message of rgA did not reach rgB over the global");
        assertNotNull(t, "a topic message of rgA did not reach rgB over the global");
        assertEquals(stampOf("rgA"), q.getStringProperty("cresco_caller"), "the global is rgB's parent: it carries the fabric");
        assertEquals(stampOf("rgA"), t.getStringProperty("cresco_caller"));
        assertEquals(0, f.stripped());
        assertTrue(f.authz.get("rgG").relaysBetweenBridges());
        assertFalse(f.authz.get("rgA").relaysBetweenBridges());
    }

    /**
     * The chain rgA -- rgB -- rgC (rgA and rgC are not bridged): a request and an advert of rgA's node for rgC's.
     * Returns what rgC's inbox and listener were given (null: nothing), in that order.
     */
    private Message[] acrossTheChain(String text) throws Exception {
        Session c = f.node("rgC");
        MessageConsumer inbox = c.createConsumer(c.createQueue("t1.rgC_n1"));
        MessageConsumer listener = f.node("rgC").createConsumer(c.createTopic("t1.global.event"));
        Thread.sleep(3500);
        Session a = f.node("rgA");
        send(a, a.createTopic("t1.global.event"), text, stampOf("rgA"));
        boolean routed = !f.byRegion.get("rgA").getDestination(new ActiveMQQueue("t1.rgC_n1")).getConsumers().isEmpty();
        if (routed) send(a, a.createQueue("t1.rgC_n1"), text, stampOf("rgA"), DeliveryMode.PERSISTENT, 0);
        Message q = routed ? inbox.receive(6000) : null, t = listener.receive(routed ? 6000 : 2500);
        inbox.close();
        listener.close();
        return new Message[]{q, t};
    }

    // ---- a fabric without a global: a region relays, or nothing gets past a neighbour ----

    @Test
    void inAFabricWithoutAGlobalARegionRelaysSoAChainStillCarries() throws Exception {
        for (String r : new String[]{"rgA", "rgB", "rgC"}) f.startAlone(r, null);
        f.bridge("rgA", "rgB");
        f.bridge("rgB", "rgC");
        assertTrue(f.authz.get("rgB").relaysBetweenBridges(), "a region with no global configured and none joined relays");
        Message[] got = acrossTheChain("over-rgB");
        assertNotNull(got[0], "a request of rgA's node for rgC's did not cross rgB: a fabric without a global carries nothing past a neighbour");
        assertNotNull(got[1], "an advert of rgA did not reach rgC: regions without a global no longer learn of each other");
        // as before this rule existed: what rgB's bridge brings for rgA's node has no caller at rgC (a signed message is still its sender's)
        assertEquals("", got[0].getStringProperty("cresco_caller"));
        assertEquals(0, f.authz.get("rgB").transitWithheldCount());
    }

    @Test
    void aRegionOfAFabricWithAGlobalDoesNotCarryItsNeighboursTrafficOn() throws Exception {
        // what a fabric with a global loses: a region is reached over its own bridges only (here rgA and rgC have none to
        // each other and the global is away), never through a peer region
        for (String r : new String[]{"rgA", "rgB", "rgC"}) f.startUnder(r, "rgG", null);
        f.bridge("rgA", "rgB");
        f.bridge("rgB", "rgC");
        assertFalse(f.authz.get("rgB").relaysBetweenBridges());
        Message[] got = acrossTheChain("not-over-rgB");
        assertNull(got[0], "rgB told rgA it is a way to rgC's node");
        assertNull(got[1], "rgB handed rgC's bridge an advert that rgA's bridge brought");
        // rgB did not even ask rgA for it: what rgC's bridge demands at rgB is not passed on to rgA's
        assertTrue(f.authz.get("rgB").transitDemandWithheldCount() >= 1);
    }

    @Test
    void aRegionThatHasKnownAGlobalNeverRelaysAgainAlsoWhileThatGlobalIsDown() throws Exception {
        for (String r : new String[]{"rgA", "rgB", "rgC"}) f.startAlone(r, null);
        AtomicBoolean joined = new AtomicBoolean();
        f.authz.get("rgB").setGlobalKnown(joined::get);                          // ControllerEngine.knewAGlobal, here by hand
        f.bridge("rgA", "rgB");
        f.bridge("rgB", "rgC");
        Session c = f.node("rgC");
        MessageConsumer listener = c.createConsumer(c.createTopic("t1.global.event"));
        Thread.sleep(3500);
        Session a = f.node("rgA");
        send(a, a.createTopic("t1.global.event"), "before", stampOf("rgA"));
        assertNotNull(listener.receive(6000), "a region that has no global did not relay");

        joined.set(true);                                                        // rgB begins to join a global it found
        send(a, a.createTopic("t1.global.event"), "with-a-global", stampOf("rgA"));
        assertNull(listener.receive(2500), "a region with a global relayed");
        assertFalse(f.authz.get("rgB").relaysBetweenBridges());

        joined.set(false);                                                       // (the engine never says no again; nor may the broker)
        send(a, a.createTopic("t1.global.event"), "global-down", stampOf("rgA"));
        assertNull(listener.receive(2500), "a region fell back to relaying when its global went away");
        assertFalse(f.authz.get("rgB").relaysBetweenBridges(), "once a region has had a global it does not relay until it restarts");
    }

    @Test
    void theSettingForcesItEitherWay() throws Exception {
        // false: a region that would relay (no global) does not
        f.startAlone("rgA", null);
        f.startAlone("rgB", Map.of(CrescoAuthorizationBroker.TRANSIT_KEY, "false"));
        f.startAlone("rgC", null);
        f.bridge("rgA", "rgB");
        f.bridge("rgB", "rgC");
        assertFalse(f.authz.get("rgB").relaysBetweenBridges());
        Message[] got = acrossTheChain("forced-off");
        assertNull(got[0]);
        assertNull(got[1], "broker_bridge_transit=false and the region relayed");
        // and what each value means on each kind of node
        assertTrue(relays(Map.of("is_global", "true")), "the global relays");
        assertFalse(relays(Map.of("is_global", "true", CrescoAuthorizationBroker.TRANSIT_KEY, "false")));
        assertFalse(relays(Map.of("global_controller_host", "10.0.0.1")), "a region configured with a global (static host)");
        assertFalse(relays(Map.of("global_controller_host", "10.0.0.1", "global_optional", "true")), "a configured global, optional or not");
        assertFalse(relays(Map.of()), "a region that finds its global by discovery and does not run without one");
        assertTrue(relays(Map.of("global_optional", "true")), "a region that runs on its own");
        assertTrue(relays(Map.of("global_controller_host", "10.0.0.1", CrescoAuthorizationBroker.TRANSIT_KEY, "true")));
        assertTrue(relays(Map.of("global_optional", "true", CrescoAuthorizationBroker.TRANSIT_KEY, "AUTO")));
        assertFalse(relays(Map.of(CrescoAuthorizationBroker.TRANSIT_KEY, "sometimes")), "a value that is none of the three is auto");
    }

    private static boolean relays(Map<String, Object> settings) throws Exception {
        Map<String, Object> cfg = new HashMap<>(settings);
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        return new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("transit").toString(), cfg),
                new PeerTrustRegistry()).relaysBetweenBridges();
    }

    // ---- a queue is never judged per message: what waits in a region's queue goes to whoever consumes it ----

    @Test
    void aBacklogFollowsANodeThatMovedToAnotherRegionAndNothingStarvesBehindIt() throws Exception {
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        f.startUnder("rgB", "rgG", null);
        f.bridge("rgA", "rgG");
        f.bridge("rgB", "rgA");
        Session a = f.node("rgA");
        MessageConsumer idle = a.createConsumer(a.createQueue("t1.rgA_n1?consumer.prefetchSize=1"));
        Thread.sleep(3000);
        Session g = f.node("rgG");
        int backlog = 260;                                                       // more than a page (200) of the queue
        for (int i = 0; i < backlog; i++) send(g, g.createQueue("t1.rgA_n1"), "from-global-" + i, stampOf("rgG"), DeliveryMode.PERSISTENT, 0);
        BridgedFabric.await(() -> {
            try { return f.depth("rgA", "t1.rgA_n1") >= backlog; } catch (Exception e) { return false; }
        }, 15_000, "the global's messages did not reach rgA");
        idle.close();                                                            // the node leaves rgA ...
        Session b = f.node("rgB", "n1");
        MessageConsumer moved = b.createConsumer(b.createQueue("t1.rgA_n1"));     // ... and consumes its former inbox at rgB
        Session a2 = f.node("rgA");
        send(a2, a2.createQueue("t1.rgA_n1"), "fresh", stampOf("rgA"), DeliveryMode.PERSISTENT, 0);
        Set<String> got = new HashSet<>();
        long end = System.currentTimeMillis() + 30_000;
        while (got.size() < backlog + 1 && System.currentTimeMillis() < end) {
            Message m = moved.receive(3000);
            if (m != null) got.add(((TextMessage) m).getText());
        }
        assertTrue(got.contains("fresh"), "a message of rgA's own client was not delivered behind " + backlog + " that waited ("
                + got.size() + " arrived)");
        assertEquals(backlog + 1, got.size(), "what waited at rgA for the node did not follow it to rgB");
        assertEquals(0, f.authz.get("rgA").transitWithheldCount(), "a queue message was judged");
    }

    // ---- the rule that strips is untouched ----

    @Test
    void whatARelayingRegionBringsForAnotherRegionsNodeIsStillStripped() throws Exception {
        // a region told to relay (as every region did) brings rgA's stamp to the global over its own bridge, and the global
        // strips it -- a region speaks for its own nodes only
        f.start("rgG", null);
        f.startUnder("rgA", "rgG", null);
        f.startUnder("rgB", "rgG", Map.of(CrescoAuthorizationBroker.TRANSIT_KEY, "true"));
        f.bridge("rgB", "rgG");
        f.bridge("rgA", "rgB");
        Session g = f.node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(3000);
        Session a = f.node("rgA");
        send(a, a.createQueue("t1.rgG_gc"), "watchdog", stampOf("rgA"));
        Message got = inbox.receive(8000);
        assertNotNull(got, "a relaying region did not relay");
        assertEquals("", got.getStringProperty("cresco_caller"), "rgB's bridge brought a stamp of rgA's node and the global kept it");
        assertEquals(1, f.authz.get("rgG").outOfScopeStampCount());
    }

    @Test
    void aStampARegionForgesForAnotherRegionsNodeIsStillStripped() throws Exception {
        // a region whose broker runs no Cresco authorization (a compromised one would not) lets its node declare anything
        f.start("rgG", null);
        java.security.cert.X509Certificate[] anchors = {BridgedFabric.ca("t1", "rgG").caCert(), BridgedFabric.ca("t1", "rgA").caCert(),
                BridgedFabric.ca("t1", "rgB").caCert(), BridgedFabric.ca("t1", "rgC").caCert()};
        BrokerService rogue = TlsTestSupport.broker("rgB_rc", BridgedFabric.id("rgB", "rc"), false, null, null, anchors);
        f.brokers.add(rogue);
        NetworkConnector nc = rogue.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(f.byRegion.get("rgG"))
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("cresco-bridge-rogue-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(new SslContext(BridgedFabric.id("rgB", "rc").keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom()));
        nc.start();
        f.byRegion.put("rgB", rogue);
        Session g = f.node("rgG");
        MessageConsumer inbox = g.createConsumer(g.createQueue("t1.rgG_gc"));
        Thread.sleep(2500);
        Session b = f.node("rgB");
        send(b, b.createQueue("t1.rgG_gc"), "forged", stampOf("rgA"));
        send(b, b.createQueue("t1.rgG_gc"), "own", stampOf("rgB"));
        Map<String, String> got = BridgedFabric.drain(inbox, 2, 500L, null);
        assertEquals("", got.get("forged"), "rgB's bridge spoke for a node of rgA");
        assertEquals(stampOf("rgB"), got.get("own"), "a region's own node is carried");
    }

    // ---- two tenants under a superuser tenant's global ----

    @Test
    void underASuperuserGlobalATenantsMeshCarriesItsOwnWithItsCallerAndRelaysNothing() throws Exception {
        f.start("sgG", null);
        for (String r : new String[]{"taA", "taB", "tbC"}) f.startUnder(r, "sgG", null);
        for (String r : new String[]{"taA", "taB", "tbC"}) f.bridge(r, "sgG");
        f.bridge("taA", "taB");
        f.bridge("taA", "tbC");                                                  // a region of another tenant is a peer too
        f.bridge("taB", "tbC");
        Session b = f.node("taB");
        MessageConsumer listener = b.createConsumer(b.createTopic("T.t1.global.event"));
        MessageConsumer inbox = f.node("taB").createConsumer(b.createQueue("T.t1.taB_n1"));
        Session c = f.node("tbC");
        MessageConsumer other = c.createConsumer(c.createTopic("T.t2.global.event"));
        Thread.sleep(3500);
        Session a = f.node("taA");
        int sent = 10;
        for (int i = 0; i < sent; i++) send(a, a.createTopic("T.t1.global.event"), "advert-" + i, stampOf("taA"));
        send(a, a.createQueue("T.t1.taB_n1"), "request", stampOf("taA"));
        List<String> all = new ArrayList<>();
        Map<String, String> got = BridgedFabric.drain(listener, sent, 1500L, all);
        assertEquals(sent, got.size(), "tenant t1's adverts did not reach its other region");
        assertEquals(sent, all.size(), "an advert was given twice");
        assertEquals(Set.of(stampOf("taA")), new HashSet<>(got.values()), "an advert of t1 arrived without its sender's stamp");
        Message q = inbox.receive(8000);
        assertNotNull(q);
        assertEquals(stampOf("taA"), q.getStringProperty("cresco_caller"));
        assertNull(other.receive(500), "tenant t2's listener was given something");
        assertEquals(0, f.stripped(), "a stamp was stripped in a mesh of two tenants' regions");
        for (String r : new String[]{"taA", "taB", "tbC"}) assertFalse(f.authz.get(r).relaysBetweenBridges(), r);
        assertTrue(f.authz.get("sgG").relaysBetweenBridges());
    }
}
