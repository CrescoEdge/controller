package io.cresco.agent.controller.regionalcontroller;

import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netmetrics.RouteView;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Region-to-region peer work. A region used to dial every region in its route view and bridge every one that answered,
 * without a cap: R regions built R x (R-1) bridge groups. And that dialing (1 s per address), the discovery of the
 * configured peers (5 s each) and the path probes (3 RPCs x 5 s per peer) ran on the timer that ages the region's
 * agents, which then lagged by minutes. Inferred peers are capped (net_infer_max_peers, 3 by default) and the peer work
 * has its own timer; the configured peers (regional_peers) are kept up as before, outside the cap.
 */
class RegionPeerWorkTest {

    private static final int REGIONS = 20;

    private static String path(int i) { return String.format("r%02d_rc", i); }
    private static String host(int i) { return "10.0.0." + i; }

    /** The watcher of region r00 in a fabric of REGIONS regions; dialing and bridging are simulated. */
    private static final class Fabric extends RegionHealthWatcher {
        final ControllerEngine engine;
        final Set<String> bridged = new HashSet<>();
        final List<String> dialed = new ArrayList<>();
        volatile Predicate<String> answers = host -> true;
        final AtomicInteger configured = new AtomicInteger(), inferred = new AtomicInteger(), probed = new AtomicInteger(), verified = new AtomicInteger();
        boolean countOnly;

        Fabric(ControllerEngine engine) { super(engine); this.engine = engine; }

        @Override boolean bridgedTo(String peerPath) { return bridged.contains(peerPath); }

        @Override DiscoveryNode dialRegion(String host, int port, int timeout) {
            dialed.add(host);
            if (!answers.test(host)) return null;
            DiscoveryNode n = new DiscoveryNode(DiscoveryType.REGION);
            n.discovered_region = String.format("r%02d", Integer.parseInt(host.substring(host.lastIndexOf('.') + 1)));
            n.discovered_agent = "rc";
            n.discovered_ip = host;
            return n;
        }

        @Override void maintainPeerConnections() { configured.incrementAndGet(); if (!countOnly) super.maintainPeerConnections(); }
        @Override public void inferConnections() { inferred.incrementAndGet(); if (!countOnly) super.inferConnections(); }
        @Override public void probeConnectedRegionPeers() { probed.incrementAndGet(); }
        @Override public void verifyGraphRoutes() { verified.incrementAndGet(); }

        /** As the broker manager does: every submitted candidate becomes a bridge. How many were submitted. */
        int bridgeTheCandidates() {
            List<DiscoveryNode> candidates = new ArrayList<>();
            engine.getIncomingCanidateBrokers().drainTo(candidates);
            for (DiscoveryNode n : candidates) bridged.add(n.getDiscoveredPath());
            return candidates.size();
        }
    }

    private Fabric watcher;

    private Fabric fabric(Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("region-peers").toString(), cfg, m -> { }, "r00", "rc");
        ControllerEngine engine = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State state = TlsTestSupport.state(pb, "r00", "rc");
        state.regional = true;
        engine.cstate = state;
        engine.setIncomingCanidateBrokers(new LinkedBlockingQueue<>());
        RouteView view = new RouteView(600_000);
        for (int i = 0; i < REGIONS; i++) {
            view.update(new RouteView.NodeState(path(i), path(i).substring(0, 3), "region", new ArrayList<>(),
                    new ArrayList<>(Collections.singletonList(host(i) + ":32005"))));
        }
        view.update(new RouteView.NodeState("g_gc", "g", "global", new ArrayList<>(), new ArrayList<>(Collections.singletonList("10.0.1.1:32005"))));
        engine.setRouteView(view);
        watcher = new Fabric(engine);
        engine.setRegionHealthWatcher(watcher);
        return watcher;
    }

    @AfterEach
    void stop() {
        if (watcher != null) watcher.shutdown();
    }

    @Test
    void aRegionBridgesNoMoreInferredPeersThanTheCap() throws Exception {
        Fabric w = fabric(new HashMap<>());
        for (int round = 0; round < 10; round++) {
            w.inferConnections();
            w.bridgeTheCandidates();
        }
        assertEquals(3, w.bridged.size(), "every region that answers was bridged (of " + (REGIONS - 1) + "): " + w.bridged);
        int dialedWhenFull = w.dialed.size();
        w.inferConnections();
        assertEquals(dialedWhenFull, w.dialed.size(), "a region that holds its inferred peers still dials the others every round");
        assertFalse(w.dialed.contains("10.0.1.1"), "only regions are inferred, not the global");
    }

    @Test
    void peersTheBrokerManagerHasNotTakenUpYetHoldTheirPlaces() throws Exception {
        Fabric w = fabric(new HashMap<>());
        for (int round = 0; round < 5; round++) w.inferConnections();          // the broker manager is behind: nothing bridged yet
        assertEquals(3, w.bridgeTheCandidates(), "the cap was passed while the first candidates were still queued");
    }

    @Test
    void theCapIsConfigurableAndZeroInfersNone() throws Exception {
        Fabric five = fabric(Collections.singletonMap("net_infer_max_peers", "5"));
        for (int round = 0; round < 10; round++) { five.inferConnections(); five.bridgeTheCandidates(); }
        assertEquals(5, five.bridged.size());
        five.shutdown();

        Fabric none = fabric(Collections.singletonMap("net_infer_max_peers", "0"));
        none.inferConnections();
        assertEquals(0, none.bridgeTheCandidates());
        assertTrue(none.dialed.isEmpty());
    }

    @Test
    void aLostInferredPeerIsReplacedAndRegionsThatDoNotAnswerDoNotHoldUpTheRest() throws Exception {
        Fabric w = fabric(new HashMap<>());
        // only three regions far down the ring are on a shared link
        Set<String> reachable = new HashSet<>(Arrays.asList(host(14), host(16), host(18)));
        w.answers = reachable::contains;
        int rounds = 0, mostDialsInARound = 0;
        while (w.bridged.size() < 3 && rounds < 20) {
            int before = w.dialed.size();
            w.inferConnections();
            w.bridgeTheCandidates();
            mostDialsInARound = Math.max(mostDialsInARound, w.dialed.size() - before);
            rounds++;
        }
        assertEquals(new HashSet<>(Arrays.asList(path(14), path(16), path(18))), w.bridged, "after " + rounds + " rounds");
        assertTrue(mostDialsInARound <= 6, "one round dialed " + mostDialsInARound + " regions: a round's dialing must not grow with the fabric");

        w.bridged.remove(path(16));                           // the bridge failed and was removed
        w.answers = host -> true;
        for (int round = 0; round < 5; round++) { w.inferConnections(); w.bridgeTheCandidates(); }
        assertEquals(3, w.bridged.size(), "a lost inferred peer frees its place: " + w.bridged);
    }

    @Test
    void configuredPeersAreKeptUpOutsideTheCap() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("regional_peers", host(7) + ", " + host(8) + ":32005," + host(9));
        cfg.put("net_infer_max_peers", "2");
        Fabric w = fabric(cfg);
        w.maintainPeerConnections();
        w.inferConnections();                                 // same round, the configured peers still queued: not inferred again
        assertEquals(5, w.bridgeTheCandidates(), "three configured peers and two inferred ones: " + w.dialed);
        for (int round = 0; round < 10; round++) { w.inferConnections(); w.bridgeTheCandidates(); }
        assertTrue(w.bridged.containsAll(Arrays.asList(path(7), path(8), path(9))));
        assertEquals(5, w.bridged.size(), "the configured peers take none of the inferred places: " + w.bridged);

        w.bridged.remove(path(8));                            // a configured peer's bridge failed
        w.maintainPeerConnections();
        assertEquals(1, w.bridgeTheCandidates(), "a configured peer that is down is submitted again");
        assertTrue(w.bridged.contains(path(8)));
    }

    @Test
    void inAFabricOfCappedRegionsTheBridgesGrowWithTheRegionsNotTheirSquare() {
        // every region picks its targets in ring order from its own name: different neighbours, and every region is picked
        Map<String, Integer> picked = new HashMap<>();
        List<String> all = new ArrayList<>();
        for (int i = 0; i < REGIONS; i++) all.add(path(i));
        int bridges = 0;
        for (String self : all) {
            List<String> others = new ArrayList<>(all);
            others.remove(self);
            List<String> targets = RegionHealthWatcher.inferTargets(self, others, 3, 0, 0);
            assertEquals(6, targets.size(), "twice the free places are dialed in a round");
            for (String t : targets.subList(0, 3)) { picked.merge(t, 1, Integer::sum); bridges++; }      // all answer: the first three
        }
        assertEquals(REGIONS * 3, bridges);
        assertEquals(REGIONS, picked.size(), "every region is some region's inferred peer");
        for (int n : picked.values()) assertEquals(3, n, "no region is everyone's hub: " + picked);
        assertEquals(Arrays.asList(path(1), path(2), path(3), path(4), path(5), path(6)),
                RegionHealthWatcher.inferTargets(path(0), all.subList(1, REGIONS), 3, 0, 0));
        assertEquals(Arrays.asList(path(19), path(0)), RegionHealthWatcher.inferTargets(path(18), Arrays.asList(path(0), path(19)), 3, 0, 0),
                "the ring wraps around");
        assertTrue(RegionHealthWatcher.inferTargets(path(0), all.subList(1, REGIONS), 3, 3, 0).isEmpty(), "no free place, no dialing");
        // later rounds move on, so regions that do not answer are not dialed ahead of the rest for ever
        Set<String> seen = new HashSet<>();
        for (int round = 0; round < 4; round++) seen.addAll(RegionHealthWatcher.inferTargets(path(0), all.subList(1, REGIONS), 3, 0, round));
        assertEquals(REGIONS - 1, seen.size());
    }

    @Test
    void theTimerThatAgesAgentsDoesNoPeerWork() throws Exception {
        Fabric w = fabric(new HashMap<>());
        w.countOnly = true;
        w.new RegionalNodeStatusWatchDog(w.engine, w.engine.getPluginBuilder().getLogger(RegionPeerWorkTest.class.getName(), io.cresco.library.utilities.CLogger.Level.Info)).run();
        assertEquals(0, w.configured.get() + w.inferred.get() + w.probed.get() + w.verified.get(),
                "the agent-aging tick waited for peer discovery (5 s a peer), inference (1 s an address) and path probes (15 s a peer)");

        w.new PeerMaintenanceTask().run();
        assertEquals(1, w.configured.get(), "configured peers");
        assertEquals(1, w.inferred.get(), "inferred peers");
        assertEquals(1, w.probed.get(), "path probes");
        assertEquals(1, w.verified.get(), "graph routes");
        assertNotNull(w.peerTimer, "the peer work has its own timer");
        assertNotSame(w.peerTimer, w.regionalUpdateTimer);
    }
}
