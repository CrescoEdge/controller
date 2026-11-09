package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end MsgEvent signatures (D4) between real controller engines (real identity keys, RSA-3072): a global rgG/gc, two
 * regions rgA/rc and rgB/rc2, an agent of each (rgA/a1, rgB/b1). Keys travel as in production: each node's certificate on
 * its pings, attestations and trust bundles on the pongs.
 */
class MsgSigningTest {

    private static final Gson GSON = new Gson();
    private static ControllerEngine g, rA, rB, a1, b1;

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    /** {@code joiner} joined {@code controller} (discovery): each binds the other's key, as DiscoveryProcessor.learnPeer does. */
    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
    }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    /** One ping/pong between child and parent, as the health watchers and executors run it. */
    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);    // the verified caller
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    @BeforeAll
    static void fabric() throws Exception {
        g = node("rgG", "gc", true);
        ((TlsTestSupport.State) g.cstate).global = true;
        rA = node("rgA", "rc", true);
        rB = node("rgB", "rc2", true);
        a1 = node("rgA", "a1", false);
        b1 = node("rgB", "b1", false);
        joined(rA, g, true);
        joined(rB, g, true);
        joined(a1, rA, false);
        joined(b1, rB, false);
        // keys travel: regions to the global, the bundle down, agents' attestations
        ping(rA, g, true);
        ping(rB, g, true);
        ping(rA, g, true);
        ping(rB, g, true);
        ping(a1, rA, false);
        ping(b1, rB, false);
    }

    /** What {@code from} sends: stamped by its controller as {@code caller}, signed as it leaves. */
    private static MsgEvent sent(ControllerEngine from, Caller caller, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("payload", "hello");
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    @Test
    void keysTravelOnThePings() {
        assertTrue(a1.msgSigning().hasAttestation(), "an agent holds an attestation from its region");
        assertTrue(b1.msgSigning().bundleSize() >= 3, "an agent of rgB holds the global, rgA's and rgB's controllers");
    }

    @Test
    void anAgentsMessageVerifiesAtItsRegionAndInAnotherRegion() {
        MsgEvent m = sent(a1, plugin(a1), b1);
        MsgSigning.Verdict at = b1.msgSigning().verify(m, plugin(a1), false);
        assertTrue(at.ok(), at.refusal);
        // to its own region: a direct link (offered on the pings), so unsigned -- accepted only as the region's broker
        // attests it first hand from a1 (DirectLinkTest)
        MsgEvent n = sent(a1, plugin(a1), rA);
        assertNull(n.getParam(MsgSigning.SIG), "a direct-link message was signed");
        assertTrue(rA.msgSigning().verify(n, plugin(a1), false, io.cresco.library.security.CrescoIdentity.of("t1", "rgA", "a1", null)).ok());
        assertFalse(rA.msgSigning().verify(n, plugin(a1), false).ok(), "an unsigned message was accepted without the first-hand attestation");
        MsgEvent toGlobal = sent(a1, plugin(a1), g);
        assertTrue(g.msgSigning().verify(toGlobal, plugin(a1), false).ok(), "the global verifies an agent's attestation by its region's key");
    }

    @Test
    void controllersVerifyEverywhere() {
        Caller gc = Caller.controller("t1", "rgG", "gc");
        assertTrue(b1.msgSigning().verify(sent(g, gc, b1), gc, false).ok(), "the global's word at an agent of another region");
        Caller rc = Caller.controller("t1", "rgA", "rc");
        assertTrue(b1.msgSigning().verify(sent(rA, rc, b1), rc, false).ok(), "a region's word at an agent of another region");
    }

    @Test
    void whatTravelsInTransitIsNotSignedAndEverythingElseIs() {
        MsgEvent m = sent(a1, plugin(a1), b1);
        m.setParam("routepath-x", "1");
        m.setParam("ttl", "3");
        m.setParam("srcroute", "a;b");
        assertTrue(b1.msgSigning().verify(m, plugin(a1), false).ok());
        MsgEvent moved = sent(a1, plugin(a1), b1);
        moved.setForwardDst("rgOther", "b1", null);                 // the destination is not signed, the recipient is
        assertTrue(b1.msgSigning().verify(moved, plugin(a1), false).ok(), "its recipient, whatever the header says on the way");
        MsgEvent elsewhere = sent(a1, plugin(a1), b1);
        assertFalse(rB.msgSigning().verify(elsewhere, null, false).ok(), "delivered to a node it does not name");
        MsgEvent t = sent(a1, plugin(a1), b1);
        t.setParam("payload", "tampered");
        assertFalse(b1.msgSigning().verify(t, plugin(a1), false).ok(), "a changed parameter");
        MsgEvent added = sent(a1, plugin(a1), b1);
        added.setParam("action_extra", "x");
        assertFalse(b1.msgSigning().verify(added, plugin(a1), false).ok(), "an added parameter");
        MsgEvent src = sent(a1, plugin(a1), b1);
        src.setSrc("rgA", "a9", "p1");
        src.setParam("src_agent", "a9");
        assertFalse(b1.msgSigning().verify(src, plugin(a1), false).ok(), "a changed parameter (src)");
    }

    @Test
    void theSignedCallerIsTheOnlyCaller() {
        MsgEvent m = sent(a1, plugin(a1), b1);
        assertFalse(b1.msgSigning().verify(m, Caller.plugin("t1", "rgA", "a1", "p2"), false).ok(), "the broker's stamp differs");
        MsgSigning.Verdict stripped = b1.msgSigning().verify(sent(a1, plugin(a1), b1), null, false);
        assertTrue(stripped.ok(), "a stamp stripped on the way: the signed caller stands");
        assertEquals(plugin(a1), stripped.caller);
        // a node signs only for itself (and its tenant's users)
        MsgEvent other = sent(a1, Caller.controller("t1", "rgA", "rc"), b1);
        assertFalse(b1.msgSigning().verify(other, Caller.controller("t1", "rgA", "rc"), false).ok());
        assertTrue(b1.msgSigning().verify(sent(a1, Caller.api("t1", "u", List.of("user")), b1), null, false).ok());
        assertFalse(b1.msgSigning().verify(sent(a1, Caller.api("t2", "u", List.of("admin")), b1), null, false).ok(),
                "a node of t1 signs no user of t2");
    }

    @Test
    void unsignedReplayedAndStaleAreRefused() {
        MsgEvent u = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgB", "b1", null, false, false);
        assertFalse(b1.msgSigning().verify(u, plugin(a1), false).ok(), "unsigned");
        Caller legacy = Caller.parse("agent:t1/rgA/a1", "t1", "user");
        assertFalse(b1.msgSigning().verify(u, legacy, false).ok(), "a legacy node outside the upgrade window");
        assertTrue(b1.msgSigning().verify(u, legacy, true).ok(), "a legacy node inside the upgrade window (agent: stamp only)");
        assertFalse(b1.msgSigning().verify(u, plugin(a1), true).ok(), "inside the window only an agent: stamp goes unsigned");
        MsgEvent m = sent(a1, plugin(a1), b1);
        assertTrue(b1.msgSigning().verify(m, plugin(a1), false).ok());
        assertFalse(b1.msgSigning().verify(wire(m), plugin(a1), false).ok(), "replayed");
        MsgEvent stale = sent(a1, plugin(a1), b1);
        stale.setParam(MsgSigning.TS, Long.toString(System.currentTimeMillis() - 3_600_000L));
        assertFalse(b1.msgSigning().verify(stale, plugin(a1), false).ok(), "an hour old (and the time is signed)");
        MsgEvent forgedCred = sent(a1, plugin(a1), b1);
        forgedCred.setParam(MsgSigning.BY, "t1/rgA/a2");
        assertFalse(b1.msgSigning().verify(forgedCred, plugin(a1), false).ok());
    }

    @Test
    void aRelayNeverSignsWhatItCarries() {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgB", "b1", null, false, false);
        Caller.controller("t1", "rgA", "a1").stamp(m);
        m.setParam(MsgSigning.PENDING, "1");                               // a sender asking the next controller to sign
        MsgEvent arrived = wire(m);
        CallerStamps.fromJms(arrived, new org.apache.activemq.command.ActiveMQTextMessage(), true);
        assertNull(arrived.getParam(MsgSigning.PENDING));
        rA.msgSigning().signIfPending(arrived);
        assertNull(arrived.getParam(MsgSigning.SIG), "the relay signed a message it did not originate");
    }

    /** Throughput of the signing path on this machine (reported, not asserted beyond sanity). */
    @Test
    void signingCost() {
        int n = 300;
        MsgEvent[] ms = new MsgEvent[n];
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", "p1", "rgB", "b1", null, false, false);
            m.setParam("action", "getstatus");
            m.setParam("i", Integer.toString(i));
            CallerStamps.stamp(m, plugin(a1));
            a1.markForSigning(m);
            a1.msgSigning().signIfPending(m);
            ms[i] = wire(m);
        }
        long t1 = System.nanoTime();
        for (MsgEvent m : ms) assertTrue(b1.msgSigning().verify(m, plugin(a1), false).ok());
        long t2 = System.nanoTime();
        double signPerS = n / ((t1 - t0) / 1e9), verifyPerS = n / ((t2 - t1) / 1e9);
        System.out.printf("D4 signing cost (RSA-3072/SHA-384, one thread): sign+serialize %.0f msg/s (%.3f ms each), verify %.0f msg/s (%.3f ms each)%n",
                signPerS, 1000.0 / signPerS, verifyPerS, 1000.0 / verifyPerS);
        assertTrue(signPerS > 20 && verifyPerS > 100);
    }

    /**
     * The replay memory keeps each signature in the bucket of its SIGNED time (a replay carries the same time) for as long as
     * that time is accepted: a copy is caught whenever it arrives inside the max age; once its time is older than the max age
     * it is refused by its age, and the bucket is gone. No scan; one lookup.
     */
    @Test
    void theReplayMemoryKeepsASignatureWhileItsTimeIsAccepted() {
        MsgSigning ms = rA.msgSigning();
        long t0 = System.currentTimeMillis(), age = 300_000L;              // msg_signature_max_age_ms default
        byte[] sig = new byte[64];
        new java.security.SecureRandom().nextBytes(sig);
        assertEquals(ReplayMemory.Result.FRESH, ms.remember(sig, t0, t0), "a new signature");
        assertEquals(ReplayMemory.Result.REPLAY, ms.remember(sig, t0, t0 + 1), "the same signature again");
        assertEquals(ReplayMemory.Result.REPLAY, ms.remember(sig, t0, t0 + age), "one max-age later: still caught");
        byte[] early = new byte[64];
        new java.security.SecureRandom().nextBytes(early);
        assertEquals(ReplayMemory.Result.FRESH, ms.remember(early, t0 + age, t0), "signed by a clock a max-age ahead");
        assertEquals(ReplayMemory.Result.REPLAY, ms.remember(early, t0 + age, t0 + 2 * age), "caught until its own time is a max-age old");
    }

    /** Two copies of one message arriving at once, on many threads: exactly one is accepted (the check and the record are one step). */
    @Test
    void theReplayMemoryAcceptsOneCopyOnly() throws Exception {
        ReplayMemory mem = new ReplayMemory(300_000L, 1_000_000L, null);
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(16);
        try {
            for (int round = 0; round < 200; round++) {
                long d = new java.security.SecureRandom().nextLong(), t = System.currentTimeMillis();
                java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
                java.util.List<java.util.concurrent.Future<ReplayMemory.Result>> fs = new java.util.ArrayList<>();
                for (int i = 0; i < 16; i++) fs.add(pool.submit(() -> { go.await(); return mem.record(d, t, t); }));
                go.countDown();
                int fresh = 0;
                for (java.util.concurrent.Future<ReplayMemory.Result> f : fs) if (f.get() == ReplayMemory.Result.FRESH) fresh++;
                assertEquals(1, fresh, "copies accepted at once");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Bounded memory: past its bound the replay memory drops its OLDEST bucket and from then on refuses what was signed in it
     * as too old -- it never grows past the bound and never accepts a replay.
     */
    @Test
    void theReplayMemoryIsBoundedAndFailsClosed() {
        long age = 300_000L, now = System.currentTimeMillis();
        ReplayMemory mem = new ReplayMemory(age, 64 * 256, null);            // 256 an stripe
        java.util.Random rnd = new java.util.Random(7);
        long[] old = new long[2000];
        for (int i = 0; i < old.length; i++) assertEquals(ReplayMemory.Result.FRESH, mem.record(old[i] = rnd.nextLong() | 1, now - age + 1000, now));
        for (int i = 0; i < 40_000; i++) {
            ReplayMemory.Result r = mem.record(rnd.nextLong() | 1, now - (i % 10) * 1000, now);
            assertNotEquals(ReplayMemory.Result.REPLAY, r, "a new signature taken for a replay");
            assertTrue(mem.size() <= 64 * 256, "past its bound: " + mem.size());
        }
        for (long d : old) assertNotEquals(ReplayMemory.Result.FRESH, mem.record(d, now - age + 1000, now), "a replay after eviction was accepted");
        // the per-entry footprint: a million entries in primitive tables
        ReplayMemory big = new ReplayMemory(age, 4_000_000L, null);
        Runtime rt = Runtime.getRuntime();
        System.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        for (int i = 0; i < 1_000_000; i++) big.record(rnd.nextLong() | 1, now - (i % 300) * 1000, now);
        System.gc();
        long used = rt.totalMemory() - rt.freeMemory() - before;
        System.out.printf("replay memory: %d entries, about %.1f bytes each%n", big.size(), used / (double) big.size());
        assertEquals(1_000_000, big.size());
    }
}
