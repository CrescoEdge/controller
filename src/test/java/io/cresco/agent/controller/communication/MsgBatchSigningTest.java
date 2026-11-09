package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Batch signatures (MsgBatchSigner): the messages a node signs at the same moment share one RSA operation (a Merkle root),
 * each still verifiable on its own by its receiver, each refused when replayed, tampered or carrying a forged path; only a
 * node that said it understands them is sent one.
 */
class MsgBatchSigningTest {

    private static final Gson GSON = new Gson();
    private static ControllerEngine g, rA, rB, a1, b1, old, quiet;

    private static ControllerEngine node(String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_sessions", "false");                    // signatures only here (MsgSessionTest: session keys)
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
    }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    @BeforeAll
    static void fabric() throws Exception {
        g = node("rgG", "gc", true, Map.of());
        ((TlsTestSupport.State) g.cstate).global = true;
        rA = node("rgA", "rc", true, Map.of());
        rB = node("rgB", "rc2", true, Map.of());
        a1 = node("rgA", "a1", false, Map.of("msg_signing_threads", "1"));       // one worker: concurrent messages queue up
        b1 = node("rgB", "b1", false, Map.of());
        old = node("rgB", "b2", false, Map.of("msg_signing_batch", "false"));    // as a node that predates batch signatures
        quiet = node("rgB", "b3", false, Map.of());                              // a1 never hears from it
        joined(rA, g, true);
        joined(rB, g, true);
        joined(a1, rA, false);
        joined(b1, rB, false);
        joined(old, rB, false);
        joined(quiet, rB, false);
        ping(rA, g, true);
        ping(rB, g, true);
        ping(rA, g, true);
        ping(rB, g, true);
        ping(a1, rA, false);
        ping(b1, rB, false);
        ping(old, rB, false);
        ping(quiet, rB, false);
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    private static MsgEvent sent(ControllerEngine from, ControllerEngine to, String payload) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("payload", payload);
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /** {@code n} messages from {@code from} to {@code to}, signed by {@code n} threads at once. */
    private static List<MsgEvent> burst(ControllerEngine from, ControllerEngine to, int n) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<MsgEvent>> fs = new ArrayList<>();
            AtomicInteger i = new AtomicInteger();
            for (int k = 0; k < n; k++) fs.add(pool.submit(() -> sent(from, to, "m" + i.incrementAndGet())));
            List<MsgEvent> out = new ArrayList<>();
            for (Future<MsgEvent> f : fs) out.add(f.get());
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void everyTreeLeadsEachLeafToItsRoot() {
        for (int n = 2; n <= 37; n++) {
            byte[][] leaves = new byte[n][];
            for (int i = 0; i < n; i++) leaves[i] = MsgBatchSigner.leaf(("message " + i).getBytes());
            List<byte[][]> levels = MsgBatchSigner.levels(leaves);
            byte[] root = levels.get(levels.size() - 1)[0];
            for (int i = 0; i < n; i++) {
                String path = MsgBatchSigner.path(levels, i, n);
                int[] count = new int[1];
                assertArrayEquals(root, MsgBatchSigner.root(leaves[i], path, count), "leaf " + i + " of " + n);
                assertEquals(n, count[0]);
                int other = (i + 1) % n;
                assertFalse(java.util.Arrays.equals(root, MsgBatchSigner.root(leaves[other], path, count)), "another leaf on leaf " + i + "'s path");
            }
        }
        byte[] l = MsgBatchSigner.leaf("x".getBytes());
        int[] c = new int[1];
        for (String bad : new String[]{"", "0/1:", "2/2:AA", "0/2:", "0/2:@@", "0/300:AA", "-1/2:AA", "0/2:" + b64(48) + "," + b64(48), "0/2:" + b64(47)}) {
            assertNull(MsgBatchSigner.root(l, bad, c), "malformed path accepted: " + bad);
        }
    }

    private static String b64(int len) { return Base64.getEncoder().encodeToString(new byte[len]); }

    @Test
    void batchesShareOneSignatureAndEachMessageVerifiesAlone() throws Exception {
        // until a node has been heard to understand batch signatures, a1 signs each message to it on its own
        for (MsgEvent m : burst(a1, quiet, 8)) {
            assertNull(m.getParam(MsgBatchSigner.TREE), "a batch signature to a node not known to read one");
            assertTrue(quiet.msgSigning().verify(m, plugin(a1), false).ok());
        }
        // b1 says so on everything it signs
        MsgEvent hello = sent(b1, a1, "hello");
        assertTrue(hello.getParam(MsgSigning.CAPS).contains("batch"));
        assertTrue(a1.msgSigning().verify(hello, plugin(b1), false).ok());

        long before = a1.msgSigning().batchedSignedCount();
        List<MsgEvent> ms = burst(a1, b1, 48);
        assertTrue(a1.msgSigning().batchedSignedCount() > before, "48 messages signed at once by one worker shared no signature");
        Set<String> sigs = new HashSet<>();
        int batched = 0;
        for (MsgEvent m : ms) {
            sigs.add(m.getParam(MsgSigning.SIG));
            if (m.getParam(MsgBatchSigner.TREE) != null) batched++;
            MsgSigning.Verdict v = b1.msgSigning().verify(m, plugin(a1), false);
            assertTrue(v.ok(), v.refusal);
        }
        assertTrue(sigs.size() < ms.size(), "no two messages share a signature (" + sigs.size() + " of " + ms.size() + ")");
        System.out.printf("batch signatures: %d messages, %d key operations, %d carried a tree path%n", ms.size(), sigs.size(), batched);
    }

    @Test
    void aBatchedMessageIsRefusedWhenReplayedTamperedOrGivenAnotherPath() throws Exception {
        assertTrue(a1.msgSigning().verify(sent(b1, a1, "hello again"), plugin(b1), false).ok());
        List<MsgEvent> batch = null;
        for (int tries = 0; tries < 5 && batch == null; tries++) {
            Map<String, List<MsgEvent>> bySig = new HashMap<>();
            for (MsgEvent m : burst(a1, b1, 32)) {
                if (m.getParam(MsgBatchSigner.TREE) != null) bySig.computeIfAbsent(m.getParam(MsgSigning.SIG), k -> new ArrayList<>()).add(m);
            }
            for (List<MsgEvent> l : bySig.values()) if (l.size() >= 3) batch = l;
        }
        assertNotNull(batch, "no batch of three formed");
        MsgEvent first = batch.get(0), second = batch.get(1), third = batch.get(2);

        MsgEvent replay = wire(first);
        assertTrue(b1.msgSigning().verify(first, plugin(a1), false).ok());
        assertFalse(b1.msgSigning().verify(replay, plugin(a1), false).ok(), "a batched message replayed");

        MsgEvent tampered = wire(second);
        tampered.setParam("payload", "something else");
        MsgSigning.Verdict t = b1.msgSigning().verify(tampered, plugin(a1), false);
        assertFalse(t.ok(), "a tampered batched message");

        MsgEvent swapped = wire(third);
        swapped.setParam(MsgBatchSigner.TREE, second.getParam(MsgBatchSigner.TREE));
        assertFalse(b1.msgSigning().verify(swapped, plugin(a1), false).ok(), "a message carrying a sibling's path");

        MsgEvent forgedCount = wire(third);
        String p = third.getParam(MsgBatchSigner.TREE);
        int slash = p.indexOf('/'), colon = p.indexOf(':');
        int n = Integer.parseInt(p.substring(slash + 1, colon));
        forgedCount.setParam(MsgBatchSigner.TREE, p.substring(0, slash + 1) + (n + 1) + p.substring(colon));
        assertFalse(b1.msgSigning().verify(forgedCount, plugin(a1), false).ok(), "a path with another count");

        assertTrue(b1.msgSigning().verify(second, plugin(a1), false).ok(), "the untouched sibling still verifies");
        assertTrue(b1.msgSigning().verify(third, plugin(a1), false).ok(), "the untouched sibling still verifies");
    }

    @Test
    void aNodeWithoutBatchSignaturesNeverAdvertisesThemAndIsNeverSentOne() throws Exception {
        MsgEvent fromOld = sent(old, a1, "from an older build");
        assertFalse(String.valueOf(fromOld.getParam(MsgSigning.CAPS)).contains("batch"), "msg_signing_batch=false advertised batch signatures");
        assertTrue(a1.msgSigning().verify(fromOld, plugin(old), false).ok());
        for (MsgEvent m : burst(a1, old, 24)) {
            assertNull(m.getParam(MsgBatchSigner.TREE), "a batch signature to a node that cannot read one");
            assertTrue(old.msgSigning().verify(m, plugin(a1), false).ok());
        }
    }
}
