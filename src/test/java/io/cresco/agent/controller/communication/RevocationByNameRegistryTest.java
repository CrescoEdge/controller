package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Revocations by name, at the registry and with what this change added to its interface: what a global does with an
 * answer (it settles the request, once, and revokes nothing), who is asked, the bounds, the reminders and the health
 * line, what survives a restart and a compaction, what a region's controller does with a request (once, of its time,
 * fresh), and what a ping costs. {@link RevocationByNameTest} holds the behaviour on the wire between real nodes,
 * {@link RevocationByNameReviewTest} the findings of the review.
 */
class RevocationByNameRegistryTest {

    private static final Gson GSON = new Gson();
    private static final String ID = "[0-9a-f]{32}";

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    private static void setLog(PeerTrustRegistry r, List<String> log) {
        try {
            java.lang.reflect.Field f = PeerTrustRegistry.class.getDeclaredField("log");
            f.setAccessible(true);
            f.set(r, (java.util.function.Consumer<String>) log::add);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** A global rgG/gc (in memory) that region rgB's controller rgB/rc joined. */
    private static PeerTrustRegistry global(List<String> log) throws Exception {
        TlsTestSupport.provider();
        PeerTrustRegistry r = new PeerTrustRegistry();
        r.setGlobal(true);
        r.setSelf(() -> "rgG/gc");
        if (log != null) setLog(r, log);
        joined(r, "rgB", "rc");
        return r;
    }

    private static void joined(PeerTrustRegistry g, String region, String agent) throws Exception {
        assertTrue(g.bind("t1", region, agent, TlsTestSupport.rsa().getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        g.allowBridgePeer(region, agent);
    }

    private static PeerTrustRegistry open(Path f, boolean global, List<String> log) {
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.global = global;
        o.self = () -> global ? "rgG/gc" : "rgB/rc";
        return PeerTrustRegistry.open(f, o, log == null ? m -> { } : log::add);
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("byname").resolve("security").resolve("peer-trust.json");
    }

    private static String state(PeerTrustRegistry g, String name) {
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation(name.substring(0, name.indexOf('/')), name.substring(name.indexOf('/') + 1));
        return nr == null ? null : nr.state;
    }

    // ------------------------------------------------------------------ who takes a revocation by name, and of what

    /** Only a global takes one, only for a name of another region; its own name and its own region's unbound names never. */
    @Test
    void onlyAGlobalReservesANameItHoldsNoKeyForAndNeverItsOwn() throws Exception {
        PeerTrustRegistry region = new PeerTrustRegistry();
        assertEquals("no key is bound to rgB/b1 (revoke its key fingerprint instead)", region.revoke("rgB/b1", "operator"));
        assertFalse(region.isTombstoned("rgB", "b1"));
        PeerTrustRegistry g = global(null);
        assertEquals("rgG/gc is this node's own identity", g.revoke("rgG/gc", "operator"));
        assertEquals("no key is bound to rgG/typo (revoke its key fingerprint instead)", g.revoke("rgG/typo", "operator"));
        assertFalse(g.isTombstoned("rgG", "gc") || g.isTombstoned("rgG", "typo"));
        assertEquals(0, g.nameRevocationsOpen());
        assertNull(g.revoke("rgB/b1", "operator"));
        assertEquals(1, g.nameRevocationsOpen());
        assertTrue(g.nameRevocation("rgB", "b1").id.matches(ID));
    }

    /** The ONE controller of a region: none, one (with its tenant), or more than one recorded under it -- then nobody. */
    @Test
    void aRegionHasOneControllerToAskOrNone() throws Exception {
        PeerTrustRegistry g = global(null);
        assertEquals("rgB/rc", g.controllerOf("rgB").name);
        assertEquals("t1", g.controllerOf("rgB").tenant);
        assertTrue(g.controllerOf("rgX").why.contains("no controller of region rgX has joined"));
        // an agent bound under the region is no controller
        assertTrue(g.bind("t1", "rgB", "b7", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertEquals("rgB/rc", g.controllerOf("rgB").name);
        // a second name bound as a controller under it, of any tenant: nobody is the region's controller for this node
        KeyPair second = TlsTestSupport.rsa();
        assertTrue(g.bind("t2", "rgB", "zz", second.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertNull(g.controllerOf("rgB").name);
        assertTrue(g.controllerOf("rgB").why.contains("2 controllers") && g.controllerOf("rgB").why.contains("rgB/rc, rgB/zz"), g.controllerOf("rgB").why);
        // ... also when it is only recorded as a bridge peer
        PeerTrustRegistry h = global(null);
        h.allowBridgePeer("rgB", "yy");
        assertNull(h.controllerOf("rgB").name);
        // revoked here, the remaining one is the region's controller again
        assertNull(g.revoke("rgB/zz", "operator"));
        assertEquals("rgB/rc", g.controllerOf("rgB").name);
    }

    // ------------------------------------------------------------------ what a global does with an answer

    /** An answer settles the request it names, once -- as reported, or as refused -- and revokes nothing. */
    @Test
    void anAnswerSettlesItsRequestOnceAndRevokesNothing() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry g = global(log);
        String k = fp(TlsTestSupport.rsa().getPublic()), other = fp(TlsTestSupport.rsa().getPublic());
        for (String n : List.of("b1", "b2", "b3", "b4", "b5", "b6")) assertNull(g.revoke("rgB/" + n, "operator"));
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b1 REVOKED by name") && l.contains("RESERVED here") && l.contains("OUTSTANDING")), log.toString());
        String id1 = g.nameRevocation("rgB", "b1").id;

        // ignored, nothing changes: another region's controller, a name nobody asked about, nothing that names a request
        assertTrue(g.acceptNameAnswer("rgC", "rgC/rc", id1 + "|rgB/b1|K|" + k).contains("not of region rgC"));
        assertTrue(g.acceptNameAnswer("rgB", "rgB/rc", id1 + "|rgB/zz|K|" + k).contains("asked nobody"));
        assertNotNull(g.acceptNameAnswer("rgB", "rgB/rc", "garbage"));
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, state(g, "rgB/b1"));
        assertEquals(6, g.nameRevocationsOpen());

        // reported: recorded as the controller's word, with the fingerprint for the operator; no key is revoked
        log.clear();
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", id1 + "|rgB/b1|K|" + k.toUpperCase()));
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation("rgB", "b1");
        assertEquals(PeerTrustRegistry.NameRevocation.REPORTED, nr.state);
        assertEquals(k, nr.key);
        assertEquals("rgB/rc", nr.by);
        assertTrue(g.revocations().isEmpty(), "a key was revoked on a region's word");
        assertFalse(g.isRevokedKey(k));
        assertTrue(g.isTombstoned("rgB", "b1"));
        assertEquals(1, log.size(), log.toString());
        assertTrue(log.get(0).contains("Name revocation of rgB/b1: REPORTED by rgB/rc, not verified: it says it revoked key " + k)
                && log.get(0).contains("To have every region refuse that key, revoke the fingerprint " + k + " here."), log.get(0));
        assertFalse(log.get(0).contains("CONFIRMED"));
        // once: the same answer again is nothing; another key for the same request is nothing
        log.clear();
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", id1 + "|rgB/b1|K|" + k));
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", id1 + "|rgB/b1|K|" + other));
        assertEquals(k, g.nameRevocation("rgB", "b1").key);
        assertNotNull(g.acceptNameAnswer("rgB", "rgB/rc", "00".repeat(16) + "|rgB/b1|K|" + other));
        assertEquals(0, log.size());

        // the other answers a controller gives
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", g.nameRevocation("rgB", "b2").id + "|rgB/b2|N|"));
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", g.nameRevocation("rgB", "b3").id + "|rgB/b3|A|"));
        assertEquals("N", g.nameRevocation("rgB", "b2").answer);
        assertEquals("A", g.nameRevocation("rgB", "b3").answer);
        assertTrue(log.stream().anyMatch(l -> l.contains("rgB/b3") && l.contains("AFTER this revocation was made") && l.contains("left that binding alone")), log.toString());

        // refused: another request's id, or no answer at all, for a request that is open -- closed, not asked again
        log.clear();
        String id4 = g.nameRevocation("rgB", "b4").id;
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", "11".repeat(16) + "|rgB/b4|K|" + other));
        assertEquals(PeerTrustRegistry.NameRevocation.REFUSED, state(g, "rgB/b4"));
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", g.nameRevocation("rgB", "b5").id + "|rgB/b5|K|not-a-key"));
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", g.nameRevocation("rgB", "b6").id + "|rgB/b6|?|"));
        assertEquals(PeerTrustRegistry.NameRevocation.REFUSED, state(g, "rgB/b5"));
        assertEquals(PeerTrustRegistry.NameRevocation.REFUSED, state(g, "rgB/b6"));
        assertEquals(3, log.size(), log.toString());
        assertTrue(log.get(0).contains("the answer of rgB/rc was REFUSED") && log.get(0).contains("not sent again") && log.get(0).contains("NOTHING is known of its key"), log.get(0));
        // no further tries: the right answer afterwards changes nothing, and the request is not sent any more
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", id4 + "|rgB/b4|K|" + other));
        assertEquals(PeerTrustRegistry.NameRevocation.REFUSED, state(g, "rgB/b4"));
        assertEquals(0, g.nameRevocationsOpen());
        assertTrue(g.nameRevocationsFor("rgB").isEmpty());
        assertTrue(g.revocations().isEmpty());
        // the health line says which is which
        String h = g.nameRevocationsOutstanding();
        assertTrue(h.contains("0 NOT answered") && h.contains("3 whose answer was REFUSED") && h.contains("rgB/b4"), h);
    }

    /** Two answers to one request on two pings at the same moment: one settles it. */
    @Test
    void twoAnswersRacingSettleTheRequestOnce() throws Exception {
        for (int round = 0; round < 20; round++) {
            List<String> log = new CopyOnWriteArrayList<>();
            PeerTrustRegistry g = global(log);
            assertNull(g.revoke("rgB/b1", "operator"));
            String id = g.nameRevocation("rgB", "b1").id;
            log.clear();
            String k1 = "aa".repeat(32), k2 = "bb".repeat(32);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger errors = new AtomicInteger();
            List<Thread> ts = new ArrayList<>();
            for (String k : List.of(k1, k2, k1, k2)) {
                Thread t = new Thread(() -> {
                    try { go.await(); if (g.acceptNameAnswer("rgB", "rgB/rc", id + "|rgB/b1|K|" + k) != null) errors.incrementAndGet(); }
                    catch (Exception e) { errors.incrementAndGet(); }
                });
                ts.add(t);
                t.start();
            }
            go.countDown();
            for (Thread t : ts) t.join();
            assertEquals(0, errors.get());
            assertEquals(PeerTrustRegistry.NameRevocation.REPORTED, state(g, "rgB/b1"));
            assertEquals(1, log.stream().filter(l -> l.contains("REPORTED by")).count(), "the request was settled more than once: " + log);
            assertTrue(log.get(0).contains(g.nameRevocation("rgB", "b1").key), "the record and the log name different keys");
            assertEquals(0, g.nameRevocationsOpen());
        }
    }

    // ------------------------------------------------------------------ bounds

    /** Open requests are bounded per region and in all; a name beyond the bound is still reserved, and the operator is told. */
    @Test
    void theOpenRequestsAreBoundedPerRegionAndInAllAndANameIsReservedAnyway() throws Exception {
        PeerTrustRegistry g = global(null);
        int per = PeerTrustRegistry.NAME_REVOCATIONS_OPEN_PER_REGION, all = PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX;
        for (int i = 0; i < per; i++) assertNull(g.revoke("rgB/n" + i, "operator"));
        String why = g.revoke("rgB/onemore", "operator");
        assertNotNull(why);
        assertTrue(why.contains("IS reserved here") && why.contains("NOT asked") && why.contains(per + " revocations by name are already open for that region"), why);
        assertTrue(g.isTombstoned("rgB", "onemore"), "a name beyond the bound is not reserved");
        assertEquals(PeerTrustRegistry.NameRevocation.UNASKED, state(g, "rgB/onemore"));
        assertNull(g.nameRevocationsOutstanding() == null ? null : g.nameRevocationsOutstanding().contains("onemore") ? "listed" : null,
                "a name nobody was asked about is listed as unanswered");
        assertEquals(per, g.nameRevocationsOpen());
        assertEquals(per, g.nameRevocationsFor("rgB").size());
        assertNull(g.revoke("rgB/onemore", "operator"), "said again and again when the operator file is read again");
        // another region has its own share
        assertNull(g.revoke("rgC/c1", "operator"));
        // one settled: room for one more of that region
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", g.nameRevocation("rgB", "n0").id + "|rgB/n0|N|"));
        assertNull(g.revoke("rgB/another", "operator"));
        assertEquals(per, g.nameRevocationsFor("rgB").size());
        // in all
        PeerTrustRegistry big = global(null);
        for (int r = 0; r < all / per; r++) for (int i = 0; i < per; i++) assertNull(big.revoke("rg" + r + "/n" + i, "operator"));
        assertEquals(all, big.nameRevocationsOpen());
        why = big.revoke("rgZ/z1", "operator");
        assertTrue(why != null && why.contains(all + " revocations by name are already open here"), why);
        assertTrue(big.isTombstoned("rgZ", "z1"));
        assertNull(big.revoke("ab".repeat(32), "operator"), "a key fingerprint can always be revoked");
        // an operator rebind lifts a name nobody was asked about too (on a registry with its operator files)
        Path f = newFile();
        PeerTrustRegistry h = open(f, true, null);
        for (int i = 0; i <= per; i++) h.revoke("rgB/n" + i, "operator");
        assertEquals(PeerTrustRegistry.NameRevocation.UNASKED, state(h, "rgB/n" + per));
        Files.writeString(Path.of(f + ".rebind"), "rgB/n" + per + "\n");
        h.checkOperatorFiles();
        assertFalse(h.isTombstoned("rgB", "n" + per), "the rebind did not lift a name whose region was never asked");
        assertNull(h.nameRevocation("rgB", "n" + per));
        assertEquals(per, h.nameRevocationsOpen());
        h.awaitRevocations();
        h = open(f, true, null);
        assertFalse(h.isTombstoned("rgB", "n" + per));
        assertTrue(h.isTombstoned("rgB", "n0"));
        assertEquals(per, h.nameRevocationsOpen());
    }

    // ------------------------------------------------------------------ what the operator is told, and how a request ends

    /** Outstanding and refused ones are said again every ten minutes, with why; a rebind withdraws one; an unconfirmed rebind ends. */
    @Test
    void whatIsOutstandingIsSaidAgainAndARebindWithdrawsIt() throws Exception {
        Path f = newFile();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry g = open(f, true, log);
        joined(g, "rgB", "rc");
        assertNull(g.revoke("rgB/b1", "operator"));
        assertNull(g.revoke("rgX/x1", "operator"));
        long t0 = System.currentTimeMillis();
        log.clear();
        g.remindNameRevocations(t0 + 30_000);
        assertEquals(0, log.size(), "said again within the first minute");
        g.remindNameRevocations(t0 + 61_000);
        assertEquals(2, log.size());
        String b1 = log.stream().filter(l -> l.contains("rgB/b1")).findFirst().orElse(""), x1 = log.stream().filter(l -> l.contains("rgX/x1")).findFirst().orElse("");
        assertTrue(b1.contains("still OUTSTANDING") && b1.contains("its controller rgB/rc has not pinged this node since"), b1);
        assertTrue(b1.contains("a binding made there after this revocation is left alone"), b1);
        assertTrue(x1.contains("no controller of region rgX has joined this node"), x1);
        g.remindNameRevocations(t0 + 5 * 60_000);
        assertEquals(2, log.size(), "said again within ten minutes");
        // what the distribution saw of the region's controller
        g.noteNameRegion("rgB", "rgB/rc", "rgB/rc says nothing of revocations by name on its pings (an earlier build)");
        log.clear();
        g.remindNameRevocations(t0 + 12 * 60_000);
        assertTrue(log.stream().anyMatch(l -> l.contains("rgB/b1") && l.contains("it is not sent: rgB/rc says nothing of revocations by name")), log.toString());
        g.noteNameRegion("rgB", "rgB/rc", null);
        log.clear();
        g.remindNameRevocations(t0 + 23 * 60_000);
        assertTrue(log.stream().anyMatch(l -> l.contains("rgB/rc was sent the request 1 time(s)") && l.contains("has not answered")), log.toString());
        String h = g.nameRevocationsOutstanding();
        assertTrue(h.contains("2 NOT answered") && h.contains("0 whose answer was REFUSED") && h.contains("rgB/b1") && h.contains("rgX/x1"), h);

        // a refused answer is said again too
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", "22".repeat(16) + "|rgB/b1|K|" + "ab".repeat(32)));
        log.clear();
        g.remindNameRevocations(t0 + 40 * 60_000);
        assertTrue(log.stream().anyMatch(l -> l.contains("rgB/b1") && l.contains("was REFUSED")), log.toString());

        // the operator withdraws both with the existing rebind token: reserved no more, nothing outstanding
        Files.writeString(Path.of(f + ".rebind"), "rgB/b1=" + "cd".repeat(32) + "\nrgX/x1\n");
        log.clear();
        g.checkOperatorFiles();
        assertFalse(g.isTombstoned("rgB", "b1") || g.isTombstoned("rgX", "x1"));
        assertNull(g.nameRevocationsOutstanding());
        assertEquals(PeerTrustRegistry.NameRevocation.LIFTING, state(g, "rgB/b1"));
        assertEquals("cd".repeat(32), g.nameRevocation("rgB", "b1").allow);
        assertNull(g.nameRevocation("rgX", "x1").allow);
        assertEquals(2, g.nameRevocationsOpen());
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgX/x1") && l.contains("REBOUND by the operator") && l.contains("the FIRST key presented there for the name takes it")
                && l.contains("WITHDRAWN -- the key the name held is revoked nowhere")), log.toString());
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b1") && l.contains("key " + "cd".repeat(8) + " takes the name there")), log.toString());
        // the tokens are consumed: a restart does not lift anything again, and the state is what it was
        g.awaitRevocations();
        g = open(f, true, log);
        assertEquals(PeerTrustRegistry.NameRevocation.LIFTING, state(g, "rgB/b1"));
        assertEquals(2, g.nameRevocationsOpen());
        // revoked again while its rebind is not confirmed: a new request, the old rebind is gone
        String before = g.nameRevocation("rgB", "b1").id;
        assertNull(g.revoke("rgB/b1", "operator"));
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, state(g, "rgB/b1"));
        assertNotEquals(before, g.nameRevocation("rgB", "b1").id);
        assertTrue(g.isTombstoned("rgB", "b1"));
        // a rebind no region confirms in a day stops being sent and counted, and says so
        log.clear();
        g.remindNameRevocations(System.currentTimeMillis() + 25 * 3_600_000L);
        assertNull(g.nameRevocation("rgX", "x1"));
        assertTrue(log.stream().anyMatch(l -> l.contains("Rebind of rgX/x1") && l.contains("did not confirm it in 24 h") && l.contains("no longer")), log.toString());
        assertEquals(1, g.nameRevocationsOpen());
    }

    /** The records, their requests and states, and a region's answers survive a restart and a compaction; an earlier build reads the log. */
    @Test
    void recordsAndAnswersSurviveARestartAndACompaction() throws Exception {
        Path f = newFile();
        PeerTrustRegistry g = open(f, true, null);
        joined(g, "rgB", "rc");
        for (String n : List.of("b1", "b2", "b3")) assertNull(g.revoke("rgB/" + n, "operator"));
        String id1 = g.nameRevocation("rgB", "b1").id, id2 = g.nameRevocation("rgB", "b2").id;
        long t1 = g.nameRevocation("rgB", "b1").t;
        String k = "ab".repeat(32);
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", id1 + "|rgB/b1|K|" + k));
        assertNull(g.acceptNameAnswer("rgB", "rgB/rc", "33".repeat(16) + "|rgB/b2|K|" + k));
        g.awaitRevocations();
        for (int pass = 0; pass < 2; pass++) {
            if (pass == 1) {
                java.lang.reflect.Method compact = PeerTrustRegistry.class.getDeclaredMethod("compact");
                compact.setAccessible(true);
                compact.invoke(g);
            }
            g = open(f, true, null);
            assertFalse(g.isSealed(), g.sealReason());
            PeerTrustRegistry.NameRevocation r1 = g.nameRevocation("rgB", "b1");
            assertEquals(PeerTrustRegistry.NameRevocation.REPORTED, r1.state);
            assertEquals(id1, r1.id);
            assertEquals(t1, r1.t, "the time of the operator's act was lost");
            assertEquals(k, r1.key);
            assertEquals("rgB/rc", r1.by);
            assertEquals(PeerTrustRegistry.NameRevocation.REFUSED, state(g, "rgB/b2"));
            assertEquals(id2, g.nameRevocation("rgB", "b2").id);
            assertEquals(PeerTrustRegistry.NameRevocation.ASKED, state(g, "rgB/b3"));
            assertEquals(1, g.nameRevocationsOpen());
            assertEquals(1, g.nameRevocationsFor("rgB").size());
            assertTrue(g.isTombstoned("rgB", "b1") && g.isTombstoned("rgB", "b2") && g.isTombstoned("rgB", "b3"));
            assertTrue(g.revocations().isEmpty());
            assertTrue(g.nameRevocationsOutstanding().contains("1 NOT answered") && g.nameRevocationsOutstanding().contains("1 whose answer was REFUSED"));
        }
        // a node that is no global (as an earlier build would) reads the same log: the names are reserved, nothing else
        PeerTrustRegistry plain = open(f, false, null);
        assertFalse(plain.isSealed(), plain.sealReason());
        assertTrue(plain.isTombstoned("rgB", "b3"));

        // at a region's controller: what it answered
        Path rf = newFile();
        PeerTrustRegistry r = open(rf, false, null);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        r.applyNameRevocation("44".repeat(16), "rgB/b1", System.currentTimeMillis() + 1000, "rgG/gc");
        r.applyNameRevocation("55".repeat(16), "rgB/nobody", System.currentTimeMillis() + 1000, "rgG/gc");
        r.awaitRevocations();
        for (int pass = 0; pass < 2; pass++) {
            if (pass == 1) {
                java.lang.reflect.Method compact = PeerTrustRegistry.class.getDeclaredMethod("compact");
                compact.setAccessible(true);
                compact.invoke(r);
            }
            r = open(rf, false, null);
            assertFalse(r.isSealed(), r.sealReason());
            assertArrayEquals(new String[]{"K", fp(b1.getPublic())}, r.nameAnswer("44".repeat(16)));
            assertArrayEquals(new String[]{"N", null}, r.nameAnswer("55".repeat(16)));
            assertTrue(r.isRevokedKey(fp(b1.getPublic())) && r.isTombstoned("rgB", "b1") && r.isTombstoned("rgB", "nobody"));
            assertTrue(r.nameRefusal("rgB", "b1").contains("at the GLOBAL"), r.nameRefusal("rgB", "b1"));
        }
    }

    // ------------------------------------------------------------------ at the region's controller

    /** A request is of its time: a binding made after the operator's act (or within a ping of it) is left alone. */
    @Test
    void aRegionLeavesABindingMadeAfterTheOperatorsActAlone() throws Exception {
        TlsTestSupport.provider();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = new PeerTrustRegistry();
        r.setSelf(() -> "rgB/rc");
        setLog(r, log);
        KeyPair before = TlsTestSupport.rsa(), after = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", before.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgB", "b2", after.getPublic(), "discovery").ok);
        long now = System.currentTimeMillis();
        r.applyNameRevocation("a1".repeat(16), "rgB/b1", now + 60_000, "rgG/gc");        // the act is later than the binding
        r.applyNameRevocation("a2".repeat(16), "rgB/b2", now - 60_000, "rgG/gc");        // the binding is later than the act
        r.applyNameRevocation("a3".repeat(16), "rgB/rc", now + 60_000, "rgG/gc");        // its own name
        assertArrayEquals(new String[]{"K", fp(before.getPublic())}, r.nameAnswer("a1".repeat(16)));
        assertArrayEquals(new String[]{"A", null}, r.nameAnswer("a2".repeat(16)));
        assertArrayEquals(new String[]{"O", null}, r.nameAnswer("a3".repeat(16)));
        assertTrue(r.isRevokedFor(fp(before.getPublic()), "rgB") && r.isTombstoned("rgB", "b1"));
        assertFalse(r.isRevokedKey(fp(after.getPublic())) || r.isTombstoned("rgB", "b2") || r.isTombstoned("rgB", "rc"));
        assertTrue(r.bind("t1", "rgB", "b2", after.getPublic(), "discovery").ok);
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b2 NOT revoked") && l.contains("bound here after that revocation")), log.toString());
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b1 REVOKED (revoked by name at rgG/gc") && l.contains("connections are closed")), log.toString());
        // answered once: the same requests again, with another time, change nothing
        r.applyNameRevocation("a2".repeat(16), "rgB/b2", now + 60_000, "rgG/gc");
        assertFalse(r.isRevokedKey(fp(after.getPublic())), "a request answered before revoked a key when it was delivered again");
        assertArrayEquals(new String[]{"A", null}, r.nameAnswer("a2".repeat(16)));
        // malformed requests are nothing
        r.applyNameRevocation("nope", "rgB/b2", now + 60_000, "rgG/gc");
        r.applyNameRevocation("a4".repeat(16), "not a name", now + 60_000, "rgG/gc");
        assertFalse(r.isRevokedKey(fp(after.getPublic())));
        assertNull(r.nameAnswer("nope"));
    }

    /** A rebind lifts only the reservation its own request made, for the key it names, once; it survives nothing it should not. */
    @Test
    void aRebindLiftsOnlyItsOwnRequestsReservation() throws Exception {
        TlsTestSupport.provider();
        Path f = newFile();
        PeerTrustRegistry r = open(f, false, null);
        KeyPair b1 = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa(), next = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        String id1 = "11".repeat(16), id2 = "22".repeat(16);
        long later = System.currentTimeMillis() + 60_000;
        r.applyNameRevocation(id1, "rgB/b1", later, "rgG/gc");
        assertTrue(r.isRevokedFor(fp(b1.getPublic()), "rgB"), "not in force when the call returned");
        r.awaitRevocations();
        assertNull(r.nameLiftAnswer(id1, "rgB/b1"));
        // a rebind allowed for a request this node answered otherwise lifts nothing here
        r.applyNameRevocation(id2, "rgB/other", later, "rgG/gc");
        r.awaitRevocations();
        r.applyNameRebind(id2, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "a rebind of another request lifted the name");
        assertEquals("X", r.nameLiftAnswer(id2, "rgB/b1"));
        // the one for this request allows exactly its key, once
        r.applyNameRebind(id1, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", next.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        assertEquals("B", r.nameLiftAnswer(id1, "rgB/b1"));
        assertTrue(r.isRevokedKey(fp(b1.getPublic())), "the revoked key did not stay revoked");
        // the first request again, also after a restart: the new key stays; the rebind again allows nothing more
        r.applyNameRevocation(id1, "rgB/b1", later, "rgG/gc");
        r.awaitRevocations();
        r = open(f, false, null);
        r.applyNameRevocation(id1, "rgB/b1", System.currentTimeMillis() + 60_000, "rgG/gc");
        r.awaitRevocations();
        assertFalse(r.isRevokedKey(fp(fresh.getPublic())), "a request answered before revoked the key the name was rebound to");
        r.applyNameRebind(id1, "rgB/b1", fp(next.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", next.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
    }

    /**
     * A rebind that overtakes its revocation WITHDRAWS it: the request was never applied here and never will be, whatever
     * holds the name keeps it -- also with a token that names no key, and also when the request itself arrives afterwards.
     */
    @Test
    void aRequestWithdrawnBeforeItWasAppliedRevokesNothingLater() throws Exception {
        TlsTestSupport.provider();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = new PeerTrustRegistry();
        setLog(r, log);
        KeyPair holder = TlsTestSupport.rsa(), newcomer = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", holder.getPublic(), "discovery").ok);
        String id = "77".repeat(16);
        r.applyNameRebind(id, "rgB/b1", null, "rgG/gc");                               // a token without a key, for a request never seen here
        assertArrayEquals(new String[]{"W", null}, r.nameAnswer(id));
        assertFalse(r.isRevokedKey(fp(holder.getPublic())) || r.isTombstoned("rgB", "b1"), "the key the name holds was revoked");
        assertEquals("B", r.nameLiftAnswer(id, "rgB/b1"));
        assertTrue(log.stream().anyMatch(l -> l.contains("rgB/b1") && l.contains("WITHDRAWN there") && l.contains("nothing is revoked for it, now or later")), log.toString());
        // the withdrawn request arrives after all: nothing; nor does the keyless rebind let a newcomer take a name that is held
        r.applyNameRevocation(id, "rgB/b1", System.currentTimeMillis() + 60_000, "rgG/gc");
        r.applyNameRebind(id, "rgB/b1", null, "rgG/gc");
        assertFalse(r.isRevokedKey(fp(holder.getPublic())), "a withdrawn request revoked the key the name holds");
        assertFalse(r.bind("t1", "rgB", "b1", newcomer.getPublic(), "discovery").ok, "a newcomer took a name that is held");
        assertTrue(r.bind("t1", "rgB", "b1", holder.getPublic(), "discovery").ok);
        // where the request WAS applied, a token without a key lets the first key presented take the reserved name -- once
        String id2 = "88".repeat(16);
        r.applyNameRevocation(id2, "rgB/b1", System.currentTimeMillis() + 60_000, "rgG/gc");
        assertTrue(r.isTombstoned("rgB", "b1"));
        r.applyNameRebind(id2, "rgB/b1", null, "rgG/gc");
        assertTrue(r.bind("t1", "rgB", "b1", newcomer.getPublic(), "discovery").ok, "the first key presented did not take the rebound name");
        assertFalse(r.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertFalse(r.isRevokedKey(fp(newcomer.getPublic())));
    }

    /** A region's controller remembers the latest answers only (bounded), and that many. */
    @Test
    void aRegionRemembersABoundedNumberOfAnswers() throws Exception {
        PeerTrustRegistry r = new PeerTrustRegistry();
        long later = System.currentTimeMillis() + 60_000;
        int n = PeerTrustRegistry.NAME_ANSWERS_MAX + 10;
        for (int i = 0; i < n; i++) r.applyNameRevocation(String.format("%032x", i + 1), "rgB/n" + i, later, "rgG/gc");
        assertNull(r.nameAnswer(String.format("%032x", 1)), "more answers are kept than the bound");
        assertNotNull(r.nameAnswer(String.format("%032x", 11)));
        assertNotNull(r.nameAnswer(String.format("%032x", n)));
    }

    // ------------------------------------------------------------------ on the wire: who is heard, freshness, cost

    private static ControllerEngine node(String region, String agent, boolean global, String tenant) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", tenant);
        if (global) cfg.put("is_global", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = true;
        st.global = global;
        if (!global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static void join(ControllerEngine global, ControllerEngine region) {
        assertNull(reg(global).pin(region.cstate.getRegion(), region.cstate.getAgent(), leaf(region), PeerTrustRegistry.ROLE_CONTROLLER, false));
        reg(global).allowBridgePeer(region.cstate.getRegion(), region.cstate.getAgent());
        assertNull(reg(region).pin(global.cstate.getRegion(), global.cstate.getAgent(), leaf(global), PeerTrustRegistry.ROLE_CONTROLLER, true));
    }

    private static MsgEvent ping(ControllerEngine from) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    /** The pong the global makes for a real ping of {@code region}'s controller (not delivered). */
    private static MsgEvent pongFor(ControllerEngine global, ControllerEngine region) {
        MsgEvent p = ping(region);
        RevocationSync.onChildPing(region, p);
        p.setParam("cresco_caller", "controller:t1/" + region.cstate.getRegion() + "/" + region.cstate.getAgent());
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
        return p;
    }

    private static String requestId(MsgEvent pong, String name) {
        if (pong.getParam(RevocationSync.NAMES) == null) return null;
        RevocationSync.SignedNames l = GSON.fromJson(pong.getCompressedParam(RevocationSync.NAMES), RevocationSync.SignedNames.class);
        for (String i : l.items) { String[] p = i.split("\\|", -1); if (p[2].equals(name)) return p[1]; }
        return null;
    }

    /** Requests or answers as a node makes them with its OWN identity key and whatever it likes to say. */
    private static String signedBy(ControllerEngine signer, String kind, String issuer, String tenant, String to, String fresh, String... items) throws Exception {
        CertificateManager cm = signer.getCertificateManager();
        RevocationSync.SignedNames l = new RevocationSync.SignedNames();
        l.kind = kind; l.issuer = issuer; l.tenant = tenant; l.to = to; l.fresh = fresh; l.issued = System.currentTimeMillis();
        l.items = List.of(items);
        l.cert = Base64.getEncoder().encodeToString(cm.getPublicCertificate()[0].getEncoded());
        String signed = "cresco-name-revocations-v2|2|" + kind + "|" + issuer + "|" + tenant + "|" + to + "|" + fresh + "|" + l.issued + "|" + String.join(",", items);
        l.sig = Base64.getEncoder().encodeToString(cm.signWithIdentity(signed.getBytes(StandardCharsets.UTF_8)));
        return GSON.toJson(l);
    }

    private static void deliver(ControllerEngine global, ControllerEngine from, String caller, String answers) {
        MsgEvent p = ping(from);
        p.setCompressedParam(RevocationSync.NAME_REPORT, answers);
        if (caller != null) p.setParam("cresco_caller", caller);
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
    }

    /** Whose answers a global reads: its region's one controller's, in its tenant, addressed to it -- and only while one is open. */
    @Test
    void aGlobalReadsOnlyTheRegionsOneControllersAnswers() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, "t1"), regionB = node("rgB", "rc", false, "t1"), regionC = node("rgC", "rc", false, "t1");
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global);
        String k = "ab".repeat(32);
        // nothing open: an answer is not parsed, not verified, not logged
        deliver(global, regionB, "controller:t1/rgB/rc", "{ this is not json");
        deliver(global, regionB, "controller:t1/rgB/rc", signedBy(regionB, "report", "rgB/rc", "t1", "rgG/gc", "", "00".repeat(16) + "|rgB/b1|K|" + k));
        assertNull(RevocationSync.lastWarning(global), "an answer was read while no request is open");

        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        String id = requestId(pongFor(global, regionB), "rgB/b1");
        assertNotNull(id);
        String good = signedBy(regionB, "report", "rgB/rc", "t1", "rgG/gc", "", id + "|rgB/b1|K|" + k);
        // another region's controller (it even knows the request), or one that claims to be rgB's
        deliver(global, regionC, "controller:t1/rgC/rc", signedBy(regionC, "report", "rgC/rc", "t1", "rgG/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("IGNORED") && RevocationSync.lastWarning(global).contains("not of region rgC"), RevocationSync.lastWarning(global));
        deliver(global, regionC, "controller:t1/rgC/rc", signedBy(regionC, "report", "rgB/rc", "t1", "rgG/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("REFUSED, none read") && RevocationSync.lastWarning(global).contains("does not verify here as its issuer"), RevocationSync.lastWarning(global));
        // an agent of rgB bound at the global
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b7", Map.of("broker_security_enabled", "true"));
        assertTrue(atGlobal.bind("t1", "rgB", "b7", leaf(agent).getPublicKey(), "test").ok);
        deliver(global, agent, "agent:t1/rgB/b7", signedBy(agent, "report", "rgB/b7", "t1", "rgG/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("is not a controller bound here"), RevocationSync.lastWarning(global));
        // rgB's controller: altered after signing; for another node; stating another tenant; on a ping stamped as another caller's;
        // requests instead of answers
        deliver(global, regionB, "controller:t1/rgB/rc", good.replace(k, "cd".repeat(32)));
        assertTrue(RevocationSync.lastWarning(global).contains("the signature does not verify"), RevocationSync.lastWarning(global));
        deliver(global, regionB, "controller:t1/rgB/rc", signedBy(regionB, "report", "rgB/rc", "t1", "rgX/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("addressed to rgX/gc"), RevocationSync.lastWarning(global));
        deliver(global, regionB, "controller:t1/rgB/rc", signedBy(regionB, "report", "rgB/rc", "t2", "rgG/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("states tenant t2"), RevocationSync.lastWarning(global));
        deliver(global, regionB, "controller:t1/rgC/rc", good);
        assertTrue(RevocationSync.lastWarning(global).contains("the verified caller controller:t1/rgC/rc is not the controller rgB/rc"), RevocationSync.lastWarning(global));
        deliver(global, regionB, "controller:t1/rgB/rc", signedBy(regionB, "ask", "rgB/rc", "t1", "rgG/gc", "", id + "|rgB/b1|K|" + k));
        assertTrue(RevocationSync.lastWarning(global).contains("malformed"), RevocationSync.lastWarning(global));
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, atGlobal.nameRevocation("rgB", "b1").state, "an answer that was not read settled the request");

        // a second controller bound under the region: the real one's answer is not read either, until the operator revokes it
        ControllerEngine second = node("rgB", "zz", false, "t1");
        assertNull(atGlobal.pin("rgB", "zz", leaf(second), PeerTrustRegistry.ROLE_CONTROLLER, false));
        deliver(global, regionB, "controller:t1/rgB/rc", good);
        assertTrue(RevocationSync.lastWarning(global).contains("2 controllers are recorded here under region rgB"), RevocationSync.lastWarning(global));
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, atGlobal.nameRevocation("rgB", "b1").state);
        assertNull(atGlobal.revoke("rgB/zz", "operator"));
        deliver(global, regionB, "controller:t1/rgB/rc", good);
        assertEquals(PeerTrustRegistry.NameRevocation.REPORTED, atGlobal.nameRevocation("rgB", "b1").state);
        assertTrue(atGlobal.revocations().size() == 1 && !atGlobal.isRevokedKey(k), "something other than the operator's revocation of rgB/zz is on the list");
    }

    /** What a region's controller takes: from the global it joined, in its tenant, addressed to it, for its own names, fresh. */
    @Test
    void aRegionTakesARequestOnlyFromItsGlobalForItsOwnNamesAndFresh() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, "t1"), regionB = node("rgB", "rc", false, "t1"), regionC = node("rgC", "rc", false, "t1");
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atB = reg(regionB), atC = reg(regionC);
        KeyPair b1 = TlsTestSupport.rsa(), c1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        assertTrue(atC.bind("t1", "rgC", "c1", c1.getPublic(), "discovery").ok);
        Thread.sleep(40);
        assertNull(reg(global).revoke("rgB/b1", "operator"));
        MsgEvent forB = pongFor(global, regionB);
        String asks = forB.getCompressedParam(RevocationSync.NAMES);
        assertNotNull(asks);
        // an agent takes none, whoever signed it
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b5", Map.of("broker_security_enabled", "true"));
        assertNull(reg(agent).pin("rgG", "gc", leaf(global), PeerTrustRegistry.ROLE_CONTROLLER, true));
        RevocationSync.onParentPong(agent, forB, "rgG", "gc");
        assertFalse(reg(agent).isTombstoned("rgB", "b1"));
        // addressed to another controller
        MsgEvent toC = ping(regionC);
        RevocationSync.onChildPing(regionC, toC);
        String freshC = toC.getParam(RevocationSync.NAMES_FRESH);
        toC.setCompressedParam(RevocationSync.NAMES, asks);
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertTrue(RevocationSync.lastWarning(regionC).contains("addressed to rgB/rc"), RevocationSync.lastWarning(regionC));
        // signed by the global, fresh and addressed to rgC -- for a name of ANOTHER region: ignored
        String id = "99".repeat(16);
        toC.setCompressedParam(RevocationSync.NAMES, signedBy(global, "ask", "rgG/gc", "t1", "rgC/rc", freshC, "A|" + id + "|rgB/b1|0", "A|" + "98".repeat(16) + "|rgG/gc|0"));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertTrue(RevocationSync.lastWarning(regionC).contains("IGNORED: it is not a name of this node's region rgC"), RevocationSync.lastWarning(regionC));
        assertFalse(atC.isTombstoned("rgB", "b1") || atC.isTombstoned("rgG", "gc"));
        // for its own name, fresh, but signed by a controller that is not the global it joined; and stating another tenant
        toC.setCompressedParam(RevocationSync.NAMES, signedBy(regionB, "ask", "rgB/rc", "t1", "rgC/rc", freshC, "A|" + id + "|rgC/c1|0"));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertFalse(atC.isRevokedKey(fp(c1.getPublic())), "a request of a controller that is not its global was applied");
        toC.setCompressedParam(RevocationSync.NAMES, signedBy(global, "ask", "rgG/gc", "t2", "rgC/rc", freshC, "A|" + id + "|rgC/c1|0"));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertTrue(RevocationSync.lastWarning(regionC).contains("states tenant t2"), RevocationSync.lastWarning(regionC));
        // not fresh: no value, a made-up value, a value of four pings ago
        toC.setCompressedParam(RevocationSync.NAMES, signedBy(global, "ask", "rgG/gc", "t1", "rgC/rc", "0123456789abcdef", "A|" + id + "|rgC/c1|0"));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertTrue(RevocationSync.lastWarning(regionC).contains("it is not fresh"), RevocationSync.lastWarning(regionC));
        for (int i = 0; i < 3; i++) RevocationSync.onChildPing(regionC, ping(regionC));
        toC.setCompressedParam(RevocationSync.NAMES, signedBy(global, "ask", "rgG/gc", "t1", "rgC/rc", freshC, "A|" + id + "|rgC/c1|0"));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        atC.awaitRevocations();
        assertFalse(atC.isRevokedKey(fp(c1.getPublic())), "a request that answers none of its last pings was applied");
        assertNull(atC.nameAnswer(id));
        // (the same request made as these were, answering its last ping: taken -- what was refused above was refused for its reason)
        MsgEvent last = ping(regionC);
        RevocationSync.onChildPing(regionC, last);
        last.setCompressedParam(RevocationSync.NAMES, signedBy(global, "ask", "rgG/gc", "t1", "rgC/rc", last.getParam(RevocationSync.NAMES_FRESH), "A|" + id + "|rgC/c1|0"));
        RevocationSync.onParentPong(regionC, last, "rgG", "gc");
        atC.awaitRevocations();
        assertTrue(atC.isRevokedKey(fp(c1.getPublic())), "a fresh request of its global was not applied");
        assertArrayEquals(new String[]{"K", fp(c1.getPublic())}, atC.nameAnswer(id));
        // the genuine one, to the controller it was made for: applied; the same pong again changes nothing
        RevocationSync.onParentPong(regionB, forB, "rgG", "gc");
        atB.awaitRevocations();
        assertTrue(atB.isRevokedKey(fp(b1.getPublic())) && atB.isTombstoned("rgB", "b1"));
        RevocationSync.onParentPong(regionB, forB, "rgG", "gc");
        atB.awaitRevocations();
        assertEquals(1, atB.revocations().size());
    }

    /** A region controller's ping carries one short value more; an agent's nothing; a pong nothing while no request is open. */
    @Test
    void whatAPingAndAPongCost() throws Exception {
        ControllerEngine global = node("rgG", "gc", true, "t1"), regionB = node("rgB", "rc", false, "t1");
        join(global, regionB);
        assertTrue(reg(global).isGlobal(), "is_global did not reach the registry");
        MsgEvent p = ping(regionB);
        RevocationSync.onChildPing(regionB, p);
        String fresh = p.getParam(RevocationSync.NAMES_FRESH);
        assertTrue(fresh != null && fresh.matches("[0-9a-f]{16}"), fresh);
        assertNull(p.getParam(RevocationSync.NAME_REPORT));
        MsgEvent p2 = ping(regionB);
        RevocationSync.onChildPing(regionB, p2);
        assertNotEquals(fresh, p2.getParam(RevocationSync.NAMES_FRESH), "the value is not new on each ping");
        p.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, p, true);
        assertNull(p.getParam(RevocationSync.NAMES));
        assertNull(p.getParam(RevocationSync.NAMES_FRESH), "the parent left the child's value on the pong");
        // an agent says nothing of revocations by name
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b5", Map.of("broker_security_enabled", "true"));
        MsgEvent ap = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b5", null, "rgB", "rc", null, true, false);
        RevocationSync.onChildPing(agent, ap);
        assertNull(ap.getParam(RevocationSync.NAMES_FRESH));
        // open: each pong's request is signed for that ping (its fresh value), and an unverified pinger is sent none
        assertNull(reg(global).revoke("rgB/b1", "operator"));
        MsgEvent a = pongFor(global, regionB), b = pongFor(global, regionB);
        RevocationSync.SignedNames la = GSON.fromJson(a.getCompressedParam(RevocationSync.NAMES), RevocationSync.SignedNames.class);
        RevocationSync.SignedNames lb = GSON.fromJson(b.getCompressedParam(RevocationSync.NAMES), RevocationSync.SignedNames.class);
        assertNotEquals(la.fresh, lb.fresh);
        assertNotEquals(la.sig, lb.sig);
        assertEquals("rgB/rc", la.to);
        assertEquals("t1", la.tenant);
        assertTrue(la.items.get(0).matches("A\\|[0-9a-f]{32}\\|rgB/b1\\|\\d+"), la.items.toString());
        MsgEvent anon = ping(regionB);
        RevocationSync.onChildPing(regionB, anon);
        RevocationSync.onPingAtParent(global, anon, true);
        assertNull(anon.getParam(RevocationSync.NAMES), "a revocation by name was sent to an unverified pinger");
        // a region's own handler (its agents' pings) never sends one
        MsgEvent ap2 = new MsgEvent(MsgEvent.Type.EXEC, "rgG", "g1", null, "rgG", "gc", null, true, false);
        ap2.setParam(RevocationSync.NAMES_FRESH, "0123456789abcdef");
        ap2.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, ap2, false);
        assertNull(ap2.getParam(RevocationSync.NAMES));
    }
}
