package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Revocations by name, at the registry and with what this change added to its interface: what a global records and
 * accepts of a region's answer (each refusal by its reason, and that it is logged), the bound on open ones, the reminders,
 * what survives a restart and a compaction, what a region does with a request it has applied before, and what a ping
 * costs. {@link RevocationByNameTest} holds the behaviour on the wire.
 */
class RevocationByNameRegistryTest {

    private static final String ID = "[0-9a-f]{32}";

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    private static PeerTrustRegistry global(List<String> log) {
        PeerTrustRegistry r = new PeerTrustRegistry();
        r.setGlobal(true);
        if (log != null) setLog(r, log);
        return r;
    }

    private static void setLog(PeerTrustRegistry r, List<String> log) {
        try {
            java.lang.reflect.Field f = PeerTrustRegistry.class.getDeclaredField("log");
            f.setAccessible(true);
            f.set(r, (java.util.function.Consumer<String>) log::add);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static PeerTrustRegistry open(Path f, boolean global, List<String> log) {
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.global = global;
        return PeerTrustRegistry.open(f, o, log == null ? m -> { } : log::add);
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("byname").resolve("security").resolve("peer-trust.json");
    }

    // ------------------------------------------------------------------ what a node that is not a global does

    /** Only a global takes a revocation by name of a name it holds no key for; elsewhere it is refused as before. */
    @Test
    void aNodeThatIsNoGlobalStillRefusesANameItHoldsNoKeyFor() {
        PeerTrustRegistry region = new PeerTrustRegistry();
        assertEquals("no key is bound to rgB/b1 (revoke its key fingerprint instead)", region.revoke("rgB/b1", "operator"));
        assertFalse(region.isTombstoned("rgB", "b1"));
        assertEquals(0, region.nameRevocationsOpen());
    }

    /** A name of the global's own region is bound there or nowhere: reserved, and nobody is asked. */
    @Test
    void aNameOfTheGlobalsOwnRegionIsReservedAndNobodyIsAsked() throws Exception {
        TlsTestSupport.provider();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry g = global(log);
        assertTrue(g.bind("t1", "rgG", "gc", TlsTestSupport.rsa().getPublic(), "own", PeerTrustRegistry.ROLE_SELF, true).ok);
        assertNull(g.revoke("rgG/ghost", "operator"));
        assertTrue(g.isTombstoned("rgG", "ghost"));
        assertEquals(0, g.nameRevocationsOpen());
        assertNull(g.nameRevocationsOutstanding());
        assertEquals("rgG/gc is this node's own identity", g.revoke("rgG/gc", "operator"));
        // a global that holds no binding of its own identity learns its region from its state, and settles what it asked
        PeerTrustRegistry h = global(log);
        assertNull(h.revoke("rgG/ghost", "operator"));
        assertEquals(1, h.nameRevocationsOpen());
        h.noteSelfRegion("rgG");
        assertEquals(0, h.nameRevocationsOpen());
        assertTrue(h.isTombstoned("rgG", "ghost"));
        assertNull(h.revoke("rgG/other", "operator"));
        assertEquals(0, h.nameRevocationsOpen());
    }

    // ------------------------------------------------------------------ what a global takes of an answer

    /** Each answer a global does not take, by its reason; then the one it takes, once. */
    @Test
    void whatAGlobalTakesOfARegionsAnswer() throws Exception {
        TlsTestSupport.provider();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry g = global(log);
        KeyPair ownAgent = TlsTestSupport.rsa(), ctrlC = TlsTestSupport.rsa(), agentB = TlsTestSupport.rsa(), ctrlB = TlsTestSupport.rsa();
        assertTrue(g.bind("t1", "rgG", "g1", ownAgent.getPublic(), "discovery").ok);
        assertTrue(g.bind("t1", "rgC", "rc", ctrlC.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(g.bind("t1", "rgB", "rc", ctrlB.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        RegionCA caC = RegionCA.generate("t1", "rgC", 2048, 1);
        assertNull(g.trustCa(caC.caCert(), "rgC", "t1", "federated"));
        assertNull(g.revoke("rgB/b1", "operator"));
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation("rgB", "b1");
        assertNotNull(nr);
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, nr.state);
        assertTrue(nr.id.matches(ID));
        assertEquals(1, g.nameRevocationsOpen());
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b1 REVOKED by name") && l.contains("RESERVED here") && l.contains("NOT confirmed")), log.toString());
        String k = fp(agentB.getPublic());
        String stale = "00112233445566778899aabbccddeeff";

        assertTrue(g.acceptNameReport("rgC", "rgC/rc", nr.id, "rgB/b1", "K", k).contains("not of region rgC"), "another region's controller");
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/zz", "K", k).contains("asked nobody"), "a name nobody asked about");
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", stale, "rgB/b1", "K", k).contains("replayed"), "another request's answer");
        assertEquals("malformed", g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", "not-a-key"));
        assertEquals("malformed", g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", null));
        assertEquals("malformed", g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "?", k));
        assertEquals("malformed", g.acceptNameReport("rgB", "rgB/rc", "short", "rgB/b1", "K", k));
        // keys this node knows as something that is not that region's node
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", fp(ownAgent.getPublic())).contains("bound here to rgG/g1, outside region rgB"));
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", fp(ctrlC.getPublic())).contains("outside region rgB"));
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", fp(ctrlB.getPublic())).contains("revokes no controller"), "its own controller's key");
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", fp(caC.caCert().getPublicKey())).contains("region CA"));
        String mine = fp(TlsTestSupport.rsa().getPublic());
        g.noteOwnKey(mine);
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", mine).contains("THIS node's own key"));
        assertEquals(0, g.revocations().size(), "a refused answer revoked a key");
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, g.nameRevocation("rgB", "b1").state);
        assertNotNull(g.nameRevocationsOutstanding());

        // the answer that counts
        assertNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", k.toUpperCase()));
        assertEquals(PeerTrustRegistry.NameRevocation.KEY, g.nameRevocation("rgB", "b1").state);
        assertEquals(k, g.nameRevocation("rgB", "b1").key);
        assertTrue(g.isRevokedFor(k, "rgC") && g.isRevokedFor(k, null));
        assertEquals(0, g.nameRevocationsOpen());
        assertNull(g.nameRevocationsOutstanding());
        assertTrue(log.stream().anyMatch(l -> l.contains("Name revocation of rgB/b1 CONFIRMED by rgB/rc") && l.contains("REVOKED here on its report")), log.toString());
        // it goes down marked as region rgB's word, and without the name (the name is reserved here and at its region only)
        PeerTrustRegistry.ListEntry e = PeerTrustRegistry.ListEntry.of(g.revocationsAfter(0, 10, false).get(0));
        assertEquals("rgB", e.via);
        assertNull(e.name);
        assertEquals(k + "||||rgB", e.format());
        assertEquals("rgB", PeerTrustRegistry.ListEntry.parse(e.format()).via);
        // answered once
        assertNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", k), "the same answer again");
        assertTrue(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", fp(TlsTestSupport.rsa().getPublic())).contains("answered once"));
        assertEquals(1, g.revocations().size());
    }

    /** A region's word is applied nowhere to a key bound outside that region; the parent's own word is. */
    @Test
    void anEntryMadeOnARegionsReportIsNotAppliedToAKeyBoundOutsideThatRegion() throws Exception {
        TlsTestSupport.provider();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry atC = new PeerTrustRegistry();
        setLog(atC, log);
        KeyPair c1 = TlsTestSupport.rsa(), free = TlsTestSupport.rsa();
        assertTrue(atC.bind("t1", "rgC", "c1", c1.getPublic(), "discovery").ok);
        String k = fp(c1.getPublic()), k2 = fp(free.getPublic());
        int n = atC.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(k, null, null, 0, "rgB"),
                new PeerTrustRegistry.ListEntry(k2, null, null, 0, "rgB")), "revocation list of rgG/gc", null, false);
        assertEquals(1, n, "the key bound nowhere here was not taken, or the one bound here to rgC/c1 was");
        assertFalse(atC.isRevokedKey(k));
        assertTrue(atC.isRevokedFor(k2, "rgC"));
        assertFalse(atC.isTombstoned("rgC", "c1"));
        assertTrue(log.stream().anyMatch(l -> l.contains("REFUSED") && l.contains("on the report of region rgB") && l.contains("rgC/c1")), log.toString());
        // the same key as the parent's own word
        assertEquals(1, atC.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(k, null, null, 0)), "revocation list of rgG/gc", null, false));
        assertTrue(atC.isTombstoned("rgC", "c1"));
        // a child's list cannot carry a mark, and a malformed one is no entry
        assertNull(PeerTrustRegistry.ListEntry.parse(k + "|rgB|||rgB"));
        assertNull(PeerTrustRegistry.ListEntry.parse(k + "||||not a region"));
    }

    // ------------------------------------------------------------------ bounds, reminders, durability

    /** At most {@link PeerTrustRegistry#NAME_REVOCATIONS_OPEN_MAX} are open at once; a key fingerprint can always be revoked. */
    @Test
    void theOpenRevocationsByNameAreBounded() {
        PeerTrustRegistry g = global(null);
        for (int i = 0; i < PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX; i++) assertNull(g.revoke("rgB/n" + i, "operator"));
        assertEquals(PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX, g.nameRevocationsOpen());
        String why = g.revoke("rgB/onemore", "operator");
        assertNotNull(why);
        assertTrue(why.contains("still open"), why);
        assertFalse(g.isTombstoned("rgB", "onemore"));
        assertNull(g.revoke("ab".repeat(32), "operator"));
        // one settled: room for one more
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation("rgB", "n0");
        assertNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/n0", "N", null));
        assertNull(g.revoke("rgB/onemore", "operator"));
        assertEquals(PeerTrustRegistry.NAME_REVOCATIONS_OPEN_MAX, g.nameRevocationsFor("rgB").size());
        assertEquals(0, g.nameRevocationsFor("rgC").size());
    }

    /** What is outstanding is said again, each at most every ten minutes, with what the region did so far. */
    @Test
    void anOutstandingRevocationIsSaidAgainEveryTenMinutes() {
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry g = global(log);
        assertNull(g.revoke("rgB/b1", "operator"));
        long t0 = System.currentTimeMillis();
        log.clear();
        g.remindNameRevocations(t0 + 30_000);
        assertEquals(0, log.size(), "said again within the first minute");
        g.remindNameRevocations(t0 + 61_000);
        assertEquals(1, log.size());
        assertTrue(log.get(0).contains("Name revocation of rgB/b1") && log.get(0).contains("still OUTSTANDING")
                && log.get(0).contains("no controller of region rgB has pinged"), log.get(0));
        g.remindNameRevocations(t0 + 5 * 60_000);
        assertEquals(1, log.size(), "said again within ten minutes");
        g.noteNameServed("rgB", "rgB/rc", false);
        g.remindNameRevocations(t0 + 12 * 60_000);
        assertEquals(2, log.size());
        assertTrue(log.get(1).contains("rgB/rc is on an earlier build"), log.get(1));
        g.noteNameServed("rgB", "rgB/rc", true);
        g.remindNameRevocations(t0 + 23 * 60_000);
        assertTrue(log.get(2).contains("was sent the request") && log.get(2).contains("has not answered"), log.get(2));
        // a refused answer is what it says next
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation("rgB", "b1");
        String mine = "cd".repeat(32);
        g.noteOwnKey(mine);
        assertNotNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", mine));
        g.remindNameRevocations(t0 + 34 * 60_000);
        assertTrue(log.get(3).contains("its answer was REFUSED here"), log.get(3));
        // settled: nothing more
        assertNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "N", null));
        log.clear();
        g.remindNameRevocations(t0 + 60 * 60_000);
        assertEquals(0, log.size());
    }

    /** The record, its request, the reported key and its mark survive a restart and a compaction. */
    @Test
    void aRevocationByNameSurvivesARestartAndACompaction() throws Exception {
        Path f = newFile();
        PeerTrustRegistry g = open(f, true, null);
        assertNull(g.revoke("rgB/b1", "operator"));
        assertNull(g.revoke("rgB/b2", "operator"));
        String id = g.nameRevocation("rgB", "b1").id;
        g = open(f, true, null);
        assertEquals(id, g.nameRevocation("rgB", "b1").id, "the request changed across a restart");
        assertEquals(2, g.nameRevocationsOpen());
        assertTrue(g.isTombstoned("rgB", "b1"));
        String k = "ab".repeat(32);
        assertNull(g.acceptNameReport("rgB", "rgB/rc", id, "rgB/b1", "K", k));
        g.awaitRevocations();
        java.lang.reflect.Method compact = PeerTrustRegistry.class.getDeclaredMethod("compact");
        compact.setAccessible(true);
        compact.invoke(g);
        g = open(f, true, null);
        assertFalse(g.isSealed(), g.sealReason());
        assertEquals(PeerTrustRegistry.NameRevocation.KEY, g.nameRevocation("rgB", "b1").state);
        assertEquals(k, g.nameRevocation("rgB", "b1").key);
        assertEquals(PeerTrustRegistry.NameRevocation.ASKED, g.nameRevocation("rgB", "b2").state);
        assertEquals(1, g.nameRevocationsOpen());
        assertTrue(g.isRevokedFor(k, "rgC"));
        assertEquals("rgB", g.revocationsAfter(0, 10, false).get(0).via, "the mark of the region's word was lost");
        // a registry of a node that is no global (or an earlier build) reads the same log: reserved, nothing else
        PeerTrustRegistry plain = open(f, false, null);
        assertFalse(plain.isSealed(), plain.sealReason());
        assertTrue(plain.isTombstoned("rgB", "b2"));
    }

    // ------------------------------------------------------------------ at the region

    /** A request is applied once: delivered again after the name was rebound, it does not revoke the new key. */
    @Test
    void aRegionAppliesARequestOnceAndALiftOnlyLiftsItsOwnRequest() throws Exception {
        TlsTestSupport.provider();
        Path f = newFile();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, false, log);
        KeyPair b1 = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa(), next = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        String k = fp(b1.getPublic());
        String id1 = "11".repeat(16), id2 = "22".repeat(16);
        assertNull(r.nameAnswer(id1));
        assertNull(r.applyNameRevocation(id1, "rgB/b1", "rgG/gc"));
        assertTrue(r.isRevokedFor(k, "rgB"), "not in force when the call returned");
        r.awaitRevocations();
        assertEquals(k, r.nameAnswer(id1));
        assertTrue(r.isTombstoned("rgB", "b1"));
        assertTrue(r.nameRefusal("rgB", "b1").contains("at the GLOBAL"), r.nameRefusal("rgB", "b1"));
        assertTrue(log.stream().anyMatch(l -> l.contains("Name rgB/b1 REVOKED (revoked by name at rgG/gc") && l.contains("connections are closed")), log.toString());
        assertEquals("malformed", r.applyNameRevocation("nope", "rgB/b1", "rgG/gc"));
        // a rebind allowed for ANOTHER request lifts nothing; the one for this request allows exactly its key
        r.allowRebindFromParent(id2, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "a rebind of another request lifted the name");
        assertNull(r.nameLiftAnswer(id1, "rgB/b1"));
        assertEquals("X", r.nameLiftAnswer(id2, "rgB/b1"));
        r.allowRebindFromParent(id1, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", next.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        assertEquals("B", r.nameLiftAnswer(id1, "rgB/b1"));
        // the first request again (its answer was lost, or it is replayed), also after a restart: the new key stays
        assertNull(r.applyNameRevocation(id1, "rgB/b1", "rgG/gc"));
        r.awaitRevocations();
        assertFalse(r.isRevokedKey(fp(fresh.getPublic())), "a request applied before revoked the key the name was rebound to");
        r = open(f, false, log);
        assertEquals(k, r.nameAnswer(id1), "what was applied for a request was forgotten at a restart");
        assertNull(r.applyNameRevocation(id1, "rgB/b1", "rgG/gc"));
        r.awaitRevocations();
        assertFalse(r.isRevokedKey(fp(fresh.getPublic())));
        // the rebind again: consumed, it allows nothing more
        r.allowRebindFromParent(id1, "rgB/b1", fp(next.getPublic()), "rgG/gc");
        assertFalse(r.bind("t1", "rgB", "b1", next.getPublic(), "discovery").ok);
        // a NEW request revokes what is bound now
        assertNull(r.applyNameRevocation(id2, "rgB/b1", "rgG/gc"));
        r.awaitRevocations();
        assertTrue(r.isRevokedKey(fp(fresh.getPublic())));
        assertEquals(fp(fresh.getPublic()), r.nameAnswer(id2));
        // its own identity is never revoked for its global
        KeyPair self = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "rc", self.getPublic(), "own", PeerTrustRegistry.ROLE_SELF, true).ok);
        assertNotNull(r.applyNameRevocation("33".repeat(16), "rgB/rc", "rgG/gc"));
        assertFalse(r.isRevokedKey(fp(self.getPublic())));
    }

    /**
     * A rebind whose revocation never reached the region (it was away while it was asked): the key the name holds is
     * revoked first, then the rebind is taken -- unless the name already holds the very key the rebind allows.
     */
    @Test
    void aRebindThatOvertakesItsRevocationStillRevokesTheOldKey() throws Exception {
        TlsTestSupport.provider();
        PeerTrustRegistry r = new PeerTrustRegistry();
        KeyPair old = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "b1", old.getPublic(), "discovery").ok);
        String id = "44".repeat(16);
        r.allowRebindFromParent(id, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertTrue(r.isRevokedFor(fp(old.getPublic()), "rgB"), "the key the name held was not revoked");
        assertEquals(fp(old.getPublic()), r.nameAnswer(id));
        assertNull(r.nameLiftAnswer(id, "rgB/b1"), "the rebind was confirmed before the new key presented");
        assertFalse(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "bound before the rebind was taken");
        r.allowRebindFromParent(id, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");        // the next pong
        assertTrue(r.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        assertEquals("B", r.nameLiftAnswer(id, "rgB/b1"));
        // the name already holds the allowed key: nothing is revoked
        PeerTrustRegistry q = new PeerTrustRegistry();
        assertTrue(q.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
        q.allowRebindFromParent(id, "rgB/b1", fp(fresh.getPublic()), "rgG/gc");
        assertFalse(q.isRevokedKey(fp(fresh.getPublic())));
        assertEquals("B", q.nameLiftAnswer(id, "rgB/b1"));
        assertNull(q.nameAnswer(id));
    }

    /** Pre-provisioned: a global that never verified the name's certificate asks its region, which revokes what it verified. */
    @Test
    void preProvisionedTheGlobalAsksTheRegionThatVerifiedTheCertificate() throws Exception {
        TlsTestSupport.provider();
        long now = System.currentTimeMillis();
        PkiTestSupport.Ca root = PkiTestSupport.root("Fabric Root");
        PkiTestSupport.Node b1 = PkiTestSupport.node(root, "t1", "rgB", "b1", false, now - 60_000, 24L * 3600 * 1000);
        PeerTrustRegistry g = global(null), r = new PeerTrustRegistry();
        g.setPki(List.of(root.cert), null);
        r.setPki(List.of(root.cert), null);
        assertNotNull(r.verifiedIdentity(b1.chain));                                   // the region verified it; the global never did
        assertNull(g.revoke("rgB/b1", "operator"));
        PeerTrustRegistry.NameRevocation nr = g.nameRevocation("rgB", "b1");
        assertNotNull(nr, "a pre-provisioned global that never saw the name's key asked nobody");
        assertNull(g.verifiedIdentity(b1.chain));
        assertNull(r.applyNameRevocation(nr.id, "rgB/b1", "rgG/gc"));
        String k = fp(b1.kp.getPublic());
        assertEquals(k, r.nameAnswer(nr.id));
        assertNull(r.verifiedIdentity(b1.chain), "the region still verifies the revoked certificate");
        assertTrue(r.isTombstoned("rgB", "b1"));
        assertNull(g.acceptNameReport("rgB", "rgB/rc", nr.id, "rgB/b1", "K", k));
        // a certificate names its region, and a pre-provisioned node holds no bindings to tell a region's word by: the
        // key is refused for that region's names -- here, and at a pre-provisioned node that gets it from a global that is not
        assertTrue(g.isRevokedFor(k, "rgB"));
        assertFalse(g.isRevokedFor(k, "rgC"), "a pre-provisioned global applied a region's word to other regions' names");
        assertEquals(k + "|rgB||", PeerTrustRegistry.ListEntry.of(g.revocationsAfter(0, 10, false).get(0)).format());
        PeerTrustRegistry other = new PeerTrustRegistry();
        other.setPki(List.of(root.cert), null);
        PkiTestSupport.Node c1 = PkiTestSupport.node(root, "t1", "rgC", "c1", false, now - 60_000, 24L * 3600 * 1000);
        String kc = fp(c1.kp.getPublic());                                             // never verified here (or forgotten at a restart)
        assertEquals(1, other.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(kc, null, null, 0, "rgB")), "revocation list of rgG/gc", null, false));
        assertNotNull(other.verifiedIdentity(c1.chain), "region rgB's word refused region rgC's certificate at a pre-provisioned node");
        // the operator's own revocation of the fingerprint: every name
        assertNull(g.revoke(k, "operator"));
        assertTrue(g.isRevokedFor(k, "rgC"));
    }

    // ------------------------------------------------------------------ on the wire: cost, and what is logged

    private static ControllerEngine node(String region, String agent, boolean global) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, global ? Map.of("broker_security_enabled", "true", "is_global", "true")
                : Map.of("broker_security_enabled", "true"));
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = true;
        st.global = global;
        if (!global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static MsgEvent ping(ControllerEngine region) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, region.cstate.getRegion(), region.cstate.getAgent(), null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    /** While no name is revoked a ping carries one short parameter more and a pong nothing; an ignored answer is logged. */
    @Test
    void aPingCostsOneParameterAndAnIgnoredAnswerIsLogged() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        X509Certificate rb = regionB.getCertificateManager().getPublicCertificate()[0], gc = global.getCertificateManager().getPublicCertificate()[0];
        PeerTrustRegistry atGlobal = global.getCertificateManager().getPeerTrust(), atB = regionB.getCertificateManager().getPeerTrust();
        assertNull(atGlobal.pin("rgB", "rc", rb, PeerTrustRegistry.ROLE_CONTROLLER, false));
        assertNull(atB.pin("rgG", "gc", gc, PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertTrue(atGlobal.isGlobal(), "is_global did not reach the registry");
        MsgEvent p = ping(regionB);
        RevocationSync.onChildPing(regionB, p);
        assertEquals("names", p.getParam(RevocationSync.CAP));
        assertNull(p.getParam(RevocationSync.NAME_REPORT));
        p.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, p, true);
        assertNull(p.getParam(RevocationSync.NAMES));
        assertNull(p.getParam(RevocationSync.CAP), "the parent left the child's parameter on the pong");
        // open: the same signed request is served again without signing (cached until it changes)
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        List<String> served = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MsgEvent q = ping(regionB);
            q.setParam(RevocationSync.CAP, RevocationSync.CAP_NAMES);
            q.setParam("cresco_caller", "controller:t1/rgB/rc");
            RevocationSync.onPingAtParent(global, q, true);
            served.add(q.getCompressedParam(RevocationSync.NAMES));
        }
        assertNotNull(served.get(0));
        assertEquals(served.get(0), served.get(1));
        assertEquals(served.get(0), served.get(2));
        // an unverified pinger (no caller the broker stamped, no signed list) is sent no request
        MsgEvent anon = ping(regionB);
        anon.setParam(RevocationSync.CAP, RevocationSync.CAP_NAMES);
        RevocationSync.onPingAtParent(global, anon, true);
        assertNull(anon.getParam(RevocationSync.NAMES), "a revocation by name was sent to an unverified pinger");
        // an agent takes no request, whoever signed it
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b5", Map.of("broker_security_enabled", "true"));
        assertNull(agent.getCertificateManager().getPeerTrust().pin("rgG", "gc", gc, PeerTrustRegistry.ROLE_CONTROLLER, true));
        MsgEvent toAgent = ping(regionB);
        toAgent.setCompressedParam(RevocationSync.NAMES, served.get(0));
        RevocationSync.onParentPong(agent, toAgent, "rgG", "gc");
        assertFalse(agent.getCertificateManager().getPeerTrust().isTombstoned("rgB", "b1"));
        // a region takes a request only from the controller it joined, and only one addressed to it
        ControllerEngine regionC = node("rgC", "rc", false);
        assertNull(regionC.getCertificateManager().getPeerTrust().pin("rgG", "gc", gc, PeerTrustRegistry.ROLE_CONTROLLER, true));
        MsgEvent toC = ping(regionC);
        toC.setCompressedParam(RevocationSync.NAMES, served.get(0));
        RevocationSync.onParentPong(regionC, toC, "rgG", "gc");
        assertTrue(RevocationSync.lastWarning(regionC).contains("addressed to rgB/rc"), RevocationSync.lastWarning(regionC));
        assertFalse(regionC.getCertificateManager().getPeerTrust().isTombstoned("rgB", "b1"));
        // an answer the global ignores is logged, with its reason
        MsgEvent pong = ping(regionB);
        pong.setCompressedParam(RevocationSync.NAMES, served.get(0));
        RevocationSync.onParentPong(regionB, pong, "rgG", "gc");
        atB.awaitRevocations();
        assertTrue(atB.isTombstoned("rgB", "b1"));
        MsgEvent report = ping(regionB);
        RevocationSync.onChildPing(regionB, report);
        String answers = report.getCompressedParam(RevocationSync.NAME_REPORT);
        assertNotNull(answers, "the region did not answer");
        MsgEvent forged = ping(regionB);
        forged.setCompressedParam(RevocationSync.NAME_REPORT, answers.replace("|N|", "|K|" + "ef".repeat(32)));
        forged.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, forged, true);
        assertTrue(RevocationSync.lastWarning(global).contains("REFUSED") && RevocationSync.lastWarning(global).contains("signature"),
                RevocationSync.lastWarning(global));
        assertFalse(atGlobal.isRevokedKey("ef".repeat(32)));
        assertNotNull(atGlobal.nameRevocationsOutstanding());
        report.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, report, true);
        assertNull(atGlobal.nameRevocationsOutstanding(), "the region's own answer was not taken");
        assertEquals(PeerTrustRegistry.NameRevocation.NOKEY, atGlobal.nameRevocation("rgB", "b1").state);
    }
}
