package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.measurement.MsgMetrics;
import io.cresco.library.security.CrescoIdentity;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a discovery exchange may record, at the registry that decides it ({@link PeerTrustRegistry#join}): nothing new without
 * proof of the key, one controller per region, one region per key, one global per node, the operator's rebind line as the
 * only replacement, names that differ by case, and what a registry of an earlier release holds.
 * {@link OneControllerPerRegionTest} and {@link OneControllerProofTest} hold the behaviour through real discovery exchanges.
 */
class OneControllerPerRegionRegistryTest {

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    /** A chain as a per-node-trust node makes it, for the key given (anyone can make one for any PUBLIC key). */
    private static X509Certificate[] chain(String tenant, String region, String agent, KeyPair kp) throws Exception {
        RegionCA ca = RegionCA.generate(tenant, region, 2048, 1);
        return ca.enroll(kp.getPublic(), tenant, region, agent, 1);
    }

    private static PeerTrustRegistry.BindResult join(PeerTrustRegistry r, String tenant, String region, String agent, KeyPair kp, String role,
                                                     boolean proved, Consumer<PeerTrustRegistry.Join> more) throws Exception {
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = region; j.agent = agent; j.chain = chain(tenant, region, agent, kp); j.role = role; j.proved = proved;
        j.exchange = "GLOBAL";
        if (more != null) more.accept(j);
        return r.join(j);
    }

    private static PeerTrustRegistry.BindResult controller(PeerTrustRegistry r, String region, String agent, KeyPair kp, boolean proved) throws Exception {
        return join(r, "t1", region, agent, kp, PeerTrustRegistry.ROLE_CONTROLLER, proved, null);
    }

    private static String pinned(PeerTrustRegistry r, String region, String agent) { return r.pinnedKey(new PeerIdentity.Expected(region, agent)); }

    private static PeerTrustRegistry open(Path f, List<String> log) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, log::add);
    }

    private static Path file() throws Exception {
        TlsTestSupport.provider();
        return Files.createTempDirectory("onectl").resolve("security").resolve("peer-trust.json");
    }

    /** Nothing NEW without proof; an exactly recorded name and key as before; the upgrade window admits a first join; proved once, proved always. */
    @Test
    void aNewNameOrKeyIsRecordedOnlyOnProofOfTheKey() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        KeyPair old = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa();
        // a node of release 1.3.378 (no proof), first join: refused, with what to do, for both operators
        PeerTrustRegistry.BindResult no = controller(r, "rgB", "rc", old, false);
        assertFalse(no.ok);
        assertNull(pinned(r, "rgB", "rc"), "something of a first join without proof was recorded");
        assertTrue(no.reason.contains("sent no proof") && no.reason.contains("Upgrade rgB/rc") && no.reason.contains("discovery_auth=compat"), no.reason);
        assertTrue(no.toPeer.contains("UPGRADE rgB/rc") && no.toPeer.contains("upgrade window"), no.toPeer);
        assertEquals(PeerTrustRegistry.KIND_PROOF, no.limited);
        // ... admitted while the upgrade window is open on this node, loudly, and marked as such
        PeerTrustRegistry.BindResult in = join(r, "t1", "rgB", "rc", old, PeerTrustRegistry.ROLE_CONTROLLER, false, j -> j.window = true);
        assertTrue(in.ok, in.reason);
        assertTrue(log.stream().anyMatch(l -> l.contains("UPGRADE WINDOW") && l.contains("rgB/rc") && l.contains("WITHOUT proof")), log.toString());
        assertFalse(r.provedKey("rgB", "rc"));
        // a re-join of exactly that name and key without proof: accepted as before, window or not
        assertTrue(controller(r, "rgB", "rc", old, false).ok);
        // without proof never another key for the name, a second name of the key, nor a controller of a region that has one
        Files.writeString(Path.of(f + ".rebind"), "rgB/rc=" + fp(fresh.getPublic()) + "\nrgB/zz\n");
        for (boolean window : new boolean[]{false, true}) {
            assertFalse(join(r, "t1", "rgB", "rc", fresh, PeerTrustRegistry.ROLE_CONTROLLER, false, j -> j.window = window).ok,
                    "another key for a recorded name was taken without proof (window " + window + "), on the operator's rebind line");
            assertFalse(join(r, "t1", "rgB", "agent-9", old, PeerTrustRegistry.ROLE_CONTROLLER, false, j -> j.window = window).ok,
                    "a second name of a recorded key was taken without proof (window " + window + ")");
            assertFalse(join(r, "t1", "rgB", "zz", fresh, PeerTrustRegistry.ROLE_CONTROLLER, false, j -> j.window = window).ok,
                    "a replacement controller was taken without proof (window " + window + ")");
        }
        assertEquals(fp(old.getPublic()), pinned(r, "rgB", "rc"));
        assertNull(pinned(r, "rgB", "agent-9"));
        assertNull(pinned(r, "rgB", "zz"));
        // the node is upgraded: it proves its key -- from then on that name and key is refused without proof
        assertTrue(controller(r, "rgB", "rc", old, true).ok);
        assertTrue(r.provedKey("rgB", "rc"));
        PeerTrustRegistry.BindResult back = controller(r, "rgB", "rc", old, false);
        assertFalse(back.ok, "a name and key that proved itself was accepted without proof afterwards");
        assertTrue(back.reason.contains("PROVED its key to this node before") && back.reason.contains("discovery_auth=compat"), back.reason);
        // ... except in the window (a node put back on the earlier release), said loudly
        assertTrue(join(r, "t1", "rgB", "rc", old, PeerTrustRegistry.ROLE_CONTROLLER, false, j -> j.window = true).ok);
        assertTrue(log.stream().anyMatch(l -> l.contains("UPGRADE WINDOW") && l.contains("sent NO proof now")), log.toString());
        // what was learned is durable, and an agent's first join follows the same rule
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertTrue(again.provedKey("rgB", "rc"));
        assertFalse(controller(again, "rgB", "rc", old, false).ok);
        assertFalse(join(again, "t1", "rgB", "a1", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, false, null).ok);
        assertTrue(join(again, "t1", "rgB", "a1", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, true, null).ok);
    }

    /**
     * The upgrade window records a first join without proof, so whoever holds the secret can have ANY public key recorded
     * under a name of its choice. Such a record never stands against the key's holder: when the holder proves the key, a
     * record of it made without proof -- another region, another name, another tenant -- gives way. A PROVED record, or one
     * of an earlier release, does not.
     */
    @Test
    void whatWasRecordedWithoutProofGivesWayToTheProvenHolderOfTheKey() throws Exception {
        Consumer<PeerTrustRegistry.Join> window = j -> j.window = true;
        KeyPair v = TlsTestSupport.rsa();
        // the victim's public key recorded as the controller of ANOTHER region
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(file(), log);
        assertTrue(join(r, "t1", "rgW", "x", v, PeerTrustRegistry.ROLE_CONTROLLER, false, window).ok);
        r.allowBridgePeer("rgW", "x");
        assertTrue(controller(r, "rgV", "rc", v, true).ok, "a record made without the key kept the key's holder out (one region per key)");
        assertNull(pinned(r, "rgW", "x"), "the record made without proof stayed beside the proven one");
        assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", "rgW", "x", null)));
        assertTrue(log.stream().anyMatch(l -> l.contains("PROVED the key") && l.contains("WITHOUT proof") && l.contains("rgW/x is no longer recorded")), log.toString());
        // ... under another name of its own region, stating another tenant
        r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(join(r, "t2", "rgV", "y", v, PeerTrustRegistry.ROLE_CONTROLLER, false, window).ok);
        assertTrue(controller(r, "rgV", "rc", v, true).ok, "a record made without the key kept the key's holder out (one controller per region)");
        assertNull(pinned(r, "rgV", "y"));
        // ... under its own name, stating another tenant
        r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(join(r, "t2", "rgV", "rc", v, PeerTrustRegistry.ROLE_CONTROLLER, false, window).ok);
        assertEquals("t2", r.boundTenant("rgV", "rc"));
        assertTrue(controller(r, "rgV", "rc", v, true).ok, "a record made without the key kept the key's holder out (its own name, another tenant)");
        assertEquals("t1", r.boundTenant("rgV", "rc"));
        assertTrue(r.provedKey("rgV", "rc"));
        // a record of ANOTHER key made without proof is a squat like any other: the operator decides
        r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(join(r, "t1", "rgV", "zz", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_CONTROLLER, false, window).ok);
        assertFalse(controller(r, "rgV", "rc", v, true).ok);
        // and a record of an earlier release (no mark at all) stands, as a proved one does: the key speaks for that region
        r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(r.bind("t1", "rgW", "x", v.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertFalse(controller(r, "rgV", "rc", v, true).ok, "a key recorded by an earlier release as one region's controller was taken as another's");
    }

    /** The same key under another agent name: with proof the new name REPLACES the former one; never two names of one key. */
    @Test
    void theSameKeyUnderANewNameReplacesTheFormerNameAndNeverAddsOne() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        List<String> replaced = new CopyOnWriteArrayList<>();
        r.setKeyReplacedListener((n, k) -> replaced.add(n + "|" + k));
        KeyPair rc = TlsTestSupport.rsa();
        assertTrue(controller(r, "rgB", "agent-1", rc, true).ok);
        r.allowBridgePeer("rgB", "agent-1");
        assertTrue(controller(r, "rgB", "agent-2", rc, true).ok, "a controller whose agent name is generated anew was refused although it proved its key");
        assertNull(pinned(r, "rgB", "agent-1"), "the former name of the key stayed recorded: two names of one key");
        assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", "rgB", "agent-1", null)));
        assertEquals(fp(rc.getPublic()), pinned(r, "rgB", "agent-2"));
        assertEquals(List.of("rgB/agent-1|" + fp(rc.getPublic())), replaced, "the removed binding was not told to the key-replaced listener");
        assertTrue(log.stream().anyMatch(l -> l.contains("is now recorded as rgB/agent-2") && l.contains("rgB/agent-1 is no longer recorded")), log.toString());
        assertTrue(r.controllerConflicts().isEmpty());
        // forty more names: still one
        for (int i = 3; i < 43; i++) assertTrue(controller(r, "rgB", "agent-" + i, rc, true).ok);
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertEquals(fp(rc.getPublic()), pinned(again, "rgB", "agent-42"));
        for (int i = 1; i < 42; i++) assertNull(pinned(again, "rgB", "agent-" + i), "agent-" + i + " is still recorded after forty renames");
        // another tenant's certificate with that key is not "the same controller"
        assertFalse(join(r, "t2", "rgB", "agent-50", rc, PeerTrustRegistry.ROLE_CONTROLLER, true, null).ok);
    }

    /** A key bound as a controller of one region is refused as a controller of another. */
    @Test
    void aKeySpeaksForOneRegion() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair a = TlsTestSupport.rsa();
        assertTrue(controller(r, "rgA", "rc", a, true).ok);
        PeerTrustRegistry.BindResult no = controller(r, "rgC", "aa", a, true);
        assertFalse(no.ok, "the key of region rgA's controller was taken as region rgC's controller too");
        assertTrue(no.reason.contains("a key speaks for ONE region") && no.reason.contains("rgA/rc"), no.reason);
        assertEquals(PeerTrustRegistry.KIND_CONTROLLER, no.limited);
        assertNull(pinned(r, "rgC", "aa"));
        // (an agent's key is not a controller's: a controller that was first seen as an agent elsewhere is its own matter)
        KeyPair ag = TlsTestSupport.rsa();
        assertTrue(join(r, "t1", "rgA", "a1", ag, PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        assertTrue(controller(r, "rgD", "rc", ag, true).ok);
    }

    /** Another key is another controller: refused, and only the operator's rebind line replaces the recorded one -- never an anchor. */
    @Test
    void onlyTheOperatorsRebindLineReplacesARecordedController() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        List<String> replaced = new CopyOnWriteArrayList<>();
        r.setKeyReplacedListener((n, k) -> replaced.add(n + "|" + k));
        KeyPair rc = TlsTestSupport.rsa(), other = TlsTestSupport.rsa();
        assertTrue(controller(r, "rgB", "rc", rc, true).ok);
        r.allowBridgePeer("rgB", "rc");
        PeerTrustRegistry.BindResult no = controller(r, "rgB", "zz", other, true);
        assertFalse(no.ok);
        assertEquals(PeerTrustRegistry.KIND_CONTROLLER, no.limited);
        String line = "'rgB/zz=" + fp(other.getPublic()) + "'";
        assertTrue(no.reason.contains("a region has ONE controller") && no.reason.contains(line) && no.reason.contains("peer-trust.json.rebind")
                && no.reason.contains("add the line 'rgB/rc' to") && no.reason.contains("peer-trust.json.revoke"), no.reason);
        // the refused joiner is told ITS remedy: the line, and where it goes
        assertTrue(no.toPeer.contains(line) && no.toPeer.contains("peer-trust.json.rebind") && no.toPeer.contains("rgB/rc"), no.toPeer);
        // controller_trust_anchors admit a controller; they replace nothing that is recorded -- whether or not the recorded one is anchored
        for (boolean recordedAnchored : new boolean[]{false, true}) {
            PeerTrustRegistry.BindResult anchored = join(r, "t1", "rgB", "zz", other, PeerTrustRegistry.ROLE_CONTROLLER, true, j -> {
                j.anchored = true;
                j.recordedAnchored = (n, k) -> recordedAnchored;
            });
            assertFalse(anchored.ok, "an anchored controller replaced a recorded one");
            assertTrue(anchored.reason.contains("an anchor admits a controller, it never replaces a recorded one"), anchored.reason);
        }
        assertEquals(fp(rc.getPublic()), pinned(r, "rgB", "rc"));
        // a dry run with the operator's line present changes nothing and spends nothing
        Files.writeString(Path.of(f + ".rebind"), "rgB/zz=" + fp(other.getPublic()) + "\n");
        assertTrue(join(r, "t1", "rgB", "zz", other, PeerTrustRegistry.ROLE_CONTROLLER, true, j -> j.record = false).ok);
        assertEquals(fp(rc.getPublic()), pinned(r, "rgB", "rc"));
        assertNull(pinned(r, "rgB", "zz"));
        // the operator's line: the recorded controller is replaced -- unbound, its bridge record gone, the listener told
        assertTrue(controller(r, "rgB", "zz", other, true).ok);
        assertNull(pinned(r, "rgB", "rc"));
        assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));
        assertEquals(fp(other.getPublic()), pinned(r, "rgB", "zz"));
        assertEquals(List.of("rgB/rc|" + fp(rc.getPublic())), replaced);
        assertTrue(log.stream().anyMatch(l -> l.contains("REPLACED by the operator") && l.contains("rgB/rc") && l.contains("NOT revoked")), log.toString());
        // the line is spent with the binding, in one write: after a restart the former controller is refused again, the new one known
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertEquals(fp(other.getPublic()), pinned(again, "rgB", "zz"));
        assertNull(pinned(again, "rgB", "rc"));
        assertFalse(controller(again, "rgB", "rc", rc, true).ok, "the spent rebind line was honoured again");
    }

    /** An agent that refuses a second controller of its region is told the case of its first discovery, and what its operator does. */
    @Test
    void anAgentsRefusalSaysTheCaseOfItsFirstDiscovery() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair first = TlsTestSupport.rsa(), real = TlsTestSupport.rsa();
        java.util.function.Consumer<PeerTrustRegistry.Join> byAnAgent = j -> { j.client = true; j.exchange = "AGENT"; };
        assertTrue(join(r, "t1", "rgA", "zz", first, PeerTrustRegistry.ROLE_CONTROLLER, true, byAnAgent).ok);
        PeerTrustRegistry.BindResult no = join(r, "t1", "rgA", "rc", real, PeerTrustRegistry.ROLE_CONTROLLER, true, byAnAgent);
        assertFalse(no.ok);
        assertTrue(no.reason.contains("an agent takes the FIRST controller that answers its first discovery")
                && no.reason.contains("'rgA/rc=" + fp(real.getPublic()) + "'") && no.reason.contains("add the line 'rgA/zz' to")
                && no.reason.contains("controller_rebind_signers") && no.reason.contains("security_profile=production"), no.reason);
        // (a global's refusal of a joining controller does not talk about agents)
        PeerTrustRegistry g = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(controller(g, "rgA", "zz", first, true).ok);
        assertFalse(controller(g, "rgA", "rc", real, true).reason.contains("an agent takes"));
    }

    /** The revoke-by-name remedy is never printed when the recorded key is bound under other names. */
    @Test
    void theRemedyNeverSaysToRevokeANameWhoseKeyHasOtherNames() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair b = TlsTestSupport.rsa();
        // a registry of before the rule: one key recorded as the controller of two regions
        assertTrue(r.bind("t1", "rgB", "rc", b.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(r.bind("t1", "rgC", "aa", b.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        PeerTrustRegistry.BindResult no = controller(r, "rgC", "rc", TlsTestSupport.rsa(), true);
        assertFalse(no.ok);
        assertFalse(no.reason.contains("add the line 'rgC/aa' to"), "the remedy revokes a name whose key is region rgB's controller too: " + no.reason);
        assertTrue(no.reason.contains("also recorded here as rgB/rc") && no.reason.contains("do NOT revoke rgC/aa by name"), no.reason);
        assertTrue(no.reason.contains("'rgC/rc="), no.reason);
    }

    /** Two controllers that claim one region at the same instant: one is recorded, every time. */
    @Test
    void ofTwoJoinsAtTheSameInstantOneIsRecorded() throws Exception {
        TlsTestSupport.provider();
        KeyPair k1 = TlsTestSupport.rsa(), k2 = TlsTestSupport.rsa();
        X509Certificate[] c1 = chain("t1", "rgB", "rc", k1), c2 = chain("t1", "rgB", "zz", k2);
        for (int round = 0; round < 40; round++) {
            PeerTrustRegistry r = new PeerTrustRegistry();
            CyclicBarrier gate = new CyclicBarrier(2);
            boolean[] ok = new boolean[2];
            Thread[] t = new Thread[2];
            for (int k = 0; k < 2; k++) {
                final int kk = k;
                t[k] = new Thread(() -> {
                    try {
                        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
                        j.region = "rgB"; j.agent = kk == 0 ? "rc" : "zz"; j.chain = kk == 0 ? c1 : c2;
                        j.role = PeerTrustRegistry.ROLE_CONTROLLER; j.proved = true;
                        gate.await();
                        ok[kk] = r.join(j).ok;
                    } catch (Exception e) { throw new IllegalStateException(e); }
                });
                t[k].start();
            }
            for (Thread x : t) x.join();
            assertTrue(ok[0] ^ ok[1], "round " + round + ": recorded rc=" + ok[0] + " zz=" + ok[1]);
            assertTrue(r.controllerConflicts().isEmpty());
        }
    }

    /** A node has one global: the first it took in a GLOBAL exchange; the operator's line moves it; an anchor only when it no longer vouches for the recorded one. */
    @Test
    void aNodeHasOneGlobalAndOnlyTheOperatorMovesIt() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        KeyPair g = TlsTestSupport.rsa(), h = TlsTestSupport.rsa(), p = TlsTestSupport.rsa();
        Consumer<PeerTrustRegistry.Join> asGlobal = j -> { j.client = true; j.global = true; };
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgG/gc", r.globalRecorded());
        assertTrue(log.stream().anyMatch(l -> l.contains("rgG/gc") && l.contains("recorded as this node's GLOBAL")), log.toString());
        // a peer region this node joined is not its global
        assertTrue(join(r, "t1", "rgP", "rc", p, PeerTrustRegistry.ROLE_CONTROLLER, true, j -> j.client = true).ok);
        // another identity in a GLOBAL exchange -- a region name never seen, or a recorded peer: refused, and nothing of it recorded
        PeerTrustRegistry.BindResult no = join(r, "t1", "rgH", "zz", h, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal);
        assertFalse(no.ok, "a second global under a region name never seen was taken");
        assertNull(pinned(r, "rgH", "zz"));
        String line = "'rgH/zz=" + fp(h.getPublic()) + "'";
        assertTrue(no.reason.contains("a node has ONE global") && no.reason.contains("rgG/gc") && no.reason.contains(line)
                && no.reason.contains("peer-trust.json.rebind") && no.reason.contains("add the line 'rgG/gc' to"), no.reason);
        assertEquals(PeerTrustRegistry.KIND_CONTROLLER, no.limited);
        assertFalse(join(r, "t1", "rgP", "rc", p, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok, "a recorded peer region's controller was taken as the global");
        // anchors that vouch for both: the recorded global stays
        PeerTrustRegistry.BindResult both = join(r, "t1", "rgH", "zz", h, PeerTrustRegistry.ROLE_CONTROLLER, true, j -> {
            asGlobal.accept(j); j.anchored = true; j.recordedAnchored = (n, k) -> true;
        });
        assertFalse(both.ok, "an anchored responder replaced a global the anchors still vouch for");
        assertEquals("rgG/gc", r.globalRecorded());
        // the operator's line on this node moves it; the former global stays recorded (not revoked), and is no longer the global
        Files.writeString(Path.of(f + ".rebind"), "rgH/zz=" + fp(h.getPublic()) + "\n");
        assertTrue(join(r, "t1", "rgH", "zz", h, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgH/zz", r.globalRecorded());
        assertEquals(fp(g.getPublic()), pinned(r, "rgG", "gc"));
        assertTrue(log.stream().anyMatch(l -> l.contains("GLOBAL MOVED from rgG/gc to rgH/zz") && l.contains("by the operator")), log.toString());
        assertFalse(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok, "the former global was taken again without the operator");
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertEquals("rgH/zz", again.globalRecorded());
        // anchors that no longer vouch for the recorded global admit an anchored one in its place
        assertTrue(join(again, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, j -> {
            asGlobal.accept(j); j.anchored = true; j.recordedAnchored = (n, k) -> false;
        }).ok);
        assertEquals("rgG/gc", again.globalRecorded());
        // the global re-keyed under its own name (the operator's line): still the global; revoked: the next one is first come
        KeyPair g2 = TlsTestSupport.rsa();
        Files.writeString(Path.of(f + ".rebind"), "rgG/gc=" + fp(g2.getPublic()) + "\n");
        assertTrue(join(again, "t1", "rgG", "gc", g2, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgG/gc", again.globalRecorded());
        assertNull(again.revoke("rgG/gc", "operator"));
        again.awaitRevocations();
        assertNull(again.globalRecorded());
        assertTrue(join(again, "t1", "rgN", "gc", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgN/gc", again.globalRecorded());
    }

    /**
     * In the upgrade window a region's first GLOBAL exchange may be answered without proof -- by its real global of release
     * 1.3.378, or by anyone with the secret and a copy of some controller's certificate. The global recorded that way is
     * confirmed when it proves its key in a GLOBAL exchange; a proof the key's holder makes in ANOTHER kind of exchange (it is
     * a peer region's controller) does not make it this node's global.
     */
    @Test
    void aGlobalRecordedWithoutProofIsConfirmedOnlyByAProofInAGlobalExchange() throws Exception {
        Consumer<PeerTrustRegistry.Join> firstGlobal = j -> { j.client = true; j.global = true; j.window = true; };
        Consumer<PeerTrustRegistry.Join> asGlobal = j -> { j.client = true; j.global = true; };
        KeyPair v = TlsTestSupport.rsa(), g = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(join(r, "t1", "rgV", "rc", v, PeerTrustRegistry.ROLE_CONTROLLER, false, firstGlobal).ok);
        assertEquals("rgV/rc", r.globalRecorded());
        // the key's holder turns out to be a peer region's controller: it joins this node in a REGION exchange, with proof
        assertTrue(controller(r, "rgV", "rc", v, true).ok);
        assertTrue(r.provedKey("rgV", "rc"));
        assertNull(r.globalRecorded(), "a global recorded without proof was confirmed by a proof made in another kind of exchange");
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok, "the real global was kept out");
        assertEquals("rgG/gc", r.globalRecorded());
        // the usual case: the global of release 1.3.378, recorded in the window, is upgraded and proves its key as the global
        r = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, false, firstGlobal).ok);
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgG/gc", r.globalRecorded());
        assertTrue(r.provedKey("rgG", "gc"));
        assertFalse(join(r, "t1", "rgV", "rc", v, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
    }

    /** A registry written by an earlier release does not say which controller is its global: only one of them is taken, then marked. */
    @Test
    void aRegistryOfAnEarlierReleaseTakesAsItsGlobalOnlyAControllerItHolds() throws Exception {
        Path f = file();
        PeerTrustRegistry r = open(f, new CopyOnWriteArrayList<>());
        KeyPair g = TlsTestSupport.rsa(), p = TlsTestSupport.rsa(), q = TlsTestSupport.rsa(), z = TlsTestSupport.rsa();
        // as release 1.3.378 recorded them: its global and two peer regions, indistinguishable
        for (Object[] c : new Object[][]{{"rgG", "gc", g}, {"rgP", "rc", p}, {"rgQ", "rc", q}}) {
            assertTrue(r.bind("t1", (String) c[0], (String) c[1], ((KeyPair) c[2]).getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        }
        Consumer<PeerTrustRegistry.Join> asGlobal = j -> { j.client = true; j.global = true; };
        // an identity it does not hold answers its GLOBAL exchange, with proof: not taken
        PeerTrustRegistry.BindResult no = join(r, "t1", "rgH", "zz", z, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal);
        assertFalse(no.ok);
        assertTrue(no.reason.contains("written by an earlier release") && no.reason.contains("'rgH/zz=" + fp(z.getPublic()) + "'"), no.reason);
        // a peer that has joined under this release (it proved its key in a REGION exchange) is known not to be "one of them"
        assertTrue(join(r, "t1", "rgP", "rc", p, PeerTrustRegistry.ROLE_CONTROLLER, true, null).ok);
        assertFalse(join(r, "t1", "rgP", "rc", p, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok, "a controller that joined as a peer was taken as the global");
        // its global, still on the earlier release (no proof): accepted as recorded, each time, and not marked
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, false, asGlobal).ok);
        assertNull(r.globalRecorded());
        // upgraded, it proves its key: it is this node's global from here on, and the other recorded controllers are known peers
        assertTrue(join(r, "t1", "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertEquals("rgG/gc", r.globalRecorded());
        assertFalse(join(r, "t1", "rgQ", "rc", q, PeerTrustRegistry.ROLE_CONTROLLER, false, asGlobal).ok, "a recorded peer was taken as the global after the global was marked");
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertEquals("rgG/gc", again.globalRecorded());
        // revoking the global does not bring the question back: the next GLOBAL responder is first come
        assertNull(again.revoke("rgG/gc", "operator"));
        again.awaitRevocations();
        assertTrue(join(again, "t1", "rgH", "zz", z, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
    }

    /** A name bound as an agent that presents itself as a controller is told that, not a nonsense about tenants. */
    @Test
    void anAgentNameThatPresentsItselfAsAControllerIsToldWhatItIs() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair k = TlsTestSupport.rsa();
        assertTrue(join(r, "t1", "rgB", "a1", k, PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        PeerTrustRegistry.BindResult no = controller(r, "rgB", "a1", k, true);
        assertFalse(no.ok);
        assertFalse(no.reason.contains("differs from the bound tenant"), no.reason);
        assertTrue(no.reason.contains("bound here as an AGENT") && no.reason.contains("now presents itself as a controller"), no.reason);
        // ... and a tenant that does differ is still said as that
        PeerTrustRegistry.BindResult t2 = join(r, "t2", "rgB", "a1", k, PeerTrustRegistry.ROLE_AGENT, true, null);
        assertTrue(!t2.ok && t2.reason.contains("tenant t2 differs from the bound tenant t1"), t2.reason);
    }

    /** A name that differs from a recorded one only by letter case is refused wherever a name is first recorded. */
    @Test
    void aNameThatDiffersOnlyByCaseFromARecordedOneIsRefused() throws Exception {
        Path f = file();
        PeerTrustRegistry r = open(f, new CopyOnWriteArrayList<>());
        KeyPair k = TlsTestSupport.rsa();
        assertTrue(controller(r, "rgB", "rc", k, true).ok);
        r.allowBridgePeer("rgB", "rc");
        for (String[] n : new String[][]{{"rgb", "rc"}, {"RGB", "zz"}, {"rgB", "RC"}, {"rgB", "Rc"}}) {
            PeerTrustRegistry.BindResult no = join(r, "t1", n[0], n[1], TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, true, null);
            assertFalse(no.ok, n[0] + "/" + n[1] + " was recorded beside rgB/rc");
            assertTrue(no.reason.contains("differs only by letter case"), no.reason);
            assertFalse(r.bind("t1", n[0], n[1], TlsTestSupport.rsa().getPublic(), "x").ok);
            r.allowBridgePeer(n[0], n[1]);
            assertFalse(r.isBridgePeer(CrescoIdentity.of("t1", n[0], n[1], null)), "a bridge peer " + n[0] + "/" + n[1] + " was recorded beside rgB/rc");
        }
        RegionCA ca = RegionCA.generate("t1", "rgb", 2048, 1);
        String why = r.trustCa(ca.caCert(), "rgb", "t1", "federated");
        assertTrue(why != null && why.contains("differs only by letter case"), "a CA for region rgb was recorded beside region rgB: " + why);
        // exact names are untouched, and so is a registry that already holds both spellings (each keeps working)
        assertTrue(join(r, "t1", "rgB", "a1", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        assertTrue(controller(r, "rgB", "rc", k, true).ok);
        // a revoked name still counts: its case variant is not a way back
        assertNull(r.revoke("rgB/a1", "operator"));
        r.awaitRevocations();
        assertFalse(join(r, "t1", "rgB", "A1", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        // ... and after a restart the registry knows the spellings it holds
        PeerTrustRegistry again = open(f, new CopyOnWriteArrayList<>());
        assertFalse(join(again, "t1", "rgb", "rc", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, true, null).ok);
    }

    /** The refusal log state is bounded, and what was said more than ten minutes ago goes when it is full. */
    @Test
    void theRefusalLogStateIsBoundedAndForgets() {
        PeerTrustRegistry r = new PeerTrustRegistry();
        String kind = PeerTrustRegistry.KIND_CONTROLLER;
        assertTrue(r.refusalLogged(kind, "rgB", "zz", "k1"), "the first refusal of a joiner was not logged");
        assertFalse(r.refusalLogged(kind, "rgB", "zz", "k1"), "the second refusal within ten minutes was logged again");
        assertEquals(2, r.refusedTimes(kind, "rgB", "zz", "k1"));
        assertTrue(r.refusalLogged(kind, "rgB", "zz", "k2"), "another key under that name is another joiner");
        assertTrue(r.refusalLogged(PeerTrustRegistry.KIND_PROOF, "rgB", "zz", "k1"), "another rule is another line");
        assertEquals(3, r.controllersRefusedCount(), "only the controller rules are counted as refused controllers");
        // a flood of distinct joiners fills the state; one line a minute is then said for whoever comes
        for (int i = 0; r.refusalsRemembered() < PeerTrustRegistry.REFUSED_MAX; i++) r.refusalLogged(kind, "rgB", "x" + i, "k" + i);
        assertTrue(r.refusalLogged(kind, "rgB", "spam", "kspam"));
        assertFalse(r.refusalLogged(kind, "rgB", "rc2", "the-real-replacement-controller"), "(one line a minute while it is full of fresh entries)");
        assertEquals(PeerTrustRegistry.REFUSED_MAX, r.refusalsRemembered(), "the state grew past its bound");
        // ten minutes later the state holds nothing back any more: a real controller's first refusal is logged, and remembered
        r.ageRefusals(PeerTrustRegistry.REFUSED_LOG_MS);
        assertTrue(r.refusalLogged(kind, "rgB", "rc2", "the-real-replacement-controller"), "a full state of old entries hid a new joiner's first refusal");
        assertTrue(r.refusalsRemembered() <= 1, "old entries were not evicted: " + r.refusalsRemembered());
        assertFalse(r.refusalLogged(kind, "rgB", "rc2", "the-real-replacement-controller"));
    }

    @Test
    void theCountIsInTheMetricInventory() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgG", "gc", Map.of("broker_security_enabled", "true"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Map<String, String> names = MsgMetrics.register(registry, e);
        assertEquals("broker", names.get("broker.controller.refused"));
        assertEquals(0.0, registry.get("broker.controller.refused").functionCounter().count());
        PeerTrustRegistry r = e.getCertificateManager().getPeerTrust();
        r.refusalLogged(PeerTrustRegistry.KIND_CONTROLLER, "rgB", "zz", "ab".repeat(32));
        r.refusalLogged(PeerTrustRegistry.KIND_CONTROLLER, "rgB", "zz", "ab".repeat(32));
        assertEquals(2.0, registry.get("broker.controller.refused").functionCounter().count());
    }

    /** A registry of before the rule with two controllers of one region reports it -- not where the region's CA is recorded. */
    @Test
    void aDoubleRegistryIsReportedUntilTheOperatorRevokesOneButNeverUnderARecordedCa() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        KeyPair rc = TlsTestSupport.rsa(), zz = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "rc", rc.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(r.bind("t1", "rgB", "zz", zz.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertEquals(1, r.controllerConflicts().size());
        assertTrue(r.controllerConflicts().get(0).contains("2 CONTROLLERS are recorded here for region rgB"), r.controllerConflicts().toString());
        // each keeps working as recorded (with or without proof), and no third one is taken
        assertTrue(controller(r, "rgB", "rc", rc, false).ok);
        assertTrue(controller(r, "rgB", "zz", zz, true).ok);
        assertFalse(controller(r, "rgB", "third", TlsTestSupport.rsa(), true).ok);
        List<String> atStart = new CopyOnWriteArrayList<>();
        PeerTrustRegistry again = open(f, atStart);
        assertTrue(atStart.stream().anyMatch(l -> l.contains("2 CONTROLLERS are recorded here for region rgB")), atStart.toString());
        assertNull(again.revoke("rgB/zz", "operator"));
        again.awaitRevocations();
        assertTrue(again.controllerConflicts().isEmpty());
        // the regional CA: two controller names issued by the region's ONE CA are that CA's own -- no conflict, no false remedy
        PeerTrustRegistry c = open(file(), new CopyOnWriteArrayList<>());
        RegionCA ca = RegionCA.generate("t1", "rgC", 2048, 1);
        assertNull(c.trustCa(ca.caCert(), "rgC", "t1", "federated"));
        TlsTestSupport.Id one = TlsTestSupport.issued(ca, "t1", "rgC", "rc"), two = TlsTestSupport.issued(ca, "t1", "rgC", "rc2");
        assertTrue(c.bind("t1", "rgC", "rc", one.chain[0].getPublicKey(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(c.bind("t1", "rgC", "rc2", two.chain[0].getPublicKey(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(c.controllerConflicts().isEmpty(), "two names of one region CA were reported as two controllers: " + c.controllerConflicts());
    }
}
