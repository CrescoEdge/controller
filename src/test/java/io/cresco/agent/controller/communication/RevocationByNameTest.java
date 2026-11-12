package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node revoked by NAME at the global, where no key is bound to that name (an agent is bound at its region's
 * controller): the name is reserved at the global at once, the region's controller revokes the key and closes the node,
 * the key becomes revoked for every name in the fabric, and nothing a region answers can have another region's node
 * revoked. On the wire only -- pings and pongs through {@link RevocationSync}, and reports an attacker makes with its own
 * key -- and with nothing this change added to the code's interface: every test here runs (and fails) against the code
 * before the change, where the revocation ends at "no key is bound to rgB/b1".
 */
class RevocationByNameTest {

    private static final Gson GSON = new Gson();

    /** A node with a persistent registry at a known place (its operator files are next to it). */
    private static ControllerEngine node(String region, String agent, boolean global) throws Exception {
        Path dir = Files.createTempDirectory("byname-" + region);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        if (global) cfg.put("is_global", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = true;
        st.global = global;
        if (!global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static Path fileOf(ControllerEngine e) {
        return Path.of(e.getPluginBuilder().getConfig().getStringParam("peer_trust_file"));
    }

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    /** The two controllers join: each binds the other's key as a controller's. */
    private static void join(ControllerEngine global, ControllerEngine region) {
        X509Certificate r = region.getCertificateManager().getPublicCertificate()[0], g = global.getCertificateManager().getPublicCertificate()[0];
        assertNull(reg(global).pin(region.cstate.getRegion(), region.cstate.getAgent(), r, PeerTrustRegistry.ROLE_CONTROLLER, false));
        assertNull(reg(region).pin(global.cstate.getRegion(), global.cstate.getAgent(), g, PeerTrustRegistry.ROLE_CONTROLLER, true));
    }

    private static MsgEvent ping(ControllerEngine region) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, region.cstate.getRegion(), region.cstate.getAgent(), null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    /** One ping of {@code region} to {@code global} and the pong back, as the health watchers and executors do it. */
    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global) {
        return exchange(region, global, true);
    }

    /** {@code thisBuild} false: the region is of an earlier build -- its ping says nothing of what it knows. */
    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global, boolean thisBuild) {
        MsgEvent p = ping(region);
        RevocationSync.onChildPing(region, p);
        if (!thisBuild) { p.removeParam("rev_cap"); p.removeParam("rev_name_report"); }
        // what the global's broker stamps on a message of a verified controller
        p.setParam("cresco_caller", "controller:t1/" + region.cstate.getRegion() + "/" + region.cstate.getAgent());
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
        if (thisBuild) {
            RevocationSync.onParentPong(region, p, "rgG", "gc");
        } else {
            // an earlier build reads the list and nothing else
            MsgEvent q = ping(region);
            for (String k : new String[]{"rev_epoch", "rev_seq", "rev_ack", "rev_parent_list"}) if (p.getParam(k) != null) q.setParam(k, p.getParam(k));
            RevocationSync.onParentPong(region, q, "rgG", "gc");
        }
        reg(region).awaitRevocations();
        return p;
    }

    /** The request ids a pong carries, by name ({@code A|id|name} / {@code L|id|name|key}). */
    private static Map<String, String> requests(MsgEvent pong) {
        Map<String, String> out = new HashMap<>();
        if (pong.getParam("rev_names") == null) return out;
        JsonObject o = GSON.fromJson(pong.getCompressedParam("rev_names"), JsonObject.class);
        o.getAsJsonArray("items").forEach(i -> { String[] p = i.getAsString().split("\\|", -1); out.put(p[2], p[1]); });
        return out;
    }

    /** The request the global makes of {@code region}'s controller for {@code name}, read off a pong and not applied. */
    private static String asked(ControllerEngine global, ControllerEngine region, String name) {
        MsgEvent p = ping(region);
        p.setParam("rev_cap", "names");
        p.setParam("cresco_caller", "controller:t1/" + region.cstate.getRegion() + "/" + region.cstate.getAgent());
        RevocationSync.onPingAtParent(global, p, true);
        return requests(p).get(name);
    }

    /**
     * Answers to revocations by name as a node makes them with its OWN identity key and whatever it likes to say: what a
     * compromised controller (or anybody holding a key) can put on a ping.
     */
    private static String answersBy(ControllerEngine signer, String issuer, String to, String... items) throws Exception {
        CertificateManager cm = signer.getCertificateManager();
        long issued = System.currentTimeMillis();
        String signed = "cresco-name-revocations-v1|1|report|" + issuer + "|t1|" + to + "|" + issued + "|" + String.join(",", items);
        Map<String, Object> o = new java.util.LinkedHashMap<>();
        o.put("v", 1);
        o.put("kind", "report");
        o.put("issuer", issuer);
        o.put("tenant", "t1");
        o.put("to", to);
        o.put("issued", issued);
        o.put("items", List.of(items));
        o.put("cert", Base64.getEncoder().encodeToString(cm.getPublicCertificate()[0].getEncoded()));
        o.put("sig", Base64.getEncoder().encodeToString(cm.signWithIdentity(signed.getBytes(StandardCharsets.UTF_8))));
        return GSON.toJson(o);
    }

    /** Deliver {@code answers} to the global on a ping from {@code from}, stamped by the broker as {@code caller}. */
    private static void deliver(ControllerEngine global, ControllerEngine from, String caller, String answers) {
        MsgEvent p = ping(from);
        p.setParam("rev_cap", "names");
        p.setCompressedParam("rev_name_report", answers);
        if (caller != null) p.setParam("cresco_caller", caller);
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
    }

    private static boolean outstanding(ControllerEngine global, String name) {
        String[] h = global.getCertificateManager().identityHealth();
        return !"OK".equals(h[0]) && h[1] != null && h[1].contains(name);
    }

    // ------------------------------------------------------------------ at the global

    /** By name, with no key bound here: the name is reserved at once, the revocation is recorded, and it is said to be outstanding. */
    @Test
    void aNameRevokedAtTheGlobalIsReservedThereAtOnceAndSaidToBeOutstanding() throws Exception {
        ControllerEngine global = node("rgG", "gc", true);
        PeerTrustRegistry atGlobal = reg(global);
        assertNull(atGlobal.revoke("rgB/b1", "operator"), "a revocation by name at the global was not applied");
        assertTrue(atGlobal.isTombstoned("rgB", "b1"), "the name is not reserved at the global");
        assertNotNull(atGlobal.nameRefusal("rgB", "b1"), "messages signed in the revoked name are accepted at the global");
        assertFalse(atGlobal.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok, "a key was bound to the reserved name");
        // the operator is told the truth: reserved here, not confirmed by the region that holds the key
        String[] h = global.getCertificateManager().identityHealth();
        assertEquals("WARN", h[0], h[1]);
        assertTrue(h[1].contains("rgB/b1") && h[1].contains("NOT confirmed"), h[1]);
        // again: nothing new, nothing refused
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        // durable: a registry opened on the same files holds the reservation
        atGlobal.awaitRevocations();
        PeerTrustRegistry again = PeerTrustRegistry.open(fileOf(global), List.of(), List.of(), 30, m -> { });
        assertTrue(again.isTombstoned("rgB", "b1"), "the reservation did not survive a restart");
        assertFalse(again.isSealed(), again.sealReason());
    }

    // ------------------------------------------------------------------ the whole way

    /**
     * The region's controller applies it as if its own operator had revoked the name -- the bound key revoked, the name
     * reserved, the node's connections closed (the revocation listener) -- and reports the key; the global then refuses
     * that key under every name, and so does every other region and the region's own agents.
     */
    @Test
    void theRegionRevokesTheKeyAndReportsItAndTheFabricRefusesItUnderAnyName() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB), atC = reg(regionC);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);       // bound where it joined, and nowhere else
        String k = fp(b1.getPublic());
        List<String> closedAtB = new CopyOnWriteArrayList<>();
        atB.setRevokedListener(closedAtB::add);                                        // what closes the key's connections
        exchange(regionB, global);
        exchange(regionC, global);

        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        assertFalse(atGlobal.isRevokedKey(k));                                          // the global never saw the key
        assertTrue(outstanding(global, "rgB/b1"));

        // the next pong takes it to the controller that holds the binding
        MsgEvent pong = exchange(regionB, global);
        assertNotNull(requests(pong).get("rgB/b1"), "the pong to region rgB's controller did not carry the revocation by name");
        assertTrue(atB.isRevokedFor(k, "rgB") && atB.isRevokedFor(k, "rgZ"), "the region did not revoke the bound key for every name");
        assertTrue(atB.isTombstoned("rgB", "b1"), "the region did not reserve the name");
        assertEquals(List.of(k), closedAtB, "the region did not close the node's connections");
        assertFalse(atB.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok, "another key took the revoked name at its region");
        assertFalse(atB.bind("t1", "rgB", "other", b1.getPublic(), "discovery").ok, "the revoked key came back under another name at its region");
        assertTrue(outstanding(global, "rgB/b1"), "the global reported success before the region's answer reached it");

        // the region's next ping reports the key: revoked at the global for every name, nothing outstanding
        exchange(regionB, global);
        assertTrue(atGlobal.isRevokedFor(k, "rgB") && atGlobal.isRevokedFor(k, "rgC") && atGlobal.isRevokedFor(k, "rgG"),
                "the global does not refuse the reported key under every name");
        assertFalse(atGlobal.bind("t1", "rgG", "x", b1.getPublic(), "discovery").ok, "the revoked key was bound at the global under another name");
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
        assertTrue(requests(exchange(regionB, global)).isEmpty(), "a settled revocation by name is still sent");

        // every other region gets the key on the global's list, and refuses it under its own names
        exchange(regionC, global);
        assertTrue(atC.isRevokedFor(k, "rgC"), "another region does not refuse the revoked key");
        assertFalse(atC.bind("t1", "rgC", "b1", b1.getPublic(), "discovery").ok, "the revoked node came back under another region");
        // the name is reserved where it was revoked and where it was bound -- not at every node (a rebind must reach each)
        assertFalse(atC.isTombstoned("rgB", "b1"));

        // and the region's own agents hold the key on their region's list
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b2", Map.of("broker_security_enabled", "true"));
        assertTrue(atB.bind("t1", "rgB", "b2", agent.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "discovery").ok);
        assertNull(reg(agent).pin("rgB", "rc", regionB.getCertificateManager().getPublicCertificate()[0], PeerTrustRegistry.ROLE_CONTROLLER, true));
        MsgEvent ap = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "b2", null, "rgB", "rc", null, true, false);
        RevocationSync.onChildPing(agent, ap);
        RevocationSync.onPingAtParent(regionB, ap, false);
        RevocationSync.onParentPong(agent, ap, "rgB", "rc");
        assertTrue(reg(agent).isRevokedKey(k), "the region's agents do not hold the revoked key");
    }

    /** A name its region holds no key for: reserved there too, and the global says exactly that (no key was revoked). */
    @Test
    void aNameItsRegionHoldsNoKeyForIsReservedThereAndSettled() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        assertNull(reg(global).revoke("rgB/never", "operator"));
        exchange(regionB, global);
        assertTrue(reg(regionB).isTombstoned("rgB", "never"), "the region did not reserve the name");
        exchange(regionB, global);
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
        assertTrue(reg(global).isTombstoned("rgB", "never"));
        assertEquals(0, reg(global).revocations().size(), "a key was revoked for a name that had none");
    }

    // ------------------------------------------------------------------ what a region cannot do with an answer

    /**
     * A region's answer is its word. A region that names the key of ANOTHER region's node as "the key of my rgB/b1" does
     * not get that node revoked: where the key is bound outside rgB -- at the node's own region -- it is not applied, and
     * the node keeps its name; at the global a key bound there outside rgB is refused outright.
     */
    @Test
    void aLyingRegionCannotHaveAnotherRegionsNodeRevoked() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), liar = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, liar);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atC = reg(regionC);
        KeyPair victim = TlsTestSupport.rsa();                                         // rgC/c1: bound at its own region only
        assertTrue(atC.bind("t1", "rgC", "c1", victim.getPublic(), "discovery").ok);
        String vk = fp(victim.getPublic());
        exchange(regionC, global);

        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        String id = asked(global, liar, "rgB/b1");                                      // it reads the request, and applies nothing
        assertNotNull(id, "the region was not asked");
        // the lie, properly signed by the region's controller, for the request it was asked
        deliver(global, liar, "controller:t1/rgB/rc", answersBy(liar, "rgB/rc", "rgG/gc", id + "|rgB/b1|K|" + vk));
        // the global cannot know better (it never saw the key): it takes the region's word -- marked as that region's word
        exchange(regionC, global);
        assertFalse(atC.isRevokedFor(vk, "rgC"), "region rgB had region rgC's node revoked");
        assertFalse(atC.isTombstoned("rgC", "c1"), "region rgB had region rgC's name reserved");
        assertTrue(atC.bind("t1", "rgC", "c1", victim.getPublic(), "discovery").ok, "the victim lost its binding at its own region");

        // a key the global itself holds bound outside the answering region is refused there: nothing is revoked anywhere
        KeyPair own = TlsTestSupport.rsa();
        assertTrue(atGlobal.bind("t1", "rgG", "g1", own.getPublic(), "discovery").ok);
        assertNull(atGlobal.revoke("rgB/b2", "operator"));
        String id2 = asked(global, liar, "rgB/b2");
        assertNotNull(id2);
        String regionCKey = fp(regionC.getCertificateManager().getPublicCertificate()[0].getPublicKey());
        String globalKey = fp(global.getCertificateManager().getPublicCertificate()[0].getPublicKey());
        for (String k : List.of(fp(own.getPublic()), regionCKey, globalKey)) {
            deliver(global, liar, "controller:t1/rgB/rc", answersBy(liar, "rgB/rc", "rgG/gc", id2 + "|rgB/b2|K|" + k));
            assertFalse(atGlobal.isRevokedKey(k), "a region's answer revoked a key bound at the global outside that region");
            assertTrue(outstanding(global, "rgB/b2"), "a refused answer settled the revocation");
        }
        assertNotNull(atGlobal.verifiedIdentity(regionC.getCertificateManager().getPublicCertificate()), "region rgC's controller is refused");
    }

    /**
     * Ignored: an answer for a name of another region, for a name nobody asked about, for another request than the one
     * made (replayed or made up), one not signed by a controller bound at the global, one altered after signing, one
     * addressed to another node, and a second answer to a request already answered.
     */
    @Test
    void answersNobodyAskedForForgedOrReplayedAreIgnored() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        KeyPair b1 = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        String k = fp(b1.getPublic());
        String other = fp(TlsTestSupport.rsa().getPublic());
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        String id = asked(global, regionB, "rgB/b1");
        assertNotNull(id);
        String stale = "0123456789abcdef0123456789abcdef";

        // another region's controller answers for rgB's name (it even knows the request)
        deliver(global, regionC, "controller:t1/rgC/rc", answersBy(regionC, "rgC/rc", "rgG/gc", id + "|rgB/b1|K|" + other));
        // ... or claims to be rgB's controller: its certificate does not verify as that issuer
        deliver(global, regionC, "controller:t1/rgC/rc", answersBy(regionC, "rgB/rc", "rgG/gc", id + "|rgB/b1|K|" + other));
        // rgB's controller answers for a name nobody asked about, and for another request than the one made
        deliver(global, regionB, "controller:t1/rgB/rc", answersBy(regionB, "rgB/rc", "rgG/gc", id + "|rgB/zz|K|" + other));
        deliver(global, regionB, "controller:t1/rgB/rc", answersBy(regionB, "rgB/rc", "rgG/gc", stale + "|rgB/b1|K|" + other));
        // an agent of rgB (bound at the global as an agent) signs an answer in its own name
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b7", Map.of("broker_security_enabled", "true"));
        assertTrue(atGlobal.bind("t1", "rgB", "b7", agent.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "test").ok);
        deliver(global, agent, "agent:t1/rgB/b7", answersBy(agent, "rgB/b7", "rgG/gc", id + "|rgB/b1|K|" + other));
        // rgB's controller's answer altered on the way, and one it made for another node
        String good = answersBy(regionB, "rgB/rc", "rgG/gc", id + "|rgB/b1|K|" + k);
        deliver(global, regionB, "controller:t1/rgB/rc", good.replace(k, other));
        deliver(global, regionB, "controller:t1/rgB/rc", answersBy(regionB, "rgB/rc", "rgX/gc", id + "|rgB/b1|K|" + other));
        // rgB's controller's answer on a ping the broker stamped as another caller's
        deliver(global, regionB, "controller:t1/rgC/rc", answersBy(regionB, "rgB/rc", "rgG/gc", id + "|rgB/b1|K|" + other));

        assertFalse(atGlobal.isRevokedKey(other), "an ignored answer revoked a key");
        assertFalse(atGlobal.isRevokedKey(k));
        assertTrue(outstanding(global, "rgB/b1"), "an ignored answer settled the revocation");
        assertFalse(atGlobal.isTombstoned("rgB", "zz"));

        // the answer that counts, then a second one for the same request with another key: answered once
        deliver(global, regionB, "controller:t1/rgB/rc", good);
        assertTrue(atGlobal.isRevokedFor(k, "rgC"));
        deliver(global, regionB, "controller:t1/rgB/rc", answersBy(regionB, "rgB/rc", "rgG/gc", id + "|rgB/b1|K|" + other));
        deliver(global, regionB, "controller:t1/rgB/rc", good);                          // the same answer again: nothing
        assertFalse(atGlobal.isRevokedKey(other), "a second answer to an answered request revoked another key");
        assertEquals(1, atGlobal.revocations().size());
    }

    // ------------------------------------------------------------------ earlier builds

    /**
     * A region controller of an earlier build is sent nothing it does not know (no request, no key revoked on a region's
     * report), and the global says the revocation is outstanding and why. Such a region gets a reported key when the
     * operator revokes the fingerprint itself.
     */
    @Test
    void anEarlierBuildIsSentNothingItDoesNotKnowAndTheGlobalSaysWhatIsOutstanding() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), old = node("rgO", "rc", false), regionB = node("rgB", "rc", false);
        join(global, old);
        join(global, regionB);
        PeerTrustRegistry atGlobal = reg(global), atOld = reg(old), atB = reg(regionB);
        KeyPair o1 = TlsTestSupport.rsa(), b1 = TlsTestSupport.rsa();
        assertTrue(atOld.bind("t1", "rgO", "o1", o1.getPublic(), "discovery").ok);
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);

        assertNull(atGlobal.revoke("rgO/o1", "operator"));
        for (int i = 0; i < 3; i++) {
            MsgEvent pong = exchange(old, global, false);
            assertNull(pong.getParam("rev_names"), "an earlier build was sent a revocation by name");
        }
        assertFalse(atOld.isRevokedKey(fp(o1.getPublic())));
        String[] h = global.getCertificateManager().identityHealth();
        assertEquals("WARN", h[0], h[1]);
        assertTrue(h[1].contains("rgO/o1") && h[1].contains("earlier build"), h[1]);

        // a key revoked on region rgB's report is not sent to the earlier build ...
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global);
        exchange(regionB, global);
        String k = fp(b1.getPublic());
        assertTrue(atGlobal.isRevokedFor(k, "rgO"));
        exchange(old, global, false);
        assertFalse(atOld.isRevokedKey(k), "a key revoked on a region's report was sent to a build that cannot tell whose word it is");
        // ... until the operator revokes that fingerprint at the global: its own word, for all
        assertNull(atGlobal.revoke(k, "operator"));
        exchange(old, global, false);
        assertTrue(atOld.isRevokedKey(k), "the operator's own revocation of the key did not reach the earlier build");
    }

    /** The node of this build that holds the reported key under ITS region's name keeps it, until the operator's own word. */
    @Test
    void theOperatorsOwnRevocationOfTheKeyOverridesARegionsWord() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB), atC = reg(regionC);
        KeyPair node = TlsTestSupport.rsa();                                           // one key, joined to two regions
        assertTrue(atB.bind("t1", "rgB", "b1", node.getPublic(), "discovery").ok);
        assertTrue(atC.bind("t1", "rgC", "c9", node.getPublic(), "discovery").ok);
        String k = fp(node.getPublic());
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global);
        exchange(regionB, global);
        assertTrue(atGlobal.isRevokedFor(k, "rgC"));
        exchange(regionC, global);
        assertFalse(atC.isRevokedFor(k, "rgC"), "region rgC dropped its own node on region rgB's word");
        assertNull(atGlobal.revoke(k, "operator"));                                    // the operator: that key, whatever it is bound to
        exchange(regionC, global);
        assertTrue(atC.isRevokedFor(k, "rgC"), "the operator's own revocation of the key did not reach region rgC");
        assertTrue(atC.isTombstoned("rgC", "c9"));
        // and it survives a restart of the global as the operator's word
        atGlobal.awaitRevocations();
        PeerTrustRegistry again = PeerTrustRegistry.open(fileOf(global), List.of(), List.of(), 30, m -> { });
        List<PeerTrustRegistry.Revocation> all = again.revocationsAfter(0, 10, false);
        assertEquals(1, all.size());
        assertEquals(4, PeerTrustRegistry.ListEntry.of(all.get(0)).format().split("\\|", -1).length, "the entry is still marked as a region's word");
    }

    // ------------------------------------------------------------------ rebind

    /**
     * The operator rebinds the name where it was revoked -- the existing token line in the global's rebind file: the
     * global lifts its reservation, the region accepts exactly that key for the name, once, and the revoked key stays
     * revoked everywhere.
     */
    @Test
    void anOperatorRebindAtTheGlobalLiftsTheNameThereAndAtItsRegion() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        KeyPair b1 = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa(), thief = TlsTestSupport.rsa();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.getPublic(), "discovery").ok);
        String k = fp(b1.getPublic());
        assertNull(atGlobal.revoke("rgB/b1", "operator"));
        exchange(regionB, global);
        exchange(regionB, global);
        assertTrue(atGlobal.isRevokedFor(k, "rgB") && atB.isTombstoned("rgB", "b1") && atGlobal.isTombstoned("rgB", "b1"));
        assertFalse(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "the reserved name was bound without a rebind");

        Files.writeString(Path.of(fileOf(global) + ".rebind"), "rgB/b1=" + fp(fresh.getPublic()) + "\n");
        long until = System.currentTimeMillis() + 25_000;                              // the operator files are read every 10 s
        while (atGlobal.isTombstoned("rgB", "b1") && System.currentTimeMillis() < until) Thread.sleep(200);
        assertFalse(atGlobal.isTombstoned("rgB", "b1"), "the operator rebind did not lift the name at the global");
        assertNull(atGlobal.nameRefusal("rgB", "b1"));

        MsgEvent pong = exchange(regionB, global);
        assertNotNull(requests(pong).get("rgB/b1"), "the rebind was not sent to the name's region");
        assertFalse(atB.bind("t1", "rgB", "b1", thief.getPublic(), "discovery").ok, "the rebind let ANOTHER key take the name");
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "the key the operator named was refused at the region");
        assertFalse(atB.isTombstoned("rgB", "b1"));
        assertTrue(atB.isRevokedFor(k, "rgB") && atGlobal.isRevokedFor(k, "rgB"), "the revoked key did not stay revoked");
        assertFalse(atB.bind("t1", "rgB", "b9", b1.getPublic(), "discovery").ok);

        exchange(regionB, global);                                                     // the region confirms
        assertTrue(requests(exchange(regionB, global)).isEmpty(), "a confirmed rebind is still sent");
        // the old request, delivered again (its answer lost, or replayed), does not revoke the key the name was rebound to
        exchange(regionB, global);
        assertFalse(atB.isRevokedKey(fp(fresh.getPublic())));
        assertTrue(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok);
    }
}
