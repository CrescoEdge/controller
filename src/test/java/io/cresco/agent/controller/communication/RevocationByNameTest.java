package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node revoked by NAME at the global, where no key is bound to that name (an agent is bound at its region's
 * controller): the name is reserved at the global at once; the region's one controller revokes the key, reserves the
 * name, closes the node and reports the key; the global records that report as the region's word and revokes nothing on
 * it -- the operator revokes the reported fingerprint to have the whole fabric refuse the key.
 *
 * <p>On the wire only: pings and pongs through {@link RevocationSync} between real nodes, with nothing this change added
 * to the code's interface. Every test runs against the code before the change too, where the revocation ended at "no key
 * is bound to rgB/b1": each test goes through all its steps and reports every one that failed, not only the first.
 */
class RevocationByNameTest {

    private static final Gson GSON = new Gson();
    private static final long DAY = 24L * 3600 * 1000;

    /** Every step of a test is looked at; the test fails at its end with all that did not hold. */
    private static final class Steps {
        private final List<String> failed = new ArrayList<>();
        void check(boolean ok, String what) { if (!ok) failed.add(what); }
        void done() { assertTrue(failed.isEmpty(), failed.size() + " step(s) did not hold:\n  - " + String.join("\n  - ", failed)); }
    }

    /** A node whose identity and registry are in {@code dir}: started again on the same directory it is the same node. */
    private static ControllerEngine node(String region, String agent, boolean global, Path dir) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("node_identity_dir", dir.resolve("security").toString());
        cfg.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        if (global) cfg.put("is_global", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = true;
        st.global = global;
        if (!global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static ControllerEngine node(String region, String agent, boolean global) throws Exception {
        return node(region, agent, global, Files.createTempDirectory("byname-" + region));
    }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static Path fileOf(ControllerEngine e) {
        return Path.of(e.getPluginBuilder().getConfig().getStringParam("peer_trust_file"));
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    /** The two controllers join: each binds the other's key as a controller's, and the joiner becomes a bridge peer (discovery). */
    private static void join(ControllerEngine global, ControllerEngine region) {
        assertNull(reg(global).pin(region.cstate.getRegion(), region.cstate.getAgent(), leaf(region), PeerTrustRegistry.ROLE_CONTROLLER, false));
        reg(global).allowBridgePeer(region.cstate.getRegion(), region.cstate.getAgent());
        assertNull(reg(region).pin(global.cstate.getRegion(), global.cstate.getAgent(), leaf(global), PeerTrustRegistry.ROLE_CONTROLLER, true));
    }

    /** An agent bound at its region's controller -- some time before anything the operator does next. */
    private static KeyPair agentOf(ControllerEngine region, String agent) throws Exception {
        KeyPair kp = TlsTestSupport.rsa();
        assertTrue(reg(region).bind("t1", region.cstate.getRegion(), agent, kp.getPublic(), "discovery").ok);
        Thread.sleep(40);
        return kp;
    }

    private static MsgEvent ping(ControllerEngine region) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, region.cstate.getRegion(), region.cstate.getAgent(), null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global) {
        return exchange(region, global, true);
    }

    /**
     * One ping of {@code region} to {@code global} and the pong back, as the health watchers and executors do it.
     * {@code thisBuild} false: the region's controller is of an earlier build -- its ping says nothing of revocations by
     * name, and it reads the list on the pong and nothing else.
     */
    private static MsgEvent exchange(ControllerEngine region, ControllerEngine global, boolean thisBuild) {
        MsgEvent p = ping(region);
        RevocationSync.onChildPing(region, p);
        if (!thisBuild) for (String k : new String[]{"rev_names_fresh", "rev_cap", "rev_name_report"}) p.removeParam(k);
        // what the global's broker stamps on a message of a verified controller
        p.setParam("cresco_caller", "controller:t1/" + region.cstate.getRegion() + "/" + region.cstate.getAgent());
        RevocationSync.onPingAtParent(global, p, true);
        reg(global).awaitRevocations();
        if (thisBuild) {
            RevocationSync.onParentPong(region, p, "rgG", "gc");
        } else {
            MsgEvent q = ping(region);
            for (String k : new String[]{"rev_epoch", "rev_seq", "rev_ack", "rev_parent_list"}) if (p.getParam(k) != null) q.setParam(k, p.getParam(k));
            RevocationSync.onParentPong(region, q, "rgG", "gc");
        }
        reg(region).awaitRevocations();
        return p;
    }

    /** An agent of {@code region} pings its controller and reads the pong. */
    private static void agentExchange(ControllerEngine agent, ControllerEngine region) {
        MsgEvent ap = new MsgEvent(MsgEvent.Type.EXEC, agent.cstate.getRegion(), agent.cstate.getAgent(), null, region.cstate.getRegion(),
                region.cstate.getAgent(), null, true, false);
        RevocationSync.onChildPing(agent, ap);
        RevocationSync.onPingAtParent(region, ap, false);
        RevocationSync.onParentPong(agent, ap, region.cstate.getRegion(), region.cstate.getAgent());
        reg(agent).awaitRevocations();
    }

    /** The names a pong asks about ({@code A|id|name|..} / {@code L|id|name|..}), with their request ids. */
    private static Map<String, String> requests(MsgEvent pong) {
        Map<String, String> out = new HashMap<>();
        if (pong.getParam("rev_names") == null) return out;
        JsonObject o = GSON.fromJson(pong.getCompressedParam("rev_names"), JsonObject.class);
        o.getAsJsonArray("items").forEach(i -> { String[] p = i.getAsString().split("\\|", -1); out.put(p[2], p[1]); });
        return out;
    }

    private static String[] health(ControllerEngine e) { return e.getCertificateManager().identityHealth(); }

    private static boolean outstanding(ControllerEngine global, String name) {
        String[] h = health(global);
        return !"OK".equals(h[0]) && h[1] != null && h[1].contains(name);
    }

    // ------------------------------------------------------------------ at the global

    /** By name, with no key bound here: the name is reserved at once, the revocation is recorded, and it is said to be outstanding. */
    @Test
    void aNameRevokedAtTheGlobalIsReservedThereAtOnceAndSaidToBeOutstanding() throws Exception {
        Steps s = new Steps();
        ControllerEngine global = node("rgG", "gc", true);
        PeerTrustRegistry atGlobal = reg(global);
        String why = atGlobal.revoke("rgB/b1", "operator");
        s.check(why == null, "a revocation by name at the global was not applied: " + why);
        s.check(atGlobal.isTombstoned("rgB", "b1"), "the name is not reserved at the global");
        s.check(atGlobal.nameRefusal("rgB", "b1") != null, "messages signed in the revoked name are accepted at the global");
        s.check(!atGlobal.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok, "a key was bound to the reserved name");
        // the operator is told the truth: reserved here, not answered by the region that holds the key, and why
        String[] h = health(global);
        s.check("WARN".equals(h[0]) && h[1] != null && h[1].contains("rgB/b1") && h[1].contains("NOT answered")
                && h[1].contains("no controller of region rgB has joined"), "the health check does not say what is outstanding: " + h[0] + " " + h[1]);
        s.check(atGlobal.revoke("rgB/b1", "operator") == null, "the same revocation again was refused");
        // its own name, and an unbound name of its own region, are not revoked this way
        s.check(atGlobal.revoke("rgG/gc", "operator") != null && !atGlobal.isTombstoned("rgG", "gc"), "the global reserved its own name");
        s.check("no key is bound to rgG/typo (revoke its key fingerprint instead)".equals(atGlobal.revoke("rgG/typo", "operator"))
                && !atGlobal.isTombstoned("rgG", "typo"), "an unbound name of the global's own region was not refused as before");
        // durable: a registry opened on the same files holds the reservation
        atGlobal.awaitRevocations();
        PeerTrustRegistry again = PeerTrustRegistry.open(fileOf(global), List.of(), List.of(), 30, m -> { });
        s.check(again.isTombstoned("rgB", "b1"), "the reservation did not survive a restart");
        s.check(!again.isSealed(), "the registry is sealed after a restart: " + again.sealReason());
        s.done();
    }

    // ------------------------------------------------------------------ the whole way

    /**
     * The region's controller applies it as if its own operator had revoked the name -- the bound key revoked, the name
     * reserved, the node's connections closed (the revocation listener) -- and reports the key. The global records the
     * report and revokes NOTHING on it: no other region learns the key until the operator revokes the reported
     * fingerprint at the global, which then travels as any revocation does.
     */
    @Test
    void theRegionRevokesTheKeyAndReportsItAndTheGlobalRevokesNothingOnItsWord() throws Exception {
        Steps s = new Steps();
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false), regionC = node("rgC", "rc", false);
        join(global, regionB);
        join(global, regionC);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB), atC = reg(regionC);
        KeyPair b1 = agentOf(regionB, "b1");                                           // bound where it joined, and nowhere else
        String k = fp(b1.getPublic());
        List<String> closedAtB = new CopyOnWriteArrayList<>();
        atB.setRevokedListener(closedAtB::add);                                        // what closes the key's connections
        exchange(regionB, global);
        exchange(regionC, global);

        s.check(atGlobal.revoke("rgB/b1", "operator") == null, "the revocation by name was not applied at the global");
        s.check(atGlobal.isTombstoned("rgB", "b1"), "the name is not reserved at the global");
        s.check(outstanding(global, "rgB/b1"), "the global does not say the revocation is outstanding");

        // the next pong takes it to the controller that holds the binding
        MsgEvent pong = exchange(regionB, global);
        s.check(requests(pong).get("rgB/b1") != null, "the pong to region rgB's controller did not carry the revocation by name");
        s.check(requests(exchange(regionC, global)).isEmpty(), "another region's controller was asked");
        s.check(atB.isRevokedFor(k, "rgB") && atB.isRevokedFor(k, "rgZ"), "the region did not revoke the bound key for every name");
        s.check(atB.isTombstoned("rgB", "b1"), "the region did not reserve the name");
        s.check(List.of(k).equals(closedAtB), "the region did not close the node's connections: " + closedAtB);
        s.check(!atB.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok, "another key took the revoked name at its region");
        s.check(!atB.bind("t1", "rgB", "other", b1.getPublic(), "discovery").ok, "the revoked key came back under another name at its region");
        s.check(outstanding(global, "rgB/b1"), "the global reported the revocation done before the region's answer reached it");

        // the region's next ping answers: the request is settled, and the global revoked nothing on the region's word
        exchange(regionB, global);
        s.check("OK".equals(health(global)[0]), "the request is not settled by the region's answer: " + health(global)[1]);
        s.check(requests(exchange(regionB, global)).isEmpty(), "a settled revocation by name is still sent");
        s.check(atGlobal.revocations().isEmpty(), "the global revoked a key on a region's word: " + atGlobal.revocations());
        exchange(regionC, global);
        s.check(atC.revocations().isEmpty(), "a key went to another region on a region's word");
        s.check(atGlobal.isTombstoned("rgB", "b1"), "the name is no longer reserved at the global");
        s.check(!atC.isTombstoned("rgB", "b1"), "the name is reserved at a region it was never bound at (no rebind could lift it there)");

        // the region's own agents hold the key on their region's list
        ControllerEngine agent = TlsTestSupport.engine("rgB", "b2", Map.of("broker_security_enabled", "true"));
        assertTrue(atB.bind("t1", "rgB", "b2", leaf(agent).getPublicKey(), "discovery").ok);
        assertNull(reg(agent).pin("rgB", "rc", leaf(regionB), PeerTrustRegistry.ROLE_CONTROLLER, true));
        agentExchange(agent, regionB);
        s.check(reg(agent).isRevokedKey(k), "the region's agents do not hold the revoked key");
        s.check(!reg(agent).isTombstoned("rgB", "b1"), "the name is reserved at the region's agents");

        // the operator's own word for the key the region reported: every node refuses it, under any name
        s.check(atGlobal.revoke(k, "operator") == null, "the operator could not revoke the reported fingerprint");
        exchange(regionC, global);
        s.check(atGlobal.isRevokedFor(k, "rgG") && atC.isRevokedFor(k, "rgC"), "the operator's revocation of the key did not reach the fabric");
        s.check(!atC.bind("t1", "rgC", "b1", b1.getPublic(), "discovery").ok, "the revoked node came back under another region");
        s.done();
    }

    /** A name its region holds no key for: reserved there too, and the request is settled (no key was revoked anywhere). */
    @Test
    void aNameItsRegionHoldsNoKeyForIsReservedThereAndSettled() throws Exception {
        Steps s = new Steps();
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        s.check(reg(global).revoke("rgB/never", "operator") == null, "the revocation by name was not applied at the global");
        exchange(regionB, global);
        s.check(reg(regionB).isTombstoned("rgB", "never"), "the region did not reserve the name");
        exchange(regionB, global);
        s.check("OK".equals(health(global)[0]) && reg(global).isTombstoned("rgB", "never"), "the request is not settled: " + health(global)[1]);
        s.check(reg(global).revocations().isEmpty() && reg(regionB).revocations().isEmpty(), "a key was revoked for a name that had none");
        // the region's controller's own name is not revoked for its global
        s.check(reg(global).revoke("rgB/rc", "operator") == null && reg(global).isRevokedKey(fp(leaf(regionB).getPublicKey())),
                "a controller bound at the global is not revoked there by its name, as before");
        s.done();
    }

    // ------------------------------------------------------------------ earlier builds

    /**
     * A region's controller of an earlier build says nothing of revocations by name on its pings: it is sent no request
     * (nothing new reaches it), and the global says the revocation is outstanding and why. Once it is upgraded it is asked.
     */
    @Test
    void anEarlierBuildIsAskedNothingAndTheGlobalSaysWhatIsOutstanding() throws Exception {
        Steps s = new Steps();
        ControllerEngine global = node("rgG", "gc", true), old = node("rgO", "rc", false);
        join(global, old);
        PeerTrustRegistry atGlobal = reg(global), atOld = reg(old);
        KeyPair o1 = agentOf(old, "o1");
        s.check(atGlobal.revoke("rgO/o1", "operator") == null, "the revocation by name was not applied at the global");
        boolean sent = false;
        for (int i = 0; i < 3; i++) {
            MsgEvent pong = exchange(old, global, false);
            sent |= pong.getParam("rev_names") != null;
        }
        s.check(!sent, "an earlier build was sent a revocation by name");
        s.check(!atOld.isRevokedKey(fp(o1.getPublic())) && !atOld.isTombstoned("rgO", "o1"), "something was applied at the earlier build");
        String[] h = health(global);
        s.check("WARN".equals(h[0]) && h[1] != null && h[1].contains("rgO/o1") && h[1].contains("earlier build"),
                "the global does not say that the region's controller is an earlier build: " + h[0] + " " + h[1]);
        // upgraded: asked, applied, answered
        s.check(requests(exchange(old, global)).get("rgO/o1") != null, "the upgraded controller was not asked");
        exchange(old, global);
        s.check(atOld.isRevokedKey(fp(o1.getPublic())) && atOld.isTombstoned("rgO", "o1"), "the upgraded controller did not apply the request");
        s.check("OK".equals(health(global)[0]), "the request is not settled after the upgrade: " + health(global)[1]);
        s.done();
    }

    // ------------------------------------------------------------------ restarts and re-keys while a request is open

    /** The global restarts between its request and the region's answer: the request, with its id, is the same after it. */
    @Test
    void aGlobalRestartBetweenRequestAndAnswerKeepsTheRequest() throws Exception {
        Steps s = new Steps();
        Path gDir = Files.createTempDirectory("byname-rgG");
        ControllerEngine global = node("rgG", "gc", true, gDir), regionB = node("rgB", "rc", false);
        join(global, regionB);
        KeyPair b1 = agentOf(regionB, "b1");
        s.check(reg(global).revoke("rgB/b1", "operator") == null, "the revocation by name was not applied at the global");
        MsgEvent pong = exchange(regionB, global);                                     // asked; applied at the region
        String id = requests(pong).get("rgB/b1");
        s.check(id != null, "the region was not asked");
        s.check(reg(regionB).isRevokedKey(fp(b1.getPublic())), "the region did not revoke the key");
        // the global stops before the answer comes, and starts again: the same node (its key), its registry read from its files
        reg(global).awaitRevocations();
        String key = fp(leaf(global).getPublicKey());
        PeerTrustRegistry.open(fileOf(global), List.of(), List.of(), 30, m -> { });
        ControllerEngine restarted = node("rgG", "gc", true, gDir);
        assertEquals(key, fp(leaf(restarted).getPublicKey()), "the restarted global is not the same node");
        assertNotSame(reg(global), reg(restarted));
        s.check(reg(restarted).isTombstoned("rgB", "b1"), "the reservation did not survive the restart");
        s.check(outstanding(restarted, "rgB/b1"), "the restarted global does not hold the revocation as outstanding");
        MsgEvent pong2 = exchange(regionB, restarted);                                 // (carries the region's answer, and is asked again)
        s.check("OK".equals(health(restarted)[0]), "the answer to the request made before the restart was not taken: " + health(restarted)[1]);
        MsgEvent pong3 = exchange(regionB, restarted);
        s.check(requests(pong3).isEmpty(), "a settled request is sent after the restart");
        s.check(id == null || requests(pong2).isEmpty() || id.equals(requests(pong2).get("rgB/b1")), "the request changed its id across the restart");
        s.check(reg(restarted).revocations().isEmpty(), "the restarted global revoked a key on the region's word");
        s.done();
    }

    /**
     * The region's controller is re-installed with a new key while a request is open, and the operator rebinds it at the
     * global: the request is answered by that controller, with its new key; an answer signed with the former key -- by
     * whoever still holds it -- is not read.
     */
    @Test
    void aControllerReKeyedWhileARequestIsOpenAnswersWithItsNewKey() throws Exception {
        Steps s = new Steps();
        Path bDir = Files.createTempDirectory("byname-rgB");
        ControllerEngine global = node("rgG", "gc", true), former = node("rgB", "rc", false, bDir);
        join(global, former);
        KeyPair b1 = agentOf(former, "b1");
        s.check(reg(global).revoke("rgB/b1", "operator") == null, "the revocation by name was not applied at the global");
        // the request is on its way to the former key's controller ...
        MsgEvent heldBack = ping(former);
        RevocationSync.onChildPing(former, heldBack);
        heldBack.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, heldBack, true);
        s.check(requests(heldBack).get("rgB/b1") != null, "the region was not asked");
        // ... when the controller loses its identity and starts with a new key: the same name, its registry as it was (a copy:
        // the former key's holder goes on below); the operator rebinds it at the global
        reg(former).awaitRevocations();
        Path copy = Files.createTempDirectory("byname-rgB-rekeyed");
        Files.createDirectories(copy.resolve("security"));
        try (var files = Files.list(bDir.resolve("security"))) {
            for (Path f : (Iterable<Path>) files::iterator) {
                String n = f.getFileName().toString();
                if (Files.isRegularFile(f) && !n.startsWith("node-identity") && !n.endsWith(".lock")) Files.copy(f, copy.resolve("security").resolve(n));
            }
        }
        ControllerEngine reKeyed = node("rgB", "rc", false, copy);
        String newKey = fp(leaf(reKeyed).getPublicKey());
        assertNotEquals(fp(leaf(former).getPublicKey()), newKey, "the controller was not re-keyed");
        assertFalse(reg(reKeyed).isSealed(), reg(reKeyed).sealReason());
        assertFalse(reg(reKeyed).bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok, "the re-keyed controller lost its bindings");
        assertNotNull(reg(global).pin("rgB", "rc", leaf(reKeyed), PeerTrustRegistry.ROLE_CONTROLLER, false), "a re-keyed controller was bound without a rebind");
        Files.writeString(Path.of(fileOf(global) + ".rebind"), "rgB/rc=" + newKey + "\n");
        assertNull(reg(global).pin("rgB", "rc", leaf(reKeyed), PeerTrustRegistry.ROLE_CONTROLLER, false));
        assertNull(reg(reKeyed).pin("rgG", "gc", leaf(global), PeerTrustRegistry.ROLE_CONTROLLER, true));
        // the former key's holder takes the request and answers it: signed with a key that is no longer rgB/rc's at the global
        RevocationSync.onParentPong(former, heldBack, "rgG", "gc");
        reg(former).awaitRevocations();
        MsgEvent late = ping(former);
        RevocationSync.onChildPing(former, late);
        s.check(late.getParam("rev_name_report") != null, "(the former key's holder made no answer: the next step shows nothing)");
        late.setParam("cresco_caller", "controller:t1/rgB/rc");
        RevocationSync.onPingAtParent(global, late, true);
        reg(global).awaitRevocations();
        s.check(outstanding(global, "rgB/b1"), "an answer signed with the controller's former key settled the request");
        // the re-keyed controller is asked and answers
        s.check(requests(exchange(reKeyed, global)).get("rgB/b1") != null, "the re-keyed controller was not asked");
        exchange(reKeyed, global);
        s.check(reg(reKeyed).isRevokedKey(fp(b1.getPublic())) && reg(reKeyed).isTombstoned("rgB", "b1"), "the re-keyed controller did not apply the request");
        s.check("OK".equals(health(global)[0]), "the re-keyed controller's answer did not settle the request: " + health(global)[1]);
        s.done();
    }

    // ------------------------------------------------------------------ rebind

    /**
     * The operator rebinds the name where it was revoked -- the existing token line in the global's rebind file: the
     * global lifts its reservation, the region accepts exactly that key for the name, once, and the key the region revoked
     * stays revoked there.
     */
    @Test
    void anOperatorRebindAtTheGlobalLiftsTheNameThereAndAtItsRegion() throws Exception {
        Steps s = new Steps();
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", false);
        join(global, regionB);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        KeyPair b1 = agentOf(regionB, "b1"), fresh = TlsTestSupport.rsa(), thief = TlsTestSupport.rsa();
        String k = fp(b1.getPublic());
        s.check(atGlobal.revoke("rgB/b1", "operator") == null, "the revocation by name was not applied at the global");
        exchange(regionB, global);
        exchange(regionB, global);
        s.check(atB.isRevokedFor(k, "rgB") && atB.isTombstoned("rgB", "b1") && atGlobal.isTombstoned("rgB", "b1"), "the name was not revoked at its region and reserved at both");
        s.check(!atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "the reserved name was bound without a rebind");

        Files.writeString(Path.of(fileOf(global) + ".rebind"), "rgB/b1=" + fp(fresh.getPublic()) + "\n");
        long until = System.currentTimeMillis() + 25_000;                              // the operator files are read every 10 s
        while (atGlobal.isTombstoned("rgB", "b1") && System.currentTimeMillis() < until) Thread.sleep(200);
        s.check(!atGlobal.isTombstoned("rgB", "b1") && atGlobal.nameRefusal("rgB", "b1") == null, "the operator rebind did not lift the name at the global");

        MsgEvent pong = exchange(regionB, global);
        s.check(requests(pong).get("rgB/b1") != null, "the rebind was not sent to the name's region");
        s.check(!atB.bind("t1", "rgB", "b1", thief.getPublic(), "discovery").ok, "the rebind let ANOTHER key take the name");
        s.check(atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "the key the operator named was refused at the region");
        s.check(!atB.isTombstoned("rgB", "b1"), "the name is still reserved at the region");
        s.check(atB.isRevokedFor(k, "rgB") && !atB.bind("t1", "rgB", "b9", b1.getPublic(), "discovery").ok, "the revoked key did not stay revoked at its region");

        exchange(regionB, global);                                                     // the region confirms
        s.check(requests(exchange(regionB, global)).isEmpty(), "a confirmed rebind is still sent");
        s.check(!atB.isRevokedKey(fp(fresh.getPublic())) && atB.bind("t1", "rgB", "b1", fresh.getPublic(), "discovery").ok, "the rebound key was revoked");
        s.done();
    }

    // ------------------------------------------------------------------ pre-provisioned identities, on the wire

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String region, String agent, boolean regional, boolean global) throws Exception {
        Path dir = Files.createTempDirectory("byname-pki-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("identity_keystore_reload_seconds", "3600");
        cfg.put("node_identity_dir", Files.createDirectories(dir.resolve("security")).toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        if (global) cfg.put("is_global", "true");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = regional;
        st.global = global;
        if (regional && !global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned());
        return e;
    }

    /**
     * Pre-provisioned (the production profile): the global never verified the agent's certificate, so it holds no key for
     * the name either. It asks the region's controller, which verified it: that controller revokes the key of the
     * certificate it saw for the name, refuses the name, and reports the key; the global revokes nothing on its word.
     */
    @Test
    void preProvisionedTheGlobalAsksTheRegionThatVerifiedTheCertificate() throws Exception {
        Steps s = new Steps();
        TlsTestSupport.provider();
        long now = System.currentTimeMillis();
        PkiTestSupport.Ca root = PkiTestSupport.root("By Name Root");
        Path pki = Files.createTempDirectory("byname-pki");
        Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        PkiTestSupport.Node gN = PkiTestSupport.write(PkiTestSupport.global(root, "t1", "rgG", "gc", now - 60_000, 10 * DAY), pki.resolve("g"));
        PkiTestSupport.Node rN = PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgB", "rc", true, now - 60_000, 10 * DAY), pki.resolve("rc"));
        PkiTestSupport.Node b1 = PkiTestSupport.node(root, "t1", "rgB", "b1", false, now - 60_000, 10 * DAY);
        ControllerEngine global = provisioned(gN, anchors, "rgG", "gc", true, true), regionB = provisioned(rN, anchors, "rgB", "rc", true, false);
        PeerTrustRegistry atGlobal = reg(global), atB = reg(regionB);
        // what a join leaves: each verified the other's certificate, and the global recorded the region's controller
        assertNotNull(atGlobal.verifiedIdentity(regionB.getCertificateManager().getPublicCertificate()));
        assertNotNull(atB.verifiedIdentity(global.getCertificateManager().getPublicCertificate()));
        assertNull(atGlobal.allowBridgePeer("t1", "rgB", "rc"));
        assertNotNull(atB.verifiedIdentity(b1.chain), "precondition: the region verifies its agent's certificate");
        Thread.sleep(40);
        String k = fp(b1.kp.getPublic());

        s.check(atGlobal.revoke("rgB/b1", "operator") == null, "the revocation by name was not applied at the global");
        s.check(atGlobal.verifiedIdentity(b1.chain) == null, "the global still verifies the revoked name's certificate");
        s.check(outstanding(global, "rgB/b1"), "the global does not say the revocation is outstanding");
        s.check(requests(exchange(regionB, global)).get("rgB/b1") != null, "the region's controller was not asked");
        s.check(atB.isRevokedFor(k, "rgB") && atB.verifiedIdentity(b1.chain) == null, "the region still verifies the revoked certificate");
        s.check(atB.isTombstoned("rgB", "b1"), "the region did not reserve the name");
        exchange(regionB, global);
        s.check("OK".equals(health(global)[0]), "the request is not settled: " + health(global)[1]);
        s.check(atGlobal.revocations().isEmpty(), "the global revoked a key on a region's word: " + atGlobal.revocations());
        s.done();
    }
}
