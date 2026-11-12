package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A region has ONE controller where a controller is recorded for it. Until this rule a holder of a discovery secret --
 * every region controller holds the global's, every agent its region's -- could join under a NEW agent name of a region
 * whose controller was already joined, be bound first come as a controller of that region, and be given everything that
 * region's controller is: its word in the registry, the stamps of its nodes over a bridge, a place in the trust bundle,
 * a revocation list that revokes the real controller, and the inboxes of the region's nodes under another tenant's name.
 *
 * <p>Everything here goes through the real discovery exchange between real nodes and uses nothing the rule added to the
 * code's interface: the tests that show a takeover refused, a second controller refused in each place one is admitted, a
 * double registry reported and the operator's ways to replace a controller all FAIL on the code before the rule; the
 * tests of what must stay as it was (legitimate re-joins, the regional CA, the pre-provisioned PKI) pass on both.
 */
class OneControllerPerRegionTest {

    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";
    private static final Gson GSON = new Gson();
    private static final long DAY = 24L * 3600 * 1000;

    /** A controller of the standard profile (per-node trust). {@code dir}: its identity and registry (the same node again on the same directory). */
    private static ControllerEngine controller(String region, String agent, String tenant, Path dir, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("broker_security_enabled", "true");
        m.put("msg_signing_sessions", "false");
        m.put("tenant_id", tenant);
        if (dir != null) {
            m.put("node_identity_dir", dir.resolve("security").toString());
            m.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        }
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = !"false".equals(m.get("is_controller"));
        st.global = "true".equals(m.get("is_global"));
        if (st.regional && !st.global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static ControllerEngine controller(String region, String agent, String tenant, String... extra) throws Exception {
        return controller(region, agent, tenant, null, extra);
    }

    private static ControllerEngine global() throws Exception { return controller("rgG", "gc", "t1", "is_global", "true"); }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(leaf(e)); }

    private static Path fileOf(ControllerEngine e) { return Path.of(e.getPluginBuilder().getConfig().getStringParam("peer_trust_file")); }

    /** The real exchange (discovery v2) of {@code type}; the client's result node, or null when either side refused. */
    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        return join(client, server, type, new String[1]);
    }

    /** As above; {@code told[0]}: what the responder told the joiner when it refused it. */
    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type, String[] told) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        if (hello == null) return null;
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        if (proof == null) return null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
        told[0] = result.reason;
        return c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005);
    }

    private static DiscoveryNode join(ControllerEngine region, ControllerEngine global) { return join(region, global, DiscoveryType.GLOBAL); }

    /** One ping of {@code child} and the pong back: revocation lists and trust data, as the watchers and executors carry them. */
    private static void ping(ControllerEngine child, String tenant, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(child, p);
        child.msgSigning().onChildPing(p);
        p = wire(p);
        // what the parent's broker stamps on a message of a verified connection -- the most a second controller could get
        Caller.controller(tenant, child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        RevocationSync.onPingAtParent(parent, p, childIsRegion);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        MsgEvent pong = wire(p);
        RevocationSync.onParentPong(child, pong, parent.cstate.getRegion(), parent.cstate.getAgent());
        child.msgSigning().onParentPong(pong, parent.cstate.getRegion(), parent.cstate.getAgent());
        reg(parent).awaitRevocations();
        reg(child).awaitRevocations();
    }

    private static MsgEvent sent(ControllerEngine from, Caller caller, ControllerEngine to, String action) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, from.cstate.getRegion(), from.cstate.getAgent(), null,
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", action);
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /** A global, region rgB's controller joined to it, and a second controller identity that then claims region rgB. */
    private static final class Fabric {
        ControllerEngine global, real, second;
        DiscoveryNode secondJoined;
        String told;
    }

    private static Fabric secondControllerOf(String tenant) throws Exception {
        Fabric f = new Fabric();
        f.global = global();
        f.real = controller("rgB", "rc", "t1");
        assertNotNull(join(f.real, f.global), "precondition: region rgB's controller joins the global");
        ping(f.real, "t1", f.global, true);
        ping(f.real, "t1", f.global, true);
        // another holder of the global's discovery secret (every region controller is one) with a new key and the name rgB/zz
        f.second = controller("rgB", "zz", tenant);
        String[] told = new String[1];
        f.secondJoined = join(f.second, f.global, DiscoveryType.GLOBAL, told);
        f.told = told[0];
        return f;
    }

    // ------------------------------------------------------------------ the takeover, piece by piece

    /** The join itself is refused, nothing of the joiner is recorded, and the joiner is told why. */
    @Test
    void aSecondControllerIsRefusedAtTheJoinAndNothingOfItIsRecorded() throws Exception {
        Fabric f = secondControllerOf("t1");
        PeerTrustRegistry atGlobal = reg(f.global);
        assertNull(f.secondJoined, "a second controller joined the global under region rgB's name");
        assertNull(atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "zz")), "its key was pinned");
        assertNull(atGlobal.boundRole("rgB", "zz", leaf(f.second).getPublicKey()));
        assertNull(atGlobal.verifiedIdentity(new X509Certificate[]{leaf(f.second)}), "its certificate verifies at the global");
        assertFalse(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgB", "zz", null)), "it is a bridge peer");
        // the refused joiner is told why, and what ITS operator can do: the rebind line for its name and key, and where it goes
        assertTrue(f.told != null && f.told.contains("region rgB already has a controller recorded there (rgB/rc)")
                && f.told.contains("'rgB/zz=" + key(f.second) + "'") && f.told.contains("peer-trust.json.rebind"), "the joiner is not told why: " + f.told);
        // a second try, and a third under yet another name: refused each time; the region's controller is what it was
        assertNull(join(f.second, f.global));
        assertNull(join(controller("rgB", "yy", "t1"), f.global));
        assertEquals(key(f.real), atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        assertTrue(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));
        assertNotNull(join(f.real, f.global), "the region's own controller no longer joins");
    }

    /** The registry: it does not speak for the region -- it may not report, or remove, the region and its agents. */
    @Test
    void aSecondControllerDoesNotSpeakForTheRegionInTheRegistry() throws Exception {
        Fabric f = secondControllerOf("t1");
        Caller zz = Caller.controller("t1", "rgB", "zz"), rc = Caller.controller("t1", "rgB", "rc");
        assertFalse(f.global.speaksForRegion(zz), "the second controller speaks for region rgB");
        assertFalse(f.global.joinedAsController("t1", "rgB", "zz"), "it stands below the global as a joined controller");
        assertFalse(reg(f.global).isJoinedController("t1", "rgB", "zz"));
        assertNotNull(RegistryBinding.refusal(zz, true, a -> "rgB", f.global::speaksForRegion, null, null, null, "rgB", null, null, null, null, null),
                "it may unregister region rgB");
        assertNotNull(RegistryBinding.refusal(zz, true, a -> "rgB", f.global::speaksForRegion, null, null, null, null, "rgB_b1", null, null, null, null),
                "it may unregister an agent of region rgB");
        assertNotNull(RegistryBinding.refusal(zz, true, a -> "rgB", f.global::speaksForRegion, "rgB", null, null, null, null, null, null, null, null),
                "it may report region rgB alive");
        assertTrue(f.global.speaksForRegion(rc), "the region's own controller no longer speaks for it");
        assertNull(RegistryBinding.refusal(rc, true, a -> "rgB", f.global::speaksForRegion, "rgB", null, null, null, null, null, null, null, null));
    }

    /** The trust bundle: its key is not listed, so nothing it attests is an agent of the region to anybody. */
    @Test
    void aSecondControllersKeyIsNotInTheTrustBundleAndWhatItAttestsIsNobody() throws Exception {
        Fabric f = secondControllerOf("t1");
        int before = f.global.msgSigning().bundleSize();
        ping(f.second, "t1", f.global, true);
        ping(f.second, "t1", f.global, true);
        assertEquals(before, f.global.msgSigning().bundleSize(), "its key was listed in the trust bundle");
        // a key only the second controller attests as rgB/b1 (the region's own controller never saw it)
        ControllerEngine fake = TlsTestSupport.engine("rgB", "b1", Map.of("broker_security_enabled", "true", "msg_signing_sessions", "false"));
        assertNull(reg(fake).pin("rgB", "zz", leaf(f.second), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(reg(f.second).pin("rgB", "b1", leaf(fake), PeerTrustRegistry.ROLE_AGENT, false));
        ((TlsTestSupport.State) fake.cstate).parentRegion = "rgB";
        ((TlsTestSupport.State) fake.cstate).parentAgent = "zz";
        ping(fake, "t1", f.second, false);
        ping(fake, "t1", f.second, false);
        Caller b1 = Caller.controller("t1", "rgB", "b1");
        assertFalse(f.global.msgSigning().verify(sent(fake, b1, f.global, "probe"), b1, false).ok(),
                "a message signed as rgB/b1 by a key only the second controller attests verifies at the global");
        // what the region's OWN controller attests still is an agent of the region (the bundle works as it did)
        ControllerEngine good = TlsTestSupport.engine("rgB", "b2", Map.of("broker_security_enabled", "true", "msg_signing_sessions", "false"));
        assertNull(reg(good).pin("rgB", "rc", leaf(f.real), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(reg(f.real).pin("rgB", "b2", leaf(good), PeerTrustRegistry.ROLE_AGENT, false));
        ((TlsTestSupport.State) good.cstate).parentRegion = "rgB";
        ((TlsTestSupport.State) good.cstate).parentAgent = "rc";
        ping(f.real, "t1", f.global, true);
        ping(good, "t1", f.real, false);
        ping(good, "t1", f.real, false);
        assertEquals(before, f.global.msgSigning().bundleSize());
        Caller b2 = Caller.controller("t1", "rgB", "b2");
        assertTrue(f.global.msgSigning().verify(sent(good, b2, f.global, "probe"), b2, false).ok(),
                "an agent the region's own controller attests no longer verifies at the global");
    }

    /** Its revocation list is nobody's: it does not revoke the region's controller (or anything) at the global. */
    @Test
    void aSecondControllersRevocationListRevokesNothing() throws Exception {
        Fabric f = secondControllerOf("t1");
        PeerTrustRegistry atGlobal = reg(f.global);
        assertNull(reg(f.second).revoke(key(f.real), "the second controller"));
        ping(f.second, "t1", f.global, true);
        ping(f.second, "t1", f.global, true);
        assertFalse(atGlobal.isRevokedFor(key(f.real), "rgB"), "the second controller's list revoked the region's controller");
        assertFalse(atGlobal.isTombstoned("rgB", "rc"));
        assertNotNull(atGlobal.verifiedIdentity(new X509Certificate[]{leaf(f.real)}), "the region's controller no longer verifies");
        assertTrue(f.global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertTrue(atGlobal.revocations().isEmpty());
    }

    // ------------------------------------------------------------------ the broker: another tenant's inboxes

    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) brokers.get(i).stop();
    }

    private static TlsTestSupport.Id idOf(ControllerEngine e) throws Exception {
        X509Certificate[] chain = e.getCertificateManager().getPublicCertificate();
        return new TlsTestSupport.Id(new KeyPair(chain[0].getPublicKey(), e.getCertificateManager().identityPrivateKey()), chain);
    }

    private Session local(BrokerService b) throws Exception {
        Connection c = new ActiveMQConnectionFactory("vm://" + b.getBrokerName() + "?create=false").createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static int drain(MessageConsumer c, long firstWaitMs) throws Exception {
        int n = 0;
        for (Message m = c.receive(firstWaitMs); m != null; m = c.receive(500)) n++;
        return n;
    }

    /**
     * A controller of tenant t2 that claims region rgB -- tenant t1's region, whose controller is joined -- is refused at
     * the join in every tenant, so the bridge it then opens to the global is no bridge peer's: it is given nothing of
     * tenant t1's inboxes of region rgB (a bridge is given the inboxes of its OWN region under any tenant's name), while
     * the region's own controller's bridge still is.
     */
    @Test
    void aControllerOfAnotherTenantThatClaimsTheRegionReadsNoneOfItsInboxes() throws Exception {
        // as the matrix's `tenants` topology: a global in the superuser tenant, whose key each tenant's region is given
        ControllerEngine global = controller("rgG", "gc", "cresco-system", "is_global", "true");
        ControllerEngine real = controller("rgB", "rc", "t1", "controller_trust_anchors", key(global));
        assertNotNull(join(real, global), "precondition: tenant t1's region rgB joins the global");
        ControllerEngine second = controller("rgB", "zz", "t2", "controller_trust_anchors", key(global));
        DiscoveryNode joined = join(second, global);

        // over real duplex bridges to the global's broker, which enforces what a bridge may carry by the global's registry
        TlsTestSupport.Id g = idOf(global);
        X509Certificate[] anchors = {last(global), last(real), last(second)};
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "cresco-system");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("onectl").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, reg(global));
        BrokerService gb = TlsTestSupport.broker("rgG_gc", g, true, new BrokerPlugin[]{authz}, authz, anchors);
        brokers.add(gb);
        int[] got = new int[2];
        ControllerEngine[] who = {second, real};
        for (int i = 0; i < 2; i++) {
            TlsTestSupport.Id id = idOf(who[i]);
            BrokerService b = TlsTestSupport.broker("rgB_" + who[i].cstate.getAgent(), id, false, null, null, anchors);
            brokers.add(b);
            NetworkConnector nc = b.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(gb)
                    + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
            nc.setName("cresco-bridge-onectl-" + i);
            nc.setDuplex(true);
            nc.setSslContext(new SslContext(id.keyManagers(), TlsTestSupport.trustManagers(anchors), new SecureRandom()));
            nc.start();
            Thread.sleep(2000);
            Session there = local(b);
            MessageConsumer inbox = there.createConsumer(there.createQueue("T.t1.rgB_b" + i));   // an inbox of tenant t1's agent in rgB
            Thread.sleep(2000);
            Session operator = local(gb);
            var p = operator.createProducer(operator.createQueue("T.t1.rgB_b" + i));
            for (int k = 0; k < 3; k++) p.send(operator.createTextMessage("for rgB_b" + i + " #" + k));
            got[i] = drain(inbox, 6000);
        }
        assertEquals(0, got[0], "tenant t2's controller was handed the messages for tenant t1's inbox of region rgB");
        assertEquals(3, got[1], "the region's own controller's bridge is no longer given its region's inbox");
        assertNull(joined, "a controller of tenant t2 joined the global as a controller of tenant t1's region rgB");
        assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t2", "rgB", "zz", null)));
        assertFalse(global.speaksForRegion(Caller.controller("t2", "rgB", "zz")));
    }

    private static X509Certificate last(ControllerEngine e) {
        X509Certificate[] c = e.getCertificateManager().getPublicCertificate();
        return c[c.length - 1];
    }

    // ------------------------------------------------------------------ every place a controller is admitted

    /** A region that bridges with a peer region (regional_peers): a second controller of that peer region is refused there. */
    @Test
    void aRegionRefusesASecondControllerOfAPeerRegion() throws Exception {
        ControllerEngine regionA = controller("rgA", "rc", "t1"), peerB = controller("rgB", "rc", "t1"), second = controller("rgB", "zz", "t1");
        assertNotNull(join(peerB, regionA, DiscoveryType.REGION), "precondition: the peer regions bridge");
        assertNull(join(second, regionA, DiscoveryType.REGION), "a second controller of region rgB joined region rgA as its peer");
        assertFalse(reg(regionA).isBridgePeer(CrescoIdentity.of("t1", "rgB", "zz", null)));
        assertNull(reg(regionA).pinnedKey(new PeerIdentity.Expected("rgB", "zz")));
        assertNotNull(join(peerB, regionA, DiscoveryType.REGION), "the peer region's own controller no longer joins");
    }

    /**
     * The other direction: a region is not told by whoever answers its exchange that a region it already knows has another
     * controller -- its global's region, or a peer's. (A responder under a region name the region knows NOTHING of is its
     * first join of that region: first come, which only controller_trust_anchors or the production profile closes.)
     */
    @Test
    void aRegionIsNotToldThatARegionItKnowsHasAnotherController() throws Exception {
        ControllerEngine global = global(), region = controller("rgA", "rc", "t1"), peerB = controller("rgB", "rc", "t1");
        assertNotNull(join(region, global), "precondition: region rgA joins its global");
        assertNotNull(join(region, peerB, DiscoveryType.REGION), "precondition: region rgA bridges to its peer rgB");
        // a holder of the global's secret answers region rgA's exchange as "the global", under its global's region name ...
        ControllerEngine rogueGlobal = controller("rgG", "zz", "t1", "is_global", "true");
        assertNull(join(region, rogueGlobal), "the region joined a second controller of its global's region");
        // ... and under its peer's region name
        ControllerEngine roguePeer = controller("rgB", "zz", "t1", "is_global", "true");
        assertNull(join(region, roguePeer), "the region joined a second controller of its peer's region as its global");
        assertNull(join(region, controller("rgB", "yy", "t1"), DiscoveryType.REGION), "the region bridged to a second controller of its peer's region");
        PeerTrustRegistry at = reg(region);
        assertNull(at.pinnedKey(new PeerIdentity.Expected("rgG", "zz")));
        assertNull(at.pinnedKey(new PeerIdentity.Expected("rgB", "zz")));
        assertFalse(at.isBridgePeer(CrescoIdentity.of("t1", "rgG", "zz", null)) || at.isBridgePeer(CrescoIdentity.of("t1", "rgB", "zz", null)));
        assertNotNull(join(region, global), "the region no longer joins its own global");
        assertNotNull(join(region, peerB, DiscoveryType.REGION));
    }

    /** An agent: whoever holds its region's secret (every agent of the region) does not become its controller under a new name. */
    @Test
    void anAgentRefusesASecondControllerOfItsRegion() throws Exception {
        ControllerEngine rc = controller("rgA", "rc", "t1"), agent = controller("rgA", "a1", "t1", "is_controller", "false");
        assertNotNull(join(agent, rc, DiscoveryType.AGENT), "precondition: the agent joins its region's controller");
        ControllerEngine rogue = controller("rgA", "zz", "t1");
        assertNull(join(agent, rogue, DiscoveryType.AGENT), "the agent joined a second controller of its region");
        assertNull(reg(agent).pinnedKey(new PeerIdentity.Expected("rgA", "zz")));
        assertNotNull(join(agent, rc, DiscoveryType.AGENT), "the agent no longer joins its own controller");
    }

    /**
     * A region's AGENTS follow a replaced controller as they follow a re-keyed one: by ONE operator action at the new
     * controller -- a rebind statement for its name and key, signed with an operator key the agents list -- and not
     * otherwise; the former controller is then no longer the agent's.
     */
    @Test
    void anAgentFollowsAReplacedControllerOnTheOperatorsSignedStatement() throws Exception {
        java.security.KeyPairGenerator g = java.security.KeyPairGenerator.getInstance("EC");
        g.initialize(new java.security.spec.ECGenParameterSpec("secp384r1"));
        KeyPair op = g.generateKeyPair();
        String signer = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(op.getPublic().getEncoded()));
        ControllerEngine rc = controller("rgA", "rc", "t1");
        ControllerEngine a1 = controller("rgA", "a1", "t1", "is_controller", "false", "controller_rebind_signers", signer);
        ControllerEngine a2 = controller("rgA", "a2", "t1", "is_controller", "false");                 // lists no signer
        assertNotNull(join(a1, rc, DiscoveryType.AGENT));
        assertNotNull(join(a2, rc, DiscoveryType.AGENT));
        // the region's controller is replaced: another name, another key
        Path dir = Files.createTempDirectory("onectl-rc2");
        ControllerEngine rc2 = controller("rgA", "rc2", "t1", dir);
        assertNull(join(a1, rc2, DiscoveryType.AGENT), "the agent followed a controller nobody vouched for");
        long now = System.currentTimeMillis();
        Files.writeString(dir.resolve("security").resolve("controller-rebind.json"),
                RebindStatement.sign(op.getPrivate(), op.getPublic(), "rgA/rc2", key(rc2), now, now + 7 * DAY));
        assertNotNull(join(a1, rc2, DiscoveryType.AGENT), "the operator's signed statement did not move the agent to the new controller");
        assertEquals(key(rc2), reg(a1).pinnedKey(new PeerIdentity.Expected("rgA", "rc2")));
        assertNull(reg(a1).pinnedKey(new PeerIdentity.Expected("rgA", "rc")), "the agent still holds the replaced controller");
        assertNull(join(a1, rc, DiscoveryType.AGENT), "the agent went back to the replaced controller");
        assertNull(join(a2, rc2, DiscoveryType.AGENT), "an agent that lists no signer followed the statement");
        assertNotNull(join(a2, rc, DiscoveryType.AGENT));
    }

    // ------------------------------------------------------------------ what is legitimate still joins

    /**
     * Re-joins after a restart; a controller whose agent name is generated anew at each start (the same key, another
     * name: the same controller); a re-keyed controller after the operator's rebind; and agents of the region, which are
     * not its controllers. Passes before the rule too: nothing of this changes.
     */
    @Test
    void legitimateJoinsAreNotRefused() throws Exception {
        ControllerEngine global = global();
        PeerTrustRegistry atGlobal = reg(global);
        Path dir = Files.createTempDirectory("onectl-rgB");
        ControllerEngine rc = controller("rgB", "rc", "t1", dir);
        assertNotNull(join(rc, global));
        assertNotNull(join(rc, global), "the same controller joins again");
        // restarted: the same directory, so the same key, under the same name ...
        ControllerEngine restarted = controller("rgB", "rc", "t1", dir);
        assertEquals(key(rc), key(restarted));
        assertNotNull(join(restarted, global), "a restarted controller does not join");
        // ... and under a generated agent name: the key it kept is what the global knows it by
        ControllerEngine renamed = controller("rgB", "agent-3f2a", "t1", dir);
        assertEquals(key(rc), key(renamed));
        assertNotNull(join(renamed, global), "a controller that kept its key was refused under its new agent name");
        // ... and the new name REPLACED the former one: one name per controller, never two names of one key
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "agent-3f2a")));
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")), "the former name of the key still speaks for the region");
        assertNull(atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "rc")), "the former name of the key stayed recorded");
        assertFalse(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));
        // (and back again: the same controller under its first name)
        assertNotNull(join(restarted, global));
        assertNull(atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "agent-3f2a")));
        assertNotNull(join(renamed, global));
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
        // re-keyed under its name (its identity lost): refused until the operator's rebind, as before
        ControllerEngine reKeyed = controller("rgB", "rc", "t1");
        assertNull(join(reKeyed, global), "a re-keyed controller joined without a rebind");
        Path gf = Files.createTempDirectory("onectl-g").resolve("peer-trust.json");
        ControllerEngine global2 = controller("rgG", "gc", "t1", "is_global", "true", "peer_trust_file", gf.toString());
        ControllerEngine first = controller("rgC", "rc", "t1"), again = controller("rgC", "rc", "t1");
        assertNotNull(join(first, global2));
        assertNull(join(again, global2));
        Files.writeString(Path.of(gf + ".rebind"), "rgC/rc=" + key(again) + "\n");
        assertNotNull(join(again, global2), "the operator's rebind did not admit the re-keyed controller");
        assertEquals(key(again), reg(global2).pinnedKey(new PeerIdentity.Expected("rgC", "rc")));
        // an agent bound under a region is not its controller: the global's own agents do not keep a joiner out, nor the reverse
        ControllerEngine own = controller("rgG", "a1", "t1", "is_controller", "false");
        assertNotNull(join(own, global, DiscoveryType.AGENT));
        assertNotNull(join(controller("rgD", "rc", "t1"), global));
        assertTrue(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgD", "rc", null)));
    }

    // ------------------------------------------------------------------ squatting, and how the operator replaces a controller

    /**
     * Whoever claims a region's name BEFORE its real controller ever joined is its controller here: first come. The real
     * one is then the one refused -- nothing better is possible without an operator's pin -- and is told so; the operator
     * revokes the squatter at the global, and the real controller joins.
     */
    @Test
    void aSquatterIsRemovedByTheOperatorAndTheRealControllerThenJoins() throws Exception {
        Path gf = Files.createTempDirectory("onectl-g").resolve("peer-trust.json");
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "peer_trust_file", gf.toString());
        ControllerEngine squatter = controller("rgB", "zz", "t1"), real = controller("rgB", "rc", "t1");
        assertNotNull(join(squatter, global), "(first come: nothing is recorded for the region yet)");
        String[] told = new String[1];
        assertNull(join(real, global, DiscoveryType.GLOBAL, told), "a second controller joined");
        assertTrue(told[0] != null && told[0].contains("region rgB already has a controller recorded there (rgB/zz)")
                && told[0].contains("'rgB/rc=" + key(real) + "'"), told[0]);
        assertEquals("OK", global.getCertificateManager().identityHealth()[0]);
        // the operator: the squatter's name in the global's revocation file (read every 10 s; here at once)
        assertNull(reg(global).revoke("rgB/zz", "operator file peer-trust.json.revoke"));
        assertNotNull(join(real, global), "the real controller does not join after the squatter was revoked");
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "zz")));
        assertNull(join(squatter, global), "the revoked squatter joined again");
    }

    /**
     * Replacing controller rgB/rc by rgB/rc2 (another host, another key): the new one is refused until the operator
     * authorises it on the recording node with the usual rebind line, {@code rgB/rc2=<its key>} in peer-trust.json.rebind
     * -- the line the refusal prints. It then joins, once; the former controller is no longer recorded, and is refused.
     */
    @Test
    void theOperatorReplacesARegionsControllerWithARebindLine() throws Exception {
        Path gf = Files.createTempDirectory("onectl-g").resolve("peer-trust.json");
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "peer_trust_file", gf.toString());
        PeerTrustRegistry atGlobal = reg(global);
        ControllerEngine rc = controller("rgB", "rc", "t1"), rc2 = controller("rgB", "rc2", "t1"), other = controller("rgB", "rc2", "t1");
        assertNotNull(join(rc, global));
        X509Certificate formerRoot = last(rc);
        assertTrue(java.util.Arrays.asList(((BoundKeyTrustManager) global.getCertificateManager().currentPkixTrustManager()).trustedIssuers())
                .contains(formerRoot), "(precondition: the global holds the former controller's certificates)");
        assertNull(join(rc2, global), "the new controller joined without the operator");
        // a line for ANOTHER key than the one that presents: not this controller
        Files.writeString(Path.of(gf + ".rebind"), "rgB/rc2=" + key(other) + "\n");
        assertNull(join(rc2, global), "a rebind line for another key admitted it");
        Files.writeString(Path.of(gf + ".rebind"), "rgB/rc2=" + key(rc2) + "\n");
        Files.setLastModifiedTime(Path.of(gf + ".rebind"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
        assertNotNull(join(rc2, global), "the operator's rebind line did not admit the new controller");
        assertEquals(key(rc2), atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "rc2")));
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc2")));
        assertTrue(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc2", null)));
        // the former controller: not recorded any more, no bridge peer, and refused as any second controller
        assertNull(atGlobal.pinnedKey(new PeerIdentity.Expected("rgB", "rc")), "the former controller is still bound");
        assertFalse(atGlobal.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertNull(atGlobal.verifiedIdentity(new X509Certificate[]{leaf(rc)}));
        // ... and what the global held for it is gone with the binding: its certificates leave the truststore
        assertFalse(java.util.Arrays.asList(((BoundKeyTrustManager) global.getCertificateManager().currentPkixTrustManager()).trustedIssuers())
                .contains(formerRoot), "the replaced controller's certificates are still trust anchors of the global");
        assertNull(join(rc, global), "the replaced controller joined again");
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
        // the line is consumed: it admits nobody else, also after the registry is read again from its files
        atGlobal.awaitRevocations();
        PeerTrustRegistry again = PeerTrustRegistry.open(gf, List.of(), List.of(), 30, m -> { });
        assertEquals(key(rc2), again.pinnedKey(new PeerIdentity.Expected("rgB", "rc2")));
        assertNull(again.pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        assertNotNull(again.pin("rgB", "rc2", leaf(other), PeerTrustRegistry.ROLE_CONTROLLER, false), "the consumed line rebound the name again");
    }

    /**
     * {@code controller_trust_anchors} ADMIT a controller; they never replace a recorded one. A controller whose key the
     * operator anchored at the global is still refused for a region that has another controller recorded there, whether the
     * recorded one is anchored or not, by key or by certificate: only the rebind line (or revoking the recorded one) replaces.
     */
    @Test
    void controllerTrustAnchorsAdmitAControllerAndNeverReplaceARecordedOne() throws Exception {
        ControllerEngine rc = controller("rgB", "rc", "t1"), rc2 = controller("rgB", "rc2", "t1"), rc3 = controller("rgB", "rc3", "t1");
        Path gf = Files.createTempDirectory("onectl-g").resolve("peer-trust.json");
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "controller_trust_anchors", key(rc2) + "," + key(rc3),
                "peer_trust_file", gf.toString());
        assertNotNull(join(rc, global), "(first come: the squatter, or the controller that is to be replaced)");
        String[] told = new String[1];
        assertNull(join(rc2, global, DiscoveryType.GLOBAL, told), "an anchored controller replaced the recorded one");
        assertTrue(told[0] != null && told[0].contains("'rgB/rc2=" + key(rc2) + "'"), told[0]);
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "rc2")));
        assertEquals(key(rc), reg(global).pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        // the operator's rebind line replaces it; then a second anchored controller does not replace the first
        Files.writeString(Path.of(gf + ".rebind"), "rgB/rc2=" + key(rc2) + "\n");
        assertNotNull(join(rc2, global), "the operator's rebind line did not admit the replacement");
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertNull(join(rc, global), "the replaced controller joined again");
        assertNull(join(rc3, global), "one anchored controller replaced another");
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc2")));
        assertEquals("OK", global.getCertificateManager().identityHealth()[0]);
    }

    // ------------------------------------------------------------------ a registry that already holds two

    /**
     * A registry written before the rule holds two controllers under one region. This build does not choose: both keep
     * working as recorded, no further one is taken, and it is said at every start and by the identity health check until
     * the operator revokes the one that is not the region's controller.
     */
    @Test
    void aRegistryThatAlreadyHoldsTwoControllersKeepsBothRefusesAThirdAndSaysSo() throws Exception {
        Path dir = Files.createTempDirectory("onectl-g");
        Path gf = dir.resolve("security").resolve("peer-trust.json");
        ControllerEngine global = controller("rgG", "gc", "t1", dir, "is_global", "true");
        PeerTrustRegistry atGlobal = reg(global);
        ControllerEngine rc = controller("rgB", "rc", "t1"), zz = controller("rgB", "zz", "t1");
        // as the code before the rule recorded two joins under one region
        for (ControllerEngine c : new ControllerEngine[]{rc, zz}) {
            assertNull(atGlobal.pin("rgB", c.cstate.getAgent(), leaf(c), PeerTrustRegistry.ROLE_CONTROLLER, false));
            atGlobal.allowBridgePeer("rgB", c.cstate.getAgent());
        }
        atGlobal.awaitRevocations();
        // this build starts on that registry: said at the start
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry.open(gf, List.of(), List.of(), 30, log::add);
        assertTrue(log.stream().anyMatch(l -> l.contains("2 CONTROLLERS are recorded here for region rgB") && l.contains("rgB/rc") && l.contains("rgB/zz")
                && l.contains("Revoke the one that is not")), "the start did not say that two controllers are recorded: " + log);
        ControllerEngine started = controller("rgG", "gc", "t1", dir, "is_global", "true");
        atGlobal = reg(started);
        String[] h = started.getCertificateManager().identityHealth();
        assertEquals("CRITICAL", h[0], h[1]);
        assertTrue(h[1].contains("2 CONTROLLERS are recorded here for region rgB"), h[1]);
        // both keep working as recorded
        for (ControllerEngine c : new ControllerEngine[]{rc, zz}) {
            assertNotNull(atGlobal.verifiedIdentity(new X509Certificate[]{leaf(c)}), "rgB/" + c.cstate.getAgent() + " no longer verifies");
            assertTrue(started.speaksForRegion(Caller.controller("t1", "rgB", c.cstate.getAgent())));
            assertNotNull(join(c, started), "rgB/" + c.cstate.getAgent() + ", recorded, was refused");
        }
        // no further one is taken
        assertNull(join(controller("rgB", "yy", "t1"), started), "a third controller was taken for the region");
        // the operator revokes the one that is not the region's controller: one is left, and nothing more is said
        assertNull(atGlobal.revoke("rgB/zz", "operator"));
        assertEquals("OK", started.getCertificateManager().identityHealth()[0], started.getCertificateManager().identityHealth()[1]);
        assertFalse(started.speaksForRegion(Caller.controller("t1", "rgB", "zz")));
        assertNotNull(join(rc, started));
        assertNull(join(zz, started));
        atGlobal.awaitRevocations();
        log.clear();
        PeerTrustRegistry.open(gf, List.of(), List.of(), 30, log::add);
        assertTrue(log.stream().noneMatch(l -> l.contains("CONTROLLERS are recorded")), log.toString());
    }

    // ------------------------------------------------------------------ the regional CA and the pre-provisioned PKI: as before

    /** With the regional CA a region has one CA where it is recorded: a second controller under another CA was and is refused. */
    @Test
    void underTheRegionalCaASecondControllerWasAndIsRefused() throws Exception {
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "security_regional_ca", "true");
        global.getCertificateManager().ensureRegionCA();
        ControllerEngine rc = controller("rgB", "rc", "t1", "security_regional_ca", "true"), zz = controller("rgB", "zz", "t1", "security_regional_ca", "true");
        rc.getCertificateManager().ensureRegionCA();
        zz.getCertificateManager().ensureRegionCA();
        assertNotNull(join(rc, global), "the region's controller does not federate with the global");
        assertNull(join(zz, global), "a second controller of the region, with another CA, federated");
        assertNotNull(join(rc, global), "the region's controller does not join again");
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
    }

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String tenant, String region, String agent, String role) throws Exception {
        Path dir = Files.createTempDirectory("onectl-pki-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", tenant);
        cfg.put("broker_security_enabled", "true");
        cfg.put(role, "true");
        cfg.put("security_profile", "production");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = true;
        st.global = "is_global".equals(role);
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned());
        return e;
    }

    /**
     * Pre-provisioned (the production profile): a controller is whoever the operator's PKI issued a controller's
     * certificate, and a region is one tenant's. As before: another tenant's controller that names the region is refused,
     * a certificate from outside the PKI is refused, and the region's controller joins again after it was re-issued (a new
     * key under its name) with no rebind. The rule changes nothing here: what the operator's PKI issued for the region's
     * own tenant is the operator's word (so is a second controller name it issued there).
     */
    @Test
    void preProvisionedTheOperatorsPkiDecidesAsBefore() throws Exception {
        TlsTestSupport.provider();
        PkiTestSupport.Ca root = PkiTestSupport.root("One Controller Root"), otherRoot = PkiTestSupport.root("Not The Fabric's Root");
        Path pki = Files.createTempDirectory("onectl-pki");
        Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        long from = System.currentTimeMillis() - 60_000;
        ControllerEngine global = provisioned(PkiTestSupport.write(PkiTestSupport.global(root, "t1", "rgG", "gc", from, 30 * DAY), pki.resolve("g")),
                anchors, "t1", "rgG", "gc", "is_global");
        ControllerEngine rc = provisioned(PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgB", "rc", true, from, 30 * DAY), pki.resolve("rc")),
                anchors, "t1", "rgB", "rc", "is_region");
        assertNotNull(join(rc, global), "the region's controller does not join over the PKI");
        // another tenant's controller naming tenant t1's region; and a controller certificate from outside the operator's PKI
        ControllerEngine t2 = provisioned(PkiTestSupport.write(PkiTestSupport.node(root, "t2", "rgB", "zz", true, from, 30 * DAY), pki.resolve("t2")),
                anchors, "t2", "rgB", "zz", "is_region");
        assertNull(join(t2, global), "another tenant's controller joined as a controller of tenant t1's region");
        Path otherAnchors = PkiTestSupport.pem(pki.resolve("other-anchors.pem"), otherRoot.cert, root.cert);
        ControllerEngine stranger = provisioned(PkiTestSupport.write(PkiTestSupport.node(otherRoot, "t1", "rgB", "yy", true, from, 30 * DAY),
                pki.resolve("x")), otherAnchors, "t1", "rgB", "yy", "is_region");
        assertNull(join(stranger, global), "a controller certificate from outside the operator's PKI joined");
        // re-issued by the operator: the same name, a new key and certificate -- no rebind, as before
        ControllerEngine reIssued = provisioned(PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgB", "rc", true, from, 30 * DAY), pki.resolve("rc-2")),
                anchors, "t1", "rgB", "rc", "is_region");
        assertNotEquals(key(rc), key(reIssued));
        assertNotNull(join(reIssued, global), "the region's controller, re-issued by the operator's PKI, was locked out");
        // a second controller NAME the operator's PKI issued for the region's own tenant: the operator's word, as before
        ControllerEngine issued = provisioned(PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgB", "rc2", true, from, 30 * DAY), pki.resolve("rc2")),
                anchors, "t1", "rgB", "rc2", "is_region");
        assertNotNull(join(issued, global), "what the operator's PKI issued for the region's tenant was refused");
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
    }
}
