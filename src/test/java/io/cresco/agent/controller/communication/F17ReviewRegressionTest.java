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
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The review probes of F-17 (of 54e80ea: the inbox of a former address is given only to the key bound to the former name),
 * kept as regression tests. Each FAILED on 54e80ea (the finding it confirmed); r4 and r7 are restated for the fix's design
 * (the broker gives the consumer while the record is not known and withholds its messages; the global answers its own
 * tenant's names only, from the controller that recorded them), r1 accepts a consumer closed by the broker as one that
 * takes nothing.
 */
class F17ReviewRegressionTest {

    private static final Gson GSON = new Gson();
    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";

    // ------------------------------------------------------------------ helpers (as RecordedMovesTest)

    private static void send(Session from, String queue, int count, String tag) throws Exception {
        MessageProducer p = from.createProducer(from.createQueue(queue));
        for (int i = 0; i < count; i++) p.send(from.createTextMessage(tag + i));
        p.close();
    }

    private static Set<String> drain(MessageConsumer c, long firstMs) throws Exception {
        Set<String> got = new TreeSet<>();
        try {
            for (Message m = c.receive(firstMs); m != null; m = c.receive(500)) got.add(((TextMessage) m).getText());
        } catch (jakarta.jms.IllegalStateException closedByTheBroker) {
            // the broker closed the consumer (ConsumerControl): it takes nothing more
        }
        return got;
    }

    private static Session tls(BrokerService b, TlsTestSupport.Id id, List<Connection> conns, java.security.cert.X509Certificate anchor) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(anchor), new SecureRandom());
        ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
        conns.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private static ControllerEngine node(String region, String agent, Path dir, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("broker_security_enabled", "true");
        m.put("msg_signing_sessions", "false");
        m.put("tenant_id", "t1");
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

    private static ControllerEngine agent(String region, String agent, Path dir) throws Exception {
        return node(region, agent, dir, "is_controller", "false");
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(e.getCertificateManager().getPublicCertificate()[0]); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        if (hello == null) return null;
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        if (proof == null) return null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
        return c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005);
    }

    private static MsgEvent ping(ControllerEngine child, ControllerEngine global) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                global.cstate.getRegion(), global.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(child, p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        RevocationSync.onPingAtParent(global, p, true);
        p.setReturn();
        MsgEvent pong = wire(p);
        RevocationSync.onParentPong(child, pong, global.cstate.getRegion(), global.cstate.getAgent());
        return pong;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, long[]> known(RecordedMoves rm) throws Exception {
        java.lang.reflect.Field f = RecordedMoves.class.getDeclaredField("known");
        f.setAccessible(true);
        return (Map<String, long[]>) f.get(rm);
    }

    // ------------------------------------------------------------------ R1: a given alias consumer is never judged again

    /**
     * R1. The broker judges the former address once, when the consumer is added. m0 is given T.t1.rgG_m0 (the record says
     * yes); then the record says NO (the operator rebound rgG/m0 to another node's key at the global, or the region's
     * cached yes was finally re-asked). The broker's live sweep runs. m0's consumer is not closed and keeps taking every
     * message addressed to rgG/m0 -- now another node's inbox -- for as long as its connection lives.
     */
    @Test
    void r1_aGivenFormerInboxIsNotTakenBackWhenTheRecordSaysNo() throws Exception {
        TlsTestSupport.provider();
        RegionCA caB = RegionCA.generate("t1", "rgB", 2048, 1);
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(caB, "t1", "rgB", "rc");
        TlsTestSupport.Id moved = TlsTestSupport.issued(caB, "t1", "rgB", "m0");
        TlsTestSupport.Id peer = TlsTestSupport.issued(caB, "t1", "rgB", "b2");
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("rv-r1").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        AtomicReference<Boolean> record = new AtomicReference<>(Boolean.TRUE);
        authz.setMoveRecord((t, r, a, fp) -> record.get());
        BrokerService b = TlsTestSupport.broker("rgB_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, caB.caCert());
        List<Connection> conns = new ArrayList<>();
        try {
            Session m0 = tls(b, moved, conns, caB.caCert());
            MessageConsumer former = m0.createConsumer(m0.createQueue("T.t1.rgG_m0"));     // given: the record said yes
            record.set(Boolean.FALSE);                                                       // ... now it says no
            authz.closeInvalidConnections();                                                 // the broker's minute sweep
            Session writer = tls(b, peer, conns, caB.caCert());
            send(writer, "T.t1.rgG_m0", 3, "for-the-new-rgG-m0-");
            Set<String> taken = drain(former, 3000);
            System.out.println("R1: after the record said no, the old holder's consumer took " + taken);
            assertTrue(taken.isEmpty(), "the former inbox stayed readable after the record said no: took " + taken);
        } finally {
            for (Connection c : conns) try { c.close(); } catch (Exception ignore) { }
            b.stop();
        }
    }

    // ------------------------------------------------------------------ R2: a region's yes outlives a rebind at the global

    /**
     * R2. m0 moved rgG -> rgB with its key; the region asked, the global said yes. Then the operator rebinds rgG/m0 at the
     * global to a NEW node (another key). The global now says no. The region keeps its yes: nothing re-asks on the pings
     * (no consumer was added), and once the hour is past, a yes is kept while the global does not answer -- for ever.
     */
    @Test
    void r2_aRebindAtTheGlobalIsNotSeenByTheRegion() throws Exception {
        ControllerEngine global = node("rgG", "gc", null, "is_global", "true");
        ControllerEngine region = node("rgB", "rc", null);
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        Path m0dir = Files.createTempDirectory("rv-m0");
        ControllerEngine before = agent("rgG", "m0", m0dir);
        assertNotNull(join(before, global, DiscoveryType.AGENT));
        ControllerEngine after = agent("rgB", "m0", m0dir);
        assertNotNull(join(after, region, DiscoveryType.AGENT));
        RecordedMoves at = region.recordedMoves();
        assertNull(at.moved("t1", "rgG", "m0", key(after)));
        ping(region, global);
        assertEquals(Boolean.TRUE, at.moved("t1", "rgG", "m0", key(after)), "precondition: the move is recorded");

        // the operator gives the name rgG/m0 to a new node of the global's region
        ControllerEngine newHolder = agent("rgG", "m0", Files.createTempDirectory("rv-m0-new"));
        global.getCertificateManager().getPeerTrust().withRebind(List.of("rgG/m0=" + key(newHolder)));
        assertNotNull(join(newHolder, global, DiscoveryType.AGENT), "the rebind was not taken");
        assertEquals(Boolean.FALSE, global.recordedMoves().moved("t1", "rgG", "m0", key(after)), "precondition: the global now says no");

        List<String> seen = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MsgEvent pong = ping(region, global);
            seen.add("ping " + i + ": asked=" + (pong.getParam(RecordedMoves.ASK) != null) + " answered=" + (pong.getParam(RecordedMoves.ANSWER) != null));
        }
        Boolean afterPings = at.moved("t1", "rgG", "m0", key(after));
        // the hour passes; the global is away (no pong)
        for (long[] v : known(at).values()) v[1] = System.currentTimeMillis() - 1;
        Boolean pastTheHourGlobalAway = at.moved("t1", "rgG", "m0", key(after));
        System.out.println("R2: " + seen + "; region after the rebind: " + afterPings + "; past the hour, global away: " + pastTheHourGlobalAway);
        assertNotEquals(Boolean.TRUE, afterPings, "the region still gives the old key the former inbox after the global rebound the name");
        assertNotEquals(Boolean.TRUE, pastTheHourGlobalAway, "a yes past its hour is still a yes while the global is away");
    }

    // ------------------------------------------------------------------ R3: the production profile keeps the hole

    /**
     * R3. security_profile=production (pre-provisioned PKI): rgB's region CA, scoped to rgB, issues rgB/a1 -- a name of its
     * own region. That node reads T.t1.rgA_a1, the inbox of rgA/a1, exactly as F-17 described: the region CA's scope does not
     * contain it, and the move record is never asked.
     */
    @Test
    void r3_productionProfileARegionCaOfRgBReadsTheInboxOfRgAA1() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca caB = PkiTestSupport.region(root, "t1", "rgB");
        long now = System.currentTimeMillis();
        PkiTestSupport.Node rc = PkiTestSupport.node(caB, "t1", "rgB", "rc", true, now - 60_000, PkiTestSupport.DAY);
        PkiTestSupport.Node a1 = PkiTestSupport.node(caB, "t1", "rgB", "a1", false, now - 60_000, PkiTestSupport.DAY);
        PkiTestSupport.Node b2 = PkiTestSupport.node(caB, "t1", "rgB", "b2", false, now - 60_000, PkiTestSupport.DAY);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        peers.setPki(List.of(root.cert), null);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("rv-r3").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        List<String> asked = new CopyOnWriteArrayList<>();
        authz.setMoveRecord((t, r, a, fp) -> { asked.add(r + "/" + a); return Boolean.FALSE; });   // no move recorded anywhere
        BrokerService b = TlsTestSupport.broker("rgB_rc", new TlsTestSupport.Id(rc.kp, rc.chain), true, new BrokerPlugin[]{authz}, authz, root.cert);
        List<Connection> conns = new ArrayList<>();
        try {
            Session writer = tls(b, new TlsTestSupport.Id(b2.kp, b2.chain), conns, root.cert);
            send(writer, "T.t1.rgA_a1", 5, "for-rgA-a1-");
            Session thief = tls(b, new TlsTestSupport.Id(a1.kp, a1.chain), conns, root.cert);
            Set<String> taken = new TreeSet<>();
            try {
                taken = drain(thief.createConsumer(thief.createQueue("T.t1.rgA_a1")), 3000);
            } catch (JMSException refused) {
                // what the fix should do here too
            }
            System.out.println("R3: production profile, rgB/a1 took " + taken + " from T.t1.rgA_a1; record asked: " + asked);
            assertTrue(taken.isEmpty(), "production profile: rgB/a1 (issued by rgB's region CA) took " + taken.size() + " message(s) of rgA/a1");
        } finally {
            for (Connection c : conns) try { c.close(); } catch (Exception ignore) { }
            b.stop();
        }
    }

    /**
     * R3, what production still allows: a moved node's certificate that STATES its former address (SAN cresco:former:), issued
     * by a CA whose scope covers the former region -- the root -- reads that inbox; the same statement issued by rgB's region
     * CA (whose scope is rgB) does not, nor does a root-issued certificate that states another former address.
     */
    @Test
    void r3b_productionProfileAMovedNodeReadsItsFormerInboxOnlyWithTheOperatorsStatement() throws Exception {
        PkiTestSupport.Ca root = PkiTestSupport.root("Cresco Fabric Root");
        PkiTestSupport.Ca caB = PkiTestSupport.region(root, "t1", "rgB");
        long now = System.currentTimeMillis();
        PkiTestSupport.Node rc = PkiTestSupport.node(caB, "t1", "rgB", "rc", true, now - 60_000, PkiTestSupport.DAY);
        PkiTestSupport.Node b2 = PkiTestSupport.node(caB, "t1", "rgB", "b2", false, now - 60_000, PkiTestSupport.DAY);
        PkiTestSupport.Node byRoot = PkiTestSupport.node(root, TlsTestSupport.rsa(), "t1", "rgB", "m0", false, false, now - 60_000,
                PkiTestSupport.DAY, "cresco:former:t1/rgA/m0");
        PkiTestSupport.Node byRegionCa = PkiTestSupport.node(caB, TlsTestSupport.rsa(), "t1", "rgB", "m1", false, false, now - 60_000,
                PkiTestSupport.DAY, "cresco:former:t1/rgA/m1");
        PeerTrustRegistry peers = new PeerTrustRegistry();
        peers.setPki(List.of(root.cert), null);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("rv-r3b").toString(), cfg), peers);
        BrokerService b = TlsTestSupport.broker("rgB_rc", new TlsTestSupport.Id(rc.kp, rc.chain), true, new BrokerPlugin[]{authz}, authz, root.cert);
        List<Connection> conns = new ArrayList<>();
        try {
            Session writer = tls(b, new TlsTestSupport.Id(b2.kp, b2.chain), conns, root.cert);
            send(writer, "T.t1.rgA_m0", 2, "for-m0-");
            send(writer, "T.t1.rgA_m1", 2, "for-m1-");
            Session m0 = tls(b, new TlsTestSupport.Id(byRoot.kp, byRoot.chain), conns, root.cert);
            assertEquals(Set.of("for-m0-0", "for-m0-1"), drain(m0.createConsumer(m0.createQueue("T.t1.rgA_m0")), 3000),
                    "the root-issued statement of the move was not honoured");
            assertThrows(JMSException.class, () -> m0.createConsumer(m0.createQueue("T.t1.rgC_m0")), "a former address its certificate does not state");
            Session m1 = tls(b, new TlsTestSupport.Id(byRegionCa.kp, byRegionCa.chain), conns, root.cert);
            assertThrows(JMSException.class, () -> m1.createConsumer(m1.createQueue("T.t1.rgA_m1")),
                    "rgB's region CA stated a move from rgA, which its scope does not cover");
        } finally {
            for (Connection c : conns) try { c.close(); } catch (Exception ignore) { }
            b.stop();
        }
    }

    // ------------------------------------------------------------------ R4: a 1.3.380 agent on an upgraded region

    private static BrokerService securedAt(int port, TlsTestSupport.Id id, CrescoAuthorizationBroker authz, java.security.cert.X509Certificate anchor) throws Exception {
        org.apache.activemq.broker.SslBrokerService b = new org.apache.activemq.broker.SslBrokerService();
        b.setBrokerName("rgB_rc");
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setSslContext(new org.apache.activemq.broker.SslContext(id.keyManagers(), TlsTestSupport.trustManagers(anchor), new SecureRandom()));
        b.setPlugins(new BrokerPlugin[]{authz});
        b.setMessageAuthorizationPolicy(authz);
        b.addConnector("nio+ssl://127.0.0.1:" + port + "?needClientAuth=true").setName("tls");
        b.start();
        b.waitUntilStarted();
        return b;
    }

    /**
     * R4 (restated for the fix). The roll order is global, region controllers, then agents. A moved agent still on 1.3.380
     * holds its alias consumer on its inbox session and does nothing on a reconnect. Its region controller is upgraded: the
     * region restarts and does not know the record yet. The 54e80ea broker refused the consumer the failover transport re-added,
     * and nobody asked again. Now the broker gives it, withholds what arrives for the former address while the answer is not
     * known, and dispatches it when the answer is yes; when it is no, the consumer is closed and takes nothing.
     */
    @Test
    void r4_aMovedAgentOfTheEarlierBuildKeepsItsFormerInboxWhenItsRegionIsUpgraded() throws Exception {
        TlsTestSupport.provider();
        RegionCA caB = RegionCA.generate("t1", "rgB", 2048, 1);
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(caB, "t1", "rgB", "rc");
        TlsTestSupport.Id m0 = TlsTestSupport.issued(caB, "t1", "rgB", "m0");
        TlsTestSupport.Id peer = TlsTestSupport.issued(caB, "t1", "rgB", "b2");
        String key = PeerIdentity.spkiFingerprint(m0.chain[0]);
        String inbox = "T.t1.rgB_m0", former = "T.t1.rgG_m0";
        int port;
        try (ServerSocket ss = new ServerSocket(0)) { port = ss.getLocalPort(); }
        AtomicReference<Boolean> record = new AtomicReference<>(Boolean.TRUE);
        java.util.function.Supplier<CrescoAuthorizationBroker> authz = () -> {
            try {
                PeerTrustRegistry peers = new PeerTrustRegistry();
                assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
                Map<String, Object> cfg = new HashMap<>();
                cfg.put("broker_security_enabled", "true");
                cfg.put("tenant_id", "t1");
                CrescoAuthorizationBroker a = new CrescoAuthorizationBroker(TestAgentService.plugin(Files.createTempDirectory("rv-r4").toString(), cfg), peers);
                a.setMoveRecord((t, r, ag, fp) -> "t1".equals(t) && "rgG".equals(r) && "m0".equals(ag) && key.equals(fp) ? record.get() : Boolean.FALSE);
                return a;
            } catch (Exception e) { throw new IllegalStateException(e); }
        };
        BrokerService b = securedAt(port, brokerId, authz.get(), caB.caCert());
        List<String> got = new CopyOnWriteArrayList<>();
        List<Connection> conns = new ArrayList<>();
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("failover:(nio+ssl://127.0.0.1:" + port
                + "?verifyHostName=false)?maxReconnectAttempts=100&initialReconnectDelay=100&useExponentialBackOff=false");
        f.setKeyAndTrustManagers(m0.keyManagers(), TlsTestSupport.trustManagers(caB.caCert()), new SecureRandom());
        Connection agent = f.createConnection();
        try {
            agent.start();
            Session sess = agent.createSession(false, Session.AUTO_ACKNOWLEDGE);         // 1.3.380: one session, both consumers
            sess.createConsumer(sess.createQueue(inbox)).setMessageListener(m -> got.add("inbox"));
            sess.createConsumer(sess.createQueue(former)).setMessageListener(m -> got.add("former"));
            // the region restarts on the new build; at first it does not know the record
            b.stop();
            b.waitUntilStopped();
            record.set(null);
            CrescoAuthorizationBroker upgraded = authz.get();
            b = securedAt(port, brokerId, upgraded, caB.caCert());
            long end = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < end && (consumers(b, inbox) == 0 || consumers(b, former) == 0)) Thread.sleep(50);
            assertEquals(1, consumers(b, inbox), "precondition: the agent reconnected");
            assertEquals(1, consumers(b, former), "the re-added consumer of the former address was refused while the record was not known");
            send(tls(b, peer, conns, caB.caCert()), former, 1, "to-the-former-address-");
            Thread.sleep(1_500);
            assertFalse(got.contains("former"), "given before the record was known");
            record.set(Boolean.TRUE);                                        // the answer arrives: the region's listener says so
            upgraded.formerAddressAnswered("t1|rgG|m0|" + key);
            end = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < end && !got.contains("former")) Thread.sleep(50);
            System.out.println("R4: consumers on the former inbox after the region restart: " + consumers(b, former) + "; delivered: " + got);
            assertTrue(got.contains("former"), "the 1.3.380 agent did not get what waited at its former address once the record was known");

            // and a no: the consumer is closed by the broker, and what comes later is not its
            record.set(Boolean.FALSE);
            upgraded.formerAddressAnswered("t1|rgG|m0|" + key);
            end = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < end && consumers(b, former) > 0) Thread.sleep(50);
            assertEquals(0, consumers(b, former), "the consumer was not closed when the record said no");
            got.clear();
            send(tls(b, peer, conns, caB.caCert()), former, 1, "after-the-no-");
            Thread.sleep(1_000);
            assertFalse(got.contains("former"), "given after the record said no");
        } finally {
            for (Connection c : conns) try { c.close(); } catch (Exception ignore) { }
            try { agent.close(); } catch (Exception ignore) { }
            b.stop();
        }
    }

    private static int consumers(BrokerService b, String queue) throws Exception {
        Destination d = b.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getConsumers().size();
    }

    // ------------------------------------------------------------------ R5: the agent's alias retry parks its lock on a dead transport

    /**
     * R5. A refused former inbox is asked for again on a timer, holding the AgentConsumer's lock, with createSession on the
     * failover connection. When the parent broker is gone (restarting), that call parks until the transport reconnects or
     * gives up, and shutdown() / the inbox rebuild wait behind it (the W-GFS-6 pattern). A move the fabric cannot record
     * (between two regions, no global) is refused for ever, so the timer runs for ever and every outage meets it.
     */
    @Test
    void r5_theAliasRetryHoldsTheConsumerLockWhileTheTransportIsDown() throws Exception {
        String INBOX = "r1_a1", FORMER = "old_a1";
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        final int p = port;
        java.util.function.Supplier<BrokerService> start = () -> {
            try {
                BrokerService br = new BrokerService();
                br.setBrokerName("rv-r5");
                br.setPersistent(false);
                br.setUseJmx(false);
                br.setUseShutdownHook(false);
                br.setAdvisorySupport(false);
                br.setPlugins(new BrokerPlugin[]{next -> new BrokerFilter(next) {
                    @Override public Subscription addConsumer(ConnectionContext ctx, ConsumerInfo info) throws Exception {
                        if (FORMER.equals(info.getDestination().getPhysicalName())) throw new SecurityException("no move recorded");
                        return super.addConsumer(ctx, info);
                    }
                }});
                br.addConnector("tcp://127.0.0.1:" + p);
                br.start();
                br.waitUntilStarted();
                return br;
            } catch (Exception e) { throw new IllegalStateException(e); }
        };
        BrokerService b = start.get();
        long retry = AgentConsumer.ALIAS_RETRY_MS, retryMax = AgentConsumer.ALIAS_RETRY_MAX_MS;
        AgentConsumer.ALIAS_RETRY_MS = 200;
        AgentConsumer.ALIAS_RETRY_MAX_MS = 200;
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("agentconsumer_dedicated_connection", "true");
        cfg.put("broker_security_enabled", "false");
        cfg.put("region_alias", "old");
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> { }, "r1", "a1");
        ControllerEngine engine = new ControllerEngine(null, pb, null, null);
        // the agents' own transport settings (BrokerTransport): 5 attempts, here 2 s apart
        String failover = "failover:(tcp://127.0.0.1:" + p + ")?maxReconnectAttempts=5&initialReconnectDelay=2000&useExponentialBackOff=false";
        AgentConsumer consumer = new AgentConsumer(engine, INBOX, failover);
        Thread stopper = null;
        long took = -1;
        try {
            Thread.sleep(600);                       // refused, retrying every 200 ms
            b.stop();
            b.waitUntilStopped();                    // the parent broker goes away
            Thread.sleep(800);                       // a retry fires meanwhile
            long t0 = System.currentTimeMillis();
            stopper = new Thread(consumer::shutdown, "rv-r5-shutdown");
            stopper.start();
            stopper.join(3_000);
            took = stopper.isAlive() ? -1 : System.currentTimeMillis() - t0;
            System.out.println("R5: AgentConsumer.shutdown() with the parent broker down: " + (took < 0 ? "STILL BLOCKED after 3 s" : took + " ms"));
            for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
                String n = e.getKey().getName();
                if (!n.equals("agentconsumer-alias") && !n.equals("rv-r5-shutdown")) continue;
                StringBuilder sb = new StringBuilder("R5: thread " + n + " " + e.getKey().getState() + ":");
                for (StackTraceElement f : e.getValue()) {
                    String s = f.toString();
                    if (s.contains("AgentConsumer") || s.contains("FailoverTransport.oneway") || s.contains("ActiveMQConnection.createSession")) sb.append("\n    at ").append(s);
                }
                System.out.println(sb);
            }
            assertTrue(took >= 0, "AgentConsumer.shutdown() parked behind the alias retry on a dead transport");
        } finally {
            if (stopper != null && stopper.isAlive()) { b = start.get(); stopper.join(30_000); }
            AgentConsumer.ALIAS_RETRY_MS = retry;
            AgentConsumer.ALIAS_RETRY_MAX_MS = retryMax;
            try { engine.getActiveClient().shutdown(); } catch (Exception ignore) { }
            try { b.stop(); } catch (Exception ignore) { }
        }
    }

    // ------------------------------------------------------------------ R6: the open-question table can be filled

    /**
     * R6. At most 4,096 open questions, one per key. A node that holds many identities of its region (standard profile: a
     * name is its own claim, first come) fills the table -- each asks once for a former name of its own -- and a node that
     * really moved is then refused ("not known here yet") with its question never put on a ping, for as long as the
     * table stays full.
     */
    @Test
    void r6_manyIdentitiesFillTheOpenQuestionsAndAMovedNodeIsNeverAsked() throws Exception {
        ControllerEngine region = node("rgB", "rc", null);
        RecordedMoves at = region.recordedMoves();
        for (int i = 0; i < RecordedMoves.MAX; i++) {
            String fp = String.format("%064x", i + 1);
            assertNull(at.moved("t1", "rgG", "x" + i, fp));
        }
        String legit = "ab".repeat(32);
        Boolean v = at.moved("t1", "rgG", "m0", legit);
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc", null, "rgG", "gc", null, true, false);
        at.onPing(ping);
        String asked = ping.getParam(RecordedMoves.ASK);
        boolean legitAsked = asked != null && asked.contains(legit);
        System.out.println("R6: open=" + at.openCount() + ", moved node answered " + v + ", its question on the ping: " + legitAsked);
        assertTrue(legitAsked, "the moved node's question was never opened (table full of other identities' questions)");
    }

    // ------------------------------------------------------------------ R7: the global answers any tenant, any region

    /**
     * R7 (restated for the fix). The global answered every well-formed question a verified region controller put on its ping:
     * any tenant (not the region's), any region (not only the global's), and every yes refreshed the binding. Now: only the
     * asking controller's own tenant's names (a superuser tenant's controller excepted), another region's names only from that
     * region's controller (relayed), and a binding is refreshed only by a question marked as in use.
     */
    @Test
    void r7_theGlobalAnswersOnlyTheAskingControllersTenant() throws Exception {
        ControllerEngine global = node("rgG", "gc", null, "is_global", "true");
        ControllerEngine region = node("rgB", "rc", null);
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        PeerTrustRegistry reg = global.getCertificateManager().getPeerTrust();
        java.security.KeyPair kp = TlsTestSupport.rsa();
        assertTrue(reg.bind("t2", "rgG", "x9", kp.getPublic(), "discovery").ok);       // tenant t2's name, in the global's region
        String fp = PeerTrustRegistry.fingerprint(kp.getPublic().getEncoded());
        RecordedMoves at = region.recordedMoves();
        assertNull(at.moved("t2", "rgG", "x9", fp));                                   // a region of t1 asks about t2's name
        MsgEvent pong = ping(region, global);
        Boolean v = at.moved("t2", "rgG", "x9", fp);
        System.out.println("R7: a region of tenant t1 asked about t2's rgG/x9: answered " + pong.getParam(RecordedMoves.ANSWER) + ", region holds " + v);
        assertNotEquals(Boolean.TRUE, v, "the global confirmed another tenant's key binding");
        assertNull(pong.getParam(RecordedMoves.ANSWER), "the global answered a question about another tenant's name");
        // the same question from the global's own record, of its own tenant's controller: answered (the rule is the tenant)
        assertEquals(List.of("t2|rgG|x9|" + fp + "|1"), global.recordedMoves().answersFor("rgB/rc", "t2", false, "t2|rgG|x9|" + fp));
        assertTrue(global.recordedMoves().answersFor("rgB/rc", "t1", false, "t2|rgG|x9|" + fp).isEmpty());
        // a name of another region is not answered from the global's own registry: from that region's one controller, and
        // here no controller is recorded for rgX, so nothing records the name: no
        assertEquals(List.of("t1|rgX|x9|" + fp + "|0"), global.recordedMoves().answersFor("rgB/rc", "t1", false, "t1|rgX|x9|" + fp));
    }
}
