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
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * F-17: the inbox of a node's former address (region_alias) is given where the fabric recorded the move -- the key the node
 * presents is the one bound to its former name -- not on the node's own word. {@link AliasInboxSameNameProbeTest} shows the
 * hole closed; this shows that a node that really moved keeps its former address, and how the record is asked for and
 * answered: on the real secured broker, and between a real global and region (real discovery, signed pongs).
 */
class RecordedMovesTest {

    private static final Gson GSON = new Gson();
    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";

    // ------------------------------------------------------------------ the broker

    private static void send(Session from, String queue, int count, String tag) throws Exception {
        MessageProducer p = from.createProducer(from.createQueue(queue));
        for (int i = 0; i < count; i++) p.send(from.createTextMessage(tag + i));
        p.close();
    }

    private static Set<String> drain(MessageConsumer c, int expected) throws Exception {
        Set<String> got = new TreeSet<>();
        for (Message m = c.receive(5000); m != null; m = c.receive(got.size() >= expected ? 300 : 3000)) got.add(((TextMessage) m).getText());
        return got;
    }

    /**
     * On the broker of region rgB: m0 moved here from the global's region rgG with its key (the record holds that key for
     * rgG/m0); a1 holds the agent name of rgG/a1 with another key. m0 is given the inbox of rgG_m0 and what waited there; a1
     * is refused rgG_a1 and is given nothing. The broker asks with the key each connection PRESENTED; while the answer is not
     * known it refuses, and gives the inbox when asked again once it is.
     */
    @Test
    void aMovedNodeReadsTheInboxOfItsFormerAddressAndASameNamedNodeDoesNot() throws Exception {
        TlsTestSupport.provider();
        RegionCA caB = RegionCA.generate("t1", "rgB", 2048, 1);
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(caB, "t1", "rgB", "rc");
        TlsTestSupport.Id moved = TlsTestSupport.issued(caB, "t1", "rgB", "m0");
        TlsTestSupport.Id sameName = TlsTestSupport.issued(caB, "t1", "rgB", "a1");
        TlsTestSupport.Id peer = TlsTestSupport.issued(caB, "t1", "rgB", "b2");
        String movedKey = PeerIdentity.spkiFingerprint(moved.chain[0]), sameNameKey = PeerIdentity.spkiFingerprint(sameName.chain[0]);

        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("moves").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        List<String> asked = new CopyOnWriteArrayList<>();
        AtomicReference<Boolean> movedAnswer = new AtomicReference<>(null);        // not known yet, at first
        authz.setMoveRecord((t, r, a, fp) -> {
            asked.add(t + "|" + r + "|" + a + "|" + fp);
            if ("t1".equals(t) && "rgG".equals(r) && "m0".equals(a) && movedKey.equals(fp)) return movedAnswer.get();
            return Boolean.FALSE;
        });
        BrokerService b = TlsTestSupport.broker("rgB_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, caB.caCert());
        List<Connection> conns = new ArrayList<>();
        try {
            java.util.function.Function<TlsTestSupport.Id, Session> client = id -> {
                try {
                    ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
                    f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(caB.caCert()), new SecureRandom());
                    ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
                    conns.add(c);
                    c.start();
                    return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };
            Session writer = client.apply(peer);
            send(writer, "T.t1.rgG_m0", 3, "for-m0-");            // its peers still address it at its former address
            send(writer, "T.t1.rgG_a1", 3, "for-a1-");

            Session m0 = client.apply(moved);
            assertThrows(JMSException.class, () -> m0.createConsumer(m0.createQueue("T.t1.rgG_m0")),
                    "given while the record was not known here yet");
            movedAnswer.set(Boolean.TRUE);                                           // the answer came: asked again, given
            MessageConsumer former = m0.createConsumer(m0.createQueue("T.t1.rgG_m0"));
            assertEquals(Set.of("for-m0-0", "for-m0-1", "for-m0-2"), drain(former, 3), "the moved node's former address");
            m0.createConsumer(m0.createQueue("T.t1.rgB_m0")).close();               // its own inbox: never asked about
            assertTrue(asked.stream().allMatch(q -> q.equals("t1|rgG|m0|" + movedKey)), "asked with another key or name: " + asked);

            Session a1 = client.apply(sameName);
            assertThrows(JMSException.class, () -> a1.createConsumer(a1.createQueue("T.t1.rgG_a1")),
                    "a node of the same agent name read the inbox of rgG/a1");
            assertThrows(JMSException.class, () -> a1.createBrowser(a1.createQueue("T.t1.rgG_a1")).getEnumeration());
            assertThrows(JMSException.class, () -> a1.createConsumer(a1.createQueue("T.t1.rgG_m0")), "another agent's name");
            assertTrue(asked.contains("t1|rgG|a1|" + sameNameKey), "the broker did not ask with the key a1 presented");
            // what waited for rgG/a1 is still there: the refused node took nothing (a consumer the record says yes to takes it)
            authz.setMoveRecord((t, r, a, fp) -> Boolean.TRUE);
            Session given = client.apply(sameName);
            assertEquals(3, drain(given.createConsumer(given.createQueue("T.t1.rgG_a1")), 3).size(),
                    "the messages addressed to rgG/a1 were not left where they were");
        } finally {
            for (Connection c : conns) try { c.close(); } catch (Exception ignore) { }
            b.stop();
        }
    }

    // ------------------------------------------------------------------ the record, between a global and a region

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

    /** The real discovery exchange; the client's result node, or null when either side refused. */
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

    /** One ping of the region {@code child} to its global and the pong back, as the watcher and the executor carry them. */
    private static MsgEvent ping(ControllerEngine child, ControllerEngine global) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                global.cstate.getRegion(), global.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        RevocationSync.onChildPing(child, p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);    // the global's broker's stamp
        RevocationSync.onPingAtParent(global, p, true);
        p.setReturn();
        MsgEvent pong = wire(p);
        RevocationSync.onParentPong(child, pong, global.cstate.getRegion(), global.cstate.getAgent());
        return pong;
    }

    /**
     * The real fabric's part: m0 joined the global in its region (rgG/m0 bound to its key there), moved to region rgB with the
     * same identity (its key kept); a1 of rgB holds the name of rgG/a1, which the global bound to another node's key. The
     * region does not know; it asks on its ping, and takes the global's signed answer from the pong: yes for m0's key, no for
     * a1's, no for a former region the global never recorded. The global answers itself. An answer made for another region's
     * controller is not taken.
     */
    @Test
    void theGlobalAnswersByTheKeyItBoundToTheFormerName() throws Exception {
        ControllerEngine global = node("rgG", "gc", null, "is_global", "true");
        ControllerEngine region = node("rgB", "rc", null), other = node("rgC", "rc3", null);
        assertNotNull(join(region, global, DiscoveryType.GLOBAL));
        assertNotNull(join(other, global, DiscoveryType.GLOBAL));
        Path m0dir = Files.createTempDirectory("m0");
        ControllerEngine before = agent("rgG", "m0", m0dir);
        assertNotNull(join(before, global, DiscoveryType.AGENT), "m0 joins the global in the global's region");
        ControllerEngine victim = agent("rgG", "a1", null);
        assertNotNull(join(victim, global, DiscoveryType.AGENT));
        ControllerEngine after = agent("rgB", "m0", m0dir);                       // moved: started in rgB on its own directory
        assertEquals(key(before), key(after), "the moved node kept its key");
        ControllerEngine sameName = agent("rgB", "a1", null);
        assertNotNull(join(after, region, DiscoveryType.AGENT));
        assertNotNull(join(sameName, region, DiscoveryType.AGENT), "rgB/a1 is a name of its own in rgB (the hole's premise)");

        RecordedMoves at = region.recordedMoves();
        assertNull(at.moved("t1", "rgG", "m0", key(after)), "known before it was asked");
        assertNull(at.moved("t1", "rgG", "a1", key(sameName)));
        assertNull(at.moved("t1", "rgX", "m0", "0".repeat(64)), "(another key: its own question)");
        assertTrue(at.takeAskNow(System.currentTimeMillis()), "a question does not ping at once");

        // an answer made for another region's controller is nothing to this one
        MsgEvent otherPong = ping(other, global);
        assertNull(otherPong.getParam(RecordedMoves.ANSWER), "the global answered a question nobody asked");
        MsgEvent pong = ping(region, global);
        assertNotNull(pong.getParam(RecordedMoves.ANSWER), "the global did not answer");
        RevocationSync.onParentPong(other, pong, "rgG", "gc");
        assertNull(other.recordedMoves().moved("t1", "rgG", "m0", key(after)), "a region took an answer addressed to another");

        assertEquals(Boolean.TRUE, at.moved("t1", "rgG", "m0", key(after)), "the move of m0, with its key, is recorded");
        assertEquals(Boolean.FALSE, at.moved("t1", "rgG", "a1", key(sameName)), "rgB/a1's key is not the one bound to rgG/a1");
        assertEquals(Boolean.FALSE, at.moved("t1", "rgX", "m0", "0".repeat(64)), "a former name the global never recorded");
        assertEquals(0, at.openCount(), "questions left open after their answer");

        // the global answers itself
        assertEquals(Boolean.TRUE, global.recordedMoves().moved("t1", "rgG", "m0", key(after)));
        assertEquals(Boolean.FALSE, global.recordedMoves().moved("t1", "rgG", "a1", key(sameName)));
        assertEquals(Boolean.TRUE, global.recordedMoves().moved("t1", "rgG", "a1", key(victim)), "(the node itself)");
        // and a node without a global to ask is given no former address
        ControllerEngine alone = node("rgD", "rc4", null);
        ((TlsTestSupport.State) alone.cstate).globalRegion = null;
        assertEquals(Boolean.FALSE, alone.recordedMoves().moved("t1", "rgG", "m0", key(after)));
    }
}
