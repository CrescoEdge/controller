package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.messaging.RPC;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * GaiaKeep gfs#26 on a secured fabric (review Phase 3 F8): the former region a moved node answers at travels with its
 * stamp, set by the broker the node is attached to from what its region recorded of the node's own registration -- never
 * from what the node (or anyone) declares.
 */
class RegionAliasStampTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id rc, a1, a2, rx;
    private final List<Connection> connections = new ArrayList<>();
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgNew", 2048, 1);
        rc = TlsTestSupport.issued(ca, "t1", "rgNew", "rc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgNew", "a1");       // moved here from rgOld (region_alias=rgOld)
        a2 = TlsTestSupport.issued(ca, "t1", "rgNew", "a2");       // never moved
        rx = TlsTestSupport.issued(ca, "t1", "rgNew", "rx");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgNew", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("alias").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        authz.setRegionAliasLookup((region, agent) -> "rgNew".equals(region) && "a1".equals(agent) ? "rgOld" : null);
        b = TlsTestSupport.broker("rgNew_rc", rc, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        b.stop();
    }

    private Session client(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private Message sent(TlsTestSupport.Id from, String caller, String roles, String declaredAlias) throws Exception {
        String q = "t1.alias." + System.nanoTime();
        Session r = client(rx);
        MessageConsumer c = r.createConsumer(r.createQueue(q));
        Session s = client(from);
        TextMessage m = s.createTextMessage("reply");
        m.setStringProperty("cresco_caller", caller);
        m.setStringProperty("cresco_caller_tenant", "t1");
        m.setStringProperty("cresco_caller_roles", roles);
        if (declaredAlias != null) m.setStringProperty(CallerStamps.P_ALIAS, declaredAlias);
        s.createProducer(s.createQueue(q)).send(m);
        Message got = c.receive(5000);
        assertNotNull(got);
        return got;
    }

    @Test
    void theBrokerStampsTheRecordedAliasOfTheMovedNode() throws Exception {
        Message got = sent(a1, "plugin:t1/rgNew/a1/plugin-gfs", "plugin", "rgEvil");
        assertEquals("rgOld", got.getStringProperty(CallerStamps.P_ALIAS), "the recorded alias replaces whatever was declared");
        // the receiver: a reply to a call made at rgOld/a1/plugin-gfs is the callee's
        MsgEvent reply = new MsgEvent(MsgEvent.Type.EXEC, "rgOld", "a1", "plugin-gfs", "rgC", "caller", "p", false, false);
        reply.setParam(Caller.PARAM_ALIAS, "rgForgedInBody");
        CallerStamps.fromJms(reply, got, true);
        assertEquals("rgOld", Caller.regionAliasOf(reply));
        assertNull(RPC.calleeRefusal("rgOld", "a1", "plugin-gfs", reply));
    }

    @Test
    void aNodeWithoutARecordedAliasCarriesNone() throws Exception {
        Message got = sent(a2, "controller:t1/rgNew/a2", "infra", "rgOld");
        assertEquals("", got.getStringProperty(CallerStamps.P_ALIAS), "a declared alias is never kept");
        MsgEvent reply = new MsgEvent(MsgEvent.Type.EXEC, "rgOld", "a2", null, "rgC", "caller", "p", false, false);
        CallerStamps.fromJms(reply, got, true);
        assertNull(Caller.regionAliasOf(reply));
        assertNotNull(RPC.calleeRefusal("rgOld", "a2", "plugin-x", reply));
    }

    @Test
    void anApiStampOfTheMovedNodesUsersCarriesNoAlias() throws Exception {
        Message got = sent(a1, "api:t1/u", "user", null);
        assertEquals("", got.getStringProperty(CallerStamps.P_ALIAS));
    }

    @Test
    void anAliasInTheBodyIsNeverRead() throws Exception {
        ActiveMQTextMessage jms = new ActiveMQTextMessage();
        CallerStamps.toJms(Caller.plugin("t1", "rgNew", "a1", "p"), jms);
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgOld", "a1", "p", "rgC", "c", "p", false, false);
        m.setParam(Caller.PARAM_ALIAS, "rgOld");
        CallerStamps.fromJms(m, jms, true);
        assertNull(m.getParam(Caller.PARAM_ALIAS));
    }
}
