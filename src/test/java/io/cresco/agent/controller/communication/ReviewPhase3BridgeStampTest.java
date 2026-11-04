package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
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
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: stamps over a broker bridge. A failure is a
 * finding. Review-only: never pushed.
 *
 * <p>{@code CrescoAuthorizationBroker.stampCaller} keeps every stamp a verified bridge peer carries ("the first broker set
 * it"). Nothing scopes what a bridge may vouch for: a region controller of an ordinary tenant -- which on a direct
 * connection may declare only itself, its plugins and its own tenant's users ({@code CallerStamps.acceptDeclared}) --
 * may, over its bridge, declare a superuser tenant's admin or another tenant's controller, and every broker downstream
 * keeps it. One compromised (non-superuser) region thus acts as anyone, anywhere. At the very least a bridge peer of a
 * non-superuser tenant should not carry principals outside its own tenant.
 */
class ReviewPhase3BridgeStampTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id brokerId, peer, a2;
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        peer = TlsTestSupport.issued(ca, "t1", "rgA", "peer");      // a region controller of tenant t1 (not a superuser tenant)
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        peers.allowBridgePeer("rgA", "peer");                       // it completed discovery with this node: a bridge peer
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("rvp3-bridge").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService s : brokers) s.stop();
    }

    private ActiveMQConnection connection(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        ActiveMQConnection c = (ActiveMQConnection) f.createConnection();
        connections.add(c);
        return c;
    }

    private int rounds;

    /** The bridge peer sends one message declaring {@code caller}; what does a2 receive? */
    private Message overTheBridge(String caller, String tenant, String roles) throws Exception {
        String queue = "t1.bridged." + (++rounds);
        Connection rxc = connection(a2);
        rxc.start();
        Session rx = rxc.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c = rx.createConsumer(rx.createQueue(queue));

        ActiveMQConnection bridge = connection(peer);
        BrokerInfo info = new BrokerInfo();                         // what a network bridge sends before its ConnectionInfo
        info.setBrokerId(new BrokerId("rgA_peer-" + rounds));
        info.setBrokerName("rgA_peer");
        bridge.getTransportChannel().oneway(info);
        bridge.start();
        Session tx = bridge.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TextMessage m = tx.createTextMessage("forwarded");
        m.setStringProperty("cresco_caller", caller);
        m.setStringProperty("cresco_caller_tenant", tenant);
        m.setStringProperty("cresco_caller_roles", roles);
        tx.createProducer(tx.createQueue(queue)).send(m);
        Message got = c.receive(5000);
        assertNotNull(got, "nothing delivered (test premise: the bridge connection was admitted)");
        return got;
    }

    @Test
    void aNonSuperuserBridgeDoesNotMintASuperuserAdmin() throws Exception {
        Message got = overTheBridge("api:cresco-system/root", "cresco-system", "admin");
        assertNotEquals("api:cresco-system/root", got.getStringProperty("cresco_caller"),
                "a bridge peer of tenant t1 declared api:cresco-system/root [admin] -- a superuser-tenant admin -- and the broker"
                        + " kept it: every downstream controller admits it everywhere (pluginadd, killjvm, getfiledata ...)");
    }

    @Test
    void aNonSuperuserBridgeDoesNotCarryAnotherTenantsController() throws Exception {
        Message got = overTheBridge("controller:t2/rgZ/z1", "t2", "infra");
        assertNotEquals("controller:t2/rgZ/z1", got.getStringProperty("cresco_caller"),
                "a bridge peer of tenant t1 declared controller:t2/rgZ/z1 [infra] (another tenant's node) and the broker kept it");
    }
}
