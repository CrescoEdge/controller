package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.AgentServiceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Config;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.TenantNamespace;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OUT-81: an agent on its own broker (the global controller) reaches it over vm://. The control
 * plane (ControlPlaneSender + the AgentConsumer inbox) gets its own vm:// connections instead of
 * sharing the pooled one the dataplane uses (controlplane_dedicated_vm, default true). Proven on a
 * real embedded broker with the tenant authorization plugin installed, so the new connections are
 * shown to be exempt and the T.*.agentPath wildcard inbox to bind and receive.
 */
class ControlPlaneVmIsolationTest {

    private static final String VM = "vm://localhost";
    private static final String AGENT_PATH = "r1_a1";

    private BrokerService broker;
    private String tcpUri;

    /** Region/agent for PluginBuilder.getRegion()/getAgent(); the rest of ControllerState unused. */
    private static final class TestAgentService implements AgentService {
        private final AgentServiceImpl loggers = new AgentServiceImpl();
        private final AgentState state = new AgentState((ControllerState) Proxy.newProxyInstance(
                ControllerState.class.getClassLoader(), new Class<?>[]{ControllerState.class},
                (p, m, a) -> {
                    switch (m.getName()) {
                        case "getRegion": return "r1";
                        case "getAgent": return "a1";
                        case "getAgentPath": return AGENT_PATH;
                        case "isActive": return Boolean.TRUE;
                        default: return m.getReturnType() == boolean.class ? Boolean.FALSE : null;
                    }
                }));
        @Override public AgentState getAgentState() { return state; }
        @Override public DataPlaneService getDataPlaneService() { return null; }
        @Override public CLogger getCLogger(PluginBuilder pb, String b, String i, CLogger.Level l) { return loggers.getCLogger(pb, b, i, l); }
        @Override public CLogger getCLogger(PluginBuilder pb, String b, String i) { return loggers.getCLogger(pb, b, i); }
        @Override public void msgOut(String id, MsgEvent msg) { }
        @Override public void setLogLevel(String logId, CLogger.Level level) { }
        @Override public String getAgentDataDirectory() { return System.getProperty("java.io.tmpdir"); }
    }

    private static PluginBuilder plugin(Map<String, Object> cfg) {
        Map<String, Object> m = new HashMap<>(cfg);
        m.putIfAbsent("tenant_namespacing", "true");
        return new PluginBuilder(new TestAgentService(), AgentServiceImpl.class.getName(), new MockBundleContext(), m);
    }

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        // the same tenant authorization the agent broker installs under broker_security_enabled
        broker.setPlugins(new BrokerPlugin[]{new CrescoAuthorizationBroker(plugin(new HashMap<>()))});
        TransportConnector tcp = broker.addConnector("tcp://127.0.0.1:0");
        broker.start();
        broker.waitUntilStarted();
        tcpUri = tcp.getPublishableConnectString();
    }

    @AfterEach
    void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private int brokerClients() throws Exception {
        return broker.getBroker().getClients().length;
    }

    private long dequeued(String queue) throws Exception {
        org.apache.activemq.broker.region.Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getDestinationStatistics().getDequeues().getCount();
    }

    private static MsgEvent watchdog() {
        return new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a1", null, "r1", "a1", null, false, false);
    }

    private void awaitDequeued(String queue, long n) throws Exception {
        long end = System.currentTimeMillis() + 10_000;
        while (dequeued(queue) < n && System.currentTimeMillis() < end) Thread.sleep(20);
        assertEquals(n, dequeued(queue), "inbox consumed the control message on " + queue);
    }

    @Test
    void controlPlaneGetsItsOwnVmConnectionsByDefault() throws Exception {
        ControllerEngine engine = new ControllerEngine(null, plugin(new HashMap<>()), null, null);
        ActiveClient ac = engine.getActiveClient();
        ActiveMQSession dataplane = ac.createSession(VM, false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(dataplane);
        Connection pooled = dataplane.getConnection();

        ControlPlaneSender sender = new ControlPlaneSender(engine, VM);
        String inbox = TenantNamespace.wildcard(AGENT_PATH);
        AgentConsumer consumer = new AgentConsumer(engine, inbox, VM);
        try {
            assertTrue(sender.isDedicatedConnection());
            assertTrue(consumer.isDedicatedConnection());
            ActiveMQConnection senderConn = sender.transportConnection();
            assertNotSame(pooled, senderConn, "control sends no longer ride the dataplane's vm:// connection");
            assertNotSame(pooled, consumer.getConnection(), "the inbox no longer rides the dataplane's vm:// connection");
            assertNotSame(senderConn, consumer.getConnection());
            assertEquals(3, brokerClients(), "pooled (dataplane) + control sender + inbox");
            assertTrue(consumer.isConnectionActive());

            // the wildcard inbox binds on the dedicated vm:// connection (exempt from tenant authz)
            // and receives a tenant-qualified control message sent on the sender's own connection
            assertEquals("T.*." + AGENT_PATH, inbox);
            assertTrue(sender.send("T.tenantA." + AGENT_PATH, watchdog(), MsgQoS.Tier.LIVENESS));
            awaitDequeued("T.tenantA." + AGENT_PATH, 1);

            // quietFailure: a failure on the control connection is the owner's to recover; it does
            // not tear down the pooled (dataplane) connection, and the next send rebuilds
            senderConn.getExceptionListener().onException(new JMSException("simulated vm:// failure"));
            assertTrue(senderConn.isClosed() || senderConn.isTransportFailed() || senderConn.isClosing());
            assertFalse(((ActiveMQConnection) pooled).isClosed(), "the dataplane connection survives");
            assertFalse(dataplane.isClosed());
            long end = System.currentTimeMillis() + 5_000;
            boolean sent = false;
            while (!sent && System.currentTimeMillis() < end) {
                sent = sender.send("T.tenantB." + AGENT_PATH, watchdog(), MsgQoS.Tier.CONTROL);
            }
            assertTrue(sent, "sender rebuilt its own connection");
            assertNotSame(senderConn, sender.transportConnection());
            assertNotSame(pooled, sender.transportConnection());
            awaitDequeued("T.tenantB." + AGENT_PATH, 1);
        } finally {
            sender.shutdown();
            consumer.shutdown();
            ac.shutdown();
        }
    }

    @Test
    void negativeControlFlagOffSharesTheDataplaneConnection() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("controlplane_dedicated_vm", "false");
        ControllerEngine engine = new ControllerEngine(null, plugin(cfg), null, null);
        ActiveClient ac = engine.getActiveClient();
        ActiveMQSession dataplane = ac.createSession(VM, false, Session.AUTO_ACKNOWLEDGE);
        Connection pooled = dataplane.getConnection();

        ControlPlaneSender sender = new ControlPlaneSender(engine, VM);
        AgentConsumer consumer = new AgentConsumer(engine, TenantNamespace.wildcard(AGENT_PATH), VM);
        try {
            assertFalse(sender.isDedicatedConnection());
            assertFalse(consumer.isDedicatedConnection());
            assertSame(pooled, sender.transportConnection(), "flag off: control shares the dataplane connection");
            assertSame(pooled, consumer.getConnection(), "flag off: the inbox shares the dataplane connection");
            assertEquals(1, brokerClients(), "one vm:// connection for everything");
            assertTrue(sender.send("T.tenantA." + AGENT_PATH, watchdog(), MsgQoS.Tier.LIVENESS));
            awaitDequeued("T.tenantA." + AGENT_PATH, 1);
        } finally {
            sender.shutdown();
            consumer.shutdown();
            ac.shutdown();
        }
    }

    @Test
    void tenantAuthorizationIsLiveForNetworkClients() throws Exception {
        // control for the exemption above: an anonymous network client may not bind the wildcard inbox
        ActiveMQConnection c = (ActiveMQConnection) new ActiveMQConnectionFactory(tcpUri).createConnection();
        try {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertThrows(JMSException.class, () -> s.createConsumer(s.createQueue(TenantNamespace.wildcard(AGENT_PATH))));
        } finally {
            c.close();
        }
    }

    @Test
    void networkUrisKeepTheirOwnFlags() {
        Map<String, Object> m = new HashMap<>();
        String net = "failover:(nio+ssl://10.0.0.1:32010)?maxReconnectAttempts=5";
        assertTrue(ActiveClient.dedicatedControlConnection(new Config(m), net, "controlplane_dedicated_connection"));
        assertTrue(ActiveClient.dedicatedControlConnection(new Config(m), VM, "controlplane_dedicated_connection"));
        m.put("controlplane_dedicated_vm", "false");
        assertTrue(ActiveClient.dedicatedControlConnection(new Config(m), net, "controlplane_dedicated_connection"),
                "the vm:// flag does not change network URIs");
        assertFalse(ActiveClient.dedicatedControlConnection(new Config(m), VM, "controlplane_dedicated_connection"));
        m.put("agentconsumer_dedicated_connection", "false");
        assertFalse(ActiveClient.dedicatedControlConnection(new Config(m), net, "agentconsumer_dedicated_connection"));
        m.remove("controlplane_dedicated_vm");
        assertTrue(ActiveClient.dedicatedControlConnection(new Config(m), VM, "agentconsumer_dedicated_connection"),
                "the network flag does not change vm://");
    }
}
