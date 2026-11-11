package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;

import java.net.URI;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Real secured brokers joined by real duplex bridges, for the tests of what crosses a bridge: one broker per controller,
 * each with the Cresco authorization, a {@link RouteGate}, an {@link ExpiredMessages} and the destination policy
 * ActiveBroker gives its broker (a subscription takes a message once; the dead-letter strategy). A node is a network
 * client with its own certificate.
 *
 * <p>A controller is named {@code <region>/<agent>}; its region's CA is of the tenant given when the region is first
 * named. The region {@code rgG} (or any region started with {@code is_global}) is a global.
 */
final class BridgedFabric implements AutoCloseable {

    private static final Map<String, RegionCA> CAS = new HashMap<>();
    private static final Map<String, String> TENANT_OF = new HashMap<>();
    private static final Map<String, TlsTestSupport.Id> IDS = new HashMap<>();

    final List<BrokerService> brokers = new ArrayList<>();
    final List<Connection> connections = new ArrayList<>();
    final Map<String, CrescoAuthorizationBroker> authz = new HashMap<>();
    final Map<String, BrokerService> byRegion = new HashMap<>();
    final Map<String, RouteGate> gates = new HashMap<>();
    final Map<String, ExpiredMessages> expired = new HashMap<>();
    final Map<String, List<String>> warnings = new HashMap<>();
    private final Map<String, String> controllerOf = new HashMap<>();

    /** The region {@code region} of {@code tenant} (its CA is made once per JVM). */
    static synchronized RegionCA ca(String tenant, String region) throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = CAS.get(region);
        if (ca == null) {
            ca = RegionCA.generate(tenant, region, 2048, 1);
            CAS.put(region, ca);
            TENANT_OF.put(region, tenant);
        }
        return ca;
    }

    static synchronized TlsTestSupport.Id id(String region, String agent) throws Exception {
        String key = region + "/" + agent;
        TlsTestSupport.Id id = IDS.get(key);
        if (id == null) {
            id = TlsTestSupport.issued(CAS.get(region), TENANT_OF.get(region), region, agent);
            IDS.put(key, id);
        }
        return id;
    }

    private static synchronized X509Certificate[] anchors() {
        List<X509Certificate> roots = new ArrayList<>();
        for (RegionCA ca : CAS.values()) roots.add(ca.caCert());
        return roots.toArray(new X509Certificate[0]);
    }

    static String controllerName(String region) { return region.endsWith("G") ? "gc" : "rc"; }

    /**
     * Start the secured broker of {@code region}'s controller (tenant t1 unless the region was named with another through
     * {@link #ca}). {@code cfg}: the node's settings beside the fixed ones ({@code is_global} is set for a region whose name
     * ends in G). Every region named so far through {@link #ca} or this method is a recorded bridge peer.
     */
    BrokerService start(String region, Map<String, Object> cfg) throws Exception {
        ca("t1", region);
        String tenant;
        List<String> known;
        synchronized (BridgedFabric.class) {
            tenant = TENANT_OF.get(region);
            known = new ArrayList<>(CAS.keySet());
        }
        PeerTrustRegistry peers = new PeerTrustRegistry();
        for (String r : known) {
            String t;
            synchronized (BridgedFabric.class) { t = TENANT_OF.get(r); }
            assertNull(peers.trustCa(CAS.get(r).caCert(), r, t, "test"));
            if (!r.equals(region)) assertNull(peers.allowBridgePeer(t, r, controllerName(r)), "bridge peer " + r);
        }
        Map<String, Object> c = new HashMap<>();
        c.put("broker_security_enabled", "true");
        c.put("tenant_id", tenant);
        if (region.endsWith("G")) c.put("is_global", "true");
        if (cfg != null) c.putAll(cfg);
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("fabric").toString(), c);
        CrescoAuthorizationBroker a = new CrescoAuthorizationBroker(pb, peers);
        RouteGate gate = new RouteGate(1500, 16);
        List<String> warned = new java.util.concurrent.CopyOnWriteArrayList<>();
        gate.setWarn(warned::add);
        ExpiredMessages exp = new ExpiredMessages();
        exp.setWarn(warned::add);
        String controller = controllerName(region);
        BrokerService b = TlsTestSupport.broker(region + "_" + controller, id(region, controller), true,
                new BrokerPlugin[]{a, exp.plugin(), gate.plugin()}, a, anchors());
        brokers.add(b);
        PolicyMap policy = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setDeadLetterStrategy(ActiveBroker.deadLetters(ActiveBroker.DLQ_EXPIRY_MS));
        entry.setExpireMessagesPeriod(500);                  // the tests do not wait ActiveMQ's 30 s for a sweep
        policy.setDefaultEntry(entry);
        b.setDestinationPolicy(policy);
        authz.put(region, a);
        byRegion.put(region, b);
        gates.put(region, gate);
        expired.put(region, exp);
        warnings.put(region, warned);
        controllerOf.put(region, controller);
        return b;
    }

    /** A region of a fabric with the global rgG: configured with it (global_controller_host) and joined to it. */
    BrokerService startUnder(String region, String global, Map<String, Object> cfg) throws Exception {
        Map<String, Object> c = new HashMap<>();
        c.put("global_controller_host", "127.0.0.1");
        if (cfg != null) c.putAll(cfg);
        BrokerService b = start(region, c);
        authz.get(region).setParent(() -> new PeerIdentity.Expected(global, controllerName(global)));
        return b;
    }

    /** A region that runs on its own (global_optional, no global configured). */
    BrokerService startAlone(String region, Map<String, Object> cfg) throws Exception {
        Map<String, Object> c = new HashMap<>();
        c.put("global_optional", "true");
        if (cfg != null) c.putAll(cfg);
        return start(region, c);
    }

    /** A duplex bridge that {@code from}'s broker opens to {@code to}'s, as ActiveBroker does (the connector is registered). */
    NetworkConnector bridge(String from, String to) throws Exception {
        BrokerService f = byRegion.get(from), t = byRegion.get(to);
        NetworkConnector nc = f.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + TlsTestSupport.port(t)
                + "?verifyHostName=false)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("cresco-bridge-" + from + "-" + to + "-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setNetworkTTL(5);                                   // broker_message_ttl, as ActiveBroker sets it
        nc.setConduitSubscriptions(false);
        nc.setDecreaseNetworkConsumerPriority(true);
        nc.setSslContext(new SslContext(id(from, controllerOf.get(from)).keyManagers(), TlsTestSupport.trustManagers(anchors()), new SecureRandom()));
        authz.get(from).registerOutboundBridge(nc.getName(), new PeerIdentity.Expected(to, controllerOf.get(to)));
        nc.start();
        return nc;
    }

    /** The node {@code agent} of {@code region}, connected to its region's broker with its certificate. */
    Session node(String region, String agent) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(byRegion.get(region))
                + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id(region, agent).keyManagers(), TlsTestSupport.trustManagers(anchors()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    Session node(String region) throws Exception { return node(region, "n1"); }

    /** The stamp the node n1 of {@code region} may declare for itself. */
    static String stampOf(String region) {
        String tenant;
        synchronized (BridgedFabric.class) { tenant = TENANT_OF.getOrDefault(region, "t1"); }
        return "controller:" + tenant + "/" + region + "/n1";
    }

    static void send(Session s, jakarta.jms.Destination to, String text, String caller) throws Exception {
        send(s, to, text, caller, DeliveryMode.NON_PERSISTENT, 0);
    }

    static void send(Session s, jakarta.jms.Destination to, String text, String caller, int mode, long ttl) throws Exception {
        TextMessage m = s.createTextMessage(text);
        m.setStringProperty("cresco_caller", caller);
        m.setStringProperty("cresco_caller_tenant", caller.substring(caller.indexOf(':') + 1, caller.indexOf('/')));
        m.setStringProperty("cresco_caller_roles", "infra");
        MessageProducer p = s.createProducer(to);
        p.send(m, mode, 4, ttl);
        p.close();
    }

    long stripped() {
        long n = 0;
        for (CrescoAuthorizationBroker a : authz.values()) n += a.forgedStampCount();
        return n;
    }

    long depth(String region, String queue) throws Exception {
        return byRegion.get(region).getDestination(new ActiveMQQueue(queue)).getDestinationStatistics().getMessages().getCount();
    }

    /** Everything {@code c} is given until it is quiet for {@code quietMs} after {@code expected} messages: text -> its caller. */
    static Map<String, String> drain(MessageConsumer c, int expected, long quietMs, List<String> all) throws Exception {
        Map<String, String> got = new HashMap<>();
        long deadline = System.currentTimeMillis() + 20_000L;
        while (System.currentTimeMillis() < deadline) {
            Message m = c.receive(got.size() >= expected ? quietMs : 5000L);
            if (m == null) break;
            String text = ((TextMessage) m).getText();
            if (all != null) all.add(text);
            got.put(text, m.getStringProperty("cresco_caller"));
        }
        return got;
    }

    static void await(java.util.function.BooleanSupplier c, long ms, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(20);
        if (!c.getAsBoolean()) throw new AssertionError(what);
    }

    @Override
    public void close() {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (int i = brokers.size() - 1; i >= 0; i--) try { brokers.get(i).stop(); } catch (Exception ignore) { }
        for (RouteGate g : gates.values()) g.stop();
    }
}
