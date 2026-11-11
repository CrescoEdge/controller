package io.cresco.agent.data;

import io.cresco.agent.controller.communication.CrescoAuthorizationBroker;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.MapMessage;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node exports its state in its OWN tenant's namespace ({@code T.<tenant>.agent.event}). The global of a superuser
 * tenant listened on its own tenant's topic like any node, so a tenant's region under it never registered ("state
 * export: no route to the parent's listener yet", for ever). Its registry listener is now on every tenant's topic; any
 * other listener, and what the node publishes, stay in its own tenant's namespace.
 */
class EveryTenantRegistryListenerTest {

    private BrokerService broker;
    private Connection writer;

    @AfterEach
    void stop() throws Exception {
        if (writer != null) try { writer.close(); } catch (Exception ignore) { }
        if (broker != null) broker.stop();
    }

    /** A region export as a region's bridge leaves it on the global's broker: on {@code topic}, {@code update_mode} REGION. */
    private void export(Session s, String topic, String region) throws Exception {
        MapMessage m = s.createMapMessage();
        m.setStringProperty("update_mode", "REGION");
        m.setStringProperty("region_id", region);
        s.createProducer(s.createTopic(topic)).send(m);
    }

    private static void await(List<String> got, int n) throws Exception {
        for (long end = System.currentTimeMillis() + 5000; got.size() < n && System.currentTimeMillis() < end; ) Thread.sleep(20);
    }

    @Test
    void theSuperuserGlobalsRegistryListenerTakesEveryTenantsRegionExports() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("cresco-system", "glob", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "cresco-system");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("everytenant").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, new PeerTrustRegistry());
        broker = TlsTestSupport.broker("glob_gc", TlsTestSupport.issued(ca, "cresco-system", "glob", "gc"), false,
                new BrokerPlugin[]{authz}, authz, ca.caCert());

        ControllerEngine gc = TlsTestSupport.engine("glob", "gc", Map.of("tenant_id", "cresco-system"));
        String uri = "vm://glob_gc?create=false";
        assertTrue(gc.getActiveClient().initActiveAgentConsumer("glob_gc", uri));
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(gc, uri);
        assertEquals("T.cresco-system.agent.event", dps.getTopicPhysicalName(TopicType.AGENT), "it publishes in its own tenant's namespace");

        List<String> registry = new CopyOnWriteArrayList<>(), ownTenant = new CopyOnWriteArrayList<>();
        String id = dps.addMessageListenerEveryTenant(TopicType.AGENT,
                m -> { try { registry.add(m.getStringProperty("region_id")); } catch (Exception e) { registry.add("?" + e); } },
                "update_mode = 'REGION'");
        assertNotNull(id);
        assertNotNull(dps.addMessageListener(TopicType.AGENT,
                m -> { try { ownTenant.add(m.getStringProperty("region_id")); } catch (Exception e) { ownTenant.add("?" + e); } },
                "update_mode = 'REGION'"));

        writer = new ActiveMQConnectionFactory(uri).createConnection();
        writer.start();
        Session s = writer.createSession(false, Session.AUTO_ACKNOWLEDGE);
        export(s, "T.ta.agent.event", "rta");
        export(s, "T.tb.agent.event", "rtb");
        export(s, "T.cresco-system.agent.event", "rops");
        await(registry, 3);
        assertEquals(List.of("rta", "rtb", "rops"), registry, "a tenant's region never registers with a global of the superuser tenant");
        await(ownTenant, 1);
        assertEquals(List.of("rops"), ownTenant, "any other listener of the node stays in its own tenant's namespace");

        dps.removeMessageListener(id);
        export(s, "T.ta.agent.event", "rta");
        export(s, "T.cresco-system.agent.event", "rops");
        await(ownTenant, 2);
        assertEquals(3, registry.size(), "a removed listener takes nothing");
    }
}
