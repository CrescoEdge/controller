package io.cresco.agent.data;

import io.cresco.agent.controller.communication.CrescoAuthorizationBroker;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A broker's refusal of a dataplane subscription reaches the caller as a SecurityException (wsapi turns it into a typed
 * error for its client) instead of a silent null; other failures still return null.
 */
class DataPlaneRefusalTest {

    private BrokerService broker;

    @AfterEach
    void stop() throws Exception {
        if (broker != null) broker.stop();
    }

    @Test
    void aRefusedSubscriptionIsReportedAndAnAllowedOneIsNot() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("authz").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, new PeerTrustRegistry());
        broker = TlsTestSupport.broker("rgA_gc", TlsTestSupport.issued(ca, "t1", "rgA", "gc"), false,
                new BrokerPlugin[]{authz}, authz, ca.caCert());

        // the controller rgA/gc on its own broker, as its plugins (wsapi included) use the dataplane
        ControllerEngine gc = TlsTestSupport.engine("rgA", "gc", null);
        String uri = "vm://rgA_gc?create=false";
        assertTrue(gc.getActiveClient().initActiveAgentConsumer("rgA_gc", uri));
        DataPlaneServiceImpl dps = new DataPlaneServiceImpl(gc, uri);

        SecurityException refused = assertThrows(SecurityException.class,
                () -> dps.addMessageListener(TopicType.GLOBAL, m -> { }, "cresco_to='rgA_a1'"),
                "a refused subscription came back as a silent null");
        assertTrue(refused.getMessage().contains("rgA_a1"), refused.getMessage());
        assertNotNull(dps.addMessageListener(TopicType.GLOBAL, m -> { }, "cresco_to='rgA_gc'"));
        assertNotNull(dps.addMessageListener(TopicType.GLOBAL, m -> { }, "stream_name='plain'"));
    }
}
