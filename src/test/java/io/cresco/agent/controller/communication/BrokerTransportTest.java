package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** No secret-looking option ever reaches a broker URI handed to ActiveMQ (it logs URIs verbatim). */
class BrokerTransportTest {

    static final String FAKE = "test-not-a-secret";

    @Test
    void secretLookingTransportOptionsAreLeftOutOfTheClientQuery() {
        List<String> dropped = new ArrayList<>();
        String kept = BrokerTransport.clientOptions("keyStorePassword=" + FAKE + "-1&trustStorePassword=" + FAKE + "-2"
                + "&tcpNoDelay=true&password=" + FAKE + "-3&jms.clientSecret=" + FAKE + "-4&wireFormat.maxInactivityDuration=30000", dropped);
        assertEquals("tcpNoDelay=true&wireFormat.maxInactivityDuration=30000", kept);
        assertEquals(List.of("keyStorePassword", "trustStorePassword", "password", "jms.clientSecret"), dropped);
        assertFalse(kept.contains(FAKE));
        for (String name : dropped) assertFalse(name.contains("="), "only names are reported, never values");

        dropped.clear();
        assertEquals("tcpNoDelay=true&soTimeout=0", BrokerTransport.clientOptions(" tcpNoDelay=true&soTimeout=0 ", dropped));
        assertTrue(dropped.isEmpty());
        assertEquals("tcpNoDelay=true", BrokerTransport.clientOptions("?tcpNoDelay=true&&", dropped));
        assertEquals("", BrokerTransport.clientOptions(null, dropped));
        assertEquals("", BrokerTransport.clientOptions("password", dropped), "a bare secret-looking flag is dropped too");
    }

    @Test
    void everyUriHandedToActiveMQLosesItsSecretLookingParameters() {
        List<String> dropped = new ArrayList<>();
        String failover = "failover:(nio+ssl://10.0.0.1:32010?verifyHostName=false&keyStorePassword=" + FAKE
                + "&socketBufferSize=0,nio+ssl://10.0.0.2:32010?trustStorePassword=" + FAKE
                + ")?maxReconnectAttempts=5&password=" + FAKE + "&initialReconnectDelay=5000";
        assertEquals("failover:(nio+ssl://10.0.0.1:32010?verifyHostName=false&socketBufferSize=0,nio+ssl://10.0.0.2:32010)"
                + "?maxReconnectAttempts=5&initialReconnectDelay=5000", BrokerTransport.withoutSecretParams(failover, dropped));
        assertEquals(List.of("keyStorePassword", "trustStorePassword", "password"), dropped);

        assertEquals("nio+ssl://0.0.0.0:32010?daemon=true",
                BrokerTransport.withoutSecretParams("nio+ssl://0.0.0.0:32010?keyStorePassword=" + FAKE + "&daemon=true", null),
                "a leading secret parameter keeps the ? for the rest");
        assertEquals("failover:(nio+ssl://h:1)?a=1",
                BrokerTransport.withoutSecretParams("failover:(nio+ssl://h:1?password=" + FAKE + ")?a=1", null),
                "a query of only secrets loses its ?");

        String clean = "static:(nio+ssl://h:32010?verifyHostName=false&socketBufferSize=0)?maxReconnectAttempts=5";
        assertSame(clean, BrokerTransport.withoutSecretParams(clean, null), "a clean URI is returned unchanged");
        assertEquals("vm://localhost", BrokerTransport.withoutSecretParams("vm://localhost", null));
        assertNull(BrokerTransport.withoutSecretParams(null, null));
    }
}
