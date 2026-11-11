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

    @Test
    void aBrokerDoesNotAdvertiseTheBrokersItIsBridgedToToItsClients() {
        // on from an early experiment: every failover client was sent the global's and the peer regions' addresses
        org.apache.activemq.broker.TransportConnector connector = new org.apache.activemq.broker.TransportConnector();
        connector.setUpdateClusterClients(true);
        connector.setUpdateClusterClientsOnRemove(true);
        connector.setRebalanceClusterClients(true);
        BrokerTransport.noClusterAdvertising(connector);
        assertFalse(connector.isUpdateClusterClients());
        assertFalse(connector.isUpdateClusterClientsOnRemove());
        assertFalse(connector.isRebalanceClusterClients());
    }

    @Test
    void anAgentsFailoverNamesItsParentsBrokerOnlyAndTakesNoAddressABrokerAdvertises() throws Exception {
        String uri = BrokerTransport.agentFailoverURI("tcp", "127.0.0.1", 32010, "?soTimeout=1000", "5000");
        assertEquals("failover:(tcp://127.0.0.1:32010?soTimeout=1000)?maxReconnectAttempts=5&initialReconnectDelay=5000"
                + "&useExponentialBackOff=false&updateURIsSupported=false", uri);
        // ActiveMQ reads it as written (an option it does not know is an error, not ignored): nothing is connected here
        org.apache.activemq.transport.failover.FailoverTransport t = (org.apache.activemq.transport.failover.FailoverTransport)
                new org.apache.activemq.transport.failover.FailoverTransportFactory()
                        .createTransport(org.apache.activemq.util.URISupport.parseComposite(new java.net.URI(uri)));
        assertFalse(t.isUpdateURIsSupported(), "an agent of a parent that still advertises its network (an earlier build) stays with its parent");
        assertEquals(5, t.getMaxReconnectAttempts());
        assertNull(BrokerTransport.agentFailoverURI("nio+ssl", "h", 1, null, "250").split("\\)")[0].contains("null") ? "null in the URI" : null);
    }
}
