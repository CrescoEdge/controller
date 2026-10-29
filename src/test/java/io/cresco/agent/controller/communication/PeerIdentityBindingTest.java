package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client-side peer identity binding over real TLS: the legacy per-node trust (pinned keys), the modes, the readiness
 * probe, and broker-to-broker bridges (ActiveBroker.bridgeSslContext).
 */
class PeerIdentityBindingTest {

    private static RegionCA ca;
    private static TlsTestSupport.Id rc, a9;
    private final List<BrokerService> brokers = new ArrayList<>();

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        rc = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a9 = TlsTestSupport.issued(ca, "t1", "rgA", "a9");
    }

    @AfterEach
    void stop() throws Exception {
        for (BrokerService b : brokers) b.stop();
    }

    private BrokerService broker(String name, TlsTestSupport.Id id) throws Exception {
        BrokerService b = TlsTestSupport.broker(name, id, false, null, null, ca.caCert());
        brokers.add(b);
        return b;
    }

    private static ActiveMQSession dial(ControllerEngine agent, BrokerService b, boolean expect) throws Exception {
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(b));
        if (expect) agent.getActiveClient().expectPeer(uri, "rgA", "rc");
        return agent.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true);
    }

    @Test
    void legacyTrustAMintedCertificateForTheRightNameIsRefusedByThePinnedKey() throws Exception {
        // legacy per-node trust: rgA/rc and a rogue node each generated their own root, and this agent trusts both
        // (both exchanged certificates with it). The rogue kept its root key and minted a leaf naming rgA/rc.
        TlsTestSupport.Id realRc = TlsTestSupport.selfRooted("rc", "t1", "rgA", "rc");
        TlsTestSupport.Id minted = TlsTestSupport.selfRooted("rogue", "t1", "rgA", "rc");
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = agent.getCertificateManager();
        cm.addCertificatesToTrustStore("rc", realRc.chain);
        cm.addCertificatesToTrustStore("rogue", minted.chain);
        // the discovery exchange with rgA/rc pinned its key (DiscoveryProcessor.setCertTrust -> learnPeer)
        assertNull(cm.getPeerTrust().pin("rgA", "rc", realRc.chain[0]));

        BrokerService fake = TlsTestSupport.broker("rogue", minted, false, null, null, realRc.chain[1]);
        brokers.add(fake);
        assertNull(dial(agent, fake, true), "a certificate naming rgA/rc minted under another trusted root was accepted");

        BrokerService real = TlsTestSupport.broker("rgA_rc", realRc, false, null, null, realRc.chain[1]);
        brokers.add(real);
        ActiveMQSession s = dial(agent, real, true);
        assertNotNull(s, "the pinned peer itself was refused");
        s.getConnection().close();
    }

    @Test
    void aBrokerUriWithNoRecordedPeerIsRefusedUnderEnforce() throws Exception {
        BrokerService real = broker("rgA_rc", rc);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        TlsTestSupport.enroll(agent, ca, "t1");
        assertNull(dial(agent, real, false), "a connection with no expected peer was accepted under enforce");
    }

    @Test
    void warnModeLogsButConnects() throws Exception {
        BrokerService fake = broker("impostor", a9);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", Map.of(PeerIdentity.MODE_KEY, "warn"));
        TlsTestSupport.enroll(agent, ca, "t1");
        ActiveMQSession s = dial(agent, fake, true);
        assertNotNull(s, "warn mode must not refuse");
        s.getConnection().close();
    }

    @Test
    void offModeIsTheOldChainOnlyCheck() throws Exception {
        BrokerService fake = broker("impostor", a9);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", Map.of(PeerIdentity.MODE_KEY, "off"));
        TlsTestSupport.enroll(agent, ca, "t1");
        ActiveMQSession s = dial(agent, fake, true);
        assertNotNull(s, "peer_identity_mode=off must behave as before (chain only)");
        s.getConnection().close();
    }

    @Test
    void theReadinessProbeStopsAtAnIdentityMismatchInsteadOfWaiting() throws Exception {
        BrokerService fake = broker("impostor", a9);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        TlsTestSupport.enroll(agent, ca, "t1");
        long t0 = System.currentTimeMillis();
        boolean ready = agent.getActiveClient().waitForBrokerTlsReady("127.0.0.1", TlsTestSupport.port(fake), 15000,
                new PeerIdentity.Expected("rgA", "rc"));
        assertFalse(ready);
        assertTrue(System.currentTimeMillis() - t0 < 5000, "the probe kept retrying a broker with the wrong identity");

        BrokerService real = broker("rgA_rc", rc);
        assertTrue(agent.getActiveClient().waitForBrokerTlsReady("127.0.0.1", TlsTestSupport.port(real), 15000,
                new PeerIdentity.Expected("rgA", "rc")));
    }

    private static boolean admitsBridge(BrokerService accepting, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < end) {
            for (org.apache.activemq.broker.Connection c : accepting.getRegionBroker().getClients()) {
                if (c.isNetworkConnection()) return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private NetworkConnector bridgeTo(ControllerEngine node, BrokerService remote) throws Exception {
        BrokerService local = new BrokerService();
        local.setBrokerName("rgB_rc-" + System.nanoTime());
        local.setPersistent(false);
        local.setUseJmx(false);
        local.setUseShutdownHook(false);
        local.start();
        brokers.add(0, local);
        int port = TlsTestSupport.port(remote);
        NetworkConnector nc = local.addNetworkConnector(new URI("static:(nio+ssl://127.0.0.1:" + port
                + "?verifyHostName=false&socketBufferSize=0)?maxReconnectAttempts=2&initialReconnectDelay=200"));
        nc.setName("bridge-" + System.nanoTime());
        nc.setDuplex(true);
        // exactly what ActiveBroker.buildConnector installs for a bridge discovery said leads to rgA/rc
        nc.setSslContext(ActiveBroker.bridgeSslContext(node.getCertificateManager(), new PeerIdentity.Expected("rgA", "rc"),
                "bridge -> 127.0.0.1:" + port));
        nc.start();
        return nc;
    }

    @Test
    void aBridgeIsBoundToTheControllerDiscoveryReturned() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgB", "rc", null);
        // what federating with rgA's controller records at rgB: rgA's CA, scoped to region rgA
        assertNull(region.getCertificateManager().getPeerTrust().trustCa(ca.caCert(), "rgA", "t1", "test"));
        region.getCertificateManager().addCertificatesToTrustStore("rgA-ca", new X509Certificate[]{ca.caCert()});

        BrokerService fake = broker("impostor", a9);
        bridgeTo(region, fake);
        assertFalse(admitsBridge(fake, 4000), "the bridge connected to a broker whose certificate names rgA/a9");

        BrokerService real = broker("rgA_rc", rc);
        bridgeTo(region, real);
        assertTrue(admitsBridge(real, 10000), "the bridge did not reach the controller it was meant to reach");
    }

    @Test
    void clientTrustManagersSeeCertificatesAddedLater() throws Exception {
        // trust learned after the connection's context was built (a later discovery exchange) still counts
        RegionCA other = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id late = TlsTestSupport.issued(other, "t1", "rgA", "rc");
        BrokerService b = TlsTestSupport.broker("rgA_rc", late, false, null, null, other.caCert());
        brokers.add(b);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(b));
        agent.getActiveClient().expectPeer(uri, "rgA", "rc");
        assertNull(agent.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true));
        agent.getCertificateManager().addCertificatesToTrustStore("rgA-ca", new X509Certificate[]{other.caCert()});
        assertNull(agent.getCertificateManager().getPeerTrust().trustCa(other.caCert(), "rgA", "t1", "test"));
        ActiveMQSession s = agent.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true);
        assertNotNull(s);
        s.getConnection().close();
    }
}
