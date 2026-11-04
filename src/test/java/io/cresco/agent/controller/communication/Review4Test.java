package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Fourth adversarial review. Each test asserts the SECURE / correct outcome; a failure is a finding. */
class Review4Test {

    private static final Gson GSON = new Gson();
    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("r4").resolve("security").resolve("peer-trust.json");
    }

    // ------------------------------------------------------------------ registry crash consistency

    /**
     * compact(): writeAtomically(new log) [rename + dir fsync] THEN writeHead(new head). A crash (power loss, kill -9,
     * OOM kill) between the two leaves the compacted (shorter) log next to the previous head anchor, which covers more
     * lines: on restart checkHead says "truncated or rolled back" and the registry SEALS -- no new agent, region or CA can
     * be bound until an operator writes 'accept'. Simulated exactly: keep the head as it was before the compaction.
     */
    @Test
    void aCrashBetweenTheCompactedLogAndItsHeadDoesNotSealTheRegistry() throws Exception {
        Path f = newFile();
        Path head = f.resolveSibling(f.getFileName() + ".head");
        PeerTrustRegistry r = open(f);
        KeyPair c1 = TlsTestSupport.rsa(), c2 = TlsTestSupport.rsa();
        boolean compacted = false;
        for (int i = 0; i < 1100 && !compacted; i++) {
            byte[] headBefore = Files.readAllBytes(head);
            long linesBefore = Files.lines(f).count();
            assertTrue(r.bind("t1", "rgA", "rc", (i % 2 == 0 ? c1 : c2).getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, true).ok);
            if (Files.lines(f).count() < linesBefore) {
                compacted = true;
                Files.write(head, headBefore);       // the crash: the new log was renamed in, the head write never happened
                System.out.println("compaction at bind " + i + ": log " + linesBefore + " -> " + Files.lines(f).count() + " lines; head left as before");
            }
        }
        assertTrue(compacted, "precondition: a compaction happened");
        PeerTrustRegistry reboot = open(f);
        System.out.println("after the simulated crash: sealed=" + reboot.isSealed() + " reason=" + reboot.sealReason());
        assertFalse(reboot.isSealed(), "a crash during a routine compaction sealed the registry (" + reboot.sealReason() + ")");
    }

    /**
     * append(): a write that fails part-way (ENOSPC / EIO on a nearly full disk) leaves the partial bytes in the log;
     * nothing truncates them, and the next append (APPEND mode) lands right after them, making one unparseable,
     * newline-terminated line in the MIDDLE of the log. The node keeps running; on its next start the chain check fails
     * there, the registry SEALS, and every binding acknowledged after the incident is not served.
     */
    @Test
    void aFailedPartialAppendDoesNotSealTheRegistryOrLoseLaterBindings() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        // what a failed write leaves behind: part of the next record, no newline
        try (FileChannel ch = FileChannel.open(f, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap("{\"op\":\"bind\",\"name\":\"rgA/a2\",\"ten".getBytes(StandardCharsets.UTF_8)));
        }
        // disk space comes back; later joins succeed and are acknowledged
        assertTrue(r.bind("t1", "rgA", "a3", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgA", "a4", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        PeerTrustRegistry reboot = open(f);
        boolean a3 = reboot.pinnedKey(new PeerIdentity.Expected("rgA", "a3")) != null;
        System.out.println("after restart: sealed=" + reboot.isSealed() + " (" + reboot.sealReason() + "); acknowledged rgA/a3 served=" + a3);
        assertFalse(reboot.isSealed(), "one failed write sealed the registry at the next restart: " + reboot.sealReason());
        assertTrue(a3, "a binding acknowledged after a failed write is not served after a restart");
    }

    /**
     * regionInit() makes a NEW CertificateManager -- and so a second PeerTrustRegistry on the same files -- on every
     * globalControllerLost (stateInit -> regionInit), without closing the first (there is no close()). The first one is
     * still held by the running broker's CrescoAuthorizationBroker and by old trust managers; since round 4 every verified
     * connection refreshes last-seen and a background batch APPENDS it. Two appenders with independent MAC chains on one
     * log: the next start seals the registry.
     */
    @Test
    void anOrphanedRegistryInstanceCannotCorruptTheLog() throws Exception {
        Path f = newFile();
        PeerTrustRegistry before = open(f);                                      // the CertificateManager before the global loss
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(before.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        PeerTrustRegistry after = open(f);                                       // regionInit: new CertificateManager, same files
        assertTrue(after.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery").ok);   // a new join after re-init
        // the old broker plugin still verifies a1's connections: a last-seen refresh more than a day after the last save
        before.backdateBinding("rgA", "a1", 2L * 24 * 3600 * 1000);
        assertNotNull(before.verifiedIdentity(a1.chain));
        before.flushSeen();                                                      // the background batch (5 s later)
        PeerTrustRegistry reboot = open(f);
        System.out.println("after two instances wrote the log: sealed=" + reboot.isSealed() + " (" + reboot.sealReason() + ")");
        assertFalse(reboot.isSealed(), "a second registry instance on the same files (CertificateManager re-created by regionInit) sealed it: "
                + reboot.sealReason());
    }

    /** The security side of the same: a rebind acknowledged after the failed write is undone -- the REPLACED key is served again. */
    @Test
    void aRebindAfterAFailedPartialAppendIsNotUndoneByARestart() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of("rgA/a1"), List.of(), 30, m -> System.out.println("[registry] " + m));
        TlsTestSupport.Id stolen = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", stolen.kp.getPublic(), "discovery").ok);
        try (FileChannel ch = FileChannel.open(f, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap("{\"op\":\"seen\",\"na".getBytes(StandardCharsets.UTF_8)));
        }
        KeyPair fresh = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "a1", fresh.getPublic(), "discovery").ok, "precondition: the operator's rebind to the new key");
        assertNull(r.boundByKey(stolen.chain), "precondition: the replaced key is refused");
        PeerTrustRegistry reboot = PeerTrustRegistry.open(f, List.of("rgA/a1"), List.of(), 30, m -> { });
        boolean stolenBack = reboot.boundByKey(stolen.chain) != null;
        System.out.println("after restart: sealed=" + reboot.isSealed() + "; the replaced (stolen) key is trusted again=" + stolenBack);
        assertFalse(stolenBack, "after a restart the registry serves the key the operator had rebound away (the rebind record is past the bad line)");
    }

    // ------------------------------------------------------------------ revoked keys at TLS time

    private static ControllerEngine engineAt(Path dir, String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("discovery_secret_agent", "agent-secret");
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");      // phase 4: this broker-policy test uses the un-namespaced dataplane topic
        if (extra != null) cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State s = new TlsTestSupport.State(pb, region, agent);
        s.regional = regional;
        e.cstate = s;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static DiscoveryNode join(ControllerEngine agent, ControllerEngine ctl) throws Exception {
        DiscoveryProcessor c = new DiscoveryProcessor(agent), s = new DiscoveryProcessor(ctl);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), agent.cstate.getAgent());
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(c.clientProof(hello, copy(ch), ctl.cstate.getAgent())), agent.cstate.getAgent());
        return r == null ? null : c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    private static boolean tlsTrustsClient(ControllerEngine ctl, X509Certificate[] chain) {
        try {
            for (TrustManager tm : ctl.getCertificateManager().getTrustManagers()) {   // what the broker's TLS context uses
                if (tm instanceof X509TrustManager) { ((X509TrustManager) tm).checkClientTrusted(chain, "RSA"); return true; }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Per-node trust. Agent rgA/a1 joins with key K1; it is re-installed (key K2) and the operator authorises one rebind.
     * K1 is now "replaced (rebound away) and stays refused" in the registry -- but its self-rooted chain is still an
     * anchor in the controller's truststore (entries are never removed, and since 8878f49 never overwritten either), so
     * the broker's TLS layer still accepts a client holding K1 until the controller restarts.
     */
    @Test
    void aKeyReboundAwayIsNoLongerAcceptedAtTls() throws Exception {
        Path cdir = Files.createTempDirectory("r4-ctl");
        ControllerEngine ctl = engineAt(cdir, "rgA", "rc", true, Map.of("peer_identity_rebind", "rgA/a1"));
        ControllerEngine oldAgent = engineAt(Files.createTempDirectory("r4-a1-old"), "rgA", "a1", false, null);
        assertNotNull(join(oldAgent, ctl), "precondition: a1 (K1) joins");
        X509Certificate[] k1 = oldAgent.getCertificateManager().getPublicCertificate();
        assertTrue(tlsTrustsClient(ctl, k1), "precondition: TLS trusts K1 after its discovery");

        ControllerEngine reinstalled = engineAt(Files.createTempDirectory("r4-a1-new"), "rgA", "a1", false, null);
        assertNotNull(join(reinstalled, ctl), "precondition: the re-installed a1 (K2) joins under the operator's one-shot rebind");
        PeerTrustRegistry reg = ctl.getCertificateManager().getPeerTrust();
        assertNull(reg.verifiedIdentity(k1), "precondition: the registry refuses K1 for rgA/a1 now");

        boolean tls = tlsTrustsClient(ctl, k1);
        System.out.println("after the rebind: registry verifies K1=" + (reg.verifiedIdentity(k1) != null) + "; broker TLS still accepts K1=" + tls);
        assertFalse(tls, "the old, rebound-away key K1 still passes the broker's TLS trust (its chain stays a truststore anchor)");
    }

    /**
     * What such a TLS-trusted but UNVERIFIED certificate gets at the broker: unlike a certificate-less connection (round 4:
     * quarantined, nothing), it is not quarantined -- it reads and writes the fabric-shared topics (agent.event,
     * region.event, global.event: the whole dataplane when tenant_namespacing is off, the default).
     */
    @Test
    void aConnectionWhoseCertificateDoesNotVerifyGetsNoMoreThanOneWithoutACertificate() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id spyId = TlsTestSupport.selfRooted("a9", "t1", "rgA", "a9");      // the old K1 of rgA/a9
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");      // phase 4: this broker-policy test uses the un-namespaced dataplane topic
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4b").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry().withRebind(List.of("rgA/a9"));
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertTrue(peers.bind("t1", "rgA", "a9", spyId.kp.getPublic(), "discovery").ok);
        assertTrue(peers.bind("t1", "rgA", "a9", TlsTestSupport.rsa().getPublic(), "discovery").ok);   // re-installed: K1 rebound away
        assertNull(peers.verifiedIdentity(spyId.chain), "precondition: K1 no longer verifies");
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert(), spyId.chain[1]);
        brokers.add(b);

        Session owner = client(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), caA);
        Session spy = client(b, spyId, caA);
        MessageConsumer spyIn = spy.createConsumer(spy.createTopic("global.event"));
        MessageConsumer ownerIn = owner.createConsumer(owner.createTopic("global.event"));
        Thread.sleep(300);
        owner.createProducer(owner.createTopic("global.event")).send(owner.createTextMessage("fabric data"));
        spy.createProducer(spy.createTopic("global.event")).send(spy.createTextMessage("forged by a revoked key"));
        Message read = spyIn.receive(2000);
        List<String> ownerGot = new ArrayList<>();
        for (Message m = ownerIn.receive(2000); m != null; m = ownerIn.receive(500)) ownerGot.add(((TextMessage) m).getText());
        System.out.println("revoked-key connection read=" + (read == null ? null : ((TextMessage) read).getText()) + "; owner received " + ownerGot);
        assertNull(read, "a connection whose certificate does not verify (revoked key) read the fabric-shared topic");
        assertFalse(ownerGot.contains("forged by a revoked key"), "a connection whose certificate does not verify injected into global.event");
    }

    private Session client(BrokerService b, TlsTestSupport.Id id, RegionCA serverCa) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(serverCa.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    // ------------------------------------------------------------------ quarantine and network bridges

    /**
     * A quarantined (certificate-less) connection may still CREATE consumers on any queue (enforce() returns early). Its
     * consumer advisory reaches the broker's network bridge like any other, so the bridge creates demand on the peer broker
     * and forwards that queue's messages here -- where the only consumer is the quarantined one, which may receive nothing.
     * The messages are stranded on this broker: the real consumer at the peer (e.g. the global controller's inbox, briefly
     * without a consumer during a restart) never gets them.
     */
    @Test
    void aQuarantinedConsumerCannotPullAnotherBrokersQueueAcrossABridge() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService peer = new BrokerService();
        peer.setBrokerName("rgG_gc"); peer.setPersistent(false); peer.setUseJmx(false); peer.setUseShutdownHook(false);
        org.apache.activemq.broker.TransportConnector pc = peer.addConnector("tcp://127.0.0.1:0");
        peer.start(); peer.waitUntilStarted();
        brokers.add(peer);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");      // phase 4: this broker-policy test uses the un-namespaced dataplane topic
        cfg.put("broker_require_client_auth", "false");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4q").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService region = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?wantClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert());
        brokers.add(0, region);
        org.apache.activemq.network.NetworkConnector nc = region.addNetworkConnector("static:(" + pc.getConnectUri() + ")");
        nc.setDecreaseNetworkConsumerPriority(true);
        nc.setConduitSubscriptions(false);
        nc.setNetworkTTL(5);
        nc.setDuplex(true);
        nc.start();
        Thread.sleep(1500);

        // a network client WITHOUT a certificate (quarantined) subscribes to the global controller's inbox
        ActiveMQSslConnectionFactory anon = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(region) + "?verifyHostName=false");
        anon.setKeyAndTrustManagers(null, TlsTestSupport.trustManagers(caA.caCert()), new SecureRandom());
        anon.setUserName("nobody");
        Connection ac = anon.createConnection();
        connections.add(ac);
        ac.start();
        Session as = ac.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer quarantined = as.createConsumer(as.createQueue("rgG_gc"));
        Thread.sleep(1500);

        // the global's inbox gets 10 messages while its controller is between consumers (e.g. restarting)
        Connection gc = new org.apache.activemq.ActiveMQConnectionFactory(pc.getConnectUri()).createConnection();
        connections.add(gc);
        gc.start();
        Session gs = gc.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = gs.createProducer(gs.createQueue("rgG_gc"));
        for (int i = 0; i < 10; i++) p.send(gs.createTextMessage("control " + i));
        Thread.sleep(2000);
        assertNull(quarantined.receive(200), "precondition: the quarantined consumer itself receives nothing");
        MessageConsumer real = gs.createConsumer(gs.createQueue("rgG_gc"));
        int got = 0;
        for (Message m = real.receive(2000); m != null; m = real.receive(500)) got++;
        long strandedAtRegion = region.getDestination(new org.apache.activemq.command.ActiveMQQueue("rgG_gc")).getDestinationStatistics().getMessages().getCount();
        System.out.println("global controller received " + got + "/10; stranded on the region broker behind a quarantined consumer: " + strandedAtRegion);
        assertEquals(10, got, "a certificate-less (quarantined) connection pulled the global controller's queue across the bridge and stranded "
                + strandedAtRegion + " message(s) on the region broker");
    }

    /**
     * Quarantine covers addConsumer/addProducer/send/delivery only. removeDestination (ActiveMQConnection.destroyDestination,
     * a DestinationInfo REMOVE command) is not checked at all: a certificate-less connection -- and any verified tenant --
     * deletes another tenant's queue, with every message waiting in it, whenever the queue has no active consumer (an agent
     * reconnecting, a controller restarting).
     */
    @Test
    void aQuarantinedConnectionCannotDestroyAnotherTenantsQueue() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");      // phase 4: this broker-policy test uses the un-namespaced dataplane topic
        cfg.put("broker_require_client_auth", "false");
        cfg.put("tenant_namespacing", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4d").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?wantClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert());
        brokers.add(b);
        Session owner = client(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), caA);
        owner.createProducer(owner.createQueue("T.t1.rgA_a1")).send(owner.createTextMessage("pending work"));

        ActiveMQSslConnectionFactory anon = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        anon.setKeyAndTrustManagers(null, TlsTestSupport.trustManagers(caA.caCert()), new SecureRandom());
        org.apache.activemq.ActiveMQConnection ac = (org.apache.activemq.ActiveMQConnection) anon.createConnection();
        connections.add(ac);
        ac.start();
        String outcome;
        try { ac.destroyDestination(new org.apache.activemq.command.ActiveMQQueue("T.t1.rgA_a1")); outcome = "destroyed"; }
        catch (Exception e) { outcome = "refused: " + e.getMessage(); }
        MessageConsumer c = owner.createConsumer(owner.createQueue("T.t1.rgA_a1"));
        Message m = c.receive(2000);
        System.out.println("certificate-less destroyDestination(T.t1.rgA_a1): " + outcome + "; owner then received " + (m == null ? null : ((TextMessage) m).getText()));
        assertNotNull(m, "a certificate-less (quarantined) connection deleted tenant t1's queue and its pending message");
    }

    /** The same with client certificates required (the default): a VERIFIED identity of tenant t2 deletes tenant t1's queue. */
    @Test
    void anotherTenantCannotDestroyATenantsQueue() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        RegionCA caB = RegionCA.generate("t2", "rgB", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");      // phase 4: this broker-policy test uses the un-namespaced dataplane topic
        cfg.put("tenant_namespacing", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4e").toString(), cfg);
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(caA.caCert(), "rgA", "t1", "test"));
        assertNull(peers.trustCa(caB.caCert(), "rgB", "t2", "test"));
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, caA.caCert(), caB.caCert());
        brokers.add(b);
        Session owner = client(b, TlsTestSupport.issued(caA, "t1", "rgA", "a1"), caA);
        owner.createProducer(owner.createQueue("T.t1.rgA_a1")).send(owner.createTextMessage("pending work"));
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(TlsTestSupport.issued(caB, "t2", "rgB", "x").keyManagers(), TlsTestSupport.trustManagers(caA.caCert()), new SecureRandom());
        org.apache.activemq.ActiveMQConnection other = (org.apache.activemq.ActiveMQConnection) f.createConnection();
        connections.add(other);
        other.start();
        String outcome;
        try { other.destroyDestination(new org.apache.activemq.command.ActiveMQQueue("T.t1.rgA_a1")); outcome = "destroyed"; }
        catch (Exception e) { outcome = "refused: " + e.getMessage(); }
        Message m = owner.createConsumer(owner.createQueue("T.t1.rgA_a1")).receive(2000);
        System.out.println("tenant t2 destroyDestination(T.t1.rgA_a1): " + outcome + "; owner then received " + (m == null ? null : ((TextMessage) m).getText()));
        assertNotNull(m, "verified tenant t2 deleted tenant t1's queue and its pending message");
    }

    // ------------------------------------------------------------------ NodeIdentityKeyManager: send order

    /** The claim: the node's certificate goes out only after the server was verified (TLS 1.2 and 1.3). */
    @Test
    void theNodeCertificateIsNeverSentToAServerThatFailedVerification() throws Exception {
        TlsTestSupport.provider();
        TlsTestSupport.Id server = TlsTestSupport.selfRooted("evil", "t1", "rgX", "gc");
        TlsTestSupport.Id node = TlsTestSupport.selfRooted("node", "t1", "rgA", "a1");
        TlsTestSupport.Id trusted = TlsTestSupport.selfRooted("good", "t1", "rgA", "rc");
        for (String proto : new String[]{"TLSv1.3", "TLSv1.2"}) {
            AtomicReference<X509Certificate[]> seen = new AtomicReference<>();
            X509TrustManager recordAll = new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] c, String a) { seen.set(c); }
                public void checkServerTrusted(X509Certificate[] c, String a) { }
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[]{trusted.chain[1]}; }   // hints do NOT name the node's issuer
            };
            SSLContext sctx = SSLContext.getInstance("TLS");
            sctx.init(server.keyManagers(), new TrustManager[]{recordAll}, new SecureRandom());
            try (SSLServerSocket ss = (SSLServerSocket) sctx.getServerSocketFactory().createServerSocket(0)) {
                ss.setNeedClientAuth(true);
                ss.setEnabledProtocols(new String[]{proto});
                Thread t = new Thread(() -> { try (SSLSocket s = (SSLSocket) ss.accept()) { s.startHandshake(); s.getInputStream().read(); } catch (Exception ignore) { } });
                t.start();
                KeyManager[] kms = node.keyManagers();
                kms[0] = new NodeIdentityKeyManager((X509KeyManager) kms[0], () -> "me");
                SSLContext cctx = SSLContext.getInstance("TLS");
                cctx.init(kms, TlsTestSupport.trustManagers(trusted.chain[1]), new SecureRandom());
                try (SSLSocket s = (SSLSocket) cctx.getSocketFactory().createSocket("127.0.0.1", ss.getLocalPort())) {
                    s.setEnabledProtocols(new String[]{proto});
                    s.startHandshake();
                    fail(proto + ": the client accepted an untrusted server");
                } catch (SSLException expected) { }
                t.join(5000);
            }
            System.out.println(proto + ": node certificate received by an unverified server = " + (seen.get() != null));
            assertNull(seen.get(), proto + ": the node's certificate reached a server the node had not verified");

            // and a server it DOES trust gets it even though its CA hints do not name the node's issuer (the round-4 claim)
            seen.set(null);
            sctx = SSLContext.getInstance("TLS");
            sctx.init(trusted.keyManagers(), new TrustManager[]{recordAll}, new SecureRandom());
            try (SSLServerSocket ss = (SSLServerSocket) sctx.getServerSocketFactory().createServerSocket(0)) {
                ss.setNeedClientAuth(true);
                ss.setEnabledProtocols(new String[]{proto});
                Thread t = new Thread(() -> { try (SSLSocket s = (SSLSocket) ss.accept()) { s.startHandshake(); } catch (Exception ignore) { } });
                t.start();
                KeyManager[] kms = node.keyManagers();
                kms[0] = new NodeIdentityKeyManager((X509KeyManager) kms[0], () -> "me");
                SSLContext cctx = SSLContext.getInstance("TLS");
                cctx.init(kms, TlsTestSupport.trustManagers(trusted.chain[1]), new SecureRandom());
                try (SSLSocket s = (SSLSocket) cctx.getSocketFactory().createSocket("127.0.0.1", ss.getLocalPort())) {
                    s.setEnabledProtocols(new String[]{proto});
                    s.startHandshake();
                    s.getOutputStream().write(1);
                } catch (Exception ignore) { }
                t.join(5000);
            }
            System.out.println(proto + ": node certificate presented to a verified server whose hints omit its issuer = " + (seen.get() != null));
            assertNotNull(seen.get(), proto + ": the node presented no certificate");
        }
    }
}
