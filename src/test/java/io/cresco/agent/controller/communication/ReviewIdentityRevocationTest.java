package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of the 1.3 identity hardening, phases 0-2 (controller ea6a98e, library c64486e),
 * 2026-11-04. Each test asserts the SECURE / correct outcome; a failure is a finding. Taken unchanged from the review's
 * ReviewIdentityTest (local review/identity-20261104: 37387dc, 9a67317, 57ac95a, 1f9d893): the revocation, renewal and
 * controller-pinning findings, fixed on hardening-identity. (The composite-destination and tenant-name findings are
 * fixed with the broker work on hardening.)
 */
class ReviewIdentityRevocationTest {

    private static final long DAY = 24L * 3600 * 1000;
    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) try { b.stop(); } catch (Exception ignore) { }
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("rvid").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    private static String fp(PublicKey k) {
        return PeerTrustRegistry.fingerprint(k.getEncoded());
    }

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static void bindAsController(ControllerEngine at, ControllerEngine who) {
        X509Certificate leaf = who.getCertificateManager().getPublicCertificate()[0];
        assertNull(at.getCertificateManager().getPeerTrust().pin(who.cstate.getRegion(), who.cstate.getAgent(), leaf,
                PeerTrustRegistry.ROLE_CONTROLLER, false));
    }

    private static MsgEvent ping(String srcRegion, String srcAgent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, srcRegion, srcAgent, null, "rgG", "gc", null, true, true);
        m.setParam("action", "ping");
        return m;
    }

    // ================================================================== revocation

    /**
     * R1. Revocation drops the binding and refuses the KEY, but leaves the NAME free. Every agent holds the agent discovery
     * secret, so the thief of a revoked agent (who has its data directory) makes a new key and joins again under the same
     * name, first come, with no operator action -- and receives that name's queue and recipient-bound streams again. The
     * design says a revoked node "gets a new key and is bound again (operator rebind)".
     */
    @Test
    void aRevokedNameIsNotReclaimedByANewKeyWithoutAnOperatorRebind() throws Exception {
        PeerTrustRegistry r = open(newFile());
        TlsTestSupport.Id stolen = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", stolen.kp.getPublic(), "discovery").ok);
        assertNull(r.revoke("rgA/a1", "operator: stolen laptop"));
        KeyPair fresh = TlsTestSupport.rsa();
        PeerTrustRegistry.BindResult issueGate = r.canBind("t1", "rgA", "a1", fresh.getPublic());   // issueEnrollment's gate
        PeerTrustRegistry.BindResult again = r.bind("t1", "rgA", "a1", fresh.getPublic(), "discovery");
        System.out.println("R1: after revoking rgA/a1, canBind(new key)=" + issueGate.ok + " (" + issueGate.reason + "), bind(new key)="
                + again.ok + " (" + again.reason + ")");
        assertFalse(again.ok, "the revoked name rgA/a1 was bound again, first come, to a NEW key without any operator rebind");
    }

    /**
     * R1 + R2 under the regional CA. The global records region rgA's CA (federateController) and binds rgA/rc. The operator
     * revokes rgA/rc (region compromised). The compromised controller still holds its REGION CA key: it issues itself a
     * leaf for a fresh key under the same name, and the global's CA path accepts it (the binding was dropped, the name is
     * not reserved, the CA is not revoked). Its bridge-peer record is by name, so the bridge exemption comes back too.
     */
    @Test
    void aRevokedRegionControllerCannotReturnByMintingANewKeyUnderItsOwnCa() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id rc = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        PeerTrustRegistry atGlobal = new PeerTrustRegistry();
        assertNull(atGlobal.trustCa(caA.caCert(), "rgA", "t1", "federated"));
        assertTrue(atGlobal.bind("t1", "rgA", "rc", rc.kp.getPublic(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        atGlobal.allowBridgePeer("rgA", "rc");
        assertNull(atGlobal.revoke("rgA/rc", "operator: region rgA compromised"));
        assertNull(atGlobal.verifiedIdentity(rc.chain), "precondition: the revoked key is refused");
        TlsTestSupport.Id reminted = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        String[] why = new String[1];
        CrescoIdentity id = atGlobal.verifiedIdentity(reminted.chain, why);
        System.out.println("R1/R2: re-minted rgA/rc under rgA's own CA -> " + id + " (" + why[0] + "); bridge peer="
                + atGlobal.isBridgePeer(id));
        assertNull(id, "a revoked region controller came back at once: it minted itself a new key for rgA/rc under its own"
                + " region CA and the global verified it (and it is still a bridge peer: " + atGlobal.isBridgePeer(id) + ")");
    }

    /**
     * R2. "A revoked key is refused everywhere" -- except as an ISSUER. Revoking a region CA's key fingerprint (a leaked CA
     * key) changes nothing on the CA path: verifiedIdentity checks only the leaf key, and recordedCas() still hands the CA
     * to the truststore at every start.
     */
    @Test
    void revokingARegionCaKeyStopsItVouchingForItsRegion() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry r = new PeerTrustRegistry();
        assertNull(r.trustCa(caA.caCert(), "rgA", "t1", "federated"));
        String caFp = fp(caA.caCert().getPublicKey());
        assertNull(r.revoke(caFp, "operator: rgA's CA key leaked"));
        assertTrue(r.isRevokedKey(caFp), "precondition: the CA key is on the revocation list");
        TlsTestSupport.Id minted = TlsTestSupport.issued(caA, "t1", "rgA", "a7");
        CrescoIdentity id = r.verifiedIdentity(minted.chain);
        boolean stillRecorded = r.recordedCas().stream().anyMatch(c -> fp(c.getPublicKey()).equals(caFp));
        System.out.println("R2: leaf minted by a REVOKED CA key -> " + id + "; CA still handed to the truststore at start: " + stillRecorded);
        assertNull(id, "a leaf issued by a REVOKED region CA key still verifies on the CA path");
        assertFalse(stillRecorded, "a revoked CA is still a recorded CA (re-added as a TLS anchor at every start)");
    }

    /**
     * R3. A region's list at the global is scoped by "a key bound HERE to a name outside that region is not revoked". Keys
     * the global does not bind -- every agent of every OTHER region (bound at its own region only), CA keys -- pass the
     * scope check, are revoked at the global, and travel down with the global's list (which applies to every name) to the
     * victim region. One region (compromised, or an operator typo there) revokes another region's agents fabric-wide,
     * permanently.
     */
    @Test
    void aRegionsListCannotRevokeTheAgentsOfAnotherRegion() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionA = node("rgA", "rc", true), regionB = node("rgB", "rc", true);
        bindAsController(global, regionA);
        bindAsController(global, regionB);
        bindAsController(regionA, global);
        bindAsController(regionB, global);
        TlsTestSupport.Id b1 = TlsTestSupport.selfRooted("b1", "t1", "rgB", "b1");
        PeerTrustRegistry atB = regionB.getCertificateManager().getPeerTrust();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.kp.getPublic(), "discovery").ok);          // b1 joined region rgB
        String b1fp = fp(b1.kp.getPublic());
        assertNull(regionA.getCertificateManager().getPeerTrust().revoke(b1fp, "operator at rgA"));
        MsgEvent pa = ping("rgA", "rc");
        RevocationSync.onChildPing(regionA, pa);
        RevocationSync.onPingAtParent(global, pa, true);
        MsgEvent pb = ping("rgB", "rc");
        RevocationSync.onChildPing(regionB, pb);
        RevocationSync.onPingAtParent(global, pb, true);
        RevocationSync.onParentPong(regionB, pb, "rgG", "gc");
        boolean atGlobal = global.getCertificateManager().getPeerTrust().isRevokedKey(b1fp);
        CrescoIdentity stillB1 = atB.verifiedIdentity(b1.chain);
        System.out.println("R3: rgA's list named rgB/b1's key -> revoked at the global=" + atGlobal + ", rgB/b1 at rgB=" + stillB1);
        assertFalse(atGlobal, "the global accepted region rgA's revocation of a key it does not bind (rgB's agent b1)");
        assertNotNull(stillB1, "region rgA revoked region rgB's agent b1 at rgB itself, through the global's list");
    }

    /**
     * R4. "Agents' lists are not accepted" -- but the global's ping handler passes childIsRegion=true for every ping, and
     * takes the issuer from the ping's own src_region/src_agent. Under the regional CA the global records rgA's CA, so
     * ANY agent of rgA verifies there as itself. An agent sends the global a 'ping' (an ordinary EXEC action, open to its
     * tenant, routed by its region like any global call) carrying its own signed list: the global merges it with rgA's
     * scope, revoking rgA's CONTROLLER (bound at the global as rgA/rc, inside the scope). The global closes rgA's bridge,
     * refuses it from then on, and sends the revocation to every other region: one agent evicts its region, permanently.
     */
    @Test
    void anAgentCannotSubmitARevocationListAtTheGlobal() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        ControllerEngine global = node("rgG", "gc", true);
        PeerTrustRegistry atGlobal = global.getCertificateManager().getPeerTrust();
        TlsTestSupport.Id rc = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        assertNull(atGlobal.trustCa(caA.caCert(), "rgA", "t1", "federated"));
        assertTrue(atGlobal.bind("t1", "rgA", "rc", rc.kp.getPublic(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        ControllerEngine agent = node("rgA", "a1", false);
        TlsTestSupport.enroll(agent, caA, "t1");                               // an ordinary agent, enrolled by rgA's CA
        assertNull(agent.getCertificateManager().getPeerTrust().revoke(fp(rc.kp.getPublic()), "a malicious agent"));
        String list = RevocationSync.signOwn(agent);
        assertNotNull(list, "precondition: the agent signs its list");
        MsgEvent p = ping("rgA", "a1");
        p.setCompressedParam(RevocationSync.CHILD_LIST, list);
        RevocationSync.onPingAtParent(global, p, true);                        // GlobalExecutor.pingReply
        CrescoIdentity rcAtGlobal = atGlobal.verifiedIdentity(rc.chain);
        System.out.println("R4: an AGENT's list at the global -> rgA/rc " + (rcAtGlobal == null ? "REVOKED" : "still verified")
                + "; ack=" + p.getParam(RevocationSync.ACK));
        assertNotNull(rcAtGlobal, "an AGENT's revocation list was accepted at the global and revoked its own region controller");
    }

    /** R5 (fail-open). On a full disk the operator's revocation of a stolen key is not enforced at all, not even in memory. */
    @Test
    void aRevocationIsEnforcedEvenWhenItCannotBeRecorded() throws Exception {
        PeerTrustRegistry r = open(newFile());
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        r.injectWriteFailures(1, false);                                        // ENOSPC part-way through the write
        String why = r.revoke("rgA/a1", "operator: stolen laptop");
        CrescoIdentity id = r.verifiedIdentity(a1.chain);
        System.out.println("R5: revoke on a full disk returned '" + why + "'; the stolen key verifies: " + id);
        assertNull(id, "a revocation that could not be written is not enforced in memory either (fail-open; compare"
                + " revokeBridgePeer, which refuses first and records second)");
    }

    /**
     * R5b. The operator file is re-read only when its mtime changes, and the mtime is remembered BEFORE its entries are
     * applied: one failed write (full disk, EIO) and that revocation is silently dropped until someone touches the file
     * again or restarts the node.
     */
    @Test
    void anOperatorFileRevocationThatHitAFailedWriteIsRetried() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        r.injectWriteFailures(1, false);
        Files.writeString(f.resolveSibling("peer-trust.json.revoke"), "# stolen laptop\nrgA/a1\n");
        long end = System.currentTimeMillis() + 25_000;                         // two re-reads (every 10 s); the disk recovers after one
        while (r.verifiedIdentity(a1.chain) != null && System.currentTimeMillis() < end) Thread.sleep(500);
        assertNull(r.verifiedIdentity(a1.chain), "the operator file's revocation was lost after one failed write: the file is"
                + " not re-read until its mtime changes, and the stolen key stays trusted");
    }

    /**
     * R6. Revocation closes INBOUND connections (CrescoAuthorizationBroker.closeConnectionsOfKey, ActiveBroker server TLS
     * sessions) but nothing re-checks a connection THIS node opened. The global's own (duplex) bridge into a revoked
     * region stays up and keeps forwarding both ways; the trust manager is consulted only at the next handshake.
     */
    @Test
    void revokingAControllerTearsDownTheBridgeThisNodeOpenedToIt() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id rc = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        ControllerEngine global = TlsTestSupport.engine("rgG", "gc", null);
        CertificateManager cm = global.getCertificateManager();
        assertNull(cm.getPeerTrust().trustCa(caA.caCert(), "rgA", "t1", "federated"));
        cm.addCertificatesToTrustStore("peerCA-rgA", new X509Certificate[]{caA.caCert()});
        assertTrue(cm.getPeerTrust().bind("t1", "rgA", "rc", rc.kp.getPublic(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        BrokerService regionBroker = TlsTestSupport.broker("rgA_rc", rc, false, null, null, caA.caCert());
        brokers.add(regionBroker);
        bridgeTo(cm, regionBroker);
        assertTrue(hasBridge(regionBroker, 10_000), "precondition: the global's bridge into rgA is up");
        assertNull(cm.getPeerTrust().revoke("rgA/rc", "operator: region rgA compromised"));
        Thread.sleep(3000);
        boolean stillUp = hasBridge(regionBroker, 100);
        System.out.println("R6: 3 s after revoking rgA/rc at the global, the global's own bridge into rgA is " + (stillUp ? "STILL UP" : "down"));
        assertFalse(stillUp, "the bridge this node opened to a controller it has just revoked is still up");
    }

    private void bridgeTo(CertificateManager cm, BrokerService remote) throws Exception {
        BrokerService local = new BrokerService();
        local.setBrokerName("rgG_gc-" + System.nanoTime());
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
        nc.setSslContext(ActiveBroker.bridgeSslContext(cm, new PeerIdentity.Expected("rgA", "rc"), "bridge -> 127.0.0.1:" + port));
        nc.start();
    }

    private static boolean hasBridge(BrokerService accepting, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        do {
            for (org.apache.activemq.broker.Connection c : accepting.getRegionBroker().getClients()) {
                if (c.isNetworkConnection()) return true;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < end);
        return false;
    }

    // ================================================================== renewal and re-enrollment

    /**
     * M4 floor bug. chainNeedsRenewal renews when at most max(identity_renew_before_days = 30 days, life/3) is left. A
     * region-issued leaf lives 7 days, so it ALWAYS has less than 30 days left: it is "due" the moment it is issued.
     */
    @Test
    void aFreshSevenDayLeafIsNotDueForRenewalUnderTheDefaults() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        X509Certificate[] leaf = ca.enrollFor(TlsTestSupport.rsa().getPublic(), "t1", "rgA", "a1", 168 * 3600_000L);
        assertFalse(CertificateManager.chainNeedsRenewal(leaf, System.currentTimeMillis(), 30 * DAY),
                "a 7-day leaf issued a moment ago is due for renewal under the default identity_renew_before_days=30");
    }

    /**
     * The consequence at a region controller: renewIfDue (every identity_renew_check_minutes = 60) re-issues its own leaf
     * at EVERY check, adds the new leaf as one more truststore entry (aliases are per fingerprint, never removed), rewrites
     * the key and trust stores, and re-initialises the broker's and every bridge's SSL context: 24 times a day, forever.
     */
    @Test
    void aRegionControllerDoesNotReissueItsOwnLeafAtEveryRenewalCheck() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("security_regional_ca", "true"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        CertificateManager cm = region.getCertificateManager();
        cm.ensureRegionCA();
        int before = trustStoreSize(cm);
        int renewed = 0;
        for (int i = 0; i < 5; i++) if (cm.renewIfDue()) renewed++;           // five hourly checks
        int after = trustStoreSize(cm);
        System.out.println("renewal: a fresh region controller renewed " + renewed + "/5 checks; truststore " + before + " -> " + after + " entries");
        assertEquals(0, renewed, "a region controller re-issued its own (fresh) leaf at every hourly check");
        assertEquals(before, after, "every renewal check left one more truststore entry behind");
    }

    /**
     * The consequence at every agent: its region-issued 7-day leaf is "due" at every hourly check, so it re-enrolls every
     * hour; each successful re-enrollment runs installEnrollment, which calls ActiveClient.refreshTrust -- closing EVERY
     * network connection of the agent (control plane, dataplane shards, listeners). Fleet-wide, hourly.
     */
    @Test
    void anEnrolledAgentDoesNotReEnrollAtEveryRenewalCheck() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        AtomicInteger reEnrolls = new AtomicInteger();
        ControllerEngine agent = countingEngine("rgA", "a1", reEnrolls);
        CertificateManager cm = agent.getCertificateManager();
        X509Certificate[] week = ca.enrollFor(cm.getPublicCertificate()[0].getPublicKey(), "t1", "rgA", "a1", 168 * 3600_000L);
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(week)), "precondition: the region-issued leaf is installed");
        for (int i = 0; i < 3; i++) cm.renewIfDue();                            // three hourly checks
        System.out.println("renewal: an agent with a 7-day leaf issued a moment ago re-enrolled " + reEnrolls.get() + "/3 checks");
        assertEquals(0, reEnrolls.get(), "an agent re-enrolled (and so would close all its network connections) at every hourly check");
    }

    private static ControllerEngine countingEngine(String region, String agent, AtomicInteger reEnrolls) throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("rvid-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null) {
            @Override public boolean reEnroll() { reEnrolls.incrementAndGet(); return true; }
        };
        e.cstate = TlsTestSupport.state(pb, region, agent);
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static int trustStoreSize(CertificateManager cm) throws Exception {
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("trustStore");
        f.setAccessible(true);
        return ((java.security.KeyStore) f.get(cm)).size();
    }

    // ================================================================== controller pinning

    /**
     * Item 7 lapses. A joiner pins its controller's key as a ROLE_CONTROLLER binding, and prune() removes every binding
     * but this node's own after peer_binding_prune_days (90) unseen -- without leaving the old key refused for the name.
     * An agent that was off for a season (edge, seasonal, a lab shelf) starts, prunes its pin, and joins whoever answers
     * first again: trust on first use, once more, for any agent-secret holder. (Per-node trust; under the regional CA the
     * pin is the CA, which is not pruned.)
     */
    @Test
    void aJoinersControllerPinDoesNotLapseAfterNinetyDaysWithoutContact() throws Exception {
        PeerTrustRegistry r = open(newFile());
        TlsTestSupport.Id realRc = TlsTestSupport.selfRooted("rc", "t1", "rgA", "rc");
        assertNull(r.pin("rgA", "rc", realRc.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, false));   // learnPeer at the first join
        r.prune(System.currentTimeMillis() + 91 * DAY);                                                   // what the next start does
        TlsTestSupport.Id impostor = TlsTestSupport.selfRooted("imp", "t1", "rgA", "rc");
        String why = r.pin("rgA", "rc", impostor.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, false);
        System.out.println("pin: after 91 days unseen, an impostor key for the joined controller rgA/rc -> " + (why == null ? "PINNED" : why));
        assertNotNull(why, "the controller this node joined was pinned, unseen for 91 days, then a different key was pinned for it first come");
    }

    // ================================================================== scale of revocation lists

    private static List<String> randomFingerprints(int n) {
        SecureRandom r = new SecureRandom();
        List<String> out = new ArrayList<>();
        byte[] b = new byte[32];
        for (int i = 0; i < n; i++) { r.nextBytes(b); out.add(java.util.HexFormat.of().formatHex(b)); }
        return out;
    }

    private static double msPerAgentPing(int revocations) throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true", "peer_trust_persist", "false"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        region.getCertificateManager().getPeerTrust().mergeRevocations(randomFingerprints(revocations), "test", null);
        String have = RevocationSync.idOf(new ArrayList<>(region.getCertificateManager().getPeerTrust().revocations().keySet()));
        int pings = 300;
        long t0 = System.nanoTime();
        for (int i = 0; i < pings; i++) {
            MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a" + i, null, "rgA", "rc", null, true, false);
            p.setParam("action", "ping");
            p.setParam(RevocationSync.HAVE, have);                              // the agent already holds the region's list
            RevocationSync.onPingAtParent(region, p, false);                     // RegionalExecutor.pingReply
        }
        return (System.nanoTime() - t0) / 1e6 / pings;
    }

    /**
     * Scale gate. Every agent ping (default every 5 s: 2,000 pings/s at a 10k-agent region) recomputes the id of the
     * region's WHOLE revocation list: revocations() copies it into a TreeMap and idOf() hashes it, even when the agent
     * already holds it. The list only grows (revocation is permanent and never expires). Measured here: 100 -> ~0.03 ms,
     * 10,000 -> ~1.6 ms (3 cores at 10k agents), 50,000 -> ~12 ms (24 cores).
     */
    @Test
    void anAgentPingDoesNotCostMoreAsTheRevocationListGrows() throws Exception {
        msPerAgentPing(100);                                                     // warm up
        double small = msPerAgentPing(100), large = msPerAgentPing(10_000);
        System.out.printf("scale: per agent ping %.3f ms with 100 revocations, %.3f ms with 10,000%n", small, large);
        assertTrue(large < Math.max(5 * small, 0.2), "the per-ping cost grows with the revocation list: " + large + " ms per ping"
                + " with 10,000 revocations (x 2,000 pings/s at a 10k-agent region)");
    }

    /**
     * Scale gate. mergeRevocations appends and fsyncs ONE record per key, holding the registry write lock throughout (no
     * bind, enrollment or revocation proceeds meanwhile), and every new joiner merges the whole parent list on its first
     * pong. Measured on APFS: ~8 ms per key, 1,000 keys ~8 s (the DGX shared /project storage is 10-18x slower to fsync).
     */
    @Test
    void mergingARevocationListIsOneDurableWriteNotOnePerKey() throws Exception {
        PeerTrustRegistry r = open(newFile());
        List<String> keys = randomFingerprints(1000);
        long t0 = System.nanoTime();
        int added = r.mergeRevocations(keys, "the parent's revocation list", null);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("scale: merging 1,000 revocations took " + ms + " ms with the registry write lock held");
        assertEquals(1000, added);
        assertTrue(ms < 1000, "merging a 1,000-key revocation list took " + ms + " ms (one fsync per key, write lock held)");
    }

    // ================================================================== design claims

    /**
     * "Short-lived leaves: an identity that is no longer re-enrolled (a node gone, or refused) lapses on its own"
     * (CertificateManager.leafValidityMs). Under the regional CA the region ALSO binds the enrolled key
     * (issueEnrollment -> bind), and the bound-key path accepts ANY chain whose leaf carries that key -- including one the
     * key holder minted itself, self-rooted, with any validity it likes (renewIfDue does exactly that, for 3 years, when a
     * re-enrollment fails). Expiry of the region-issued leaf therefore revokes nothing; only an explicit revocation does.
     */
    @Test
    void aRegionIssuedIdentityLapsesWhenItIsNotReEnrolled() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry atRegion = new PeerTrustRegistry();
        assertNull(atRegion.trustCa(ca.caCert(), "rgA", "t1", "own"));
        KeyPair agentKey = TlsTestSupport.rsa();
        ca.enrollFor(agentKey.getPublic(), "t1", "rgA", "a1", 168 * 3600_000L);          // what issueEnrollment signs...
        assertTrue(atRegion.bind("t1", "rgA", "a1", agentKey.getPublic(), "enrolled").ok); // ...and records
        // the region stops re-enrolling a1 (refused, or it is gone and its key was stolen): the holder mints its own chain
        X509Certificate[] selfMade = selfRootedFor(agentKey, "t1", "rgA", "a1", 3 * 365 * DAY);
        CrescoIdentity id = atRegion.verifiedIdentity(selfMade);
        System.out.println("lapse: a self-minted 3-year chain for an enrolled key verifies at its region as " + id);
        assertNull(id, "a region-enrolled key is accepted on a chain its holder minted itself: the 7-day leaf lapses nothing");
    }

    private static X509Certificate[] selfRootedFor(KeyPair leafKey, String tenant, String region, String agent, long validMs) throws Exception {
        KeyPair rootKp = TlsTestSupport.rsa();
        org.bouncycastle.asn1.x500.X500Name rn = new org.bouncycastle.asn1.x500.X500Name("CN=rootCA-self");
        long now = System.currentTimeMillis();
        org.bouncycastle.operator.ContentSigner s = new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA384withRSA").build(rootKp.getPrivate());
        X509Certificate root = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(
                new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(rn, java.math.BigInteger.ONE, new Date(now - 60_000), new Date(now + validMs), rn, rootKp.getPublic())
                        .addExtension(org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new org.bouncycastle.asn1.x509.BasicConstraints(true)).build(s));
        X509Certificate leaf = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(
                new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(rn, java.math.BigInteger.TWO, new Date(now - 60_000), new Date(now + validMs),
                        new org.bouncycastle.asn1.x500.X500Name(CrescoIdentity.of(tenant, region, agent, null).toX500Name()), leafKey.getPublic())
                        .addExtension(org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new org.bouncycastle.asn1.x509.BasicConstraints(false)).build(s));
        return new X509Certificate[]{leaf, root};
    }

    /**
     * revoke("region/agent") refuses this node's own identity ("is this node's own identity"), but revoke(<fingerprint>)
     * of the same key is accepted: the node then refuses its own certificate and its signed list carries its own key to
     * every child, which revokes its controller (onParentPong merges a parent's list for every name).
     */
    @Test
    void aNodeCannotRevokeItsOwnKeyByFingerprint() throws Exception {
        PeerTrustRegistry r = new PeerTrustRegistry();
        TlsTestSupport.Id me = TlsTestSupport.selfRooted("me", "t1", "rgA", "rc");
        assertTrue(r.bind("t1", "rgA", "rc", me.kp.getPublic(), "own", PeerTrustRegistry.ROLE_SELF, true).ok);
        assertNotNull(r.revoke("rgA/rc", "operator"), "precondition: revoking the own NAME is refused");
        String why = r.revoke(fp(me.kp.getPublic()), "operator typo in peer-trust.json.revoke");
        System.out.println("self-revocation by fingerprint -> " + (why == null ? "ACCEPTED" : why) + "; own key now in the list: "
                + r.revocations().containsKey(fp(me.kp.getPublic())));
        assertNotNull(why, "this node's own key was revoked by fingerprint (refused by name, accepted by fingerprint)");
    }

    /**
     * A child stops sending its list once the parent acknowledged that list id (RevocationSync CHILD_STATE[1]); the
     * parent's acknowledgements live only in memory and a missing ACK on a later pong does not clear the child's state.
     * A global whose registry is reset (the documented 'reset' recovery) or re-installed never learns the regions'
     * revocations again -- and so never sends them to the other regions -- until each region restarts or revokes
     * something new.
     */
    @Test
    void aParentThatLostItsRegistryLearnsTheChildsRevocationsAgain() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), region = node("rgB", "rc", true);
        bindAsController(global, region);
        bindAsController(region, global);
        TlsTestSupport.Id b1 = TlsTestSupport.selfRooted("b1", "t1", "rgB", "b1");
        assertTrue(region.getCertificateManager().getPeerTrust().bind("t1", "rgB", "b1", b1.kp.getPublic(), "discovery").ok);
        assertNull(region.getCertificateManager().getPeerTrust().revoke("rgB/b1", "operator"));
        MsgEvent p1 = ping("rgB", "rc");
        RevocationSync.onChildPing(region, p1);
        RevocationSync.onPingAtParent(global, p1, true);
        RevocationSync.onParentPong(region, p1, "rgG", "gc");                       // acknowledged
        String b1fp = fp(b1.kp.getPublic());
        assertTrue(global.getCertificateManager().getPeerTrust().isRevokedKey(b1fp), "precondition: the global merged the list");
        // the global's registry is reset (or the global re-installed): a fresh registry that knows the region again
        ControllerEngine global2 = node("rgG", "gc", true);
        bindAsController(global2, region);
        for (int i = 0; i < 3; i++) {
            MsgEvent p = ping("rgB", "rc");
            RevocationSync.onChildPing(region, p);
            RevocationSync.onPingAtParent(global2, p, true);
            RevocationSync.onParentPong(region, p, "rgG", "gc");
        }
        assertTrue(global2.getCertificateManager().getPeerTrust().isRevokedKey(b1fp),
                "after three pings the reset global still does not hold region rgB's revocation: the region never sends it again");
    }

    /**
     * What every re-enrollment does to a connected agent: installEnrollment calls ActiveClient.refreshTrust, which closes
     * every NETWORK connection the agent holds (pooled and dedicated: control plane, dataplane shards), although the key is
     * unchanged and NodeIdentityKeyManager already presents the current chain on the next handshake. With the renewal
     * floor (above) a multicast-discovered agent re-enrolls at every hourly check: its connections drop every hour.
     */
    @Test
    void aReEnrollmentWithTheSameKeyDoesNotDropTheAgentsLiveConnections() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        BrokerService region = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(ca, "t1", "rgA", "rc"), false, null, null, ca.caCert());
        brokers.add(region);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = agent.getCertificateManager();
        PublicKey key = cm.getPublicCertificate()[0].getPublicKey();
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(ca.enrollFor(key, "t1", "rgA", "a1", 168 * 3600_000L))), "precondition: enrolled");
        String uri = TlsTestSupport.agentUri(TlsTestSupport.port(region));
        agent.getActiveClient().expectPeer(uri, "rgA", "rc");
        org.apache.activemq.ActiveMQSession s = agent.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true);
        assertNotNull(s, "precondition: the agent is connected to its region");
        org.apache.activemq.ActiveMQConnection c = (org.apache.activemq.ActiveMQConnection) s.getConnection();
        // the hourly check re-enrolls: the region issues a fresh 7-day leaf for the SAME key
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(ca.enrollFor(key, "t1", "rgA", "a1", 168 * 3600_000L))));
        System.out.println("re-enrollment: the agent's live connection to its region is " + (c.isClosed() ? "CLOSED" : "still open"));
        assertFalse(c.isClosed(), "a same-key re-enrollment closed the agent's live connection to its region");
    }

    /**
     * R4 without the regional CA. The global is also the region controller of its own agents (glob/a1 in the live harness),
     * and binds them by key. Such an agent pings the global as a "region" (GlobalExecutor.pingReply passes
     * childIsRegion=true) with its own signed list; the list verifies as glob/a1, takes the scope of region glob -- and
     * every key the global does not bind (all agents of every other region) passes that scope check. One agent of the
     * global's region revokes an agent of region rgB, at the global and then at rgB itself.
     */
    @Test
    void anAgentOfTheGlobalsOwnRegionCannotRevokeAnotherRegionsAgent() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), regionB = node("rgB", "rc", true), agent = node("rgG", "a9", false);
        bindAsController(global, regionB);
        bindAsController(regionB, global);
        PeerTrustRegistry atGlobal = global.getCertificateManager().getPeerTrust();
        X509Certificate a9 = agent.getCertificateManager().getPublicCertificate()[0];
        assertTrue(atGlobal.bind("t1", "rgG", "a9", a9.getPublicKey(), "discovery").ok);        // a9 joined the global's region
        TlsTestSupport.Id b1 = TlsTestSupport.selfRooted("b1", "t1", "rgB", "b1");
        PeerTrustRegistry atB = regionB.getCertificateManager().getPeerTrust();
        assertTrue(atB.bind("t1", "rgB", "b1", b1.kp.getPublic(), "discovery").ok);
        assertNull(agent.getCertificateManager().getPeerTrust().revoke(fp(b1.kp.getPublic()), "a malicious agent"));
        MsgEvent p = ping("rgG", "a9");
        p.setCompressedParam(RevocationSync.CHILD_LIST, RevocationSync.signOwn(agent));
        RevocationSync.onPingAtParent(global, p, true);                                          // GlobalExecutor.pingReply
        MsgEvent pb = ping("rgB", "rc");
        RevocationSync.onChildPing(regionB, pb);
        RevocationSync.onPingAtParent(global, pb, true);
        RevocationSync.onParentPong(regionB, pb, "rgG", "gc");
        System.out.println("R4 (per-node trust): agent rgG/a9's list -> rgB/b1 at the global revoked=" + atGlobal.isRevokedKey(fp(b1.kp.getPublic()))
                + ", at rgB=" + (atB.verifiedIdentity(b1.chain) == null ? "REVOKED" : "verified"));
        assertNotNull(atB.verifiedIdentity(b1.chain), "an agent of the global's own region revoked region rgB's agent b1 (at the global and at rgB)");
    }
}
