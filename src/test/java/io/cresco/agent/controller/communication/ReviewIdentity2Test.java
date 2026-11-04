package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review, round 2, of the identity fixes on hardening-identity (controller ee48daa), 2026-11-04.
 * Each test asserts the SECURE / correct outcome; a failure is a finding.
 */
class ReviewIdentity2Test {

    private static final long DAY = 24L * 3600 * 1000;
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (BrokerService b : brokers) try { b.stop(); } catch (Exception ignore) { }
    }

    private static String fp(PublicKey k) {
        return PeerTrustRegistry.fingerprint(k.getEncoded());
    }

    private static String randomFp() {
        byte[] b = new byte[32];
        new SecureRandom().nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    /** A node on a GIVEN data directory (a restart, or a restore, of the same node). */
    private static ControllerEngine engineAt(Path dir, String region, String agent, boolean regional) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = regional;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static void bindAsController(ControllerEngine at, ControllerEngine who) {
        X509Certificate leaf = who.getCertificateManager().getPublicCertificate()[0];
        assertNull(at.getCertificateManager().getPeerTrust().pin(who.cstate.getRegion(), who.cstate.getAgent(), leaf,
                PeerTrustRegistry.ROLE_CONTROLLER, false));
    }

    private static MsgEvent ping(String srcRegion, String srcAgent, String dstRegion, String dstAgent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, srcRegion, srcAgent, null, dstRegion, dstAgent, null, true, false);
        m.setParam("action", "ping");
        return m;
    }

    /** One ping/pong between an agent {@code child} and its region {@code parent} (RegionalExecutor.pingReply). */
    private static void pingRound(ControllerEngine child, ControllerEngine parent) {
        MsgEvent p = ping(child.cstate.getRegion(), child.cstate.getAgent(), parent.cstate.getRegion(), parent.cstate.getAgent());
        RevocationSync.onChildPing(child, p);
        RevocationSync.onPingAtParent(parent, p, false);
        RevocationSync.onParentPong(child, p, parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    // ================================================================== revocation expiry

    /**
     * "Revocations of CA-enrolled keys expire": the revoking region sets exp = now + its CURRENT leaf lifetime (7 days) +
     * 2 x backdate + 1 day, assuming no leaf for that key outlives it. Leaves issued before the upgrade lived 3 years
     * (YEARS_VALID), and renewal keeps them until a year is left; an operator may also shorten identity_leaf_validity_hours.
     * Only the revoking region tombstones the name (only it binds the key). At the global -- and every other region -- the
     * entry is all there is: once it expires, the revoked agent's still-valid CA-issued leaf verifies there again.
     */
    @Test
    void anExpiredRevocationDoesNotReadmitALeafThatOutlivesIt() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        KeyPair a5 = TlsTestSupport.rsa();
        X509Certificate[] preUpgrade = caA.enrollFor(a5.getPublic(), "t1", "rgA", "a5", 3 * 365 * DAY);   // enrolled before the upgrade
        // at the region: its CA enrolled a5; the operator revokes it (stolen device)
        PeerTrustRegistry atRegion = new PeerTrustRegistry();
        atRegion.setIssuedLeafLifetime(168 * 3600_000L);
        assertTrue(atRegion.bind("t1", "rgA", "a5", a5.getPublic(), "enrolled").ok);
        assertNull(atRegion.revoke("rgA/a5", "operator: stolen device"));
        PeerTrustRegistry.Revocation r = atRegion.revocationsAfter(0, 10, true).get(0);
        assertTrue(r.exp > 0, "precondition: the region's revocation of a CA-enrolled key expires");
        // its list reaches the global, which records rgA's CA but binds none of rgA's agents
        PeerTrustRegistry atGlobal = new PeerTrustRegistry();
        assertNull(atGlobal.trustCa(caA.caCert(), "rgA", "t1", "federated"));
        atGlobal.mergeRevocations(List.of(PeerTrustRegistry.ListEntry.of(r)), "revocation list of region rgA/rc", "rgA", false);
        assertNull(atGlobal.verifiedIdentity(preUpgrade), "precondition: the revoked key is refused at the global");
        long later = r.exp + 60_000L;                                            // the hourly expiry pass, after exp
        atGlobal.expireRevocations(later);
        atRegion.expireRevocations(later);
        String[] why = new String[1];
        Object id = atGlobal.verifiedIdentity(preUpgrade, why);
        System.out.println("expiry: " + (r.exp - System.currentTimeMillis()) / DAY + " days after the revocation, the revoked a5's 3-year"
                + " leaf at the global -> " + (id == null ? "refused (" + why[0] + ")" : "ACCEPTED as " + id) + "; at its region -> "
                + atRegion.verifiedIdentity(preUpgrade, new String[1]));
        assertNull(id, "a revoked agent is accepted again at the global once the revocation expired: its CA-issued leaf outlives the"
                + " expiry computed from the region's current leaf lifetime, and only its own region tombstoned the name");
    }

    // ================================================================== signed delta cache

    /**
     * signedDelta caches the signed list by (epoch, from, upTo) with the certificate it was signed with, and nothing clears the
     * cache when this node's leaf is renewed. Under the regional CA a controller's leaf lives 7 days; when the leaf a cached
     * delta carries expires, every child that asks for the same position -- every NEW child, every child whose state was reset
     * (a restart) -- gets a list it must refuse ("not valid now"), and keeps asking for the same hot cache entry: no child that
     * starts from 0 receives the parent's revocations until the parent revokes something new or restarts.
     */
    @Test
    void aCachedSignedListIsNotServedAfterTheCertificateItCarriesExpired() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        ControllerEngine parent = node("rgA", "rc", true);
        CertificateManager pcm = parent.getCertificateManager();
        PublicKey pkey = pcm.getPublicCertificate()[0].getPublicKey();
        assertTrue(pcm.installEnrollment(pcm.getJsonFromCerts(caA.enrollFor(pkey, "t1", "rgA", "rc", 4000L))), "precondition: a leaf that expires in 4 s");
        String revokedFp = randomFp();
        assertNull(pcm.getPeerTrust().revoke(revokedFp, "operator"));
        ControllerEngine first = child(caA, parent, "a0");
        pingRound(first, parent);
        assertTrue(first.getCertificateManager().getPeerTrust().isRevokedKey(revokedFp), "precondition: a child receives the list");
        // the parent's leaf is renewed (same key), then the old one expires
        assertTrue(pcm.installEnrollment(pcm.getJsonFromCerts(caA.enrollFor(pkey, "t1", "rgA", "rc", 7 * DAY))));
        Thread.sleep(5000);
        ControllerEngine joinedLater = child(caA, parent, "a1");
        for (int i = 0; i < 3; i++) pingRound(joinedLater, parent);
        boolean got = joinedLater.getCertificateManager().getPeerTrust().isRevokedKey(revokedFp);
        System.out.println("cache: a child that joined after the parent's renewed leaf replaced an expired one received the list: " + got);
        assertTrue(got, "a child that joined later never receives its parent's revocation list: the cached signed list carries the"
                + " parent's expired leaf");
    }

    private static ControllerEngine child(RegionCA ca, ControllerEngine parent, String name) throws Exception {
        ControllerEngine c = node("rgA", name, false);
        assertNull(c.getCertificateManager().getPeerTrust().trustCa(ca.caCert(), "rgA", "t1", "joined"));
        bindAsController(c, parent);
        return c;
    }

    // ================================================================== positions reused after a restore

    /**
     * Positions are unique only while the registry is never rolled back. Restoring a controller's security/ directory from
     * a backup -- the documented way to bring back a re-installed controller without a rebind at every joiner -- restores the
     * same EPOCH with an older list. Its next revocations take positions its children already hold ("I hold epoch.3"): the
     * parent sends nothing for them (from >= durable), and they are never delivered. Nothing tells either side.
     */
    @Test
    void aRestoredRegistryDoesNotHideNewRevocationsFromChildren() throws Exception {
        Path dirP = Files.createTempDirectory("rvid2-parent");
        ControllerEngine p1 = engineAt(dirP, "rgA", "rc", true);
        PeerTrustRegistry reg1 = p1.getCertificateManager().getPeerTrust();
        String fp1 = randomFp(), fp2 = randomFp(), fp3 = randomFp(), fp4 = randomFp();
        assertNull(reg1.revoke(fp1, "operator"));
        Path backup = Files.createTempDirectory("rvid2-backup");
        copyDir(dirP.resolve("security"), backup);                                 // the nightly backup of security/
        assertNull(reg1.revoke(fp2, "operator"));
        assertNull(reg1.revoke(fp3, "operator"));
        ControllerEngine agent = node("rgA", "a1", false);
        bindAsController(agent, p1);
        pingRound(agent, p1);
        PeerTrustRegistry atAgent = agent.getCertificateManager().getPeerTrust();
        assertTrue(atAgent.isRevokedKey(fp3), "precondition: the agent holds the region's list");
        String epoch1 = reg1.revocationEpoch();
        // the controller is re-installed and its security/ directory restored from the backup
        p1.getCertificateManager().close();
        reg1.close();
        copyDir(backup, dirP.resolve("security"));
        ControllerEngine p2 = engineAt(dirP, "rgA", "rc", true);
        PeerTrustRegistry reg2 = p2.getCertificateManager().getPeerTrust();
        assertFalse(reg2.isSealed(), "precondition: the restored registry opens (" + reg2.sealReason() + ")");
        assertNull(reg2.revoke(fp4, "operator: revoked after the restore"));
        for (int i = 0; i < 3; i++) pingRound(agent, p2);
        System.out.println("restore: epoch before " + epoch1 + ", after " + reg2.revocationEpoch() + "; durable seq " + reg2.revocationSeq()
                + "; the agent received the new revocation: " + atAgent.isRevokedKey(fp4));
        assertTrue(atAgent.isRevokedKey(fp4), "a revocation made after the controller's registry was restored from a backup never reaches"
                + " the agent: the same epoch, and positions the agent already holds");
    }

    private static void copyDir(Path from, Path to) throws Exception {
        Files.createDirectories(to);
        try (java.util.stream.Stream<Path> s = Files.list(from)) {
            for (Path f : (Iterable<Path>) s::iterator) {
                if (Files.isRegularFile(f)) Files.copy(f, to.resolve(f.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    // ================================================================== clock skew alarms

    /**
     * onPingAtParent records the clock of ANY ping (rev_ts) under the name the ping claims (src_region/src_agent, written by
     * the sender), before and regardless of any verification, and a recorded skew is cleared only when that same name pings
     * again with a good clock. One ping from a name this node never bound -- or a skewed agent that is then decommissioned --
     * leaves this controller's 'identity' health CRITICAL for good (up to 1024 such names), masking real CRITICALs.
     */
    @Test
    void aPingFromAnUnknownNameCannotPinTheIdentityHealthCritical() throws Exception {
        ControllerEngine region = node("rgA", "rc", true);
        String before = region.getCertificateManager().identityHealth()[0];
        MsgEvent p = ping("rgA", "ghost", "rgA", "rc");
        p.setParam(RevocationSync.CLOCK, Long.toString(System.currentTimeMillis() + 3 * DAY));
        RevocationSync.onPingAtParent(region, p, false);
        String[] after = region.getCertificateManager().identityHealth();
        System.out.println("skew: identity health " + before + " -> " + after[0] + " (" + after[1] + ")");
        assertNotEquals("CRITICAL", after[0], "one ping claiming an unbound name with a skewed clock made this controller's identity"
                + " health CRITICAL (and nothing clears it unless that name pings again)");
    }

    // ================================================================== outbound connections and TLS resumption

    /**
     * Outbound connections are recorded for revocation only in PeerIdentityTrustManager.checkServerTrusted. A reconnect that
     * RESUMES its TLS session (TLS 1.3 session tickets: the JDK default for clients and servers) never calls the trust
     * manager, so the reconnected bridge is not recorded and a later revocation of its peer does not close it.
     */
    @Test
    void aBridgeThatReconnectedIsClosedWhenItsPeerIsRevoked() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id rc = TlsTestSupport.issued(caA, "t1", "rgA", "rc");
        ControllerEngine global = TlsTestSupport.engine("rgG", "gc", null);
        CertificateManager cm = global.getCertificateManager();
        assertNull(cm.getPeerTrust().trustCa(caA.caCert(), "rgA", "t1", "federated"));
        cm.addCertificatesToTrustStore("peerCA-rgA", new X509Certificate[]{caA.caCert()});
        assertTrue(cm.getPeerTrust().bind("t1", "rgA", "rc", rc.kp.getPublic(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        BrokerService region = TlsTestSupport.broker("rgA_rc", rc, false, null, null, caA.caCert());
        brokers.add(region);
        bridgeTo(cm, region);
        assertTrue(waitBridge(region, true, 10_000), "precondition: the global's bridge into rgA is up");
        int recordedFirst = cm.outboundConnections().size();
        // a network blip: the region drops the bridge's connection; the bridge reconnects
        for (org.apache.activemq.broker.Connection c : region.getRegionBroker().getClients()) {
            if (c.isNetworkConnection()) c.serviceExceptionAsync(new java.io.IOException("network blip"));
        }
        Thread.sleep(500);
        assertTrue(waitBridge(region, true, 10_000), "precondition: the bridge reconnected");
        Thread.sleep(500);
        int recordedAfter = cm.outboundConnections().size();
        assertNull(cm.getPeerTrust().revoke("rgA/rc", "operator: region rgA compromised"));
        boolean down = waitBridge(region, false, 4000);
        System.out.println("resumption: outbound connections recorded " + recordedFirst + " before the blip, " + recordedAfter
                + " after the reconnect; the reconnected bridge after the revocation is " + (down ? "closed" : "STILL UP"));
        assertTrue(down, "a bridge that reconnected (resumed TLS session) stays up after its peer was revoked");
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
                + "?verifyHostName=false&socketBufferSize=0)?maxReconnectAttempts=20&initialReconnectDelay=200&useExponentialBackOff=false"));
        nc.setName("bridge-" + System.nanoTime());
        nc.setDuplex(true);
        nc.setSslContext(ActiveBroker.bridgeSslContext(cm, new PeerIdentity.Expected("rgA", "rc"), "bridge -> 127.0.0.1:" + port));
        nc.start();
    }

    private static boolean waitBridge(BrokerService accepting, boolean up, long ms) throws Exception {
        long end = System.currentTimeMillis() + ms;
        do {
            boolean any = false;
            for (org.apache.activemq.broker.Connection c : accepting.getRegionBroker().getClients()) if (c.isNetworkConnection()) any = true;
            if (any == up) return true;
            Thread.sleep(50);
        } while (System.currentTimeMillis() < end);
        return false;
    }
}
