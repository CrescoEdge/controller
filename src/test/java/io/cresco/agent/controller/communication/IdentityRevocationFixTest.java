package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The fixes for the identity review of 2026-11-04 beyond the review's own tests: positive revocation scope, versioned
 * lists, fail-closed durability, expiry, key-bound rebind tokens, vouched controller re-keys, the registry lock alarm,
 * clock skew and the renewal fallback.
 */
class IdentityRevocationFixTest {

    private static final long DAY = 24L * 3600 * 1000;

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("idfix").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    private static String fp(java.security.PublicKey k) {
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

    private static List<String> randomFingerprints(int n) {
        SecureRandom r = new SecureRandom();
        List<String> out = new ArrayList<>();
        byte[] b = new byte[32];
        for (int i = 0; i < n; i++) { r.nextBytes(b); out.add(java.util.HexFormat.of().formatHex(b)); }
        return out;
    }

    // ------------------------------------------------------------------ positive scope

    /** A child region's entry applies to that region's names only: the same key under another region's name is untouched. */
    @Test
    void aChildRegionsRevocationAppliesOnlyToItsOwnNames() {
        PeerTrustRegistry atGlobal = new PeerTrustRegistry();
        String k = randomFingerprints(1).get(0);
        int n = atGlobal.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(k, null, "rgB/b1", 0)), "region rgB", "rgB", false);
        assertEquals(1, n, "an entry naming an identity of the child's own region was refused");
        assertTrue(atGlobal.isRevokedFor(k, "rgB"));
        assertFalse(atGlobal.isRevokedFor(k, "rgA"), "a region's revocation applied to another region's names");
        // an entry naming another region's identity, or no identity at all, is refused
        String k2 = randomFingerprints(1).get(0), k3 = randomFingerprints(1).get(0);
        assertEquals(0, atGlobal.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(k2, null, "rgA/a1", 0),
                new PeerTrustRegistry.ListEntry(k3, null, null, 0)), "region rgB", "rgB", false));
        assertFalse(atGlobal.isRevokedKey(k2));
        assertFalse(atGlobal.isRevokedKey(k3));
    }

    /** A region's list never revokes a CA key. */
    @Test
    void aChildRegionsListCannotRevokeACa() throws Exception {
        TlsTestSupport.provider();
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry atGlobal = new PeerTrustRegistry();
        assertNull(atGlobal.trustCa(caA.caCert(), "rgA", "t1", "federated"));
        String caFp = fp(caA.caCert().getPublicKey());
        assertEquals(0, atGlobal.mergeRevocations(List.of(new PeerTrustRegistry.ListEntry(caFp, null, "rgA/x", 0)), "region rgA", "rgA", false));
        assertNotNull(atGlobal.verifiedIdentity(TlsTestSupport.issued(caA, "t1", "rgA", "a1").chain));
    }

    // ------------------------------------------------------------------ versioned lists

    /** The parent's list comes down in chunks; once held, a ping costs nothing (no list) on the epoch.seq path. */
    @Test
    void aLongListComesDownInChunksAndThenStaysQuiet() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true", "peer_trust_persist", "false",
                "revocation_sync_max_entries", "1000"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", Map.of("broker_security_enabled", "true", "peer_trust_persist", "false"));
        bindAsController(agent, region);
        region.getCertificateManager().getPeerTrust().mergeRevocations(randomFingerprints(2500), "global", null);
        int pongs = 0, withList = 0;
        for (int i = 0; i < 6; i++) {
            MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
            RevocationSync.onChildPing(agent, p);
            RevocationSync.onPingAtParent(region, p, false);
            if (p.getParam(RevocationSync.PARENT_LIST) != null) withList++;
            RevocationSync.onParentPong(agent, p, "rgA", "rc");
            pongs++;
        }
        assertEquals(3, withList, "2,500 entries at 1,000 per pong should take exactly three pongs");
        assertEquals(2500, agent.getCertificateManager().getPeerTrust().revocations().size());
        // steady state: what one ping costs with 2,500 entries held
        long t0 = System.nanoTime();
        for (int i = 0; i < 2000; i++) {
            MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
            RevocationSync.onChildPing(agent, p);
            RevocationSync.onPingAtParent(region, p, false);
            assertNull(p.getParam(RevocationSync.PARENT_LIST));
            RevocationSync.onParentPong(agent, p, "rgA", "rc");
        }
        double ms = (System.nanoTime() - t0) / 1e6 / 2000;
        System.out.printf("versioned lists: %d pongs to fetch 2,500 entries; then %.4f ms per ping/pong round%n", pongs, ms);
        assertTrue(ms < 0.5, "a ping/pong round costs " + ms + " ms with nothing to send");
    }

    /**
     * Under the regional CA a joiner pins its region's CA, not its controller's key: it accepts the region's list when the
     * signer verifies by that CA as the controller it joined -- and not a list another CA-issued identity of the region signs.
     */
    @Test
    void underTheRegionalCaAnAgentAcceptsItsRegionsListByTheRegionCa() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true", "security_regional_ca", "true",
                "peer_trust_persist", "false"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        region.getCertificateManager().ensureRegionCA();
        X509Certificate regionCa = region.getCertificateManager().getPublicCertificate()[1];
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", Map.of("broker_security_enabled", "true", "peer_trust_persist", "false"));
        PeerTrustRegistry atAgent = agent.getCertificateManager().getPeerTrust();
        assertNull(atAgent.trustCa(regionCa, "rgA", "t1", "joined"));
        String victim = randomFingerprints(1).get(0);
        assertNull(region.getCertificateManager().getPeerTrust().revoke(victim, "operator"));
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        RevocationSync.onChildPing(agent, p);
        RevocationSync.onPingAtParent(region, p, false);
        assertNotNull(p.getParam(RevocationSync.PARENT_LIST));
        RevocationSync.onParentPong(agent, p, "rgA", "rc");
        assertTrue(atAgent.isRevokedKey(victim), "the region's list was refused although its controller verifies by the pinned CA");
        // a list from the region's agent a2 (CA-issued, but not the controller this node joined) changes nothing
        ControllerEngine a2 = TlsTestSupport.engine("rgA", "a2", Map.of("broker_security_enabled", "true", "peer_trust_persist", "false"));
        X509Certificate[] a2chain = region.getCertificateManager().getCertsfromJson(
                region.getCertificateManager().issueEnrollment(a2.getCertificateManager().getJsonFromCerts(a2.getCertificateManager().getPublicCertificate())));
        assertTrue(a2.getCertificateManager().installEnrollment(a2.getCertificateManager().getJsonFromCerts(a2chain)));
        String other = randomFingerprints(1).get(0);
        assertNull(a2.getCertificateManager().getPeerTrust().revoke(other, "a malicious agent"));
        MsgEvent q = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
        q.setCompressedParam(RevocationSync.PARENT_LIST, RevocationSync.signOwn(a2));
        RevocationSync.onParentPong(agent, q, "rgA", "rc");
        assertFalse(atAgent.isRevokedKey(other), "an agent's list was accepted as its controller's");
    }

    /**
     * Merging a large child list happens off the ping path: the ping returns at once with the entries in force, and the
     * bindings, tombstones and record follow in ONE durable write.
     */
    @Test
    void aBulkChildListDoesNotDelayThePong() throws Exception {
        ControllerEngine global = node("rgG", "gc", true), region = node("rgB", "rc", true);
        bindAsController(global, region);
        bindAsController(region, global);
        PeerTrustRegistry atRegion = region.getCertificateManager().getPeerTrust(), atGlobal = global.getCertificateManager().getPeerTrust();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            KeyPair kp = TlsTestSupport.rsa();
            assertTrue(atRegion.bind("t1", "rgB", "b" + i, kp.getPublic(), "discovery").ok);
            assertTrue(atGlobal.bind("t1", "rgB", "b" + i, kp.getPublic(), "test").ok);
            names.add("rgB/b" + i);
        }
        for (String n : names) assertNull(atRegion.revoke(n, "operator"));
        MsgEvent p = ping("rgB", "rc");
        RevocationSync.onChildPing(region, p);
        long t0 = System.nanoTime();
        RevocationSync.onPingAtParent(global, p, true);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("bulk merge: 300 revocations on a ping took " + ms + " ms on the ping path");
        assertTrue(ms < 500, "the ping path took " + ms + " ms");
        assertEquals(300, atGlobal.revocations().size(), "the entries were not in force when the pong left");
        atGlobal.awaitRevocations();
        assertEquals(300, atGlobal.revocationSeq(), "the merged entries were not recorded");
        for (String n : names) {
            String[] ra = n.split("/");
            assertTrue(atGlobal.isTombstoned(ra[0], ra[1]), n + " was not tombstoned at the global");
        }
    }

    // ------------------------------------------------------------------ durability and expiry

    /** A sealed registry still enforces a revocation (in memory), and reports that it is not recorded. */
    @Test
    void aRevocationOnASealedRegistryIsEnforcedAndReported() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        r.close();
        List<String> lines = new ArrayList<>(Files.readAllLines(f));
        assertTrue(lines.get(2).contains("\"rgA/a2\""), lines.get(2));
        lines.set(2, lines.get(2).replace("\"rgA/a2\"", "\"rgA/a3\""));            // corrupt the LAST record: the registry seals
        Files.write(f, lines);
        PeerTrustRegistry sealed = open(f);
        assertTrue(sealed.isSealed(), "precondition: sealed");
        assertNotNull(sealed.verifiedIdentity(a1.chain), "precondition: a sealed registry still serves what verified");
        assertNull(sealed.revoke("rgA/a1", "operator"));
        assertNull(sealed.verifiedIdentity(a1.chain), "a sealed registry did not enforce the revocation");
        assertNotNull(sealed.revocationDurability(), "an unrecorded revocation was not reported");
    }

    /** A revocation of a key this region's CA enrolled expires once every leaf for it has; its name stays reserved. */
    @Test
    void aRevocationOfAnEnrolledKeyExpiresButItsNameStaysReserved() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        r.setIssuedLeafLifetime(168 * 3600_000L);
        KeyPair kp = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "a1", kp.getPublic(), "enrolled").ok);
        assertNull(r.revoke("rgA/a1", "operator"));
        String k = fp(kp.getPublic());
        assertTrue(r.isRevokedKey(k));
        r.prune(System.currentTimeMillis() + 30 * DAY);
        assertFalse(r.isRevokedKey(k), "an expired revocation was kept");
        assertTrue(r.isTombstoned("rgA", "a1"), "the name was released with the revocation");
        assertFalse(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "enrolled").ok);
        PeerTrustRegistry reopened = open(f);
        assertFalse(reopened.isRevokedKey(k), "the expiry was not recorded");
        assertTrue(reopened.isTombstoned("rgA", "a1"));
    }

    /** Revocations, tombstones and list positions survive a compaction and a restart; the epoch stays. */
    @Test
    void revocationsTombstonesAndPositionsSurviveCompactionAndRestart() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        KeyPair kp = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "a1", kp.getPublic(), "discovery").ok);
        assertNull(r.revoke("rgA/a1", "operator"));
        r.mergeRevocations(randomFingerprints(50), "parent", null);
        String epoch = r.revocationEpoch();
        long seq = r.revocationSeq(), own = r.ownRevocationSeq();
        java.lang.reflect.Method compact = PeerTrustRegistry.class.getDeclaredMethod("compact");
        compact.setAccessible(true);
        compact.invoke(r);
        assertTrue(Files.readAllLines(f).get(0).contains(epoch), "the compacted log does not carry the epoch");
        PeerTrustRegistry reopened = open(f);
        assertEquals(epoch, reopened.revocationEpoch(), "the epoch changed across a restart");
        assertEquals(seq, reopened.revocationSeq());
        assertEquals(own, reopened.ownRevocationSeq());
        assertTrue(reopened.isTombstoned("rgA", "a1"));
        assertTrue(reopened.isRevokedKey(fp(kp.getPublic())));
        assertEquals(1, reopened.revocationsAfter(0, 10_000, true).size(), "the own list lost its entry");
    }

    // ------------------------------------------------------------------ rebind tokens and vouched re-keys

    /** A rebind token naming a key fingerprint rebinds the (revoked) name to that key only. */
    @Test
    void aKeyBoundRebindTokenRebindsOnlyToThatKey() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        KeyPair old = TlsTestSupport.rsa(), thief = TlsTestSupport.rsa(), fresh = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "a1", old.getPublic(), "discovery").ok);
        assertNull(r.revoke("rgA/a1", "operator"));
        Files.writeString(f.resolveSibling("peer-trust.json.rebind"), "rgA/a1=" + fp(fresh.getPublic()) + "\n");
        assertFalse(r.bind("t1", "rgA", "a1", thief.getPublic(), "discovery").ok, "a token bound to one key let another take the name");
        assertTrue(r.bind("t1", "rgA", "a1", fresh.getPublic(), "discovery").ok, "the token's own key was refused");
        assertFalse(r.isTombstoned("rgA", "a1"));
        assertTrue(r.isRevokedKey(fp(old.getPublic())), "a rebind un-revoked the old key");
        PeerTrustRegistry reopened = open(f);
        assertEquals(fp(fresh.getPublic()), reopened.pinnedKey(new PeerIdentity.Expected("rgA", "a1")));
        assertFalse(reopened.isTombstoned("rgA", "a1"));
    }

    /** A pinned controller that re-keyed under the CA pinned for its region is followed; a revoked one is not. */
    @Test
    void aControllerReKeyedUnderItsPinnedCaIsFollowedUnlessRevoked() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry atAgent = new PeerTrustRegistry();
        assertNull(atAgent.trustCa(ca.caCert(), "rgA", "t1", "joined"));
        TlsTestSupport.Id rc = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        assertNull(atAgent.pin("rgA", "rc", rc.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, false));
        TlsTestSupport.Id reinstalled = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        assertNotNull(atAgent.pin("rgA", "rc", reinstalled.chain[0], PeerTrustRegistry.ROLE_CONTROLLER, false), "precondition: pinned");
        assertTrue(atAgent.issuedByRecordedCa(reinstalled.chain));
        assertTrue(atAgent.rebindVouched("t1", "rgA", "rc", reinstalled.kp.getPublic(), "vouched by rgA's CA").ok);
        assertNotNull(atAgent.verifiedIdentity(reinstalled.chain));
        assertNull(atAgent.verifiedIdentity(rc.chain), "the old key still verifies");
        // a self-made chain for the controller's name is not vouched for
        TlsTestSupport.Id selfMade = TlsTestSupport.selfRooted("x", "t1", "rgA", "rc");
        assertFalse(atAgent.issuedByRecordedCa(selfMade.chain));
        // once the controller is revoked, no re-key is followed
        assertNull(atAgent.revoke("rgA/rc", "operator"));
        TlsTestSupport.Id again = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        assertFalse(atAgent.issuedByRecordedCa(again.chain));
        assertFalse(atAgent.rebindVouched("t1", "rgA", "rc", again.kp.getPublic(), "vouched").ok);
    }

    // ------------------------------------------------------------------ registry lock (P6)

    /** A registry that cannot take its lock is fenced LOUDLY: refusals name it and the identity health is CRITICAL. */
    @Test
    void aRegistryThatCannotLockItsStoreIsCriticalAndRefusesNewBindings() throws Exception {
        Path f = newFile();
        Files.createDirectories(f.getParent());
        Path lock = f.resolveSibling("peer-trust.json.lock");
        try (FileChannel ch = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE); FileLock held = ch.lock()) {
            PeerTrustRegistry r = open(f);
            assertNotNull(r.lockFailure(), "the lock failure was not reported");
            PeerTrustRegistry.BindResult b = r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery");
            assertFalse(b.ok);
            assertTrue(b.reason.contains("FENCED"), b.reason);
            r.close();
        }
        // and through the node: the identity health check
        Path dir = Files.createTempDirectory("idfix-lock");
        Path sec = dir.resolve("security");
        Files.createDirectories(sec);
        try (FileChannel ch = FileChannel.open(sec.resolve("peer-trust.json.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock held = ch.lock()) {
            ControllerEngine e = engineIn(dir, "rgA", "a1", Map.of());
            String[] h = e.getCertificateManager().identityHealth();
            assertEquals("CRITICAL", h[0], h[1]);
            assertTrue(h[1].contains("FENCED"), h[1]);
        }
    }

    private static ControllerEngine engineIn(Path dir, String region, String agent, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("node_identity_dir", dir.resolve("security").toString());
        cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, region, agent);
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    // ------------------------------------------------------------------ clocks (P4)

    /** Certificates are backdated a day by default; a parent clock skewed past it is CRITICAL, half of it WARN. */
    @Test
    void clockSkewIsDetectedOnPongsAndCertificatesAreBackdatedADay() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgA", "a1", null);
        X509Certificate leaf = e.getCertificateManager().getPublicCertificate()[0];
        assertTrue(System.currentTimeMillis() - leaf.getNotBefore().getTime() >= 23 * 3600_000L, "not backdated a day: " + leaf.getNotBefore());
        CertificateManager cm = e.getCertificateManager();
        assertEquals("OK", cm.identityHealth()[0], cm.identityHealth()[1]);
        cm.noteClock(System.currentTimeMillis() + 13 * 3600_000L, "parent rgA/rc");
        assertEquals("WARN", cm.identityHealth()[0], cm.identityHealth()[1]);
        cm.noteClock(System.currentTimeMillis() + 25 * 3600_000L, "parent rgA/rc");
        assertEquals("CRITICAL", cm.identityHealth()[0], cm.identityHealth()[1]);
        cm.noteClock(System.currentTimeMillis(), "parent rgA/rc");
        assertEquals("OK", cm.identityHealth()[0], "a fixed clock did not clear the alarm");
        // on the ping/pong path
        ControllerEngine region = node("rgA", "rc", true), agent = node("rgA", "a2", false);
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a2", null, "rgA", "rc", null, true, false);
        RevocationSync.onChildPing(agent, p);
        p.setParam(RevocationSync.CLOCK, Long.toString(System.currentTimeMillis() - 30 * 3600_000L));   // the agent's clock lags 30 h
        RevocationSync.onPingAtParent(region, p, false);
        assertEquals("CRITICAL", region.getCertificateManager().identityHealth()[0], region.getCertificateManager().identityHealth()[1]);
    }

    // ------------------------------------------------------------------ renewal fallback

    /**
     * A region-issued leaf whose re-enrollment keeps failing is kept (never replaced by a self-made chain) and reported:
     * CRITICAL in its last tenth.
     */
    @Test
    void aFailingReEnrollmentKeepsTheRegionLeafAndAlarms() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        Path dir = Files.createTempDirectory("idfix-renew");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "a1");
        ControllerEngine agent = new ControllerEngine(null, pb, null, null) {
            @Override public boolean reEnroll() { return false; }
        };
        agent.cstate = TlsTestSupport.state(pb, "rgA", "a1");
        agent.setCertificateManager(new CertificateManager(agent));
        CertificateManager cm = agent.getCertificateManager();
        X509Certificate[] hour = ca.enrollFor(cm.getPublicCertificate()[0].getPublicKey(), "t1", "rgA", "a1", 3600_000L);   // its last tenth
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(hour)));
        assertFalse(cm.renewIfDue());
        X509Certificate now = cm.getPublicCertificate()[0];
        assertEquals(hour[0], now, "the region-issued leaf was replaced");
        assertEquals(ca.caCert().getSubjectX500Principal(), now.getIssuerX500Principal());
        String[] h = cm.identityHealth();
        assertEquals("CRITICAL", h[0], h[1]);
        System.out.println("renewal fallback: " + h[1]);
    }

    /** Re-enrolling with the same key and CA leaves the truststore as it was. */
    @Test
    void reEnrollmentsDoNotGrowTheTruststore() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", null);
        CertificateManager cm = agent.getCertificateManager();
        java.security.PublicKey key = cm.getPublicCertificate()[0].getPublicKey();
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(ca.enrollFor(key, "t1", "rgA", "a1", 168 * 3600_000L))));
        int before = trustStoreSize(cm);
        for (int i = 0; i < 5; i++) assertTrue(cm.installEnrollment(cm.getJsonFromCerts(ca.enrollFor(key, "t1", "rgA", "a1", 168 * 3600_000L))));
        assertEquals(before, trustStoreSize(cm), "re-enrollments added truststore entries");
        // a renewed region CA (same key) replaces its entry instead of adding one
        RegionCA renewed = ca.renewed(2048, 2);
        assertTrue(cm.installEnrollment(cm.getJsonFromCerts(renewed.enrollFor(key, "t1", "rgA", "a1", 168 * 3600_000L))));
        assertEquals(before, trustStoreSize(cm), "a renewed CA was added next to the old one");
    }

    private static int trustStoreSize(CertificateManager cm) throws Exception {
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("trustStore");
        f.setAccessible(true);
        return ((java.security.KeyStore) f.get(cm)).size();
    }

    /** A leaf whose identity is verified, used by assertions above. */
    @SuppressWarnings("unused")
    private static CrescoIdentity id(PeerTrustRegistry r, X509Certificate[] chain) { return r.verifiedIdentity(chain); }
}
