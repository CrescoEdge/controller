package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The registry's integrity, recovery and pruning rules (third review, findings 4, 6, 8, 12). */
class RegistryIntegrityTest {

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("ri").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    @Test
    void aRecoveryRequestIsAppliedOnlyToAnAlreadySealedRegistryAndOnlyOnce() throws Exception {
        Path f = newFile();
        KeyPair k = TlsTestSupport.rsa();
        assertTrue(open(f).bind("t1", "rgA", "a1", k.getPublic(), "test").ok);
        Path recover = f.resolveSibling("peer-trust.json.recover");
        Files.writeString(recover, "reset\n");
        PeerTrustRegistry r = open(f);                                   // not sealed: the request is set aside, not applied
        assertFalse(r.isSealed());
        assertNotNull(r.pinnedKey(new PeerIdentity.Expected("rgA", "a1")), "a reset was applied to a registry that was not sealed");
        assertFalse(Files.exists(recover), "an ignored request was left in place (it would apply later)");
        try (var s = Files.list(f.getParent())) {
            assertTrue(s.anyMatch(p -> p.getFileName().toString().startsWith("peer-trust.json.recover.ignored-")));
        }
    }

    @Test
    void acceptKeepsOnlyRecordsThatVerify() throws Exception {
        Path f = newFile();
        KeyPair k = TlsTestSupport.rsa(), forged = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", k.getPublic(), "test").ok);
        assertTrue(r.bind("t1", "rgA", "a2", k.getPublic(), "test").ok);
        String fpForged = PeerTrustRegistry.fingerprint(forged.getPublic().getEncoded());
        // an edit without the key: a2's record no longer verifies
        Files.writeString(f, Files.readString(f).replaceFirst("(\"name\":\"rgA/a2\",\"tenant\":\"t1\",\"key\":\")[0-9a-f]+", "$1" + fpForged));
        assertTrue(open(f).isSealed());
        Files.writeString(f.resolveSibling("peer-trust.json.recover"), "accept\n");
        PeerTrustRegistry accepted = open(f);
        assertFalse(accepted.isSealed());
        assertNotNull(accepted.pinnedKey(new PeerIdentity.Expected("rgA", "a1")), "a record that verifies was dropped");
        assertNotEquals(fpForged, accepted.pinnedKey(new PeerIdentity.Expected("rgA", "a2")), "accept re-signed a record that did not verify");
        assertFalse(open(f).isSealed(), "the accepted registry did not verify on the next start");
    }

    @Test
    void aRegistryDeletedEntirelyIsSealedWhenTheNodeIdentityPersists() throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("ri-node");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("node_identity_dir", dir.toString());
        ControllerEngine first = engine(dir, cfg);
        assertTrue(Files.exists(dir.resolve("node-identity.p12")));
        assertFalse(first.getCertificateManager().getPeerTrust().isSealed());
        for (String n : new String[]{"peer-trust.json", "peer-trust.json.key", "peer-trust.json.head"}) Files.deleteIfExists(dir.resolve(n));
        ControllerEngine second = engine(dir, cfg);
        PeerTrustRegistry r = second.getCertificateManager().getPeerTrust();
        System.out.println("registry deleted (log, key and head), identity kept: sealed=" + r.isSealed() + " (" + r.sealReason() + ")");
        assertTrue(r.isSealed(), "a registry deleted entirely was silently started empty although the node identity persists");
    }

    private static ControllerEngine engine(Path dir, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "rc");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = new TlsTestSupport.State(pb, "rgA", "rc");
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    @Test
    void aGroupOrWorldWritableRegistryDirectoryIsTightened() throws Exception {
        Path f = newFile();
        Files.createDirectories(f.getParent());
        Files.setPosixFilePermissions(f.getParent(), PosixFilePermissions.fromString("rwxrwxrwx"));
        open(f);
        String perms = PosixFilePermissions.toString(Files.getPosixFilePermissions(f.getParent()));
        assertFalse(perms.charAt(4) == 'w' || perms.charAt(7) == 'w', "the registry directory is still writable by group/others: " + perms);
    }

    @Test
    void useOverTheBrokerRefreshesABindingAndIsPersistedInABatch() throws Exception {
        Path f = newFile();
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("r", "t1", "rgA", "a1");
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", a1.kp.getPublic(), "test").ok);
        r.backdateBinding("rgA", "a1", 80L * 24 * 3600 * 1000);
        assertNotNull(r.verifiedIdentity(a1.chain));                    // the agent connects to the broker
        assertTrue(System.currentTimeMillis() - r.lastSeen("rgA", "a1") < 60_000, "broker use did not refresh the binding");
        long size = Files.size(f);
        r.flushSeen();                                                   // the background writer's batch
        assertTrue(Files.size(f) > size, "the refresh was not persisted");
        PeerTrustRegistry reopened = open(f);
        assertTrue(System.currentTimeMillis() - reopened.lastSeen("rgA", "a1") < 60_000, "the refresh did not survive a restart");
        reopened.prune(System.currentTimeMillis() + 89L * 24 * 3600 * 1000);
        assertNotNull(reopened.pinnedKey(new PeerIdentity.Expected("rgA", "a1")), "a binding in daily use was pruned");
    }

    @Test
    void aRefreshRacingARebindNeverRestoresTheOldKey() throws Exception {
        Path f = newFile();
        KeyPair oldK = TlsTestSupport.rsa(), newK = TlsTestSupport.rsa();
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of("rgA/a1"), List.of(), 30, m -> { });
        assertTrue(r.bind("t1", "rgA", "a1", oldK.getPublic(), "test").ok);
        r.backdateBinding("rgA", "a1", 10 * 60_000L);
        java.lang.reflect.Field fld = PeerTrustRegistry.class.getDeclaredField("identities");
        fld.setAccessible(true);
        @SuppressWarnings("unchecked") Map<String, Object> ids = (Map<String, Object>) fld.get(r);
        Object stale = ids.get("rgA/a1");                                 // a refresh read the binding ...
        assertTrue(r.bind("t1", "rgA", "a1", newK.getPublic(), "operator").ok);   // ... the operator rebinds meanwhile ...
        java.lang.reflect.Method touch = PeerTrustRegistry.class.getDeclaredMethod("touch", Map.class, String.class, stale.getClass(),
                long.class, String.class);
        touch.setAccessible(true);
        touch.invoke(r, ids, "rgA/a1", stale, System.currentTimeMillis(), "id");  // ... and the refresh completes
        assertEquals(PeerTrustRegistry.fingerprint(newK.getPublic().getEncoded()), r.pinnedKey(new PeerIdentity.Expected("rgA", "a1")),
                "a refresh racing an operator rebind put the old key back");
    }

    @Test
    void aReplacedKeyStaysRefusedForItsNameAfterThePruneAndOnAStrictJoin() throws Exception {
        Path f = newFile();
        KeyPair oldK = TlsTestSupport.rsa(), newK = TlsTestSupport.rsa();
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of("rgA/a1"), List.of(), 30, m -> { });
        assertTrue(r.bind("t1", "rgA", "a1", oldK.getPublic(), "test").ok);
        assertTrue(r.bind("t1", "rgA", "a1", newK.getPublic(), "operator").ok);
        r.prune(System.currentTimeMillis() + 91L * 24 * 3600 * 1000);
        assertNull(r.pinnedKey(new PeerIdentity.Expected("rgA", "a1")), "precondition: pruned");
        PeerTrustRegistry reopened = open(f);
        assertFalse(reopened.bind("t1", "rgA", "a1", oldK.getPublic(), "join").ok, "the replaced key took the name back after a prune");
        assertTrue(reopened.bind("t1", "rgA", "a1", newK.getPublic(), "join").ok);
    }
}
