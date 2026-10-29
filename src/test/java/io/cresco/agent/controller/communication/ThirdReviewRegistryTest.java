package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Third adversarial review (registry). Each test asserts the SECURE / correct outcome; a failure is a finding. */
class ThirdReviewRegistryTest {

    private static PeerTrustRegistry open(Path f, List<String> rebind) {
        return PeerTrustRegistry.open(f, rebind, List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("r3").resolve("security").resolve("peer-trust.json");
    }

    /**
     * Compaction runs INSIDE append(), from the in-memory maps, before bind() puts the new binding in them: the record that
     * triggered compaction is written to the old file and then lost when the compacted file replaces it. After a restart
     * the acknowledged binding is gone (the name is free again, first come).
     */
    @Test
    void theBindThatTriggersCompactionSurvivesARestart() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f, List.of());
        KeyPair c1 = TlsTestSupport.rsa(), c2 = TlsTestSupport.rsa(), newAgent = TlsTestSupport.rsa();
        // a client following its controller's key (or any churn of seen/bind records): 1003 records, live state = 1 name
        for (int i = 0; i < 1003; i++) {
            assertTrue(r.bind("t1", "rgA", "rc", (i % 2 == 0 ? c1 : c2).getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, true).ok);
        }
        long before = Files.lines(f).count();
        // the next record crosses 4*live+1000: a brand-new agent's first (acknowledged) binding
        PeerTrustRegistry.BindResult b = r.bind("t1", "rgA", "a7", newAgent.getPublic(), "discovery");
        long after = Files.lines(f).count();
        System.out.println("log lines before=" + before + " after=" + after + " bind ok=" + b.ok + " in-memory pinned="
                + (r.pinnedKey(new PeerIdentity.Expected("rgA", "a7")) != null));
        assertTrue(b.ok);
        PeerTrustRegistry reboot = open(f, List.of());
        String pinned = reboot.pinnedKey(new PeerIdentity.Expected("rgA", "a7"));
        System.out.println("after restart rgA/a7 pinned=" + pinned + " sealed=" + reboot.isSealed());
        assertNotNull(pinned, "an acknowledged binding (rgA/a7) was lost by the compaction it triggered; after a restart the name is free to anyone");
    }

    /**
     * Same root cause for an operator rebind: the bind record triggers compaction (lost), then 'consume' is appended to the
     * new file. After a restart the OLD key is bound again and the operator's one-shot token is spent.
     */
    @Test
    void anOperatorRebindThatTriggersCompactionIsNotRevertedOnRestart() throws Exception {
        Path f = newFile();
        KeyPair oldKey = TlsTestSupport.rsa(), newKey = TlsTestSupport.rsa(), c1 = TlsTestSupport.rsa(), c2 = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(f, List.of("rgA/a1"));
        assertTrue(r.bind("t1", "rgA", "a1", oldKey.getPublic(), "discovery").ok);
        // live = 2 names -> threshold 1008; lines = 2 now
        for (int i = 0; i < 1006; i++) {
            assertTrue(r.bind("t1", "rgA", "rc", (i % 2 == 0 ? c1 : c2).getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, true).ok);
        }
        assertTrue(r.bind("t1", "rgA", "a1", newKey.getPublic(), "discovery").ok, "the operator-authorised rebind");
        String fpNew = PeerTrustRegistry.fingerprint(newKey.getPublic().getEncoded());
        assertEquals(fpNew, r.pinnedKey(new PeerIdentity.Expected("rgA", "a1")));
        PeerTrustRegistry reboot = open(f, List.of("rgA/a1"));
        String pinned = reboot.pinnedKey(new PeerIdentity.Expected("rgA", "a1"));
        System.out.println("after restart rgA/a1 pinned new key=" + fpNew.equals(pinned));
        assertEquals(fpNew, pinned, "the operator's rebind (old key -> new key) was reverted by a restart: the OLD key is bound again");
    }

    /**
     * An attacker who can write the data directory but cannot read the key: delete the log AND the key file. The next open
     * starts a brand-new, unsealed, empty registry (the "key says initialised, log missing" seal is bypassed).
     */
    @Test
    void deletingTheLogAndItsKeyIsDetected() throws Exception {
        Path f = newFile();
        KeyPair legit = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgA", "a1", legit.getPublic(), "test").ok);
        Files.delete(f);
        Files.delete(f.resolveSibling("peer-trust.json.key"));
        PeerTrustRegistry after = open(f, List.of());
        boolean taken = !after.isSealed() && after.bind("t1", "rgA", "a1", attacker.getPublic(), "attacker").ok;
        System.out.println("after deleting log+key: sealed=" + after.isSealed() + " attacker bound rgA/a1=" + taken);
        assertFalse(taken, "deleting the log and its key file silently reset the registry: rgA/a1 re-bound to a new key");
    }

    /**
     * Directory-write attacker (cannot read the key): replace the log with forged records and drop '.recover' = accept.
     * The next start re-signs the forged records with the real key -- the registry is not even sealed beforehand.
     */
    @Test
    void recoverAcceptDoesNotLaunderAForgedLogOnAnUnsealedRegistry() throws Exception {
        Path f = newFile();
        KeyPair legit = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgA", "a1", legit.getPublic(), "test").ok);
        String fpAtt = PeerTrustRegistry.fingerprint(attacker.getPublic().getEncoded());
        String forged = "{\"op\":\"init\",\"source\":\"cresco-peer-trust-v2\",\"t\":1}\n"
                + "{\"op\":\"bind\",\"name\":\"rgA/a1\",\"tenant\":\"t1\",\"key\":\"" + fpAtt + "\",\"source\":\"x\",\"role\":\"agent\",\"t\":1}\n"
                + "{\"op\":\"bind\",\"name\":\"rgB/rcB\",\"tenant\":\"t1\",\"key\":\"" + fpAtt + "\",\"source\":\"x\",\"role\":\"controller\",\"t\":"
                + System.currentTimeMillis() + "}\n"
                + "{\"op\":\"bridge\",\"name\":\"rgB/rcB\",\"t\":" + System.currentTimeMillis() + "}\n";
        Files.delete(f);                                                   // unlink: needs only directory write
        Files.writeString(f, forged, StandardCharsets.UTF_8);
        Files.writeString(f.resolveSibling("peer-trust.json.recover"), "accept\n");
        PeerTrustRegistry after = open(f, List.of());
        String a1 = after.pinnedKey(new PeerIdentity.Expected("rgA", "a1"));
        boolean bridge = after.isBridgePeer(io.cresco.library.security.CrescoIdentity.of("t1", "rgB", "rcB", null));
        System.out.println("after forged log + recover=accept: sealed=" + after.isSealed() + " rgA/a1 -> attacker key=" + fpAtt.equals(a1)
                + " forged bridge peer rgB/rcB=" + bridge);
        assertFalse(fpAtt.equals(a1) || bridge, "a forged, unsigned log was accepted and re-signed with the real key");
    }

    /** The documented recovery for "key missing" (accept) re-seals on the same start: the new key it signs with is never saved. */
    @Test
    void acceptRecoversARegistryWhoseKeyWasLost() throws Exception {
        Path f = newFile();
        KeyPair legit = TlsTestSupport.rsa();
        assertTrue(open(f, List.of()).bind("t1", "rgA", "a1", legit.getPublic(), "test").ok);
        Files.delete(f.resolveSibling("peer-trust.json.key"));
        PeerTrustRegistry sealed = open(f, List.of());
        assertTrue(sealed.isSealed(), "precondition: a missing key seals");
        Files.writeString(f.resolveSibling("peer-trust.json.recover"), "accept\n");
        PeerTrustRegistry recovered = open(f, List.of());
        System.out.println("after accept: sealed=" + recovered.isSealed() + " reason=" + recovered.sealReason()
                + " recover file left=" + Files.exists(f.resolveSibling("peer-trust.json.recover")));
        assertFalse(recovered.isSealed(), "operator 'accept' does not recover a registry whose key file was lost (it seals again at once)");
    }

    /** Truncating the log at a line boundary (dropping the newest records) verifies: a consumed one-shot rebind is re-armed. */
    @Test
    void truncatingTheLogDoesNotReArmAConsumedRebind() throws Exception {
        Path f = newFile();
        KeyPair oldKey = TlsTestSupport.rsa(), newKey = TlsTestSupport.rsa(), attacker = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(f, List.of("rgA/a1"));
        assertTrue(r.bind("t1", "rgA", "a1", oldKey.getPublic(), "test").ok);
        String keep = Files.readString(f);
        assertTrue(r.bind("t1", "rgA", "a1", newKey.getPublic(), "test").ok, "operator rebind consumed");
        Files.writeString(f, keep);                                       // drop the rebind + consume records
        PeerTrustRegistry after = open(f, List.of("rgA/a1"));
        boolean retaken = !after.isSealed() && after.bind("t1", "rgA", "a1", attacker.getPublic(), "attacker").ok;
        System.out.println("after truncation: sealed=" + after.isSealed() + " consumed rebind re-armed (attacker bound)=" + retaken);
        assertFalse(retaken, "truncating the log re-armed a consumed one-shot rebind token");
    }

    /**
     * Regional CA: an operator rebind replaces a compromised key; the old CA-issued certificate (valid for years) is refused
     * only because the name's binding names the new key. Bindings are refreshed only by discovery (verifiedIdentity, i.e.
     * live broker use, never refreshes them), and are pruned after peer_binding_prune_days: from then on the region CA path
     * accepts EVERY certificate it ever issued for the name -- the compromised one included.
     */
    @Test
    void pruningABindingDoesNotResurrectARevokedCaIssuedCertificate() throws Exception {
        Path f = newFile();
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        PeerTrustRegistry r = open(f, List.of("rgA/a1"));
        assertNull(r.trustCa(ca.caCert(), "rgA", "t1", "own"));
        TlsTestSupport.Id compromised = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", compromised.kp.getPublic(), "enrolled").ok);
        TlsTestSupport.Id reissued = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", reissued.kp.getPublic(), "enrolled").ok, "operator rebind to the new key");
        for (int i = 0; i < 5; i++) assertNotNull(r.verifiedIdentity(reissued.chain));   // the agent in live use over the broker
        assertNull(r.verifiedIdentity(compromised.chain), "precondition: the old key is refused after the rebind");
        java.lang.reflect.Method prune = PeerTrustRegistry.class.getDeclaredMethod("prune", long.class);
        prune.setAccessible(true);
        prune.invoke(r, System.currentTimeMillis() + 91L * 24 * 3600 * 1000);            // the restart 91 days later
        io.cresco.library.security.CrescoIdentity back = r.verifiedIdentity(compromised.chain);
        System.out.println("after pruning (agent used daily over the broker, no rediscovery): compromised certificate verifies as " + back);
        assertNull(back, "pruning the binding re-validated the compromised (rebound-away) CA-issued certificate for rgA/a1");
    }
}
