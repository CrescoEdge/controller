package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The peer trust registry's store: recovery, torn writes, permissions, one-shot rebind, no I/O on refresh, scale. */
class PeerTrustStoreTest {

    private static Path file() throws Exception {
        return Files.createTempDirectory("pts").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> { });
    }

    @Test
    void aSealIsLiftedOnlyByTheOperatorAndWithoutARestart() throws Exception {
        Path f = file();
        KeyPair a = TlsTestSupport.rsa(), b = TlsTestSupport.rsa();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", a.getPublic(), "test").ok);
        Files.writeString(f, Files.readString(f).replace("rgA/a1", "rgA/a9"));
        PeerTrustRegistry sealed = open(f);
        assertTrue(sealed.isSealed());
        assertFalse(sealed.bind("t1", "rgA", "a2", b.getPublic(), "test").ok);
        // the operator inspects and accepts what parses: applied on the next bind, no restart
        Files.writeString(f.resolveSibling("peer-trust.json.recover"), "accept\n");
        Thread.sleep(5_100);                                                    // recovery requests are polled every 5 s
        assertTrue(sealed.bind("t1", "rgA", "a2", b.getPublic(), "test").ok, "accept did not lift the seal");
        assertFalse(sealed.isSealed());
        assertFalse(Files.exists(f.resolveSibling("peer-trust.json.recover")), "the recovery request was not consumed");
        assertTrue(open(f).bind("t1", "rgA", "a9", a.getPublic(), "test").ok, "accepted records were not kept");
    }

    @Test
    void resetStartsEmptyButOnlyWhenAsked() throws Exception {
        Path f = file();
        KeyPair a = TlsTestSupport.rsa(), b = TlsTestSupport.rsa();
        assertTrue(open(f).bind("t1", "rgA", "a1", a.getPublic(), "test").ok);
        Files.delete(f);                                                        // deleted while its key remains
        assertTrue(open(f).isSealed(), "a deleted registry was silently replaced by an empty one");
        assertTrue(open(f).isSealed(), "a restart lifted the seal");
        Files.writeString(f.resolveSibling("peer-trust.json.recover"), "reset");
        PeerTrustRegistry r = open(f);
        assertFalse(r.isSealed());
        assertTrue(r.bind("t1", "rgA", "a1", b.getPublic(), "test").ok, "after an explicit reset every name is open again (first come)");
    }

    @Test
    void aTornLastLineIsDroppedNotSealed() throws Exception {
        Path f = file();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "test").ok);
        Files.writeString(f, Files.readString(f) + "{\"op\":\"bind\",\"name\":\"rgA/a2\",\"ten", java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        PeerTrustRegistry after = open(f);
        assertFalse(after.isSealed(), "a crash mid-append sealed the registry");
        assertEquals(1, after.pinCount());
        assertTrue(after.bind("t1", "rgA", "a3", TlsTestSupport.rsa().getPublic(), "test").ok);
        assertFalse(open(f).isSealed(), "the log was not left consistent after dropping the torn line");
    }

    @Test
    void filesAreOwnerOnlyFromTheStart() throws Exception {
        Path f = file();
        open(f).bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "test");
        for (Path p : new Path[]{f, f.resolveSibling("peer-trust.json.key")}) {
            assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(p)), p.toString());
        }
        assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(f.getParent())));
    }

    @Test
    void rebindFromTheOperatorFileIsReloadedAndOneShotPerTag() throws Exception {
        Path f = file();
        PublicKey k1 = TlsTestSupport.rsa().getPublic(), k2 = TlsTestSupport.rsa().getPublic(), k3 = TlsTestSupport.rsa().getPublic();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", k1, "test").ok);
        PeerTrustRegistry.BindResult refused = r.bind("t1", "rgA", "a1", k2, "test");
        assertFalse(refused.ok);
        assertTrue(refused.reason.contains("peer-trust.json.rebind"), "the refusal does not say what the operator must do: " + refused.reason);
        Files.writeString(f.resolveSibling("peer-trust.json.rebind"), "rgA/a1\n");   // the operator, no restart
        assertTrue(r.bind("t1", "rgA", "a1", k2, "test").ok);
        assertFalse(r.bind("t1", "rgA", "a1", k3, "test").ok, "a rebind stayed open after it was used");
        assertFalse(open(f).bind("t1", "rgA", "a1", k3, "test").ok, "a consumed rebind came back after a restart");
        Files.writeString(f.resolveSibling("peer-trust.json.rebind"), "rgA/a1\nrgA/a1#again\n");
        Thread.sleep(1100);                                                     // a new mtime
        Files.setLastModifiedTime(f.resolveSibling("peer-trust.json.rebind"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
        assertTrue(r.bind("t1", "rgA", "a1", k3, "test").ok, "a new tag did not authorise a second rebind");
    }

    @Test
    void refreshesWriteNothingAndUnseenBindingsArePruned() throws Exception {
        Path f = file();
        PublicKey k = TlsTestSupport.rsa().getPublic();
        PeerTrustRegistry r = open(f);
        for (int i = 0; i < 50; i++) assertTrue(r.bind("t1", "rgA", "a" + i, k, "test").ok);
        long size = Files.size(f);
        for (int i = 0; i < 50; i++) assertTrue(r.bind("t1", "rgA", "a" + i, k, "test").ok);
        assertEquals(size, Files.size(f), "refreshing known bindings wrote to the log");
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.bindingPruneDays = 1;
        PeerTrustRegistry reopened = PeerTrustRegistry.open(f, o, m -> { });
        assertEquals(50, reopened.pinCount());
        reopened.prune(System.currentTimeMillis() + 2L * 24 * 3600 * 1000);       // two days later, never seen again
        assertEquals(0, reopened.pinCount(), "bindings unseen past peer_binding_prune_days were kept");
        assertEquals(0, PeerTrustRegistry.open(f, o, m -> { }).pinCount(), "the prune was not persisted");
    }

    @Test
    void tenThousandBindings() throws Exception {
        Path f = file();
        PublicKey k = TlsTestSupport.rsa().getPublic();
        PeerTrustRegistry r = open(f);
        int n = 10_000, threads = 16;
        long s0 = System.nanoTime();
        for (int i = 0; i < 200; i++) assertTrue(r.bind("t1", "rgS", "seq-" + i, k, "test").ok);
        long seqUs = (System.nanoTime() - s0) / 1_000 / 200;
        long t0 = System.nanoTime();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.List<java.util.concurrent.Future<Boolean>> fs = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) { final int j = i; fs.add(pool.submit(() -> r.bind("t1", "rgA", "agent-" + j, k, "test").ok)); }
        for (java.util.concurrent.Future<Boolean> x : fs) assertTrue(x.get());
        pool.shutdown();
        long bindMs = (System.nanoTime() - t0) / 1_000_000;
        n += 200;
        long t1 = System.nanoTime();
        PeerTrustRegistry reopened = open(f);
        long loadMs = (System.nanoTime() - t1) / 1_000_000;
        long t2 = System.nanoTime();
        for (int i = 0; i < 100_000; i++) reopened.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null));
        long checkNs = (System.nanoTime() - t2) / 100_000;
        System.out.println("10k bindings from " + threads + " concurrent joiners: " + bindMs + " ms (group commit); one sequential"
                + " new binding " + seqUs + " us (one fsync); log "
                + Files.size(f) + " bytes; reload " + loadMs + " ms; isBridgePeer " + checkNs + " ns");
        assertEquals(n, reopened.pinCount());
        assertTrue(loadMs < 10_000, "reloading 10k bindings took " + loadMs + " ms");
    }
}
