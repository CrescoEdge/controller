package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-4 review H3, L1, L4 and M2: the registry never acknowledges or serves a change that is not durable, puts the log
 * back after a failed write at once, has one writer per store, and reports a replaced key so TLS refuses it.
 */
class RegistryDurabilityTest {

    private static Path newFile() throws Exception {
        return Files.createTempDirectory("reg-dur").resolve("security").resolve("peer-trust.json");
    }

    private static PeerTrustRegistry open(Path f) {
        return PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> System.out.println("[registry] " + m));
    }

    /** L4 + H3: a write that fails part-way is not acknowledged, changes nothing in memory, and leaves no bytes behind. */
    @Test
    void aFailedWriteIsNotAcknowledgedNotServedAndLeavesNoBytes() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        long before = Files.size(f);
        r.injectWriteFailures(1, false);
        PeerTrustRegistry.BindResult failed = r.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery");
        assertFalse(failed.ok, "a bind whose write failed was acknowledged");
        assertNull(r.pinnedKey(new PeerIdentity.Expected("rgA", "a2")), "a binding whose write failed is served");
        assertEquals(before, Files.size(f), "the failed write's partial bytes were left in the log");
        assertTrue(r.bind("t1", "rgA", "a3", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        PeerTrustRegistry reboot = open(f);
        assertFalse(reboot.isSealed(), reboot.sealReason());
        assertNotNull(reboot.pinnedKey(new PeerIdentity.Expected("rgA", "a3")));
        assertNull(reboot.pinnedKey(new PeerIdentity.Expected("rgA", "a2")));
    }

    /** L4: an fsync that fails rolls the binding back: it is neither acknowledged nor served, and the log is put back. */
    @Test
    void aFailedFsyncRollsTheBindingBack() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        KeyPair k1 = TlsTestSupport.rsa(), k2 = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "a1", k1.getPublic(), "discovery").ok);
        long before = Files.size(f);
        PeerTrustRegistry withRebind = PeerTrustRegistry.open(f, List.of("rgA/a1"), List.of(), 30, m -> { });
        withRebind.injectWriteFailures(1, true);
        assertFalse(withRebind.bind("t1", "rgA", "a1", k2.getPublic(), "discovery").ok, "a rebind whose fsync failed was acknowledged");
        assertEquals(PeerTrustRegistry.fingerprint(k1.getPublic().getEncoded()), withRebind.pinnedKey(new PeerIdentity.Expected("rgA", "a1")),
                "a rebind whose fsync failed replaced the key in memory");
        assertEquals(before, Files.size(f));
        // the operator's rebind token was not consumed by the failed write: the retry succeeds
        assertTrue(withRebind.bind("t1", "rgA", "a1", k2.getPublic(), "discovery").ok);
        assertFalse(open(f).isSealed());
    }

    /** L1: opening a store again fences the instance that had it; the fenced one serves but records nothing. */
    @Test
    void aSecondOpenFencesTheFirstWriter() throws Exception {
        Path f = newFile();
        PeerTrustRegistry first = open(f);
        assertTrue(first.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        PeerTrustRegistry second = open(f);
        assertNotNull(first.fencedReason(), "the first instance still writes");
        assertNotNull(first.pinnedKey(new PeerIdentity.Expected("rgA", "a1")), "a fenced instance stopped serving");
        PeerTrustRegistry.BindResult r = first.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery");
        assertFalse(r.ok);
        assertTrue(r.reason.contains("records nothing"), r.reason);
        assertTrue(second.bind("t1", "rgA", "a3", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertFalse(open(f).isSealed());
    }

    /** L1: shared() hands re-created components the instance already open; files deleted underneath it mean a new one. */
    @Test
    void sharedReturnsTheLiveInstanceOnlyWhileItsFilesAreIntact() throws Exception {
        Path f = newFile();
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        PeerTrustRegistry a = PeerTrustRegistry.shared(f, o, m -> { });
        assertSame(a, PeerTrustRegistry.shared(f, o, m -> { }), "a re-created component opened a second writer");
        a.close();
        PeerTrustRegistry b = PeerTrustRegistry.shared(f, o, m -> { });
        assertNotSame(a, b, "a closed instance was handed out");
        Files.delete(f);
        PeerTrustRegistry c = PeerTrustRegistry.shared(f, o, m -> { });
        assertNotSame(b, c, "an instance whose log was deleted underneath it was handed out");
        assertNotNull(b.fencedReason());
        c.close();
    }

    /** H3: bytes past the last durable record (a write that failed part-way earlier) are removed before the next append. */
    @Test
    void foreignBytesPastTheLastDurableRecordAreRemovedBeforeTheNextAppend() throws Exception {
        Path f = newFile();
        AtomicReference<String> said = new AtomicReference<>("");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> said.set(said.get() + m + "\n"));
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        Files.writeString(f, "{\"op\":\"bind\",\"na", java.nio.file.StandardOpenOption.APPEND);
        assertTrue(r.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertTrue(said.get().contains("after the last durable record"), said.get());
        for (String line : Files.readAllLines(f)) assertTrue(line.endsWith("}"), "a partial line remains: " + line);
    }

    /** H3: a log truncated underneath the process seals instead of being appended to. */
    @Test
    void aLogTruncatedUnderneathTheProcessSeals() throws Exception {
        Path f = newFile();
        PeerTrustRegistry r = open(f);
        assertTrue(r.bind("t1", "rgA", "a1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertTrue(r.bind("t1", "rgA", "a2", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(f, java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(Files.size(f) - 20);
        }
        assertFalse(r.bind("t1", "rgA", "a3", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertTrue(r.isSealed());
    }

    /** M2: a key rebound away is refused at TLS even while its chain is still an anchor. */
    @Test
    void aReplacedKeyIsReportedAndRefused() throws Exception {
        TlsTestSupport.provider();
        PeerTrustRegistry r = new PeerTrustRegistry().withRebind(List.of("rgA/a1"));
        AtomicReference<String> replaced = new AtomicReference<>();
        r.setKeyReplacedListener((n, k) -> replaced.set(n + "=" + k));
        TlsTestSupport.Id k1 = TlsTestSupport.selfRooted("k1", "t1", "rgA", "a1");
        TlsTestSupport.Id k2 = TlsTestSupport.selfRooted("k2", "t1", "rgA", "a1");
        assertTrue(r.bind("t1", "rgA", "a1", k1.kp.getPublic(), "discovery").ok);
        assertNull(r.refusedKey(k1.chain));
        assertTrue(r.bind("t1", "rgA", "a1", k2.kp.getPublic(), "discovery").ok);
        assertEquals("rgA/a1=" + PeerTrustRegistry.fingerprint(k1.kp.getPublic().getEncoded()), replaced.get());
        assertNotNull(r.refusedKey(k1.chain), "the replaced key is not refused at TLS");
        assertNull(r.refusedKey(k2.chain));
    }
}
