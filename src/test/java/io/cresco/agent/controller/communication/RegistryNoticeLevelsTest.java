package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the registry says, at the level its meaning deserves: what a node now holds is not an error (INFO), an expected event
 * of the upgrade window is a WARN, and what is refused, replaced by the operator or moved stays an error. The fabric matrix
 * fails a run on any ERROR line it does not know, and every region controller recorded its global at ERROR, as did every
 * first join while the window was open. The node's own log reaches these consumers as CertificateManager sets them.
 */
class RegistryNoticeLevelsTest {

    private final List<String> errors = new CopyOnWriteArrayList<>(), warns = new CopyOnWriteArrayList<>(), infos = new CopyOnWriteArrayList<>();

    private static X509Certificate[] chain(String region, String agent, KeyPair kp) throws Exception {
        return RegionCA.generate("t1", region, 2048, 1).enroll(kp.getPublic(), "t1", region, agent, 1);
    }

    private PeerTrustRegistry open() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("notices").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, errors::add);
        r.setNoticeLogs(infos::add, warns::add);
        return r;
    }

    private static PeerTrustRegistry.BindResult join(PeerTrustRegistry r, String region, String agent, KeyPair kp, String role, boolean proved,
                                                     Consumer<PeerTrustRegistry.Join> more) throws Exception {
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = region; j.agent = agent; j.chain = chain(region, agent, kp); j.role = role; j.proved = proved; j.exchange = "AGENT";
        if (more != null) more.accept(j);
        return r.join(j);
    }

    private static boolean any(List<String> lines, String... parts) {
        return lines.stream().anyMatch(l -> { for (String p : parts) if (!l.contains(p)) return false; return true; });
    }

    @Test
    void theGlobalARegionRecordsWithProofIsInformation() throws Exception {
        PeerTrustRegistry r = open();
        assertTrue(join(r, "rgG", "gc", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_CONTROLLER, true,
                j -> { j.client = true; j.global = true; j.exchange = "GLOBAL"; }).ok);
        assertEquals("rgG/gc", r.globalRecorded());
        assertTrue(any(infos, "rgG/gc", "is recorded as this node's GLOBAL"), infos.toString());
        assertFalse(any(errors, "is recorded as this node's GLOBAL"), "the global a region recorded was said as an ERROR: " + errors);
        assertFalse(any(warns, "is recorded as this node's GLOBAL"), warns.toString());
    }

    @Test
    void aFirstJoinWithoutProofInTheWindowIsAWarning() throws Exception {
        PeerTrustRegistry r = open();
        assertTrue(join(r, "rgB", "a1", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_AGENT, false, j -> j.window = true).ok);
        assertTrue(any(warns, "UPGRADE WINDOW: rgB/a1", "recorded WITHOUT proof"), warns.toString());
        assertFalse(any(errors, "UPGRADE WINDOW"), "a first join in the upgrade window was said as an ERROR: " + errors);
        // a global recorded without proof in the window: a WARN (unconfirmed); its proof in a GLOBAL exchange confirms it (INFO)
        KeyPair g = TlsTestSupport.rsa();
        assertTrue(join(r, "rgG", "gc", g, PeerTrustRegistry.ROLE_CONTROLLER, false,
                j -> { j.window = true; j.client = true; j.global = true; j.exchange = "GLOBAL"; }).ok);
        assertTrue(any(warns, "rgG/gc", "is recorded as this node's GLOBAL", "UNCONFIRMED"), warns.toString());
        assertTrue(r.globalUnconfirmed());
        X509Certificate[] same = chain("rgG", "gc", g);
        PeerTrustRegistry.Join proof = new PeerTrustRegistry.Join();
        proof.region = "rgG"; proof.agent = "gc"; proof.chain = same; proof.role = PeerTrustRegistry.ROLE_CONTROLLER; proof.proved = true;
        proof.client = true; proof.global = true; proof.exchange = "GLOBAL";
        assertTrue(r.join(proof).ok);
        assertFalse(r.globalUnconfirmed());
        assertTrue(any(infos, "rgG/gc", "PROVED its key in a GLOBAL exchange: confirmed"), infos.toString());
        assertFalse(any(errors, "GLOBAL"), errors.toString());
    }

    @Test
    void whatIsRefusedOrMovedByTheOperatorStaysAnError() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("notices").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, errors::add);
        r.setNoticeLogs(infos::add, warns::add);
        Consumer<PeerTrustRegistry.Join> asGlobal = j -> { j.client = true; j.global = true; j.exchange = "GLOBAL"; };
        assertTrue(join(r, "rgG", "gc", TlsTestSupport.rsa(), PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        KeyPair other = TlsTestSupport.rsa();
        // another global: refused (the caller logs the refusal at ERROR), then moved by the operator's rebind line: an ERROR
        assertFalse(join(r, "rgH", "gc2", other, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        Files.writeString(Path.of(f + ".rebind"), "rgH/gc2=" + PeerTrustRegistry.fingerprint(other.getPublic().getEncoded()) + "\n");
        assertTrue(join(r, "rgH", "gc2", other, PeerTrustRegistry.ROLE_CONTROLLER, true, asGlobal).ok);
        assertTrue(any(errors, "GLOBAL MOVED from rgG/gc to rgH/gc2"), errors.toString());
        // a re-key of a recorded name by the operator: the REBOUND line stays an error
        KeyPair a = TlsTestSupport.rsa(), b = TlsTestSupport.rsa();
        assertTrue(join(r, "rgB", "a1", a, PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        Files.writeString(Path.of(f + ".rebind"), "rgB/a1=" + PeerTrustRegistry.fingerprint(b.getPublic().getEncoded()) + "\n");
        assertTrue(join(r, "rgB", "a1", b, PeerTrustRegistry.ROLE_AGENT, true, null).ok);
        assertTrue(any(errors, "Identity rgB/a1 REBOUND from key", "operator rebind"), errors.toString());
    }
}
