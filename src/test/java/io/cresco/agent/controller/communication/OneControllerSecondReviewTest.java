package io.cresco.agent.controller.communication;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The second review of the proof (2026-11-12) found the proof sound and the rules around it locking out nodes that are
 * what they say: a controller of release 1.3.378 whose agent name is generated at each start; the real global after it
 * proved its key in another kind of exchange, or under a new name of its key; a global recorded in the upgrade window. And
 * a registry restored from an older copy forgot what it had learned without a word, and a chain kept on record stopped
 * vouching when its leaf expired. Each of its registry probes (P1-P6), kept as a test of what must happen instead.
 */
class OneControllerSecondReviewTest {

    private static X509Certificate[] chain(String tenant, String region, String agent, KeyPair kp) throws Exception {
        RegionCA ca = RegionCA.generate(tenant, region, 2048, 1);
        return ca.enroll(kp.getPublic(), tenant, region, agent, 1);
    }

    private static PeerTrustRegistry.BindResult join(PeerTrustRegistry r, String region, String agent, KeyPair kp, String role,
                                                     boolean proved, Consumer<PeerTrustRegistry.Join> more) throws Exception {
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = region; j.agent = agent; j.chain = chain("t1", region, agent, kp); j.role = role; j.proved = proved;
        j.exchange = "GLOBAL";
        if (more != null) more.accept(j);
        return r.join(j);
    }

    private static PeerTrustRegistry open(Path f, List<String> log) { return PeerTrustRegistry.open(f, List.of(), List.of(), 30, log::add); }

    private static Path file() throws Exception {
        TlsTestSupport.provider();
        return Files.createTempDirectory("rev2").resolve("security").resolve("peer-trust.json");
    }

    private static String pinned(PeerTrustRegistry r, String region, String agent) { return r.pinnedKey(new PeerIdentity.Expected(region, agent)); }
    private static String fp(KeyPair k) { return PeerTrustRegistry.fingerprint(k.getPublic().getEncoded()); }

    private static final String C = PeerTrustRegistry.ROLE_CONTROLLER, A = PeerTrustRegistry.ROLE_AGENT;
    private static final Consumer<PeerTrustRegistry.Join> WINDOW = j -> j.window = true;
    private static final Consumer<PeerTrustRegistry.Join> AS_GLOBAL = j -> { j.client = true; j.global = true; };

    /**
     * P1. A region controller of release 1.3.378 without a fixed agentname restarts -- a new generated name, the same key,
     * no proof -- at an upgraded global. Outside the upgrade window it is refused with exactly what to do; inside it the
     * new name REPLACES the former one (one name at a time), unproved, and the key holder's later proof takes over.
     */
    @Test
    void aControllerOfRelease378WithAGeneratedNameIsToldWhatToDoAndIsTakenInTheWindow() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(file(), log);
        KeyPair k = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgB", "agent-old", k.getPublic(), "discovery", C, false).ok);       // as release 1.3.378 recorded it
        PeerTrustRegistry.BindResult closed = join(r, "rgB", "agent-new", k, C, false, null);
        assertFalse(closed.ok);
        assertTrue(closed.reason.contains("recorded as rgB/agent-old") && closed.reason.contains("set agentname=agent-old on that node")
                && closed.reason.contains("upgrade it") && closed.reason.contains("open the upgrade window on this node"), closed.reason);
        assertTrue(closed.toPeer.contains("set agentname=agent-old on this node") && closed.toPeer.contains("upgrade it")
                && closed.toPeer.contains("upgrade window"), closed.toPeer);
        assertEquals(fp(k), pinned(r, "rgB", "agent-old"));
        // the window: the new name replaces the former one, loudly, unproved
        PeerTrustRegistry.BindResult open = join(r, "rgB", "agent-new", k, C, false, WINDOW);
        assertTrue(open.ok, "the window did not take a 1.3.378 controller under its new generated name: " + open.reason);
        assertEquals(fp(k), pinned(r, "rgB", "agent-new"));
        assertNull(pinned(r, "rgB", "agent-old"), "two names of one key");
        assertFalse(r.provedKey("rgB", "agent-new"));
        assertTrue(log.stream().anyMatch(l -> l.contains("UPGRADE WINDOW") && l.contains("rgB/agent-new in place of rgB/agent-old")
                && l.contains("WITHOUT proof")), log.toString());
        // ... one name at a time, at every restart
        assertTrue(join(r, "rgB", "agent-3", k, C, false, WINDOW).ok);
        assertNull(pinned(r, "rgB", "agent-new"));
        assertTrue(r.controllerConflicts().isEmpty());
        // not for another tenant's certificate with that key, and not beside another controller of the region
        PeerTrustRegistry.Join other = new PeerTrustRegistry.Join();
        other.region = "rgB"; other.agent = "agent-x"; other.chain = chain("t2", "rgB", "agent-x", k); other.role = C; other.window = true;
        assertFalse(r.join(other).ok);
        assertFalse(join(r, "rgB", "zz", TlsTestSupport.rsa(), C, false, WINDOW).ok);
        // the same at a joiner of this build for ITS controller of release 1.3.378
        PeerTrustRegistry atAgent = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(atAgent.bind("t1", "rgB", "agent-old", k.getPublic(), "discovery", C, false).ok);
        Consumer<PeerTrustRegistry.Join> itsController = j -> { j.client = true; j.exchange = "AGENT"; };
        PeerTrustRegistry.BindResult no = join(atAgent, "rgB", "agent-new", k, C, false, itsController);
        assertTrue(!no.ok && no.reason.contains("set agentname=agent-old on that node"), no.reason);
        assertTrue(join(atAgent, "rgB", "agent-new", k, C, false, j -> { itsController.accept(j); j.window = true; }).ok);
        // the controller is upgraded: its proof takes over, under whatever name it has then -- and from then on its name changes only on proof
        assertTrue(join(r, "rgB", "agent-4", k, C, true, null).ok);
        assertNull(pinned(r, "rgB", "agent-3"));
        assertTrue(r.provedKey("rgB", "agent-4"));
        PeerTrustRegistry.BindResult back = join(r, "rgB", "agent-5", k, C, false, WINDOW);
        assertFalse(back.ok, "the name of a PROVED key was replaced without proof");
        assertTrue(back.reason.contains("agentname=agent-4"), back.reason);
        // an AGENT of 1.3.378 with a generated name: a first join each time, admitted by the window as before
        KeyPair a = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgA", "agent-1", a.getPublic(), "discovery", A, false).ok);
        PeerTrustRegistry.BindResult agentClosed = join(r, "rgA", "agent-2", a, A, false, null);
        assertTrue(!agentClosed.ok && agentClosed.reason.contains("set agentname=agent-1 on that node"), agentClosed.reason);
        assertTrue(join(r, "rgA", "agent-2", a, A, false, WINDOW).ok);
    }

    /** P2. The real global proves its key in a REGION exchange (regional peering) before the GLOBAL one: it is still what the registry held. */
    @Test
    void theGlobalThatProvedItsKeyInAnotherKindOfExchangeFirstIsStillTakenAsTheGlobal() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair g = TlsTestSupport.rsa(), p = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgG", "gc", g.getPublic(), "discovery", C, false).ok);              // 1.3.378: its global
        assertTrue(r.bind("t1", "rgP", "rc", p.getPublic(), "discovery", C, false).ok);              // 1.3.378: a peer region
        assertTrue(join(r, "rgG", "gc", g, C, true, j -> j.exchange = "REGION").ok);                  // upgraded global, peering first
        PeerTrustRegistry.BindResult res = join(r, "rgG", "gc", g, C, true, AS_GLOBAL);
        assertTrue(res.ok, "the recorded global, with proof, was refused in its GLOBAL exchange: " + res.reason);
        assertEquals("rgG/gc", r.globalRecorded());
        // and the peer is then a known peer
        assertFalse(join(r, "rgP", "rc", p, C, true, AS_GLOBAL).ok);
        // a controller this build records in a REGION exchange was never a candidate
        PeerTrustRegistry r2 = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(r2.bind("t1", "rgG", "gc", g.getPublic(), "discovery", C, false).ok);
        KeyPair z = TlsTestSupport.rsa();
        assertTrue(join(r2, "rgZ", "zc", z, C, true, j -> j.exchange = "REGION").ok);
        assertFalse(join(r2, "rgZ", "zc", z, C, true, AS_GLOBAL).ok, "a controller that joined as a peer under this build was taken as the global");
    }

    /** P3. The global's agent name is generated; it restarted (upgraded) while this region was down: its recorded key under a new name. */
    @Test
    void theGlobalUnderANewNameOfItsRecordedKeyIsTakenAlthoughAPeerIsRecorded() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair g = TlsTestSupport.rsa(), p = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgG", "agent-old", g.getPublic(), "discovery", C, false).ok);
        assertTrue(r.bind("t1", "rgP", "rc", p.getPublic(), "discovery", C, false).ok);
        PeerTrustRegistry.BindResult res = join(r, "rgG", "agent-new", g, C, true, AS_GLOBAL);
        assertTrue(res.ok, "the key this registry holds as a controller of its global's region, proved, was refused: " + res.reason);
        assertEquals("rgG/agent-new", r.globalRecorded());
        assertNull(pinned(r, "rgG", "agent-old"));
        // of release 1.3.378 still (no proof), in the window: taken under its new name, and still what it was -- not yet marked
        PeerTrustRegistry w = open(file(), new CopyOnWriteArrayList<>());
        assertTrue(w.bind("t1", "rgG", "agent-old", g.getPublic(), "discovery", C, false).ok);
        assertTrue(w.bind("t1", "rgP", "rc", p.getPublic(), "discovery", C, false).ok);
        assertTrue(join(w, "rgG", "agent-new", g, C, false, j -> { AS_GLOBAL.accept(j); j.window = true; }).ok);
        assertNull(w.globalRecorded());
        assertTrue(join(w, "rgG", "agent-new", g, C, true, AS_GLOBAL).ok);
        assertEquals("rgG/agent-new", w.globalRecorded());
    }

    /**
     * P4. A global recorded in the upgrade window without proof, which then proves its key in a REGION exchange: the place
     * stays taken (unconfirmed) -- no other responder becomes the global -- until that identity proves its key as the global.
     */
    @Test
    void aGlobalRecordedWithoutProofKeepsItsPlaceUntilItProvesItsKeyAsTheGlobal() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair g = TlsTestSupport.rsa(), x = TlsTestSupport.rsa();
        assertTrue(join(r, "rgG", "gc", g, C, false, j -> { j.client = true; j.global = true; j.window = true; }).ok);   // window: 1.3.378 global
        assertEquals("rgG/gc", r.globalRecorded());
        assertTrue(join(r, "rgG", "gc", g, C, true, j -> j.exchange = "REGION").ok);                 // upgraded; a REGION exchange first
        assertEquals("rgG/gc", r.globalRecorded(), "the node was left without a global");
        PeerTrustRegistry.BindResult any = join(r, "rgX", "xc", x, C, true, AS_GLOBAL);              // any holder of the global secret
        assertFalse(any.ok, "another responder with a proven key was taken as the global");
        assertTrue(any.reason.contains("rgG/gc") && any.reason.contains("a node has ONE global"), any.reason);
        PeerTrustRegistry.BindResult real = join(r, "rgG", "gc", g, C, true, AS_GLOBAL);
        assertTrue(real.ok, real.reason);
        assertEquals("rgG/gc", r.globalRecorded());
    }

    /** P5. A registry restored from a copy made before the proof says so at open, and what the operator should check. */
    @Test
    void aRegistryRestoredFromAnOlderCopySaysSoAtOpen() throws Exception {
        Path f = file();
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = open(f, log);
        KeyPair g = TlsTestSupport.rsa();
        assertTrue(r.bind("t1", "rgG", "gc", g.getPublic(), "discovery", C, false).ok);
        r.close();
        Path dir = f.getParent(), backup = dir.resolveSibling("security.bak");
        Files.createDirectories(backup);
        try (var s = Files.list(dir)) { for (Path p : (Iterable<Path>) s::iterator) Files.copy(p, backup.resolve(p.getFileName())); }
        r = open(f, log);
        assertTrue(join(r, "rgG", "gc", g, C, true, AS_GLOBAL).ok);
        assertTrue(r.provedKey("rgG", "gc"));
        assertEquals("rgG/gc", r.globalRecorded());
        r.close();
        // a restart as it is: nothing to say
        log.clear();
        r = open(f, log);
        assertTrue(log.stream().noneMatch(l -> l.contains("BEHIND")), log.toString());
        assertTrue(r.provedKey("rgG", "gc"));
        r.close();
        try (var s = Files.list(dir)) { for (Path p : (Iterable<Path>) s::iterator) Files.delete(p); }
        try (var s = Files.list(backup)) { for (Path p : (Iterable<Path>) s::iterator) Files.copy(p, dir.resolve(p.getFileName())); }
        log.clear();
        PeerTrustRegistry back = open(f, log);
        assertFalse(back.isSealed());
        assertFalse(back.provedKey("rgG", "gc"), "(what the copy did not hold is gone: that cannot be helped)");
        assertTrue(log.stream().anyMatch(l -> l.contains("BEHIND ITS OWN RECORD") && l.contains("restored from an older copy")
                        && l.contains("PROVED their key") && l.contains("GLOBAL") && l.contains("rebind lines") && l.contains("Check the controllers recorded here")),
                "a registry restored from a copy made before the proof says nothing: " + log);
    }

    private static X509Certificate cert(String subject, String issuer, java.security.PublicKey key, java.security.PrivateKey signer, boolean ca,
                                        long notBefore, long notAfter) throws Exception {
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(new X500Name(issuer), new BigInteger(64, new SecureRandom()),
                new Date(notBefore), new Date(notAfter), new X500Name(subject), key);
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").build(signer)));
    }

    /**
     * P6. The chain kept with a recorded controller follows the chain it presents when it proves its key again (a renewed
     * leaf), and "do the anchors vouch for the recorded one" is judged without validity dates: a leaf on record that has
     * expired is still what the operator's anchor vouched for.
     */
    @Test
    void theChainKeptWithAControllerFollowsItsRenewedLeafAndIsJudgedWithoutDates() throws Exception {
        PeerTrustRegistry r = open(file(), new CopyOnWriteArrayList<>());
        KeyPair g = TlsTestSupport.rsa();
        RegionCA ca = RegionCA.generate("t1", "rgG", 2048, 1);
        X509Certificate[] first = ca.enroll(g.getPublic(), "t1", "rgG", "gc", 1), renewed = ca.enroll(g.getPublic(), "t1", "rgG", "gc", 1);
        assertNotEquals(first[0], renewed[0]);
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = "rgG"; j.agent = "gc"; j.chain = first; j.role = C; j.proved = true; j.client = true; j.global = true; j.exchange = "GLOBAL";
        assertTrue(r.join(j).ok);
        assertEquals(first[0], r.recordedChain("rgG/gc")[0]);
        j.chain = renewed;                                                   // the global renewed its leaf (same key) and proves again
        assertTrue(r.join(j).ok);
        assertEquals(renewed[0], r.recordedChain("rgG/gc")[0], "the kept chain is frozen at the first proof");
        // an exchange WITHOUT proof never rewrites it
        j.chain = first; j.proved = false; j.window = true;
        r.join(j);
        assertEquals(renewed[0], r.recordedChain("rgG/gc")[0]);
        // a chain on record whose leaf has expired: with today's date it has no path; judged as a record, it has
        KeyPair root = TlsTestSupport.rsa(), leaf = TlsTestSupport.rsa();
        long now = System.currentTimeMillis(), day = 86_400_000L;
        X509Certificate rootCert = cert("CN=rootCA-x", "CN=rootCA-x", root.getPublic(), root.getPrivate(), true, now - 30 * day, now + 365 * day);
        X509Certificate expired = cert("CN=old-leaf", "CN=rootCA-x", leaf.getPublic(), root.getPrivate(), false, now - 30 * day, now - day);
        X509Certificate[] kept = {expired, rootCert};
        java.util.Set<String> anchors = java.util.Set.of(CertificateManager.certFingerprint(rootCert));   // the CA, as an operator lists it
        assertNull(AnchorPath.path(kept, anchors, new Date()), "(a PRESENTED chain with an expired leaf is not vouched for)");
        assertNotNull(AnchorPath.path(kept, anchors, null), "a chain kept on record stops being vouched for when its leaf expires");
    }

    /**
     * A region of release 1.3.378 under the regional CA recorded the CA of its global and of its peers alike, and no identity
     * of the controllers it joined. Upgraded, it takes as its global only a controller whose leaf one of THOSE CAs issued --
     * not whoever answers its first GLOBAL exchange with a proven key -- and settles which CA is whose when its global proves.
     */
    @Test
    void anUpgradedRegionUnderTheRegionalCaTakesAsItsGlobalOnlyAControllerOfACaItHolds() throws Exception {
        Path f = file();
        PeerTrustRegistry r = open(f, new CopyOnWriteArrayList<>());
        RegionCA caG = RegionCA.generate("t1", "rgG", 2048, 1);
        RegionCA caP = RegionCA.generate("t1", "rgP", 2048, 1);
        RegionCA caH = RegionCA.generate("t1", "rgH", 2048, 1);
        assertNull(r.trustCa(caG.caCert(), "rgG", "t1", "federated"));      // as release 1.3.378 recorded them
        assertNull(r.trustCa(caP.caCert(), "rgP", "t1", "federated"));
        r.close();
        r = open(f, new CopyOnWriteArrayList<>());
        java.security.KeyPair g = TlsTestSupport.rsa(), h = TlsTestSupport.rsa(), p = TlsTestSupport.rsa();
        java.util.function.BiFunction<RegionCA, Object[], PeerTrustRegistry.Join> asGlobal = (ca, who) -> {
            try {
                PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
                j.region = (String) who[0]; j.agent = (String) who[1];
                j.chain = ca.enroll(((java.security.KeyPair) who[2]).getPublic(), "t1", j.region, j.agent, 1);
                j.role = C; j.proved = true; j.client = true; j.global = true; j.exchange = "GLOBAL";
                return j;
            } catch (Exception e) { throw new IllegalStateException(e); }
        };
        PeerTrustRegistry.BindResult stranger = r.join(asGlobal.apply(caH, new Object[]{"rgH", "hc", h}));
        assertFalse(stranger.ok, "a controller of a region this registry never held was taken as its global, on a proven key");
        assertTrue(stranger.reason.contains("written by an earlier release") && stranger.reason.contains("rgG") && stranger.reason.contains("rgP"), stranger.reason);
        assertTrue(r.join(asGlobal.apply(caG, new Object[]{"rgG", "gc", g})).ok, "the controller of a region whose CA this registry holds was refused as its global");
        assertEquals("rgG/gc", r.globalRecorded());
        assertFalse(r.join(asGlobal.apply(caP, new Object[]{"rgP", "rc", p})).ok, "a peer region's controller was taken as the global after the global was marked");
        // settled for good: after a restart, and after the global is revoked (the next one is first come)
        r.close();
        r = open(f, new CopyOnWriteArrayList<>());
        assertEquals("rgG/gc", r.globalRecorded());
        assertNull(r.revoke("rgG/gc", "operator"));
        r.awaitRevocations();
        assertTrue(r.join(asGlobal.apply(caH, new Object[]{"rgH", "hc", h})).ok);
    }
}
