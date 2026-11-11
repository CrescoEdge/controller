package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.X509KeyManager;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The identity review's follow-ups: CRL freshness (nextUpdate) and CRL numbers that only go up; the role a presented
 * certificate carries, never one remembered for its name; an atomic hot reload (never a half-loaded state); the region CA's
 * key encrypted under the operator's password file; a tombstone lifted only after the PKI accepts the chain; a forced
 * revocation epoch that cannot make a peer skip positions.
 */
class IdentityFollowUpsTest {

    private static final long DAY = 24L * 3600 * 1000;
    private static PkiTestSupport.Ca root, rgA;
    private static Path pki, anchors;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        root = PkiTestSupport.root("Cresco Fabric Root");
        rgA = PkiTestSupport.region(root, "t1", "rgA");
        pki = Files.createTempDirectory("idfu");
        anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
    }

    private static String pem(X509Certificate c) throws Exception {
        return "-----BEGIN CERTIFICATE-----\n" + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(c.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
    }

    /** A revocation file holding region CA rgA (it signs its own CRL) and its CRL {@code number}, due at {@code next}. */
    private static Path crlFile(Path f, long number, Date next, BigInteger... revoked) throws Exception {
        long now = System.currentTimeMillis();
        Files.writeString(f, pem(rgA.cert) + PkiTestSupport.crl(rgA, BigInteger.valueOf(number), new Date(now - 3600_000L), next, revoked));
        PkiTestSupport.ownerOnly(f);
        return f;
    }

    private static ProvisionedIdentity.Revocations read(Path f, long graceMs) throws Exception {
        return ProvisionedIdentity.Revocations.read(f, List.of(root.cert), graceMs);
    }

    // ------------------------------------------------------------------ CRL freshness and CRL numbers

    @Test
    void anOutOfDateCrlRefusesItsIssuersCertificatesUntilItIsRefreshed() throws Exception {
        long now = System.currentTimeMillis();
        PkiTestSupport.Node a1 = PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY);
        PkiTestSupport.Node g = PkiTestSupport.global(root, "t1", "glob", "gc", now - 60_000, 10 * DAY);
        Path f = Files.createTempDirectory("idfu-crl").resolve("revocations.pem");

        ProvisionedIdentity.Revocations fresh = read(crlFile(f, 5, new Date(now + DAY)), 0);
        assertNull(fresh.refusal(a1.chain), "a certificate under a fresh CRL was refused");
        assertTrue(fresh.summary().contains("next CRL update due"), fresh.summary());

        ProvisionedIdentity.Revocations stale = read(crlFile(f, 6, new Date(now - 3600_000L)), 0);
        String why = stale.refusal(a1.chain);
        assertNotNull(why, "a certificate under an out-of-date CRL was accepted");
        assertTrue(why.contains("out of date"), why);
        assertNull(stale.refusal(g.chain), "a certificate of another CA (the root's) was refused for rgA's stale CRL");
        assertEquals(List.of(rgA.cert.getSubjectX500Principal().getName()), stale.staleIssuers(now));
        assertNull(read(f, 2 * 3600_000L).refusal(a1.chain), "the configured grace past nextUpdate was not honoured");

        // a CRL whose freshness or order cannot be judged is refused outright
        Files.writeString(f, pem(rgA.cert) + PkiTestSupport.crl(rgA, BigInteger.ONE, new Date(now - 60_000), null));
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> read(f, 0), "a CRL without nextUpdate");
        Files.writeString(f, pem(rgA.cert) + PkiTestSupport.crl(rgA, null, new Date(now - 60_000), new Date(now + DAY)));
        assertThrows(ProvisionedIdentity.ProvisioningException.class, () -> read(f, 0), "a CRL without a CRL number");
    }

    @Test
    void crlNumbersOnlyGoUp() throws Exception {
        long now = System.currentTimeMillis();
        Path f = Files.createTempDirectory("idfu-num").resolve("revocations.pem");
        ProvisionedIdentity.Revocations r10 = read(crlFile(f, 10, new Date(now + DAY)), 0);
        String dn = rgA.cert.getSubjectX500Principal().getName();
        assertEquals(BigInteger.TEN, r10.crlNumbers().get(dn));
        assertNull(r10.rollbackRefusal(Map.of(dn, BigInteger.TEN)), "the same CRL again");
        assertNull(r10.rollbackRefusal(Map.of(dn, BigInteger.valueOf(9))), "a newer CRL");
        String back = r10.rollbackRefusal(Map.of(dn, BigInteger.valueOf(11)));
        assertNotNull(back, "an older CRL put back was accepted");
        assertTrue(back.contains("BACKWARDS"), back);
        // the CRL of an issuer that is still trusted cannot simply disappear; one no longer trusted at all may go
        Files.writeString(f, pem(rgA.cert));
        ProvisionedIdentity.Revocations none = read(f, 0);
        assertNotNull(none.rollbackRefusal(Map.of(dn, BigInteger.TEN)), "removing a trusted CA's CRL un-revoked what it listed");
        assertNull(none.rollbackRefusal(Map.of("CN=retired CA,O=t9", BigInteger.TEN)), "a retired CA's record blocked the file");
    }

    private static ControllerEngine provisioned(PkiTestSupport.Node n, String region, String agent, Path revocations, Path idDir,
                                                String... extra) throws Exception {
        Path dir = Files.createTempDirectory("idfu-node-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("identity_keystore_reload_seconds", "3600");
        cfg.put("node_identity_dir", idDir.toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        if (revocations != null) cfg.put("revocation_file", revocations.toString());
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, region, agent);
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned());
        return e;
    }

    private static void touch(Path f, long plusMs) throws Exception {
        Files.setLastModifiedTime(f, FileTime.fromMillis(Files.getLastModifiedTime(f).toMillis() + plusMs));
    }

    @Test
    void anOlderCrlPutBackIsRefusedAtReloadAndAtStart() throws Exception {
        long now = System.currentTimeMillis();
        Path dir = Files.createTempDirectory("idfu-rb");
        PkiTestSupport.Node a1 = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY), dir.resolve("a1"));
        BigInteger victim = new BigInteger(63, new SecureRandom()).add(BigInteger.ONE);
        Path f = crlFile(dir.resolve("revocations.pem"), 20, new Date(now + DAY), victim);
        Path idDir = Files.createDirectories(dir.resolve("security"));
        ControllerEngine e = provisioned(a1, "rgA", "a1", f, idDir);
        assertTrue(Files.readString(idDir.resolve("crl-numbers")).contains("20\t"), "the CRL number in force was not recorded");

        // the operator's newer CRL is taken; an older one put back is refused (the newer revocations stay in force)
        crlFile(f, 21, new Date(now + DAY), victim);
        touch(f, 2000);
        assertTrue(e.getCertificateManager().reloadProvisionedIfChanged(), "a newer CRL was not loaded");
        crlFile(f, 19, new Date(now + DAY));
        touch(f, 4000);
        assertFalse(e.getCertificateManager().reloadProvisionedIfChanged(), "an older CRL put back was loaded");
        assertTrue(Files.readString(idDir.resolve("crl-numbers")).contains("21\t"));
        // and a restart with it refuses to start
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> provisioned(a1, "rgA", "a1", f, idDir));
        assertTrue(String.valueOf(refused.getMessage()).contains("BACKWARDS"), refused.getMessage());
    }

    // ------------------------------------------------------------------ the role of the presented certificate

    @Test
    void theRoleIsTheOneThePresentedCertificateCarries() throws Exception {
        long now = System.currentTimeMillis();
        java.security.KeyPair kp = TlsTestSupport.rsa();
        // the same name and key, issued twice by the PKI: as a controller, and (demoted) as an agent
        PkiTestSupport.Node asController = PkiTestSupport.node(rgA, kp, "t1", "rgA", "rx", true, now - 60_000, 10 * DAY);
        PkiTestSupport.Node asAgent = PkiTestSupport.node(rgA, kp, "t1", "rgA", "rx", false, now - 60_000, 10 * DAY);
        PeerTrustRegistry reg = new PeerTrustRegistry();
        reg.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.NONE);
        CrescoIdentity id = CrescoIdentity.of("t1", "rgA", "rx", null);
        assertNotNull(reg.verifiedIdentity(asController.chain));
        assertFalse(reg.isBridgePeer(id, asController.chain[0]), "a controller certificate alone made a bridge peer (no discovery with this node)");
        assertNull(reg.allowBridgePeer("t1", "rgA", "rx"));                          // rgA/rx completed discovery with this node
        for (int round = 0; round < 2; round++) {
            // whichever was verified last, each connection is judged by its own certificate
            assertNotNull(reg.verifiedIdentity(round == 0 ? asController.chain : asAgent.chain));
            assertNotNull(reg.verifiedIdentity(round == 0 ? asAgent.chain : asController.chain));
            assertTrue(reg.isBridgePeer(id, asController.chain[0]), "the controller certificate may not bridge (round " + round + ")");
            assertFalse(reg.isBridgePeer(id, asAgent.chain[0]), "the agent certificate may bridge by the name's other certificate (round " + round + ")");
            assertEquals(PeerTrustRegistry.ROLE_CONTROLLER, reg.boundRole("rgA", "rx", asController.chain));
            assertEquals(PeerTrustRegistry.ROLE_AGENT, reg.boundRole("rgA", "rx", asAgent.chain));
        }
        assertFalse(reg.isBridgePeer(id, null), "a connection without a certificate");
        assertNull(reg.boundRole("rgA", "other", asController.chain), "a certificate of another name");
    }

    // ------------------------------------------------------------------ atomic hot reload

    @Test
    void aHandshakeKeepsOneChainAndKeyAcrossAReload() throws Exception {
        long now = System.currentTimeMillis();
        PkiTestSupport.Node first = PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY);
        PkiTestSupport.Node second = PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY);   // a new key
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("rgA_a1", first.kp.getPrivate(), "pw".toCharArray(), first.chain);
        javax.net.ssl.KeyManagerFactory f = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        f.init(ks, "pw".toCharArray());
        NodeIdentityKeyManager.Identity[] current = {new NodeIdentityKeyManager.Identity(first.chain, first.kp.getPrivate(), 1)};
        NodeIdentityKeyManager km = new NodeIdentityKeyManager((X509KeyManager) f.getKeyManagers()[0], () -> "rgA_a1")
                .withCurrentIdentity(() -> current[0]);
        String alias = km.chooseClientAlias(new String[]{"RSA"}, null, null);
        assertNotNull(alias);
        // the reload lands between the handshake's two lookups: it still gets the pair it started with
        current[0] = new NodeIdentityKeyManager.Identity(second.chain, second.kp.getPrivate(), 2);
        assertEquals(first.chain[0], km.getCertificateChain(alias)[0]);
        assertEquals(first.kp.getPrivate(), km.getPrivateKey(alias));
        String next = km.chooseServerAlias("RSA", null, null);
        assertEquals(second.chain[0], km.getCertificateChain(next)[0], "the next handshake did not present the reloaded chain");
        assertEquals(second.kp.getPrivate(), km.getPrivateKey(next));
    }

    @Test
    void aReloadPublishesTheNewKeyWithItsChainAndNeverEmptiesTheTrustStore() throws Exception {
        long now = System.currentTimeMillis();
        Path dir = Files.createTempDirectory("idfu-rl");
        PkiTestSupport.Node a1 = PkiTestSupport.write(PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY), dir.resolve("a1"));
        ControllerEngine e = provisioned(a1, "rgA", "a1", null, Files.createDirectories(dir.resolve("security")));
        CertificateManager cm = e.getCertificateManager();
        KeyStore before = trustStore(cm);
        int anchorsBefore = before.size();
        PkiTestSupport.Node renewed = PkiTestSupport.node(rgA, "t1", "rgA", "a1", false, now - 60_000, 20 * DAY);
        renewed.keystore = a1.keystore;
        renewed.password = a1.password;
        rewriteKeystore(renewed);
        touch(a1.keystore, 2000);
        assertTrue(cm.reloadProvisionedIfChanged());
        NodeIdentityKeyManager.Identity id = cm.presentedIdentity();
        assertEquals(renewed.chain[0], id.chain[0]);
        assertTrue(signsFor(id.chain[0], id.key), "the presented key is not the presented certificate's");
        assertTrue(signsFor(cm.getPublicCertificate()[0], cm.identityPrivateKey()), "the signing key and the certificate disagree");
        assertNotSame(before, trustStore(cm), "the trust store was refilled in place (a handshake could see it empty)");
        assertEquals(anchorsBefore, before.size(), "the trust store in force was emptied while the new one was made");
    }

    private static KeyStore trustStore(CertificateManager cm) throws Exception {
        java.lang.reflect.Field f = CertificateManager.class.getDeclaredField("trustStore");
        f.setAccessible(true);
        return (KeyStore) f.get(cm);
    }

    private static void rewriteKeystore(PkiTestSupport.Node n) throws Exception {
        String pass = Files.readAllLines(n.password).get(0).trim();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("node", n.kp.getPrivate(), pass.toCharArray(), n.chain);
        Path tmp = n.keystore.resolveSibling("node.p12.tmp");
        try (java.io.OutputStream o = Files.newOutputStream(tmp)) { ks.store(o, pass.toCharArray()); }
        Files.move(tmp, n.keystore, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        PkiTestSupport.ownerOnly(n.keystore);
    }

    /** Does the private half sign what {@code cert}'s public key verifies (one pair)? */
    private static boolean signsFor(X509Certificate cert, PrivateKey privatePart) throws Exception {
        Signature s = Signature.getInstance("SHA384withRSA");
        s.initSign(privatePart);
        s.update(new byte[]{1, 2, 3});
        byte[] sig = s.sign();
        s.initVerify(cert.getPublicKey());
        s.update(new byte[]{1, 2, 3});
        return s.verify(sig);
    }

    // ------------------------------------------------------------------ the region CA's key at rest

    private static ControllerEngine regionCaNode(Path dir, String... extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("security_regional_ca", "true");
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "rc");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, "rgA", "rc");
        st.regional = true;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        e.getCertificateManager().ensureRegionCA();
        assertTrue(e.getCertificateManager().isRegionCA());
        return e;
    }

    private static Path findStore(Path dir) throws Exception {
        try (java.util.stream.Stream<Path> s = Files.walk(dir)) {
            return s.filter(p -> p.getFileName().toString().equals("region-ca.p12")).findFirst().orElse(null);
        }
    }

    @Test
    void theRegionCaKeyIsEncryptedUnderTheOperatorsPasswordFile() throws Exception {
        Path dir = Files.createTempDirectory("idfu-ca");
        // made before the setting existed: under the random password beside it
        X509Certificate ca = regionCaNode(dir).getCertificateManager().getPublicCertificate()[1];
        Path store = findStore(dir);
        assertNotNull(store);
        Path sibling = store.resolveSibling("region-ca.p12.pass");
        assertTrue(Files.exists(sibling));
        // the operator's password file: the key is re-encrypted under it, and the password beside it is removed
        Path secrets = Files.createDirectories(Files.createTempDirectory("idfu-secret").resolve("s"));
        Path pw = secrets.resolve("region-ca.pass");
        Files.writeString(pw, "an-operator-region-ca-password-not-real\n");
        PkiTestSupport.ownerOnly(pw);
        ControllerEngine again = regionCaNode(dir, CertificateManager.REGION_CA_PASSWORD_FILE, pw.toString());
        assertEquals(ca.getPublicKey(), again.getCertificateManager().getPublicCertificate()[1].getPublicKey(), "the CA changed");
        assertFalse(Files.exists(sibling), "the password beside the CA key was kept");
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (java.io.InputStream in = Files.newInputStream(store)) { ks.load(in, "an-operator-region-ca-password-not-real".toCharArray()); }
        assertNotNull(ks.getKey("region-ca", "an-operator-region-ca-password-not-real".toCharArray()), "not encrypted under the operator's password");
        // restarted with it: the same CA; without it (or with another), the CA is unavailable (never silently a new one)
        assertEquals(ca.getPublicKey(), regionCaNode(dir, CertificateManager.REGION_CA_PASSWORD_FILE, pw.toString())
                .getCertificateManager().getPublicCertificate()[1].getPublicKey());
        Files.writeString(pw, "another-password-that-is-wrong-not-real\n");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("security_regional_ca", "true");
        cfg.put(CertificateManager.REGION_CA_PASSWORD_FILE, pw.toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, "rgA", "rc");
        ControllerEngine wrong = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, "rgA", "rc");
        st.regional = true;
        wrong.cstate = st;
        wrong.setCertificateManager(new CertificateManager(wrong));
        wrong.getCertificateManager().ensureRegionCA();
        assertFalse(wrong.getCertificateManager().isRegionCA(), "a CA key opened with the wrong password");
        assertNotNull(wrong.getCertificateManager().regionCaFailure());
    }

    // ------------------------------------------------------------------ tombstones and the PKI

    @Test
    void aRebindLiftsATombstoneOnlyForAChainThePkiAccepts() throws Exception {
        long now = System.currentTimeMillis();
        PkiTestSupport.Node good = PkiTestSupport.node(rgA, "t1", "rgA", "a3", false, now - 60_000, 10 * DAY);
        // the same new key under a CA that is not of the fabric's PKI
        PkiTestSupport.Ca rogue = PkiTestSupport.region(PkiTestSupport.root("Rogue Root"), "t1", "rgA");
        PkiTestSupport.Node bad = PkiTestSupport.node(rogue, good.kp, "t1", "rgA", "a3", false, now - 60_000, 10 * DAY);
        String fp = PeerTrustRegistry.fingerprint(good.kp.getPublic().getEncoded());
        PeerTrustRegistry reg = new PeerTrustRegistry().withRebind(List.of("rgA/a3=" + fp));
        reg.setPki(List.of(root.cert), ProvisionedIdentity.Revocations.NONE);
        assertNull(reg.revoke("rgA/a3", "operator"));
        assertTrue(reg.isTombstoned("rgA", "a3"));
        String[] why = new String[1];
        assertNull(reg.verifiedIdentity(bad.chain, why), "a chain outside the PKI was accepted");
        assertTrue(reg.isTombstoned("rgA", "a3"), "a chain the PKI refuses lifted the tombstone (and spent the operator's rebind)");
        assertNotNull(reg.verifiedIdentity(good.chain, why), "the re-provisioned chain was refused: " + why[0]);
        assertFalse(reg.isTombstoned("rgA", "a3"));
    }

    // ------------------------------------------------------------------ forced revocation epochs

    private static String randomFp() {
        byte[] b = new byte[32];
        new SecureRandom().nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    @Test
    void withinAnHourOfANewEpochAStaleAcknowledgementIsNotTrusted() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        PeerTrustRegistry reg = region.getCertificateManager().getPeerTrust();
        assertNull(reg.revoke(randomFp(), "operator"));
        // the parent acknowledges more of this node's list than it has: a new epoch (once an hour)
        String e0 = reg.revocationEpoch();
        MsgEvent pong = new MsgEvent(MsgEvent.Type.EXEC, "glob", "gc", null, "rgA", "rc", null, true, false);
        pong.setParam(RevocationSync.EPOCH, "parent-epoch");
        pong.setParam(RevocationSync.ACK, e0 + ".99");
        RevocationSync.onParentPong(region, pong, "glob", "gc");
        String e1 = reg.revocationEpoch();
        assertNotEquals(e0, e1);
        // again within the hour: no new epoch -- and the acknowledgement is not trusted, so this node's list is sent again
        assertNull(reg.revoke(randomFp(), "operator"));
        MsgEvent pong2 = new MsgEvent(MsgEvent.Type.EXEC, "glob", "gc", null, "rgA", "rc", null, true, false);
        pong2.setParam(RevocationSync.EPOCH, "parent-epoch");
        pong2.setParam(RevocationSync.ACK, e1 + ".99");
        RevocationSync.onParentPong(region, pong2, "glob", "gc");
        assertEquals(e1, reg.revocationEpoch(), "a second new epoch within the hour");
        MsgEvent ping = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "rc", null, "glob", "gc", null, true, false);
        RevocationSync.onChildPing(region, ping);
        assertNotNull(ping.getParam(RevocationSync.CHILD_LIST), "a stale acknowledgement kept this node's revocations from its parent");
    }

    @Test
    void withinAnHourOfANewEpochAChildAheadIsServedTheWholeList() throws Exception {
        ControllerEngine region = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) region.cstate).regional = true;
        ControllerEngine agent = TlsTestSupport.engine("rgA", "a1", Map.of("broker_security_enabled", "true"));
        PeerTrustRegistry reg = region.getCertificateManager().getPeerTrust();
        assertTrue(reg.bind("t1", "rgA", "a1", agent.getCertificateManager().getPublicCertificate()[0].getPublicKey(), "discovery").ok);
        assertNull(reg.revoke(randomFp(), "operator"));
        for (int i = 0; i < 2; i++) {
            MsgEvent q = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "rc", null, true, false);
            q.setParam(RevocationSync.HAVE, reg.revocationEpoch() + ".99");
            q.setParam("cresco_caller", "agent:t1/rgA/a1");
            RevocationSync.onPingAtParent(region, q, false);
            assertNotNull(q.getParam(RevocationSync.PARENT_LIST), "round " + i + ": a child ahead of this registry was not served the whole list");
        }
    }
}
