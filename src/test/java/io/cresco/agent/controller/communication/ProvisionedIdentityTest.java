package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pre-provisioned identities (security_profile=production): a node's certificate and key come from the operator's PKI and
 * the fabric's trust anchors; nothing is generated, enrolled or learned on first use.
 */
class ProvisionedIdentityTest {

    private static final long DAY = 24L * 3600 * 1000;

    /** One PKI per test: a root, region rgr's CA, and the anchors bundle (root + region CA). */
    private static final class Fabric {
        final PkiTestSupport.Ca root, rgr;
        final Path dir, anchors;
        Fabric() throws Exception {
            root = PkiTestSupport.root("Cresco Fabric Root");
            rgr = PkiTestSupport.region(root, "t1", "rgr");
            dir = Files.createTempDirectory("pki");
            anchors = PkiTestSupport.pem(dir.resolve("trust-anchors.pem"), root.cert, rgr.cert);
        }
        PkiTestSupport.Node node(String region, String agent, boolean controller) throws Exception {
            PkiTestSupport.Ca issuer = "rgr".equals(region) ? rgr : root;
            return PkiTestSupport.write(PkiTestSupport.node(issuer, "t1", region, agent, controller, System.currentTimeMillis() - 60_000, 365 * DAY),
                    dir.resolve(region + "_" + agent));
        }
    }

    private static Map<String, Object> config(Fabric f, PkiTestSupport.Node n, String... extra) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", f.anchors.toString());
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        return cfg;
    }

    private static ControllerEngine engine(String region, String agent, boolean regional, Map<String, Object> cfg) throws Exception {
        TlsTestSupport.provider();
        Path dir = Files.createTempDirectory("prov-" + agent);
        cfg.putIfAbsent("public_key_directory", dir.resolve("public-keys").toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = regional;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static io.cresco.library.plugin.Config cfgOf(Map<String, Object> m) throws Exception {
        return TestAgentService.plugin(Files.createTempDirectory("cfg").toString(), m, x -> { }, "rgr", "x").getConfig();
    }

    // ------------------------------------------------------------------ start

    @Test
    void aProvisionedNodeUsesItsOperatorIdentityAndGeneratesNothing() throws Exception {
        Fabric f = new Fabric();
        PkiTestSupport.Node rc = f.node("rgr", "rc", true);
        ControllerEngine e = engine("rgr", "rc", true, config(f, rc, "security_regional_ca", "true", "is_region", "true"));
        CertificateManager cm = e.getCertificateManager();
        assertTrue(cm.isProvisioned());
        assertEquals(rc.chain[0], cm.getPublicCertificate()[0], "the node does not present its provisioned certificate");
        cm.ensureRegionCA();
        assertFalse(cm.isRegionCA(), "a provisioned node made a region CA");
        PkiTestSupport.Node other = f.node("rgr", "b9", false);
        assertNull(cm.issueEnrollment(cm.getJsonFromCerts(other.chain)), "a provisioned node issued an identity");
        assertFalse(cm.ensureLeafIdentity(), "a provisioned leaf was re-issued");
        assertFalse(cm.renewIfDue(), "a provisioned leaf was renewed by the node");
        Path security = Path.of(e.getPluginBuilder().getAgentService().getAgentDataDirectory()).resolve("security");
        assertFalse(Files.exists(security.resolve("node-identity.p12")), "a node identity was generated and saved");
        assertFalse(Files.exists(security.resolve("region-ca.p12")), "a region CA was generated");
        assertEquals("OK", cm.identityHealth()[0], cm.identityHealth()[1]);
        assertEquals(CrescoIdentity.of("t1", "rgr", "rc", null), CrescoNames.identityOf(cm.getPublicCertificate()[0]));
    }

    @Test
    void anUnusableProvisioningRefusesToStart() throws Exception {
        Fabric f = new Fabric();
        long now = System.currentTimeMillis();
        PkiTestSupport.Ca stranger = PkiTestSupport.root("Not Our Root");
        Map<String, PkiTestSupport.Node> bad = new HashMap<>();
        bad.put("expired", PkiTestSupport.write(PkiTestSupport.node(f.rgr, "t1", "rgr", "a1", false, now - 10 * DAY, 5 * DAY), f.dir.resolve("expired")));
        bad.put("unknown root", PkiTestSupport.write(PkiTestSupport.node(stranger, "t1", "rgr", "a1", false, now - DAY, 30 * DAY), f.dir.resolve("stranger")));
        bad.put("another region's CA", PkiTestSupport.write(PkiTestSupport.node(f.rgr, "t1", "rgx", "a1", false, now - DAY, 30 * DAY), f.dir.resolve("scope")));
        for (Map.Entry<String, PkiTestSupport.Node> b : bad.entrySet()) {
            String why = CertificateManager.provisioningRefusal(cfgOf(config(f, b.getValue())));
            System.out.println("provisioned (" + b.getKey() + "): " + why);
            assertNotNull(why, b.getKey() + " was accepted");
        }
        PkiTestSupport.Node good = f.node("rgr", "a1", false);
        assertNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good))));
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good, "agentname", "a2"))), "a certificate for another name was accepted");
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good, "is_region", "true"))), "an agent certificate started a controller");
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good, "tenant_id", "t2"))), "a certificate for another tenant was accepted");
        Files.setPosixFilePermissions(good.password, java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good))), "a world-readable password file was accepted");
        Files.setPosixFilePermissions(good.password, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        // revoked by the operator: deny list (key) and a CRL-free name entry
        Path rev = f.dir.resolve("revocations.pem");
        Files.writeString(rev, "key:" + PeerTrustRegistry.fingerprint(good.kp.getPublic().getEncoded()) + "  # stolen\n");
        PkiTestSupport.ownerOnly(rev);
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(config(f, good, "revocation_file", rev.toString()))), "a revoked node started");
        assertThrows(IllegalStateException.class, () -> engine("rgr", "a1", false, config(f, good, "revocation_file", rev.toString())),
                "the certificate manager of a revoked node came up");
        // security_profile=production without a keystore
        Map<String, Object> none = new HashMap<>();
        none.put("security_profile", "production");
        assertNotNull(CertificateManager.provisioningRefusal(cfgOf(none)), "production without a provisioned identity was accepted");
    }

    // ------------------------------------------------------------------ verification by the PKI

    @Test
    void peersAreJudgedByThePkiNeverByFirstUse() throws Exception {
        Fabric f = new Fabric();
        ControllerEngine rc = engine("rgr", "rc", true, config(f, f.node("rgr", "rc", true), "is_region", "true"));
        PeerTrustRegistry reg = rc.getCertificateManager().getPeerTrust();
        PkiTestSupport.Node b1 = f.node("rgr", "b1", false), gc = f.node("glob", "gc", true);
        assertEquals(CrescoIdentity.of("t1", "rgr", "b1", null), reg.verifiedIdentity(b1.chain));
        assertFalse(reg.isBridgePeer(CrescoIdentity.of("t1", "rgr", "b1", null)), "an agent's certificate made a bridge peer");
        assertEquals(CrescoIdentity.of("t1", "glob", "gc", null), reg.verifiedIdentity(gc.chain));
        assertTrue(reg.isBridgePeer(CrescoIdentity.of("t1", "glob", "gc", null)), "a controller of the PKI is not a bridge peer");
        // a different key for b1, also issued by the PKI (the operator re-issued it): accepted -- the PKI decides, not first use
        PkiTestSupport.Node b1again = f.node("rgr", "b1", false);
        assertNotNull(reg.verifiedIdentity(b1again.chain));
        // a self-made chain naming b1, and one from another root: refused; nothing is bound
        assertNull(reg.verifiedIdentity(TlsTestSupport.selfRooted("x", "t1", "rgr", "b1").chain));
        assertNull(reg.verifiedIdentity(PkiTestSupport.node(PkiTestSupport.root("Other"), "t1", "rgr", "b1", false,
                System.currentTimeMillis() - 60_000, DAY).chain));
        assertNull(reg.boundByKey(b1.chain), "a key was trusted by a binding in a pre-provisioned fabric");
        assertFalse(reg.bind("t1", "rgr", "b7", b1.kp.getPublic(), "discovery").ok, "a pre-provisioned registry bound a joiner");
        // revocation: the operator's name revocation, and a key revocation from a signed list, both hold
        assertNull(reg.revoke("rgr/b1", "operator"));
        assertNull(reg.verifiedIdentity(b1again.chain), "a revoked name verified");
        PkiTestSupport.Node a2 = f.node("rgr", "a2", false);
        reg.mergeRevocations(java.util.List.of(PeerTrustRegistry.fingerprint(a2.kp.getPublic().getEncoded())), "the global's list", null);
        assertNull(reg.verifiedIdentity(a2.chain), "a key revoked by a signed list verified");
    }

    // ------------------------------------------------------------------ discovery without enrollment

    @Test
    void anAgentJoinsItsRegionWithNoEnrollmentAndNoPin() throws Exception {
        Fabric f = new Fabric();
        ControllerEngine rc = engine("rgr", "rc", true, config(f, f.node("rgr", "rc", true), "is_region", "true",
                "discovery_secret_agent", "agent-secret"));
        ControllerEngine b1 = engine("rgr", "b1", false, config(f, f.node("rgr", "b1", false), "discovery_secret_agent", "agent-secret"));
        assertNotNull(io.cresco.agent.controller.netdiscovery.DiscoveryTestHooks.join(b1, rc), "a provisioned agent could not join its region");
        assertNull(rc.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgr", "b1")), "the region bound the joiner");
        assertNull(b1.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgr", "rc")), "the agent pinned its controller");
        // a node with a certificate from another root, an agent of another region, and an agent answering as a controller: refused
        PkiTestSupport.Ca other = PkiTestSupport.root("Other");
        PkiTestSupport.Node foreign = PkiTestSupport.write(PkiTestSupport.node(other, "t1", "rgr", "b2", false, System.currentTimeMillis() - 60_000, DAY),
                f.dir.resolve("foreign"));
        Path otherAnchors = PkiTestSupport.pem(f.dir.resolve("other-anchors.pem"), other.cert);
        Map<String, Object> fc = config(f, foreign, "discovery_secret_agent", "agent-secret");
        fc.put("trust_anchors", otherAnchors.toString());                         // it trusts its own root
        ControllerEngine stranger = engine("rgr", "b2", false, fc);
        assertNull(io.cresco.agent.controller.netdiscovery.DiscoveryTestHooks.join(stranger, rc), "a certificate of another PKI joined");
        ControllerEngine wrongRegion = engine("glob", "b3", false, config(f, f.node("glob", "b3", false), "discovery_secret_agent", "agent-secret"));
        assertNull(io.cresco.agent.controller.netdiscovery.DiscoveryTestHooks.join(wrongRegion, rc), "an agent of another region joined");
        ControllerEngine fakeController = engine("rgr", "b4", true, config(f, f.node("rgr", "b4", false), "discovery_secret_agent", "agent-secret"));
        assertNull(io.cresco.agent.controller.netdiscovery.DiscoveryTestHooks.join(b1, fakeController), "an agent answered as a controller");
    }

    // ------------------------------------------------------------------ renewal: the operator replaces the keystore

    @Test
    void aReplacedKeystoreIsReloadedWithoutARestartAndABadOneIsRefused() throws Exception {
        Fabric f = new Fabric();
        PkiTestSupport.Node b1 = f.node("rgr", "b1", false);
        ControllerEngine e = engine("rgr", "b1", false, config(f, b1));
        CertificateManager cm = e.getCertificateManager();
        X509Certificate first = cm.getPublicCertificate()[0];
        javax.net.ssl.X509KeyManager km = (javax.net.ssl.X509KeyManager) cm.getKeyManagers()[0];
        // the operator issues a new certificate (new key) and replaces the keystore in place
        PkiTestSupport.Node renewed = PkiTestSupport.node(f.rgr, "t1", "rgr", "b1", false, System.currentTimeMillis() - 60_000, 730 * DAY);
        renewed.keystore = b1.keystore; renewed.password = b1.password;
        PkiTestSupport.write(renewed, b1.keystore.getParent());
        Files.setLastModifiedTime(b1.keystore, FileTime.fromMillis(System.currentTimeMillis() + 2000));
        assertTrue(cm.reloadProvisionedIfChanged(), "the replaced keystore was not reloaded");
        assertEquals(renewed.chain[0], cm.getPublicCertificate()[0]);
        assertNotEquals(first, cm.getPublicCertificate()[0]);
        String alias = e.cstate.getAgentPath();
        assertEquals(renewed.kp.getPrivate(), km.getPrivateKey(alias), "a key manager made before the reload does not present the new key");
        assertEquals(renewed.chain[0], km.getCertificateChain(alias)[0]);
        // a replacement that does not validate (another root) is refused: the node keeps its identity, CRITICAL
        PkiTestSupport.Node rogue = PkiTestSupport.node(PkiTestSupport.root("Rogue"), "t1", "rgr", "b1", false, System.currentTimeMillis() - 60_000, DAY);
        PkiTestSupport.write(rogue, b1.keystore.getParent());
        Files.setLastModifiedTime(b1.keystore, FileTime.fromMillis(System.currentTimeMillis() + 4000));
        assertFalse(cm.reloadProvisionedIfChanged());
        assertEquals(renewed.chain[0], cm.getPublicCertificate()[0], "a rogue keystore replaced the identity");
        assertEquals("CRITICAL", cm.identityHealth()[0], cm.identityHealth()[1]);
    }

    @Test
    void anIdentityNearItsExpiryIsAlarmed() throws Exception {
        Fabric f = new Fabric();
        PkiTestSupport.Node b1 = PkiTestSupport.write(PkiTestSupport.node(f.rgr, "t1", "rgr", "b1", false,
                System.currentTimeMillis() - 300 * DAY, 310 * DAY), f.dir.resolve("old"));
        CertificateManager cm = engine("rgr", "b1", false, config(f, b1)).getCertificateManager();
        String[] h = cm.identityHealth();
        assertEquals("WARN", h[0], h[1]);
        PkiTestSupport.Node last = PkiTestSupport.write(PkiTestSupport.node(f.rgr, "t1", "rgr", "b2", false,
                System.currentTimeMillis() - 360 * DAY, 362 * DAY), f.dir.resolve("last"));
        h = engine("rgr", "b2", false, config(f, last)).getCertificateManager().identityHealth();
        assertEquals("CRITICAL", h[0], h[1]);
    }

    @Test
    void theOperatorsRevocationFileIsReadAgainWhenItChanges() throws Exception {
        Fabric f = new Fabric();
        Path rev = f.dir.resolve("revocations.pem");
        Files.writeString(rev, "# none yet\n");
        PkiTestSupport.ownerOnly(rev);
        ControllerEngine rc = engine("rgr", "rc", true, config(f, f.node("rgr", "rc", true), "is_region", "true", "revocation_file", rev.toString()));
        PkiTestSupport.Node b1 = f.node("rgr", "b1", false), b2 = f.node("rgr", "b2", false);
        PeerTrustRegistry reg = rc.getCertificateManager().getPeerTrust();
        assertNotNull(reg.verifiedIdentity(b1.chain));
        Files.writeString(rev, "key:" + PeerTrustRegistry.fingerprint(b1.kp.getPublic().getEncoded()) + "\nname:rgr/b2\n");
        Files.setLastModifiedTime(rev, FileTime.fromMillis(System.currentTimeMillis() + 2000));
        assertTrue(rc.getCertificateManager().reloadProvisionedIfChanged());
        assertNull(reg.verifiedIdentity(b1.chain), "a key the operator's file revokes verified");
        assertNull(reg.verifiedIdentity(b2.chain), "a name the operator's file revokes verified");
        assertNotNull(reg.refusedKey(b1.chain), "TLS does not refuse a revoked key");
    }
}
