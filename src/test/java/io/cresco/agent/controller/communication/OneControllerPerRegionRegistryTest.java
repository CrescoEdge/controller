package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.measurement.MsgMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * "A region has one controller here", at the registry that decides it ({@link PeerTrustRegistry#claimController}): what is
 * the same controller and what is another, what the refusal says to the operator, how the operator's rebind line and
 * anchors replace a controller, the count and the rate of the log line, and what a registry of before the rule reports.
 * {@link OneControllerPerRegionTest} holds the behaviour through real discovery exchanges.
 */
class OneControllerPerRegionRegistryTest {

    private static String fp(java.security.PublicKey k) { return PeerTrustRegistry.fingerprint(k.getEncoded()); }

    private static X509Certificate[] chain(String tenant, String region, String agent, KeyPair kp) throws Exception {
        TlsTestSupport.Id root = TlsTestSupport.selfRooted("r", tenant, region, agent);
        // a self-rooted chain as a per-node-trust node makes it, for the key given (anyone can make one for any key)
        RegionCA ca = RegionCA.generate(tenant, region, 2048, 1);
        return ca.enroll(kp.getPublic(), tenant, region, agent, 1);
    }

    private static PeerTrustRegistry.ControllerRefusal claim(PeerTrustRegistry r, String tenant, String region, String agent, KeyPair kp,
                                                              String vouched, boolean record) throws Exception {
        return r.claimController(tenant, region, agent, chain(tenant, region, agent, kp), vouched, null, record);
    }

    private static void bound(PeerTrustRegistry r, String tenant, String region, String agent, KeyPair kp) {
        assertTrue(r.bind(tenant, region, agent, kp.getPublic(), "discovery", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        r.allowBridgePeer(region, agent);
    }

    /** What is the same controller, what is another, and what the refusal tells the operator. */
    @Test
    void anotherKeyIsAnotherControllerAndTheRefusalSaysWhatToDo() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("onectl").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, m -> { });
        KeyPair rc = TlsTestSupport.rsa(), other = TlsTestSupport.rsa();
        // nothing recorded for the region: first come, as before
        assertNull(claim(r, "t1", "rgB", "rc", rc, null, false));
        bound(r, "t1", "rgB", "rc", rc);
        // the same name: that name's own rules (a re-join, a re-key), as before
        assertNull(claim(r, "t1", "rgB", "rc", rc, null, false));
        assertNull(claim(r, "t1", "rgB", "rc", other, null, false));
        // the same key under another agent name, in the same tenant: the same controller
        assertNull(claim(r, "t1", "rgB", "agent-77", rc, null, false));
        // ... but not when the certificate states another tenant
        assertNotNull(claim(r, "t2", "rgB", "agent-77", rc, null, false), "the region's key under another tenant was taken for its controller");
        // an agent bound under the region is no controller; another region is another matter
        assertTrue(r.bind("t1", "rgB", "b1", TlsTestSupport.rsa().getPublic(), "discovery").ok);
        assertNull(claim(r, "t1", "rgC", "rc", other, null, false));
        // another key under another name: refused, in every tenant
        PeerTrustRegistry.ControllerRefusal no = claim(r, "t1", "rgB", "zz", other, null, false);
        assertNotNull(no);
        assertNotNull(claim(r, "t2", "rgB", "zz", other, null, false));
        assertEquals("rgB", no.region);
        assertEquals("rgB/rc", no.recordedName);
        assertTrue(no.recorded.contains("rgB/rc (tenant t1, key " + fp(rc.getPublic()).substring(0, 16)), no.recorded);
        // the operator's two cases, with the exact lines and files
        assertTrue(no.remedy.contains("If rgB/zz is to REPLACE it") && no.remedy.contains("'rgB/zz=" + fp(other.getPublic()) + "'")
                && no.remedy.contains(f + ".rebind") && no.remedy.contains("controller_trust_anchors"), no.remedy);
        assertTrue(no.remedy.contains("If rgB/rc is NOT that region's controller (it took the region before the real one joined), revoke it here")
                && no.remedy.contains("'rgB/rc'") && no.remedy.contains(f + ".revoke"), no.remedy);
        // nothing was recorded by asking
        assertNull(r.pinnedKey(new PeerIdentity.Expected("rgB", "zz")));
        assertTrue(r.controllerConflicts().isEmpty());
        // a pre-provisioned node is not asked: the operator's PKI decides there
        PeerTrustRegistry pki = new PeerTrustRegistry();
        bound(pki, "t1", "rgB", "rc", rc);
        pki.setPki(List.of(PkiTestSupport.root("x").cert), null);
        assertNull(claim(pki, "t1", "rgB", "zz", other, null, false));
        assertTrue(pki.controllerConflicts().isEmpty());
    }

    /** A refusal is counted every time and logged once per joiner, then at most every ten minutes; the joiners remembered are bounded. */
    @Test
    void refusalsAreCountedAndTheirLineIsRateLimited() throws Exception {
        PeerTrustRegistry r = new PeerTrustRegistry();
        String k = "ab".repeat(32);
        assertEquals(0, r.controllersRefusedCount());
        assertTrue(r.controllerRefused("rgB", "zz", k), "the first refusal of a joiner is not logged");
        for (int i = 0; i < 50; i++) assertFalse(r.controllerRefused("rgB", "zz", k), "said again within ten minutes");
        assertEquals(51, r.controllersRefusedCount());
        assertEquals(51, r.controllerRefusedTimes("rgB", "zz", k));
        assertTrue(r.controllerRefused("rgB", "zz", "cd".repeat(32)), "the same name with another key is another joiner");
        assertTrue(r.controllerRefused("rgB", "yy", k));
        // ten minutes on, it is said again
        java.lang.reflect.Field f = PeerTrustRegistry.class.getDeclaredField("refusedControllers");
        f.setAccessible(true);
        @SuppressWarnings("unchecked") Map<String, long[]> seen = (Map<String, long[]>) f.get(r);
        seen.get("rgB/zz|" + k)[1] -= PeerTrustRegistry.REFUSED_CONTROLLER_LOG_MS + 1;
        assertTrue(r.controllerRefused("rgB", "zz", k));
        assertFalse(r.controllerRefused("rgB", "zz", k));
        // a holder of the secret that tries a thousand names does not get a thousand lines, nor unbounded memory
        int logged = 0;
        for (int i = 0; i < 5 * PeerTrustRegistry.REFUSED_CONTROLLERS_MAX; i++) if (r.controllerRefused("rgB", "n" + i, k)) logged++;
        assertTrue(seen.size() <= PeerTrustRegistry.REFUSED_CONTROLLERS_MAX, "the joiners remembered are not bounded: " + seen.size());
        assertTrue(logged <= PeerTrustRegistry.REFUSED_CONTROLLERS_MAX + 2, logged + " lines");
        assertEquals(55 + 5L * PeerTrustRegistry.REFUSED_CONTROLLERS_MAX, r.controllersRefusedCount());
    }

    /** The count is in the metric inventory, next to the broker's own. */
    @Test
    void theCountIsInTheMetricInventory() throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgG", "gc", Map.of("broker_security_enabled", "true"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Map<String, String> names = MsgMetrics.register(registry, e);
        assertEquals("broker", names.get("broker.controller.refused"));
        assertEquals(0.0, registry.get("broker.controller.refused").functionCounter().count());
        e.getCertificateManager().getPeerTrust().controllerRefused("rgB", "zz", "ab".repeat(32));
        assertEquals(1.0, registry.get("broker.controller.refused").functionCounter().count());
    }

    /** The operator's rebind line, and a voucher, replace what is recorded -- in one durable write, with the line consumed. */
    @Test
    void theOperatorsRebindLineAndAVoucherReplaceTheRecordedController() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("onectl").resolve("security").resolve("peer-trust.json");
        List<String> log = new CopyOnWriteArrayList<>();
        PeerTrustRegistry r = PeerTrustRegistry.open(f, List.of(), List.of(), 30, log::add);
        KeyPair rc = TlsTestSupport.rsa(), rc2 = TlsTestSupport.rsa(), rc3 = TlsTestSupport.rsa(), rc4 = TlsTestSupport.rsa();
        bound(r, "t1", "rgB", "rc", rc);
        assertNotNull(claim(r, "t1", "rgB", "rc2", rc2, null, true));
        // a line for the name with another key is not for this controller; a line without a key takes the first key presented (said)
        Files.writeString(Path.of(f + ".rebind"), "rgB/rc2=" + fp(rc3.getPublic()) + "\n");
        assertNotNull(claim(r, "t1", "rgB", "rc2", rc2, null, true));
        Files.writeString(Path.of(f + ".rebind"), "rgB/rc2=" + fp(rc2.getPublic()) + "\nrgB/rc4\n");
        Files.setLastModifiedTime(Path.of(f + ".rebind"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
        // asked without recording: allowed, and nothing changes yet
        assertNull(claim(r, "t1", "rgB", "rc2", rc2, null, false));
        assertEquals(fp(rc.getPublic()), r.pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        log.clear();
        assertNull(claim(r, "t1", "rgB", "rc2", rc2, null, true));
        assertNull(r.pinnedKey(new PeerIdentity.Expected("rgB", "rc")), "the replaced controller is still bound");
        assertFalse(r.isBridgePeer(io.cresco.library.security.CrescoIdentity.of("t1", "rgB", "rc", null)));
        assertFalse(r.isRevokedKey(fp(rc.getPublic())), "a replaced controller's key is revoked by the replacement");
        assertTrue(log.stream().anyMatch(l -> l.contains("Controller of region rgB REPLACED by the operator") && l.contains("rgB/rc (tenant t1")
                && l.contains("rgB/rc2") && l.contains("now consumed") && l.contains("NOT revoked")), log.toString());
        bound(r, "t1", "rgB", "rc2", rc2);                                             // (the caller binds it then)
        // the line is consumed: the same name and key replace nothing again once another controller holds the region
        PeerTrustRegistry again = PeerTrustRegistry.open(f, List.of(), List.of(), 30, log::add);
        assertNull(again.pinnedKey(new PeerIdentity.Expected("rgB", "rc")));
        assertEquals(fp(rc2.getPublic()), again.pinnedKey(new PeerIdentity.Expected("rgB", "rc2")));
        // the keyless line: the first key presented under that name -- and the log says that it named no key
        log.clear();
        assertNull(claim(again, "t1", "rgB", "rc4", rc4, null, true));
        assertTrue(log.stream().anyMatch(l -> l.contains("names no key") && l.contains("rgB/rc4")), log.toString());
        assertNull(again.pinnedKey(new PeerIdentity.Expected("rgB", "rc2")));
        bound(again, "t1", "rgB", "rc4", rc4);
        assertNotNull(claim(again, "t1", "rgB", "rc4b", TlsTestSupport.rsa(), null, true), "a consumed keyless line admitted another controller");
        // a voucher the caller verified (controller_trust_anchors, a signed rebind statement) replaces too -- but not a
        // controller whose own key the operator anchored
        KeyPair v = TlsTestSupport.rsa();
        java.util.function.Predicate<String> rc4Anchored = k -> k.equals(fp(rc4.getPublic()));
        assertNotNull(again.claimController("t1", "rgB", "v", chain("t1", "rgB", "v", v), "its key is listed in controller_trust_anchors", rc4Anchored, true));
        assertEquals(fp(rc4.getPublic()), again.pinnedKey(new PeerIdentity.Expected("rgB", "rc4")));
        assertNull(again.claimController("t1", "rgB", "v", chain("t1", "rgB", "v", v), "an operator-signed rebind statement", null, true));
        assertNull(again.pinnedKey(new PeerIdentity.Expected("rgB", "rc4")));
    }

    /** A registry of before the rule: what it reports, and when it stops. Aliases of one key are one controller. */
    @Test
    void aDoubleRegistryIsReportedUntilTheOperatorRevokesOne() throws Exception {
        TlsTestSupport.provider();
        PeerTrustRegistry r = new PeerTrustRegistry();
        KeyPair rc = TlsTestSupport.rsa(), zz = TlsTestSupport.rsa();
        bound(r, "t1", "rgB", "rc", rc);
        bound(r, "t1", "rgB", "agent-old", rc);                                        // the same controller under a former generated name
        bound(r, "t1", "rgC", "rc", TlsTestSupport.rsa());
        assertTrue(r.controllerConflicts().isEmpty(), r.controllerConflicts().toString());
        bound(r, "t1", "rgB", "zz", zz);
        List<String> c = r.controllerConflicts();
        assertEquals(1, c.size(), c.toString());
        assertTrue(c.get(0).contains("2 CONTROLLERS are recorded here for region rgB") && c.get(0).contains("rgB/rc") && c.get(0).contains("rgB/zz")
                && c.get(0).contains("Each keeps working as it is recorded") && c.get(0).contains("Revoke the one that is not"), c.get(0));
        // no further one is taken; what is recorded is asked nothing
        assertNotNull(claim(r, "t1", "rgB", "yy", TlsTestSupport.rsa(), null, false));
        assertNull(claim(r, "t1", "rgB", "zz", zz, null, false));
        assertNull(claim(r, "t1", "rgB", "agent-new", rc, null, false), "a recorded controller's key under a new name was refused");
        assertNull(r.revoke("rgB/zz", "operator"));
        assertTrue(r.controllerConflicts().isEmpty(), r.controllerConflicts().toString());
        // the same key recorded under two tenants is two controllers
        PeerTrustRegistry t = new PeerTrustRegistry();
        bound(t, "t1", "rgB", "rc", rc);
        bound(t, "t2", "rgB", "zz", rc);
        assertEquals(1, t.controllerConflicts().size());
    }
}
