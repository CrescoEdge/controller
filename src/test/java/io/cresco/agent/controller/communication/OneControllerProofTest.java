package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netdiscovery.Release378;
import io.cresco.library.security.Caller;
import io.cresco.library.security.CrescoIdentity;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The independent review of the first "one controller per region" (2026-11-12) broke it; each of its probes is kept here
 * as a test, through real discovery exchanges. The root of most: the secret's exchange never proved the private key of
 * the certificate presented, so a holder of the discovery secret was recorded under ANY certificate it had a copy of
 * (they are public) -- "the controller is its key" was not true where the key is bound. Then: an anchor replaced a
 * recorded controller; two joins at once were both recorded; a region took a second global under a region name it had
 * never seen; two names of one region CA were reported as a conflict; a name that differs by case replaced another's
 * certificates. {@link io.cresco.agent.controller.netdiscovery.DiscoveryResponderProofTest} has the responder's side,
 * {@link io.cresco.agent.controller.netdiscovery.DiscoveryProofOfPossessionTest} the proof itself and what a node of
 * release 1.3.378 (no proof) may still do.
 */
class OneControllerProofTest {

    private static final String SECRET = "a-discovery-secret-every-member-of-its-kind-holds";
    private static final Gson GSON = new Gson();

    private static ControllerEngine controller(String region, String agent, String tenant, Path dir, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        m.put("broker_security_enabled", "true");
        m.put("msg_signing_sessions", "false");
        m.put("tenant_id", tenant);
        if (dir != null) {
            m.put("node_identity_dir", dir.resolve("security").toString());
            m.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        }
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        TlsTestSupport.State st = (TlsTestSupport.State) e.cstate;
        st.regional = !"false".equals(m.get("is_controller"));
        st.global = "true".equals(m.get("is_global"));
        if (st.regional && !st.global) { st.globalRegion = "rgG"; st.globalAgent = "gc"; }
        return e;
    }

    private static ControllerEngine controller(String region, String agent, String tenant, String... extra) throws Exception {
        return controller(region, agent, tenant, null, extra);
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }
    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }
    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }
    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(leaf(e)); }
    private static String pinned(ControllerEngine at, String region, String agent) { return reg(at).pinnedKey(new PeerIdentity.Expected(region, agent)); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type, String[] told) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        if (hello == null) return null;
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.1.2.3");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        if (proof == null) return null;
        DiscoveryAuth.Msg result = s.serverResult(copy(hello), ch, copy(proof), "10.1.2.3");
        told[0] = result.reason;
        return c.clientResult(hello, copy(ch), copy(result), "10.9.9.9", 32005);
    }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server, DiscoveryType type) { return join(client, server, type, new String[1]); }
    private static DiscoveryNode join(ControllerEngine region, ControllerEngine global) { return join(region, global, DiscoveryType.GLOBAL); }

    private static X509Certificate cert(X500Name subject, X500Name issuer, PublicKey key, PrivateKey signer, boolean ca) throws Exception {
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(issuer, new BigInteger(64, new SecureRandom()),
                new Date(now - 60_000L), new Date(now + 86_400_000L), subject, key);
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(ca ? (KeyUsage.keyCertSign | KeyUsage.cRLSign) : (KeyUsage.digitalSignature | KeyUsage.keyEncipherment)));
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").build(signer)));
    }

    /** What a holder of the secret WITHOUT the private key makes: [a leaf carrying somebody else's PUBLIC key under a name of its choice, its own root]. */
    private static X509Certificate[] forged(PublicKey victims, String tenant, String region, String agent) throws Exception {
        KeyPair root = TlsTestSupport.rsa();
        X500Name rn = new X500Name("CN=rootCA-attacker");
        X509Certificate r = cert(rn, rn, root.getPublic(), root.getPrivate(), true);
        X509Certificate l = cert(new X500Name(CrescoIdentity.of(tenant, region, agent, null).toX500Name()), rn, victims, root.getPrivate(), false);
        return new X509Certificate[]{l, r};
    }

    /** A certificate that names {@code region/agent} for {@code holder}'s OWN key (it has the private key): what a controller makes of itself. */
    private static X509Certificate[] renamed(ControllerEngine holder, String tenant, String region, String agent) throws Exception {
        return new X509Certificate[]{cert(new X500Name(CrescoIdentity.of(tenant, region, agent, null).toX500Name()), new X500Name("CN=rootCA-own"),
                leaf(holder).getPublicKey(), holder.getCertificateManager().identityPrivateKey(), false)};
    }

    /**
     * The real exchange as a secret holder of this build runs it, presenting {@code chain} as {@code region/agent}: the
     * responder's RESULT (ok or not), or null when the exchange did not get that far.
     */
    private static DiscoveryAuth.Msg present(ControllerEngine secretHolder, ControllerEngine server, DiscoveryType type, String region, String agent,
                                             X509Certificate[] chain) {
        DiscoveryProcessor c = new DiscoveryProcessor(secretHolder), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        hello.creg = region; hello.cagt = agent; hello.ccert = secretHolder.getCertificateManager().getJsonFromCerts(chain);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.6.6.6");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "10.9.9.9");
        if (proof == null) return null;
        return s.serverResult(copy(hello), ch, copy(proof), "10.6.6.6");
    }

    private static boolean taken(DiscoveryAuth.Msg r) { return r != null && r.ok; }

    /** The CA certificates {@code at}'s truststore holds now (what its TLS trusts a chain to). */
    private static java.util.List<X509Certificate> held(ControllerEngine at) {
        return Arrays.asList(((BoundKeyTrustManager) at.getCertificateManager().currentPkixTrustManager()).trustedIssuers());
    }

    /** Every way a holder of the secret can present {@code chain} without its key: as a node of this build, and as one of release 1.3.378. */
    private static boolean takenWithoutTheKey(ControllerEngine secretHolder, ControllerEngine server, DiscoveryType type, String region, String agent,
                                              X509Certificate[] chain) {
        boolean thisBuild = taken(present(secretHolder, server, type, region, agent, chain));
        boolean earlier = taken(Release378.join(server, type, SECRET, region, agent, chain));
        return thisBuild || earlier;
    }

    // ------------------------------------------------------------------ a certificate without its key

    /**
     * P1. A holder of the global's discovery secret presents a chain of its own making whose leaf carries the PUBLIC key of
     * region rgB's real controller, under a name of its choice. Nothing of it is recorded: not as a second name of rgB's
     * controller, not forty times, not as the controller of a region that has none -- and that region's real controller
     * then joins.
     */
    @Test
    void aCertificateWithoutItsKeyIsRecordedUnderNoName() throws Exception {
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true");
        ControllerEngine real = controller("rgB", "rc", "t1"), attacker = controller("rgA", "rc", "t1");
        assertNotNull(join(real, global));
        assertNotNull(join(attacker, global));
        PeerTrustRegistry g = reg(global);
        PublicKey victims = leaf(real).getPublicKey();
        assertFalse(takenWithoutTheKey(attacker, global, DiscoveryType.GLOBAL, "rgB", "aa", forged(victims, "t1", "rgB", "aa")),
                "a chain with the real controller's PUBLIC key was taken as rgB/aa from a node that does not hold its key");
        assertNull(pinned(global, "rgB", "aa"));
        assertFalse(g.isBridgePeer(CrescoIdentity.of("t1", "rgB", "aa", null)));
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "aa")));
        int accepted = 0;
        for (int i = 0; i < 40; i++) {
            if (takenWithoutTheKey(attacker, global, DiscoveryType.GLOBAL, "rgB", "al" + i, forged(victims, "t1", "rgB", "al" + i))) accepted++;
        }
        assertEquals(0, accepted, "more names of rgB's controller were made without its key");
        // the same public key as the controller of ANOTHER region, which has none yet
        assertFalse(takenWithoutTheKey(attacker, global, DiscoveryType.GLOBAL, "rgC", "aa", forged(victims, "t1", "rgC", "aa")),
                "rgB's public key was taken as region rgC's controller");
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgC", "aa")));
        // so region rgC's real controller joins, and nothing an operator is told to do touches rgB's controller
        String[] told = new String[1];
        assertNotNull(join(controller("rgC", "rc", "t1"), global, DiscoveryType.GLOBAL, told), "region rgC's real controller was refused: " + told[0]);
        assertFalse(g.isRevokedFor(key(real), "rgB"));
        assertNotNull(join(real, global), "rgB's real controller no longer joins");
        assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
    }

    /**
     * P1, the remedy. Even the HOLDER of a key (region rgB's controller itself, proving it) is not taken as another region's
     * controller: a key speaks for one region. So no refusal ever tells an operator to revoke, by name, a key that is also
     * another region's controller.
     */
    @Test
    void aKeyThatIsOneRegionsControllerIsNotTakenAsAnothers() throws Exception {
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true");
        ControllerEngine b = controller("rgB", "rc", "t1");
        assertNotNull(join(b, global));
        DiscoveryAuth.Msg r = present(b, global, DiscoveryType.GLOBAL, "rgC", "aa", renamed(b, "t1", "rgC", "aa"));
        assertFalse(taken(r), "the key of region rgB's controller was taken as region rgC's controller");
        assertTrue(r != null && r.reason != null && r.reason.contains("a key speaks for one region"), r == null ? null : r.reason);
        assertNull(pinned(global, "rgC", "aa"));
        String[] told = new String[1];
        assertNotNull(join(controller("rgC", "rc", "t1"), global, DiscoveryType.GLOBAL, told), told[0]);
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
    }

    /**
     * P5. No second name of the controller's key was made, so after a legitimate re-key of rgB/rc (the operator's rebind
     * line) the OLD key is nobody at the global, and its health is OK.
     */
    @Test
    void afterALegitimateReKeyTheOldKeyIsNobody() throws Exception {
        Path gdir = Files.createTempDirectory("onectl-g");
        Path gf = gdir.resolve("security").resolve("peer-trust.json");
        ControllerEngine global = controller("rgG", "gc", "t1", gdir, "is_global", "true");
        ControllerEngine real = controller("rgB", "rc", "t1"), attacker = controller("rgA", "rc", "t1");
        assertNotNull(join(real, global));
        assertNotNull(join(attacker, global));
        assertFalse(takenWithoutTheKey(attacker, global, DiscoveryType.GLOBAL, "rgB", "zz", forged(leaf(real).getPublicKey(), "t1", "rgB", "zz")));
        ControllerEngine reKeyed = controller("rgB", "rc", "t1");
        assertNull(join(reKeyed, global), "a re-keyed controller joined without the operator's rebind line");
        Files.writeString(Path.of(gf + ".rebind"), "rgB/rc=" + key(reKeyed) + "\n");
        assertNotNull(join(reKeyed, global), "the operator's rebind line did not admit the re-keyed controller");
        String[] h = global.getCertificateManager().identityHealth();
        assertEquals("OK", h[0], h[1]);
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "zz")), "the OLD key still speaks for the region under another name");
        assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgB", "zz", null)));
        assertNull(reg(global).verifiedIdentity(renamed(real, "t1", "rgB", "zz")), "a certificate of the OLD key still verifies at the global");
        assertNull(reg(global).verifiedIdentity(new X509Certificate[]{leaf(real)}), "the OLD certificate still verifies at the global");
    }

    /**
     * The upgrade window (a first join without proof) lets a holder of the secret have a victim's PUBLIC key recorded under a
     * name of its choice. It keeps the victim out of nothing: when the victim joins and proves its key, the record made
     * without it gives way.
     */
    @Test
    void aNameMadeWithoutTheKeyInTheUpgradeWindowDoesNotKeepTheKeysHolderOut() throws Exception {
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", DiscoveryAuth.MODE_KEY, "compat",
                "discovery_compat_until", java.time.LocalDate.now(java.time.ZoneOffset.UTC).plusDays(7).toString());
        ControllerEngine victim = controller("rgV", "rc", "t1");
        PublicKey victims = leaf(victim).getPublicKey();
        DiscoveryAuth.Msg squat = Release378.join(global, DiscoveryType.GLOBAL, SECRET, "rgW", "x", forged(victims, "t1", "rgW", "x"));
        assertTrue(taken(squat), "(the window admits a first join without proof: " + (squat == null ? null : squat.reason) + ")");
        String[] told = new String[1];
        assertNotNull(join(victim, global, DiscoveryType.GLOBAL, told), "the holder of the key was kept out by a record made without it: " + told[0]);
        assertNull(pinned(global, "rgW", "x"), "the record made without the key is still there, under the key of region rgV's controller");
        assertFalse(global.speaksForRegion(Caller.controller("t1", "rgW", "x")));
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgV", "rc")));
    }

    // ------------------------------------------------------------------ controller_trust_anchors admit; they replace nothing

    /**
     * P2. A chain of the attacker's making whose leaf carries the PUBLIC key the operator anchored (region rgA's
     * controller), named rgB/zz in tenant t2: it evicts nobody. Region rgB's controller, tenant and bridge are what they were.
     */
    @Test
    void aForgedChainWithAnAnchoredPublicKeyEvictsNobody() throws Exception {
        ControllerEngine a = controller("rgA", "rc", "t1"), b = controller("rgB", "rc", "t1"), attacker = controller("rgX", "rc", "t1");
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "controller_trust_anchors", key(a));
        assertNotNull(join(a, global));
        assertNotNull(join(b, global));
        assertNotNull(join(attacker, global));
        PeerTrustRegistry g = reg(global);
        assertFalse(takenWithoutTheKey(attacker, global, DiscoveryType.GLOBAL, "rgB", "zz", forged(leaf(a).getPublicKey(), "t2", "rgB", "zz")),
                "a forged chain carrying an anchored PUBLIC key was taken as region rgB's controller");
        assertEquals(key(b), pinned(global, "rgB", "rc"), "region rgB's recorded controller was evicted");
        assertTrue(g.isBridgePeer(CrescoIdentity.of("t1", "rgB", "rc", null)));
        assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")));
        assertEquals("t1", g.tenantOfRegion("rgB"));
        assertNotNull(join(b, global), "region rgB's real controller no longer joins");
    }

    /**
     * P3. The HOLDER of an anchored key -- region rgA's own controller -- names itself a controller of region rgB. An anchor
     * admits a controller; it never replaces a recorded one, and a key speaks for one region: refused, whether rgB's
     * recorded controller is anchored or not, by its key or by its certificate's fingerprint.
     */
    @Test
    void anAnchoredControllerOfOneRegionDoesNotTakeAnother() throws Exception {
        String[] how = {"key(A)", "key(A), certificate(B)", "key(A), key(B)"};
        for (int v = 0; v < 3; v++) {
            ControllerEngine a = controller("rgA", "rc", "t1"), b = controller("rgB", "rc", "t1");
            String anchors = v == 0 ? key(a) : v == 1 ? key(a) + "," + CertificateManager.certFingerprint(leaf(b)) : key(a) + "," + key(b);
            ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true", "controller_trust_anchors", anchors);
            assertNotNull(join(a, global));
            assertNotNull(join(b, global));
            DiscoveryAuth.Msg r = present(a, global, DiscoveryType.GLOBAL, "rgB", "zz", renamed(a, "t1", "rgB", "zz"));
            assertFalse(taken(r), "anchors=" + how[v] + ": region rgA's anchored controller was taken as region rgB's controller");
            assertFalse(global.speaksForRegion(Caller.controller("t1", "rgB", "zz")), how[v]);
            assertFalse(reg(global).isBridgePeer(CrescoIdentity.of("t1", "rgB", "zz", null)), how[v]);
            assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")), "anchors=" + how[v] + ": region rgB's controller was replaced");
            assertTrue(global.speaksForRegion(Caller.controller("t1", "rgA", "rc")), how[v]);
            assertEquals("OK", global.getCertificateManager().identityHealth()[0], how[v]);
        }
    }

    /**
     * The same from the joiner's side: an agent of region rgB whose operator anchored the controllers of regions rgA and rgB.
     * Region rgA's controller answers the agent as "rgB/rc" with its own (anchored) key: an anchor does not vouch for
     * another key of a recorded name either.
     */
    @Test
    void anAnchoredControllerDoesNotReKeyAnotherRegionsControllerAtItsAgents() throws Exception {
        ControllerEngine a = controller("rgA", "rc", "t1"), b = controller("rgB", "rc", "t1");
        ControllerEngine agent = controller("rgB", "a1", "t1", "is_controller", "false", "controller_trust_anchors", key(a) + "," + key(b));
        assertNotNull(join(agent, b, DiscoveryType.AGENT));
        // region rgA's controller under the name rgB/rc: its own node identity re-issued under that name, as a re-install makes it
        Path dir = Files.createTempDirectory("onectl-a");
        ControllerEngine a1 = controller("rgA", "rc", "t1", dir);
        ControllerEngine agent2 = controller("rgB", "a2", "t1", "is_controller", "false", "controller_trust_anchors", key(a1) + "," + key(b));
        assertNotNull(join(agent2, b, DiscoveryType.AGENT));
        ControllerEngine asB = controller("rgB", "rc", "t1", dir);
        assertEquals(key(a1), key(asB), "(the same key, now naming itself rgB/rc)");
        assertNull(join(agent2, asB, DiscoveryType.AGENT), "an agent followed an anchored key of ANOTHER region as its own controller's new key");
        assertEquals(key(b), pinned(agent2, "rgB", "rc"));
        assertNotNull(join(agent2, b, DiscoveryType.AGENT));
    }

    // ------------------------------------------------------------------ two joins at once

    /** P4. Two controllers claim one region at the same instant: one is recorded, every round. */
    @Test
    void ofTwoControllersJoiningAtTheSameInstantOneIsRecorded() throws Exception {
        int rounds = 12;
        for (int i = 0; i < rounds; i++) {
            ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true");
            ControllerEngine[] who = {controller("rgB", "rc", "t1"), controller("rgB", "zz", "t1")};
            DiscoveryProcessor[] c = new DiscoveryProcessor[2], s = new DiscoveryProcessor[2];
            DiscoveryAuth.Msg[] hello = new DiscoveryAuth.Msg[2], ch = new DiscoveryAuth.Msg[2], proof = new DiscoveryAuth.Msg[2], res = new DiscoveryAuth.Msg[2];
            for (int k = 0; k < 2; k++) {
                c[k] = new DiscoveryProcessor(who[k]);
                s[k] = new DiscoveryProcessor(global);
                hello[k] = c[k].clientHello(DiscoveryType.GLOBAL);
                ch[k] = s[k].serverChallenge(copy(hello[k]), "10.1.2." + k);
                proof[k] = c[k].clientProof(hello[k], copy(ch[k]), "10.9.9.9");
            }
            CyclicBarrier gate = new CyclicBarrier(2);
            Thread[] t = new Thread[2];
            for (int k = 0; k < 2; k++) {
                final int kk = k;
                t[k] = new Thread(() -> {
                    try { gate.await(); res[kk] = s[kk].serverResult(copy(hello[kk]), ch[kk], copy(proof[kk]), "10.1.2." + kk); } catch (Exception e) { throw new IllegalStateException(e); }
                });
                t[k].start();
            }
            for (Thread x : t) x.join();
            assertTrue(res[0] != null && res[1] != null, "round " + i);
            assertTrue(res[0].ok ^ res[1].ok, "round " + i + ": of two controllers of one region joining at once, recorded: rc=" + res[0].ok + " zz=" + res[1].ok);
            assertEquals("OK", global.getCertificateManager().identityHealth()[0], global.getCertificateManager().identityHealth()[1]);
            assertTrue(global.speaksForRegion(Caller.controller("t1", "rgB", "rc")) ^ global.speaksForRegion(Caller.controller("t1", "rgB", "zz")), "round " + i);
            // the one refused is told its own remedy
            DiscoveryAuth.Msg no = res[0].ok ? res[1] : res[0];
            assertTrue(no.reason != null && no.reason.contains("already has a controller recorded there") && no.reason.contains("peer-trust.json.rebind"), no.reason);
        }
    }

    // ------------------------------------------------------------------ one global

    /**
     * P11. A region that has its global recorded is answered in a GLOBAL exchange by a holder of the global secret as a
     * "global" of a region name the region has never seen. The global stands above a region: refused, nothing recorded.
     * Moving a region to another global is the operator's act on that region: the rebind line for the NEW global's name and
     * key in its peer-trust.json.rebind -- after which the former global is no longer taken.
     */
    @Test
    void aRegionHasOneGlobalAndOnlyItsOperatorMovesIt() throws Exception {
        Path dir = Files.createTempDirectory("onectl-region");
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true"), region = controller("rgA", "rc", "t1", dir);
        assertNotNull(join(region, global));
        ControllerEngine other = controller("rgH", "zz", "t1", "is_global", "true");
        assertNull(join(region, other), "a region took a second global, under a region name it had never seen");
        assertNull(reg(region).boundRole("rgH", "zz", leaf(other).getPublicKey()), "the second global was recorded as a controller");
        assertFalse(reg(region).isBridgePeer(CrescoIdentity.of("t1", "rgH", "zz", null)), "the second global is a bridge peer");
        assertEquals("OK", region.getCertificateManager().identityHealth()[0]);
        // a controller of a peer region the region knows is not its global either
        ControllerEngine peer = controller("rgP", "rc", "t1", "is_global", "true");
        assertNotNull(join(region, peer, DiscoveryType.REGION), "(a peer region, joined as a peer)");
        assertNull(join(region, peer, DiscoveryType.GLOBAL), "a recorded peer region's controller was taken as the region's global");
        assertNotNull(join(region, global), "the region's own global no longer joins");
        // the operator moves the region: one line on the region, naming the new global and its key
        Files.writeString(dir.resolve("security").resolve("peer-trust.json.rebind"), "rgH/zz=" + key(other) + "\n");
        assertNotNull(join(region, other), "the operator's rebind line for the new global did not move the region");
        assertNull(join(region, global), "the former global was taken again after the region was moved");
        assertNotNull(join(region, other));
        // ... and it holds across the region's restart
        ControllerEngine restarted = controller("rgA", "rc", "t1", dir);
        assertNull(join(restarted, global));
        assertNotNull(join(restarted, other));
    }

    /**
     * Anchors and the one global: an anchored responder is not taken as a region's global while the anchors vouch for the
     * recorded one -- listed by its key or by its certificate, judged alike. Only when the operator took the recorded
     * global OUT of the region's anchors is an anchored one taken in its place without the rebind line.
     */
    @Test
    void anAnchoredResponderIsTakenAsTheGlobalOnlyWhenTheAnchorsNoLongerVouchForTheRecordedOne() throws Exception {
        for (int v = 0; v < 2; v++) {
            ControllerEngine g = controller("rgG", "gc", "t1", "is_global", "true"), g2 = controller("rgH", "gc", "t1", "is_global", "true");
            String anchors = (v == 0 ? key(g) : CertificateManager.certFingerprint(leaf(g))) + "," + key(g2);
            ControllerEngine region = controller("rgA", "rc", "t1", "controller_trust_anchors", anchors);
            assertNotNull(join(region, g));
            assertNull(join(region, g2), "an anchored responder replaced a global the anchors vouch for (listed by " + (v == 0 ? "key" : "certificate") + ")");
            assertNotNull(join(region, g));
        }
        // the operator moves the region by its anchors: the recorded global is no longer listed, the new one is
        Path dir = Files.createTempDirectory("onectl-region");
        ControllerEngine g = controller("rgG", "gc", "t1", "is_global", "true"), g2 = controller("rgH", "gc", "t1", "is_global", "true");
        ControllerEngine region = controller("rgA", "rc", "t1", dir, "controller_trust_anchors", key(g));
        assertNotNull(join(region, g));
        ControllerEngine moved = controller("rgA", "rc", "t1", dir, "controller_trust_anchors", key(g2));
        assertEquals(key(region), key(moved));
        assertNull(join(moved, g), "(the anchors no longer admit the former global)");
        assertNotNull(join(moved, g2), "the region was not moved to the global its anchors now list");
        assertEquals(key(g), pinned(moved, "rgG", "gc"), "the former global was unbound: an anchor replaces nothing that is recorded");
    }

    // ------------------------------------------------------------------ the regional CA is unchanged

    /** P12. Two controller names issued by a region's ONE CA are not "2 CONTROLLERS ... revoke one". */
    @Test
    void twoControllerNamesOfOneRegionCaAreNoConflict() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("onectl-ca").resolve("peer-trust.json");
        PeerTrustRegistry r = PeerTrustRegistry.open(f, java.util.List.of(), java.util.List.of(), 30, m -> { });
        RegionCA ca = RegionCA.generate("t1", "rgB", 2048, 1);
        assertNull(r.trustCa(ca.caCert(), "rgB", "t1", "federated"));
        TlsTestSupport.Id one = TlsTestSupport.issued(ca, "t1", "rgB", "rc"), two = TlsTestSupport.issued(ca, "t1", "rgB", "rc2");
        assertTrue(r.bind("t1", "rgB", "rc", one.chain[0].getPublicKey(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(r.bind("t1", "rgB", "rc2", two.chain[0].getPublicKey(), "federated", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        assertTrue(r.controllerConflicts().isEmpty(), "two names of the region's one CA are reported as a conflict: " + r.controllerConflicts());
    }

    // ------------------------------------------------------------------ the re-key path, and an agent's first discovery

    /** P18. The existing re-key path (same name, another key) at an agent and at a peer region: the operator's rebind line, as before. */
    @Test
    void aReKeyedControllerIsFollowedOnTheOperatorsLineAtAnAgentAndAPeerRegion() throws Exception {
        Path adir = Files.createTempDirectory("onectl-agent"), pdir = Files.createTempDirectory("onectl-peer");
        ControllerEngine rc = controller("rgA", "rc", "t1"), a1 = controller("rgA", "a1", "t1", adir, "is_controller", "false");
        ControllerEngine peer = controller("rgP", "rc", "t1", pdir);
        assertNotNull(join(a1, rc, DiscoveryType.AGENT));
        assertNotNull(join(rc, peer, DiscoveryType.REGION));
        assertNotNull(join(peer, rc, DiscoveryType.REGION));
        ControllerEngine reKeyed = controller("rgA", "rc", "t1");
        assertNull(join(a1, reKeyed, DiscoveryType.AGENT));
        assertNull(join(reKeyed, peer, DiscoveryType.REGION));
        assertNull(join(peer, reKeyed, DiscoveryType.REGION));
        Files.writeString(adir.resolve("security").resolve("peer-trust.json.rebind"), "rgA/rc=" + key(reKeyed) + "\n");
        Files.writeString(pdir.resolve("security").resolve("peer-trust.json.rebind"), "rgA/rc=" + key(reKeyed) + "\n");
        assertNotNull(join(a1, reKeyed, DiscoveryType.AGENT), "the agent did not follow its re-keyed controller on the operator's line");
        assertNotNull(join(reKeyed, peer, DiscoveryType.REGION), "the peer region did not take the re-keyed controller on the operator's line");
        assertNotNull(join(peer, reKeyed, DiscoveryType.REGION));
    }

    /**
     * P7. Under the standard profile an agent takes the first controller that answers its first discovery and holds the
     * agent secret. Squatted, it refuses its real controller -- across a restart -- until its operator acts on the agent:
     * the rebind line for the real controller, or revoking the rogue.
     */
    @Test
    void anAgentSquattedAtItsFirstDiscoveryIsFreedByItsOperator() throws Exception {
        Path adir = Files.createTempDirectory("onectl-agent");
        ControllerEngine rc = controller("rgA", "rc", "t1"), rogue = controller("rgA", "zz", "t1");
        ControllerEngine a1 = controller("rgA", "a1", "t1", adir, "is_controller", "false");
        assertNotNull(join(a1, rogue, DiscoveryType.AGENT), "(first come: the standard profile takes the first controller that answers)");
        assertNull(join(a1, rc, DiscoveryType.AGENT), "an agent took a second controller of its region");
        ControllerEngine restarted = controller("rgA", "a1", "t1", adir, "is_controller", "false");
        assertNull(join(restarted, rc, DiscoveryType.AGENT), "after its restart the agent took a second controller of its region");
        Files.writeString(adir.resolve("security").resolve("peer-trust.json.rebind"), "rgA/rc=" + key(rc) + "\n");
        assertNotNull(join(restarted, rc, DiscoveryType.AGENT), "the rebind line on the agent did not free it");
        assertNull(pinned(restarted, "rgA", "zz"), "the rogue is still recorded at the agent");
        assertNull(join(restarted, rogue, DiscoveryType.AGENT), "the rogue was taken again");
        // the operator's other remedy on the agent: revoke the rogue
        ControllerEngine a3 = controller("rgA", "a3", "t1", Files.createTempDirectory("onectl-agent3"), "is_controller", "false");
        assertNotNull(join(a3, rogue, DiscoveryType.AGENT));
        assertNull(reg(a3).revoke("rgA/zz", "operator"));
        reg(a3).awaitRevocations();
        assertNotNull(join(a3, rc, DiscoveryType.AGENT), "the real controller does not join after the rogue was revoked on the agent");
    }

    // ------------------------------------------------------------------ names that differ only by case

    /**
     * The truststore names its entries in lower case: a controller named rgb/rc -- another region by exact comparison,
     * first come -- replaced the certificates the global held for rgB/rc. A name that equals a recorded one except for
     * letter case is refused, and the recorded one's certificates stay.
     */
    @Test
    void aNameThatDiffersOnlyByCaseDoesNotReplaceTheRecordedOnesCertificates() throws Exception {
        ControllerEngine global = controller("rgG", "gc", "t1", "is_global", "true");
        ControllerEngine real = controller("rgB", "rc", "t1");
        assertNotNull(join(real, global));
        X509Certificate[] chain = real.getCertificateManager().getPublicCertificate();
        X509Certificate root = chain[chain.length - 1];
        assertTrue(held(global).contains(root), "(precondition)");
        for (String[] n : new String[][]{{"rgb", "rc"}, {"RGB", "RC"}, {"rgB", "Rc"}}) {
            ControllerEngine other = controller(n[0], n[1], "t1");
            String[] told = new String[1];
            assertNull(join(other, global, DiscoveryType.GLOBAL, told), n[0] + "/" + n[1] + " was recorded beside rgB/rc");
            assertTrue(held(global).contains(root),
                    "the join of " + n[0] + "/" + n[1] + " removed the certificates the global holds for rgB/rc");
            assertNull(pinned(global, n[0], n[1]));
        }
        assertNotNull(join(real, global));
        // an agent name under the global's own region likewise
        ControllerEngine a1 = controller("rgG", "a1", "t1", "is_controller", "false"), a1b = controller("rgG", "A1", "t1", "is_controller", "false");
        assertNotNull(join(a1, global, DiscoveryType.AGENT));
        assertNull(join(a1b, global, DiscoveryType.AGENT), "an agent name that differs only by case was recorded");
    }
}
