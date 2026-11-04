package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Fourth adversarial review, continued (local only). Each test asserts the SECURE / correct outcome; a failure is a finding. */
class Review4ExtraTest {

    private static final Gson GSON = new Gson();
    private static final long DAY = 24L * 3600 * 1000;
    private final List<Connection> connections = new ArrayList<>();
    private final List<BrokerService> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService b : brokers) b.stop();
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static ControllerEngine engineAt(Path dir, String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("discovery_secret_agent", "agent-secret");
        cfg.put("discovery_secret_region", "region-secret");
        cfg.put("discovery_secret_global", "global-secret");
        cfg.put("broker_security_enabled", "true");
        if (extra != null) cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State s = new TlsTestSupport.State(pb, region, agent);
        s.regional = regional;
        e.cstate = s;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static boolean tlsTrustsClient(ControllerEngine ctl, X509Certificate[] chain) {
        try {
            for (TrustManager tm : ctl.getCertificateManager().getTrustManagers()) {   // what the broker's TLS context uses
                if (tm instanceof X509TrustManager) { ((X509TrustManager) tm).checkClientTrusted(chain, "RSA"); return true; }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    /** One v2 exchange in which an on-path attacker (who holds NO discovery secret) replaced the joiner's HELLO chain. */
    private static DiscoveryNode joinThroughMitm(ControllerEngine joiner, ControllerEngine responder, DiscoveryType type,
                                                 X509Certificate[] forgedHelloChain) {
        DiscoveryProcessor c = new DiscoveryProcessor(joiner), s = new DiscoveryProcessor(responder);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        DiscoveryAuth.Msg tampered = copy(hello);
        tampered.ccert = joiner.getCertificateManager().getJsonFromCerts(forgedHelloChain);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(tampered), "joiner-via-mitm");
        assertNotNull(ch, "precondition: the responder answers the (tampered) hello");
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "responder-via-mitm");
        assertNotNull(proof, "precondition: the real joiner authenticates the responder");
        DiscoveryAuth.Msg r = s.serverResult(copy(tampered), ch, copy(proof), "joiner-via-mitm");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    // ------------------------------------------------------------------ discovery v2: the chains are not in the transcript

    /**
     * Regional CA. The v2 transcript covers only the SHA-384 of each LEAF KEY, not the chains. A region joining its global
     * presents [leaf, itsRegionCA, root] in HELLO; federateController records chain[1] as region rgB's CA (strict, first come).
     * An on-path attacker with NO secret re-issues the joiner's leaf (same public key, same subject) under its OWN CA for
     * rgB: every MAC still verifies, the exchange succeeds for both ends, and the global records the attacker's CA as rgB's.
     * The attacker then mints a certificate for any unbound rgB name and the global verifies it.
     */
    @Test
    void aDiscoveryManInTheMiddleCannotReplaceAJoiningRegionsCa() throws Exception {
        ControllerEngine global = engineAt(Files.createTempDirectory("r4x-g"), "rgG", "gc", true, Map.of("security_regional_ca", "true"));
        ControllerEngine region = engineAt(Files.createTempDirectory("r4x-b"), "rgB", "rc", true, Map.of("security_regional_ca", "true"));
        global.getCertificateManager().ensureRegionCA();
        region.getCertificateManager().ensureRegionCA();
        X509Certificate[] real = region.getCertificateManager().getPublicCertificate();
        RegionCA evil = RegionCA.generate("t1", "rgB", 2048, 1);                       // the attacker's own CA for region rgB
        X509Certificate[] forged = evil.enroll(real[0].getPublicKey(), "t1", "rgB", "rc", 1);   // the joiner's key, re-issued
        DiscoveryNode joined = joinThroughMitm(region, global, DiscoveryType.GLOBAL, forged);
        System.out.println("MITM federation: exchange completed for the real region = " + (joined != null)
                + "; global's recorded CA for rgB = " + global.getCertificateManager().getPeerTrust().caState("rgB"));
        TlsTestSupport.Id minted = TlsTestSupport.issued(evil, "t1", "rgB", "a7");
        CrescoIdentity id = global.getCertificateManager().getPeerTrust().verifiedIdentity(minted.chain);
        boolean tls = tlsTrustsClient(global, minted.chain);
        System.out.println("attacker-minted rgB/a7: global verifies identity=" + id + "; global broker TLS accepts=" + tls);
        assertNull(id, "a man in the middle without any secret replaced region rgB's CA at the global: it mints verified rgB identities");
    }

    /**
     * Per-node trust. The responder adds the joiner's WHOLE chain to its truststore (configureCertTrust), but only the leaf
     * key is authenticated. An on-path attacker with no secret re-issues the joiner's leaf under its own root: the root
     * becomes a trust anchor of the controller's broker, so any certificate the attacker mints passes its TLS (and such an
     * unverified certificate is not quarantined: Review4Test shows it reads and writes the fabric-shared topics).
     */
    @Test
    void aDiscoveryManInTheMiddleCannotPlantATrustAnchor() throws Exception {
        ControllerEngine ctl = engineAt(Files.createTempDirectory("r4x-c"), "rgA", "rc", true, null);
        ControllerEngine agent = engineAt(Files.createTempDirectory("r4x-a"), "rgA", "a1", false, null);
        X509Certificate[] real = agent.getCertificateManager().getPublicCertificate();
        RegionCA attacker = RegionCA.generate("t1", "rgA", 2048, 1);
        X509Certificate[] forged = attacker.enroll(real[0].getPublicKey(), "t1", "rgA", "a1", 1);
        TlsTestSupport.Id minted = TlsTestSupport.issued(attacker, "t1", "rgA", "ghost");
        assertFalse(tlsTrustsClient(ctl, minted.chain), "precondition: the attacker's root is not trusted before");
        DiscoveryNode joined = joinThroughMitm(agent, ctl, DiscoveryType.AGENT, forged);
        boolean tls = tlsTrustsClient(ctl, minted.chain);
        System.out.println("MITM agent join: completed=" + (joined != null) + "; controller TLS now accepts an attacker-minted chain=" + tls);
        assertFalse(tls, "a man in the middle without any secret planted a trust anchor in the controller's broker TLS");
    }

    // ------------------------------------------------------------------ pruning a peer that never reconnects

    /**
     * Last-seen is refreshed only when a connection is VERIFIED (addConnection) or a bridge is used; an agent whose single
     * connection stays up refreshes nothing however much it sends or receives. 91 days later its controller restarts and
     * open() prunes it: the agent's reconnect then fails TLS (no binding, no anchor) and the name is free, first come.
     */
    @Test
    @SuppressWarnings("unchecked")
    void aPeerConnectedThroughoutIsNotPrunedOnTheNextRestart() throws Exception {
        TlsTestSupport.provider();
        Path f = Files.createTempDirectory("r4x-p").resolve("security").resolve("peer-trust.json");
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.bindingPruneDays = 90;
        PeerTrustRegistry reg = PeerTrustRegistry.open(f, o, m -> System.out.println("[registry] " + m));
        TlsTestSupport.Id a1 = TlsTestSupport.selfRooted("a1", "t1", "rgA", "a1");
        assertTrue(reg.bind("t1", "rgA", "a1", a1.kp.getPublic(), "discovery").ok);
        RegionCA caA = RegionCA.generate("t1", "rgA", 2048, 1);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4x-pb").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, reg);
        BrokerService b = TlsTestSupport.broker("rgA_rc", TlsTestSupport.issued(caA, "t1", "rgA", "rc"), "?needClientAuth=true",
                new BrokerPlugin[]{authz}, authz, a1.chain[1]);
        brokers.add(b);
        ActiveMQSslConnectionFactory cf = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        cf.setKeyAndTrustManagers(a1.keyManagers(), TlsTestSupport.trustManagers(caA.caCert()), new SecureRandom());
        Connection c = cf.createConnection();
        connections.add(c);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // the connection has now been up for 91 days (bound then, nothing has reconnected since)
        Field idf = PeerTrustRegistry.class.getDeclaredField("identities");
        idf.setAccessible(true);
        Map<String, PeerTrustRegistry.Binding> ids = (Map<String, PeerTrustRegistry.Binding>) idf.get(reg);
        PeerTrustRegistry.Binding cur = ids.get("rgA/a1");
        long old = System.currentTimeMillis() - 91 * DAY;
        ids.put("rgA/a1", new PeerTrustRegistry.Binding(cur.tenant, cur.key, cur.cert, cur.source, cur.role, cur.follow, old, old, old));
        Method compact = PeerTrustRegistry.class.getDeclaredMethod("compact");      // persist that state as the log would hold it
        compact.setAccessible(true);
        compact.invoke(reg);

        // the live connection is busy: consumers, producers, sends, receives
        MessageConsumer in = s.createConsumer(s.createQueue("rgA_a1"));
        for (int i = 0; i < 20; i++) s.createProducer(s.createTopic("agent.event")).send(s.createTextMessage("telemetry " + i));
        in.receive(200);
        reg.flushSeen();
        long seen = reg.lastSeen("rgA", "a1");
        System.out.println("a1 last seen " + ((System.currentTimeMillis() - seen) / DAY) + " days ago, although its connection is up and busy");

        PeerTrustRegistry restarted = PeerTrustRegistry.open(f, o, m -> System.out.println("[registry/restart] " + m));
        boolean kept = restarted.pinnedKey(new PeerIdentity.Expected("rgA", "a1")) != null;
        System.out.println("after the controller restart: binding of the continuously connected rgA/a1 kept=" + kept);
        assertTrue(kept, "a peer connected the whole time was pruned at the next restart (only connects refresh last-seen)");
    }

    // ------------------------------------------------------------------ persisted identity expiry

    private static X509Certificate expiredLeaf(X509Certificate like, KeyPair signer) throws Exception {
        long now = System.currentTimeMillis();
        X500Name subject = new X500Name(like.getSubjectX500Principal().getName());
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(new X500Name("CN=rootCA-old"), new BigInteger(64, new SecureRandom()),
                new Date(now - 3L * 366 * DAY), new Date(now - DAY), subject, like.getPublicKey());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(signer.getPrivate())));
    }

    /**
     * node-identity.p12 is persisted (new on this branch) with a 3-year leaf and reloaded as-is on every start; nothing checks
     * or renews its validity. Before, every start made a fresh chain. Three years after install every node presents an
     * expired certificate (PKIX and the bound-key path both refuse it) and a restart no longer fixes it.
     */
    @Test
    void aPersistedIdentityPastItsValidityIsRenewedAtStart() throws Exception {
        Path dir = Files.createTempDirectory("r4x-exp");
        ControllerEngine first = engineAt(dir, "rgA", "a1", false, null);
        X509Certificate[] c1 = first.getCertificateManager().getPublicCertificate();
        Path p12;
        try (var w = Files.walk(dir)) { p12 = w.filter(p -> p.getFileName().toString().equals("node-identity.p12")).findFirst().orElseThrow(); }
        char[] pw = Files.readString(p12.resolveSibling("node-identity.p12.pass")).trim().toCharArray();
        KeyStore ks = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        try (FileInputStream in = new FileInputStream(p12.toFile())) { ks.load(in, pw); }
        PrivateKey k = (PrivateKey) ks.getKey("node", pw);
        ks.setKeyEntry("node", k, pw, new Certificate[]{ expiredLeaf(c1[0], TlsTestSupport.rsa()) });   // three years later
        try (FileOutputStream out = new FileOutputStream(p12.toFile())) { ks.store(out, pw); }

        ControllerEngine restarted = engineAt(dir, "rgA", "a1", false, null);
        X509Certificate leaf = restarted.getCertificateManager().getPublicCertificate()[0];
        String state;
        try { leaf.checkValidity(); state = "valid"; } catch (Exception e) { state = "EXPIRED (" + e.getClass().getSimpleName() + ")"; }
        System.out.println("after a restart with an expired persisted identity, the node presents a leaf that is " + state + ", notAfter=" + leaf.getNotAfter());
        assertDoesNotThrow(() -> leaf.checkValidity(), "an expired persisted node identity is loaded and presented as-is; nothing renews it");
    }

    // ------------------------------------------------------------------ upgrade compatibility

    /**
     * A mixed fleet (rolling upgrade): an agent still on main has no NodeIdentityKeyManager. It was bound at its upgraded
     * controller (compat legacy exchange). The controller restarts: its CertificateRequest names only its truststore's
     * issuers, the old agent's JDK key manager then presents NO certificate, and needClientAuth (now the default with
     * broker security) refuses the handshake. The old agent is cut off at every controller restart until it rediscovers.
     */
    @Test
    void anAgentStillOnMainReconnectsAfterItsUpgradedControllerRestarts() throws Exception {
        Path cdir = Files.createTempDirectory("r4x-mixed");
        ControllerEngine ctl = engineAt(cdir, "rgA", "rc", true, Map.of("discovery_auth", "compat"));
        TlsTestSupport.Id old = TlsTestSupport.selfRooted("old-main-agent", "t1", "rgA", "a5");
        assertTrue(ctl.getCertificateManager().getPeerTrust().bind("t1", "rgA", "a5", old.kp.getPublic(), "discovery").ok);
        ctl.getCertificateManager().addCertificatesToTrustStore("rgA_a5", old.chain);
        ControllerEngine restarted = engineAt(cdir, "rgA", "rc", true, Map.of("discovery_auth", "compat"));
        assertNotNull(restarted.getCertificateManager().getPeerTrust().boundByKey(old.chain), "precondition: the binding persisted");

        Map<String, Boolean> outcome = new LinkedHashMap<>();
        for (String who : new String[]{"old agent (main: JDK key manager)", "new agent (NodeIdentityKeyManager)"}) {
            SSLContext sctx = SSLContext.getInstance("TLS");
            sctx.init(restarted.getCertificateManager().getKeyManagers(), restarted.getCertificateManager().getTrustManagers(), new SecureRandom());
            AtomicBoolean accepted = new AtomicBoolean();
            try (SSLServerSocket ss = (SSLServerSocket) sctx.getServerSocketFactory().createServerSocket(0)) {
                ss.setNeedClientAuth(true);
                Thread t = new Thread(() -> {
                    try (SSLSocket s = (SSLSocket) ss.accept()) { s.startHandshake(); s.getOutputStream().write(1); s.getOutputStream().flush(); accepted.set(true); }
                    catch (Exception ignore) { }
                });
                t.start();
                KeyManager[] kms = old.keyManagers();
                if (who.startsWith("new")) kms[0] = new NodeIdentityKeyManager((X509KeyManager) kms[0], () -> "me");
                SSLContext cctx = SSLContext.getInstance("TLS");
                X509Certificate[] sc = restarted.getCertificateManager().getPublicCertificate();
                cctx.init(kms, TlsTestSupport.trustManagers(sc[sc.length - 1]), new SecureRandom());
                try (SSLSocket s = (SSLSocket) cctx.getSocketFactory().createSocket("127.0.0.1", ss.getLocalPort())) {
                    s.startHandshake();
                    s.getInputStream().read();
                } catch (Exception ignore) { }
                t.join(5000);
            }
            outcome.put(who, accepted.get());
        }
        System.out.println("after the upgraded controller restarted, its broker TLS admits: " + outcome);
        assertTrue(outcome.get("old agent (main: JDK key manager)"), "an agent still on main is refused by its upgraded controller after every restart");
    }

    /**
     * Existing deployments whose tenant_id has a '.' (legal before) cannot join at all after the upgrade, and no setting
     * restores the old behaviour (peer_identity_mode=off still applies the name grammar).
     *
     * <p>Resolution (hardening): a dotted tenant can never be an identity again (round 3: tenant t1's namespace T.t1. is a
     * prefix of t1.x's, TenantNameTest), and mapping it implicitly could merge two tenants. The documented upgrade path is the
     * explicit migration setting {@code tenant_id_dot_replacement=-} on every node of the tenant; this test now runs with it.
     * Without it the node refuses to start with that instruction ({@link #aDottedTenantWithoutTheMigrationSettingIsRefusedWithTheFix}).
     */
    @Test
    void aDeploymentWithADottedTenantStillJoinsAfterTheUpgrade() throws Exception {
        Map<String, Object> legacy = Map.of("tenant_id", "acme.prod", "peer_identity_mode", "off", "discovery_auth", "compat",
                "tenant_id_dot_replacement", "-");
        ControllerEngine ctl = engineAt(Files.createTempDirectory("r4x-t1"), "rgA", "rc", true, legacy);
        ControllerEngine agent = engineAt(Files.createTempDirectory("r4x-t2"), "rgA", "a1", false, legacy);
        DiscoveryProcessor c = new DiscoveryProcessor(agent), s = new DiscoveryProcessor(ctl);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "a1");
        DiscoveryNode n = null;
        if (ch != null) {
            DiscoveryAuth.Msg p = c.clientProof(hello, copy(ch), "rc");
            DiscoveryAuth.Msg r = p == null ? null : s.serverResult(copy(hello), ch, copy(p), "a1");
            n = r == null ? null : c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
        }
        System.out.println("tenant 'acme.prod', peer_identity_mode=off: agent leaf=" + agent.getCertificateManager().getPublicCertificate()[0].getSubjectX500Principal()
                + "; joined=" + (n != null));
        assertNotNull(n, "a deployment whose tenant name has a '.' can no longer join after the upgrade, even with peer_identity_mode=off");
        assertEquals("acme-prod", CrescoNames.identityOf(agent.getCertificateManager().getPublicCertificate()[0]).getTenant());
    }

    /** The other half of the documented path: without the migration setting a dotted tenant is refused, with the fix named. */
    @Test
    void aDottedTenantWithoutTheMigrationSettingIsRefusedWithTheFix() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("r4x-t3").toString(), Map.of("tenant_id", "acme.prod"));
        String why = SecuritySettings.tenantRefusal(pb.getConfig());
        System.out.println("tenant 'acme.prod' without the migration setting: " + why);
        assertNotNull(why, "a dotted tenant was accepted without the explicit migration setting");
        assertTrue(why.contains("tenant_id_dot_replacement"), "the refusal does not name the migration setting: " + why);
        PluginBuilder ok = TestAgentService.plugin(Files.createTempDirectory("r4x-t4").toString(),
                Map.of("tenant_id", "acme.prod", "tenant_id_dot_replacement", "-"));
        assertNull(SecuritySettings.tenantRefusal(ok.getConfig()));
        assertEquals("acme-prod", SecuritySettings.tenant(ok.getConfig()));
    }
}
