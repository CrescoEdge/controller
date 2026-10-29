package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.core.ControllerStateImp;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/** Real TLS material and real brokers for the identity-binding tests. */
public final class TlsTestSupport {

    static final char[] PW = "test-only".toCharArray();

    private TlsTestSupport() { }

    public static void provider() {
        if (Security.getProvider("BC") == null) Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static KeyPair rsa() throws Exception {
        provider();
        KeyPairGenerator g = KeyPairGenerator.getInstance("RSA", "BC");
        g.initialize(2048, new SecureRandom());
        return g.generateKeyPair();
    }

    /** A node identity: key pair + chain. */
    public static final class Id {
        public final KeyPair kp;
        public final X509Certificate[] chain;
        Id(KeyPair kp, X509Certificate[] chain) { this.kp = kp; this.chain = chain; }
        KeyManager[] keyManagers() throws Exception { return TlsTestSupport.keyManagers(kp.getPrivate(), chain); }
    }

    /** A leaf issued by {@code ca} for tenant/region/agent (the regional-CA path, RegionCA.enroll). */
    public static Id issued(RegionCA ca, String tenant, String region, String agent) throws Exception {
        KeyPair kp = rsa();
        return new Id(kp, ca.enroll(kp.getPublic(), tenant, region, agent, 1));
    }

    /**
     * A self-rooted chain [leaf, root] whose leaf names tenant/region/agent: what a node minting certificates under
     * its OWN root looks like (legacy per-node trust, where every node generated a root CA and peers trust it).
     */
    public static Id selfRooted(String rootName, String tenant, String region, String agent) throws Exception {
        provider();
        KeyPair rootKp = rsa();
        X500Name rn = new X500Name("CN=rootCA-" + rootName);
        X509Certificate root = cert(rn, rn, rootKp.getPublic(), rootKp.getPrivate(), true);
        KeyPair kp = rsa();
        X509Certificate leaf = cert(new X500Name(io.cresco.library.security.CrescoIdentity.of(tenant, region, agent, null).toX500Name()),
                rn, kp.getPublic(), rootKp.getPrivate(), false);
        return new Id(kp, new X509Certificate[]{leaf, root});
    }

    private static X509Certificate cert(X500Name subject, X500Name issuer, java.security.PublicKey key, PrivateKey signer, boolean ca) throws Exception {
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(issuer, new BigInteger(64, new SecureRandom()),
                new Date(now - 60_000L), new Date(now + 86_400_000L), subject, key);
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        b.addExtension(Extension.keyUsage, true, new KeyUsage(ca ? (KeyUsage.keyCertSign | KeyUsage.cRLSign) : (KeyUsage.digitalSignature | KeyUsage.keyEncipherment)));
        return new JcaX509CertificateConverter().getCertificate(b.build(new JcaContentSignerBuilder("SHA384withRSA").setProvider("BC").build(signer)));
    }

    static KeyManager[] keyManagers(PrivateKey key, X509Certificate[] chain) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("me", key, PW, chain);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, PW);
        return kmf.getKeyManagers();
    }

    static TrustManager[] trustManagers(X509Certificate... anchors) throws Exception {
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        int i = 0;
        for (X509Certificate a : anchors) ts.setCertificateEntry("a" + (i++), a);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf.getTrustManagers();
    }

    /** A started non-persistent TLS broker presenting {@code id}, trusting {@code anchors}. */
    public static BrokerService broker(String name, Id id, boolean needClientAuth, BrokerPlugin[] plugins,
                                org.apache.activemq.security.MessageAuthorizationPolicy map, X509Certificate... anchors) throws Exception {
        return broker(name, id, needClientAuth ? "?needClientAuth=true" : "", plugins, map, anchors);
    }

    /** As above with the connector's TLS options ({@code ?wantClientAuth=true}: what ActiveBroker sets when client auth is optional). */
    public static BrokerService broker(String name, Id id, String tlsOptions, BrokerPlugin[] plugins,
                                org.apache.activemq.security.MessageAuthorizationPolicy map, X509Certificate... anchors) throws Exception {
        SslBrokerService b = new SslBrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setAdvisorySupport(true);
        b.setSslContext(new SslContext(id.keyManagers(), trustManagers(anchors), new SecureRandom()));
        if (plugins != null && plugins.length > 0) b.setPlugins(plugins);
        if (map != null) b.setMessageAuthorizationPolicy(map);
        TransportConnector c = b.addConnector("nio+ssl://127.0.0.1:0" + tlsOptions);
        c.setName("tls");
        b.start();
        b.waitUntilStarted();
        return b;
    }

    public static int port(BrokerService b) throws Exception {
        return b.getTransportConnectorByName("tls").getConnectUri().getPort();
    }

    /** The failover URI shape agents use (ControllerSMHandler.agentBrokerURI), bounded so a refusal is quick. */
    static String agentUri(int port) {
        return "failover:(nio+ssl://127.0.0.1:" + port + "?verifyHostName=false&socketBufferSize=0)"
                + "?maxReconnectAttempts=1&startupMaxReconnectAttempts=1&initialReconnectDelay=100&useExponentialBackOff=false";
    }

    /** Controller state of a test node: fixed identity, never "active" (so no plugin loading starts). */
    public static final class State extends ControllerStateImp {
        public volatile String region, agent;
        State(PluginBuilder pb, String region, String agent) { super(pb, null); this.region = region; this.agent = agent; }
        @Override public ControllerMode getControllerState() { return ControllerMode.AGENT; }
        @Override public boolean isActive() { return false; }
        @Override public boolean isFailed() { return true; }
        public volatile boolean regional;
        @Override public boolean isRegionalController() { return regional; }
        @Override public boolean isGlobalController() { return false; }
        @Override public String getRegion() { return region; }
        @Override public String getAgent() { return agent; }
        @Override public String getAgentPath() { return region + "_" + agent; }
    }

    /** A controller engine with a real CertificateManager (and its ActiveClient), for region/agent. */
    public static ControllerEngine engine(String region, String agent, Map<String, Object> extra) throws Exception {
        provider();
        Path dir = Files.createTempDirectory("peerid-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        if (extra != null) cfg.putAll(extra);
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = new State(pb, region, agent);
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    /** Give {@code e} an identity issued by {@code ca} (as a regional-CA enrollment does), trusting the CA. */
    static void enroll(ControllerEngine e, RegionCA ca, String tenant) throws Exception {
        CertificateManager cm = e.getCertificateManager();
        X509Certificate[] chain = ca.enroll(cm.getPublicCertificate()[0].getPublicKey(), tenant, e.cstate.getRegion(), e.cstate.getAgent(), 1);
        if (!cm.installEnrollment(cm.getJsonFromCerts(chain))) throw new IllegalStateException("enrollment not installed");
    }
}
