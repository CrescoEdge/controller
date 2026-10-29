package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryAuth;
import io.cresco.agent.controller.netdiscovery.DiscoveryProcessor;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Bound peers stay trusted for TLS across a restart, both directions; nothing else does (third review, finding 5). */
class RestartTrustTest {

    private static final Gson GSON = new Gson();

    private static ControllerEngine at(Path dir, String region, String agent, boolean regional) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        for (String t : new String[]{"agent", "region", "global"}) cfg.put("discovery_secret_" + t, t + "-secret");
        cfg.put("broker_security_enabled", "true");
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State s = new TlsTestSupport.State(pb, region, agent);
        s.regional = regional;
        e.cstate = s;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static void join(ControllerEngine client, ControllerEngine server, DiscoveryType type) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg h = c.clientHello(type);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(h), "c");
        DiscoveryAuth.Msg r = s.serverResult(copy(h), ch, copy(c.clientProof(h, copy(ch), "s")), "c");
        assertNotNull(c.clientResult(h, copy(ch), copy(r), "127.0.0.1", 32005));
    }

    private static boolean trustsClient(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkClientTrusted(chain, "RSA"); return true; } catch (Exception x) { return false; }
    }

    private static boolean trustsServer(ControllerEngine e, X509Certificate[] chain) {
        try { e.getCertificateManager().currentPkixTrustManager().checkServerTrusted(chain, "RSA"); return true; } catch (Exception x) { return false; }
    }

    private static SSLContext tls(ControllerEngine e) throws Exception {
        SSLContext c = SSLContext.getInstance("TLS");
        c.init(e.getCertificateManager().getKeyManagers(), e.getCertificateManager().getTrustManagers(), new SecureRandom());
        return c;
    }

    /**
     * A real TLS handshake with client certificates required, the node's own key and trust managers on both ends: the
     * client chain the server accepted, or null when the server refused the handshake.
     */
    private static X509Certificate[] handshake(ControllerEngine server, ControllerEngine client) throws Exception {
        InetAddress lo = InetAddress.getLoopbackAddress();
        ExecutorService ex = Executors.newSingleThreadExecutor();
        try (SSLServerSocket ss = (SSLServerSocket) tls(server).getServerSocketFactory().createServerSocket(0, 1, lo)) {
            ss.setNeedClientAuth(true);
            Future<X509Certificate[]> seen = ex.submit(() -> {
                try (SSLSocket s = (SSLSocket) ss.accept()) {
                    s.setSoTimeout(10_000);
                    s.startHandshake();                       // completes only once the client's certificate is verified
                    return (X509Certificate[]) s.getSession().getPeerCertificates();
                } catch (Exception refused) {
                    return null;
                }
            });
            try (SSLSocket c = (SSLSocket) tls(client).getSocketFactory().createSocket(lo, ss.getLocalPort())) {
                c.setSoTimeout(10_000);
                c.startHandshake();
                c.getInputStream().read();                    // until the server closes (or alerts)
            } catch (Exception ignore) {
            }
            return seen.get(20, TimeUnit.SECONDS);
        } finally {
            ex.shutdownNow();
        }
    }

    /**
     * The same, end to end: a restarted server's truststore no longer names its bound peers' issuers, so its certificate
     * request hints at none of them, and the JDK key manager then presented NO client certificate -- the server's trust
     * of the bound key never got a chance (live restart drill). The node always presents its identity.
     */
    @Test
    void aRestartedServerAdmitsItsBoundClientsInARealHandshake() throws Exception {
        Path g = Files.createTempDirectory("rh-g"), r = Files.createTempDirectory("rh-r"), a = Files.createTempDirectory("rh-a");
        ControllerEngine global = at(g, "glob", "gc", true), region = at(r, "rgr", "rc", true), agent = at(a, "rgr", "b1", false);
        join(region, global, DiscoveryType.GLOBAL);
        join(agent, region, DiscoveryType.AGENT);
        assertNotNull(handshake(global, region), "the bridge handshake failed before any restart");

        ControllerEngine global2 = at(g, "glob", "gc", true);                     // the global restarts
        X509Certificate[] bridge = handshake(global2, region);
        assertNotNull(bridge, "a restarted global refused the region's bridge handshake (no client certificate?)");
        assertEquals(region.getCertificateManager().getPublicCertificate()[0], bridge[0]);

        ControllerEngine region2 = at(r, "rgr", "rc", true);                      // the region restarts
        X509Certificate[] agentSeen = handshake(region2, agent);
        assertNotNull(agentSeen, "a restarted region refused its agent's handshake (no client certificate?)");
        assertEquals(agent.getCertificateManager().getPublicCertificate()[0], agentSeen[0]);
        assertNotNull(handshake(global2, region2), "a restarted region's bridge was refused by a restarted global");

        // a stranger still gets nowhere: it presents its certificate now, and the server refuses it
        ControllerEngine stranger = at(Files.createTempDirectory("rh-x"), "rgx", "x1", false);
        assertNull(handshake(region2, stranger), "a restarted region admitted an unbound stranger");
    }

    @Test
    void bridgesAndAgentsReconnectAfterEitherSideRestarts() throws Exception {
        Path g = Files.createTempDirectory("rt-g"), r = Files.createTempDirectory("rt-r"), a = Files.createTempDirectory("rt-a");
        ControllerEngine global = at(g, "glob", "gc", true), region = at(r, "rgr", "rc", true), agent = at(a, "rgr", "b1", false);
        join(region, global, DiscoveryType.GLOBAL);
        join(agent, region, DiscoveryType.AGENT);
        X509Certificate[] regionChain = region.getCertificateManager().getPublicCertificate();
        X509Certificate[] globalChain = global.getCertificateManager().getPublicCertificate();
        X509Certificate[] agentChain = agent.getCertificateManager().getPublicCertificate();

        ControllerEngine global2 = at(g, "glob", "gc", true);                     // the global restarts
        assertTrue(trustsClient(global2, regionChain), "after a global restart its broker refuses the region's bridge");
        ControllerEngine region2 = at(r, "rgr", "rc", true);                      // the region restarts
        assertTrue(trustsServer(region2, globalChain), "after a region restart its bridge refuses the global's broker");
        assertTrue(trustsClient(region2, agentChain), "after a region restart its broker refuses its agent");
        ControllerEngine agent2 = at(a, "rgr", "b1", false);                       // the agent restarts
        assertTrue(trustsServer(agent2, region.getCertificateManager().getPublicCertificate()), "after an agent restart it refuses its region's broker");

        // nothing that is not bound: a stranger, and a certificate that names a bound name with another key
        assertFalse(trustsClient(global2, TlsTestSupport.selfRooted("x", "t1", "rgx", "x1").chain));
        assertFalse(trustsClient(region2, TlsTestSupport.selfRooted("x", "t1", "rgr", "b1").chain), "another key for a bound name was trusted");
    }
}
