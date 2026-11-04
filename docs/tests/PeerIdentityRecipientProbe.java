import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Live probe of stream recipient binding over the dataplane, run by peer_identity_live_test.sh.
 *
 * <p>A recipient-bound stream is one whose frames carry {@code cresco_to=<region>_<agent>}: only infrastructure and plugins
 * write that property (through their node's broker connection; wsapi refuses it from API clients by design). This probe
 * writes such a stream the supported way -- on a NODE's own broker connection, as a plugin's dataplane does -- by opening
 * the broker connections of the harness's nodes with their own identities (their {@code security/node-identity.p12}):
 *
 * <pre>
 *   sender   a1's identity -> the global broker   writes 200 frames bound to rgr_b1 (cresco_to)
 *   receiver b1's identity -> the region broker   subscribes to the stream: must get all 200, across the bridge
 *   spy      a1's identity -> the global broker   subscribes to the same stream: must get none
 *   named    a1's identity -> the global broker   subscribes to rgr_b1's stream by its recipient: must be refused
 * </pre>
 *
 * The probe trusts any broker certificate: it judges the brokers' authorization, not the client's verification.
 * Usage: java -cp &lt;activemq-client, hawtbuf, jakarta.jms-api, slf4j api&gt; PeerIdentityRecipientProbe.java
 *        &lt;global-broker-port&gt; &lt;a1 node dir&gt; &lt;region-broker-port&gt; &lt;b1 node dir&gt; &lt;topic&gt; &lt;to&gt; &lt;tenant&gt; &lt;messages&gt;
 *        [&lt;stream name&gt;]
 * Prints one JSON line; exit 0 when the receiver got every frame and the spy none, and the named subscription was refused.
 */
public class PeerIdentityRecipientProbe {

    static Connection connect(int port, Path nodeDir) throws Exception {
        Path store = nodeDir.resolve("cresco-data/security/node-identity.p12");
        char[] pw = Files.readString(store.resolveSibling("node-identity.p12.pass")).trim().toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(store.toFile())) { ks.load(in, pw); }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);
        TrustManager[] any = {new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] c, String a) { }
            public void checkServerTrusted(X509Certificate[] c, String a) { }
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }};
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + port + "?verifyHostName=false");
        f.setKeyAndTrustManagers(kmf.getKeyManagers(), any, new SecureRandom());
        Connection c = f.createConnection();
        c.start();
        return c;
    }

    public static void main(String[] a) throws Exception {
        int globalPort = Integer.parseInt(a[0]), regionPort = Integer.parseInt(a[2]), n = Integer.parseInt(a[7]);
        Path a1 = Path.of(a[1]), b1 = Path.of(a[3]);
        String topic = a[4], to = a[5], tenant = a[6];
        String stream = a.length > 8 ? a[8] : "rb-" + Long.toHexString(System.nanoTime());
        String sel = "stream_name = '" + stream + "'";
        AtomicInteger got = new AtomicInteger(), spied = new AtomicInteger();
        String named = null;
        Connection rc = connect(regionPort, b1), sc = connect(globalPort, a1), tc = connect(globalPort, a1), nc = connect(globalPort, a1);
        try {
            Session rs = rc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer r = rs.createConsumer(rs.createTopic(topic), sel);
            r.setMessageListener(m -> got.incrementAndGet());
            Session ss = sc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer s = ss.createConsumer(ss.createTopic(topic), sel);
            s.setMessageListener(m -> spied.incrementAndGet());
            try {
                Session ns = nc.createSession(false, Session.AUTO_ACKNOWLEDGE);
                ns.createConsumer(ns.createTopic(topic), "cresco_to = '" + to + "'");
            } catch (JMSException refused) {
                named = String.valueOf(refused.getMessage());
            }
            Thread.sleep(3000);                                        // subscription demand crosses the bridge
            Session ts = tc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = ts.createProducer(ts.createTopic(topic));
            for (int i = 0; i < n; i++) {
                TextMessage m = ts.createTextMessage("frame " + i);
                m.setStringProperty("stream_name", stream);
                m.setStringProperty("type", "output");
                m.setStringProperty("cresco_to", to);
                m.setStringProperty("cresco_to_tenant", tenant);
                p.send(m);
            }
            long end = System.currentTimeMillis() + 30_000;
            while (got.get() < n && System.currentTimeMillis() < end) Thread.sleep(100);
            Thread.sleep(1500);                                        // anything a spy would get arrives too
        } finally {
            for (Connection c : new Connection[]{rc, sc, tc, nc}) try { c.close(); } catch (Exception ignore) { }
        }
        boolean ok = got.get() == n && spied.get() == 0 && named != null;
        System.out.println("{\"stream\": \"" + stream + "\", \"messages\": " + n + ", \"bound_received\": " + got.get()
                + ", \"bound_spy_received\": " + spied.get() + ", \"bound_named_refused\": "
                + (named == null ? "null" : "\"" + named.replace("\\", "/").replace("\"", "'") + "\"") + ", \"ok\": " + ok + "}");
        System.exit(ok ? 0 : 1);
    }
}
