package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-message costs of the hardened control plane, in one JVM (not a test: run it on purpose,
 * {@code mvn test -Dtest=ControlPlaneCostBench -Dcresco.bench=true}; docs/tests/control_plane_bench.sh measures a live
 * fabric). Prints one line per measurement: signing and verification of a MsgEvent (RSA-3072, the default identity key),
 * the canonical form they cover, revocation checks with an empty and a 100,000-entry revocation list (they must not grow
 * with it), and a secured broker (mutual TLS, stamping, tenant authorization) against a plain one.
 */
@EnabledIfSystemProperty(named = "cresco.bench", matches = "true")
class ControlPlaneCostBench {

    private static final Gson GSON = new Gson();

    private static void line(String what, long ops, long nanos) {
        double us = nanos / 1000.0 / ops;
        System.out.printf("BENCH %-58s %10.2f us/op %12.0f ops/s%n", what, us, 1e6 / us);
    }

    private static ControllerEngine node(String region, String agent, boolean regional) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static MsgEvent message(ControllerEngine from, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, true, false);
        m.setParam("action", "iscontrolleractive");
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + from.cstate.getRegion() + "-" + from.cstate.getAgent() + "-p1", java.util.UUID.randomUUID().toString());
        return m;
    }

    @Test
    void signaturesCanonicalFormAndRevocationChecks() throws Exception {
        ControllerEngine region = node("rgA", "rc", true), a1 = node("rgA", "a1", false);
        // the region knows a1's key (as discovery binds it)
        region.getCertificateManager().getPeerTrust().pin("rgA", "a1", leaf(a1), PeerTrustRegistry.ROLE_AGENT, false);
        Caller caller = Caller.controller("t1", "rgA", "a1");
        int n = 600;
        List<MsgEvent> wire = new ArrayList<>(n);
        for (int i = 0; i < 100; i++) {                        // warm up
            MsgEvent m = message(a1, region);
            CallerStamps.stamp(m, caller);
            a1.markForSigning(m);
            a1.msgSigning().signIfPending(m);
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            MsgEvent m = message(a1, region);
            CallerStamps.stamp(m, caller);
            a1.markForSigning(m);
            a1.msgSigning().signIfPending(m);
            wire.add(m);
        }
        line("sign a MsgEvent at its origin (RSA-3072, SHA384withRSA)", n, System.nanoTime() - t0);
        List<MsgEvent> copies = new ArrayList<>(n);
        for (MsgEvent m : wire) copies.add(GSON.fromJson(GSON.toJson(m), MsgEvent.class));
        int ok = 0;
        t0 = System.nanoTime();
        for (MsgEvent m : copies) if (region.msgSigning().verify(m, caller, false).ok()) ok++;
        line("verify it at the receiver (credential cached)", n, System.nanoTime() - t0);
        if (ok != n) throw new AssertionError("only " + ok + "/" + n + " verified");
        MsgEvent sample = wire.get(0);
        int k = 50_000;
        t0 = System.nanoTime();
        for (int i = 0; i < k; i++) MsgSigning.canonical(sample);
        line("the canonical form a signature covers", k, System.nanoTime() - t0);
        System.out.println("BENCH signature parameters add " + (GSON.toJson(sample).length() - GSON.toJson(message(a1, region)).length())
                + " bytes to a " + GSON.toJson(message(a1, region)).length() + "-byte MsgEvent");

        PeerTrustRegistry reg = region.getCertificateManager().getPeerTrust();
        X509Certificate[] chain = region.getCertificateManager().getPublicCertificate();
        X509Certificate[] a1chain = {leaf(a1)};
        for (int pass = 0; pass < 2; pass++) {
            if (pass == 1) {
                SecureRandom r = new SecureRandom();
                List<String> fps = new ArrayList<>();
                for (int i = 0; i < 100_000; i++) {
                    byte[] b = new byte[32];
                    r.nextBytes(b);
                    fps.add(java.util.HexFormat.of().formatHex(b));
                }
                reg.mergeRevocations(fps, "bench", null);            // one batch (a revocation list from the parent)
            }
            String label = pass == 0 ? "no revocations" : "100,000 revocations";
            int m = 200_000;
            t0 = System.nanoTime();
            for (int i = 0; i < m; i++) reg.refusedKey(a1chain);
            line("TLS revocation check of a peer (" + label + ")", m, System.nanoTime() - t0);
            t0 = System.nanoTime();
            for (int i = 0; i < m; i++) reg.verifiedIdentity(a1chain);
            line("broker identity of a connection (" + label + ")", m, System.nanoTime() - t0);
        }
        if (chain == null) throw new AssertionError();
    }

    // ------------------------------------------------------------------ broker: secured vs plain

    @Test
    void securedBrokerAgainstPlain() throws Exception {
        TlsTestSupport.provider();
        RegionCA ca = RegionCA.generate("t1", "rgA", 2048, 1);
        TlsTestSupport.Id brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        TlsTestSupport.Id a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        TlsTestSupport.Id a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
        for (boolean secured : new boolean[]{false, true}) {
            BrokerService b;
            if (secured) {
                PeerTrustRegistry peers = new PeerTrustRegistry();
                peers.trustCa(ca.caCert(), "rgA", "t1", "test");
                Map<String, Object> cfg = new HashMap<>();
                cfg.put("broker_security_enabled", "true");
                cfg.put("tenant_namespacing", "false");
                cfg.put("tenant_id", "t1");
                PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("bench").toString(), cfg);
                CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
                b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
            } else {
                b = TlsTestSupport.broker("rgA_rc", brokerId, false, new BrokerPlugin[0], null, ca.caCert());
            }
            List<Connection> conns = new ArrayList<>();
            try {
                Session rx = session(b, a2, ca, conns), tx = session(b, a1, ca, conns);
                String q = "t1.bench.inbox";          // in the tenant's namespace (an inbox needs a recorded owner)
                MessageConsumer c = rx.createConsumer(rx.createQueue(q));
                MessageProducer p = tx.createProducer(tx.createQueue(q));
                p.setDeliveryMode(jakarta.jms.DeliveryMode.NON_PERSISTENT);
                String body = GSON.toJson(new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", null, "rgA", "a2", null, true, false));
                for (int i = 0; i < 2000; i++) { p.send(tx.createTextMessage(body)); c.receive(5000); }
                int n = 5000;
                long[] lat = new long[n];
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++) {
                    long s = System.nanoTime();
                    p.send(tx.createTextMessage(body));
                    Message got = c.receive(5000);
                    if (got == null) throw new AssertionError("lost a message");
                    lat[i] = System.nanoTime() - s;
                }
                long el = System.nanoTime() - t0;
                Arrays.sort(lat);
                String what = (secured ? "secured" : "plain") + " broker, mTLS client -> broker -> consumer round trip";
                line(what, n, el);
                System.out.printf("BENCH   %s: p50 %.0f us, p99 %.0f us%n", secured ? "secured" : "plain", lat[n / 2] / 1000.0, lat[n * 99 / 100] / 1000.0);
            } finally {
                for (Connection x : conns) try { x.close(); } catch (Exception ignore) { }
                b.stop();
            }
        }
    }

    private static Session session(BrokerService b, TlsTestSupport.Id id, RegionCA ca, List<Connection> conns) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        conns.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}
