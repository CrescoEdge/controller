package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.core.ControllerEngine;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A storm of joins at one responder, over real sockets from one address (as a host that starts hundreds of agents): every
 * request is answered -- queued and served, or told to come back -- none is closed without a word; the certificate
 * exchange is answered by the responder's workers, and the trust the broker already holds includes the joiner at once.
 *
 * <p>On the DGX gate of agent release 1.3.378 (job 268125), 731 of 1,000 agents started together had a discovery closed
 * without an answer (the listener allowed 16 connections per address) and logged it as "Discovery Failed: Index 0 out of
 * bounds for length 0", 6,024 ERROR lines, each to come back 3 s later.
 */
class DiscoveryStormTest {

    private EventLoopGroup group;
    private final List<DiscoveryJobs> made = new ArrayList<>();

    @BeforeEach
    void up() { group = new NioEventLoopGroup(2); }

    @AfterEach
    void down() throws Exception {
        for (DiscoveryJobs j : made) j.shutdown();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }

    private DiscoveryJobs jobs(int workers, int maxWaitMs) {
        DiscoveryJobs j = new DiscoveryJobs(workers, maxWaitMs, DiscoveryGuard.DEFAULT_MAX_CONNECTIONS, null);
        made.add(j);
        return j;
    }

    private Channel listen(ControllerEngine e, DiscoveryGuard.Admission a, DiscoveryJobs jobs) throws Exception {
        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(TCPDiscoveryEngine.childInitializer(a, 10, e, null, jobs))
                .bind("127.0.0.1", 0).sync().channel();
    }

    private static int port(Channel server) { return ((InetSocketAddress) server.localAddress()).getPort(); }

    /** Hold every worker of {@code jobs} until the returned latch is counted down (requests then wait in the queue). */
    private static CountDownLatch hold(DiscoveryJobs jobs) throws Exception {
        CountDownLatch gate = new CountDownLatch(1), started = new CountDownLatch(jobs.workers());
        for (int i = 0; i < jobs.workers(); i++) {
            assertEquals(0, jobs.submit("hold-" + i, () -> { started.countDown(); try { gate.await(); } catch (InterruptedException ignore) { } }));
        }
        assertTrue(started.await(10, TimeUnit.SECONDS), "the workers did not start");
        return gate;
    }

    private static void eventually(java.util.function.BooleanSupplier cond, long ms, java.util.function.Supplier<String> what) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > end) fail(what.get());
            Thread.sleep(20);
        }
    }

    /**
     * Fails before the change: with the listener's limits as they were (16 connections per address), 48 of the 64 were
     * closed without an answer.
     */
    @Test
    void sixtyFourJoinersOfOneHostAtTheSameMomentAreAllAnswered() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(DiscoveryGuard.DEFAULT_MAX_CONNECTIONS,
                DiscoveryGuard.DEFAULT_MAX_CONNECTIONS_PER_ADDRESS, 30_000, null);
        DiscoveryJobs jobs = jobs(2, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs);
        try {
            final int n = 64;
            CountDownLatch gate = hold(jobs);                      // all 64 are connected and waiting at the same moment
            ControllerEngine joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
            AtomicInteger answered = new AtomicInteger();
            List<TCPDiscoveryStatic.Outcome> outcomes = java.util.Collections.synchronizedList(new ArrayList<>());
            Thread[] t = new Thread[n];
            for (int i = 0; i < n; i++) {
                t[i] = new Thread(() -> {
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
                    try {
                        List<DiscoveryNode> found = ds.discover(DiscoveryType.AGENT, 20_000, "127.0.0.1", port(server), false);
                        if (found != null && found.size() == 1 && "rc".equals(found.get(0).discovered_agent)) answered.incrementAndGet();
                    } catch (Exception e) {
                        // counted by its outcome
                    }
                    outcomes.add(ds.lastAttempt().outcome);
                });
                t[i].start();
            }
            eventually(() -> jobs.queued() == n, 15_000, () -> "only " + jobs.queued() + " of " + n + " requests are waiting ("
                    + admission.refused() + " connections refused)");
            assertEquals(n, admission.open("127.0.0.1"), "connections of the one address held open");
            gate.countDown();
            for (Thread x : t) x.join(30_000);
            assertEquals(n, answered.get(), "joiners answered; outcomes " + outcomes);
            assertEquals(0, admission.refused(), "connections closed for the per-address limit");
            assertEquals(0, jobs.busy());
            assertTrue(outcomes.stream().allMatch(o -> o == TCPDiscoveryStatic.Outcome.ANSWERED), "outcomes " + outcomes);
        } finally {
            server.close().sync();
        }
    }

    @Test
    void aResponderThatIsBehindTellsTheJoinerWhenToComeBack() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(1000, 1000, 30_000, null);
        DiscoveryJobs jobs = jobs(1, 20);                          // a request that would wait more than 20 ms is sent away
        Channel server = listen(responder, admission, jobs);
        try {
            CountDownLatch gate = hold(jobs);
            ControllerEngine joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
            final int n = 12;
            List<TCPDiscoveryStatic.Attempt> attempts = java.util.Collections.synchronizedList(new ArrayList<>());
            Thread[] t = new Thread[n];
            for (int i = 0; i < n; i++) {
                t[i] = new Thread(() -> {
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
                    try { ds.discover(DiscoveryType.AGENT, 20_000, "127.0.0.1", port(server), false); } catch (Exception ignore) { }
                    attempts.add(ds.lastAttempt());
                });
                t[i].start();
            }
            eventually(() -> jobs.queued() + jobs.busy() == n, 15_000, () -> "requests seen: " + (jobs.queued() + jobs.busy()));
            assertTrue(jobs.busy() >= 1 && jobs.queued() >= 1, "queued " + jobs.queued() + ", sent away " + jobs.busy());
            gate.countDown();
            for (Thread x : t) x.join(30_000);
            long answered = attempts.stream().filter(a -> a.outcome == TCPDiscoveryStatic.Outcome.ANSWERED).count();
            long busy = attempts.stream().filter(a -> a.outcome == TCPDiscoveryStatic.Outcome.BUSY).count();
            assertEquals(n, answered + busy, "a joiner got neither an answer nor a time to come back: "
                    + attempts.stream().map(a -> a.outcome.name()).toList());
            assertEquals(jobs.busy(), busy);
            for (TCPDiscoveryStatic.Attempt a : attempts) {
                if (a.outcome != TCPDiscoveryStatic.Outcome.BUSY) continue;
                assertTrue(a.busyRetryMs >= DiscoveryJobs.MIN_RETRY_MS && a.busyRetryMs <= DiscoveryJobs.MAX_RETRY_MS, "come back in " + a.busyRetryMs);
                JoinBackoff b = new JoinBackoff(3000, 1000, 30_000);
                long next = b.failed(a.outcome, a.busyRetryMs);
                String line = JoinBackoff.line(a, "joining the controller at 127.0.0.1:" + port(server), b.attempts(), next);
                assertTrue(line.contains("is busy and asked this node to come back in " + a.busyRetryMs + " ms"), line);
                assertTrue(next >= a.busyRetryMs, "the joiner comes back before the time it was given: " + next);
            }
        } finally {
            server.close().sync();
        }
    }

    /** A connection over the listener's limit was closed without a word; now its peer is told to come back. */
    @Test
    void aConnectionOverTheLimitIsToldToComeBack() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null);
        DiscoveryJobs jobs = jobs(1, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        com.google.gson.Gson gson = new com.google.gson.Gson();
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 2, 30_000, null)
                .answerWhenRefused(() -> TCPDiscoveryEngineHandler.busyFrame(gson, responder.getPluginBuilder(), 1500, "too many connections"));
        Channel server = listen(responder, admission, jobs);
        try {
            CountDownLatch gate = hold(jobs);
            ControllerEngine joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
            List<TCPDiscoveryStatic.Attempt> held = java.util.Collections.synchronizedList(new ArrayList<>());
            Thread[] t = new Thread[2];
            for (int i = 0; i < 2; i++) {
                t[i] = new Thread(() -> {
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
                    try { ds.discover(DiscoveryType.AGENT, 20_000, "127.0.0.1", port(server), false); } catch (Exception ignore) { }
                    held.add(ds.lastAttempt());
                });
                t[i].start();
            }
            eventually(() -> admission.open("127.0.0.1") == 2 && jobs.queued() == 2, 10_000, () -> "the two connections are not held");
            TCPDiscoveryStatic third = new TCPDiscoveryStatic(joiner);
            List<DiscoveryNode> found = third.discover(DiscoveryType.AGENT, 5_000, "127.0.0.1", port(server), false);
            assertTrue(found.isEmpty());
            assertEquals(TCPDiscoveryStatic.Outcome.BUSY, third.lastAttempt().outcome, "the connection over the limit was closed without a word");
            assertEquals(1500, third.lastAttempt().busyRetryMs);
            assertEquals(1, admission.refused());
            gate.countDown();
            for (Thread x : t) x.join(30_000);
            assertTrue(held.stream().allMatch(a -> a.outcome == TCPDiscoveryStatic.Outcome.ANSWERED));
            // nothing listens: said as that, not as an empty answer
            int closed = port(server);
            server.close().sync();
            TCPDiscoveryStatic gone = new TCPDiscoveryStatic(joiner);
            try { gone.discover(DiscoveryType.AGENT, 2_000, "127.0.0.1", closed, false); } catch (Exception expected) { }
            assertEquals(TCPDiscoveryStatic.Outcome.UNREACHABLE, gone.lastAttempt().outcome);
        } finally {
            server.close().sync();
        }
    }

    /**
     * The certificate exchange through the queue: answered by a worker, the joiner bound, and the trust the responder's
     * broker ALREADY holds trusts the joiner's chain the moment the exchange is answered (no TLS context is rebuilt).
     */
    @Test
    void aCertificateExchangeIsAnsweredByAWorkerAndTheJoinerIsTrustedAtOnce() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null);
        CertificateManager rcm = new CertificateManager(responder);
        responder.setCertificateManager(rcm);
        X509TrustManager held = null;
        for (TrustManager tm : rcm.getTrustManagers()) if (tm instanceof X509TrustManager) held = (X509TrustManager) tm;
        assertNotNull(held);                                       // what the broker's TLS context was given at start
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(2, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs);
        try {
            ControllerEngine joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
            CertificateManager jcm = new CertificateManager(joiner);
            joiner.setCertificateManager(jcm);
            final X509TrustManager before = held;
            assertThrows(java.security.cert.CertificateException.class, () -> before.checkClientTrusted(jcm.getPublicCertificate(), "RSA"));
            long served = jobs.served();
            List<DiscoveryNode> found = new TCPDiscoveryStatic(joiner).discover(DiscoveryType.AGENT, 20_000, "127.0.0.1", port(server), true);
            assertEquals(1, found.size(), "the certificate exchange did not complete");
            assertEquals("rc", found.get(0).discovered_agent);
            assertTrue(jobs.served() >= served + 2, "the hello and the proof were not answered by the workers: " + (jobs.served() - served));
            held.checkClientTrusted(jcm.getPublicCertificate(), "RSA");          // the same trust manager, nothing rebuilt
            assertNotNull(rcm.getPeerTrust().boundByKey(jcm.getPublicCertificate()), "the joiner's key is not bound");
            eventually(() -> admission.open() == 0, 5_000, () -> "the connection was not released");
        } finally {
            server.close().sync();
        }
    }
}
