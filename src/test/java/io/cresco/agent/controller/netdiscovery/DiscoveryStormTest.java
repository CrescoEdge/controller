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

    private Channel listen(ControllerEngine e, DiscoveryGuard.Admission a, DiscoveryJobs jobs, int idleSeconds) throws Exception {
        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(TCPDiscoveryEngine.childInitializer(a, idleSeconds, e, null, jobs))
                .bind("127.0.0.1", 0).sync().channel();
    }

    /** A node with an identity of its own (a certificate manager): a responder when {@code regional}. */
    private static ControllerEngine node(String agent, boolean regional) throws Exception {
        ControllerEngine e = DiscoveryDosTest.engine("rgA", agent, regional, null);
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static final com.google.gson.Gson GSON = new com.google.gson.Gson();

    /** The locator frame a joiner sends. */
    private static String locatorFrame(ControllerEngine joiner) {
        return GSON.toJson(new TCPDiscoveryStaticHandler(joiner, new ArrayList<>(), DiscoveryType.AGENT, "127.0.0.1", 1, false).genDiscoverMsg());
    }

    /** The hello frame a joiner sends (TCPDiscoveryStaticHandler.channelActive): its hello and the probe for a legacy responder. */
    private static String helloFrame(ControllerEngine joiner, DiscoveryAuth.Msg hello) {
        io.cresco.library.messaging.MsgEvent me = new io.cresco.library.messaging.MsgEvent(io.cresco.library.messaging.MsgEvent.Type.DISCOVER,
                "rgA", joiner.cstate.getAgent(), "p", "Discovery v2 hello.");
        me.setParam(DiscoveryAuth.PARAM, GSON.toJson(hello));
        DiscoveryNode probe = new DiscoveryNode(DiscoveryType.AGENT);
        byte[] junk = new byte[16];
        new java.security.SecureRandom().nextBytes(junk);
        probe.setCertify(java.util.Base64.getEncoder().encodeToString(junk), null, "rgA", joiner.cstate.getAgent());
        me.setCompressedParam("discovery_node", GSON.toJson(probe));
        return GSON.toJson(me);
    }

    private static String proofFrame(ControllerEngine joiner, DiscoveryAuth.Msg proof) {
        io.cresco.library.messaging.MsgEvent me = new io.cresco.library.messaging.MsgEvent(io.cresco.library.messaging.MsgEvent.Type.DISCOVER,
                "rgA", joiner.cstate.getAgent(), "p", "Discovery v2 proof.");
        me.setParam(DiscoveryAuth.PARAM, GSON.toJson(proof));
        return GSON.toJson(me);
    }

    /** A connection by hand: what it read, and whether the responder closed it. */
    private static final class Raw {
        final List<String> read = java.util.Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch closed = new CountDownLatch(1);
        volatile Channel ch;
        boolean closedWithin(long ms) throws InterruptedException { return closed.await(ms, TimeUnit.MILLISECONDS); }
    }

    /** Connect and send {@code frames} at once. */
    private Raw raw(int port, String... frames) throws Exception {
        Raw r = new Raw();
        r.ch = new io.netty.bootstrap.Bootstrap().group(group).channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                .handler(new io.netty.channel.ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                    @Override
                    protected void initChannel(io.netty.channel.socket.SocketChannel ch) {
                        DiscoveryWire.install(ch.pipeline());
                        ch.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(io.netty.channel.ChannelHandlerContext ctx) {
                                for (String f : frames) ctx.write(f);
                                ctx.flush();
                            }

                            @Override
                            public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) { r.read.add((String) msg); }

                            @Override
                            public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) { r.closed.countDown(); }
                        });
                    }
                }).connect("127.0.0.1", port).sync().channel();
        return r;
    }

    /**
     * One certificate exchange by hand: the hello at once, the proof {@code thinkMs} after the challenge arrived.
     *
     * @return "result ok=..." when the responder's result arrived, "busy at <step>" for a busy answer, else "closed after
     * <step> at <ms> ms"
     */
    private String exchange(ControllerEngine joiner, int port, long thinkMs, long[] challengeAfterMs) throws Exception {
        DiscoveryProcessor dp = new DiscoveryProcessor(joiner);
        java.util.concurrent.CompletableFuture<String> out = new java.util.concurrent.CompletableFuture<>();
        long t0 = System.nanoTime();
        Channel ch = new io.netty.bootstrap.Bootstrap().group(group).channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                .handler(new io.netty.channel.ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                    @Override
                    protected void initChannel(io.netty.channel.socket.SocketChannel c) {
                        DiscoveryWire.install(c.pipeline());
                        c.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
                            DiscoveryAuth.Msg hello;
                            String step = "connect";

                            @Override
                            public void channelActive(io.netty.channel.ChannelHandlerContext ctx) {
                                hello = dp.clientHello(DiscoveryType.AGENT);
                                ctx.writeAndFlush(helloFrame(joiner, hello));
                                step = "hello";
                            }

                            @Override
                            public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
                                io.cresco.library.messaging.MsgEvent me = GSON.fromJson((String) msg, io.cresco.library.messaging.MsgEvent.class);
                                DiscoveryAuth.Msg m = GSON.fromJson(me.getParam(DiscoveryAuth.PARAM), DiscoveryAuth.Msg.class);
                                if ("busy".equals(m.step)) { out.complete("busy at " + step); return; }
                                if ("challenge".equals(m.step)) {
                                    step = "challenge";
                                    if (challengeAfterMs != null) challengeAfterMs[0] = (System.nanoTime() - t0) / 1_000_000L;
                                    ctx.executor().schedule(() -> {
                                        ctx.writeAndFlush(proofFrame(joiner, dp.clientProof(hello, m, "127.0.0.1")));
                                        step = "proof";
                                    }, thinkMs, TimeUnit.MILLISECONDS);
                                } else if ("result".equals(m.step)) {
                                    out.complete("result ok=" + m.ok);
                                }
                            }

                            @Override
                            public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) {
                                out.complete("closed after " + step + " at " + (System.nanoTime() - t0) / 1_000_000L + " ms");
                            }
                        });
                    }
                }).connect("127.0.0.1", port).sync().channel();
        try {
            return out.get(30, TimeUnit.SECONDS);
        } finally {
            ch.close().sync();
        }
    }

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

    // ------------------------------------------------------------------ what the review of the queue found

    /**
     * What a responder reads is near what a request is. Measured here with a node's own three-certificate RSA-3072 chain
     * and the proof of possession: the largest request is the hello. A frame larger than the bound is refused when its
     * length arrives, before a byte of it is held -- an open connection could hold 256 KiB less one byte of a frame that
     * nobody had authenticated (2,048 connections: 512 MiB). Fails before: such a frame was read to its end.
     */
    @Test
    void aResponderReadsNoFrameLargerThanARequestIs() throws Exception {
        ControllerEngine responder = node("rc", true), joiner = node("a1", false);
        DiscoveryProcessor c = new DiscoveryProcessor(joiner), s = new DiscoveryProcessor(responder);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(GSON.fromJson(GSON.toJson(hello), DiscoveryAuth.Msg.class), "127.0.0.1");
        DiscoveryAuth.Msg proof = c.clientProof(hello, GSON.fromJson(GSON.toJson(ch), DiscoveryAuth.Msg.class), "127.0.0.1");
        int locator = locatorFrame(joiner).length(), helloBytes = helloFrame(joiner, hello).length(), proofBytes = proofFrame(joiner, proof).length();
        System.out.println("DiscoveryStormTest: request frames of a node with a three-certificate RSA-3072 chain: locator " + locator
                + " bytes, hello " + helloBytes + " bytes, proof (with the proof of possession) " + proofBytes + " bytes; a responder reads at most "
                + DiscoveryWire.MAX_REQUEST_BYTES + " (a joiner reads answers of at most " + DiscoveryWire.MAX_OBJECT_BYTES + ")");
        assertNotNull(proof.pop, "the proof carries no proof of possession");
        int largest = Math.max(locator, Math.max(helloBytes, proofBytes));
        assertTrue(largest * 3 <= DiscoveryWire.MAX_REQUEST_BYTES, "the bound leaves less than three times the largest real request: " + largest);
        assertTrue(DiscoveryWire.MAX_REQUEST_BYTES * 4 <= DiscoveryWire.MAX_OBJECT_BYTES, "the bound is not near what a request is");

        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(2, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs);
        try {
            // the frame the limits were measured against: 256 KiB less one byte announced, then a trickle
            for (int announced : new int[]{DiscoveryWire.MAX_OBJECT_BYTES - 1, DiscoveryWire.MAX_REQUEST_BYTES + 1}) {
                try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port(server))) {
                    java.io.OutputStream out = sock.getOutputStream();
                    out.write(java.nio.ByteBuffer.allocate(4).putInt(announced).array());
                    out.write(new byte[512]);
                    out.flush();
                    sock.setSoTimeout(3000);
                    int r;
                    try { r = sock.getInputStream().read(); } catch (java.net.SocketTimeoutException open) { r = 0; } catch (java.io.IOException reset) { r = -1; }
                    assertEquals(-1, r, "a frame of " + announced + " bytes was being read: the connection is still open");
                }
            }
            eventually(() -> admission.open() == 0, 3000, () -> "connections not released: " + admission.open());
            assertEquals(0, jobs.accepted(), "an oversized frame reached the queue");
            // a real exchange is as before
            assertEquals(1, new TCPDiscoveryStatic(joiner).discover(DiscoveryType.AGENT, 10_000, "127.0.0.1", port(server), true).size());
        } finally {
            server.close().sync();
        }
    }

    /**
     * A joiner's allowance for its next frame runs from the responder's answer. Fails before: it ran from the joiner's
     * last frame, so the time its hello waited in the queue was taken from it (waited 1.5 s of 2 s: closed 0.5 s after the
     * challenge was written, while it was signing its proof).
     */
    @Test
    void aJoinersAllowanceForItsProofRunsFromTheRespondersAnswer() throws Exception {
        ControllerEngine responder = node("rc", true);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs, 2);                                   // idle: 2 s
        try {
            long[] at = new long[1];
            ControllerEngine a2 = node("a2", false), a3 = node("a3", false);
            assertEquals(0, jobs.submit("other", () -> { try { Thread.sleep(1_500); } catch (InterruptedException ignore) { } }));
            String waited = exchange(a2, port(server), 1_200, at);                                 // its hello waits 1.5 s; its proof takes 1.2 s
            System.out.println("DiscoveryStormTest: hello waited in the queue (challenge after " + at[0] + " ms), proof 1.2 s later: " + waited);
            assertTrue(at[0] >= 1_300, "the hello did not wait: " + at[0] + " ms");
            assertTrue(waited.startsWith("result ok=true"), "a joiner that sent its proof 1.2 s after its challenge, of 2 s allowed, was closed: " + waited);
            assertEquals(0, admission.closedIdle());
            // a joiner that sends nothing after its challenge is still closed when the allowance is over
            String silent = exchange(a3, port(server), 10_000, at);
            assertTrue(silent.startsWith("closed after challenge"), silent);
            assertEquals(1, admission.closedIdle());
        } finally {
            server.close().sync();
        }
    }

    /**
     * The proof of a challenge this responder wrote is answered although the queue's wait passed its bound meanwhile.
     * Fails before: it was told "busy" like a new hello, and the hello's work and the joiner's signature were thrown away.
     */
    @Test
    void aProofIsAnsweredAlthoughTheQueueFilledSinceItsHello() throws Exception {
        ControllerEngine responder = node("rc", true), joiner = node("a4", false);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, 200);                                                        // wait bound 200 ms
        Channel server = listen(responder, admission, jobs);
        try {
            DiscoveryProcessor dp = new DiscoveryProcessor(joiner);
            DiscoveryAuth.Msg hello = dp.clientHello(DiscoveryType.AGENT);
            Raw r = raw(port(server), helloFrame(joiner, hello));
            eventually(() -> r.read.size() == 1, 10_000, () -> "no challenge");
            DiscoveryAuth.Msg challenge = GSON.fromJson(GSON.fromJson(r.read.get(0), io.cresco.library.messaging.MsgEvent.class).getParam(DiscoveryAuth.PARAM),
                    DiscoveryAuth.Msg.class);
            assertEquals("challenge", challenge.step);
            // while the joiner makes its proof, the one worker is taken and other addresses fill the queue past its bound
            CountDownLatch gate = hold(jobs);
            jobs.serviceNanosForTests(100_000_000L);                                               // a request takes 100 ms now
            int n = 0;
            while (jobs.submit("other-" + n, () -> { }) == 0) n++;
            final int filled = n;
            assertTrue(filled >= 2 && jobs.submit("127.0.0.1", () -> { }) > 0, "the queue is not past its bound for a new request (" + filled + " waiting)");
            long busyBefore = jobs.busy();
            r.ch.writeAndFlush(proofFrame(joiner, dp.clientProof(hello, challenge, "127.0.0.1"))).sync();
            eventually(() -> jobs.queued() == filled + 1, 5_000, () -> "the proof was not queued: " + jobs.queued() + " waiting, " + (jobs.busy() - busyBefore)
                    + " told busy, read " + r.read);
            assertEquals(busyBefore, jobs.busy(), "the proof of an exchange already answered was sent away");
            assertFalse(r.closedWithin(300), "the connection was closed on its proof");
            gate.countDown();
            eventually(() -> r.read.size() == 2, 10_000, () -> "no result");
            DiscoveryAuth.Msg result = GSON.fromJson(GSON.fromJson(r.read.get(1), io.cresco.library.messaging.MsgEvent.class).getParam(DiscoveryAuth.PARAM),
                    DiscoveryAuth.Msg.class);
            assertEquals("result", result.step);
            assertTrue(result.ok, "the proof was refused: " + result.reason);
        } finally {
            server.close().sync();
        }
    }

    /**
     * A request whose turn comes later than the queue's bound (the service slowed after it was queued) is told to come
     * back, and its joiner reads that as "busy". Fails before: it was answered late, or waited without a word until its
     * connection's lifetime ended.
     */
    @Test
    void aRequestWhoseTurnComesTooLateIsToldToComeBack() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null), joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, 300);
        Channel server = listen(responder, admission, jobs);
        try {
            CountDownLatch gate = hold(jobs);
            List<TCPDiscoveryStatic.Attempt> attempts = java.util.Collections.synchronizedList(new ArrayList<>());
            Thread[] t = new Thread[3];
            for (int i = 0; i < t.length; i++) {
                t[i] = new Thread(() -> {
                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
                    try { ds.discover(DiscoveryType.AGENT, 10_000, "127.0.0.1", port(server), false); } catch (Exception ignore) { }
                    attempts.add(ds.lastAttempt());
                });
                t[i].start();
            }
            eventually(() -> jobs.queued() == 3, 10_000, () -> "queued " + jobs.queued() + " (told busy at once: " + jobs.busy() + ")");
            Thread.sleep(700);                                    // the one worker stays held far past the 300 ms bound
            gate.countDown();
            for (Thread x : t) x.join(20_000);
            assertEquals(3, jobs.late(), "requests told only when their turn came: " + attempts.stream().map(a -> a.outcome.name()).toList());
            for (TCPDiscoveryStatic.Attempt a : attempts) {
                assertEquals(TCPDiscoveryStatic.Outcome.BUSY, a.outcome);
                assertTrue(a.busyRetryMs >= DiscoveryJobs.MIN_RETRY_MS, "come back in " + a.busyRetryMs);
            }
            eventually(() -> admission.open() == 0, 3000, () -> "a connection told to come back stayed open: " + admission.open());
        } finally {
            server.close().sync();
        }
    }

    /** An answer that fails closes its connection. Fails before: the connection stayed open and silent until it was idle. */
    @Test
    void aRequestWhoseAnswerFailsClosesItsConnection() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null), joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(new io.netty.channel.ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                    @Override
                    protected void initChannel(io.netty.channel.socket.SocketChannel ch) {
                        ch.pipeline().addLast(admission);
                        DiscoveryWire.install(ch.pipeline(), DiscoveryWire.MAX_REQUEST_BYTES);
                        ch.pipeline().addLast(new TCPDiscoveryEngineHandler(responder, jobs) {
                            @Override
                            void answer(io.netty.channel.ChannelHandlerContext ctx, Object msg, Request rq) {
                                throw new IllegalStateException("made to fail");
                            }
                        });
                    }
                }).bind("127.0.0.1", 0).sync().channel();
        try {
            Raw r = raw(port(server), locatorFrame(joiner));
            assertTrue(r.closedWithin(3000), "the connection of a request whose answer failed is open and silent");
            eventually(() -> admission.open() == 0, 3000, () -> "not released: " + admission.open());
            // the connection's next-frame guard was released too: the responder goes on answering others
            assertEquals(0, jobs.queued());
        } finally {
            server.close().sync();
        }
    }

    /** A joiner that arrives while the responder stops is not told "busy, come back in 30 s": there is nothing to come back to. */
    @Test
    void aJoinerArrivingWhileTheResponderStopsIsNotToldBusy() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null), joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs);
        try {
            jobs.shutdown();                                      // the listener still accepts for a moment
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
            assertTrue(ds.discover(DiscoveryType.AGENT, 5_000, "127.0.0.1", port(server), false).isEmpty());
            assertEquals(TCPDiscoveryStatic.Outcome.NO_ANSWER, ds.lastAttempt().outcome, "told busy by a responder that is stopping");
            assertTrue(ds.lastAttempt().elapsedMs < 3000, "closed only after " + ds.lastAttempt().elapsedMs + " ms");
            assertEquals(0, jobs.busy());
        } finally {
            server.close().sync();
        }
    }

    /**
     * One request of a connection at a time: a second frame before the first is answered closes the connection, and only
     * the first is queued. (No exchange sends one; two answered at once would race on the connection's hello and
     * challenge.) Fails when the handler's in-flight guard is removed.
     */
    @Test
    void aSecondRequestBeforeTheFirstIsAnsweredClosesTheConnection() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null), joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(2, DiscoveryJobs.DEFAULT_MAX_WAIT_MS);
        Channel server = listen(responder, admission, jobs);
        try {
            CountDownLatch gate = hold(jobs);
            long before = jobs.accepted();
            String frame = locatorFrame(joiner);
            Raw r = raw(port(server), frame, frame, frame);
            assertTrue(r.closedWithin(3000), "a connection that sent three requests at once stayed open");
            assertEquals(1, jobs.accepted() - before, "requests of ONE connection queued at the same time");
            gate.countDown();
            eventually(() -> jobs.queued() == 0, 5000, () -> "queued " + jobs.queued());
            // one at a time is as before: a locator, answered, then the next on the same connection
            Raw one = raw(port(server), frame);
            eventually(() -> one.read.size() == 1, 5000, () -> "the first request was not answered");
            one.ch.writeAndFlush(frame).sync();
            eventually(() -> one.read.size() == 2, 5000, () -> "a second request AFTER the first was answered was not answered");
            assertFalse(one.closedWithin(200));
            one.ch.close().sync();
        } finally {
            server.close().sync();
        }
    }

    /**
     * A connection whose request waits in this responder's queue is not idle: it is not closed for it. Fails when the
     * "working" mark is ignored: the request waits 2.5 s here and the allowance is 1 s.
     */
    @Test
    void aConnectionWhoseRequestWaitsIsNotClosedAsIdle() throws Exception {
        ControllerEngine responder = DiscoveryDosTest.engine("rgA", "rc", true, null), joiner = DiscoveryDosTest.engine("rgA", "a1", false, null);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, 600_000);
        Channel server = listen(responder, admission, jobs, 1);                                   // idle: 1 s
        try {
            CountDownLatch gate = hold(jobs);
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(joiner);
            List<DiscoveryNode> found = new ArrayList<>();
            Thread t = new Thread(() -> {
                try { found.addAll(ds.discover(DiscoveryType.AGENT, 20_000, "127.0.0.1", port(server), false)); } catch (Exception ignore) { }
            });
            t.start();
            eventually(() -> jobs.queued() == 1, 10_000, () -> "not queued");
            Thread.sleep(2_500);
            assertEquals(0, admission.closedIdle(), "a connection that waits for THIS node's answer was closed as idle");
            assertEquals(1, admission.open());
            gate.countDown();
            t.join(20_000);
            assertEquals(1, found.size(), "outcome " + ds.lastAttempt().outcome);
        } finally {
            server.close().sync();
        }
    }

    /** A v2 message as release 1.3.379 knows it: no field for a busy answer's time. */
    static final class Msg379 {
        int v;
        String step, type, nc, ns, creg, cagt, ccert, sreg, sagt, scert, mac, reason, rcert, rebind, auth, pop;
        int sport;
        boolean ok;
    }

    /**
     * A busy answer to a joiner of an earlier release. It reads the answer as a step it does not know (its v2 exchange) or
     * as an answer without a node (its locator), ends that attempt, and tries again as it always did; the responder closes
     * the connection behind the answer, and the next attempt is answered.
     */
    @Test
    void anEarlierJoinerToldBusyJustTriesAgain() throws Exception {
        ControllerEngine responder = node("rc", true), joiner = node("a5", false);
        DiscoveryGuard.Admission admission = new DiscoveryGuard.Admission(100, 100, 30_000, null);
        DiscoveryJobs jobs = jobs(1, 20);
        Channel server = listen(responder, admission, jobs);
        try {
            CountDownLatch gate = hold(jobs);
            for (int i = 0; i < 40; i++) jobs.submit("127.0.0.1", () -> { });                     // this address's queue is past the bound
            DiscoveryAuth.Msg hello = new DiscoveryProcessor(joiner).clientHello(DiscoveryType.AGENT);
            for (String frame : new String[]{helloFrame(joiner, hello), locatorFrame(joiner)}) {
                Raw r = raw(port(server), frame);
                eventually(() -> r.read.size() == 1, 5000, () -> "no answer");
                assertTrue(r.closedWithin(3000), "the responder left the connection open behind its busy answer");
                io.cresco.library.messaging.MsgEvent me = GSON.fromJson(r.read.get(0), io.cresco.library.messaging.MsgEvent.class);
                // its locator path: an answer that carries no node ("NO DISCOVERY NODE", nothing discovered, it retries)
                assertNull(me.getParam("discovery_node"));
                // its v2 path: a message of a step it does not know ("unexpected message", the attempt ends, it retries)
                Msg379 m = GSON.fromJson(me.getParam(DiscoveryAuth.PARAM), Msg379.class);
                assertEquals("busy", m.step);
                assertNull(m.mac);
                assertFalse(m.ok);
            }
            gate.countDown();
            eventually(() -> jobs.queued() == 0, 5000, () -> "queued " + jobs.queued());
            // every other message is as release 1.3.379 has it: the busy answer's time is written into none of them
            String challenge = GSON.toJson(new DiscoveryProcessor(responder).serverChallenge(GSON.fromJson(GSON.toJson(hello), DiscoveryAuth.Msg.class), "127.0.0.1"));
            assertFalse(GSON.toJson(hello).contains("\"retry\"") || challenge.contains("\"retry\""), "a hello or a challenge carries the busy answer's field");
            assertEquals(1, new TCPDiscoveryStatic(joiner).discover(DiscoveryType.AGENT, 10_000, "127.0.0.1", port(server), true).size(),
                    "the next attempt was not answered");
        } finally {
            server.close().sync();
        }
    }
}
