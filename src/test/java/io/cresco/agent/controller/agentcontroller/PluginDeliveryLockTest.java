package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.agent.test.MockBundleContext;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.service.cm.Configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression (2026-11-09): PluginAdmin.msgIn handed every message to its plugin while it held the agent-wide plugin
 * lock. A plugin's inMsg submits to its bounded queue with CallerRuns (PluginBuilder), so when that plugin was busy its
 * handler ran right there, under the lock: every other plugin's messages and every send of every plugin
 * (bundleRunsPlugin) waited for it, and a handler that made an RPC waited for a reply that was itself stuck behind the
 * lock until the RPC timed out. Delivery now takes no lock.
 */
class PluginDeliveryLockTest {

    private Path tmp;
    private DBEngine dbe;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("plugin-delivery-lock");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dbe != null) {
            try { dbe.shutdown(); } catch (Exception ignored) { }
        }
        // DBEngine.shutdown() stops the whole Derby engine, which deregisters the driver
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static Object field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(o);
    }

    private DBInterfaceImpl db(PluginBuilder pb) {
        dbe = new DBEngine(pb);
        dbe.addRNode("r1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.addANode("a1", 10, "test", 15000, System.currentTimeMillis(), "{}");
        dbe.assoicateANodetoRNode("r1", "a1");
        return new DBInterfaceImpl(pb, dbe);
    }

    /**
     * A running plugin of {@code bundle}, as addPlugin and startPlugin leave it; its inMsg is {@code inMsg}. With no
     * {@code inMsg}: added but not started (no service yet), an inactive plugin.
     */
    @SuppressWarnings("unchecked")
    private static PluginNode running(PluginAdmin pa, PluginBuilder pb, DBInterfaceImpl gdb, String id, long bundle,
                                      Consumer<MsgEvent> inMsg) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco." + id);
        cfg.put("jarfile", id + ".jar");
        cfg.put("persistence_code", "0");
        PluginNode node = new PluginNode(pb, gdb, bundle, id, cfg, null);
        node.setStatus(10, "Plugin Active");
        if (inMsg != null) node.setPluginService((PluginService) Proxy.newProxyInstance(PluginService.class.getClassLoader(),
                new Class<?>[]{PluginService.class}, (p, m, a) -> {
                    if ("inMsg".equals(m.getName())) {
                        inMsg.accept((MsgEvent) a[0]);
                        return Boolean.TRUE;
                    }
                    return m.getReturnType() == boolean.class ? Boolean.TRUE : null;
                }));
        Configuration conf = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[]{Configuration.class}, (p, m, a) -> "getPid".equals(m.getName()) ? "pid." + id : null);
        ((Map<String, PluginNode>) field(pa, "pluginMap")).put(id, node);
        ((Map<String, Configuration>) field(pa, "configMap")).put(id, conf);
        return node;
    }

    private static MsgEvent to(String plugin, String action) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", "plugin-x", "r1", "a1", plugin, false, false);
        m.setParam("action", action);
        return m;
    }

    private static Executor exec(java.util.function.Function<MsgEvent, MsgEvent> onExec) {
        return (Executor) Proxy.newProxyInstance(Executor.class.getClassLoader(), new Class<?>[]{Executor.class},
                (p, m, a) -> "executeEXEC".equals(m.getName()) ? onExec.apply((MsgEvent) a[0]) : null);
    }

    @Test
    void aHandlerRunByCallerRunsGetsItsRpcReplyFromAnotherThreadAtOnce() throws Exception {
        String dir = tmp.resolve("agent").toString();
        PluginBuilder agent = TestAgentService.plugin(dir, new HashMap<>());
        DBInterfaceImpl gdb = db(agent);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(dir), agent, null, gdb, new MockBundleContext(), null);
        AtomicReference<String> sendCheck = new AtomicReference<>();
        // plugin-a's sends, as the agent takes them (AgentServiceImpl.msgOutFromBundle): the send check, then the callee
        // answers on its own thread and the reply is delivered to plugin-a through PluginAdmin.msgIn
        Consumer<MsgEvent> agentOut = req -> {
            sendCheck.set(String.valueOf(pa.bundleRunsPlugin(42L, "plugin-a")));
            Thread callee = new Thread(() -> {
                req.setReturn();
                req.setParam("ttl", "1");
                pa.msgIn(req);
            }, "callee");
            callee.setDaemon(true);
            callee.start();
        };
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginID", "plugin-a");
        PluginBuilder a = TestAgentService.plugin(dir, cfg, agentOut, "r1", "a1");
        CountDownLatch fillGate = new CountDownLatch(1);
        AtomicReference<String> handlerThread = new AtomicReference<>();
        AtomicReference<MsgEvent> reply = new AtomicReference<>();
        AtomicLong rpcMs = new AtomicLong(-1);
        a.setExecutor(exec(m -> {
            if ("fill".equals(m.getParam("action"))) {
                try { fillGate.await(30, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return null;
            }
            handlerThread.set(Thread.currentThread().getName());
            long t0 = System.nanoTime();
            MsgEvent req = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", "plugin-a", "r1", "a2", "plugin-x", false, false);
            req.setParam("action", "lookup");
            reply.set(a.sendRPC(req, 4000));
            rpcMs.set((System.nanoTime() - t0) / 1_000_000);
            return null;
        }));
        running(pa, agent, gdb, "plugin-a", 42L, a::msgIn);
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            // plugin-a is busy: its threads and its queue (PluginBuilder: max(4, cpus) threads, 1024 queued) are taken
            int busy = Math.max(4, Runtime.getRuntime().availableProcessors()) + 1024;
            for (int i = 0; i < busy; i++) a.msgIn(to("plugin-a", "fill"));

            // the next message to plugin-a runs on the thread that delivers it (CallerRuns), inside PluginAdmin.msgIn
            Future<?> delivered = pool.submit(() -> {
                Thread.currentThread().setName("deliverer");
                pa.msgIn(to("plugin-a", "work"));
            });
            delivered.get(10, TimeUnit.SECONDS);
            assertEquals("deliverer", handlerThread.get(), "the handler ran on the delivering thread (CallerRuns)");
            assertEquals("true", sendCheck.get(), "the handler's send passed the send check");
            assertNotNull(reply.get(), "the RPC reply, delivered on another thread, reached the handler (it waited "
                    + rpcMs.get() + " ms)");
            assertTrue(rpcMs.get() < 2000, "the reply came at once, not after a wait on a lock: " + rpcMs.get() + " ms");
        } finally {
            fillGate.countDown();
            pool.shutdownNow();
            pa.close();
        }
    }

    @Test
    void onePluginThatIsSlowToTakeAMessageHoldsUpNoOtherPlugin() throws Exception {
        String dir = tmp.resolve("agent").toString();
        PluginBuilder agent = TestAgentService.plugin(dir, new HashMap<>());
        DBInterfaceImpl gdb = db(agent);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(dir), agent, null, gdb, new MockBundleContext(), null);
        CountDownLatch aEntered = new CountDownLatch(1), aGate = new CountDownLatch(1), bGot = new CountDownLatch(1);
        running(pa, agent, gdb, "plugin-a", 42L, m -> {
            aEntered.countDown();
            try { aGate.await(20, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        running(pa, agent, gdb, "plugin-b", 43L, m -> bGot.countDown());
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            Future<?> toA = pool.submit(() -> pa.msgIn(to("plugin-a", "work")));
            assertTrue(aEntered.await(5, TimeUnit.SECONDS));

            // while plugin-a holds its delivery: plugin-b receives, plugin-b sends, the agent lists its plugins
            pool.submit(() -> pa.msgIn(to("plugin-b", "work")));
            assertTrue(bGot.await(2, TimeUnit.SECONDS), "plugin-b received while plugin-a was slow to take its message");
            assertEquals(Boolean.TRUE, pool.submit(() -> pa.bundleRunsPlugin(43L, "plugin-b")).get(2, TimeUnit.SECONDS),
                    "plugin-b's send passed the send check at once");
            String list = pool.submit(pa::getPluginList).get(2, TimeUnit.SECONDS);
            assertTrue(list.contains("plugin-a") && list.contains("plugin-b"), list);
            assertFalse(toA.isDone());

            aGate.countDown();
            toA.get(5, TimeUnit.SECONDS);
        } finally {
            aGate.countDown();
            pool.shutdownNow();
            pa.close();
        }
    }

    @Test
    void anInactiveOrRemovedPluginGetsNothingAndNoLongerSends() throws Exception {
        String dir = tmp.resolve("agent").toString();
        PluginBuilder agent = TestAgentService.plugin(dir, new HashMap<>());
        DBInterfaceImpl gdb = db(agent);
        PluginAdmin pa = new PluginAdmin(new TestAgentService(dir), agent, null, gdb, new MockBundleContext(), null);
        AtomicInteger toC = new AtomicInteger(), toD = new AtomicInteger();
        try {
            running(pa, agent, gdb, "plugin-c", 42L, null);
            running(pa, agent, gdb, "plugin-d", 42L, m -> toD.incrementAndGet());
            pa.msgIn(to("plugin-d", "x"));
            assertEquals(1, toD.get());

            // inactive (added, not started): not delivered
            pa.msgIn(to("plugin-c", "x"));
            assertEquals(0, toC.get(), "an inactive plugin gets nothing");

            // removed: not delivered, and its id is no longer owned by its bundle; another bundle never sends as a plugin
            assertTrue(pa.bundleRunsPlugin(42L, "plugin-d"));
            assertFalse(pa.bundleRunsPlugin(43L, "plugin-d"), "another bundle never sends as plugin-d");
            assertTrue(pa.removePlugin("plugin-d"));
            pa.msgIn(to("plugin-d", "x"));
            assertEquals(1, toD.get(), "a removed plugin gets nothing");
            assertFalse(pa.bundleRunsPlugin(42L, "plugin-d"), "a removed plugin no longer sends");
            assertFalse(pa.pluginIdsOf(42L).contains("plugin-d"));
            assertTrue(pa.pluginIdsOf(42L).contains("plugin-c"));
            pa.msgIn(to(null, "x"));         // no plugin named: nothing, no error
        } finally {
            pa.close();
        }
    }
}
