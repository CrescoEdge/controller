package io.cresco.agent.controller.measurement;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OUT-32: the broker's own memory, store and temp usage reach the metric inventory, and the per-destination
 * sample stays bounded however many destinations the broker has.
 */
class BrokerMemoryMetricsTest {

    private static final long MEMORY_LIMIT = 64L * 1024 * 1024;
    private static final long STORE_LIMIT = 1024L * 1024 * 1024;
    private static final long TEMP_LIMIT = 256L * 1024 * 1024;

    private BrokerService broker;
    private Connection conn;

    private BrokerService start(String name) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.setAdvisorySupport(false);          // only the test's own destinations hold memory: exact expectations
        SystemUsage su = new SystemUsage();
        MemoryUsage mu = new MemoryUsage();
        mu.setLimit(MEMORY_LIMIT);
        su.setMemoryUsage(mu);
        su.getStoreUsage().setLimit(STORE_LIMIT);
        su.getTempUsage().setLimit(TEMP_LIMIT);
        b.setSystemUsage(su);
        b.start();
        b.waitUntilStarted();
        conn = new ActiveMQConnectionFactory("vm://" + name + "?create=false&jms.alwaysSyncSend=true").createConnection();
        conn.start();
        return b;
    }

    @AfterEach
    void stop() throws Exception {
        if (conn != null) try { conn.close(); } catch (Exception ignored) { }
        if (broker != null) { broker.stop(); broker.waitUntilStopped(); }
    }

    /** Sends (synchronously) {@code count} non-persistent messages of {@code bytes} each to a queue nobody consumes. */
    private void fill(String queue, int count, int bytes) throws Exception {
        Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = s.createProducer(new ActiveMQQueue(queue));
        p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        for (int i = 0; i < count; i++) {
            jakarta.jms.BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[bytes]);
            p.send(m);
        }
        s.close();
    }

    private static double value(SimpleMeterRegistry r, String name) {
        return r.get(name).gauge().value();
    }

    private long usageOf(String queue) throws Exception {
        Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d.getMemoryUsage().getUsage();
    }

    @Test
    void brokerWideUsageAndLimitsAreInTheInventory() throws Exception {
        broker = start("bmm-wide");
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        assertEquals(BrokerMemoryMetrics.NAMES, new BrokerMemoryMetrics(() -> broker, 64, 0).register(reg));
        for (String n : BrokerMemoryMetrics.NAMES) assertNotNull(reg.find(n).gauge(), n);

        assertEquals(MEMORY_LIMIT, value(reg, "broker.memory.limit"));
        assertEquals(0.0, value(reg, "broker.memory.used"));
        fill("q.big", 50, 16 * 1024);
        fill("q.small", 2, 1024);
        double used = value(reg, "broker.memory.used");
        assertTrue(used >= 50 * 16 * 1024, "the broker-wide figure counts every queued message: " + used);
        assertEquals(broker.getSystemUsage().getMemoryUsage().getPercentUsage(), value(reg, "broker.memory.percent"));
        assertEquals(broker.getSystemUsage().getStoreUsage().getLimit(), value(reg, "broker.store.limit"));
        assertEquals(broker.getSystemUsage().getTempUsage().getLimit(), value(reg, "broker.temp.limit"));
        assertTrue(value(reg, "broker.temp.limit") > 0);
        assertFalse(Double.isNaN(value(reg, "broker.store.used")));
        assertFalse(Double.isNaN(value(reg, "broker.temp.used")));
        assertTrue(value(reg, "broker.destinations") >= 2, "two queues (plus advisory topics)");

        assertEquals(usageOf("q.big"), (long) value(reg, "broker.destination.memory.max"));
        assertEquals(broker.getDestination(new ActiveMQQueue("q.big")).getMemoryUsage().getPercentUsage(),
                value(reg, "broker.destination.memory.max.percent"));
    }

    @Test
    void thePerDestinationSampleIsBoundedAndFindsTheLargest() throws Exception {
        broker = start("bmm-bounded");
        int queues = 60, scanMax = 10;
        for (int i = 0; i < queues; i++) fill("q." + i, 1, 512);
        fill("q.27", 40, 8 * 1024);                                       // the largest, somewhere in the map
        Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        s.createConsumer(new ActiveMQTopic("t.1")).close();               // a topic counts too

        BrokerMemoryMetrics m = new BrokerMemoryMetrics(() -> broker, scanMax, 0);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        m.register(reg);
        assertTrue(value(reg, "broker.destinations") >= queues + 1);

        long max = 0;
        int refreshes = 0;
        // a full cycle of the cursor takes ceil(destinations / scanMax) refreshes; each one visits <= scanMax
        for (; refreshes < 2 * (queues / scanMax + 2); refreshes++) {
            m.refresh();
            assertTrue(m.lastVisited() <= scanMax, "visited " + m.lastVisited());
            max = (long) value(reg, "broker.destination.memory.max");
            if (max == usageOf("q.27")) break;
        }
        assertEquals(usageOf("q.27"), max, "found within a cursor cycle");
        assertTrue(refreshes <= queues / scanMax + 2, "refreshes " + refreshes);

        // the largest drains: its candidate is re-read on the next refresh, so the max drops at once
        MessageConsumer c = s.createConsumer(new ActiveMQQueue("q.27"));
        for (int i = 0; i < 41; i++) assertNotNull(c.receive(5_000));
        c.close();
        long deadline = System.currentTimeMillis() + 10_000;
        while (usageOf("q.27") > 0 && System.currentTimeMillis() < deadline) Thread.sleep(20);
        m.refresh();
        assertTrue(value(reg, "broker.destination.memory.max") < 8 * 1024, "drained: " + value(reg, "broker.destination.memory.max"));
    }

    @Test
    void refreshesAreRateLimited() throws Exception {
        broker = start("bmm-rate");
        fill("q.a", 5, 1024);
        BrokerMemoryMetrics m = new BrokerMemoryMetrics(() -> broker, 64, 60_000);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        m.register(reg);
        double first = value(reg, "broker.destination.memory.max");
        assertEquals(usageOf("q.a"), (long) first);
        fill("q.b", 50, 8 * 1024);                                        // bigger, but the sample is not due yet
        assertEquals(first, value(reg, "broker.destination.memory.max"), "no rescan on every scrape");
        assertTrue(value(reg, "broker.memory.used") > first, "the broker-wide figure is always live");
    }

    @Test
    void noBrokerOrAStoppedOneReportsNaNAndARestartedOneIsFollowed() throws Exception {
        AtomicReference<BrokerService> ref = new AtomicReference<>();
        BrokerMemoryMetrics m = new BrokerMemoryMetrics(ref::get, 64, 0);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        m.register(reg);
        for (String n : BrokerMemoryMetrics.NAMES) assertTrue(Double.isNaN(value(reg, n)), n);

        broker = start("bmm-restart");
        ref.set(broker);
        fill("q.x", 3, 1024);
        assertEquals(MEMORY_LIMIT, value(reg, "broker.memory.limit"));
        assertEquals(usageOf("q.x"), (long) value(reg, "broker.destination.memory.max"));

        conn.close(); conn = null;
        broker.stop();
        broker.waitUntilStopped();
        assertTrue(Double.isNaN(value(reg, "broker.memory.used")));
        assertTrue(Double.isNaN(value(reg, "broker.destination.memory.max")));

        broker = start("bmm-restart2");                                    // the controller's broker rebuilt
        ref.set(broker);
        assertEquals(0.0, value(reg, "broker.destination.memory.max"), "no stale candidate from the old broker");
        fill("q.y", 2, 2048);
        assertEquals(usageOf("q.y"), (long) value(reg, "broker.destination.memory.max"));
    }
}
