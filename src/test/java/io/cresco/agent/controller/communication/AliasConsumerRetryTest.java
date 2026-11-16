package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * F-17, the moved node's side: its broker gives it the inbox of its former address (region_alias) only once the fabric's
 * record of its move is known there, which a region controller may first have to ask its global -- also after it restarted.
 * Here a broker refuses that consumer until told otherwise. The node's inbox works meanwhile, the consumer is asked for
 * again until it is given, and after a broker restart (the failover transport re-adds it, the broker refuses it without the
 * client being told) it is asked for anew -- with no inbox failure: removing a consumer the broker never registered failed
 * the whole connection (seen live: "Cannot remove a consumer that had not been registered").
 */
class AliasConsumerRetryTest {

    private static final String INBOX = "r1_a1", FORMER = "old_a1";
    private final AtomicBoolean give = new AtomicBoolean();
    private final AtomicInteger refused = new AtomicInteger();
    private BrokerService broker;
    private int port;

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("alias-retry");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        broker.setPlugins(new BrokerPlugin[]{next -> new BrokerFilter(next) {
            @Override
            public Subscription addConsumer(ConnectionContext ctx, ConsumerInfo info) throws Exception {
                if (FORMER.equals(info.getDestination().getPhysicalName()) && !give.get()) {
                    refused.incrementAndGet();
                    throw new SecurityException("not known here yet (test)");
                }
                return super.addConsumer(ctx, info);
            }
        }});
        broker.addConnector("tcp://127.0.0.1:" + port);
        broker.start();
        broker.waitUntilStarted();
    }

    private void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        startBroker();
        AgentConsumer.ALIAS_RETRY_MS = 100;
        AgentConsumer.ALIAS_RETRY_MAX_MS = 200;
    }

    @AfterEach
    void tearDown() throws Exception {
        AgentConsumer.ALIAS_RETRY_MS = 2_000;
        AgentConsumer.ALIAS_RETRY_MAX_MS = 15_000;
        stopBroker();
    }

    private void send(String queue) throws Exception {
        try (Connection c = new ActiveMQConnectionFactory("tcp://127.0.0.1:" + port).createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MsgEvent me = new MsgEvent(MsgEvent.Type.WATCHDOG, "r1", "a9", null, "r1", "a1", null, false, false);
            s.createProducer(s.createQueue(queue)).send(s.createTextMessage(new Gson().toJson(me)));
        }
    }

    private int consumers(String queue) throws Exception {
        Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getConsumers().size();
    }

    private long dequeued(String queue) throws Exception {
        Destination d = broker.getDestination(new ActiveMQQueue(queue));
        return d == null ? 0 : d.getDestinationStatistics().getDequeues().getCount();
    }

    private static void await(String what, long ms, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long end = System.currentTimeMillis() + ms;
        while (!cond.call() && System.currentTimeMillis() < end) Thread.sleep(25);
        assertTrue(cond.call(), what);
    }

    @Test
    void theInboxOfTheFormerAddressIsAskedForUntilGivenAlsoAfterAReconnect() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("agentconsumer_dedicated_connection", "true");
        cfg.put("broker_security_enabled", "false");
        cfg.put("region_alias", "old");
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> { }, "r1", "a1");
        ControllerEngine engine = new ControllerEngine(null, pb, null, null);
        String failover = "failover:(tcp://127.0.0.1:" + port + ")?maxReconnectAttempts=100&initialReconnectDelay=100&useExponentialBackOff=false";
        long failures = ControlPlaneFailures.INBOX_FAILURES.get();
        AgentConsumer consumer = new AgentConsumer(engine, INBOX, failover);     // refused: not the inbox's failure
        try {
            send(INBOX);
            await("the inbox works while the former address is refused", 10_000, () -> dequeued(INBOX) == 1);
            await("asked for again", 10_000, () -> refused.get() >= 2);
            assertEquals(0, consumers(FORMER));

            give.set(true);
            await("given once the broker gives it", 10_000, () -> consumers(FORMER) == 1);
            send(FORMER);
            await("what is addressed to the former address arrives", 10_000, () -> dequeued(FORMER) == 1);

            // the broker restarts and, at first, does not know the record (a region controller that restarted)
            give.set(false);
            int before = refused.get();
            stopBroker();
            startBroker();
            await("the reconnected transport's consumer is refused, and it is asked for again", 30_000, () -> refused.get() >= before + 2);
            assertEquals(0, consumers(FORMER));
            give.set(true);
            await("given again after the reconnect", 10_000, () -> consumers(FORMER) == 1);
            send(FORMER);
            send(INBOX);
            await("the former address after the reconnect", 10_000, () -> dequeued(FORMER) == 1);
            await("the inbox after the reconnect", 10_000, () -> dequeued(INBOX) == 1);
            assertEquals(failures, ControlPlaneFailures.INBOX_FAILURES.get(), "the inbox failed (and was rebuilt)");
            assertEquals(0, consumer.rebuilds(), "the inbox was rebuilt");
        } finally {
            consumer.shutdown();
            engine.getActiveClient().shutdown();
        }
    }
}
