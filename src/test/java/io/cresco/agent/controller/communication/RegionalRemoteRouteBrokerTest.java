package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.MsgRouter.Hop;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.regionalcontroller.RegionalExecutor;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A regional-controller RPC sent at the GLOBAL for a REMOTE region ({@code regional_controller_msgevent} with
 * region_id/agent_id, as the global's wsapi builds it) over real embedded brokers: the global's broker and the
 * region's broker joined by the duplex bridge a region builds to its global; the region's agent consumes on the
 * region's broker, as agents do. Every node routes what it consumes with {@link MsgRouter#routePath} and
 * {@link MsgRouter#decide}: relays go to the queue named by the forward destination (what AgentProducer does);
 * the region's regional-controller hop runs the real {@link RegionalExecutor} (sendRegionalMsg's local branch)
 * and sends its reply and its pluginadd back through the same routing.
 *
 * <p>With the fixed decision the rpipelinesubmit runs on the region's RegionalExecutor, its pluginadd reaches
 * the target agent and the reply reaches the global's wsapi; with the 191d895 decision it lands in the region's
 * AgentExecutor and nothing comes back (the RPC timeout seen live).
 */
class RegionalRemoteRouteBrokerTest {

    private static final Gson GSON = new Gson();
    private static final String WSAPI = "plugin/1";

    interface Decider { Hop decide(int rp, MsgEvent m, String region, String agent, String pluginId); }

    private BrokerService globalBroker;
    private BrokerService regionBroker;
    private final List<Connection> connections = new ArrayList<>();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<MsgEvent> wsapiReplies = new CopyOnWriteArrayList<>();

    /** One fabric node: consumes its inbox "<region>_<agent>" on its broker and routes what it gets. */
    final class Node {
        final String name, region, agent;
        final boolean rc, gc;
        final Decider decider;
        Session session;
        RegionalExecutor regionalExecutor;

        Node(String name, String region, String agent, boolean rc, boolean gc, Decider decider) {
            this.name = name; this.region = region; this.agent = agent; this.rc = rc; this.gc = gc; this.decider = decider;
        }

        void start(BrokerService broker) throws Exception {
            Connection c = new ActiveMQConnectionFactory("vm://" + broker.getBrokerName() + "?create=false").createConnection();
            connections.add(c);
            c.start();
            session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session consumerSession = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumerSession.createConsumer(new ActiveMQQueue(region + "_" + agent)).setMessageListener(msg -> {
                try {
                    route(GSON.fromJson(((TextMessage) msg).getText(), MsgEvent.class));
                } catch (Exception e) {
                    events.add(name + " error " + e);
                }
            });
        }

        synchronized void send(MsgEvent m) throws Exception {
            MessageProducer p = session.createProducer(new ActiveMQQueue(m.getForwardDst()));
            p.send(session.createTextMessage(GSON.toJson(m)));
            p.close();
        }

        void route(MsgEvent m) throws Exception {
            int rp = MsgRouter.routePath(m, rc, gc, region, agent, null);
            Hop hop = decider.decide(rp, m, region, agent, null);
            String action = m.getParam("action");
            switch (hop) {
                case LOCAL_AGENT:
                    events.add(name + " AgentExecutor " + action + " rp=" + rp);
                    break;
                case LOCAL_PLUGIN:
                    events.add(name + " " + m.getDstPlugin() + " " + action + " rp=" + rp);
                    if (WSAPI.equals(m.getDstPlugin())) wsapiReplies.add(m);
                    break;
                case LOCAL_REGIONAL_CONTROLLER:
                    // RegionHealthWatcher.sendRegionalMsg, non-global branch
                    if (!m.isGlobal() && m.dstIsLocal(region, agent, null) && regionalExecutor != null) {
                        events.add(name + " RegionalExecutor " + action + " rp=" + rp);
                        MsgEvent ret = regionalExecutor.executeCONFIG(m);
                        if (ret != null && ret.getParams().containsKey("is_rpc")) {
                            ret.setReturn();
                            route(ret);                                    // plugin.msgOut -> msgIn -> route
                        }
                    } else {
                        events.add(name + " regional controller DROP " + action + " rp=" + rp);
                    }
                    break;
                default:
                    events.add(name + " relay " + action + " -> " + m.getForwardDst() + " rp=" + rp);
                    send(m);
            }
        }
    }

    private static BrokerService broker(String name) throws Exception {
        BrokerService b = new BrokerService();
        b.setBrokerName(name);
        b.setPersistent(false);
        b.setUseJmx(false);
        b.setUseShutdownHook(false);
        b.addConnector("tcp://127.0.0.1:0");
        b.start();
        b.waitUntilStarted();
        return b;
    }

    private static boolean await(BooleanSupplier cond, long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    private static String cadl() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("pluginname", "io.cresco.example");
        params.put("jarfile", "example.jar");
        params.put("location_region", "r1");
        params.put("location_agent", "a2");
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type", "dummy");
        node.put("node_name", "n1");
        node.put("node_id", "n1");
        node.put("params", params);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("pipeline_id", "rp-remote");
        payload.put("pipeline_name", "rp-remote");
        payload.put("nodes", List.of(node));
        payload.put("edges", List.of());
        return GSON.toJson(payload);
    }

    /** Run one rpipelinesubmit from the global's wsapi to region r1's controller (r1/a1) under {@code decider}. */
    private void submitFromTheGlobal(Decider decider) throws Exception {
        globalBroker = broker("g-broker-" + System.nanoTime());
        regionBroker = broker("r1-broker-" + System.nanoTime());

        Node global = new Node("G", "rg", "gc", true, true, decider);
        Node region = new Node("R1", "r1", "a1", true, false, decider);   // TestAgentService's identity
        Node agent = new Node("A2", "r1", "a2", false, false, decider);
        // the region's controller: its outgoing messages (the pluginadd) enter its own router
        ControllerEngine engine = new ControllerEngine(null, TestAgentService.plugin(
                System.getProperty("java.io.tmpdir"), new HashMap<>(), m -> {
                    try { region.route(m); } catch (Exception e) { events.add("R1 msgOut error " + e); }
                }), null, null);
        region.regionalExecutor = new RegionalExecutor(engine);

        global.start(globalBroker);
        region.start(regionBroker);
        agent.start(regionBroker);                                          // agents consume on their region's broker

        NetworkConnector nc = regionBroker.addNetworkConnector("static:("
                + globalBroker.getTransportConnectors().get(0).getPublishableConnectString() + ")?maxReconnectDelay=500");
        nc.setName("region-to-global");
        nc.setDuplex(true);
        nc.start();
        assertTrue(await(() -> ActiveBroker.hasLiveQueueRoute(globalBroker, "r1_a1")
                && ActiveBroker.hasLiveQueueRoute(regionBroker, "rg_gc"), 15_000), "bridge carries both inboxes");

        // what the global's wsapi builds for regional_controller_msgevent(region_id=r1, agent_id=a1)
        MsgEvent req = new MsgEvent(MsgEvent.Type.CONFIG, "rg", "gc", WSAPI, "r1", "a1", null, true, false);
        req.setParam("action", "rpipelinesubmit");
        req.setParam("cadl", cadl());
        req.setParam("is_rpc", "true");
        global.route(req);                                                   // wsapi -> plugin.msgOut -> global's router
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) {
            try { c.close(); } catch (Exception ignored) { }
        }
        for (BrokerService b : new BrokerService[]{regionBroker, globalBroker}) {
            if (b != null) { b.stop(); b.waitUntilStopped(); }
        }
    }

    @Test
    void aRegionalRpcSentAtTheGlobalRunsOnTheRemoteRegionsRegionalExecutor() throws Exception {
        submitFromTheGlobal(MsgRouter::decide);

        assertTrue(await(() -> !wsapiReplies.isEmpty() && events.stream().anyMatch(e -> e.startsWith("A2 AgentExecutor pluginadd")),
                15_000), "reply and pluginadd: " + events);
        System.out.println("RegionalRemoteRouteBrokerTest fixed: " + events);
        MsgEvent reply = wsapiReplies.get(0);
        assertEquals("true", reply.getParam("scheduled_regionally"), reply.getParams().toString());
        assertEquals("r1", reply.getSrcRegion());
        assertEquals("a1", reply.getSrcAgent());
        assertTrue(events.contains("G relay rpipelinesubmit -> r1_a1 rp=29359"), events.toString());
        assertTrue(events.contains("R1 RegionalExecutor rpipelinesubmit rp=20847"), events.toString());
        assertTrue(events.stream().noneMatch(e -> e.contains("AgentExecutor rpipelinesubmit")), events.toString());
        assertTrue(events.stream().noneMatch(e -> e.contains("error") || e.contains("DROP")), events.toString());
        Thread.sleep(300);                                                   // nothing loops or repeats
        assertEquals(1, Collections.frequency(events, "R1 RegionalExecutor rpipelinesubmit rp=20847"), events.toString());
        assertEquals(1, wsapiReplies.size(), events.toString());
    }

    @Test
    void theOldDecisionHandedItToTheRegionsAgentExecutor() throws Exception {
        Map<Integer, Hop> pinned = MsgRouterTableTest.pinnedTable();
        submitFromTheGlobal((rp, m, r, a, p) -> MsgRouterTableTest.pinnedDecision(pinned, rp, m, r, a, p));

        assertTrue(await(() -> events.contains("R1 AgentExecutor rpipelinesubmit rp=20847"), 15_000), events.toString());
        Thread.sleep(500);
        System.out.println("RegionalRemoteRouteBrokerTest 191d895: " + events);
        assertTrue(wsapiReplies.isEmpty(), "no reply ever reaches the wsapi: " + events);
        assertTrue(events.stream().noneMatch(e -> e.contains("RegionalExecutor")), events.toString());
    }
}
