package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.statemachine.ControllerSMHandler;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node can be asked before it can answer: its inbox consumer existed some 20 ms before its producer (at its start, and
 * each time it joined a parent again), so a request that was waiting in its inbox was served and answered into nothing --
 * {@code AgentProducer is null. Message not sent}, one ERROR line and a caller left to time out -- and so was whatever a
 * plugin sent in that moment. Now the producer is made before the inbox consumer (ControllerSMHandler.initIOChannels);
 * and whenever a message still meets a node without a producer (its link is being made anew), it is held for it, bounded
 * in count and time, or its sender is told at once: never dropped with only a line in the log.
 *
 * <p>A node with its real inbox, router and producer on a broker in this JVM ({@code vm://localhost}, the broker a
 * controller's own node uses).
 */
class ReplyAtFirstInstantTest {

    private static final Gson GSON = new Gson();
    private static final String VM = "vm://localhost?create=false";
    private BrokerService broker;
    private ControllerEngine node;
    private ControllerSMHandler handler;
    private ActiveMQConnection raw;
    private Session rawSession;
    /** Everything the node's controller sent by itself (error replies among them), as it was handed over. */
    private final List<MsgEvent> sentByController = new CopyOnWriteArrayList<>();
    /** Did the node have its producer at the moment the broker registered its inbox consumer? (null: no consumer yet) */
    private final java.util.concurrent.atomic.AtomicReference<Boolean> producerWhenInboxOpened = new java.util.concurrent.atomic.AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.setAdvisorySupport(false);
        // the broker sees the inbox consumer the moment it exists: from then on the node is handed its messages
        broker.setPlugins(new org.apache.activemq.broker.BrokerPlugin[]{b -> new org.apache.activemq.broker.BrokerFilter(b) {
            @Override
            public org.apache.activemq.broker.region.Subscription addConsumer(org.apache.activemq.broker.ConnectionContext context,
                                                                              org.apache.activemq.command.ConsumerInfo info) throws Exception {
                ControllerEngine e = node;
                if (e != null && info.getDestination() != null && "r1_n1".equals(info.getDestination().getPhysicalName())) {
                    producerWhenInboxOpened.compareAndSet(null, e.getActiveClient().hasActiveProducer());
                }
                return super.addConsumer(context, info);
            }
        }});
        broker.start();
        broker.waitUntilStarted();
        raw = (ActiveMQConnection) new ActiveMQConnectionFactory(VM).createConnection();
        raw.start();
        rawSession = raw.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    void stop() throws Exception {
        if (handler != null) { try { handler.shutdown(); } catch (Exception ignore) { } }
        if (node != null) {
            try { node.getActiveClient().stopHolding(); } catch (Exception ignore) { }
            try { node.getActiveClient().shutdown(); } catch (Exception ignore) { }
        }
        try { raw.close(); } catch (Exception ignore) { }
        broker.stop();
        broker.waitUntilStopped();
    }

    /** The node r1/n1: what its controller sends goes the way AgentServiceImpl.msgOut sends it (stamped, then routed). */
    private ControllerEngine node(Map<String, Object> extra) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "false");
        cfg.put("tenant_namespacing", "false");
        cfg.put("tenant_id", "t1");
        cfg.put("route_ready_wait_ms", "100");
        cfg.put("enable_tcp_discovery", "false");
        if (extra != null) cfg.putAll(extra);
        ControllerEngine[] self = new ControllerEngine[1];
        PluginBuilder pb = TestAgentService.plugin(System.getProperty("java.io.tmpdir"), cfg, m -> {
            sentByController.add(GSON.fromJson(GSON.toJson(m), MsgEvent.class));
            self[0].stampSelf(m);
            self[0].msgIn(m);
        }, "r1", "n1");
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        e.cstate = TlsTestSupport.state(pb, "r1", "n1");
        self[0] = e;
        node = e;
        return e;
    }

    private static void await(java.util.concurrent.Callable<Boolean> c, String what) throws Exception {
        long end = System.currentTimeMillis() + 10_000;
        while (!c.call() && System.currentTimeMillis() < end) Thread.sleep(5);
        assertTrue(c.call(), what);
    }

    /**
     * An RPC request of r1/caller's plugin for a plugin n1 does not run: n1's router answers it with an error
     * ({@code route unavailable}), a reply n1's controller sends to the caller. The cheapest request a node answers.
     */
    private static MsgEvent request(int i) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "caller", "pc", "r1", "n1", "plugin-x", false, false);
        m.setParam("action", "noop");
        m.setParam("is_rpc", "true");
        m.setParam("callId-r1-caller-pc", "call-" + i);
        return m;
    }

    private void intoInbox(MsgEvent m) throws Exception {
        rawSession.createProducer(rawSession.createQueue("r1_n1")).send(rawSession.createTextMessage(GSON.toJson(m)));
    }

    /**
     * What reached the caller's inbox within {@code waitMs}, in the order it came: an answer as its call id (it is the
     * router's error: the caller is answered), a one-way report as its name.
     */
    private List<String> atCaller(int expected, long waitMs) throws Exception {
        List<String> got = new ArrayList<>();
        try (MessageConsumer c = rawSession.createConsumer(rawSession.createQueue("r1_caller"))) {
            long end = System.currentTimeMillis() + waitMs;
            while (got.size() < expected && System.currentTimeMillis() < end) {
                TextMessage t = (TextMessage) c.receive(Math.max(1, end - System.currentTimeMillis()));
                if (t == null) break;
                MsgEvent m = GSON.fromJson(t.getText(), MsgEvent.class);
                if (m.getParam("report") != null) {
                    got.add(m.getParam("report"));
                } else {
                    assertNotNull(m.getParam(MsgRouter.ROUTE_UNAVAILABLE), "an answer (here: the router's error)");
                    got.add(m.getParam("callId-r1-caller-pc"));
                }
            }
        }
        return got;
    }

    @Test
    void requestsWaitingInTheInboxWhenTheNodeComesUpAreAllAnswered() throws Exception {
        ControllerEngine e = node(null);
        // they are in its inbox before it consumes: its first messages, taken in its first milliseconds
        int n = 20;
        for (int i = 0; i < n; i++) intoInbox(request(i));
        handler = new ControllerSMHandler(e);
        Method init = ControllerSMHandler.class.getDeclaredMethod("initIOChannels", String.class);
        init.setAccessible(true);
        assertEquals(Boolean.TRUE, init.invoke(handler, "localhost"), "the node's channels came up");
        assertEquals(Boolean.TRUE, producerWhenInboxOpened.get(), "the node had its producer when its inbox consumer was made:"
                + " whatever it is handed first, it can answer");

        List<String> answered = atCaller(n, 10_000);
        assertEquals(n, answered.size(), "every request the node took was answered; it dropped "
                + e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER) + " answer(s) for want of a producer");
        assertEquals(0, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER));
        assertEquals(0, e.getActiveClient().sendsHeldCount(), "the producer was there before the inbox took its first message:"
                + " nothing even had to wait for it");
    }

    @Test
    void anAnswerMadeWhileThereIsNoProducerIsSentWhenThereIsOneAndARequestIsToldAtOnce() throws Exception {
        ControllerEngine e = node(null);
        ActiveClient client = e.getActiveClient();
        // the inbox without the producer: what the old start-up order was for its first 20 ms, and what a node is whenever
        // its link is being made anew
        assertTrue(client.initActiveAgentConsumer("r1_n1", VM));
        int n = 5;
        for (int i = 0; i < n; i++) intoInbox(request(i));
        await(() -> client.sendsHeldCount() == n, "the " + n + " answers wait for the producer (held " + client.sendsHeldCount()
                + ", dropped " + e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER) + ")");
        assertEquals(0, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER), "none was dropped");
        assertEquals(n, client.sendsHeldNow());
        assertTrue(atCaller(1, 200).isEmpty(), "nothing can have left yet");

        // an RPC request this node sends in that state is not held: its caller is answered with an error at once
        MsgEvent own = new MsgEvent(MsgEvent.Type.EXEC, "r1", "n1", "p1", "r1", "caller", null, false, false);
        own.setParam("action", "noop");
        own.setParam("is_rpc", "true");
        own.setParam("callId-r1-n1-p1", "own-call");
        int before = sentByController.size();
        e.stampSelf(own);
        e.msgIn(own);
        assertEquals(n, client.sendsHeldNow(), "the request is not held");
        assertEquals(1, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER), "it is counted as not sent");
        assertTrue(sentByController.size() > before, "and its caller is told before the send returns");
        MsgEvent told = sentByController.get(before);
        assertEquals("own-call", told.getParam("callId-r1-n1-p1"));
        assertNotNull(told.getParam(MsgRouter.ROUTE_UNAVAILABLE), "with the router's error, not a timeout");

        // a plugin's one-way messages in that state wait as well (they were dropped with a line in the log, like the answers)
        for (int i = 0; i < 3; i++) {
            MsgEvent report = new MsgEvent(MsgEvent.Type.EXEC, "r1", "n1", "p1", "r1", "caller", null, false, false);
            report.setParam("action", "noop");
            report.setParam("report", "report-" + i);
            e.stampSelf(report);
            e.msgIn(report);
        }
        assertEquals(n + 3, client.sendsHeldNow());

        // the producer comes: everything that waited leaves, in the order it came to wait
        assertTrue(client.initActiveAgentProducer(VM));
        List<String> arrived = atCaller(n + 3, 10_000);
        assertEquals(java.util.Set.of("call-0", "call-1", "call-2", "call-3", "call-4"), new java.util.HashSet<>(arrived.subList(0, n)),
                "the answers (made by several threads) first: " + arrived);
        assertEquals(List.of("report-0", "report-1", "report-2"), arrived.subList(n, n + 3), "then the reports, in their order");
        await(() -> client.heldSentCount() == n + 3, "sent " + client.heldSentCount() + " of " + (n + 3));
        assertEquals(0, client.sendsHeldNow());
        assertEquals(0, client.heldNotSentCount());
        assertEquals(1, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER), "nothing but the request was counted as not sent");
    }

    // ---- the other half of "it can answer": a credential the far node can verify

    private static java.security.cert.X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    /** A secured agent rgA/{@code name} under the controller {@code rc} (joined: each holds the other's key), on the broker. */
    private ControllerEngine securedAgent(String name, ControllerEngine rc, Map<String, Object> extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_namespacing", "false");
        ControllerEngine a = TlsTestSupport.engine("rgA", name, cfg);
        ((TlsTestSupport.State) a.cstate).parentRegion = "rgA";
        ((TlsTestSupport.State) a.cstate).parentAgent = "rc";
        assertNull(a.getCertificateManager().getPeerTrust().pin("rgA", "rc", leaf(rc), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(rc.getCertificateManager().getPeerTrust().pin("rgA", name, leaf(a), PeerTrustRegistry.ROLE_AGENT, false));
        assertTrue(a.getActiveClient().initActiveAgentProducer(VM));
        return a;
    }

    /** The agent's ping and its controller's pong: the pong brings the attestation. */
    private static void firstPong(ControllerEngine child, ControllerEngine parent) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, "rgA", child.cstate.getAgent(), null, "rgA", "rc", null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = GSON.fromJson(GSON.toJson(p), MsgEvent.class);
        io.cresco.library.security.Caller.controller("t1", "rgA", child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, false);
        p.setReturn();
        child.msgSigning().onParentPong(GSON.fromJson(GSON.toJson(p), MsgEvent.class), "rgA", "rc");
    }

    /** A message of the agent's plugin for {@code region/agent}, stamped as the node stamps it. */
    private static MsgEvent from(ControllerEngine a, String region, String agent, String payload) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgA", a.cstate.getAgent(), "p1", region, agent, null, false, false);
        m.setParam("action", "noop");
        m.setParam("payload", payload);
        a.stampAs(m, a.pluginCaller("p1"), null);
        return m;
    }

    /** The next message in {@code queue} within {@code waitMs}, or null. */
    private MsgEvent next(String queue, long waitMs) throws Exception {
        try (MessageConsumer c = rawSession.createConsumer(rawSession.createQueue(queue))) {
            TextMessage t = (TextMessage) c.receive(waitMs);
            return t == null ? null : GSON.fromJson(t.getText(), MsgEvent.class);
        }
    }

    @Test
    void anAgentNotYetAttestedHoldsWhatOnlyItsAttestationCanSignUntilItsControllersFirstPong() throws Exception {
        ControllerEngine rc = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) rc.cstate).regional = true;
        ControllerEngine a = securedAgent("a1", rc, Map.of());
        node = a;
        ActiveClient client = a.getActiveClient();

        // its own controller verifies its certificate: that message leaves at once
        assertTrue(client.sendMessage(from(a, "rgA", "rc", "for the controller")));
        MsgEvent toController = next("rgA_rc", 5_000);
        assertNotNull(toController, "a message for its own controller does not wait");
        assertTrue(toController.getParam(MsgSigning.CRED).startsWith("cert:"));
        assertEquals(0, client.sendsHeldNow());

        // a node beyond it would refuse the certificate ("the signer's credential does not verify here"): that message
        // waits for the attestation, a request like any other
        assertTrue(client.sendMessage(from(a, "rgB", "b1", "an answer made in the first moments")));
        MsgEvent request = from(a, "rgB", "b1", "a request made in the first moments");
        request.setParam("is_rpc", "true");
        request.setParam("callId-rgA-a1-p1", "c1");
        assertTrue(client.sendMessage(request));
        assertEquals(2, client.sendsHeldNow());
        assertNull(next("rgB_b1", 200), "nothing left with a credential its recipient cannot verify");

        firstPong(a, rc);                                        // some tens of milliseconds after the node came up
        MsgEvent first = next("rgB_b1", 5_000), second = next("rgB_b1", 5_000);
        assertNotNull(first, "sent as soon as the node is attested");
        assertNotNull(second);
        assertEquals("an answer made in the first moments", first.getParam("payload"), "in the order they came");
        assertTrue(first.getParam(MsgSigning.CRED).startsWith("att:"), "signed with the attestation");
        assertTrue(second.getParam(MsgSigning.CRED).startsWith("att:"));
        assertEquals(0, client.sendsHeldNow());
        assertEquals(2, client.heldSentCount());
        assertEquals(0, a.msgCounts().droppedCount());

        // attested: nothing waits any more
        assertTrue(client.sendMessage(from(a, "rgB", "b1", "later")));
        assertEquals(0, client.sendsHeldNow());
        assertNotNull(next("rgB_b1", 5_000));
    }

    @Test
    void anAnswerMadeBeforeTheAgentHasRecordedItsControllerWaitsForTheAttestationToo() throws Exception {
        // the inbox consumes before the node records the controller it joins (initIOChannels, then setAgentSuccess): the
        // release answered a request waiting in its inbox in exactly that window, with its certificate
        ControllerEngine rc = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) rc.cstate).regional = true;
        ControllerEngine a = securedAgent("a3", rc, Map.of());
        node = a;
        TlsTestSupport.State state = (TlsTestSupport.State) a.cstate;
        state.parentRegion = null;
        state.parentAgent = null;
        ActiveClient client = a.getActiveClient();
        assertTrue(client.sendMessage(from(a, "rgB", "b1", "an answer made in the first milliseconds")));
        assertEquals(1, client.sendsHeldNow());
        assertNull(next("rgB_b1", 200));
        state.parentRegion = "rgA";                              // setAgentSuccess, the registration, the first ping
        state.parentAgent = "rc";
        firstPong(a, rc);
        MsgEvent sent = next("rgB_b1", 5_000);
        assertNotNull(sent);
        assertTrue(sent.getParam(MsgSigning.CRED).startsWith("att:"));
    }

    @Test
    void aControllerThatNeverAttestsDoesNotSilenceItsAgent() throws Exception {
        ControllerEngine rc = TlsTestSupport.engine("rgA", "rc", Map.of("broker_security_enabled", "true"));
        ((TlsTestSupport.State) rc.cstate).regional = true;
        ControllerEngine a = securedAgent("a2", rc, Map.of("msg_signing_hold_ms", "300"));
        node = a;
        ActiveClient client = a.getActiveClient();
        long t0 = System.currentTimeMillis();
        assertTrue(client.sendMessage(from(a, "rgB", "b1", "first")));
        assertEquals(1, client.sendsHeldNow());
        MsgEvent sent = next("rgB_b1", 5_000);
        long waited = System.currentTimeMillis() - t0;
        assertNotNull(sent, "after the hold time it is sent as it stands");
        assertTrue(waited >= 300 && waited < 3_000, "held " + waited + " ms, the hold time being 300");
        assertTrue(sent.getParam(MsgSigning.CRED).startsWith("cert:"), "with the certificate, as before");
        assertEquals(0, a.msgCounts().droppedCount(), "it was not dropped");
        // and the wait of this stretch is over: the next message does not wait at all
        assertTrue(client.sendMessage(from(a, "rgB", "b1", "second")));
        assertEquals(0, client.sendsHeldNow());
        assertNotNull(next("rgB_b1", 5_000));
    }

    @Test
    void whatWaitsForAProducerIsBoundedInCountAndInTime() throws Exception {
        ControllerEngine e = node(Map.of("msgin_fault_wait_ms", "400"));
        ActiveClient client = e.getActiveClient();
        int over = 7;
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < SendHold.MAX_MESSAGES + over; i++) {
            MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "n1", "p1", "r1", "caller", null, false, false);
            m.setParam("action", "report");
            m.setParam("n", String.valueOf(i));
            assertEquals(i < SendHold.MAX_MESSAGES, client.sendMessage(m), "message " + i);
        }
        assertEquals(SendHold.MAX_MESSAGES, client.sendsHeldNow(), "at most " + SendHold.MAX_MESSAGES + " wait");
        assertEquals(over, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER), "the others are counted as not sent, at once");
        // one message that would take more room than there is in all is not taken either
        MsgEvent huge = new MsgEvent(MsgEvent.Type.EXEC, "r1", "n1", "p1", "r1", "caller", null, false, false);
        huge.setParam("action", "report");
        huge.setParam("payload", "x".repeat((int) SendHold.MAX_CHARS));
        assertFalse(client.sendMessage(huge));

        // no producer comes: each waited its time and no longer, and each is counted
        await(() -> client.sendsHeldNow() == 0, "still held after the hold time: " + client.sendsHeldNow());
        long waited = System.currentTimeMillis() - t0;
        assertTrue(waited >= 400 && waited < 5_000, "held " + waited + " ms, the hold time being 400");
        assertEquals(SendHold.MAX_MESSAGES, client.heldNotSentCount());
        assertEquals(SendHold.MAX_MESSAGES + over + 1, e.msgCounts().droppedCount(MsgCounts.Drop.NO_SENDER), "every one of them is accounted for");
        assertEquals(0, client.heldSentCount());
        // a producer that comes now has nothing stale to send
        assertTrue(client.initActiveAgentProducer(VM));
        assertTrue(atCaller(1, 300).isEmpty());
    }
}
