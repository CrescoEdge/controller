package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: StdinGuard. Each test asserts the promised
 * outcome ("no dataplane subscription but the executor's sees a process's stdin: a selector naming it is refused, any
 * other is narrowed so it cannot match"); a failure is a finding. Review-only: never pushed.
 *
 * <p>The hole: {@link StdinGuard#narrow} returns a selector unchanged when it merely CONTAINS the text of
 * {@link StdinGuard#NO_STDIN}, and {@link StdinGuard#refusal} only looks for the literal {@code 'input} followed by
 * {@code - ' % _}. {@code NO_STDIN OR type LIKE '_nput%'} passes the refusal (no {@code 'input} literal; the excluded
 * mentions inside NO_STDIN are blanked first), is then not narrowed (it contains NO_STDIN), and matches every stdin frame.
 */
class ReviewPhase3StdinGuardTest {

    /** Selectors that ask for stdin without the literal the refusal looks for, wrapped around the guard's own text. */
    static final String[] SMUGGLED = {
            StdinGuard.NO_STDIN + " OR type LIKE '_nput%'",
            "type LIKE '%nput-%' OR " + StdinGuard.NO_STDIN,
            "(" + StdinGuard.NO_STDIN + ") OR (type > 'inpus' AND type < 'inpuu')",
    };

    private static boolean matchesStdin(String selector) throws Exception {
        BooleanExpression e = SelectorParser.parse(selector);
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setStringProperty("stream_name", "proc1");
        m.setStringProperty("type", "input-0a1b2c");
        m.setText("secret typed into a process");
        MessageEvaluationContext ctx = new NonCachedMessageEvaluationContext();
        ctx.setMessageReference(m);
        return e.matches(ctx);
    }

    @Test
    void aSelectorThatQuotesTheGuardIsStillRefusedOrNarrowed() throws Exception {
        assertTrue(matchesStdin(SMUGGLED[0]), "test premise: " + SMUGGLED[0] + " selects stdin");
        for (String s : SMUGGLED) {
            boolean selects;
            try {
                selects = matchesStdin(s);
            } catch (jakarta.jms.InvalidSelectorException unparseable) {
                // (fix commit) this selector engine cannot read it at all ('inpus' > is no valid comparison): it must be refused
                assertNotNull(StdinGuard.refusal(s), "an unparseable selector is accepted: [" + s + "]");
                continue;
            }
            if (!selects) continue;                                      // this selector engine does not read it so
            String why = StdinGuard.refusal(s);
            if (why != null) continue;                                   // refused: fine
            String effective = StdinGuard.narrow(s);
            assertFalse(matchesStdin(effective), "StdinGuard accepted [" + s + "] and its narrowed form [" + effective
                    + "] still matches a stdin frame (type=input-0a1b2c): narrow() skips any selector that contains NO_STDIN");
        }
    }

    // ---- the same selector against a real broker with the Cresco authorization plugin (the broker-side guard) ----

    private static RegionCA ca;
    private static TlsTestSupport.Id brokerId, a1, a2;
    private final List<BrokerService> brokers = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();
    private BrokerService b;

    @BeforeAll
    static void pki() throws Exception {
        TlsTestSupport.provider();
        ca = RegionCA.generate("t1", "rgA", 2048, 1);
        brokerId = TlsTestSupport.issued(ca, "t1", "rgA", "rc");
        a1 = TlsTestSupport.issued(ca, "t1", "rgA", "a1");
        a2 = TlsTestSupport.issued(ca, "t1", "rgA", "a2");
    }

    @BeforeEach
    void start() throws Exception {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        assertNull(peers.trustCa(ca.caCert(), "rgA", "t1", "test"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("tenant_id", "t1");
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("rvp3-stdin").toString(), cfg);
        CrescoAuthorizationBroker authz = new CrescoAuthorizationBroker(pb, peers);
        b = TlsTestSupport.broker("rgA_rc", brokerId, true, new BrokerPlugin[]{authz}, authz, ca.caCert());
        brokers.add(b);
    }

    @AfterEach
    void stop() throws Exception {
        for (Connection c : connections) try { c.close(); } catch (Exception ignore) { }
        for (BrokerService s : brokers) s.stop();
    }

    private Session client(TlsTestSupport.Id id) throws Exception {
        ActiveMQSslConnectionFactory f = new ActiveMQSslConnectionFactory("nio+ssl://127.0.0.1:" + TlsTestSupport.port(b) + "?verifyHostName=false");
        f.setKeyAndTrustManagers(id.keyManagers(), TlsTestSupport.trustManagers(ca.caCert()), new SecureRandom());
        Connection c = f.createConnection();
        connections.add(c);
        c.start();
        return c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    void aRemoteNodeCannotReadStdinWithASmuggledSelector() throws Exception {
        Session spy = client(a2);
        MessageConsumer c;
        try {
            c = spy.createConsumer(spy.createTopic("global.event"), SMUGGLED[0]);
        } catch (JMSException refused) {
            return;                                                      // refused at subscribe: fine
        }
        Session tx = client(a1);
        MessageProducer p = tx.createProducer(tx.createTopic("global.event"));
        TextMessage stdin = tx.createTextMessage("secret typed into a process");
        stdin.setStringProperty("type", "input-0a1b2c");
        p.send(stdin);
        TextMessage got = (TextMessage) c.receive(3000);
        assertNull(got, "a2 subscribed to global.event with [" + SMUGGLED[0] + "] and received a1's stdin frame: '"
                + (got == null ? "" : got.getText()) + "'");
    }
}
