package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.ControllerTierRoutingTest.Node;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The controller tiers answer an RPC request and nothing else, and no message is routed more than {@link MsgRouter#TTL_MAX}
 * times whatever count its sender wrote. A message that merely carried {@code is_rpc} used to be answered; its answer,
 * with no call waiting, was run as a request by its receiver and answered again -- and a sender could write a negative
 * {@code ttl} so that the limit that ends such a loop was two billion routings away.
 */
class OnlyRequestsAreAnsweredControllerTest {

    @Test
    void theGlobalAnswersARequest() {
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        g.router.route(ControllerTierRoutingTest.rpc(MsgEvent.Type.EXEC, "netresourceinfo", true, "r1", "a1"));
        assertEquals("net-of-r1_a1", g.onlyReply().getParam("netresourceinfo"));
    }

    @Test
    void theGlobalRunsButDoesNotAnswerAMessageThatOnlyCarriesIsRpc() {
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = ControllerTierRoutingTest.rpc(MsgEvent.Type.EXEC, "netresourceinfo", true, "r1", "a1");
        m.removeParam("callId-r1-a2-plugin/1");                      // nobody waits for an answer
        g.router.route(m);
        assertTrue(g.sent.isEmpty(), "answered: " + g.sent);
    }

    @Test
    void anAnswerNobodyWaitsForIsNotAnsweredAgain() {
        // what r1/a2's plugin sends back for a call this global never made: the call id is keyed by the destination
        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", "plugin/1", "r1", "a1", null, true, true);
        m.setParam("action", "netresourceinfo");
        m.setParam("is_rpc", "true");
        m.setParam("callId-r9-x-plugin/7", "c");
        g.router.route(m);
        assertTrue(g.sent.isEmpty(), "answered again (the bounce): " + g.sent);
    }

    @Test
    void theRegionalTierAnswersOnlyRequestsToo() {
        Node r = new Node(ControllerMode.REGION_GLOBAL, Map.of());
        r.state.globalRegion = "rg";
        r.state.globalAgent = "gc";
        MsgEvent ping = ControllerTierRoutingTest.rpc(MsgEvent.Type.EXEC, "ping", false, "r1", "a1");
        r.router.route(ping);
        assertEquals(1, r.sent.size(), "a ping request is answered: " + r.sent);
        r.sent.clear();
        MsgEvent notARequest = ControllerTierRoutingTest.rpc(MsgEvent.Type.EXEC, "ping", false, "r1", "a1");
        notARequest.removeParam("callId-r1-a2-plugin/1");
        r.router.route(notARequest);
        assertTrue(r.sent.isEmpty(), "answered: " + r.sent);
    }

    @Test
    void aMessageIsRoutedAtMostTtlMaxTimesWhateverCountItsSenderWrote() {
        assertEquals(Integer.valueOf(0), MsgRouter.nextTtl(null), "a message with no count starts at 0");
        assertEquals(Integer.valueOf(1), MsgRouter.nextTtl("0"));
        assertEquals(Integer.valueOf(MsgRouter.TTL_MAX + 1), MsgRouter.nextTtl(String.valueOf(MsgRouter.TTL_MAX)));
        assertNull(MsgRouter.nextTtl(String.valueOf(MsgRouter.TTL_MAX + 1)), "past the limit: a loop");
        assertNull(MsgRouter.nextTtl("-1"), "a negative count is no router's");
        assertNull(MsgRouter.nextTtl("-2000000000"), "two billion routings before the limit");
        assertNull(MsgRouter.nextTtl("many"), "not a number");
        assertNull(MsgRouter.nextTtl(""), "empty");

        Node g = new Node(ControllerMode.GLOBAL, Map.of());
        MsgEvent m = ControllerTierRoutingTest.rpc(MsgEvent.Type.EXEC, "netresourceinfo", true, "r1", "a1");
        m.setParam("ttl", "-2000000000");
        g.router.route(m);
        assertTrue(g.sent.isEmpty(), "a message with a negative ttl was run and answered: " + g.sent);
    }
}
