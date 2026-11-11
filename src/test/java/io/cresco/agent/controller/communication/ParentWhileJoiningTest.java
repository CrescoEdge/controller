package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.RegionalRebuildWindowPingTest.Fixture;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.agent.ControllerMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.cresco.agent.controller.communication.BridgedFabric.send;
import static io.cresco.agent.controller.communication.BridgedFabric.stampOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Whose bridge is a region's parent's while its controller state names no global, over real bridges ({@link BridgedFabric})
 * and with the region's real engine deciding ({@code ControllerEngine.parentWhileNotJoined}, {@code ActiveBroker.parentOf}).
 *
 * <p>The bridge to a global is up about a second before the region records the join, and a restarted global's own
 * connector is back at a region before the region has found it again. What the global relayed in those moments -- another
 * region's advert -- lost its caller ({@code a node of region r1 over the bridge of region glob (outside its subtree)};
 * fabric matrix, after a restart of the global: rc3 15:13:14,866, the join recorded at 15:13:15,072). The global a region
 * is joining, or joined last and lost, is its parent now -- on a connection that verified as that controller, and on no
 * other; a join that failed leaves the region joining nobody. (That the state machine tells the engine so:
 * {@code JoiningGlobalIsToldTest}.)
 */
class ParentWhileJoiningTest {

    private final BridgedFabric f = new BridgedFabric();
    private Fixture region;

    @BeforeAll
    static void pki() throws Exception {
        for (String r : new String[]{"rgG", "rgA", "rgB", "rgC"}) BridgedFabric.ca("t1", r);
    }

    @AfterEach
    void stop() {
        f.close();
    }

    /** The global rgG, the region rgB joined to it, and rgA: configured with the global, its state as {@code mode} says. */
    private MessageConsumer fabric(ControllerMode mode) throws Exception {
        f.start("rgG", null);
        f.startUnder("rgB", "rgG", null);
        f.start("rgA", Map.of("global_controller_host", "127.0.0.1"));
        region = new Fixture(mode, Map.of());
        ControllerEngine engine = region.engine;
        f.authz.get("rgA").setParent(() -> ActiveBroker.parentOf(region.state));           // as ActiveBroker wires it
        f.authz.get("rgA").setParentWhileNotJoined(engine::parentWhileNotJoined);
        f.bridge("rgA", "rgG");
        f.bridge("rgB", "rgG");
        Session a = f.node("rgA");
        MessageConsumer listener = a.createConsumer(a.createTopic("t1.global.event"));
        Thread.sleep(3000);
        return listener;
    }

    /** rgB's node publishes an advert; the caller it has when rgA's listener is given it (the global relayed it). */
    private String relayedByTheGlobal(MessageConsumer listener, String text) throws Exception {
        Session b = f.node("rgB");
        send(b, b.createTopic("t1.global.event"), text, stampOf("rgB"));
        Message m = listener.receive(8000);
        assertNotNull(m, "the global did not relay " + text);
        return m.getStringProperty("cresco_caller");
    }

    @Test
    void theGlobalARegionIsJoiningIsItsParentOnItsOwnBridgeAndAFailedJoinEndsIt() throws Exception {
        MessageConsumer listener = fabric(ControllerMode.REGION_GLOBAL_INIT);
        assertNull(ActiveBroker.parentOf(region.state), "the state names no global");
        assertEquals("", relayedByTheGlobal(listener, "not-joining"), "a region that joins nobody took a controller's bridge for its parent's");

        region.engine.joiningGlobal("rgG", "gc");                            // ControllerSMHandler.isRegionGlobal, at its start
        assertEquals(stampOf("rgB"), relayedByTheGlobal(listener, "joining"),
                "what the global relayed between its bridge and the recorded join lost its caller");
        assertEquals(1, f.authz.get("rgA").outOfScopeStampCount(), "only the advert before the join began was stripped");

        region.engine.joinEnded(false);                                      // the attempt failed
        assertEquals("", relayedByTheGlobal(listener, "join-failed"), "a join that failed left the global the region's parent");
        assertTrue(region.engine.knewAGlobal(), "a region that began to join a global has known one");
    }

    @Test
    void theGlobalARegionJoinedAndLostIsStillItsParentAndAnotherJoinedGlobalEndsThat() throws Exception {
        MessageConsumer listener = fabric(ControllerMode.REGION_GLOBAL_FAILED);
        region.engine.joiningGlobal("rgG", "gc");
        region.engine.joinEnded(true);                                       // joined ... and the state lost it again (it restarts)
        assertNull(ActiveBroker.parentOf(region.state));
        assertEquals(stampOf("rgB"), relayedByTheGlobal(listener, "lost"),
                "the global's bridge, back before the region found it again, was judged as a peer region's");

        region.state.globalRegion = "rgX";                                   // the region joined another global meanwhile
        region.state.globalAgent = "gc";
        assertEquals("", relayedByTheGlobal(listener, "moved"), "the global a region left is still its parent");
    }

    @Test
    void joiningAGlobalGrantsNothingToAnotherControllersBridge() throws Exception {
        // rgA is joining rgG; rgC, told to relay, brings rgA a message of rgB's node: over rgC's bridge it is rgC's word
        f.start("rgG", null);
        f.startUnder("rgB", "rgG", null);
        f.startUnder("rgC", "rgG", Map.of(CrescoAuthorizationBroker.TRANSIT_KEY, "true"));
        f.start("rgA", Map.of("global_controller_host", "127.0.0.1"));
        region = new Fixture(ControllerMode.REGION_GLOBAL_INIT, Map.of());
        f.authz.get("rgA").setParent(() -> ActiveBroker.parentOf(region.state));
        f.authz.get("rgA").setParentWhileNotJoined(region.engine::parentWhileNotJoined);
        region.engine.joiningGlobal("rgG", "gc");
        f.bridge("rgB", "rgC");
        f.bridge("rgC", "rgA");
        Session a = f.node("rgA");
        MessageConsumer listener = a.createConsumer(a.createTopic("t1.global.event"));
        Thread.sleep(3000);
        Session b = f.node("rgB");
        send(b, b.createTopic("t1.global.event"), "over-rgC", stampOf("rgB"));
        Message m = listener.receive(8000);
        assertNotNull(m, "rgC did not relay");
        assertEquals("", m.getStringProperty("cresco_caller"), "the bridge of a region that is not the global being joined was taken for the parent's");
    }
}
