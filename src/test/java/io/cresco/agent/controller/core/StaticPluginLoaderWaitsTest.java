package io.cresco.agent.controller.core;

import io.cresco.library.agent.ControllerMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The static plugin loader waits for the controller to become active through failed joins: a FAILED state is a join that
 * is tried again. It used to give up on the first one, so a node that joined on its second try ran without its system and
 * persisted plugins until its JVM was restarted. Only a shutdown ends the wait.
 */
class StaticPluginLoaderWaitsTest {

    @Test
    void aFailedJoinIsWaitedOutAndOnlyAShutdownEndsTheWait() {
        for (ControllerMode mode : ControllerMode.values()) {
            boolean shuttingDown = mode.name().endsWith("_SHUTDOWN");
            assertEquals(shuttingDown, StaticPluginLoader.givesUp(mode), mode.name());
        }
        assertFalse(StaticPluginLoader.givesUp(ControllerMode.AGENT_FAILED));
        assertFalse(StaticPluginLoader.givesUp(ControllerMode.AGENT_INIT));
        assertTrue(StaticPluginLoader.givesUp(ControllerMode.AGENT_SHUTDOWN));
        assertFalse(StaticPluginLoader.givesUp(null));
    }
}
