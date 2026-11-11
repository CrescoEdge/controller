package io.cresco.agent.controller.agentcontroller;

import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An agent's watchdog (its keep-alive to its region, with its config export) runs on a java.util.Timer. One failed run
 * -- here: the agent database is not there -- used to end that timer for good: the agent sent no watchdog again and
 * its region aged it to LOST while its pings were still answered.
 */
class AgentWatchdogSurvivesFailureTest {

    @Test
    void theWatchdogTimerOutlivesAFailedWatchdogUpdate() throws Exception {
        PluginBuilder pb = TestAgentService.plugin(Files.createTempDirectory("agent-watchdog").toString(), new HashMap<>(), m -> { }, "r1", "a1");
        ControllerEngine engine = new ControllerEngine(null, pb, null, null);
        engine.cstate = TlsTestSupport.state(pb, "r1", "a1");                 // an agent; no database: getGDB() is null
        AgentHealthWatcher watcher = new AgentHealthWatcher(engine);
        Timer timer = new Timer("agent-watchdog-test", true);
        try {
            assertThrows(NullPointerException.class, watcher::sendWatchdogUpdate, "precondition: the update fails");
            timer.schedule(watcher.new CommunicationHealthWatcherTask(), 0, 10);
            Thread.sleep(200);
            CountDownLatch ran = new CountDownLatch(1);
            try {
                timer.schedule(new TimerTask() { public void run() { ran.countDown(); } }, 0);
            } catch (IllegalStateException cancelled) {
                fail("the agent's watchdog timer ended with one failed update: no watchdog is sent again and the region ages"
                        + " the agent to LOST");
            }
            assertTrue(ran.await(5, TimeUnit.SECONDS));
        } finally {
            timer.cancel();
            watcher.shutdown();
        }
    }
}
