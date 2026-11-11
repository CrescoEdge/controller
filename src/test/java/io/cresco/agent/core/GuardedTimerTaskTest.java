package io.cresco.agent.core;

import org.junit.jupiter.api.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A Throwable that leaves a TimerTask ends its java.util.Timer for good, silently. Every periodic task of the
 * controller runs behind one guard: the failure is logged in one line and the task keeps its schedule.
 */
class GuardedTimerTaskTest {

    /** The controller's periodic timer tasks (those of the files this fix covers). */
    private static final String[] PERIODIC = {
            "io.cresco.agent.controller.communication.ActiveBrokerManager$BrokerWatchDog",
            "io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher$GlobalNodeStatusWatchDog",
            "io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher$CommunicationHealthWatcherTask",
            "io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher$RegionalNodeStatusWatchDog",
            "io.cresco.agent.controller.regionalcontroller.RegionHealthWatcher$ActivePingTask",
            "io.cresco.agent.controller.agentcontroller.AgentHealthWatcher$CommunicationHealthWatcherTask",
            "io.cresco.agent.controller.agentcontroller.AgentHealthWatcher$ActivePingTask",
            "io.cresco.agent.controller.statemachine.ControllerSMHandler$stateUpdateTask",
    };

    @Test
    void everyPeriodicTaskRunsBehindTheGuard() throws Exception {
        StringBuilder bare = new StringBuilder();
        for (String name : PERIODIC) {
            Class<?> c = Class.forName(name, false, GuardedTimerTaskTest.class.getClassLoader());
            assertTrue(TimerTask.class.isAssignableFrom(c), name + " is no timer task");
            if (!GuardedTimerTask.class.isAssignableFrom(c)) bare.append("\n  ").append(name);
        }
        assertEquals("", bare.toString(), "a Throwable leaving one of these ends its timer for good:");
    }

    /** Whether {@code timer} still runs tasks. */
    private static boolean alive(Timer timer) throws InterruptedException {
        CountDownLatch ran = new CountDownLatch(1);
        try {
            timer.schedule(new TimerTask() { public void run() { ran.countDown(); } }, 0);
        } catch (IllegalStateException cancelled) {
            return false;
        }
        return ran.await(5, TimeUnit.SECONDS);
    }

    @Test
    void aBareTaskThatThrowsEndsItsTimer() throws Exception {
        Timer timer = new Timer("bare", true);
        CountDownLatch thrown = new CountDownLatch(1);
        timer.schedule(new TimerTask() {
            public void run() { thrown.countDown(); throw new IllegalStateException("one failed read"); }
        }, 0, 10);
        assertTrue(thrown.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertFalse(alive(timer), "the premise of this fix: java.util.Timer does not survive a task's exception");
    }

    @Test
    void aGuardedTaskThatThrowsKeepsItsSchedule() throws Exception {
        Timer timer = new Timer("guarded", true);
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch five = new CountDownLatch(5);
        timer.schedule(new GuardedTimerTask(null) {
            @Override protected void tick() {
                five.countDown();
                switch (runs.incrementAndGet()) {
                    case 1: throw new IllegalStateException("one failed read");
                    case 2: throw new NoClassDefFoundError("an Error, not an Exception");
                    default:
                }
            }
        }, 0, 10);
        assertTrue(five.await(5, TimeUnit.SECONDS), "the task stopped after " + runs.get() + " run(s)");
        assertTrue(alive(timer), "the timer ended with the task's failure");
        timer.cancel();
    }

    @Test
    void aFatalJvmErrorIsPassedOnSoTheJvmIsHalted() {
        assertThrows(OutOfMemoryError.class, () -> GuardedTimerTask.guarded("t", null, () -> { throw new OutOfMemoryError("test"); }));
        assertThrows(IllegalStateException.class, () -> GuardedTimerTask.guarded("t", null,
                () -> { throw new IllegalStateException(new OutOfMemoryError("wrapped")); }));
        assertFalse(GuardedTimerTask.guarded("t", null, () -> { throw new IllegalArgumentException("not fatal"); }));
        assertTrue(GuardedTimerTask.guarded("t", null, () -> { }));
    }
}
