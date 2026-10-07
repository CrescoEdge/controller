package io.cresco.agent.core;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** controller#23: a thread killed by a fatal JVM error halts the agent instead of leaving a zombie. */
class FatalErrorGuardTest {

    @Test
    void aFatalErrorIsFoundAnywhereInTheCauseChain() {
        assertTrue(FatalErrorGuard.fatal(new OutOfMemoryError("Java heap space")));
        assertTrue(FatalErrorGuard.fatal(new ExceptionInInitializerError(new OutOfMemoryError("Java heap space"))));
        assertTrue(FatalErrorGuard.fatal(new RuntimeException(new RuntimeException(new StackOverflowError()))));
        assertFalse(FatalErrorGuard.fatal(new RuntimeException("ordinary")));
        assertFalse(FatalErrorGuard.fatal(null));
    }

    @Test
    void aThreadDyingOfAnOutOfMemoryErrorHaltsTheJvm() throws Exception {
        assertEquals(FatalErrorGuard.EXIT_STATUS, child("oom"), "the JVM halted with the guard's status");
        assertEquals(0, child("ordinary"), "an ordinary uncaught exception leaves the JVM running to a normal exit");
    }

    private static int child(String mode) throws Exception {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        Process p = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), Child.class.getName(), mode)
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "the child exits");
        return p.exitValue();
    }

    /** The child JVM: installs the guard, lets one thread die, then exits normally if still running. */
    public static final class Child {
        public static void main(String[] a) throws Exception {
            FatalErrorGuard.install(null);
            Thread t = new Thread(() -> {
                if ("oom".equals(a[0])) throw new ExceptionInInitializerError(new OutOfMemoryError("Java heap space"));
                throw new IllegalStateException("ordinary");
            }, "ActiveMQ Task-3");
            t.start();
            t.join();
            Thread.sleep(200);
            System.exit(0);
        }
    }
}
