package io.cresco.agent.core;

import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

import java.util.Optional;

/**
 * Who asked, in-JVM (review F5). A per-bundle AgentService knows the bundle on whose BundleContext it was obtained -- and
 * any bundle can borrow another's context with plain OSGi API. For the strongest capability a bundle holds (sending on
 * behalf of an API user, {@code on_behalf_bundles}) the controller also checks the code that asked: the first frame below
 * the library, the controller and the JDK must belong to that very bundle, so borrowing a context is not enough by itself.
 *
 * <p>This is a structural mitigation, not a sandbox: Java 21 has no SecurityManager, and in-process code can reach any
 * object it can name (reflection, or code of the borrowed bundle itself). In-JVM plugins are trusted code; only signed
 * plugins are installed (docs/caller-authorization.md, "The in-JVM trust boundary").
 */
final class CallerFrames {

    private CallerFrames() { }

    private static final StackWalker WALKER = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

    private static final ClassValue<Long> BUNDLE_OF = new ClassValue<Long>() {
        @Override
        protected Long computeValue(Class<?> c) {
            try {
                Bundle b = FrameworkUtil.getBundle(c);
                return b == null ? -1L : b.getBundleId();
            } catch (RuntimeException e) {
                return -1L;
            }
        }
    };

    /** Frames that pass a request along rather than make it: the JDK, the OSGi framework, the Cresco library and controller. */
    static boolean passesAlong(Class<?> c) {
        String n = c.getName();
        return n.startsWith("java.") || n.startsWith("javax.") || n.startsWith("jdk.") || n.startsWith("sun.") || n.startsWith("com.sun.")
                || n.startsWith("io.cresco.library.") || n.startsWith("io.cresco.agent.") || n.startsWith("org.apache.felix.")
                || n.startsWith("org.osgi.");
    }

    /** The class whose code asked (the first frame that does not pass a request along), if any. */
    static Optional<Class<?>> asker() {
        return WALKER.walk(frames -> frames.map(StackWalker.StackFrame::getDeclaringClass).filter(c -> !passesAlong(c)).findFirst());
    }

    /** Null when the code that asked belongs to {@code bound}; else why not. */
    static String refusal(Bundle bound) {
        if (bound == null) return "no bundle";
        Optional<Class<?>> asker = asker();
        if (!asker.isPresent()) return "no code outside the Cresco library and controller asked for it";
        long id = BUNDLE_OF.get(asker.get());
        if (id == bound.getBundleId()) return null;
        return "the AgentService of " + bound.getSymbolicName() + "#" + bound.getBundleId() + " was used by code of "
                + (id < 0 ? "no bundle" : "bundle #" + id) + " (" + asker.get().getName() + ")";
    }
}
