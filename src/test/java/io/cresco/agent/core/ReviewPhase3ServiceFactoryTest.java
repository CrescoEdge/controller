package io.cresco.agent.core;

import io.cresco.library.agent.AgentService;
import org.apache.felix.framework.Felix;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: the per-bundle AgentService. A failure is a
 * finding. Review-only: never pushed.
 *
 * <p>Contract (docs/caller-authorization.md section 8): "Each bundle gets its own AgentService (a service factory), so the
 * controller knows the calling bundle. A message a plugin sends goes out as plugin:... only for a plugin id that bundle
 * runs". The factory decides by the bundle on whose BundleContext getService is called -- and any bundle can get any
 * other bundle's BundleContext with plain OSGi API ({@code Bundle.getBundleContext()}; no permission check runs without a
 * SecurityManager), e.g. the controller bundle's (its packages are exported, so {@code FrameworkUtil.getBundle(
 * AgentServiceImpl.class)} finds it too). Through it a plugin gets the controller's own AgentServiceImpl: msgOut stamped
 * {@code controller:...} ({@code infra}), the raw dataplane (no StdinGuard, controller writer). Through wsapi's context it gets
 * the AgentService bound to io.cresco.wsapi, which passes the on_behalf_bundles check: any API user of the node's tenant, as
 * admin (any tenant's on a superuser-tenant node).
 *
 * <p>A real Felix framework, the real factory (AgentServiceImpl.registerAgentServiceFactory).
 */
class ReviewPhase3ServiceFactoryTest {

    private Path dir;
    private Felix felix;
    private Bundle controller, wsapi, evil;
    private AgentServiceImpl core;

    private static InputStream bundleJar(String bsn) throws Exception {
        Manifest mf = new Manifest();
        Attributes a = mf.getMainAttributes();
        a.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        a.putValue("Bundle-ManifestVersion", "2");
        a.putValue("Bundle-SymbolicName", bsn);
        a.putValue("Bundle-Version", "1.0.0");
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        try (JarOutputStream jo = new JarOutputStream(bo, mf)) {
            jo.flush();
        }
        return new ByteArrayInputStream(bo.toByteArray());
    }

    @BeforeEach
    void start() throws Exception {
        dir = Files.createTempDirectory("rvp3-felix");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(Constants.FRAMEWORK_STORAGE, dir.toString());
        cfg.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        felix = new Felix(cfg);
        felix.init();
        felix.start();
        BundleContext sys = felix.getBundleContext();
        controller = sys.installBundle("test:controller", bundleJar("io.cresco.controller"));
        wsapi = sys.installBundle("test:wsapi", bundleJar("io.cresco.wsapi"));
        evil = sys.installBundle("test:evil", bundleJar("some.signed.plugin"));
        for (Bundle b : new Bundle[]{controller, wsapi, evil}) b.start();

        core = new AgentServiceImpl();
        Method reg = AgentServiceImpl.class.getDeclaredMethod("registerAgentServiceFactory", BundleContext.class);
        reg.setAccessible(true);                                     // test wiring only: what activate() does
        reg.invoke(core, controller.getBundleContext());
    }

    @AfterEach
    void stop() throws Exception {
        if (felix != null) {
            felix.stop();
            felix.waitForStop(10_000);
        }
        try (Stream<Path> w = Files.walk(dir)) {
            w.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static Object agentServiceVia(BundleContext ctx) {
        ServiceReference<?> ref = ctx.getServiceReference(AgentService.class.getName());
        assertNotNull(ref, "test premise: the AgentService factory is registered");
        return ctx.getService(ref);
    }

    @Test
    void aBundleGetsItsOwnAgentService() {
        Object own = agentServiceVia(evil.getBundleContext());
        assertTrue(own instanceof BundleAgentService, "premise: " + own);
        assertEquals(evil, ((BundleAgentService) own).bundle());
    }

    @Test
    void aPluginCannotObtainTheControllersOwnAgentService() {
        BundleContext evilCtx = evil.getBundleContext();
        // plain OSGi API, no reflection: the controller bundle, then its context
        BundleContext stolen = evilCtx.getBundle(controller.getBundleId()).getBundleContext();
        Object as = agentServiceVia(stolen);
        assertNotSame(core, as, "bundle 'some.signed.plugin' obtained the controller's own AgentServiceImpl through"
                + " Bundle.getBundleContext() of the controller bundle: its msgOut is stamped controller:<node> (infra)");
    }

    /*
     * (fix commit, review F5) The review's third test asserted that a bundle cannot obtain the AgentService bound to another
     * bundle by borrowing that bundle's BundleContext. No OSGi service factory can achieve that: the framework calls the
     * factory with the bundle whose context was used, caches one instance per bundle, and Java 21 has no SecurityManager to
     * stop Bundle.getBundleContext(). It is replaced by a test of the documented in-JVM trust boundary
     * (docs/caller-authorization.md, "The in-JVM trust boundary") and of the mitigations that do hold.
     */
    @Test
    void borrowingAnotherBundlesContextIsTheDocumentedBoundaryAndTheCapabilityStillChecksWhoAsks() {
        Object as = agentServiceVia(evil.getBundleContext().getBundle(wsapi.getBundleId()).getBundleContext());
        assertTrue(as instanceof BundleAgentService && wsapi.equals(((BundleAgentService) as).bundle()),
                "the documented boundary: in-process code can borrow a context (it is trusted code; only signed plugins are installed)");
        // mitigation 1: a capability bound to a symbolic name holds only for a bundle the controller installed itself; this
        // io.cresco.wsapi was installed by other code (here the test, in production a plugin through its own context)
        assertFalse(core.trustedNamedBundle(wsapi), "a bundle the controller did not install holds no named capability");
        // mitigation 2: a send on behalf of a user through wsapi's AgentService must be made by wsapi's own code
        assertNotNull(CallerFrames.refusal(wsapi), "code outside wsapi used wsapi's AgentService to send on behalf of a user");
        SecurityException refused = assertThrows(SecurityException.class, () -> ((BundleAgentService) as).msgOutOnBehalfOf("p1",
                new io.cresco.library.messaging.MsgEvent(), io.cresco.library.security.Caller.api("t1", "alice", java.util.List.of("admin"))));
        assertNotNull(refused.getMessage());
    }

    @Test
    void theControllerBundleGetsNoControllerIdentity() {
        Object as = agentServiceVia(controller.getBundleContext());
        assertTrue(as instanceof BundleAgentService, "the controller bundle's registry instance is an ordinary per-bundle one");
        assertEquals(controller, ((BundleAgentService) as).bundle());
    }
}
