package io.cresco.agent.controller.statemachine;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.crypto.CryptoPolicy;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The fabric crypto policy's distribution (CryptoPolicySync): the global builds it from its config, parents stamp
 * it on registration replies and pongs, children adopt it, and nodes below the global ignore local keys.
 * One JVM holds one node's policy, so each step swaps the held policy as the node it plays would hold it.
 */
public class CryptoPolicySyncTest {

    private Path dir;

    @org.junit.jupiter.api.BeforeEach
    void dir() throws Exception {
        dir = java.nio.file.Files.createTempDirectory("cryptopolicy");
    }

    @AfterEach
    void clear() {
        CryptoPolicy.uninstall();
    }

    private static ControllerState inMode(ControllerMode mode) {
        return (ControllerState) Proxy.newProxyInstance(ControllerState.class.getClassLoader(), new Class<?>[]{ControllerState.class},
                (p, m, a) -> "getControllerState".equals(m.getName()) ? mode
                        : m.getReturnType() == boolean.class ? Boolean.FALSE : null);
    }

    private PluginBuilder plugin(Map<String, Object> cfg) {
        return TestAgentService.plugin(dir.toString(), cfg);
    }

    private static MsgEvent ping() {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a2", null, "r1", "a1", null, true, false);
        m.setParam("action", "ping");
        return m;
    }

    @Test
    void authorityModesAreGlobalAndStandaloneOnly() {
        assertTrue(CryptoPolicySync.isAuthority(ControllerMode.GLOBAL));
        assertTrue(CryptoPolicySync.isAuthority(ControllerMode.GLOBAL_INIT));
        assertTrue(CryptoPolicySync.isAuthority(ControllerMode.STANDALONE));
        assertFalse(CryptoPolicySync.isAuthority(ControllerMode.STANDALONE_INIT));   // every node passes through it at start
        assertFalse(CryptoPolicySync.isAuthority(ControllerMode.REGION_GLOBAL));
        assertFalse(CryptoPolicySync.isAuthority(ControllerMode.REGION));
        assertFalse(CryptoPolicySync.isAuthority(ControllerMode.AGENT));
        assertFalse(CryptoPolicySync.isAuthority(null));
    }

    @Test
    void globalConfigReachesAnAgentByRegistrationAndAChangeByPong() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("crypto_allow_hash", "SHA-384,SHA-256");
        PluginBuilder global = plugin(cfg);
        CLogger log = global.getLogger(CryptoPolicySyncTest.class.getName(), CLogger.Level.Info);

        // the global: its config sets the policy
        CryptoPolicy p1 = CryptoPolicySync.becomeAuthority(global, log, "g/gc");
        assertNotNull(p1);
        assertTrue(p1.allows(CryptoPolicy.Family.HASH, "SHA-256"));
        assertEquals(p1, CryptoPolicy.current());

        // the agent_enable reply carries the full policy
        MsgEvent enableReply = new MsgEvent(MsgEvent.Type.CONFIG, "r1", "a1", null, "r1", "rc", null, true, true);
        CryptoPolicySync.stamp(enableReply, null);
        assertEquals(p1.canonical(), enableReply.getParam(CryptoPolicySync.PARAM));

        // the agent (holds nothing yet) adopts it; an authority never does
        CryptoPolicy.uninstall();
        assertFalse(CryptoPolicySync.adopt(enableReply, inMode(ControllerMode.GLOBAL), "region r1", log));
        assertNull(CryptoPolicy.current());
        assertTrue(CryptoPolicySync.adopt(enableReply, inMode(ControllerMode.AGENT), "region r1", log));
        assertEquals(p1, CryptoPolicy.current());
        assertEquals("region r1", CryptoPolicy.currentSource());

        // steady state: the agent's ping names p1, the pong carries only the id
        MsgEvent ping = ping();
        CryptoPolicySync.advertise(ping);
        String childId = ping.getParam(CryptoPolicySync.ID_PARAM);
        assertEquals(p1.id(), childId);
        CryptoPolicySync.stamp(ping, childId);              // the parent still holds p1
        assertEquals(p1.id(), ping.getParam(CryptoPolicySync.ID_PARAM));
        assertNull(ping.getParam(CryptoPolicySync.PARAM));
        assertFalse(CryptoPolicySync.adopt(ping, inMode(ControllerMode.AGENT), "parent", log));

        // the global's config changes (restart): the parent holds p2, the agent's next pong carries it
        CryptoPolicy p2 = CryptoPolicy.DEFAULT;
        MsgEvent ping2 = ping();
        CryptoPolicySync.advertise(ping2);                  // agent still holds p1
        String id2 = ping2.getParam(CryptoPolicySync.ID_PARAM);
        CryptoPolicy.install(p2, "global");                 // ... the parent's view
        CryptoPolicySync.stamp(ping2, id2);
        assertEquals(p2.canonical(), ping2.getParam(CryptoPolicySync.PARAM));
        CryptoPolicy.install(p1, "region r1");              // back to the agent's view
        assertTrue(CryptoPolicySync.adopt(ping2, inMode(ControllerMode.AGENT), "parent", log));
        assertEquals(p2, CryptoPolicy.current());
        assertFalse(CryptoPolicy.current().allows(CryptoPolicy.Family.HASH, "SHA-256"));
    }

    @Test
    void aParentWithoutAPolicyStampsNothingAndTheChildKeepsFailingClosed() {
        MsgEvent reply = ping();
        CryptoPolicySync.stamp(reply, null);
        assertNull(reply.getParam(CryptoPolicySync.PARAM));
        assertNull(reply.getParam(CryptoPolicySync.ID_PARAM));
        CLogger log = plugin(new HashMap<>()).getLogger(CryptoPolicySyncTest.class.getName(), CLogger.Level.Info);
        assertFalse(CryptoPolicySync.adopt(reply, inMode(ControllerMode.AGENT), "parent", log));
        assertThrows(CryptoPolicy.Refused.class,
                () -> CryptoPolicy.requireForNew(CryptoPolicy.Family.HASH, "SHA-384", "a new domain"));
    }

    @Test
    void anUnusablePolicyIsIgnoredAndTheHeldOneKept() {
        CLogger log = plugin(new HashMap<>()).getLogger(CryptoPolicySyncTest.class.getName(), CLogger.Level.Info);
        CryptoPolicy.install(CryptoPolicy.DEFAULT, "region r1");
        MsgEvent reply = ping();
        reply.setParam(CryptoPolicySync.PARAM, CryptoPolicy.DEFAULT.canonical().replace("hash=SHA-384,SHA-512", "hash=MD5"));
        assertFalse(CryptoPolicySync.adopt(reply, inMode(ControllerMode.AGENT), "parent", log));
        assertEquals(CryptoPolicy.DEFAULT, CryptoPolicy.current());
    }

    @Test
    void anInvalidGlobalConfigInstallsNothing() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("crypto_allow_hash", "MD5");
        PluginBuilder global = plugin(cfg);
        CLogger log = global.getLogger(CryptoPolicySyncTest.class.getName(), CLogger.Level.Info);
        CryptoPolicy.install(CryptoPolicy.DEFAULT, "earlier");
        assertNull(CryptoPolicySync.becomeAuthority(global, log, "g/gc"));
        assertNull(CryptoPolicy.current());                 // fail closed, fabric-wide
    }
}
