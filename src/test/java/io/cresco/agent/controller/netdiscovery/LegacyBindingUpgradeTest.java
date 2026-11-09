package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The upgrade window: an earlier 1.3 build keeps no identity key across restarts, so the upgraded build of the same agent
 * presents a new key. The binding the earlier build made over the legacy exchange is replaced, once, by the key the
 * upgraded node proves over the authenticated exchange -- only inside the window, only for a binding the legacy exchange
 * made; a strict (authenticated) binding is never taken over, and a legacy exchange never replaces anything.
 */
class LegacyBindingUpgradeTest {

    private static final String SECRET = "test-secret-not-real";
    private static final String WINDOW_END = java.time.LocalDate.now(java.time.ZoneOffset.UTC).plusDays(7).toString();

    private static Map<String, Object> cfg(boolean window) {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        if (window) {
            m.put(DiscoveryAuth.MODE_KEY, "compat");
            m.put("discovery_compat_until", WINDOW_END);
        }
        return m;
    }

    private static ControllerEngine responder(boolean window) throws Exception {
        ControllerEngine e = TlsTestSupport.engine("rgG", "gc", cfg(window));
        ((TlsTestSupport.State) e.cstate).regional = true;
        return e;
    }

    private static PeerTrustRegistry reg(ControllerEngine e) {
        return e.getCertificateManager().getPeerTrust();
    }

    /** An earlier build's single-message certify of {@code agent} at {@code me}: certified? */
    private static boolean legacyCertify(ControllerEngine me, ControllerEngine agent, String name) throws Exception {
        DiscoveryCrypto legacyCrypto = new DiscoveryCrypto(TlsTestSupport.engine("rgX", "x", cfg(true)));
        DiscoveryNode n = new DiscoveryNode(DiscoveryType.AGENT);
        n.setCertify(legacyCrypto.encryptLegacy("DISCOVERY_MESSAGE_VERIFIED", SECRET),
                agent.getCertificateManager().getJsonFromCerts(agent.getCertificateManager().getPublicCertificate()), "rgG", name);
        DiscoveryNode out = new DiscoveryProcessor(me).processIncomingBroadCastDiscovery(n, "127.0.0.1", 32005, "127.0.0.1", 40002);
        return out != null && out.nodeType == DiscoveryNode.NodeType.CERTIFIED;
    }

    @Test
    void insideTheWindowTheUpgradedNodeReplacesItsLegacyBindingOnce() throws Exception {
        ControllerEngine me = responder(true);
        ControllerEngine earlier = TlsTestSupport.engine("rgG", "a7", cfg(false));
        assertTrue(legacyCertify(me, earlier, "a7"), "the earlier build's legacy certify inside the window");
        assertTrue(reg(me).isLegacyBinding("rgG", "a7"), "the legacy exchange's binding is not marked legacy");

        ControllerEngine upgraded = TlsTestSupport.engine("rgG", "a7", cfg(false));      // a new key, the same name
        assertNotNull(DiscoveryTestHooks.join(upgraded, me), "the upgraded node was refused its own name");
        assertFalse(reg(me).isLegacyBinding("rgG", "a7"), "the authenticated binding is still marked legacy");
        java.security.PublicKey oldKey = earlier.getCertificateManager().getPublicCertificate()[0].getPublicKey();
        java.security.PublicKey newKey = upgraded.getCertificateManager().getPublicCertificate()[0].getPublicKey();
        assertTrue(reg(me).canBind("t1", "rgG", "a7", newKey).ok);
        assertFalse(reg(me).canBind("t1", "rgG", "a7", oldKey).ok, "the earlier build's key is still accepted for the name");

        // once: a third key for the name is refused like any taken name (the binding is strict now)
        ControllerEngine third = TlsTestSupport.engine("rgG", "a7", cfg(false));
        assertNull(DiscoveryTestHooks.join(third, me), "a strict binding was taken over");
        // and the earlier build cannot come back over the legacy exchange
        assertFalse(legacyCertify(me, earlier, "a7"), "the replaced legacy key was certified again");
    }

    @Test
    void outsideTheWindowALegacyBindingIsAnyOtherBinding() throws Exception {
        ControllerEngine me = responder(false);
        ControllerEngine earlier = TlsTestSupport.engine("rgG", "a7", cfg(false));
        assertNull(reg(me).pin("rgG", "a7", earlier.getCertificateManager().getPublicCertificate()[0],
                PeerTrustRegistry.ROLE_AGENT, false, PeerTrustRegistry.SOURCE_LEGACY));
        assertTrue(reg(me).isLegacyBinding("rgG", "a7"));
        ControllerEngine upgraded = TlsTestSupport.engine("rgG", "a7", cfg(false));
        assertNull(DiscoveryTestHooks.join(upgraded, me), "a legacy binding was replaced outside the window");
        assertTrue(reg(me).isLegacyBinding("rgG", "a7"));
    }

    @Test
    void anAuthenticatedBindingIsNeverTakenOverInsideTheWindow() throws Exception {
        ControllerEngine me = responder(true);
        ControllerEngine first = TlsTestSupport.engine("rgG", "a8", cfg(false));
        assertNotNull(DiscoveryTestHooks.join(first, me));
        assertFalse(reg(me).isLegacyBinding("rgG", "a8"));
        ControllerEngine other = TlsTestSupport.engine("rgG", "a8", cfg(false));
        assertNull(DiscoveryTestHooks.join(other, me), "a v2-bound agent name was taken by another key inside the window");
        assertFalse(legacyCertify(me, other, "a8"), "a legacy exchange replaced an authenticated binding");
        // upgradeLegacy itself refuses anything that is not a legacy agent binding
        assertFalse(reg(me).upgradeLegacy("t1", "rgG", "a8", other.getCertificateManager().getPublicCertificate()[0].getPublicKey()).ok);
        assertFalse(reg(me).upgradeLegacy("t1", "rgG", "gc", other.getCertificateManager().getPublicCertificate()[0].getPublicKey()).ok,
                "the controller's own name");
    }

    @Test
    void aResponderAnswersALocatorInTheRequestersFormat() throws Exception {
        // inside the window, an upgraded agent WITHOUT the window settings still finds its controller (v2 asked, v2 answered),
        // and an earlier build is answered in its own format
        ControllerEngine me = responder(true);
        DiscoveryProcessor rp = new DiscoveryProcessor(me);
        ControllerEngine upgraded = TlsTestSupport.engine("rgG", "a5", cfg(false));
        DiscoveryProcessor up = new DiscoveryProcessor(upgraded);
        DiscoveryNode v2 = up.generateBroadCastDiscovery(DiscoveryType.AGENT, false);
        assertFalse(DiscoveryCrypto.isLegacyFormat(v2.broadcast_validator), "an upgraded node sent a legacy locator");
        DiscoveryNode a = rp.processIncomingBroadCastDiscovery(v2, "127.0.0.1", 32005, "127.0.0.1", 40003);
        assertNotNull(a);
        assertFalse(DiscoveryCrypto.isLegacyFormat(a.discovered_validator), "a v2 locator was answered in the legacy format");
        assertTrue(up.isValidatedAuthenication(a), "the upgraded agent without the window could not read its controller's answer");

        ControllerEngine windowed = TlsTestSupport.engine("rgG", "a6", cfg(true));
        DiscoveryNode legacy = new DiscoveryProcessor(windowed).generateBroadCastDiscovery(DiscoveryType.AGENT, false, true);
        assertTrue(DiscoveryCrypto.isLegacyFormat(legacy.broadcast_validator));
        DiscoveryNode b = rp.processIncomingBroadCastDiscovery(legacy, "127.0.0.1", 32005, "127.0.0.1", 40004);
        assertNotNull(b);
        assertTrue(DiscoveryCrypto.isLegacyFormat(b.discovered_validator), "an earlier build's locator was not answered in its format");

        // outside the window a legacy locator is neither produced nor answered
        assertNull(up.generateBroadCastDiscovery(DiscoveryType.AGENT, false, true));
        DiscoveryNode legacy2 = new DiscoveryProcessor(windowed).generateBroadCastDiscovery(DiscoveryType.AGENT, false, true);
        DiscoveryNode c = new DiscoveryProcessor(responder(false)).processIncomingBroadCastDiscovery(legacy2, "127.0.0.1", 32005, "127.0.0.1", 40005);
        assertTrue(c == null || c.nodeType != DiscoveryNode.NodeType.DISCOVERED, "a legacy locator was answered outside the window");
    }
}
