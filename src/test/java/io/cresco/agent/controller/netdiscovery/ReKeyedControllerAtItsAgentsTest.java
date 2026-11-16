package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The operator's re-key of a region controller, at its agents, over the exchange with proof of possession (the fabric
 * matrix's rekey-region event): an agent refuses the controller's new key and says exactly what to do -- the line and the
 * file -- and doing exactly that, nothing else, makes it take the new key; a key nobody authorised is still refused.
 * The text is read as the matrix reads it (its pattern, docs/tests/fabric_matrix.py ev_rekey_region).
 */
class ReKeyedControllerAtItsAgentsTest {

    private static final String SECRET = "test-secret-not-real";
    /** As ev_rekey_region reads a refusal: the line to add and the file to add it to. */
    private static final String ASK = "authorise ONE rebind on this node: add the line '(%s=%s)' \\(its new key\\) to (\\S+) \\(no restart\\)";

    private static ControllerEngine node(String region, String agent, boolean controller, Path dir) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", SECRET);
        m.put("discovery_secret_region", SECRET);
        m.put("discovery_secret_global", SECRET);
        if (dir != null) {
            m.put("node_identity_dir", dir.resolve("security").toString());
            m.put("peer_trust_file", dir.resolve("security").resolve("peer-trust.json").toString());
        }
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        ((TlsTestSupport.State) e.cstate).regional = controller;
        return e;
    }

    private static String key(ControllerEngine e) { return PeerIdentity.spkiFingerprint(e.getCertificateManager().getPublicCertificate()[0]); }

    private static PeerTrustRegistry reg(ControllerEngine e) { return e.getCertificateManager().getPeerTrust(); }

    /** What the agent says when {@code rc} answers its exchange, with proof (nothing is recorded by asking). */
    private static PeerTrustRegistry.BindResult judged(ControllerEngine agent, ControllerEngine rc) {
        PeerTrustRegistry.Join j = new PeerTrustRegistry.Join();
        j.region = rc.cstate.getRegion(); j.agent = rc.cstate.getAgent(); j.chain = rc.getCertificateManager().getPublicCertificate();
        j.role = PeerTrustRegistry.ROLE_CONTROLLER; j.proved = true; j.client = true; j.exchange = "AGENT"; j.record = false;
        return reg(agent).join(j);
    }

    @Test
    void theAgentTakesTheNewKeyOnTheLineItsRefusalNamesAndNoOtherKey() throws Exception {
        Path adir = Files.createTempDirectory("rekey-agent");
        ControllerEngine rc = node("rgA", "rc", true, null), a1 = node("rgA", "a1", false, adir);
        assertNotNull(DiscoveryTestHooks.join(a1, rc), "precondition: the agent joins its controller");
        assertTrue(reg(a1).provedKey("rgA", "rc"), "precondition: over the exchange with proof");

        ControllerEngine reKeyed = node("rgA", "rc", true, null);           // stopped, identity removed, started: a new key
        assertNull(DiscoveryTestHooks.join(a1, reKeyed), "an agent took its controller's new key without the operator");
        PeerTrustRegistry.BindResult refused = judged(a1, reKeyed);
        assertFalse(refused.ok);
        Matcher m = Pattern.compile(String.format(ASK, Pattern.quote("rgA/rc"), key(reKeyed))).matcher(refused.reason);
        assertTrue(m.find(), "the refusal does not say which line to add where, as the matrix reads it: " + refused.reason);
        Path named = Path.of(m.group(2));
        assertEquals(adir.resolve("security").resolve("peer-trust.json.rebind").toAbsolutePath().normalize(), named.toAbsolutePath().normalize(),
                "the refusal names a file the agent does not read");

        Files.writeString(named, m.group(1) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        assertNotNull(DiscoveryTestHooks.join(a1, reKeyed), "the agent refused its re-keyed controller after the line its refusal asked for");
        assertEquals(key(reKeyed), reg(a1).pinnedKey(new PeerIdentity.Expected("rgA", "rc")));

        // the line is spent: a key nobody authorised is refused, and so is the old one
        assertNull(DiscoveryTestHooks.join(a1, node("rgA", "rc", true, null)), "an unauthorised second key was taken after the rebind");
        assertNull(DiscoveryTestHooks.join(a1, rc), "the replaced key was taken back");
    }
}
