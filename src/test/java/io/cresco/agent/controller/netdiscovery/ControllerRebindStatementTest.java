package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.RebindStatement;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.spec.ECGenParameterSpec;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P3 of the identity review (2026-11-04): recovering joiners from a re-installed controller (a NEW key, its security
 * directory lost) takes ONE operator action at the controller -- an operator-signed rebind statement it serves in discovery
 * -- not one rebind per joiner. Only joiners that list the operator key accept it, only for the key it names.
 */
class ControllerRebindStatementTest {

    private static final String AGENT_SECRET = "agent-secret-every-agent-has";
    private static final Gson GSON = new Gson();

    private static ControllerEngine node(String region, String agent, boolean regional, String... extra) throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", AGENT_SECRET);
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, m);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    private static DiscoveryNode join(ControllerEngine client, ControllerEngine server) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "client");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }

    private static KeyPair operatorKey() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp384r1"));
        return g.generateKeyPair();
    }

    private static String fp(KeyPair kp) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(kp.getPublic().getEncoded()));
    }

    private static String keyOf(ControllerEngine e) {
        return PeerIdentity.spkiFingerprint(e.getCertificateManager().getPublicCertificate()[0]);
    }

    @Test
    void oneSignedStatementAtTheControllerRecoversItsJoiners() throws Exception {
        KeyPair op = operatorKey();
        ControllerEngine rc = node("rgA", "rc", true);
        ControllerEngine a1 = node("rgA", "a1", false, "controller_rebind_signers", fp(op));
        ControllerEngine a2 = node("rgA", "a2", false);                                  // lists no signer
        assertNotNull(join(a1, rc), "precondition: a1 joins the controller");
        assertNotNull(join(a2, rc), "precondition: a2 joins the controller");
        // the controller is re-installed: same name, new key, nothing restored
        Path dir = Files.createTempDirectory("rebind-rc");
        ControllerEngine reinstalled = node("rgA", "rc", true, "node_identity_dir", dir.toString());
        assertNull(join(a1, reinstalled), "precondition: a joiner refuses the re-keyed controller");
        long now = System.currentTimeMillis();
        Files.writeString(dir.resolve("controller-rebind.json"),
                RebindStatement.sign(op.getPrivate(), op.getPublic(), "rgA/rc", keyOf(reinstalled), now, now + 7L * 24 * 3600 * 1000));
        assertNotNull(join(a1, reinstalled), "the signed statement did not recover a joiner that trusts its signer");
        assertEquals(keyOf(reinstalled), a1.getCertificateManager().getPeerTrust().pinnedKey(new PeerIdentity.Expected("rgA", "rc")));
        assertNull(join(a2, reinstalled), "a joiner that lists no signer followed a statement");
        // the same statement does not help anybody else answer as rgA/rc
        Path other = Files.createTempDirectory("rebind-impostor");
        Files.copy(dir.resolve("controller-rebind.json"), other.resolve("controller-rebind.json"));
        ControllerEngine impostor = node("rgA", "rc", true, "node_identity_dir", other.toString());   // serves the same statement
        assertNull(join(a1, impostor), "a statement for another key let an impostor pose as the controller");
        assertNotNull(join(a1, reinstalled), "the recovered controller is still accepted");
    }

    @Test
    void aStatementIsRefusedUnlessItsSignerIsListedAndItIsValidForThatNameAndKey() throws Exception {
        KeyPair op = operatorKey(), other = operatorKey();
        String key = "ab".repeat(32);
        long now = System.currentTimeMillis(), week = 7L * 24 * 3600 * 1000;
        String good = RebindStatement.sign(op.getPrivate(), op.getPublic(), "rgA/rc", key, now, now + week);
        List<String> signers = List.of(fp(op));
        assertNull(RebindStatement.refusal(good, "rgA", "rc", key, signers, now));
        assertNotNull(RebindStatement.refusal(good, "rgA", "rc", key, List.of(fp(other)), now), "an unlisted signer was accepted");
        assertNotNull(RebindStatement.refusal(good, "rgA", "rc", key, List.of(), now), "no configured signer, yet accepted");
        assertNotNull(RebindStatement.refusal(good, "rgA", "rc2", key, signers, now), "accepted for another name");
        assertNotNull(RebindStatement.refusal(good, "rgA", "rc", "cd".repeat(32), signers, now), "accepted for another key");
        assertNotNull(RebindStatement.refusal(good, "rgA", "rc", key, signers, now + week + 1), "an expired statement was accepted");
        assertNotNull(RebindStatement.refusal(good.replace("\"rgA/rc\"", "\"rgA/rx\""), "rgA", "rx", key, signers, now), "an altered statement verified");
        String forever = RebindStatement.sign(op.getPrivate(), op.getPublic(), "rgA/rc", key, now, now + 365L * 24 * 3600 * 1000);
        assertNotNull(RebindStatement.refusal(forever, "rgA", "rc", key, signers, now), "a statement valid for a year was accepted");
        String forged = RebindStatement.sign(other.getPrivate(), op.getPublic(), "rgA/rc", key, now, now + week);   // claims op's key
        assertNotNull(RebindStatement.refusal(forged, "rgA", "rc", key, signers, now), "a statement not signed by its signer verified");
    }

    @Test
    void aStatementNeverRebindsARevokedController() throws Exception {
        KeyPair op = operatorKey();
        ControllerEngine rc = node("rgA", "rc", true);
        ControllerEngine a1 = node("rgA", "a1", false, "controller_rebind_signers", fp(op));
        assertNotNull(join(a1, rc));
        assertNull(a1.getCertificateManager().getPeerTrust().revoke("rgA/rc", "operator: controller compromised"));
        Path dir = Files.createTempDirectory("rebind-rc2");
        ControllerEngine reinstalled = node("rgA", "rc", true, "node_identity_dir", dir.toString());
        long now = System.currentTimeMillis();
        Files.writeString(dir.resolve("controller-rebind.json"),
                RebindStatement.sign(op.getPrivate(), op.getPublic(), "rgA/rc", keyOf(reinstalled), now, now + 24L * 3600 * 1000));
        assertNull(join(a1, reinstalled), "a statement rebound a REVOKED controller name");
    }
}
