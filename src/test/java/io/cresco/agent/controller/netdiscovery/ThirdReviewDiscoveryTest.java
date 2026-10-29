package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.communication.PeerIdentity;
import io.cresco.agent.controller.communication.PeerTrustRegistry;
import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.agent.controller.communication.TlsTestSupport;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Third adversarial review (discovery). Each test asserts the SECURE outcome; a failure is a finding. */
class ThirdReviewDiscoveryTest {

    private static final Gson GSON = new Gson();

    private static Map<String, Object> cfg(String... extra) {
        Map<String, Object> m = new HashMap<>();
        m.put("discovery_secret_agent", "agent-secret");
        m.put("discovery_secret_region", "region-secret");
        m.put("discovery_secret_global", "global-secret");
        for (int i = 0; i + 1 < extra.length; i += 2) m.put(extra[i], extra[i + 1]);
        return m;
    }

    private static ControllerEngine controller(String region, String agent, String... extra) throws Exception {
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg(extra));
        ((TlsTestSupport.State) e.cstate).regional = true;
        return e;
    }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) {
        return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class);
    }

    private static X509Certificate leafOf(ControllerEngine e, String json) {
        X509Certificate[] c = e.getCertificateManager().getCertsfromJson(json);
        return c == null ? null : c[0];
    }

    /**
     * Regional CA. A holder of the GLOBAL discovery secret (any regional controller) answers a victim regional controller's
     * GLOBAL exchange claiming the victim's OWN region and returns a "CA chain" for (victim region, tenant cresco-system).
     * installEnrollment records it with follow=true BEFORE learnPeer refuses the own-region claim, and adds it to the
     * truststore: the attacker can then mint a superuser-tenant identity the victim's broker verifies.
     */
    @Test
    void aFederationAnswerCannotReplaceTheVictimsOwnRegionCaOrMintSuperusers() throws Exception {
        ControllerEngine victim = controller("rgA", "rc", "security_regional_ca", "true");
        victim.getCertificateManager().ensureRegionCA();
        ControllerEngine attacker = controller("rgA", "evil");                     // holds the global secret, per-node chain
        DiscoveryProcessor c = new DiscoveryProcessor(victim), s = new DiscoveryProcessor(attacker);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "victim");          // sreg=rgA sagt=evil, MAC'd with the global secret
        assertNotNull(c.clientProof(hello, copy(ch), "attacker"));
        RegionCA evilCa = RegionCA.generate("cresco-system", "rgA", 2048, 1);
        DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
        r.step = "result"; r.type = hello.type; r.ok = true;
        r.rcert = victim.getCertificateManager().getJsonFromCerts(evilCa.caChain());
        byte[] k = DiscoveryAuth.key("global-secret", "handshake|" + hello.type);
        r.mac = DiscoveryAuth.mac(k, "S2", DiscoveryAuth.transcript(hello, ch, leafOf(victim, hello.ccert), leafOf(victim, ch.scert)),
                DiscoveryAuth.resultDigest(r));
        DiscoveryNode n = c.clientResult(hello, copy(ch), copy(r), "10.6.6.6", 32005);
        System.out.println("clientResult returned " + (n == null ? "null (refused by learnPeer)" : "a node"));

        TlsTestSupport.Id minted = TlsTestSupport.issued(evilCa, "cresco-system", "rgA", "admin");
        PeerTrustRegistry reg = victim.getCertificateManager().getPeerTrust();
        String[] why = new String[1];
        CrescoIdentity asSeen = reg.verifiedIdentity(minted.chain, why);
        boolean pkix;
        try { victim.getCertificateManager().currentPkixTrustManager().checkClientTrusted(minted.chain, "RSA"); pkix = true; }
        catch (Exception e) { pkix = false; }
        System.out.println("attacker-minted leaf: PKIX-trusted by the victim's broker=" + pkix + "; verifiedIdentity=" + asSeen
                + (asSeen == null ? " (" + why[0] + ")" : "") + "; broker_superuser_tenants default = cresco-system");
        assertFalse(pkix && asSeen != null, "a GLOBAL-secret holder made the victim trust its CA for the victim's own region: "
                + "it now mints identities (incl. the superuser tenant) the victim's broker verifies");
    }

    /**
     * Same, any other region: a regional-CA controller CLIENT records whatever CA the responder returns, for the region the
     * responder names, with any tenant -- including a superuser tenant. Not limited to the controller it meant to join.
     */
    @Test
    void aFederatedCaCannotCarryASuperuserTenant() throws Exception {
        ControllerEngine victim = controller("rgA", "rc", "security_regional_ca", "true");
        victim.getCertificateManager().ensureRegionCA();
        ControllerEngine attacker = controller("rgZ", "gz");
        DiscoveryProcessor c = new DiscoveryProcessor(victim), s = new DiscoveryProcessor(attacker);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "victim");
        assertNotNull(c.clientProof(hello, copy(ch), "attacker"));
        RegionCA evilCa = RegionCA.generate("cresco-system", "rgZ", 2048, 1);
        DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
        r.step = "result"; r.type = hello.type; r.ok = true;
        r.rcert = victim.getCertificateManager().getJsonFromCerts(evilCa.caChain());
        byte[] k = DiscoveryAuth.key("global-secret", "handshake|" + hello.type);
        r.mac = DiscoveryAuth.mac(k, "S2", DiscoveryAuth.transcript(hello, ch, leafOf(victim, hello.ccert), leafOf(victim, ch.scert)),
                DiscoveryAuth.resultDigest(r));
        DiscoveryNode n = c.clientResult(hello, copy(ch), copy(r), "10.6.6.6", 32005);
        TlsTestSupport.Id minted = TlsTestSupport.issued(evilCa, "cresco-system", "rgZ", "anyone");
        CrescoIdentity asSeen = victim.getCertificateManager().getPeerTrust().verifiedIdentity(minted.chain);
        System.out.println("federation accepted=" + (n != null) + "; minted cresco-system/rgZ/anyone verifies as " + asSeen);
        assertFalse(asSeen != null && "cresco-system".equals(asSeen.getTenant()),
                "a federation answer made the victim accept a CA for the superuser tenant: every name it mints is SUPERUSER at the victim's broker");
    }

    /**
     * Per-node trust. A controller V learned rgB/rcB STRICTLY when rgB joined it (REGION responder). Later V is a CLIENT of a
     * GLOBAL exchange answered by a global-secret holder claiming to be rgB/rcB with its own key: the client "follow"
     * overwrites the strict binding, and the impostor becomes a verified identity and bridge peer for rgB/rcB at V.
     */
    @Test
    void aClientExchangeCannotOverwriteAStrictlyLearnedControllerBinding() throws Exception {
        ControllerEngine victim = controller("rgA", "rc");
        ControllerEngine rgB = controller("rgB", "rcB");
        // rgB joins the victim (victim is the responder): strict, first come
        DiscoveryProcessor cb = new DiscoveryProcessor(rgB), sv = new DiscoveryProcessor(victim);
        DiscoveryAuth.Msg h1 = cb.clientHello(DiscoveryType.REGION);
        DiscoveryAuth.Msg c1 = sv.serverChallenge(copy(h1), "rgB");
        DiscoveryAuth.Msg r1 = sv.serverResult(copy(h1), c1, copy(cb.clientProof(h1, copy(c1), "victim")), "rgB");
        assertTrue(r1.ok);
        PeerTrustRegistry reg = victim.getCertificateManager().getPeerTrust();
        assertNotNull(reg.verifiedIdentity(rgB.getCertificateManager().getPublicCertificate()));

        ControllerEngine impostor = controller("rgB", "rcB");                     // new key, same name, holds the global secret
        DiscoveryProcessor cv = new DiscoveryProcessor(victim), si = new DiscoveryProcessor(impostor);
        DiscoveryAuth.Msg h2 = cv.clientHello(DiscoveryType.GLOBAL);
        DiscoveryAuth.Msg c2 = si.serverChallenge(copy(h2), "victim");
        DiscoveryAuth.Msg r2 = si.serverResult(copy(h2), c2, copy(cv.clientProof(h2, copy(c2), "impostor")), "victim");
        DiscoveryNode n = cv.clientResult(h2, copy(c2), copy(r2), "10.6.6.6", 32005);
        CrescoIdentity imp = reg.verifiedIdentity(impostor.getCertificateManager().getPublicCertificate());
        CrescoIdentity real = reg.verifiedIdentity(rgB.getCertificateManager().getPublicCertificate());
        System.out.println("victim as client accepted impostor=" + (n != null) + "; impostor verifies as " + imp + "; the real rgB/rcB now "
                + real + "; impostor bridge peer=" + reg.isBridgePeer(imp));
        assertNull(imp, "a GLOBAL-secret holder replaced the strictly learned key of controller rgB/rcB at the victim (and the real one is now refused)");
    }

    /**
     * Pre-auth: the legacy TCP discovery path inflates a gzip'd 'discovery_node' param (cap 128 MiB) from a < 1 MiB frame
     * before anything is authenticated. Measured allocation for ONE request on the responder's handler.
     */
    @Test
    void oneUnauthenticatedDiscoveryFrameCannotAllocateHundredsOfMegabytes() throws Exception {
        ControllerEngine responder = controller("rgA", "rc");
        StringBuilder big = new StringBuilder(120 * 1024 * 1024 + 64);
        big.append("{\"discovery_type\":\"AGENT\",\"broadcast_validator\":\"");
        for (int i = 0; i < 120 * 1024 * 1024; i++) big.append('A');
        big.append("\"}");
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, "x", "y", "z", "pre-auth");
        me.setCompressedParam("discovery_node", big.toString());
        big = null;
        String frame = GSON.toJson(me);
        System.out.println("frame size (what the attacker sends) = " + frame.length() + " bytes");
        TCPDiscoveryEngineHandler h = new TCPDiscoveryEngineHandler(responder);
        com.sun.management.ThreadMXBean tm = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().getId();
        System.gc();
        long a0 = tm.getThreadAllocatedBytes(tid);
        try { h.channelRead(null, frame); } catch (Exception ignore) { /* null ctx after parsing */ }
        long mb = (tm.getThreadAllocatedBytes(tid) - a0) >> 20;
        System.out.println("allocated by one pre-auth frame on the responder: " + mb + " MiB (netty worker threads handle frames concurrently)");
        assertTrue(frame.length() < DiscoveryWire.MAX_OBJECT_BYTES);
        assertTrue(mb < 64, "one unauthenticated discovery frame of " + frame.length() + " bytes made the responder allocate " + mb + " MiB");
    }
}
