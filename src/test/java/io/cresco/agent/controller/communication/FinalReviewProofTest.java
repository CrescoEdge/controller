package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Final adversarial review (2026-11-09): regression tests. Each test was first written as a proof of a weakness (it passed
 * while the weakness existed); each now asserts the fixed behaviour.
 */
class FinalReviewProofTest {

    private static final Gson GSON = new Gson();
    private static final long DAY = 24L * 3600 * 1000;

    private static MsgEvent wire(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    private static X509Certificate leaf(ControllerEngine e) { return e.getCertificateManager().getPublicCertificate()[0]; }

    private static ControllerEngine std(String region, String agent, boolean regional, String... extra) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_sessions", "false");          // first messages between two nodes are signed anyway
        for (int i = 0; i + 1 < extra.length; i += 2) cfg.put(extra[i], extra[i + 1]);
        ControllerEngine e = TlsTestSupport.engine(region, agent, cfg);
        ((TlsTestSupport.State) e.cstate).regional = regional;
        return e;
    }

    private static void joined(ControllerEngine joiner, ControllerEngine controller, boolean joinerIsController) {
        assertNull(joiner.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                leaf(controller), PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(joiner.cstate.getRegion(), joiner.cstate.getAgent(),
                leaf(joiner), joinerIsController ? PeerTrustRegistry.ROLE_CONTROLLER : PeerTrustRegistry.ROLE_AGENT, false));
        if (joinerIsController) controller.getCertificateManager().getPeerTrust().allowBridgePeer(joiner.cstate.getRegion(), joiner.cstate.getAgent());
        if (!joinerIsController) {
            ((TlsTestSupport.State) joiner.cstate).parentRegion = controller.cstate.getRegion();
            ((TlsTestSupport.State) joiner.cstate).parentAgent = controller.cstate.getAgent();
        }
    }

    private static void ping(ControllerEngine child, ControllerEngine parent, boolean childIsRegion) {
        MsgEvent p = new MsgEvent(MsgEvent.Type.EXEC, child.cstate.getRegion(), child.cstate.getAgent(), null,
                parent.cstate.getRegion(), parent.cstate.getAgent(), null, true, false);
        p.setParam("action", "ping");
        child.msgSigning().onChildPing(p);
        p = wire(p);
        Caller.controller("t1", child.cstate.getRegion(), child.cstate.getAgent()).stamp(p);
        parent.msgSigning().onPingAtParent(p, childIsRegion);
        p.setReturn();
        child.msgSigning().onParentPong(wire(p), parent.cstate.getRegion(), parent.cstate.getAgent());
    }

    private static MsgEvent sent(ControllerEngine from, Caller caller, ControllerEngine to, String action) {
        return sent(from, caller, to.cstate.getRegion(), to.cstate.getAgent(), null, action);
    }

    private static MsgEvent sent(ControllerEngine from, Caller caller, String toRegion, String toAgent, String toPlugin, String action) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, from.cstate.getRegion(), from.cstate.getAgent(), null,
                toRegion, toAgent, toPlugin, false, false);
        m.setParam("action", action);
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /** What a node that received {@code m} does to send it on to {@code target}: rewrite the (unsigned) destination. */
    private static MsgEvent retargeted(MsgEvent m, ControllerEngine target) {
        MsgEvent r = wire(m);
        r.setForwardDst(target.cstate.getRegion(), target.cstate.getAgent(), null);
        return r;
    }

    /** A small fabric: the global rgG/gc, regions rgA/rc and rgB/rc2, agents rgA/a1 and rgB/b1, keys exchanged on pings. */
    private static ControllerEngine[] fabric(String... agentExtra) throws Exception {
        ControllerEngine g = std("rgG", "gc", true);
        ((TlsTestSupport.State) g.cstate).global = true;
        ControllerEngine rA = std("rgA", "rc", true), rB = std("rgB", "rc2", true);
        ControllerEngine a1 = std("rgA", "a1", false), b1 = std("rgB", "b1", false, agentExtra);
        joined(rA, g, true);
        joined(rB, g, true);
        joined(a1, rA, false);
        joined(b1, rB, false);
        ping(rA, g, true); ping(rB, g, true); ping(rA, g, true); ping(rB, g, true);
        ping(a1, rA, false); ping(b1, rB, false);
        return new ControllerEngine[]{g, rA, rB, a1, b1};
    }

    /**
     * F1: the recipient is signed, and a receiver refuses a message that does not name it: a signed command for one node (an
     * admin's stopcontroller for b1) re-sent by b1 to any other node of the tenant -- a peer agent, another region's
     * controller, the global itself -- is refused there; so is the global's own infra message reflected back to it.
     */
    @Test
    void f1_aSignedCommandIsAcceptedOnlyByItsAddressee() throws Exception {
        ControllerEngine[] f = fabric();
        ControllerEngine g = f[0], rA = f[1], rB = f[2], a1 = f[3], b1 = f[4];

        Caller admin = Caller.api("t1", "alice", List.of(Caller.ROLE_ADMIN));
        MsgEvent toB1 = sent(g, admin, b1, "stopcontroller");            // what wsapi at the global sends for alice
        assertNotNull(toB1.getParam(MsgSigning.SIG), "signed (b1 is no child of the global)");
        assertEquals("rgB/b1/", toB1.getParam(MsgSigning.TO), "the recipient is signed");
        for (ControllerEngine victim : new ControllerEngine[]{a1, rA, rB, g}) {
            MsgSigning.Verdict v = victim.msgSigning().verify(retargeted(toB1, victim), null, false);
            assertFalse(v.ok(), "re-sent by b1 to " + victim.cstate.getAgent() + ": accepted");
            assertTrue(v.refusal.contains("not for this node"), v.refusal);
        }
        assertTrue(b1.msgSigning().verify(wire(toB1), admin, false).ok(), "its addressee accepts it");
        Caller gc = Caller.controller("t1", "rgG", "gc");
        MsgEvent infra = sent(g, gc, b1, "pluginremove");
        assertFalse(g.msgSigning().verify(retargeted(infra, g), null, false).ok(), "the global accepted its own infra message, reflected");
        // the recipient cannot be rewritten: it is under the signature
        MsgEvent forged = retargeted(infra, a1);
        forged.setParam(MsgSigning.TO, "rgA/a1/");
        assertFalse(a1.msgSigning().verify(forged, null, false).ok(), "a rewritten recipient verified");
        // ... nor the plugin it is for
        MsgEvent toPlugin = sent(g, admin, "rgB", "b1", "plugin/1", "clearrepo");
        MsgEvent otherPlugin = wire(toPlugin);
        otherPlugin.setForwardDst("rgB", "b1", "plugin/2");
        assertFalse(b1.msgSigning().verify(otherPlugin, null, false).ok(), "delivered to another plugin of the addressee");
        assertTrue(b1.msgSigning().verify(toPlugin, null, false).ok(), "to the plugin it names");
        assertTrue(b1.msgSigning().verify(retargeted(sent(g, admin, b1, "x"), b1), null, false).ok(), "an unchanged address is accepted");
    }

    /** F1, the legitimate re-addressing: a source route's last waypoint, a node's former address, the agent-to-global hop. */
    @Test
    void f1_whatTheFabricReAddressesStillReachesItsRecipient() throws Exception {
        ControllerEngine[] f = fabric("region_alias", "rgOld");
        ControllerEngine g = f[0], rA = f[1], a1 = f[3], b1 = f[4];
        Caller pa = Caller.plugin("t1", "rgA", "a1", "p1");
        // a source route through the global: the waypoint only carries it on; its last waypoint is the recipient
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", "p1", "rgB", "b1", "plugin/7", false, false);
        m.setParam("action", "getstatus");
        m.setParam(MsgRouter.SRCROUTE, "rgG,gc;rgB,b1");
        m.setParam(MsgRouter.SRCROUTE_DST_PLUGIN, "plugin/7");
        m.setForwardDst("rgG", "gc", null);
        CallerStamps.stamp(m, pa);
        a1.markForSigning(m);
        a1.msgSigning().signIfPending(m);
        assertEquals("rgB/b1/plugin/7", m.getParam(MsgSigning.TO));
        MsgEvent atWaypoint = wire(m);
        assertTrue(g.msgSigning().relayOnly(atWaypoint), "the waypoint checks it as a relay, not as its recipient");
        g.msgSigning().deferred(atWaypoint);
        assertFalse(g.msgSigning().verifyIfDeferred(atWaypoint, false).ok(), "dispatched at the waypoint: refused");
        MsgEvent atEnd = wire(m);
        atEnd.setParam(MsgRouter.SRCROUTE, "rgB,b1");
        atEnd.setForwardDst("rgB", "b1", null);
        assertFalse(b1.msgSigning().relayOnly(atEnd));
        MsgSigning.Verdict v = b1.msgSigning().verify(atEnd, pa, false);
        assertTrue(v.ok(), v.refusal);
        // region_alias: b1 answers at its former address rgOld/b1
        MsgEvent old = sent(g, Caller.controller("t1", "rgG", "gc"), "rgOld", "b1", null, "x");
        assertEquals("rgOld/b1/", old.getParam(MsgSigning.TO));
        RegionAlias.normalize(old, "rgOld", "rgB", "b1");
        assertTrue(b1.msgSigning().verify(old, null, false).ok(), "a message for its former address");
        // an agent's global function rides its region controller to the global: signed for "the global above rgA/rc"
        MsgEvent gf = new MsgEvent(MsgEvent.Type.EXEC, "rgA", "a1", "p1", "rgA", "rc", null, true, true);
        gf.setParam("action", "listregions");
        CallerStamps.stamp(gf, pa);
        a1.markForSigning(gf);
        a1.msgSigning().signIfPending(gf);
        assertEquals("^rgA/rc", gf.getParam(MsgSigning.TO));
        assertNotNull(gf.getParam(MsgSigning.SIG), "never unsigned over the direct link: the region only carries it");
        MsgEvent atRegion = wire(gf);
        assertTrue(rA.msgSigning().relayOnly(atRegion), "the region carries it on unchecked");
        rA.msgSigning().deferred(atRegion);
        MsgSigning.Verdict atRegionRun = rA.msgSigning().verifyIfDeferred(atRegion, false);
        assertFalse(atRegionRun.ok(), "run at the region it is refused");
        MsgEvent atGlobal = wire(gf);
        atGlobal.setForwardDst("rgG", "gc", null);
        MsgSigning.Verdict vg = g.msgSigning().verify(atGlobal, pa, false);
        assertTrue(vg.ok(), vg.refusal);
        MsgEvent toOtherGlobal = wire(gf);
        ControllerEngine g2 = std("rgH", "gc2", true);
        ((TlsTestSupport.State) g2.cstate).global = true;
        assertFalse(g2.msgSigning().verify(toOtherGlobal, pa, false).ok(), "a global rgA/rc is no region of");
    }

    /**
     * LOW: a function re-addressed to this node on the way (the controller tiers turn a global function addressed elsewhere
     * into one for this controller) is verified when it is dispatched here, as addressed to this node: refused.
     */
    @Test
    void aMessageReAddressedToThisNodeIsVerifiedWhereItIsDispatched() throws Exception {
        ControllerEngine[] f = fabric();
        ControllerEngine rB = f[2], a1 = f[3], b1 = f[4];
        Caller pa = Caller.plugin("t1", "rgA", "a1", "p1");
        MsgEvent forB1 = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", "a1", "p1", "rgB", "b1", null, true, false);
        forB1.setParam("action", "removeplugin");                       // a global-tier function
        CallerStamps.stamp(forB1, pa);
        a1.markForSigning(forB1);
        a1.msgSigning().signIfPending(forB1);
        MsgEvent arrived = wire(forB1);
        // rB's consumer: not addressed to rB, so not checked on arrival but noted ...
        rB.msgSigning().deferred(arrived);
        // ... and its router re-addresses it to rB (GLOBAL_VIA_THIS_CONTROLLER) and dispatches it: checked now
        arrived.setForwardDst("rgB", "rc2", null);
        assertFalse(rB.admitDeferred(arrived), "a re-addressed message was dispatched unverified");
        // an in-JVM message (no note) and one checked on arrival pass straight through
        MsgEvent local = new MsgEvent(MsgEvent.Type.EXEC, "rgB", "rc2", "p1", "rgB", "rc2", null, false, false);
        assertTrue(rB.admitDeferred(local));
        MsgEvent forB1Again = wire(forB1);
        b1.msgSigning().deferred(forB1Again);
        assertTrue(b1.admitDeferred(forB1Again), "deferred, then dispatched at its own recipient");
        // a sender cannot pre-set the note to skip or force a check: every receiver removes it from what arrives
        MsgEvent preset = wire(forB1);
        preset.setParam(MsgSigning.DEFERRED, "1");
        MsgSigning.notPending(preset);
        assertNull(preset.getParam(MsgSigning.DEFERRED));
    }

    // ------------------------------------------------------------------ production: operator revocation vs message auth

    private static ControllerEngine provisioned(PkiTestSupport.Node n, Path anchors, String region, String agent, Path revocations,
                                                boolean regional, boolean global, boolean sessions) throws Exception {
        Path dir = Files.createTempDirectory("frv-node-" + agent);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("identity_keystore", n.keystore.toString());
        cfg.put("identity_keystore_password_file", n.password.toString());
        cfg.put("trust_anchors", anchors.toString());
        cfg.put("identity_keystore_reload_seconds", "3600");
        cfg.put("node_identity_dir", Files.createDirectories(dir.resolve("security")).toString());
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("msg_signing_direct", "false");             // every message signed: what is proven is the credential check
        cfg.put("msg_signing_sessions", Boolean.toString(sessions));
        if (revocations != null) cfg.put("revocation_file", revocations.toString());
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State st = TlsTestSupport.state(pb, region, agent);
        st.regional = regional;
        st.global = global;
        e.cstate = st;
        e.setCertificateManager(new CertificateManager(e));
        assertTrue(e.getCertificateManager().isProvisioned());
        return e;
    }

    private static void reloaded(ControllerEngine e, Path rev, String crl) throws Exception {
        long before = Files.getLastModifiedTime(rev).toMillis();
        Files.writeString(rev, crl);
        Files.setLastModifiedTime(rev, FileTime.fromMillis(Math.max(before, Files.getLastModifiedTime(rev).toMillis()) + 5_000));
        assertTrue(e.getCertificateManager().reloadProvisionedIfChanged(), "the CRL was not loaded");
    }

    /**
     * F2 (production): the operator's revocation_file reaches message authentication. A CRL-revoked agent's key no longer
     * authenticates through its region's attestation, nor a CRL-revoked region controller's through the trust bundle or a
     * cached credential, nor either under a session; the global drops the region from its bundle; and an out-of-date CRL
     * refuses (fail closed) what its issuer issued.
     */
    @Test
    void f2_operatorRevocationsStopMessageAuthentication() throws Exception {
        TlsTestSupport.provider();
        long now = System.currentTimeMillis();
        PkiTestSupport.Ca root = PkiTestSupport.root("Final Review Root");
        Path pki = Files.createTempDirectory("frv-pki");
        Path anchors = PkiTestSupport.pem(pki.resolve("trust-anchors.pem"), root.cert);
        PkiTestSupport.Node gN = PkiTestSupport.write(PkiTestSupport.global(root, "t1", "rgG", "gc", now - 60_000, 10 * DAY), pki.resolve("g"));
        PkiTestSupport.Node rN = PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgA", "rc", true, now - 60_000, 10 * DAY), pki.resolve("rc"));
        PkiTestSupport.Node aN = PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgA", "a1", false, now - 60_000, 10 * DAY), pki.resolve("a1"));
        PkiTestSupport.Node a2N = PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgA", "a2", false, now - 60_000, 10 * DAY), pki.resolve("a2"));
        PkiTestSupport.Node a3N = PkiTestSupport.write(PkiTestSupport.node(root, "t1", "rgA", "a3", false, now - 60_000, 10 * DAY), pki.resolve("a3"));
        Path rev = pki.resolve("revoked.pem");
        Files.writeString(rev, PkiTestSupport.crl(root, BigInteger.ONE, new Date(now - 60_000), new Date(now + DAY)));

        ControllerEngine g = provisioned(gN, anchors, "rgG", "gc", rev, true, true, true);
        ControllerEngine rA = provisioned(rN, anchors, "rgA", "rc", null, true, false, false);
        ControllerEngine a1 = provisioned(aN, anchors, "rgA", "a1", null, false, false, true);
        ControllerEngine a2 = provisioned(a2N, anchors, "rgA", "a2", null, false, false, false);
        ControllerEngine a3 = provisioned(a3N, anchors, "rgA", "a3", null, false, false, false);
        ping(rA, g, true); ping(rA, g, true);
        ping(a1, rA, false);
        ping(a2, rA, false);
        ping(a3, rA, false);
        assertTrue(a1.msgSigning().hasAttestation(), "a1 holds its region's attestation");
        int bundleBefore = g.msgSigning().bundleSize();

        Caller a1c = Caller.controller("t1", "rgA", "a1"), rc = Caller.controller("t1", "rgA", "rc"), a2c = Caller.controller("t1", "rgA", "a2"),
                a3c = Caller.controller("t1", "rgA", "a3");
        assertTrue(g.msgSigning().verify(sent(a1, a1c, g, "x"), a1c, false).ok(), "before revocation");
        assertTrue(g.msgSigning().verify(sent(rA, rc, g, "x"), rc, false).ok(), "before revocation");
        // a1 and the global agree a session (a1's next messages carry MACs)
        Caller gc = Caller.controller("t1", "rgG", "gc");
        boolean mac = false;
        for (int i = 0; i < 6 && !mac; i++) {
            MsgEvent ag = sent(a1, a1c, g, "s" + i);
            assertTrue(g.msgSigning().verify(ag, a1c, false).ok());
            assertTrue(a1.msgSigning().verify(sent(g, gc, a1, "r" + i), gc, false).ok());
            mac = ag.getParam(MsgSessions.MAC) != null;
        }
        assertTrue(mac, "no session between a1 and the global");
        MsgEvent underSession = sent(a1, a1c, g, "after-revocation-mac");
        assertNotNull(underSession.getParam(MsgSessions.MAC));

        // 1. the operator revokes a1's and a2's certificates (keyCompromise) on the global, with a newer CRL due again in 2 s
        assertTrue(g.msgSigning().verify(sent(a2, a2c, g, "before"), a2c, false).ok(), "before revocation (its credential cached)");
        reloaded(g, rev, PkiTestSupport.crl(root, BigInteger.TWO, new Date(now - 30_000), new Date(System.currentTimeMillis() + 2_000),
                aN.chain[0].getSerialNumber(), a2N.chain[0].getSerialNumber()));
        String[] why = new String[1];
        assertNull(g.getCertificateManager().getPeerTrust().verifiedIdentity(aN.chain, why), "the PKI still accepts a1");
        MsgSigning.Verdict va = g.msgSigning().verify(sent(a2, a2c, g, "after-revocation"), null, false);
        assertFalse(va.ok(), "a2's revoked key, through its attestation");
        assertTrue(va.refusal.contains("on its CRL"), va.refusal);
        MsgSigning.Verdict vm = g.msgSigning().verify(underSession, a1c, false);
        assertFalse(vm.ok(), "a1's revoked key, under its session");
        assertFalse(g.msgSigning().verify(sent(a1, a1c, g, "after-revocation"), null, false).ok(), "a1's next message");
        assertTrue(g.msgSigning().verify(sent(a3, a3c, g, "unrevoked"), a3c, false).ok(), "a3, not revoked");
        assertTrue(g.msgSigning().verify(sent(rA, rc, g, "unrevoked"), rc, false).ok(), "rc, not revoked");

        // 2. the CRL goes out of date: what its issuer issued is of unknown status, refused (fail closed), with no reload
        Thread.sleep(2_500);
        MsgSigning.Verdict stale = g.msgSigning().verify(sent(a3, a3c, g, "stale-crl"), a3c, false);
        assertFalse(stale.ok(), "a3 under an out-of-date CRL");
        assertTrue(stale.refusal.contains("out of date"), stale.refusal);
        assertFalse(g.msgSigning().verify(sent(rA, rc, g, "stale-crl"), rc, false).ok(), "rc under an out-of-date CRL");

        // 3. a fresh CRL that also revokes the region controller: its messages are refused, the global drops it from the
        //    bundle it serves, and what it attested (a3) no longer authenticates
        reloaded(g, rev, PkiTestSupport.crl(root, BigInteger.valueOf(3), new Date(now - 20_000), new Date(now + DAY),
                aN.chain[0].getSerialNumber(), rN.chain[0].getSerialNumber()));
        MsgSigning.Verdict vr = g.msgSigning().verify(sent(rA, rc, g, "after-revocation"), null, false);
        assertFalse(vr.ok(), "rc's revoked key (trust bundle / credential cache)");
        assertEquals(bundleBefore - 1, g.msgSigning().bundleSize(), "the global still lists the revoked region controller");
        assertFalse(g.msgSigning().servedBundle().contains(Base64Key.of(rN)), "the bundle served down still carries rc");
        assertFalse(g.msgSigning().verify(sent(a3, a3c, g, "issuer-revoked"), a3c, false).ok(), "an attestation of a revoked controller");
    }

    /** The base64 of a node's public key, as a bundle entry carries it. */
    private static final class Base64Key {
        static String of(PkiTestSupport.Node n) { return java.util.Base64.getEncoder().encodeToString(n.chain[0].getPublicKey().getEncoded()); }
    }

    /**
     * F3: discovery_compat_until more than 90 days ahead is IGNORED for good: the window is decided when the configuration
     * is first read (the node's start), and does not open by itself as the date comes nearer; a restart (the configuration
     * read anew) within 90 days of it opens it.
     */
    @Test
    void f3_anIgnoredUpgradeWindowNeverOpensWithoutARestart() throws Exception {
        long now = System.currentTimeMillis();
        String until = java.time.Instant.ofEpochMilli(now + 120 * DAY).toString();
        io.cresco.library.plugin.Config c = TestAgentService.plugin(Files.createTempDirectory("frv-cfg").toString(),
                Map.of("discovery_auth", "compat", SecuritySettings.DISCOVERY_COMPAT_UNTIL, until)).getConfig();
        assertFalse(SecuritySettings.compatOpen(c, now), "open at boot");
        assertTrue(SecuritySettings.compatProblems(c, now).get(0).contains("IGNORED"), "boot says IGNORED");
        assertFalse(SecuritySettings.compatOpen(c, now + 31 * DAY), "31 days later the window opened without a restart");
        assertTrue(SecuritySettings.compatProblems(c, now + 31 * DAY).get(0).contains("IGNORED"));
        io.cresco.library.plugin.Config restarted = TestAgentService.plugin(Files.createTempDirectory("frv-cfg").toString(),
                Map.of("discovery_auth", "compat", SecuritySettings.DISCOVERY_COMPAT_UNTIL, until)).getConfig();
        assertTrue(SecuritySettings.compatOpen(restarted, now + 31 * DAY), "restarted within 90 days of its end: open");
        assertTrue(SecuritySettings.bootWarnings(restarted, now + 31 * DAY).stream().anyMatch(w -> w.startsWith("UPGRADE WINDOW OPEN")));
        assertFalse(SecuritySettings.compatOpen(restarted, now + 121 * DAY), "closed after its end");
    }

    /**
     * F1b: a re-sent message carrying a session offer cannot break another pair's session: a1's signed offer to b3, re-sent to
     * b1 (which holds a confirmed session with a1), is refused there (it does not name b1), and a1 and b1 go on with MACs.
     */
    @Test
    void f1b_aReSentOfferCannotBreakAnotherPairsSession() throws Exception {
        Map<String, Object> on = Map.of("broker_security_enabled", "true");
        ControllerEngine g = TlsTestSupport.engine("rgG", "gc", on);
        ((TlsTestSupport.State) g.cstate).regional = true;
        ((TlsTestSupport.State) g.cstate).global = true;
        ControllerEngine rA = TlsTestSupport.engine("rgA", "rc", on), rB = TlsTestSupport.engine("rgB", "rc2", on);
        ((TlsTestSupport.State) rA.cstate).regional = true;
        ((TlsTestSupport.State) rB.cstate).regional = true;
        ControllerEngine a1 = TlsTestSupport.engine("rgA", "a1", on), b1 = TlsTestSupport.engine("rgB", "b1", on),
                b3 = TlsTestSupport.engine("rgB", "b3", on);
        joined(rA, g, true); joined(rB, g, true); joined(a1, rA, false); joined(b1, rB, false); joined(b3, rB, false);
        ping(rA, g, true); ping(rB, g, true); ping(rA, g, true); ping(rB, g, true);
        ping(a1, rA, false); ping(b1, rB, false); ping(b3, rB, false);
        Caller pa = Caller.plugin("t1", "rgA", "a1", "p1"), pb1 = Caller.plugin("t1", "rgB", "b1", "p1"), pb3 = Caller.plugin("t1", "rgB", "b3", "p1");
        boolean mac = false;
        for (int i = 0; i < 6 && !mac; i++) {
            MsgEvent ab = sentAs(a1, pa, b1), ba = sentAs(b1, pb1, a1);
            assertTrue(b1.msgSigning().verify(ab, pa, false).ok());
            assertTrue(a1.msgSigning().verify(ba, pb1, false).ok());
            mac = ab.getParam(MsgSessions.MAC) != null && ba.getParam(MsgSessions.MAC) != null;
        }
        assertTrue(mac, "no session between a1 and b1");
        assertTrue(a1.msgSigning().verify(sentAs(b3, pb3, a1), pb3, false).ok());
        MsgEvent offer = sentAs(a1, pa, b3);
        assertNotNull(offer.getParam(MsgSessions.KX), "a1 offered b3 a session");
        assertNotNull(offer.getParam(MsgSigning.SIG));
        MsgSigning.Verdict reSent = b1.msgSigning().verify(retargeted(offer, b1), null, false);
        assertFalse(reSent.ok(), "b1 accepted the offer a1 made to b3");
        // a1 and b1 still talk under their session, both ways
        for (int i = 0; i < 4; i++) {
            MsgEvent ba = sentAs(b1, pb1, a1), ab = sentAs(a1, pa, b1);
            assertTrue(a1.msgSigning().verify(ba, pb1, false).ok());
            assertTrue(b1.msgSigning().verify(ab, pa, false).ok());
            assertNotNull(ba.getParam(MsgSessions.MAC), "b1 no longer sends a1 MACs");
            assertNotNull(ab.getParam(MsgSessions.MAC), "a1 no longer sends b1 MACs");
        }
        // a MAC'd message re-addressed is refused as well: the MAC covers the recipient
        MsgEvent macd = sentAs(a1, pa, b1);
        assertNotNull(macd.getParam(MsgSessions.MAC));
        assertFalse(b3.msgSigning().verify(retargeted(macd, b3), null, false).ok());
    }

    private static MsgEvent sentAs(ControllerEngine from, Caller caller, ControllerEngine to) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "getstatus");
        m.setParam("n", Long.toString(System.nanoTime()));
        CallerStamps.stamp(m, caller);
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        return wire(m);
    }

    /**
     * F4 (scale): a child's ping costs its parent O(1) and takes no lock, whatever the size of the trust bundle; the bundle is
     * rebuilt (and signed) once per change.
     */
    @Test
    void f4_aPingCostsTheSameWhateverTheFabricsSize() throws Exception {
        ControllerEngine r = std("rgA", "rc", true);
        java.lang.reflect.Field f = MsgSigning.class.getDeclaredField("bundle");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, MsgSigning.BundleEntry> bundle =
                (java.util.concurrent.ConcurrentHashMap<String, MsgSigning.BundleEntry>) f.get(r.msgSigning());
        java.security.PublicKey k = leaf(r).getPublicKey();
        List<PeerTrustRegistry.CertRef> refs = List.of(PeerTrustRegistry.CertRef.of(leaf(r)));
        double[] per = new double[3];
        int[] sizes = {10, 1000, 5000};
        for (int s = 0; s < sizes.length; s++) {
            bundle.clear();
            for (int i = 0; i < sizes[s]; i++) {
                MsgSigning.BundleEntry e = new MsgSigning.BundleEntry();
                e.t = "t1"; e.r = "rg" + i; e.a = "rc" + i; e.role = "region"; e.key = k; e.refs = refs;
                e.k = java.util.Base64.getEncoder().encodeToString(k.getEncoded());
                e.fp = PeerTrustRegistry.fingerprint(k.getEncoded()) + i;
                bundle.put(e.r + "/" + e.a, e);
            }
            r.msgSigning().bundleChanged();
            String first = r.msgSigning().servedBundle();                // signed once (the first ping after a change)
            assertTrue(first.contains("rg" + (sizes[s] - 1)), "the change was not served");
            int n = 20_000;
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) assertSame(first, r.msgSigning().servedBundle());
            per[s] = (System.nanoTime() - t0) / (double) n / 1000.0;
            System.out.printf("F4: trust bundle of %d regions: %.3f us per child ping%n", sizes[s], per[s]);
        }
        assertTrue(per[2] < 5.0 && per[2] < 10 * Math.max(0.05, per[0]), "the per-ping cost grows with the regions: " + java.util.Arrays.toString(per));
    }

    /**
     * LOW (bounded memory, no thrash cliff): the credential cache is a bounded LRU -- past its bound the least recently used
     * credential goes, never all of them at once.
     */
    @Test
    void theCredentialCacheEvictsOneAtATimeNeverAll() throws Exception {
        ControllerEngine[] f = fabric();
        ControllerEngine g = f[0], rA = f[1], rB = f[2], a1 = f[3];
        ControllerEngine b1 = std("rgB", "b9", false, "msg_signing_credentials_max", "2");
        joined(b1, rB, false);
        ping(b1, rB, false);
        Caller gc = Caller.controller("t1", "rgG", "gc"), rc = Caller.controller("t1", "rgA", "rc"), a1c = Caller.controller("t1", "rgA", "a1");
        assertTrue(b1.msgSigning().verify(sent(g, gc, b1, "x"), gc, false).ok());
        assertTrue(b1.msgSigning().verify(sent(rA, rc, b1, "x"), rc, false).ok());
        assertEquals(2, b1.msgSigning().credentialsCached());
        assertTrue(b1.msgSigning().verify(sent(a1, a1c, b1, "x"), a1c, false).ok());
        assertEquals(2, b1.msgSigning().credentialsCached(), "the cache was emptied at its bound (or grew past it)");
        assertTrue(b1.msgSigning().verify(sent(g, gc, b1, "y"), gc, false).ok(), "an evicted credential is verified anew");
    }

    /** LOW: production no longer assumes FIPS approved-only mode: a JVM with the JDK's own providers is refused. */
    @Test
    void productionRefusesAJvmThatIsNotInFipsMode() {
        String why = FipsMode.refusal();
        assertNotNull(why, "this test JVM (JDK providers) passed as FIPS approved-only mode");
        assertTrue(why.contains("is not a FIPS 140-3 validated module"), why);
        assertNotNull(FipsMode.refusal(new java.security.Provider[0]));
        java.security.Provider jsse = java.security.Security.getProvider("SunJSSE");
        assertTrue(FipsMode.refusal(new java.security.Provider[]{jsse}).contains("BCFIPS"), "no Bouncy Castle FIPS: refused");
    }
}
