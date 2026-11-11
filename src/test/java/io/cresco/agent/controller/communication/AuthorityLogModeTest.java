package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.measurement.AuthorityMetrics;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Config;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.ActionAuthorizer.Mode;
import io.cresco.library.security.ActionAuthorizer.Position;
import io.cresco.library.security.Caller;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.cresco.agent.controller.communication.AuthorityFabric.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code authority_direction}: {@code log} unless set -- the calls {@code enforce} refuses ({@link PeerAuthorityTest}) are
 * allowed, counted under their action and logged once per action and position; {@code off} looks at nothing; a value
 * that is no mode stops the node.
 */
class AuthorityLogModeTest {

    private static AuthorityFabric f;

    /** A fabric that sets no mode at all: the default. */
    @BeforeAll
    static void fabric() throws Exception { f = new AuthorityFabric(null); }

    private final List<String> lines = new CopyOnWriteArrayList<>();

    @BeforeEach
    void tap() {
        f.clearSent();
        for (ControllerEngine e : new ControllerEngine[]{f.g, f.rA, f.a1, f.a2, f.rB, f.b1}) e.authority().lineTap = lines::add;
    }

    private static MsgEvent received(ControllerEngine from, Caller caller, String type, String action, ControllerEngine to,
                                     ControllerEngine attestedBy) throws Exception {
        MsgEvent m = arrive(to, send(from, caller, request(MsgEvent.Type.valueOf(type), action, from, to, false, false)), caller, attestedBy);
        assertNotNull(m, AuthorityFabric.lastRefusal);
        return m;
    }

    @Test
    void theDefaultIsLog() {
        assertEquals(Mode.LOG, f.a1.authority().mode());
        assertEquals(Mode.LOG, f.g.authority().mode());
    }

    @Test
    void theCallsEnforceRefusesAreAllowedCountedAndLoggedOnce() throws Exception {
        ControllerEngine at = f.a2;
        long before = at.authority().wouldRefuseCount();
        // a sibling's controller, three times each: every management action runs, every call is counted, each action is logged once
        for (int round = 1; round <= 3; round++) {
            for (String[] a : PeerAuthorityTest.DOWN) {
                assertTrue(admittedByAgentTier(at, received(f.a1, controller(f.a1), a[0], a[1], at, null)), a[1] + " was refused in log mode");
                assertEquals(round, at.authority().wouldRefuseCount(a[1]), a[1] + " counted");
                assertEquals(round, at.authority().wouldRefuseCount(a[1], Position.ELSEWHERE));
            }
        }
        assertTrue(f.sent(at).isEmpty(), "log mode answered a call 403");
        assertEquals(0, at.getPluginBuilder().deniedCount());
        assertEquals(before + 3L * PeerAuthorityTest.DOWN.length, at.authority().wouldRefuseCount());
        assertEquals(PeerAuthorityTest.DOWN.length, lines.size(), "one line per action and position, however often it is called: " + lines);
        String killjvm = lines.stream().filter(l -> l.contains(" killjvm ")).findFirst().orElseThrow();
        assertEquals("AUTHORITY: WOULD REFUSE CONFIG killjvm from controller:t1/rgA/a1[infra], the word of t1/rgA/a1 (ELSEWHERE):"
                + " it is honoured only from the node itself or a controller above it; allowed (authority_direction=log); logged once"
                + " per action and position, every one counted in authz.direction.would_refuse", killjvm);
        // the same action from another position is another line: an admin the global's region agent vouches for is elsewhere
        // too, an admin nobody can be named for is not
        lines.clear();
        MsgEvent noSpeaker = request(MsgEvent.Type.CONFIG, "killjvm", f.rA, at, false, false);
        admin().stamp(noSpeaker);
        assertTrue(admittedByAgentTier(at, noSpeaker), "log mode refused a speaker it cannot place");
        assertEquals(1, at.authority().wouldRefuseCount("killjvm", Position.UNKNOWN));
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).startsWith("AUTHORITY: WOULD REFUSE CONFIG killjvm from api:t1/root[admin], the word of no node this node"
                + " can name (UNKNOWN): it is honoured only from the node itself or a controller above it; allowed"), lines.get(0));
    }

    @Test
    void anApiUsersLineNamesTheNodeThatVouchedForIt() throws Exception {
        assertTrue(admittedByAgentTier(f.b1, received(f.a1, admin(), "EXEC", "getlog", f.b1, null)));
        assertEquals(1, lines.size(), lines.toString());
        assertTrue(lines.get(0).startsWith("AUTHORITY: WOULD REFUSE EXEC getlog from api:t1/root[admin], the word of t1/rgA/a1 (ELSEWHERE): "),
                lines.get(0));
        assertEquals(1, f.b1.authority().wouldRefuseCount("getlog"));
    }

    @Test
    void aCallFromTheRightPlaceIsNeitherCountedNorLogged() throws Exception {
        ControllerEngine at = f.a1;
        long before = at.authority().wouldRefuseCount();
        for (String[] a : PeerAuthorityTest.DOWN) {
            assertTrue(admittedByAgentTier(at, received(f.rA, controller(f.rA), a[0], a[1], at, f.rA)));
            assertTrue(admittedByAgentTier(at, received(f.g, admin(), a[0], a[1], at, null)));
        }
        // what is honoured from anywhere, from a sibling
        assertTrue(admittedByAgentTier(at, received(f.a2, controller(f.a2), "EXEC", "ping", at, null)));
        assertTrue(admittedByAgentTier(at, received(f.a2, controller(f.a2), "CONFIG", "pluginlist", at, null)));
        assertEquals(before, at.authority().wouldRefuseCount());
        assertTrue(lines.isEmpty(), lines.toString());
    }

    @Test
    void aReportFromAboveIsCountedUnderItsTypeNeverUnderASendersAction() throws Exception {
        ControllerEngine at = f.rA;
        for (String action : new String[]{null, "x1", "x2", "anything-a-sender-writes"}) {
            Caller caller = controller(f.rB);
            MsgEvent wire = send(f.rB, caller, request(MsgEvent.Type.WATCHDOG, action, f.rB, at, true, false));
            MsgEvent m = arrive(at, wire, caller, null);
            assertNotNull(m);
            assertTrue(admittedByControllerTier(at, m, false));
        }
        assertEquals(4, at.authority().wouldRefuseCount("WATCHDOG"));
        assertEquals(0, at.authority().wouldRefuseCount("x1"));
        assertEquals(1, lines.size(), "one line for the type and position: " + lines);
        assertTrue(lines.get(0).contains("(ELSEWHERE): it is honoured only from the node itself or a node below it;"), lines.get(0));
    }

    @Test
    void theCountsAreInTheMetricInventoryByAction() throws Exception {
        ControllerEngine at = f.rB;
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Map<String, String> names = AuthorityMetrics.register(registry, at);
        for (String expected : List.of("authz.direction.would_refuse", "authz.direction.would_refuse.pluginadd",
                "authz.direction.would_refuse.killjvm", "authz.direction.would_refuse.getfiledata", "authz.direction.would_refuse.getlog",
                "authz.direction.would_refuse.agent_enable", "authz.direction.would_refuse.region_enable",
                "authz.direction.would_refuse.gpipelinesubmit", "authz.direction.would_refuse.rpipelinesubmit",
                "authz.direction.would_refuse.watchdog", "authz.direction.would_refuse.kpi", "authz.direction.would_refuse.run_process",
                "authz.direction.would_refuse.putfiles", "authz.direction.would_refuse.other")) {
            assertTrue(names.containsKey(expected), expected + " has no meter: " + names.keySet());
        }
        for (Map.Entry<String, String> n : names.entrySet()) {
            assertEquals("authz", n.getValue(), "the inventory group of " + n.getKey());
            assertNotNull(registry.find(n.getKey()).functionCounter(), n.getKey() + " is not registered");
        }
        assertFalse(names.containsKey("authz.direction.would_refuse.ping"), "an action honoured from anywhere has no meter");
        for (int i = 0; i < 2; i++) assertTrue(admittedByAgentTier(at, received(f.a1, controller(f.a1), "CONFIG", "pluginadd", at, null)));
        assertTrue(admittedByAgentTier(at, received(f.a1, controller(f.a1), "EXEC", "getfiledata", at, null)));
        assertEquals(3.0, registry.get("authz.direction.would_refuse").functionCounter().count());
        assertEquals(2.0, registry.get("authz.direction.would_refuse.pluginadd").functionCounter().count());
        assertEquals(1.0, registry.get("authz.direction.would_refuse.getfiledata").functionCounter().count());
        assertEquals(0.0, registry.get("authz.direction.would_refuse.killjvm").functionCounter().count());
        assertEquals(0.0, registry.get("authz.direction.would_refuse.other").functionCounter().count());
        // an action a plugin gave a direction of its own is counted under "other"
        ActionAuthorizer plugin = new ActionAuthorizer("io.cresco.someplugin");
        plugin.open("EXEC", "itsown");
        plugin.direct("EXEC", "itsown", ActionAuthorizer.Direction.DOWN);
        ActionAuthorizer.Decision own = plugin.decide(stamped("itsown"), T, java.util.Set.of(), Position.ELSEWHERE, Mode.LOG);
        assertTrue(own.wouldRefuse);
        at.authority().wouldRefuse(stamped("itsown"), own);
        assertEquals(1.0, registry.get("authz.direction.would_refuse.other").functionCounter().count());
        assertEquals(4.0, registry.get("authz.direction.would_refuse").functionCounter().count());
    }

    private static MsgEvent stamped(String action) {
        MsgEvent m = request(MsgEvent.Type.EXEC, action, f.a1, f.rB, false, false);
        controller(f.a1).stamp(m);
        return m;
    }

    // ------------------------------------------------------------------ the setting

    private static Config cfg(Object... kv) throws Exception {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return TestAgentService.plugin(Files.createTempDirectory("authority").toString(), m).getConfig();
    }

    @Test
    void theSettingIsOffLogOrEnforceAndAnythingElseStopsTheNode() throws Exception {
        assertEquals(Mode.LOG, SecuritySettings.authorityDirection(cfg()));
        assertEquals(Mode.LOG, SecuritySettings.authorityDirection(null));
        assertEquals(Mode.OFF, SecuritySettings.authorityDirection(cfg("authority_direction", "off")));
        assertEquals(Mode.LOG, SecuritySettings.authorityDirection(cfg("authority_direction", "log")));
        assertEquals(Mode.ENFORCE, SecuritySettings.authorityDirection(cfg("authority_direction", "Enforce")));
        for (String mode : new String[]{"off", "log", "enforce"}) {
            assertTrue(SecuritySettings.startupRefusals(cfg("authority_direction", mode)).isEmpty(), mode);
        }
        Config typo = cfg("authority_direction", "enforced");
        List<String> refusals = SecuritySettings.startupRefusals(typo);
        assertEquals(1, refusals.size(), "a value that is no mode must stop the node: " + refusals);
        assertTrue(refusals.get(0).startsWith("authority_direction=enforced is not a mode"), refusals.get(0));
        assertEquals(Mode.ENFORCE, SecuritySettings.authorityDirection(typo), "and reads as enforce wherever it is read before that");
    }

    @Test
    void productionDoesNotForceEnforceInThisRelease() throws Exception {
        assertEquals(Mode.LOG, SecuritySettings.authorityDirection(cfg("security_profile", "production")));
        assertEquals(Mode.OFF, SecuritySettings.authorityDirection(cfg("security_profile", "production", "authority_direction", "off")));
        assertEquals(Mode.ENFORCE, SecuritySettings.authorityDirection(cfg("security_profile", "production", "authority_direction", "enforce")));
    }

    @Test
    void everyBootSaysWhenAuthorityIsNotEnforced() throws Exception {
        long now = System.currentTimeMillis();
        List<String> log = SecuritySettings.bootWarnings(cfg("broker_security_enabled", "true"), now);
        assertTrue(log.stream().anyMatch(w -> w.startsWith("authority_direction=log (this release's default)")), log.toString());
        List<String> off = SecuritySettings.bootWarnings(cfg("broker_security_enabled", "true", "authority_direction", "off"), now);
        assertTrue(off.stream().anyMatch(w -> w.startsWith("authority_direction=off: authority does not follow the tree")), off.toString());
        List<String> enforce = SecuritySettings.bootWarnings(cfg("broker_security_enabled", "true", "authority_direction", "enforce"), now);
        assertTrue(enforce.stream().noneMatch(w -> w.startsWith("authority_direction")), enforce.toString());
        // insecure development mode checks no caller at all: nothing to say about where one stands
        assertTrue(SecuritySettings.bootWarnings(cfg("broker_security_enabled", "false"), now).stream().noneMatch(w -> w.startsWith("authority_direction")));
    }

    // ------------------------------------------------------------------ enforce stands on signatures and recorded joins

    private static boolean starts(Config c) throws Exception {
        if (!SecuritySettings.startupRefusals(c).isEmpty()) return false;
        return true;
    }

    @Test
    void enforceWithoutMessageSigningRefusesTheStartAndLogStartsAndWarns() throws Exception {
        Config enforce = cfg("broker_security_enabled", "true", "authority_direction", "enforce", "msg_signing_required", "false");
        List<String> refusals = SecuritySettings.startupRefusals(enforce);
        assertEquals(1, refusals.size(), refusals.toString());
        assertTrue(refusals.get(0).startsWith("authority_direction=enforce with msg_signing_required=false: without verified message"
                + " signatures there is no speaker"), refusals.get(0));
        // the node itself does not start: nothing listens, nothing is made
        Map<String, Object> m = new HashMap<>();
        m.put("broker_security_enabled", "true");
        m.put("authority_direction", "enforce");
        m.put("msg_signing_required", "false");
        ControllerEngine engine = new ControllerEngine(null, TestAgentService.plugin(Files.createTempDirectory("authority").toString(), m), null, null);
        assertFalse(engine.start(), "a node that enforces what it cannot verify started");
        assertTrue(engine.isRefusedByConfiguration());
        // with signing (the default) enforce starts
        assertTrue(starts(cfg("broker_security_enabled", "true", "authority_direction", "enforce")));
        assertTrue(starts(cfg("broker_security_enabled", "true", "authority_direction", "enforce", "msg_signing_required", "true")));
        // log (and the default, and off) starts without signing, and log says what its count is then worth
        long now = System.currentTimeMillis();
        for (String mode : new String[]{"log", null}) {
            Config log = mode == null ? cfg("broker_security_enabled", "true", "msg_signing_required", "false")
                    : cfg("broker_security_enabled", "true", "authority_direction", mode, "msg_signing_required", "false");
            assertTrue(starts(log), "authority_direction=" + mode + " must start without message signing");
            List<String> warned = SecuritySettings.bootWarnings(log, now);
            assertEquals(1, warned.stream().filter(w -> w.startsWith("authority_direction=log with msg_signing_required=false: the"
                    + " would-refuse count (authz.direction.would_refuse) is NOT meaningful on this node for API callers and for other"
                    + " regions' nodes")).count(), warned.toString());
        }
        assertTrue(starts(cfg("broker_security_enabled", "true", "authority_direction", "off", "msg_signing_required", "false")));
        // with signing, log has nothing to warn of there
        assertTrue(SecuritySettings.bootWarnings(cfg("broker_security_enabled", "true"), now).stream()
                .noneMatch(w -> w.contains("msg_signing_required")));
        assertTrue(SecuritySettings.msgSigningRequired(cfg("broker_security_enabled", "true")));
        assertFalse(SecuritySettings.msgSigningRequired(cfg("broker_security_enabled", "true", "msg_signing_required", "false")));
        assertTrue(SecuritySettings.msgSigningRequired(cfg("security_profile", "production", "msg_signing_required", "false")),
                "production signs whatever the setting says (and refuses to start on it)");
    }

    @Test
    void enforceWithPeerIdentityOffRefusesTheStartAndLogStartsAndWarns() throws Exception {
        Config enforce = cfg("broker_security_enabled", "true", "authority_direction", "enforce", "peer_identity_mode", "off");
        List<String> refusals = SecuritySettings.startupRefusals(enforce);
        assertEquals(1, refusals.size(), refusals.toString());
        assertTrue(refusals.get(0).startsWith("authority_direction=enforce with peer_identity_mode=off: nothing records which region"
                + " controllers joined a global"), refusals.get(0));
        Map<String, Object> m = new HashMap<>();
        m.put("broker_security_enabled", "true");
        m.put("authority_direction", "enforce");
        m.put("peer_identity_mode", "off");
        ControllerEngine engine = new ControllerEngine(null, TestAgentService.plugin(Files.createTempDirectory("authority").toString(), m), null, null);
        assertFalse(engine.start());
        assertTrue(engine.isRefusedByConfiguration());
        // both at once: both are named
        assertEquals(2, SecuritySettings.startupRefusals(cfg("broker_security_enabled", "true", "authority_direction", "enforce",
                "peer_identity_mode", "off", "msg_signing_required", "false")).size());
        // the other identity modes record who joined
        for (String mode : new String[]{"enforce", "warn"}) {
            assertTrue(starts(cfg("broker_security_enabled", "true", "authority_direction", "enforce", "peer_identity_mode", mode)), mode);
        }
        long now = System.currentTimeMillis();
        Config log = cfg("broker_security_enabled", "true", "authority_direction", "log", "peer_identity_mode", "off");
        assertTrue(starts(log));
        List<String> warned = SecuritySettings.bootWarnings(log, now);
        assertEquals(1, warned.stream().filter(w -> w.startsWith("authority_direction=log with peer_identity_mode=off: the would-refuse"
                + " count (authz.direction.would_refuse) is NOT meaningful at a global for the controllers of its regions")).count(),
                warned.toString());
        assertTrue(starts(cfg("broker_security_enabled", "true", "authority_direction", "off", "peer_identity_mode", "off")));
        assertTrue(SecuritySettings.bootWarnings(cfg("broker_security_enabled", "true"), now).stream().noneMatch(w -> w.contains("peer_identity_mode")));
    }

    @Test
    void enforceInInsecureDevelopmentModeRefusesTheStart() throws Exception {
        // nothing is checked there at all: a node that says enforce and enforces nothing must not start
        List<String> refusals = SecuritySettings.startupRefusals(cfg("broker_security_enabled", "false", "authority_direction", "enforce"));
        assertEquals(1, refusals.size(), refusals.toString());
        assertTrue(refusals.get(0).startsWith("authority_direction=enforce with broker_security_enabled=false"), refusals.get(0));
        assertTrue(starts(cfg("broker_security_enabled", "false")), "insecure development mode with the default starts");
        assertTrue(starts(cfg("broker_security_enabled", "false", "authority_direction", "log")));
    }

    /** One node of its own, with security on and the mode given; nothing signed (its admission reads the stamp alone). */
    private static ControllerEngine alone(String mode, Map<String, Object> more) throws Exception {
        Map<String, Object> c = new HashMap<>(more);
        c.put("broker_security_enabled", "true");
        if (mode != null) c.put("authority_direction", mode);
        ControllerEngine e = TlsTestSupport.engine("rgA", "a2", c);
        TestAgentService as = (TestAgentService) e.getPluginBuilder().getAgentService();
        as.securityEnabled = true;
        as.tenant = T;
        as.authority = e.authority();
        return e;
    }

    private static MsgEvent fromSibling(String action) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", "a1", null, "rgA", "a2", null, false, false);
        m.setParam("action", action);
        Caller.controller(T, "rgA", "a1").stamp(m);
        return m;
    }

    @Test
    void offLooksAtNothing() throws Exception {
        ControllerEngine e = alone("off", Map.of());
        List<String> said = new CopyOnWriteArrayList<>();
        e.authority().lineTap = said::add;
        assertEquals(Mode.OFF, e.authority().mode());
        assertTrue(e.getPluginBuilder().admit(fromSibling("killjvm")));
        assertFalse(e.getPluginBuilder().authorize(fromSibling("killjvm")).wouldRefuse);
        assertEquals(0, e.authority().wouldRefuseCount());
        assertTrue(said.isEmpty());
        // and enforce on the same node, for the same stamp
        ControllerEngine strict = alone("enforce", Map.of());
        assertFalse(strict.getPluginBuilder().authorize(fromSibling("killjvm")).allowed);
        assertEquals(0, strict.authority().wouldRefuseCount(), "a refusal is not a would-refuse");
    }

    @Test
    void withoutMessageSigningAnApiUsersWordCannotBeTracedToANode() throws Exception {
        // msg_signing_required=false (development): nothing says which node vouched for a user that came over the wire.
        // enforce does not start so (above); in log the call is counted from a speaker nobody can name
        ControllerEngine e = alone("log", Map.of("msg_signing_required", "false"));
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", "rc", null, "rgA", "a2", null, false, false);
        m.setParam("action", "killjvm");
        m.setParam(MsgSigning.SPEAKER, "t1/rgA/a2");                   // a sender's own claim to be this very node
        MsgEvent got = arrive(e, wire(m), admin(), null);
        assertNotNull(got, "premise: without signing the broker's stamp is taken");
        assertNull(MsgSigning.speakerOf(got), "a sender's note stayed on a message nobody verified");
        ActionAuthorizer.Decision d = e.getPluginBuilder().authorize(got);
        assertEquals(Position.UNKNOWN, d.position, "an API admin nobody can be named for was placed");
        assertTrue(d.wouldRefuse, "and enforce would refuse it");
        // a controller's own word needs no note: its stamp is its node
        MsgEvent own = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", "a2", null, "rgA", "a2", null, false, false);
        own.setParam("action", "killjvm");
        e.stampSelf(own);
        assertFalse(e.getPluginBuilder().authorize(own).wouldRefuse);
    }
}
