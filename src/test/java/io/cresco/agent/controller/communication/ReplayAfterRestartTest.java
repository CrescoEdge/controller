package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.test.TestAgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The replay memory was in RAM only: a node that restarted remembered nothing, and a signed message it had accepted
 * could be delivered to it again, and accepted, for as long as its signature was young enough (five minutes). Now the
 * latest signing time it accepted from each signer is on disk ({@link ReplayFloor}), and after a restart a message that
 * signer signed at or before it is refused -- while everything the signer signs later, sent after the restart or stored
 * by a broker during it, is accepted as before.
 */
class ReplayAfterRestartTest {

    private static final Gson GSON = new Gson();

    /** The node {@code region/agent} on {@code dir}: called again with the same directory, it is that node, restarted. */
    private static ControllerEngine at(Path dir, String region, String agent, boolean regional) throws Exception {
        return at(dir, region, agent, regional, Map.of());
    }

    private static ControllerEngine at(Path dir, String region, String agent, boolean regional, Map<String, Object> extra) throws Exception {
        TlsTestSupport.provider();
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put("public_key_directory", dir.resolve("public-keys").toString());
        cfg.put("tenant_id", "t1");
        cfg.put("broker_security_enabled", "true");
        cfg.put("msg_signing_sessions", "false");                // every message signed: what a restart could let through
        PluginBuilder pb = TestAgentService.plugin(dir.toString(), cfg, m -> { }, region, agent);
        ControllerEngine e = new ControllerEngine(null, pb, null, null);
        TlsTestSupport.State s = new TlsTestSupport.State(pb, region, agent);
        s.regional = regional;
        e.cstate = s;
        e.setCertificateManager(new CertificateManager(e));
        return e;
    }

    private static void joined(ControllerEngine agent, ControllerEngine controller) {
        assertNull(agent.getCertificateManager().getPeerTrust().pin(controller.cstate.getRegion(), controller.cstate.getAgent(),
                controller.getCertificateManager().getPublicCertificate()[0], PeerTrustRegistry.ROLE_CONTROLLER, true));
        assertNull(controller.getCertificateManager().getPeerTrust().pin(agent.cstate.getRegion(), agent.cstate.getAgent(),
                agent.getCertificateManager().getPublicCertificate()[0], PeerTrustRegistry.ROLE_AGENT, false));
    }

    private static Caller plugin(ControllerEngine e) { return Caller.plugin("t1", e.cstate.getRegion(), e.cstate.getAgent(), "p1"); }

    private static MsgEvent copy(MsgEvent m) { return GSON.fromJson(GSON.toJson(m), MsgEvent.class); }

    /** A message of {@code from}'s plugin for the controller {@code to}, signed as it leaves; each a few ms after the one before. */
    private static MsgEvent signed(ControllerEngine from, ControllerEngine to, String what) throws Exception {
        Thread.sleep(5);
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, from.cstate.getRegion(), from.cstate.getAgent(), "p1",
                to.cstate.getRegion(), to.cstate.getAgent(), null, false, false);
        m.setParam("action", "do");
        m.setParam("what", what);
        CallerStamps.stamp(m, plugin(from));
        from.markForSigning(m);
        from.msgSigning().signIfPending(m);
        assertNotNull(m.getParam(MsgSigning.SIG), "signed");
        return copy(m);
    }

    private static MsgSigning.Verdict arrives(ControllerEngine to, MsgEvent m, ControllerEngine from) {
        return to.msgSigning().verify(copy(m), plugin(from), false);
    }

    @Test
    void aSignedMessageAcceptedBeforeARestartIsRefusedAfterItAndWhatIsSignedLaterIsNot() throws Exception {
        Path rcDir = Files.createTempDirectory("floor-rc"), aDir = Files.createTempDirectory("floor-a"), bDir = Files.createTempDirectory("floor-b");
        ControllerEngine rc = at(rcDir, "rgA", "rc", true), a = at(aDir, "rgA", "a1", false), b = at(bDir, "rgA", "a2", false);
        joined(a, rc);
        joined(b, rc);
        MsgEvent overtaken = signed(a, rc, "signed first, still on its way");
        MsgEvent accepted = signed(a, rc, "stop the pump");
        assertTrue(arrives(rc, accepted, a).ok());
        assertEquals(MsgSigning.Refusal.REPLAY, arrives(rc, accepted, a).reason, "in one life the replay memory refuses the copy");
        MsgEvent duringTheOutage = signed(a, rc, "signed while rc was down, stored by a broker");
        MsgEvent fromB = signed(b, rc, "another signer's, never delivered");

        // rc is killed (nothing is closed, nothing flushed by it) and starts again: the same node, an empty replay memory
        ControllerEngine rc2 = at(rcDir, "rgA", "rc", true);
        MsgSigning.Verdict again = arrives(rc2, accepted, a);
        assertFalse(again.ok(), "a restarted node accepted a signed message it had accepted before");
        assertEquals(MsgSigning.Refusal.REPLAY, again.reason);
        assertTrue(again.refusal.contains("before this node restarted"), again.refusal);
        assertEquals(1, rc2.msgSigning().replayFloorRefused());

        // what its signer signed AFTER the last message rc accepted from it is accepted: stored during the outage, or new
        assertTrue(arrives(rc2, duringTheOutage, a).ok(), "a message signed during the outage and stored is refused");
        MsgEvent fresh = signed(a, rc, "signed after the restart");
        MsgSigning.Verdict v = arrives(rc2, fresh, a);
        assertTrue(v.ok(), v.refusal);
        // a signer rc had accepted nothing from is not touched
        assertTrue(arrives(rc2, fromB, b).ok());
        // what the signer signed BEFORE the last one rc accepted, and rc never saw: it cannot tell it from one it accepted
        MsgSigning.Verdict late = arrives(rc2, overtaken, a);
        assertEquals(MsgSigning.Refusal.REPLAY, late.reason, "a message that may have been accepted before the restart");

        // and a second restart keeps the floor of the first life and takes the second's
        ControllerEngine rc3 = at(rcDir, "rgA", "rc", true);
        assertEquals(MsgSigning.Refusal.REPLAY, arrives(rc3, accepted, a).reason, "the first life's floor");
        assertEquals(MsgSigning.Refusal.REPLAY, arrives(rc3, fresh, a).reason, "the second life's");
        assertEquals(MsgSigning.Refusal.REPLAY, arrives(rc3, fromB, b).reason);
        assertTrue(arrives(rc3, signed(a, rc, "later still"), a).ok());
    }

    @Test
    void theFloorCanBeTurnedOffAndThenARestartForgetsAsBefore() throws Exception {
        Path rcDir = Files.createTempDirectory("floor-off-rc"), aDir = Files.createTempDirectory("floor-off-a");
        ControllerEngine a = at(aDir, "rgA", "a1", false);
        ControllerEngine rc = at(rcDir, "rgA", "rc", true);
        joined(a, rc);
        MsgEvent m = signed(a, rc, "once");
        assertTrue(arrives(rc, m, a).ok());
        ControllerEngine rc2 = at(rcDir, "rgA", "rc", true, Map.of("msg_replay_floor", "false"));
        assertTrue(arrives(rc2, m, a).ok(), "msg_replay_floor=false: as before the floor existed");
    }

    // ---------------------------------------------------------------- the record itself

    private static final List<String> LINES = new ArrayList<>();

    private static ReplayFloor open(Path file, long keepMs, long rotateBytes) {
        return new ReplayFloor(file, LINES::add, keepMs, rotateBytes);
    }

    @Test
    void aSignersLatestSigningTimeIsItsFloorAfterARestartAndOnlyTimesOfOneSignerAreCompared() throws Exception {
        Path file = Files.createTempDirectory("floor").resolve("replay-floor.bin");
        long now = System.currentTimeMillis();
        ReplayFloor first = open(file, 360_000L, ReplayFloor.ROTATE_BYTES);
        first.accepted(7L, 1_000L, now);
        first.accepted(7L, 3_000L, now);
        first.accepted(7L, 2_000L, now);                         // an earlier one arriving later: nothing to record
        first.accepted(9L, 9_000_000L, now);                     // another signer, another clock
        assertFalse(first.mayHaveAccepted(7L, 1_000L), "within one life the replay memory judges, not the floor");
        assertEquals(2 * ReplayFloor.RECORD + ReplayFloor.RECORD, Files.size(file), "one record per later signing time, written through");

        // the process dies here: no close, no force
        ReplayFloor second = open(file, 360_000L, ReplayFloor.ROTATE_BYTES);
        assertTrue(second.mayHaveAccepted(7L, 3_000L));
        assertTrue(second.mayHaveAccepted(7L, 2_500L), "signed before the last accepted one: it may have been accepted");
        assertFalse(second.mayHaveAccepted(7L, 3_001L), "signed after it: never refused");
        assertFalse(second.mayHaveAccepted(9L, 9_000_001L));
        assertTrue(second.mayHaveAccepted(9L, 3_000L), "judged against its own signer's time, not another's");
        assertFalse(second.mayHaveAccepted(11L, 1L), "a signer never heard of");
        assertEquals(2 * ReplayFloor.RECORD, Files.size(file), "rewritten at start: one record per signer");
        second.accepted(7L, 4_000L, now);
        second.close();
        ReplayFloor third = open(file, 360_000L, ReplayFloor.ROTATE_BYTES);
        assertTrue(third.mayHaveAccepted(7L, 4_000L));
        assertTrue(third.mayHaveAccepted(9L, 9_000_000L), "a floor is carried over a life in which its signer said nothing");
        third.close();
    }

    @Test
    void aRecordIsDroppedOnceNothingAtOrBeforeItCouldStillBeAcceptedAndTheFileStaysSmall() throws Exception {
        Path file = Files.createTempDirectory("floor").resolve("replay-floor.bin");
        long now = System.currentTimeMillis();
        ReplayFloor f = open(file, 1_500L, 20L * ReplayFloor.RECORD);
        for (int i = 1; i <= 100; i++) f.accepted(7L, i, now);    // 100 records of one signer: past the size to rewrite at
        f.accepted(8L, 5L, now);
        assertEquals(101L * ReplayFloor.RECORD, Files.size(file));
        f.tick();                                                // the background thread's look
        assertEquals(2L * ReplayFloor.RECORD, Files.size(file), "rewritten: one record per signer");
        f.close();
        ReplayFloor next = open(file, 60_000L, ReplayFloor.ROTATE_BYTES);
        assertTrue(next.mayHaveAccepted(7L, 100L));
        next.close();
        Thread.sleep(1_700);                                     // longer than a signature may be old (here: 1.5 s)
        ReplayFloor later = open(file, 1_500L, ReplayFloor.ROTATE_BYTES);
        assertFalse(later.mayHaveAccepted(7L, 100L), "such a message is refused for its age: the record is not needed");
        assertEquals(0, later.signers());
        assertEquals(0L, Files.size(file));
        later.close();
    }

    @Test
    void aFileThatCannotBeWrittenIsReportedAndTheNodeGoesOnAccepting() throws Exception {
        Path dir = Files.createTempDirectory("floor");
        Path file = dir.resolve("no-such-dir-is-a-file");
        Files.writeString(file, "x");
        LINES.clear();
        ReplayFloor f = open(file.resolve("replay-floor.bin"), 360_000L, ReplayFloor.ROTATE_BYTES);   // its parent is a file
        f.accepted(7L, 1_000L, System.currentTimeMillis());      // no exception reaches the message path
        assertFalse(f.mayHaveAccepted(7L, 1_000L));
        assertTrue(LINES.stream().anyMatch(l -> l.contains("REPLAY FLOOR")), "the operator is told: " + LINES);
        f.close();
    }
}
