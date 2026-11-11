package io.cresco.agent.controller.communication;

import io.cresco.agent.test.TestAgentService;
import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The file groups a node is receiving (AgentConsumer). An entry -- the group's receiver with its MsgEvent -- used to
 * stay for good, completed or abandoned, an abandoned transfer's parts with it in the dataplane journal directory; a
 * caller could register groups without limit; and a second registration of a group id replaced the first one's owner.
 */
class IncomingFileGroupsTest {

    private static final long IDLE = 1_000;
    private static PluginBuilder plugin;

    @BeforeAll
    static void plugin() throws Exception {
        plugin = TestAgentService.plugin(Files.createTempDirectory("file-groups").toString(), new HashMap<>());
    }

    /** A receiver for a group of files, {@code parts} parts each, named as a sender names them. */
    private static FileObjectGroupReceiver receiver(String group, int parts, String... dataNames) {
        Map<String, FileObject> files = new HashMap<>();
        for (String dataName : dataNames) {
            Map<String, String> partHashes = new HashMap<>();
            for (int i = 0; i < parts; i++) partHashes.put(dataName + "." + i, "hash" + i);
            files.put(dataName, new FileObject(dataName + ".bin", "filehash", partHashes, dataName));
        }
        MsgEvent me = new MsgEvent(MsgEvent.Type.EXEC, "r1", "a1", null, "r1", "a1", null, false, false);
        return new FileObjectGroupReceiver(plugin, me, files, group);
    }

    private static String register(IncomingFileGroups groups, String id, String owner, long now) {
        return groups.register(id, owner, receiver(id, 2, id + "-data"), Arrays.asList(id + "-data"), now);
    }

    @Test
    void aCompletedGroupIsRemoved() {
        IncomingFileGroups groups = new IncomingFileGroups(8, 8, IDLE);
        assertNull(register(groups, "g1", "alice", 0));
        FileObjectGroupReceiver g1 = groups.active("g1", 10);
        assertNotNull(g1);
        groups.complete("g1", g1);
        assertEquals(0, groups.size(), "a completed group stayed for good, with its MsgEvent");
        assertNull(groups.active("g1", 20), "a part repeated after completion would complete (and deliver) the group again");
    }

    @Test
    void anAbandonedGroupExpiresAndAnActiveOneDoesNot() {
        IncomingFileGroups groups = new IncomingFileGroups(8, 8, IDLE);
        register(groups, "abandoned", "alice", 0);
        register(groups, "slow", "alice", 0);
        assertNotNull(groups.active("slow", 900));                       // a part arrives
        assertTrue(groups.expire(IDLE - 1).isEmpty(), "not idle long enough yet");
        List<IncomingFileGroups.Group> expired = groups.expire(IDLE + 1);
        assertEquals(1, expired.size(), "an abandoned group was never removed");
        assertEquals("abandoned", expired.get(0).id);
        assertEquals(Arrays.asList("abandoned-data"), expired.get(0).dataNames);
        assertNotNull(groups.get("slow"), "a transfer whose parts keep arriving is not abandoned");
        assertEquals(1, groups.expire(900 + IDLE + 1).size(), "it expires once its parts stop");
        assertEquals(0, groups.size());
    }

    @Test
    void theFilesOfAnExpiredGroupLeaveTheJournalDirectory() throws Exception {
        Path journal = Files.createTempDirectory("journal");
        Path d1 = Files.createDirectories(journal.resolve("d1")), d2 = Files.createDirectories(journal.resolve("d2"));
        Files.write(d1.resolve("d1.0"), new byte[64]);                 // one of two parts arrived
        Files.write(d2.resolve("d2.0"), new byte[64]);
        Files.write(d2.resolve("d2.1"), new byte[64]);
        Files.write(d2.resolve("d2.bin"), new byte[128]);              // this file was combined; the group never completed
        Path other = Files.createDirectories(journal.resolve("other"));
        Files.write(other.resolve("other.0"), new byte[64]);           // another transfer's part
        Path outside = Files.write(journal.getParent().resolve("outside-" + System.nanoTime()), new byte[8]);

        IncomingFileGroups groups = new IncomingFileGroups(8, 8, IDLE);
        groups.register("g", "alice", receiver("g", 2, "d1", "d2"), Arrays.asList("d1", "d2", "../" + outside.getFileName(), "missing"), 0);
        List<IncomingFileGroups.Group> expired = groups.expire(IDLE + 1);
        assertEquals(1, expired.size(), "an abandoned group's parts stayed in the journal directory for good");
        assertEquals(4, IncomingFileGroups.deleteFiles(expired.get(0), journal));
        assertFalse(Files.exists(d1), "parts left behind: " + d1);
        assertFalse(Files.exists(d2), "parts left behind: " + d2);
        assertTrue(Files.exists(other.resolve("other.0")), "another transfer's part was deleted");
        assertTrue(Files.exists(outside), "a name leading out of the journal directory was followed");
        Files.delete(outside);
    }

    @Test
    void theGroupsHeldAreBounded() {
        IncomingFileGroups groups = new IncomingFileGroups(4, 2, IDLE);
        assertNull(register(groups, "a1", "alice", 0));
        assertNull(register(groups, "a2", "alice", 0));
        assertNotNull(register(groups, "a3", "alice", 0), "one caller registered groups without limit");
        assertNull(register(groups, "b1", "bob", 0));
        assertNull(register(groups, "c1", "carol", 0));
        assertNotNull(register(groups, "d1", "dave", 0), "groups were held without limit");
        assertEquals(4, groups.size());
        groups.complete("a1", groups.get("a1"));
        assertNull(register(groups, "d1", "dave", 0), "a completed group frees its place");
        assertNull(register(groups, "a2", "alice", 5), "registering a held group again is no new group");

        // without broker security there is no caller to count by: only the total bounds
        IncomingFileGroups open = new IncomingFileGroups(4, 2, IDLE);
        for (int i = 0; i < 4; i++) assertNull(register(open, "g" + i, "", 0));
        assertNotNull(register(open, "g4", "", 0));
    }

    @Test
    void aGroupIdHeldByAnotherCallerIsRefused() {
        IncomingFileGroups groups = new IncomingFileGroups(8, 8, IDLE);
        assertNull(register(groups, "g", "alice", 0));
        FileObjectGroupReceiver alices = groups.get("g");
        String refused = register(groups, "g", "mallory", 1);
        assertNotNull(refused, "a second registration of a group id replaced the first one's owner");
        assertTrue(refused.contains("another caller"), refused);
        assertSame(alices, groups.get("g"), "the first registration stands");
        assertNull(register(groups, "g", "alice", 2), "its own caller may register it again (a transfer started over)");
        assertNotSame(alices, groups.get("g"));
        groups.complete("g", alices);
        assertEquals(1, groups.size(), "the transfer that was started over is not ended by the one it replaced");
    }

    @Test
    void aSweepIsDueOncePerInterval() {
        IncomingFileGroups groups = new IncomingFileGroups(8, 8, IDLE);
        long t = 10 * IncomingFileGroups.SWEEP_INTERVAL_MS;
        assertTrue(groups.sweepDue(t), "nothing ever looked for abandoned groups");
        assertFalse(groups.sweepDue(t + 1), "once per interval, not on every message");
        assertTrue(groups.sweepDue(t + IncomingFileGroups.SWEEP_INTERVAL_MS + 1));
    }

    @Test
    void theDefaultsAreBounded() {
        assertTrue(IncomingFileGroups.MAX_GROUPS > 0 && IncomingFileGroups.MAX_GROUPS <= 1024);
        assertTrue(IncomingFileGroups.MAX_GROUPS_PER_CALLER < IncomingFileGroups.MAX_GROUPS, "one caller cannot take every place");
        assertTrue(IncomingFileGroups.IDLE_EXPIRY_MS <= 60 * 60_000L);
    }
}
