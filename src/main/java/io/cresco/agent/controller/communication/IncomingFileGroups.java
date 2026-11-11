package io.cresco.agent.controller.communication;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The file groups a node is receiving ({@link AgentConsumer}): per group its receiver -- the files and parts expected,
 * with the group's MsgEvent -- from the registration until the group completes.
 *
 * <p>An entry used to stay for good, completed or abandoned, and an abandoned transfer's parts with it in the dataplane
 * journal directory; a caller could register groups without limit; and a second registration of a group id replaced
 * the first one's owner. Now a group leaves when it completes; one that receives no part for {@link #IDLE_EXPIRY_MS}
 * expires and its files are deleted ({@link #deleteFiles}); at most {@link #MAX_GROUPS} are held, and at most
 * {@link #MAX_GROUPS_PER_CALLER} of one verified caller; a group id held by another caller is refused.
 */
final class IncomingFileGroups {

    static final int MAX_GROUPS = 256, MAX_GROUPS_PER_CALLER = 64;
    static final long IDLE_EXPIRY_MS = 30 * 60_000L, SWEEP_INTERVAL_MS = 60_000L;

    /** One group being received. */
    static final class Group {
        final String id;
        /** The principal of the caller that registered it; empty where callers are not verified (no broker security). */
        final String owner;
        final FileObjectGroupReceiver receiver;
        /** Its files' data names: each a directory of the journal directory, holding that file's parts. */
        final List<String> dataNames;
        private long lastActive;

        Group(String id, String owner, FileObjectGroupReceiver receiver, Collection<String> dataNames, long now) {
            this.id = id;
            this.owner = owner;
            this.receiver = receiver;
            this.dataNames = new ArrayList<>(dataNames);
            this.lastActive = now;
        }
    }

    private final Map<String, Group> groups = new HashMap<>();
    private final int maxGroups, maxPerCaller;
    private final long idleExpiryMs;
    private volatile long nextSweep;

    IncomingFileGroups() {
        this(MAX_GROUPS, MAX_GROUPS_PER_CALLER, IDLE_EXPIRY_MS);
    }

    IncomingFileGroups(int maxGroups, int maxPerCaller, long idleExpiryMs) {
        this.maxGroups = maxGroups;
        this.maxPerCaller = maxPerCaller;
        this.idleExpiryMs = idleExpiryMs;
    }

    /**
     * Register the group {@code id} for {@code owner}. Its own caller may register a held group again (a transfer
     * started over).
     *
     * @return null when registered, else why it was refused
     */
    synchronized String register(String id, String owner, FileObjectGroupReceiver receiver, Collection<String> dataNames, long now) {
        if (owner == null) owner = "";
        Group held = groups.get(id);
        if (held != null && !held.owner.equals(owner)) {
            return "the group id is held by another caller";
        }
        if (held == null) {
            if (groups.size() >= maxGroups) {
                return "this node is receiving " + groups.size() + " file groups, the most it holds";
            }
            if (!owner.isEmpty()) {
                int own = 0;
                for (Group g : groups.values()) if (g.owner.equals(owner)) own++;
                if (own >= maxPerCaller) {
                    return "the caller has " + own + " file groups in transfer, the most one caller may";
                }
            }
        }
        groups.put(id, new Group(id, owner, receiver, dataNames, now));
        return null;
    }

    /** The receiver of the held group {@code id}, or null. */
    synchronized FileObjectGroupReceiver get(String id) {
        Group g = groups.get(id);
        return g == null ? null : g.receiver;
    }

    /** As {@link #get}, for a part that arrived: the group is active as of {@code now}. */
    synchronized FileObjectGroupReceiver active(String id, long now) {
        Group g = groups.get(id);
        if (g == null) return null;
        g.lastActive = now;
        return g.receiver;
    }

    /** The group {@code id} completed with {@code receiver}: it is no longer held. */
    synchronized void complete(String id, FileObjectGroupReceiver receiver) {
        Group g = groups.get(id);
        if (g != null && g.receiver == receiver) groups.remove(id);
    }

    /** Remove and return the groups that received no part for the idle time: abandoned transfers. */
    synchronized List<Group> expire(long now) {
        List<Group> expired = new ArrayList<>();
        for (Iterator<Group> it = groups.values().iterator(); it.hasNext(); ) {
            Group g = it.next();
            if (now - g.lastActive > idleExpiryMs) {
                it.remove();
                expired.add(g);
            }
        }
        return expired;
    }

    /** Whether to look for abandoned groups now: true once per {@link #SWEEP_INTERVAL_MS}. Costs nothing otherwise. */
    boolean sweepDue(long now) {
        if (now < nextSweep) return false;
        synchronized (this) {
            if (now < nextSweep) return false;
            nextSweep = now + SWEEP_INTERVAL_MS;
            return true;
        }
    }

    synchronized int size() {
        return groups.size();
    }

    /**
     * Delete what an expired group left in the journal directory: the files in the directory of each of its data names
     * (the parts that arrived, a file already combined) and the directory itself. A data name that is not a plain
     * name inside the journal directory is skipped.
     *
     * @return the number of files deleted
     */
    static int deleteFiles(Group group, Path journalBase) {
        int deleted = 0;
        for (String dataName : group.dataNames) {
            try {
                Path dir = AgentConsumer.resolveInJournal(journalBase, dataName);
                if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) continue;
                try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
                    for (Path file : files) {
                        if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) && Files.deleteIfExists(file)) deleted++;
                    }
                }
                Files.delete(dir);
            } catch (IOException skipped) {
                // an unsafe name, or a directory that holds something else: left as it is
            }
        }
        return deleted;
    }
}
