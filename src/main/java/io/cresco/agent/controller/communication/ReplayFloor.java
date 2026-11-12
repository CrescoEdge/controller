package io.cresco.agent.controller.communication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * What a node accepted before it restarted is not accepted again after it. The replay memory ({@link ReplayMemory}) is in
 * RAM: a node that restarted remembered nothing, so a signed message it had accepted could be delivered to it once more,
 * and accepted, for as long as its signature was young enough ({@code msg_signature_max_age_ms}, 5 minutes). (A message
 * under a session MAC cannot: the restarted node holds no session.)
 *
 * <p><b>The rule.</b> Per signer, the latest SIGNING TIME of a signed message this node accepted from that signer is kept
 * on disk. At the next start it is that signer's floor: a signed message of that signer whose signing time is at or
 * before it MAY have been accepted before the restart, and is refused. Only times of one clock are ever compared -- two
 * signing times of the same signer -- so no clock difference between nodes, and no step of this node's clock, can make a
 * message pass. A message signed after the last one this node accepted from its signer -- everything that signer signs
 * after this node went down, whether it is sent later or was stored by a broker meanwhile -- has a later signing time
 * and is never refused by this (unless the signer's own clock was set back past its last signature). What IS refused
 * that never arrived: a message its signer signed BEFORE one this node accepted from it, still on its way when the node
 * went down. The node cannot tell it from one it accepted, and refuses it.
 *
 * <p><b>Written through.</b> The record of a signing time is handed to the operating system before the message is
 * accepted ({@link #accepted}: one 24-byte append, a few microseconds, only for a signed message whose signing time is
 * later than its signer's last): nothing a process loses -- a kill, an out-of-memory error, an upgrade -- opens a
 * window. It is forced to the disk within a second by a background thread, so after a failure of the HOST (power, the
 * operating system) what was accepted in its last second may be accepted once more: that takes the host failing in that
 * second and a copy of such a message in a relay's hands, and a force per message would cost every signed message a disk
 * write.
 *
 * <p><b>Bounded.</b> A signer's record is needed only while a message at or before it could still be accepted: for the
 * signature's maximum age (and the allowance for a signer's clock being ahead) after it was written, by this node's own
 * clock both times. Older records are dropped when the file is rewritten: at start, and whenever it has grown past
 * {@value #ROTATE_BYTES} bytes (one record per signer then: 24 bytes each). At most {@value #SIGNERS_MAX} signers; past
 * that a new signer's time is not recorded (logged): its messages are accepted as before this existed.
 *
 * <p>No lock is held across anything but the file append; a storage error is logged (at most once a minute) and the node
 * goes on accepting, as it did before this existed -- a full disk must not take the control plane down.
 */
final class ReplayFloor implements AutoCloseable {

    static final int RECORD = 24;
    static final long ROTATE_BYTES = 8L << 20;
    static final int SIGNERS_MAX = 2 * ReplayMemory.SIGNERS_MAX;

    interface Log { void line(String l); }

    /** One signer: the floor restored at start (or none), the latest signing time written, and when (this node's clock). */
    private static final class Entry {
        final boolean restored;
        final long floor;
        volatile long latest;
        volatile long writtenAt;
        Entry(boolean restored, long floor, long latest, long writtenAt) {
            this.restored = restored; this.floor = floor; this.latest = latest; this.writtenAt = writtenAt;
        }
    }

    private final Path file;
    private final long keepMs, rotateBytes;
    private final Log log;
    private final ConcurrentHashMap<Long, Entry> signers = new ConcurrentHashMap<>();
    private final Object io = new Object();
    private final ByteBuffer one = ByteBuffer.allocateDirect(RECORD);
    private FileChannel channel;
    private long size;
    private volatile boolean dirty, closed;
    private volatile long lastErrorLog;
    /** The background thread: made at the first record, gone when idle (a node that accepts no signed message has none). */
    private ScheduledThreadPoolExecutor flusher;
    private boolean tickScheduled;
    private final java.util.concurrent.atomic.AtomicLong refusedCount = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Open (or begin) the record in {@code file}; every signer in it whose record is still needed becomes a floor.
     * {@code maxAgeMs}, {@code maxFutureMs}: how old, and how far ahead of this node's clock, a signature may be.
     */
    ReplayFloor(Path file, long maxAgeMs, long maxFutureMs, Log log) {
        this(file, log, maxAgeMs + maxFutureMs + 60_000L, ROTATE_BYTES);
    }

    /** As above, with the time a record is kept and the size the file is rewritten at given (tests). */
    ReplayFloor(Path file, Log log, long keepMs, long rotateBytes) {
        this.file = file;
        this.keepMs = keepMs;
        this.rotateBytes = rotateBytes;
        this.log = log;
        long now = System.currentTimeMillis();
        int restored = 0;
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            if (Files.exists(file)) {
                byte[] all = Files.readAllBytes(file);
                ByteBuffer b = ByteBuffer.wrap(all, 0, all.length - all.length % RECORD);     // a torn last record is nothing
                while (b.remaining() >= RECORD) {
                    long signer = b.getLong(), time = b.getLong(), at = b.getLong();
                    if (now - at > keepMs) continue;             // nothing at or before it could still be accepted
                    Entry had = signers.get(signer);
                    if (had == null || time > had.floor) signers.put(signer, new Entry(true, time, time, had == null ? at : Math.max(at, had.writtenAt)));
                }
                restored = signers.size();
            }
            synchronized (io) {
                rewrite();
            }
        } catch (IOException | RuntimeException e) {
            error("could not be opened (" + e + ")");
        }
        if (restored > 0 && log != null) {
            log.line("replay floor: " + restored + " signer(s) this node accepted signed messages from before it restarted; a message"
                    + " any of them signed at or before its last accepted one is refused (it may have been accepted then)");
        }
    }

    /** With the io lock: the background thread looks at the file a second from now (one look is scheduled at a time). */
    private void tickSoon() {
        if (tickScheduled || closed) return;
        if (flusher == null) {
            flusher = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "cresco-replay-floor");
                t.setDaemon(true);
                return t;
            });
            flusher.setKeepAliveTime(30, TimeUnit.SECONDS);
            flusher.allowCoreThreadTimeOut(true);
        }
        try {
            flusher.schedule(this::tick, 1, TimeUnit.SECONDS);
            tickScheduled = true;
        } catch (java.util.concurrent.RejectedExecutionException stopping) {
            // the node is stopping: close() forces what is written
        }
    }

    /**
     * May this node have accepted, before it restarted, a message {@code signer} signed at {@code signedAt}? True when it
     * accepted from that signer, in an earlier life, a message signed then or later. One map lookup.
     */
    boolean mayHaveAccepted(long signer, long signedAt) {
        Entry e = signers.get(signer);
        if (e == null || !e.restored || signedAt > e.floor) return false;
        refusedCount.incrementAndGet();
        return true;
    }

    /**
     * A signed message of {@code signer}, signed at {@code signedAt}, is being accepted ({@code now}: this node's clock).
     * When that is later than the latest time recorded for the signer, it is recorded before this returns -- in the
     * operating system's hands, so that no end of this process loses it.
     */
    void accepted(long signer, long signedAt, long now) {
        Entry e = signers.get(signer);
        if (e == null) {
            if (signers.size() >= SIGNERS_MAX) {
                error("holds " + SIGNERS_MAX + " signers: the signing time of another is not recorded");
                return;
            }
            Entry made = new Entry(false, Long.MIN_VALUE, Long.MIN_VALUE, now);
            e = signers.putIfAbsent(signer, made);
            if (e == null) e = made;
        }
        if (signedAt <= e.latest) return;
        synchronized (io) {
            if (signedAt <= e.latest) return;
            try {
                if (channel != null) {
                    one.clear();
                    one.putLong(signer).putLong(signedAt).putLong(now).flip();
                    while (one.hasRemaining()) channel.write(one);
                    size += RECORD;
                    dirty = true;
                }
            } catch (IOException | RuntimeException x) {
                error("could not be written (" + x + ")");
            }
            tickSoon();
            e.latest = signedAt;
            e.writtenAt = now;
        }
    }

    /** The background thread, a second after an append: what was appended reaches the disk; a file that has grown is rewritten. */
    void tick() {
        if (closed) return;
        try {
            synchronized (io) {
                tickScheduled = false;
                if (channel == null) {
                    rewrite();                                   // it could not be opened or written before: try again
                } else if (size > rotateBytes) {
                    rewrite();
                } else if (dirty) {
                    dirty = false;
                    channel.force(false);
                }
            }
        } catch (IOException | RuntimeException e) {
            error("could not be forced to disk (" + e + ")");
        }
    }

    /**
     * With the io lock: write one record per signer still needed into a new file, force it, and put it in the place of
     * the old one. A signer whose record is older than a signature may be (by this node's clock) is dropped: no message
     * at or before its time could still be accepted.
     */
    private void rewrite() throws IOException {
        long now = System.currentTimeMillis();
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        ByteBuffer b = ByteBuffer.allocate(Math.max(RECORD, Math.min(1 << 20, (signers.size() + 16) * RECORD)));
        try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ownerOnly(tmp);
            long written = 0;
            for (java.util.Iterator<Map.Entry<Long, Entry>> it = signers.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, Entry> s = it.next();
                Entry e = s.getValue();
                if (now - e.writtenAt > keepMs) {
                    it.remove();
                    continue;
                }
                long time = Math.max(e.latest, e.restored ? e.floor : Long.MIN_VALUE);
                if (time == Long.MIN_VALUE) continue;            // nothing accepted from it yet
                if (b.remaining() < RECORD) {
                    b.flip();
                    while (b.hasRemaining()) out.write(b);
                    b.clear();
                }
                b.putLong(s.getKey()).putLong(time).putLong(e.writtenAt);
                written += RECORD;
            }
            b.flip();
            while (b.hasRemaining()) out.write(b);
            out.force(true);
            FileChannel old = channel;
            channel = null;
            if (old != null) {
                try { old.close(); } catch (IOException ignore) { }
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            size = written;
            dirty = false;
        }
    }

    private static void ownerOnly(Path p) {
        try {
            Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-------"));
        } catch (IOException | UnsupportedOperationException ignore) {
            // not a POSIX file system: the directory's own protection stands
        }
    }

    private void error(String what) {
        long now = System.currentTimeMillis();
        if (log != null && now - lastErrorLog > 60_000L) {
            lastErrorLog = now;
            log.line("REPLAY FLOOR " + file + " " + what + ": until it is, a signed message this node accepted shortly before a"
                    + " restart could be accepted again after it (as before this record existed)");
        }
    }

    /** Signers recorded (floors and this life's); messages refused by a floor; the file's size (diagnostics, tests). */
    int signers() { return signers.size(); }
    long refused() { return refusedCount.get(); }
    long fileBytes() { synchronized (io) { return size; } }

    /** What is appended reaches the disk; the file is closed (the node stops). */
    @Override
    public void close() {
        closed = true;
        synchronized (io) {
            if (flusher != null) flusher.shutdownNow();
            try {
                if (channel != null) {
                    channel.force(false);
                    channel.close();
                }
            } catch (IOException ignore) {
                // the node is stopping
            } finally {
                channel = null;
            }
        }
    }
}
