package io.cresco.agent.controller.netdiscovery;

import io.cresco.library.utilities.CLogger;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The discovery responder's work, off its I/O threads: a bounded queue per peer address, served in turn by a fixed pool.
 *
 * <p>Answering a discovery exchange checks certificates, signs (the production profile), and records the joiner durably
 * (a disk sync). That ran on the listener's I/O threads, and the only protection against a storm was a limit on open
 * connections per address (16), over which a connection was closed without a word -- so a host that starts hundreds of
 * agents had most of its requests dropped, each of them to come back 3 s later.
 *
 * <p>Here a request read by an I/O thread is queued under its peer's address and answered by one of {@code workers}
 * threads. The addresses with work waiting are served in turn (one request each), so an address that sends a thousand
 * requests waits behind itself, not in front of the others: it cannot keep another address from being answered, and it
 * cannot take more than its turn of the workers while others wait. The queue is bounded by time: a request whose turn
 * would come later than {@code maxWaitMs} is not queued; its peer is told at once that the responder is busy and after
 * how long to ask again ({@link #submit} returns that time). Nothing is dropped without an answer, and a peer that is
 * told to come back later costs the responder one small write.
 *
 * <p>What waits is also bounded in bytes, per address and in all: a request is a frame of up to 256 KiB that nobody has
 * authenticated yet, and a queue bounded only by count would let a few addresses hold gigabytes of them. A real request
 * is a few KiB (a hello with its certificate chain), so a host's thousand requests are far below the bound.
 *
 * <p>The cost of {@link #submit} and of taking the next request does not depend on how many are queued or how many
 * addresses wait.
 */
public final class DiscoveryJobs {

    public static final int DEFAULT_MAX_WAIT_MS = 5_000;       // discovery_max_wait_ms
    public static final long DEFAULT_MAX_BYTES_PER_ADDRESS = 32L << 20;   // discovery_max_queued_bytes_per_address
    public static final long DEFAULT_MAX_BYTES = 256L << 20;              // discovery_max_queued_bytes
    /** A retry time handed to a peer is never shorter or longer than this. */
    static final long MIN_RETRY_MS = 250, MAX_RETRY_MS = 30_000;

    private final int workers, maxQueued;
    private final long maxWaitNanos;
    private final CLogger logger;
    private final Object lock = new Object();
    private final HashMap<String, ArrayDeque<Job>> byAddress = new HashMap<>();        // with lock
    private final HashMap<String, long[]> bytesOf = new HashMap<>();                   // with lock: bytes waiting per address
    private long bytes;                                                                // with lock: bytes waiting in all
    private final long maxBytesPerAddress, maxBytes;

    private static final class Job {
        final Runnable run;
        final long weight;
        Job(Runnable run, long weight) { this.run = run; this.weight = weight; }
    }
    private final ArrayDeque<String> turn = new ArrayDeque<>();                        // with lock: addresses with work, in order
    private int queued;                                                                // with lock
    private boolean stopped;                                                           // with lock
    /** What one request takes a worker, smoothed (ns). Starts at a guess; a request's own time replaces it gradually. */
    private volatile long serviceNanos = 5_000_000L;
    private final Thread[] threads;
    private final AtomicLong accepted = new AtomicLong(), busy = new AtomicLong(), served = new AtomicLong(), failed = new AtomicLong();
    private volatile int maxQueuedSeen;

    /**
     * @param workers   threads that answer requests (at least 1)
     * @param maxWaitMs a request whose turn would come later than this is answered "busy" instead of queued
     * @param maxQueued requests held at most, whatever the wait (memory)
     */
    public DiscoveryJobs(int workers, int maxWaitMs, int maxQueued, CLogger logger) {
        this(workers, maxWaitMs, maxQueued, DEFAULT_MAX_BYTES_PER_ADDRESS, DEFAULT_MAX_BYTES, logger);
    }

    /** As above; with the most bytes of requests one address, and all addresses, may have waiting. */
    public DiscoveryJobs(int workers, int maxWaitMs, int maxQueued, long maxBytesPerAddress, long maxBytes, CLogger logger) {
        this.maxBytesPerAddress = Math.max(1, maxBytesPerAddress);
        this.maxBytes = Math.max(1, maxBytes);
        this.workers = Math.max(1, workers);
        this.maxWaitNanos = Math.max(1, maxWaitMs) * 1_000_000L;
        this.maxQueued = Math.max(1, maxQueued);
        this.logger = logger;
        this.threads = new Thread[this.workers];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(this::work, "discovery-join-" + i);
            threads[i].setDaemon(true);
            threads[i].start();
        }
    }

    /**
     * Queue {@code job} for {@code address}.
     *
     * @return 0 when it is queued (it will run); otherwise it is NOT queued and the peer should be told to ask again after
     * this many milliseconds (the responder is busy, or stopped)
     */
    public long submit(String address, Runnable job) {
        return submit(address, 0, job);
    }

    /** As above; {@code weight}: the bytes the request holds while it waits (its frame). */
    public long submit(String address, long weight, Runnable job) {
        String a = address == null ? "?" : address;
        long w = Math.max(0, weight);
        synchronized (lock) {
            if (stopped) return MAX_RETRY_MS;
            long wait = waitNanosLocked(a);
            long[] held = bytesOf.get(a);
            if (queued >= maxQueued || wait > maxWaitNanos || bytes + w > maxBytes || (held == null ? 0 : held[0]) + w > maxBytesPerAddress) {
                busy.incrementAndGet();
                return Math.max(MIN_RETRY_MS, Math.min(MAX_RETRY_MS, wait / 1_000_000L));
            }
            ArrayDeque<Job> q = byAddress.get(a);
            if (q == null) {
                q = new ArrayDeque<>();
                byAddress.put(a, q);
                turn.addLast(a);
            }
            q.addLast(new Job(job, w));
            if (w > 0) {
                if (held == null) bytesOf.put(a, held = new long[1]);
                held[0] += w;
                bytes += w;
            }
            queued++;
            if (queued > maxQueuedSeen) maxQueuedSeen = queued;
            accepted.incrementAndGet();
            lock.notify();
            return 0;
        }
    }

    /**
     * With the lock: how long a request of {@code address} queued now would wait for a worker. The addresses are served
     * in turn, so it waits for its own address's earlier requests, each of which takes one round of the addresses that
     * wait -- but never for more than everything that is queued.
     */
    private long waitNanosLocked(String address) {
        ArrayDeque<Job> q = byAddress.get(address);
        long own = q == null ? 0 : q.size();
        long rounds = own * (long) Math.max(1, byAddress.size()) + (q == null ? byAddress.size() : 0);
        long ahead = Math.min(rounds, queued);
        return ahead * serviceNanos / workers;
    }

    private void work() {
        while (true) {
            Runnable job;
            synchronized (lock) {
                while (queued == 0 && !stopped) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (queued == 0) return;                      // stopped and drained
                String a = turn.pollFirst();
                ArrayDeque<Job> q = byAddress.get(a);
                Job j = q.pollFirst();
                job = j.run;
                if (q.isEmpty()) byAddress.remove(a); else turn.addLast(a);
                if (j.weight > 0) {
                    long[] held = bytesOf.get(a);
                    if (held != null && (held[0] -= j.weight) <= 0) bytesOf.remove(a);
                    bytes -= j.weight;
                }
                queued--;
            }
            long t0 = System.nanoTime();
            try {
                job.run();
            } catch (Throwable t) {
                failed.incrementAndGet();
                DiscoveryGuard.warnLimited(logger, "discovery-job", "Discovery: a request failed: " + t);
            }
            long took = System.nanoTime() - t0;
            serviceNanos = Math.max(50_000L, (serviceNanos * 7 + took) / 8);
            served.incrementAndGet();
        }
    }

    /** Stop taking requests; what is queued is still answered, then the workers end. */
    public void shutdown() {
        synchronized (lock) {
            stopped = true;
            lock.notifyAll();
        }
    }

    /** Requests waiting now. */
    public int queued() { synchronized (lock) { return queued; } }

    /** Bytes of requests waiting now. */
    public long queuedBytes() { synchronized (lock) { return bytes; } }

    /** Addresses with a request waiting now. */
    public int addressesWaiting() { synchronized (lock) { return byAddress.size(); } }

    /** Requests queued so far, told "busy" so far, answered so far, and that failed with an exception. */
    public long accepted() { return accepted.get(); }
    public long busy() { return busy.get(); }
    public long served() { return served.get(); }
    public long failed() { return failed.get(); }

    /** The most requests ever waiting at once. */
    public int maxQueuedSeen() { return maxQueuedSeen; }

    /** What one request takes a worker, smoothed, in microseconds. */
    public long serviceMicros() { return serviceNanos / 1_000L; }

    public int workers() { return workers; }
}
