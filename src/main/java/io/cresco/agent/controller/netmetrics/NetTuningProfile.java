package io.cresco.agent.controller.netmetrics;

import io.cresco.library.plugin.PluginBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The single, mutable source of truth for every end-to-end I/O tunable — socket buffer size, per-read
 * (block) size, write high-water mark, and per-link connection count. Seeded from static config, then
 * updated live by the {@link AutoTuner} from measured {@link LinkMetrics}. Every I/O site (wsapi Netty
 * server, stunnel channels, dataplane broker connections, inter-broker bridge) reads its current value
 * from here at (re)connect time and — where the transport allows — has running channels re-set live.
 *
 * In-controller consumers (dataplane/bridge) read this object directly. Out-of-controller plugins
 * (wsapi, stunnel) receive changes via a {@code nettuning} CONFIG MsgEvent carrying this snapshot, so
 * the whole fabric moves buffer/block sizes together. Bounds keep the auto-tuner from ever choosing a
 * pathological value.
 */
public class NetTuningProfile {

    // Live tunables (atomics: updated by the tuner thread, read by many I/O threads).
    private final AtomicInteger socketBufferBytes = new AtomicInteger();
    private final AtomicInteger readChunkBytes = new AtomicInteger();     // per-read / block size
    private final AtomicInteger writeHighWaterBytes = new AtomicInteger();
    private final AtomicInteger connectionsPerLink = new AtomicInteger();
    private final AtomicLong version = new AtomicLong();                  // bumps on every change

    // Bounds (the tuner never steps outside these).
    private final int minBuf, maxBuf, minChunk, maxChunk, minConns, maxConns;

    public NetTuningProfile(PluginBuilder plugin) {
        // seed from the same config keys the static I/O sites already use, so default == today's behavior
        this.socketBufferBytes.set(plugin.getConfig().getIntegerParam("net_socket_buffer_bytes", 4 * 1024 * 1024));
        this.readChunkBytes.set(plugin.getConfig().getIntegerParam("net_read_chunk_bytes", 256 * 1024));
        this.writeHighWaterBytes.set(plugin.getConfig().getIntegerParam("net_write_high_water_bytes", 2 * 1024 * 1024));
        this.connectionsPerLink.set(plugin.getConfig().getIntegerParam("net_connections_per_link", 1));
        this.minBuf = plugin.getConfig().getIntegerParam("net_socket_buffer_min", 256 * 1024);
        this.maxBuf = plugin.getConfig().getIntegerParam("net_socket_buffer_max", 32 * 1024 * 1024);
        this.minChunk = plugin.getConfig().getIntegerParam("net_read_chunk_min", 16 * 1024);
        this.maxChunk = plugin.getConfig().getIntegerParam("net_read_chunk_max", 4 * 1024 * 1024);
        this.minConns = plugin.getConfig().getIntegerParam("net_connections_min", 1);
        this.maxConns = plugin.getConfig().getIntegerParam("net_connections_max", 16);
    }

    public int getSocketBufferBytes() { return socketBufferBytes.get(); }
    public int getReadChunkBytes() { return readChunkBytes.get(); }
    public int getWriteHighWaterBytes() { return writeHighWaterBytes.get(); }
    public int getConnectionsPerLink() { return connectionsPerLink.get(); }
    public long getVersion() { return version.get(); }

    private int clamp(int v, int lo, int hi) { return Math.max(lo, Math.min(hi, v)); }

    /** Set the socket buffer size (clamped). Returns true if it actually changed. */
    public boolean setSocketBufferBytes(int v) {
        int nv = clamp(v, minBuf, maxBuf);
        boolean changed = socketBufferBytes.getAndSet(nv) != nv;
        if (changed) version.incrementAndGet();
        return changed;
    }

    /** Set the per-read/block size (clamped). Returns true if it actually changed. */
    public boolean setReadChunkBytes(int v) {
        int nv = clamp(v, minChunk, maxChunk);
        boolean changed = readChunkBytes.getAndSet(nv) != nv;
        if (changed) version.incrementAndGet();
        return changed;
    }

    public boolean setWriteHighWaterBytes(int v) {
        int nv = clamp(v, minChunk, maxBuf);
        boolean changed = writeHighWaterBytes.getAndSet(nv) != nv;
        if (changed) version.incrementAndGet();
        return changed;
    }

    public boolean setConnectionsPerLink(int v) {
        int nv = clamp(v, minConns, maxConns);
        boolean changed = connectionsPerLink.getAndSet(nv) != nv;
        if (changed) version.incrementAndGet();
        return changed;
    }

    public int getMaxConns() { return maxConns; }
    public int getMinConns() { return minConns; }

    /** Snapshot for propagation to out-of-controller plugins via a CONFIG MsgEvent. */
    public Map<String, String> snapshot() {
        Map<String, String> m = new HashMap<>();
        m.put("net_socket_buffer_bytes", Integer.toString(socketBufferBytes.get()));
        m.put("net_read_chunk_bytes", Integer.toString(readChunkBytes.get()));
        m.put("net_write_high_water_bytes", Integer.toString(writeHighWaterBytes.get()));
        m.put("net_connections_per_link", Integer.toString(connectionsPerLink.get()));
        m.put("net_tuning_version", Long.toString(version.get()));
        return m;
    }

    @Override
    public String toString() {
        return String.format("NetTuning[v%d sockBuf=%dKB readChunk=%dKB writeHi=%dKB conns=%d]",
                version.get(), socketBufferBytes.get() / 1024, readChunkBytes.get() / 1024,
                writeHighWaterBytes.get() / 1024, connectionsPerLink.get());
    }
}
