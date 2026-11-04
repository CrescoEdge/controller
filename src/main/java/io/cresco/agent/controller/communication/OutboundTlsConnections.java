package io.cresco.agent.controller.communication;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.lang.ref.WeakReference;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * The TLS connections THIS node opened (its broker's bridges to other brokers, its client connections to its controller),
 * with the certificate chain each peer presented: what a revocation must tear down besides the connections others opened
 * to this node's broker. The trust manager of every outbound connection ({@link PeerIdentityTrustManager}) records the
 * connection when its handshake is accepted; {@link #closeRefused} closes each one whose peer is refused now (its key
 * revoked, its name tombstoned, its region's CA revoked), after invalidating its TLS session so a reconnect cannot resume
 * it without the certificate being checked again. A bridge or failover transport then reconnects, and the new handshake is
 * refused by the trust manager.
 *
 * <p>Held weakly and pruned as connections close, so it never keeps a socket alive; bounded ({@link #MAX}).
 */
public final class OutboundTlsConnections {

    static final int MAX = 8192;

    private static final class Entry {
        final WeakReference<Object> conn;      // SSLSocket or SSLEngine
        final X509Certificate[] chain;
        final String endpoint;
        Entry(Object conn, X509Certificate[] chain, String endpoint) { this.conn = new WeakReference<>(conn); this.chain = chain; this.endpoint = endpoint; }
    }

    private final List<Entry> entries = new ArrayList<>();

    /** A handshake on {@code conn} (an SSLSocket or SSLEngine) accepted the peer's {@code chain}. */
    public void record(Object conn, X509Certificate[] chain, String endpoint) {
        if (conn == null || chain == null || chain.length == 0) return;
        synchronized (entries) {
            if (entries.size() >= MAX / 2) prune();
            if (entries.size() >= MAX) entries.remove(0);
            entries.add(new Entry(conn, chain.clone(), endpoint));
        }
    }

    private void prune() {
        for (Iterator<Entry> it = entries.iterator(); it.hasNext(); ) {
            Object c = it.next().conn.get();
            if (c == null || (c instanceof Socket && ((Socket) c).isClosed())
                    || (c instanceof SSLEngine && ((SSLEngine) c).isOutboundDone())) it.remove();
        }
    }

    /** How many live connections are recorded (tests, health). */
    public int size() {
        synchronized (entries) {
            prune();
            return entries.size();
        }
    }

    /**
     * Close every recorded connection whose peer chain {@code refusal} refuses now (returns why, or null to keep it).
     *
     * @return the descriptions of the connections closed
     */
    public List<String> closeRefused(Function<X509Certificate[], String> refusal) {
        List<Entry> doomed = new ArrayList<>();
        List<String> why = new ArrayList<>();
        synchronized (entries) {
            prune();
            for (Iterator<Entry> it = entries.iterator(); it.hasNext(); ) {
                Entry e = it.next();
                String r = refusal.apply(e.chain);
                if (r != null) { doomed.add(e); why.add(e.endpoint + ": " + r); it.remove(); }
            }
        }
        for (Entry e : doomed) close(e.conn.get());
        return why;
    }

    private static void close(Object c) {
        try {
            if (c instanceof SSLSocket) {
                SSLSession s = ((SSLSocket) c).getSession();
                if (s != null) s.invalidate();                  // no resumption without the certificate being checked again
                ((SSLSocket) c).close();
            } else if (c instanceof Socket) {
                ((Socket) c).close();
            } else if (c instanceof SSLEngine) {
                SSLEngine e = (SSLEngine) c;
                SSLSession s = e.getSession();
                if (s != null) s.invalidate();
                e.closeOutbound();                               // the transport fails at its next read or write
                try { e.closeInbound(); } catch (Exception ignore) { }
            }
        } catch (Exception ignore) {
            // already closed
        }
    }
}
