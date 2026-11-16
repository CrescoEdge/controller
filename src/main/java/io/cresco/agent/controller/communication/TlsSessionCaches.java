package io.cresco.agent.controller.communication;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * The TLS session caches of every TLS context this node makes for its broker connections -- its broker's (the peers are
 * clients), each bridge's and each of its own links to a broker (the peers are servers) -- so that a session whose peer
 * this node's trust refuses NOW is not resumed.
 *
 * <p><b>Why.</b> A resumed session skips the trust manager: its peer was judged once, at the full handshake that made
 * the session. A peer whose key was revoked here, or whose anchors were removed and whose key is not bound here, was
 * still accepted when it resumed (TLS 1.3 tickets, TLS 1.2 session ids), for as long as the session lived (24 h). Here,
 * each time the trust changes (keys revoked, anchors removed, the truststore replaced), every cached session whose peer
 * chain carries one of the keys concerned is judged again by the trust as it stands, and invalidated when it is refused:
 * the peer's next connection makes a full handshake, judged like any new one.
 *
 * <p><b>Server tickets.</b> A JDK server hands out its TLS 1.3 tickets SEALED by default (stateless): the client holds
 * the session, the server keeps nothing to take back. It keeps them in its cache instead (stateful) for a session that
 * carries a value ({@code SSLSession.putValue}): {@link #judged} puts one on every session whose peer this node's trust
 * judged at its handshake, so the server's tickets for it are in the cache {@link #invalidate} sweeps -- and invalidating
 * a session invalidates the tickets made from it with it. A client's tickets are kept in its own cache in any case.
 *
 * <p>The contexts are held weakly: a context nobody uses any more (a closed link's) goes with its sessions. A sweep
 * reads every cached session of every registered context, and judges only those whose chain carries a key concerned:
 * it happens when the trust changes, never on a connection.
 */
final class TlsSessionCaches {

    /** The value {@link #judged} puts on a session (any value does: see the class comment). */
    static final String JUDGED = "io.cresco.tls.judged";

    /** Is {@code peerChain} refused now, judged as a peer that is a TLS client ({@code peerIsClient}) or server? */
    interface Refusal {
        boolean refused(X509Certificate[] peerChain, boolean peerIsClient);
    }

    private final Set<SSLContext> contexts = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /** Sweep {@code context}'s sessions from now on (held weakly). */
    void register(SSLContext context) {
        if (context != null) contexts.add(context);
    }

    int contexts() {
        synchronized (contexts) {
            return contexts.size();
        }
    }

    /** At a handshake this node's trust judged: keep the server's tickets of {@code handshakeSession} where they can be taken back. */
    static void judged(SSLSession handshakeSession) {
        if (handshakeSession == null) return;
        try {
            handshakeSession.putValue(JUDGED, Boolean.TRUE);
        } catch (RuntimeException ignore) {
            // a session that takes no values: its tickets stay as the JDK makes them
        }
    }

    /**
     * Invalidate every cached session, of every registered context, whose peer chain carries one of {@code keyFps} (SHA-256
     * of a SubjectPublicKeyInfo, hex; null: every session) and that {@code refusal} refuses now. How many were invalidated.
     */
    int invalidate(Collection<String> keyFps, Refusal refusal) {
        List<SSLContext> now;
        synchronized (contexts) {
            now = new ArrayList<>(contexts);
        }
        int n = 0;
        for (SSLContext c : now) {
            n += sweep(c.getServerSessionContext(), true, keyFps, refusal);
            n += sweep(c.getClientSessionContext(), false, keyFps, refusal);
        }
        return n;
    }

    /** As {@link #invalidate}, over one session cache whose peers are TLS clients ({@code peersAreClients}) or servers. */
    static int sweep(SSLSessionContext sessions, boolean peersAreClients, Collection<String> keyFps, Refusal refusal) {
        if (sessions == null) return 0;
        int n = 0;
        for (byte[] id : Collections.list(sessions.getIds())) {
            SSLSession s = sessions.getSession(id);
            if (s == null) continue;
            X509Certificate[] chain = peerChain(s);
            if (chain == null || (keyFps != null && !carriesAny(chain, keyFps))) continue;
            boolean refused;
            try {
                refused = refusal.refused(chain, peersAreClients);
            } catch (RuntimeException e) {
                refused = true;                                  // what cannot be judged now is not resumed
            }
            if (refused) {
                s.invalidate();
                n++;
            }
        }
        return n;
    }

    private static X509Certificate[] peerChain(SSLSession s) {
        try {
            Certificate[] peer = s.getPeerCertificates();
            List<X509Certificate> x = new ArrayList<>(peer.length);
            for (Certificate c : peer) if (c instanceof X509Certificate) x.add((X509Certificate) c);
            return x.isEmpty() ? null : x.toArray(new X509Certificate[0]);
        } catch (SSLPeerUnverifiedException none) {
            return null;                                         // no peer certificate: nothing of it to judge
        }
    }

    private static boolean carriesAny(X509Certificate[] chain, Collection<String> keyFps) {
        if (keyFps.isEmpty()) return false;
        for (X509Certificate c : chain) {
            if (c != null && keyFps.contains(PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()))) return true;
        }
        return false;
    }
}
