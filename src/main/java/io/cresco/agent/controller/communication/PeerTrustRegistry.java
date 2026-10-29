package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;

import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What this node learned about its peers over the discovery-secret-authenticated certificate exchange, used
 * to bind TLS connections to identities (see {@link PeerIdentity}). In memory only: every connection is
 * preceded by a discovery exchange with its peer, which re-learns both facts after a restart.
 *
 * <ul>
 *   <li><b>Pinned keys.</b> In the legacy per-node trust mode the discovery response carries the peer's own
 *       certificate chain; the leaf key is recorded against the identity the leaf names. A later connection to
 *       that identity must present that key. A re-exchange replaces the pin (a restarted peer has new keys).</li>
 *   <li><b>Bridge peers.</b> Region and global controllers that completed a REGION or GLOBAL discovery exchange
 *       with this node (they hold the region/global discovery secret, which leaf agents do not). Only these may
 *       open a broker-to-broker bridge into this node's broker; see {@link CrescoAuthorizationBroker}.</li>
 * </ul>
 */
public final class PeerTrustRegistry {

    private final Map<PeerIdentity.Expected, String> pins = new ConcurrentHashMap<>();
    private final Map<PeerIdentity.Expected, Long> bridgePeers = new ConcurrentHashMap<>();

    /**
     * Pin the leaf key of a peer learned over authenticated discovery. The pin is recorded only when the leaf
     * names the identity discovery reported ({@code region}/{@code agent}); otherwise nothing is pinned and the
     * mismatch is returned so the caller can log it (the connection's name check will then refuse it).
     *
     * @return null when pinned, else why not
     */
    public String pin(String region, String agent, X509Certificate leaf) {
        if (region == null || agent == null || leaf == null) return "missing identity or certificate";
        CrescoIdentity id = CrescoIdentity.fromCertificate(leaf);
        if (id == null || !region.equals(id.getRegion()) || !agent.equals(id.getAgent())) {
            return "the certificate names " + (id == null ? "no Cresco identity" : ("region=" + id.getRegion() + ", agent=" + id.getAgent()))
                    + ", not region=" + region + ", agent=" + agent;
        }
        pins.put(new PeerIdentity.Expected(region, agent), PeerIdentity.spkiFingerprint(leaf));
        return null;
    }

    /** The pinned key fingerprint for {@code peer}, or null when none was learned. */
    public String pinnedKey(PeerIdentity.Expected peer) {
        return peer == null ? null : pins.get(peer);
    }

    /** Record a controller that may bridge into this node's broker. */
    public void allowBridgePeer(String region, String agent) {
        if (region == null || agent == null || region.isEmpty() || agent.isEmpty()) return;
        bridgePeers.put(new PeerIdentity.Expected(region, agent), System.currentTimeMillis());
    }

    /** Is {@code id} a controller recorded by {@link #allowBridgePeer}? */
    public boolean isBridgePeer(CrescoIdentity id) {
        if (id == null || id.getRegion() == null || id.getAgent() == null) return false;
        return bridgePeers.containsKey(new PeerIdentity.Expected(id.getRegion(), id.getAgent()));
    }

    int pinCount() { return pins.size(); }

    int bridgePeerCount() { return bridgePeers.size(); }
}
