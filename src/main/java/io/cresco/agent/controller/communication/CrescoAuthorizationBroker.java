package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.security.SecurityContext;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ActiveMQ broker plugin that enforces Cresco tenant isolation — "who can see what, who can do what" —
 * on every consumer, producer, and send. The decision logic lives in {@link TenantPolicy}; this class is
 * the adapter that (a) decides whether a connection is subject to enforcement and (b) resolves the
 * connection's asserted {@link CrescoIdentity}.
 *
 * <p>Enforcement scope:
 * <ul>
 *   <li><b>Local in-JVM controller</b> ({@code vm://} connection) — exempt (full rights). This bounds the
 *       blast radius: a misconfigured ACL can never lock a controller out of its own broker.</li>
 *   <li><b>Broker-to-broker bridge</b> ({@code isNetworkConnection()}) — exempt; cross-region tenant
 *       filtering is done by the bridge's included/excluded destinations, not here. Any client can CLAIM to be a
 *       bridge (it only has to send a BrokerInfo), so under mutual TLS the claim must be backed by a certificate
 *       naming a controller that completed a REGION/GLOBAL discovery exchange with this node
 *       ({@link PeerTrustRegistry#isBridgePeer}); a claim from anyone else is refused at connect
 *       ({@code peer_identity_mode=enforce}) and never earns the exemption. Without client certificates
 *       (broker_require_client_auth=false) a bridge claim cannot be verified and is exempt, as before.</li>
 *   <li><b>External client</b> — its identity is taken from the connection principal (the JMS username
 *       {@code tenant|region|agent}, or a certificate subject DN under mutual TLS) and checked against
 *       {@link TenantPolicy}. A client with no resolvable identity is denied.</li>
 * </ul>
 * Gated by {@code broker_security_enabled} at the wiring site ({@code ActiveBroker}); when off, the
 * plugin is never installed and the broker behaves exactly as before.
 *
 * <p>It is also the broker's {@link MessageAuthorizationPolicy} ({@code broker_stream_recipient_binding}, default
 * on): a dataplane message bound to a recipient ({@link StreamRecipientPolicy#TO}) is dispatched to a client
 * subscription only when the client is that recipient, and a subscription that asks for another identity's stream
 * is refused.
 */
public class CrescoAuthorizationBroker implements BrokerPlugin, MessageAuthorizationPolicy {

    private final CLogger logger;
    private final PeerTrustRegistry peers;
    private final PeerIdentity.Mode peerMode;
    // identity of each client connection, resolved once at connect (dispatch checks run per message)
    private final Map<ConnectionId, CrescoIdentity> identities = new ConcurrentHashMap<>();
    // connections that sent a BrokerInfo whose certificate names a verified bridge peer
    private final Set<ConnectionId> verifiedBridges = ConcurrentHashMap.newKeySet();
    // connections that presented a TLS client certificate (whatever it names): their bridge claims are checkable
    private final Set<ConnectionId> presentedCert = ConcurrentHashMap.newKeySet();
    private final AtomicLong withheld = new AtomicLong();
    private volatile long lastWithheldLog;
    private volatile boolean warnedUnverifiableBridge;
    private final Set<String> sharedPrefixes;
    private final Set<String> superuserTenants;
    private final java.util.List<TenantPolicy.CrossTenantSink> crossTenantSinks;
    private final boolean logAllow;
    private final boolean recipientBinding;

    public CrescoAuthorizationBroker(PluginBuilder plugin) {
        this(plugin, new PeerTrustRegistry());
    }

    /** @param peers the controllers allowed to bridge into this broker (learned in discovery) */
    public CrescoAuthorizationBroker(PluginBuilder plugin, PeerTrustRegistry peers) {
        this.logger = plugin.getLogger(CrescoAuthorizationBroker.class.getName(), CLogger.Level.Info);
        this.peers = (peers != null) ? peers : new PeerTrustRegistry();
        this.peerMode = PeerIdentity.Mode.parse(plugin.getConfig().getStringParam(PeerIdentity.MODE_KEY, "enforce"));
        String shared = plugin.getConfig().getStringParam("broker_shared_destinations",
                "agent.event,region.event,global.event");
        this.sharedPrefixes = new HashSet<>();
        for (String s : shared.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) this.sharedPrefixes.add(t);
        }
        // Tenants whose network clients are granted the cross-tenant SUPERUSER role (god view). The local
        // controller vm:// and broker bridges are already exempt; this is for an explicit superuser CLIENT
        // (e.g. a fabric-admin dashboard) that connects over the network and must see every tenant.
        String su = plugin.getConfig().getStringParam("broker_superuser_tenants", "cresco-system");
        this.superuserTenants = new HashSet<>();
        for (String s : su.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) this.superuserTenants.add(t);
        }
        this.logAllow = plugin.getConfig().getBooleanParam("broker_security_log_allow", false);
        this.recipientBinding = plugin.getConfig().getBooleanParam("broker_stream_recipient_binding", true);
        // W-GFS-1: named cross-tenant sinks (see TenantPolicy.CrossTenantSink). Empty by default -> the
        // strict own-subtree rule, exactly as before.
        this.crossTenantSinks = TenantPolicy.CrossTenantSink.parse(
                plugin.getConfig().getStringParam("broker_cross_tenant_sinks", ""));
        logger.info("Cresco tenant authorization active. shared destinations=" + this.sharedPrefixes
                + " superuser tenants=" + this.superuserTenants + " cross-tenant sinks=" + this.crossTenantSinks
                + " stream recipient binding=" + this.recipientBinding + " " + PeerIdentity.MODE_KEY + "=" + this.peerMode);
    }

    /** Authorization role for a resolved identity: SUPERUSER for a configured superuser tenant, else TENANT. */
    private TenantPolicy.Role roleOf(CrescoIdentity id) {
        if (id != null && id.getTenant() != null && superuserTenants.contains(id.getTenant())) {
            return TenantPolicy.Role.SUPERUSER;
        }
        return TenantPolicy.Role.TENANT;
    }

    @Override
    public Broker installPlugin(Broker next) {
        return new AuthFilter(next);
    }

    /**
     * Identity of a connection. Prefers the cert-bound principal set in {@link #addConnection}
     * (the SecurityContext username = the validated certificate DN — non-spoofable), and falls back
     * to the self-asserted JMS username "tenant|region|agent" only when no client certificate is
     * present (mutual TLS off).
     */
    private CrescoIdentity identityOf(ConnectionContext ctx) {
        if (ctx != null && ctx.getConnectionId() != null) {
            CrescoIdentity cached = identities.get(ctx.getConnectionId());
            if (cached != null) return cached;
        }
        return resolveIdentity(ctx);
    }

    private CrescoIdentity resolveIdentity(ConnectionContext ctx) {
        String u = null;
        // A client that presented a certificate is identified by it alone: one that names no Cresco identity has no
        // identity, never the username it asserts (that fallback is only for connections without client certificates).
        if (ctx != null && !(ctx.getSecurityContext() instanceof CertSecurityContext)
                && ctx.getConnectionId() != null && presentedCert.contains(ctx.getConnectionId())) {
            return null;
        }
        if (ctx != null) {
            if (ctx.getSecurityContext() != null) {
                u = ctx.getSecurityContext().getUserName();
            }
            if (u == null || u.isEmpty()) {
                u = ctx.getUserName();
            }
        }
        if (u == null || u.isEmpty()) {
            return null;
        }
        if (u.contains("CN=") || u.contains("OU=") || u.contains("O=")) {
            return CrescoIdentity.fromDN(u);
        }
        String[] p = u.split("\\|");
        if (p.length >= 3) {
            return CrescoIdentity.of(p[0], p[1], p[2], p.length >= 4 ? p[3] : null);
        }
        return null;
    }

    /** Trusted infrastructure connections (local controller vm://, or a broker bridge) are exempt. */
    private boolean isTrustedInfra(ConnectionContext ctx) {
        try {
            if (ctx == null) return true;
            if (ctx.isNetworkConnection()) return isTrustedBridge(ctx); // broker-to-broker bridge (verified)
            if (ctx.getConnector() == null) return true;     // broker-internal
            Connection c = ctx.getConnection();
            String ra = (c != null) ? c.getRemoteAddress() : null;
            if (ra != null && ra.startsWith("vm")) return true; // local in-JVM controller
        } catch (Exception ignore) {
            // fall through -> not trusted -> enforced
        }
        return false;
    }

    /**
     * A connection that claimed to be a broker bridge. With a client certificate the claim counts only for a verified
     * bridge peer (or under peer_identity_mode warn/off); a late claim (BrokerInfo after ConnectionInfo) is judged here
     * too, so it cannot buy the exemption either. Without a certificate the claim cannot be checked: exempt, as before.
     */
    private boolean isTrustedBridge(ConnectionContext ctx) {
        ConnectionId cid = ctx.getConnectionId();
        if (cid != null && verifiedBridges.contains(cid)) return true;
        if (cid == null || !presentedCert.contains(cid)) return true; // no client certificate: unverifiable, as before
        if (peerMode != PeerIdentity.Mode.ENFORCE) return true;
        CrescoIdentity id = identityOf(ctx);                          // certificate-derived (see resolveIdentity)
        if (peers.isBridgePeer(id)) {
            if (cid != null) verifiedBridges.add(cid);
            return true;
        }
        return false;
    }

    private void enforce(ConnectionContext ctx, ActiveMQDestination dest, TenantPolicy.Access access) {
        if (dest == null) {
            return;
        }
        if (isTrustedInfra(ctx)) {
            return;
        }
        String name = dest.getPhysicalName();
        CrescoIdentity id = identityOf(ctx);
        TenantPolicy.Decision d = TenantPolicy.check(id, name, access, sharedPrefixes, roleOf(id), crossTenantSinks);
        if (!d.allowed) {
            String who = (id != null) ? id.toString() : ("username=" + (ctx != null ? ctx.getUserName() : "?"));
            logger.warn("DENY " + access + " '" + name + "' for " + who + " : " + d.reason);
            throw new SecurityException("Cresco tenant policy: " + access + " denied on '" + name + "' (" + d.reason + ")");
        } else if (logAllow) {
            logger.info("ALLOW " + access + " '" + name + "' : " + d.reason);
        }
    }

    /** SecurityContext carrying the certificate-derived principal (username = validated cert DN). */
    private static final class CertSecurityContext extends SecurityContext {
        CertSecurityContext(String userName) { super(userName); }
        @Override public Set<Principal> getPrincipals() { return Collections.emptySet(); }
    }

    // ---- MessageAuthorizationPolicy: recipient-bound dataplane messages (StreamRecipientPolicy) ----

    @Override
    public boolean isAllowedToConsume(ConnectionContext context, Message message) {
        String to;
        String toTenant;
        try {
            Object p = message.getProperty(StreamRecipientPolicy.TO);
            if (p == null) return true;                       // not recipient-bound: the common case, no further work
            to = p.toString();
            Object t = message.getProperty(StreamRecipientPolicy.TO_TENANT);
            toTenant = (t == null) ? null : t.toString();
        } catch (Exception e) {
            return false;                                     // unreadable properties on a secured broker: withhold
        }
        if (isTrustedInfra(context)) return true;             // own controller (vm://) or a verified bridge forwards it
        TenantPolicy.Decision d = StreamRecipientPolicy.mayReceive(identityOf(context), to, toTenant);
        if (!d.allowed) {
            long n = withheld.incrementAndGet();
            long now = System.currentTimeMillis();
            if (now - lastWithheldLog > 10_000L) {            // one line per 10 s, with the running count
                lastWithheldLog = now;
                logger.warn("WITHHELD recipient-bound message on '" + (message.getDestination() == null ? "?" : message.getDestination().getPhysicalName())
                        + "' from " + identityOf(context) + " : " + d.reason + " (" + n + " withheld so far)");
            }
        }
        return d.allowed;
    }

    private void forget(ConnectionId cid) {
        if (cid == null) return;
        identities.remove(cid);
        verifiedBridges.remove(cid);
        presentedCert.remove(cid);
    }

    /** Recipient-bound messages withheld from non-recipients so far. */
    public long withheldCount() {
        return withheld.get();
    }

    private final class AuthFilter extends BrokerFilter {
        AuthFilter(Broker next) { super(next); }

        @Override
        public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
            // Cryptographic identity binding: when mutual TLS captured a validated client certificate
            // chain (the broker's trust managers already vouched for it during the handshake), derive
            // the principal from the certificate DN and make it authoritative. This overrides any
            // client-asserted username, so a client cannot spoof its tenant/region/agent — it would
            // need the private key of a certificate the broker trusts.
            boolean certificate = false;
            try {
                Object tc = info.getTransportContext();
                if (tc instanceof X509Certificate[]) {
                    X509Certificate[] chain = (X509Certificate[]) tc;
                    if (chain.length > 0) {
                        certificate = true;
                        CrescoIdentity id = CrescoIdentity.fromCertificate(chain[0]);
                        if (id != null && id.getTenant() != null) {
                            String dn = id.toX500Name();
                            info.setUserName(dn);
                            context.setSecurityContext(new CertSecurityContext(dn));
                            logger.info("mTLS identity bound from certificate: " + id
                                    + " (authoritative; overrides any asserted username)");
                        }
                    }
                }
            } catch (Exception ex) {
                logger.warn("addConnection: certificate identity extraction failed: " + ex.getMessage());
            }
            // Bridge claim (BrokerInfo before ConnectionInfo, as every real bridge does): judge it now so an impostor
            // is refused outright with a clear reason instead of merely losing the exemption.
            if (certificate && info.getConnectionId() != null) presentedCert.add(info.getConnectionId());
            if (context.isNetworkConnection()) {
                if (certificate) {
                    // a certificate naming no Cresco identity resolves to null here and is refused like any non-peer
                    CrescoIdentity id = (context.getSecurityContext() instanceof CertSecurityContext) ? resolveIdentity(context) : null;
                    if (peers.isBridgePeer(id)) {
                        if (info.getConnectionId() != null) verifiedBridges.add(info.getConnectionId());
                        logger.info("Broker bridge from verified controller " + id);
                    } else if (peerMode == PeerIdentity.Mode.ENFORCE) {
                        logger.error("BRIDGE REFUSED: a connection claiming to be a broker bridge presented " + id
                                + ", which is not a region/global controller that completed discovery with this node ("
                                + PeerIdentity.MODE_KEY + "=enforce)");
                        throw new SecurityException("Cresco: broker bridge refused for " + id + " (not a discovered controller)");
                    } else if (peerMode == PeerIdentity.Mode.WARN) {
                        logger.warn("Broker bridge from " + id + " is not a discovered controller -- ACCEPTED because "
                                + PeerIdentity.MODE_KEY + "=warn");
                    }
                } else if (!warnedUnverifiableBridge) {
                    warnedUnverifiableBridge = true;
                    logger.warn("A broker bridge connected without a client certificate: bridge claims cannot be verified"
                            + " unless broker_require_client_auth=true (logged once)");
                }
            }
            CrescoIdentity resolved = resolveIdentity(context);
            if (resolved != null && info.getConnectionId() != null) identities.put(info.getConnectionId(), resolved);
            try {
                super.addConnection(context, info);
            } catch (Exception e) {
                forget(info.getConnectionId());
                throw e;
            }
        }

        @Override
        public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
            if (info != null) forget(info.getConnectionId());
            super.removeConnection(context, info, error);
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            enforce(context, info.getDestination(), TenantPolicy.Access.READ);
            if (recipientBinding && info.getSelector() != null && !isTrustedInfra(context)) {
                CrescoIdentity id = identityOf(context);
                TenantPolicy.Decision d = StreamRecipientPolicy.maySubscribe(id, info.getSelector());
                if (!d.allowed) {
                    String name = info.getDestination() == null ? "?" : info.getDestination().getPhysicalName();
                    logger.warn("DENY READ '" + name + "' for " + id + " : " + d.reason);
                    throw new SecurityException("Cresco stream policy: subscription to '" + name + "' denied (" + d.reason + ")");
                }
            }
            return super.addConsumer(context, info);
        }

        @Override
        public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
            // A producer may be created without a fixed destination (set per-send); that case is caught
            // in send(). Only enforce here when the producer binds a destination up front.
            if (info.getDestination() != null) {
                enforce(context, info.getDestination(), TenantPolicy.Access.WRITE);
            }
            super.addProducer(context, info);
        }

        @Override
        public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
            ConnectionContext ctx = (producerExchange != null) ? producerExchange.getConnectionContext() : null;
            enforce(ctx, messageSend.getDestination(), TenantPolicy.Access.WRITE);
            super.send(producerExchange, messageSend);
        }
    }
}
