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
import java.util.Set;
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

    /**
     * JMS username prefix on the connections this controller's own ActiveClient opens to its broker over vm://
     * ({@code cresco-local|<tenant>|<region>|<agent>}). Honoured only on vm:// (in-JVM) connections. Those connections
     * serve the controller and every plugin on it -- wsapi included, and through wsapi any client holding the service
     * key -- so for recipient-bound streams they are the controller's own identity, not infrastructure.
     */
    public static final String LOCAL_CONTROLLER_PREFIX = "cresco-local|";

    private final CLogger logger;
    private final PeerTrustRegistry peers;
    private final PeerIdentity.Mode peerMode;
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
     * Per-connection identity, attached to the connection's own ConnectionContext at addConnection. Nothing is kept in
     * maps keyed by connection id: a second ConnectionInfo for a live connection gets a new context, so whether it
     * succeeds or fails it can never alter or erase the live connection's state.
     */
    private static final class CrescoSecurityContext extends SecurityContext {
        final CrescoIdentity identity;    // certificate-bound, local controller, or (no certificate) asserted username
        final boolean certificate;        // the TLS session presented a client certificate
        final boolean localController;    // this controller's own vm:// connection (LOCAL_CONTROLLER_PREFIX)
        CrescoSecurityContext(String user, CrescoIdentity identity, boolean certificate, boolean localController) {
            super(user == null ? "" : user);
            this.identity = identity;
            this.certificate = certificate;
            this.localController = localController;
        }
        @Override public Set<Principal> getPrincipals() { return Collections.emptySet(); }
    }

    private static CrescoSecurityContext stateOf(ConnectionContext ctx) {
        return (ctx != null && ctx.getSecurityContext() instanceof CrescoSecurityContext)
                ? (CrescoSecurityContext) ctx.getSecurityContext() : null;
    }

    /** The identity of a connection (null: none). */
    private CrescoIdentity identityOf(ConnectionContext ctx) {
        CrescoSecurityContext sc = stateOf(ctx);
        return sc == null ? null : sc.identity;
    }

    private static boolean isVm(ConnectionContext ctx) {
        try {
            Connection c = ctx.getConnection();
            String ra = (c != null) ? c.getRemoteAddress() : null;
            return ra != null && ra.startsWith("vm");
        } catch (Exception e) {
            return false;
        }
    }

    /** Asserted {@code tenant|region|agent[|uid]}, valid names only. */
    private static CrescoIdentity parseAsserted(String u) {
        if (u == null || u.isEmpty()) return null;
        String[] p = u.split("\\|");
        if (p.length < 3) return null;
        CrescoIdentity asserted = CrescoIdentity.of(p[0], p[1], p[2], p.length >= 4 ? p[3] : null);
        return CrescoNames.isValid(asserted) ? asserted : null;
    }

    /**
     * Resolve a new connection's identity:
     * <ul>
     *   <li>a TLS client certificate: the identity it names, only when its key is the one bound to that name or it was
     *       issued by the CA recorded for that name's region ({@link PeerTrustRegistry#verifiedIdentity}); otherwise none.
     *       The username the client asserts is never used;</li>
     *   <li>this controller's own vm:// connection: the controller's identity;</li>
     *   <li>no certificate (mutual TLS off): the asserted {@code tenant|region|agent} username, as before (unverifiable).</li>
     * </ul>
     * With peer_identity_mode=off a certificate's identity is read as before (by name, without the binding check).
     */
    private CrescoSecurityContext resolve(ConnectionContext context, ConnectionInfo info) {
        Object tc = info.getTransportContext();
        X509Certificate[] chain = (tc instanceof X509Certificate[] && ((X509Certificate[]) tc).length > 0) ? (X509Certificate[]) tc : null;
        if (chain != null) {
            CrescoIdentity id;
            if (peerMode == PeerIdentity.Mode.OFF) {
                CrescoIdentity n = CrescoIdentity.fromCertificate(chain[0]);
                id = (n != null && n.getTenant() != null) ? n : null;
            } else {
                String[] why = new String[1];
                id = peers.verifiedIdentity(chain, why);
                if (id == null) {
                    logger.warn("Client certificate '" + chain[0].getSubjectX500Principal().getName() + "' carries no Cresco identity here: "
                            + why[0] + " (the connection is treated as unidentified)");
                }
            }
            if (id != null) {
                logger.info("mTLS identity bound from certificate: " + id + " (authoritative; overrides any asserted username)");
            }
            return new CrescoSecurityContext(id == null ? null : id.toX500Name(), id, true, false);
        }
        String u = info.getUserName();
        if (isVm(context) && u != null && u.startsWith(LOCAL_CONTROLLER_PREFIX)) {
            CrescoIdentity id = parseAsserted(u.substring(LOCAL_CONTROLLER_PREFIX.length()));
            return new CrescoSecurityContext(u, id, false, id != null);
        }
        return new CrescoSecurityContext(u, parseAsserted(u), false, false);
    }

    /** Trusted infrastructure connections (local controller vm://, or a verified broker bridge) are exempt. */
    private boolean isTrustedInfra(ConnectionContext ctx) {
        try {
            if (ctx == null) return true;
            if (ctx.isNetworkConnection()) return isTrustedBridge(ctx); // broker-to-broker bridge (verified)
            if (ctx.getConnector() == null) return true;     // broker-internal
            if (isVm(ctx)) return true;                       // in-JVM: this controller, or a bridge's local side
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
        CrescoSecurityContext sc = stateOf(ctx);
        if (sc == null) return true;                                   // broker-internal context
        if (!sc.certificate) return true;                              // no client certificate: unverifiable, as before
        if (peerMode != PeerIdentity.Mode.ENFORCE) return true;
        if (peers.isBridgePeer(sc.identity)) {
            peers.touchBridgePeer(sc.identity);
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
        CrescoSecurityContext sc = stateOf(context);
        // this controller's own vm:// connection (and so every plugin and wsapi client on it) is the controller's
        // identity for recipient-bound streams; other vm:// connections (bridges' local sides) and verified bridges
        // forward, and the next broker applies the same rule
        if (!(sc != null && sc.localController) && isTrustedInfra(context)) return true;
        TenantPolicy.Decision d = StreamRecipientPolicy.mayReceive(sc == null ? null : sc.identity, to, toTenant);
        if (!d.allowed) {
            long n = withheld.incrementAndGet();
            long now = System.currentTimeMillis();
            if (now - lastWithheldLog > 10_000L) {            // one line per 10 s, with the running count
                lastWithheldLog = now;
                logger.warn("WITHHELD recipient-bound message on '" + (message.getDestination() == null ? "?" : message.getDestination().getPhysicalName())
                        + "' from " + (sc == null ? null : sc.identity) + " : " + d.reason + " (" + n + " withheld so far)");
            }
        }
        return d.allowed;
    }

    /** Recipient-bound messages withheld from non-recipients so far. */
    public long withheldCount() {
        return withheld.get();
    }

    private final class AuthFilter extends BrokerFilter {
        AuthFilter(Broker next) { super(next); }

        @Override
        public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
            CrescoSecurityContext sc = resolve(context, info);
            context.setSecurityContext(sc);
            // every connection is judged per message, vm:// included: the in-JVM connector is created lazily and does
            // not inherit the broker's policy, and this controller's own vm:// connection is not exempt (see above)
            if (recipientBinding) context.setMessageAuthorizationPolicy(CrescoAuthorizationBroker.this);
            if (sc.certificate && sc.identity != null) info.setUserName(sc.identity.toX500Name());
            // Bridge claim (BrokerInfo before ConnectionInfo, as every real bridge does): judge it now so an impostor
            // is refused outright with a clear reason instead of merely losing the exemption.
            if (context.isNetworkConnection()) {
                if (sc.certificate) {
                    if (peers.isBridgePeer(sc.identity)) {
                        logger.info("Broker bridge from verified controller " + sc.identity);
                    } else if (peerMode == PeerIdentity.Mode.ENFORCE) {
                        logger.error("BRIDGE REFUSED: a connection claiming to be a broker bridge presented " + sc.identity
                                + ", which is not a verified region/global controller that completed discovery with this node ("
                                + PeerIdentity.MODE_KEY + "=enforce)");
                        throw new SecurityException("Cresco: broker bridge refused for " + sc.identity + " (not a discovered controller)");
                    } else if (peerMode == PeerIdentity.Mode.WARN) {
                        logger.warn("Broker bridge from " + sc.identity + " is not a verified discovered controller -- ACCEPTED because "
                                + PeerIdentity.MODE_KEY + "=warn");
                    }
                } else if (!warnedUnverifiableBridge) {
                    warnedUnverifiableBridge = true;
                    logger.warn("A broker bridge connected without a client certificate: bridge claims cannot be verified"
                            + " unless broker_require_client_auth=true (logged once)");
                }
            }
            super.addConnection(context, info);
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            enforce(context, info.getDestination(), TenantPolicy.Access.READ);
            CrescoSecurityContext sc = stateOf(context);
            boolean local = sc != null && sc.localController;
            if (recipientBinding && info.getSelector() != null && (local || !isTrustedInfra(context))) {
                CrescoIdentity id = sc == null ? null : sc.identity;
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
