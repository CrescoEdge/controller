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
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.ActiveMQTempDestination;
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

    /**
     * {@code broker_shared_destinations} default: the fabric-shared topics. {@code global.event} only while tenant
     * namespacing is off (audit D2a; see the constructor).
     */
    public static String defaultSharedDestinations(io.cresco.library.plugin.Config config) {
        return SecuritySettings.tenantNamespacing(config) ? "agent.event,region.event" : "agent.event,region.event,global.event";
    }

    private final CLogger logger;
    private final PeerTrustRegistry peers;
    private final PeerIdentity.Mode peerMode;
    private final AtomicLong withheld = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();
    private volatile long lastAnonymousLog;
    private volatile long lastWithheldLog;
    private volatile boolean warnedUnverifiableBridge;
    private final Set<String> sharedPrefixes;
    private final Set<String> superuserTenants;
    private final java.util.List<TenantPolicy.CrossTenantSink> crossTenantSinks;
    private final boolean logAllow;
    private final boolean recipientBinding;
    private final boolean allowUnverifiedBridges;
    private final String ownTenant;
    private volatile CrescoIdentity ownIdentity;
    /** Bounds on what a quarantined connection may hold (it receives nothing and its sends are discarded). */
    private final int quarantineMaxConnections, quarantineMaxConsumers, quarantineMaxProducers, quarantineMaxDestinations;
    private final java.util.concurrent.atomic.AtomicInteger quarantinedConnections = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger quarantineCreatedDestinations = new java.util.concurrent.atomic.AtomicInteger();
    /**
     * The verified network connections that are up now (connection id -> context): kept fresh in the registry, and closed
     * when their certificate expires or their key is revoked.
     */
    private final java.util.Map<String, ConnectionContext> liveVerified = new java.util.concurrent.ConcurrentHashMap<>();
    private final long seenRefreshMs;
    private volatile java.util.concurrent.ScheduledExecutorService seenRefresher;

    public CrescoAuthorizationBroker(PluginBuilder plugin) {
        this(plugin, new PeerTrustRegistry());
    }

    /** @param peers the controllers allowed to bridge into this broker (learned in discovery) */
    public CrescoAuthorizationBroker(PluginBuilder plugin, PeerTrustRegistry peers) {
        this.logger = plugin.getLogger(CrescoAuthorizationBroker.class.getName(), CLogger.Level.Info);
        this.peers = (peers != null) ? peers : new PeerTrustRegistry();
        this.peerMode = PeerIdentity.Mode.parse(plugin.getConfig().getStringParam(PeerIdentity.MODE_KEY, "enforce"));
        // global.event is NOT shared when tenants are namespaced (audit D2a): a tenant's dataplane is then its own
        // T.<tenant>.global.event, and the un-namespaced fabric-wide topic would carry data across tenants. Without
        // namespacing the dataplane IS global.event (there is no tenant boundary on it to keep), so it stays shared there.
        // An operator lists it explicitly to share it anyway.
        String shared = plugin.getConfig().getStringParam("broker_shared_destinations", defaultSharedDestinations(plugin.getConfig()));
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
        this.ownTenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
        // migration escape only: a bridge without a client certificate cannot be verified; keep its old exemption
        this.allowUnverifiedBridges = plugin.getConfig().getBooleanParam("broker_allow_unverified_bridges", false);
        if (allowUnverifiedBridges) {
            logger.warn("broker_allow_unverified_bridges=true: ANY client that claims to be a broker bridge without a client"
                    + " certificate is exempt from tenant authorization (migration only; turn on broker_require_client_auth)");
        }
        // W-GFS-1: named cross-tenant sinks (see TenantPolicy.CrossTenantSink). Empty by default -> the
        // strict own-subtree rule, exactly as before.
        this.crossTenantSinks = TenantPolicy.CrossTenantSink.parse(
                plugin.getConfig().getStringParam("broker_cross_tenant_sinks", ""));
        this.quarantineMaxConnections = Math.max(0, plugin.getConfig().getIntegerParam("broker_quarantine_max_connections", 64));
        this.quarantineMaxConsumers = Math.max(0, plugin.getConfig().getIntegerParam("broker_quarantine_max_consumers", 4));
        this.quarantineMaxProducers = Math.max(0, plugin.getConfig().getIntegerParam("broker_quarantine_max_producers", 4));
        this.quarantineMaxDestinations = Math.max(0, plugin.getConfig().getIntegerParam("broker_quarantine_max_destinations", 256));
        this.seenRefreshMs = Math.max(60_000L, plugin.getConfig().getLongParam("peer_seen_refresh_ms", 3_600_000L));
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
        // this broker's controller: the broker is named after its agent path (ActiveBroker); a vm:// connection may claim
        // only this identity with LOCAL_CONTROLLER_PREFIX
        String[] path = CrescoNames.splitPath(next.getBrokerName());
        if (path != null) ownIdentity = CrescoIdentity.of(ownTenant, path[0], path[1], null);
        java.util.concurrent.ScheduledExecutorService ex = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cresco-peer-seen-refresh");
            t.setDaemon(true);
            return t;
        });
        ex.scheduleWithFixedDelay(this::refreshLiveIdentities, seenRefreshMs, seenRefreshMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        // an identity that expires or is revoked while connected loses its connection within a minute
        ex.scheduleWithFixedDelay(() -> { try { closeInvalidConnections(); } catch (Exception ignore) { } }, 60, 60,
                java.util.concurrent.TimeUnit.SECONDS);
        seenRefresher = ex;
        return new AuthFilter(next);
    }

    /**
     * Per-connection identity, attached to the connection's own ConnectionContext at addConnection. Nothing is kept in
     * maps keyed by connection id: a second ConnectionInfo for a live connection gets a new context, so whether it
     * succeeds or fails it can never alter or erase the live connection's state.
     */
    private static final class CrescoSecurityContext extends SecurityContext {
        final CrescoIdentity identity;    // certificate-bound, local controller, or (no certificate) asserted username
        final boolean verified;           // bound by a certificate (or this controller's own vm://): may receive cresco_to
        final boolean certificate;        // the TLS session presented a client certificate
        final boolean localController;    // this controller's own vm:// connection (LOCAL_CONTROLLER_PREFIX)
        final boolean anonymous;          // a network connection without a client certificate: no identity, no role
        volatile X509Certificate leaf;    // the TLS client certificate (leaf), when one was presented
        volatile X509Certificate[] chain; // the whole chain it presented (the live sweep judges every certificate on its path)
        volatile boolean bridgeLocal;     // a vm:// connection carrying a network bridge's demand subscriptions
        volatile Object bridgePeer;       // the controller at the other end of this bridge connection (resolved once), or NO_PEER
        final java.util.concurrent.atomic.AtomicInteger consumers = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger producers = new java.util.concurrent.atomic.AtomicInteger();
        /** A bridge connection: what its peer was last found to be (see bridgeReach), when, and its verdicts per destination. */
        volatile boolean inJvm;           // a vm:// connection (set when it is added)
        volatile int bridgeReach;
        volatile long bridgeReachAt;
        final java.util.concurrent.ConcurrentHashMap<String, Object> bridgeVerdicts = new java.util.concurrent.ConcurrentHashMap<>();
        /** Holds one of the quarantine's connection slots (given back exactly once: when it is removed, or its add fails). */
        final java.util.concurrent.atomic.AtomicBoolean quarantineSlot = new java.util.concurrent.atomic.AtomicBoolean();
        CrescoSecurityContext(String user, CrescoIdentity identity, boolean verified, boolean certificate, boolean localController) {
            this(user, identity, verified, certificate, localController, false);
        }
        CrescoSecurityContext(String user, CrescoIdentity identity, boolean verified, boolean certificate, boolean localController,
                              boolean anonymous) {
            super(user == null ? "" : user);
            this.identity = identity;
            this.verified = verified;
            this.certificate = certificate;
            this.localController = localController;
            this.anonymous = anonymous;
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
     *   <li>no certificate (broker_require_client_auth=false): NO identity and no role, whatever username it typed -- not a
     *       tenant, never SUPERUSER. Such a network connection is quarantined: it may connect, but its subscriptions receive
     *       nothing (advisories included) and its sends are discarded (logged). Cresco nodes present their certificates even
     *       then (the broker asks for one: wantClientAuth).</li>
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
                if (id == null && peerMode == PeerIdentity.Mode.WARN) {
                    id = CrescoNames.identityOf(chain[0]);
                    if (id != null) {
                        logger.warn("Client certificate names " + id + " but " + why[0] + " -- ACCEPTED because " + PeerIdentity.MODE_KEY
                                + "=warn (enforce would treat the connection as unidentified)");
                    }
                } else if (id == null) {
                    logger.warn("Client certificate '" + chain[0].getSubjectX500Principal().getName() + "' carries no Cresco identity here: "
                            + why[0] + " (the connection is treated as unidentified)");
                }
            }
            if (id != null) {
                logger.info("mTLS identity bound from certificate: " + id + " (authoritative; overrides any asserted username)");
            }
            // a certificate that passed TLS but carries no identity here (unbound, rebound away, foreign CA) gets no more than
            // a connection without a certificate: quarantined
            CrescoSecurityContext sc = new CrescoSecurityContext(id == null ? null : id.toX500Name(), id, id != null, true, false, id == null && !isVm(context));
            sc.leaf = chain[0];
            sc.chain = chain.clone();
            return sc;
        }
        String u = info.getUserName();
        if (isVm(context) && u != null && u.startsWith(LOCAL_CONTROLLER_PREFIX)) {
            CrescoIdentity claimed = parseAsserted(u.substring(LOCAL_CONTROLLER_PREFIX.length()));
            if (claimed != null && ownIdentity != null && claimed.getRegion().equals(ownIdentity.getRegion())
                    && claimed.getAgent().equals(ownIdentity.getAgent()) && claimed.getTenant().equals(ownIdentity.getTenant())) {
                return new CrescoSecurityContext(u, claimed, true, false, true);
            }
            logger.warn("An in-JVM connection claimed the controller identity " + claimed + ", which is not this broker's controller "
                    + ownIdentity + " -- treated as unidentified");
            return new CrescoSecurityContext(u, null, false, false, false);
        }
        // no certificate: a typed username proves nothing, so it confers nothing -- no tenant, no SUPERUSER. In-JVM (vm://)
        // connections are this JVM's own code (exempt from tenant policy as before); a network one is quarantined.
        return new CrescoSecurityContext(u, null, false, false, false, !isVm(context));
    }

    /**
     * Trusted infrastructure connections (local controller vm://, or a verified broker bridge) are not refused here. What a
     * BRIDGE then carries is still its peer's own: its sends, the messages dispatched to it and what it administers are
     * judged by {@link #bridgeRefusal}.
     */
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
        if (!sc.certificate) return allowUnverifiedBridges;            // no client certificate: unverifiable -> not exempt
        if (peerMode != PeerIdentity.Mode.ENFORCE) return true;
        if (peers.isBridgePeer(sc.identity, sc.leaf)) {
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
        CrescoSecurityContext q = stateOf(ctx);
        if (quarantined(ctx, q) && !q.certificate) {
            // quarantined without a certificate: it may subscribe (nothing is delivered to it) and send (discarded). A
            // connection whose certificate does not verify is quarantined the same way AND still refused outright below,
            // with no identity, on everything but advisory and fabric-shared destinations
            return;
        }
        if (dest.isComposite()) {
            // every component is judged on its own, for this very operation; and only infrastructure uses composites
            // beyond the client's own advisory consumer (a composite judged by its whole name read another tenant's data)
            if (!allAdvisory(dest)) {
                CrescoIdentity who = identityOf(ctx);
                logger.warn("DENY " + access + " composite '" + dest.getPhysicalName() + "' for " + who);
                throw new SecurityException("Cresco tenant policy: composite destinations are for infrastructure only ('"
                        + dest.getPhysicalName() + "')");
            }
            for (ActiveMQDestination c : dest.getCompositeDestinations()) enforce(ctx, c, access);
            return;
        }
        String name = dest.getPhysicalName();
        CrescoIdentity id = identityOf(ctx);
        if (access == TenantPolicy.Access.WRITE && legacyRawInboxWrite(id, name)) return;
        TenantPolicy.Decision d = TenantPolicy.check(id, name, access, sharedPrefixes, roleOf(id), crossTenantSinks, CrescoAuthorizationBroker.this::boundTenantOf);
        if (!d.allowed) {
            String who = (id != null) ? id.toString() : ("username=" + (ctx != null ? ctx.getUserName() : "?"));
            logger.warn("DENY " + access + " '" + name + "' for " + who + " : " + d.reason);
            throw new SecurityException("Cresco tenant policy: " + access + " denied on '" + name + "' (" + d.reason + ")");
        } else if (logAllow) {
            logger.info("ALLOW " + access + " '" + name + "' : " + d.reason);
        }
    }

    private volatile java.util.function.BooleanSupplier upgradeWindow = () -> false;

    /** Is the upgrade window open on this node (docs/upgrade-1.3-secure-defaults.md section 3)? Set by the controller. */
    public void setUpgradeWindow(java.util.function.BooleanSupplier open) {
        this.upgradeWindow = open == null ? () -> false : open;
    }

    /**
     * The upgrade window: a verified node of this broker's region may write this broker's controller's RAW inbox
     * ({@code <region>_<agent>}), as nodes of an earlier build (no tenant namespacing) do, whatever its tenant -- in a
     * multi-tenant region the controller is of a superuser tenant, and an earlier build wrote there across tenants. It
     * grants nothing else: the controller consumes that inbox only while the window is open and judges every message
     * there by its verified caller (a node without stamping has membership only; see LegacyNodes).
     */
    boolean legacyRawInboxWrite(CrescoIdentity id, String name) {
        CrescoIdentity own = ownIdentity;
        if (id == null || own == null || name == null || !id.getRegion().equals(own.getRegion())) return false;
        if (!name.equals(own.getRegion() + "_" + own.getAgent())) return false;
        return upgradeWindow.getAsBoolean();
    }

    // ---- MessageAuthorizationPolicy: recipient-bound dataplane messages (StreamRecipientPolicy) ----

    @Override
    public boolean isAllowedToConsume(ConnectionContext context, Message message) {
        CrescoSecurityContext q = stateOf(context);
        if (quarantined(context, q)) return withheld(message, null, "a connection without a client certificate receives nothing");
        if (bridgeCarrier(context, q)) {
            // a bridge's demand subscription: only its peer's own traffic leaves over it (judged per message: the demand may
            // be a wildcard, and refusing the subscription itself would take the whole bridge down)
            ActiveMQDestination about = message.getDestination();
            try {
                Object inner = message.getDataStructure();
                if (inner instanceof Message && ((Message) inner).getDestination() != null) about = ((Message) inner).getDestination();
            } catch (Exception ignore) { }
            String why = bridgeRefusal(context, q, about, TenantPolicy.Access.READ);
            if (why != null) return bridgeOutOfScope(q, about, TenantPolicy.Access.READ, why);
        }
        // an advisory that carries a copy of a message (MessageDiscarded, DLQ'd, ...) is judged as that message: its
        // recipient binding and its destination's tenant, so properties never leak to other recipients or tenants
        Message subject = message;
        boolean advisoryCopy = false;
        try {
            Object ds = message.getDataStructure();
            if (ds instanceof Message) { subject = (Message) ds; advisoryCopy = true; }
            else if (ds != null && isAdvisory(message)) return advisoryAllowed(context, q, message, ds);
        } catch (Exception ignore) { }
        String to;
        String toTenant;
        try {
            Object p = subject.getProperty(StreamRecipientPolicy.TO);
            to = (p == null) ? null : p.toString();
            Object t = subject.getProperty(StreamRecipientPolicy.TO_TENANT);
            toTenant = (t == null) ? null : t.toString();
        } catch (Exception e) {
            return false;                                     // unreadable properties on a secured broker: withhold
        }
        if (to == null && !advisoryCopy) return true;         // not recipient-bound: the common case, no further work
        CrescoSecurityContext sc = stateOf(context);
        if (isForwarder(context, sc)) return true;            // a verified bridge or a bridge's local side: the next broker judges
        CrescoIdentity who = (sc != null && sc.verified) ? sc.identity : null;
        if (advisoryCopy && subject.getDestination() != null) {
            TenantPolicy.Decision t = TenantPolicy.check(who, subject.getDestination().getPhysicalName(), TenantPolicy.Access.READ,
                    sharedPrefixes, roleOf(who), crossTenantSinks);
            if (!t.allowed) return withheld(message, who, "an advisory about another tenant's destination (" + t.reason + ")");
        }
        if (to == null) return true;
        TenantPolicy.Decision d = StreamRecipientPolicy.mayReceive(who, to, toTenant);
        return d.allowed || withheld(message, who, d.reason + (sc != null && !sc.verified && sc.identity != null ? " (asserted, unverified identity "
                + sc.identity + ")" : ""));
    }

    /**
     * A network connection without a verified identity (no client certificate, or one that does not verify): no identity
     * and no role. Its subscriptions receive nothing and its sends are discarded -- except a certificate-less bridge claim
     * while broker_allow_unverified_bridges=true (migration only).
     */
    private boolean quarantined(ConnectionContext ctx, CrescoSecurityContext sc) {
        if (sc == null || !sc.anonymous) return false;
        try { return !(allowUnverifiedBridges && !sc.certificate && ctx.isNetworkConnection()); } catch (Exception e) { return true; }
    }

    /**
     * May {@code ctx} remove or create {@code dest} (an administrative operation)? Trusted infrastructure may; a quarantined
     * connection may not; a temporary destination may be removed only by the connection that owns it; otherwise the identity
     * must be able to both read and write it, and it must not be a fabric-shared or advisory destination (those belong to
     * the fabric, not to any one tenant).
     */
    private String administerRefusal(ConnectionContext ctx, ActiveMQDestination dest) {
        if (dest == null) return null;
        CrescoSecurityContext sc = stateOf(ctx);
        if (bridgeCarrier(ctx, sc)) {
            // a bridge: temporary destinations as before; anything else only when its peer may both read and write it
            ActiveMQDestination[] parts = dest.isComposite() ? dest.getCompositeDestinations() : new ActiveMQDestination[]{dest};
            for (ActiveMQDestination d : parts) {
                if (d.isTemporary()) continue;
                String r = bridgeRefusal(ctx, sc, d, TenantPolicy.Access.READ);
                if (r == null) r = bridgeRefusal(ctx, sc, d, TenantPolicy.Access.WRITE);
                if (r != null) return "'" + d.getPhysicalName() + "' is not this bridge's peer's to administer (" + r + ")";
            }
            return null;
        }
        if (isTrustedInfra(ctx)) return null;
        if (quarantined(ctx, sc)) return "a connection without a verified identity administers no destination";
        ActiveMQDestination[] each = dest.isComposite() ? dest.getCompositeDestinations() : new ActiveMQDestination[]{dest};
        for (ActiveMQDestination d : each) {
            if (d.isTemporary()) {
                String owner = d instanceof ActiveMQTempDestination ? ((ActiveMQTempDestination) d).getConnectionId() : null;
                String mine = (ctx != null && ctx.getConnectionId() != null) ? ctx.getConnectionId().getValue() : null;
                if (owner == null || !owner.equals(mine)) return "temporary destination " + d.getPhysicalName() + " belongs to another connection";
                continue;
            }
            String name = d.getPhysicalName();
            if (name.startsWith("ActiveMQ.Advisory")) return "advisory destinations belong to the broker";
            for (String p : sharedPrefixes) {
                if (p != null && !p.isEmpty() && (name.equals(p) || name.startsWith(p + "."))) return "fabric-shared destination " + name + " belongs to the fabric";
            }
            CrescoIdentity id = identityOf(ctx);
            TenantPolicy.Decision r = TenantPolicy.check(id, name, TenantPolicy.Access.READ, sharedPrefixes, roleOf(id), crossTenantSinks, CrescoAuthorizationBroker.this::boundTenantOf);
            TenantPolicy.Decision w = TenantPolicy.check(id, name, TenantPolicy.Access.WRITE, sharedPrefixes, roleOf(id), crossTenantSinks, CrescoAuthorizationBroker.this::boundTenantOf);
            if (!r.allowed || !w.allowed) return "'" + name + "' is not this identity's to administer (" + (r.allowed ? w.reason : r.reason) + ")";
        }
        return null;
    }

    private void refuseAdmin(ConnectionContext ctx, String op, ActiveMQDestination dest, String why) {
        CrescoIdentity id = identityOf(ctx);
        String who = id != null ? id.toString() : ("an unidentified connection from " + remoteOf(ctx));
        logger.warn("DENY " + op + " '" + (dest == null ? "?" : dest.getPhysicalName()) + "' for " + who + " : " + why);
        throw new SecurityException("Cresco tenant policy: " + op + " denied on '" + (dest == null ? "?" : dest.getPhysicalName()) + "' (" + why + ")");
    }

    /**
     * A quarantined connection holds little: a bounded number of consumers and producers, no composite destination, and
     * the destinations all quarantined connections may create (by consuming or producing on a name that does not exist)
     * are bounded for the broker's lifetime.
     */
    private void boundQuarantined(ConnectionContext ctx, CrescoSecurityContext sc, ActiveMQDestination dest, boolean consumer) {
        if (!quarantined(ctx, sc)) return;
        int n = consumer ? sc.consumers.incrementAndGet() : sc.producers.incrementAndGet();
        int max = consumer ? quarantineMaxConsumers : quarantineMaxProducers;
        if (n > max) {
            throw new SecurityException("Cresco: a connection without a verified identity may hold at most " + max
                    + (consumer ? " consumers" : " producers"));
        }
        if (dest != null && dest.isComposite() && !allAdvisory(dest)) {
            throw new SecurityException("Cresco: a connection without a verified identity cannot use a composite destination");
        }
    }

    /** A composite made only of advisory topics (every ActiveMQ client's own temp-destination advisory consumer is one). */
    private static boolean allAdvisory(ActiveMQDestination dest) {
        for (ActiveMQDestination d : dest.getCompositeDestinations()) {
            if (!d.isTopic() || !d.getPhysicalName().startsWith("ActiveMQ.Advisory.")) return false;
        }
        return true;
    }

    /** Refresh last-seen for every verified network connection that is up (a peer connected for months is never pruned). */
    void refreshLiveIdentities() {
        for (ConnectionContext ctx : liveVerified.values()) {
            CrescoSecurityContext sc = stateOf(ctx);
            if (sc != null && sc.identity != null) {
                try { peers.touchIdentity(sc.identity); } catch (Exception ignore) { }
            }
        }
    }

    /**
     * Close every live verified connection whose key is {@code keyFp} (just revoked). Returns how many were closed.
     */
    public int closeConnectionsOfKey(String keyFp) {
        int n = 0;
        for (ConnectionContext ctx : liveVerified.values()) {
            CrescoSecurityContext sc = stateOf(ctx);
            if (sc == null || sc.leaf == null) continue;
            if (keyFp.equals(PeerTrustRegistry.fingerprint(sc.leaf.getPublicKey().getEncoded()))) {
                close(ctx, "its key was revoked");
                n++;
            }
        }
        return n;
    }

    /** Close every live verified connection whose certificate is no longer valid or whose key is revoked. */
    int closeInvalidConnections() {
        int n = 0;
        long now = System.currentTimeMillis();
        for (ConnectionContext ctx : liveVerified.values()) {
            CrescoSecurityContext sc = stateOf(ctx);
            if (sc == null || sc.leaf == null) continue;
            String why = null;
            if (now >= sc.leaf.getNotAfter().getTime()) why = "its certificate expired at " + sc.leaf.getNotAfter();
            else why = peers.revokedReason(sc.chain != null ? sc.chain : new X509Certificate[]{sc.leaf});   // key/name/issuer/anchor revoked
            if (why != null) { close(ctx, why); n++; }
        }
        return n;
    }

    private void close(ConnectionContext ctx, String why) {
        CrescoSecurityContext sc = stateOf(ctx);
        logger.warn("CLOSING the connection of " + (sc == null ? "?" : sc.identity) + " from " + remoteOf(ctx) + ": " + why);
        try {
            if (ctx.getConnectionId() != null) liveVerified.remove(ctx.getConnectionId().getValue());
            ctx.getConnection().serviceExceptionAsync(new java.io.IOException("Cresco: connection closed: " + why));
        } catch (Exception e) {
            logger.error("could not close a connection (" + why + "): " + e.getMessage());
        }
    }

    /**
     * The tenant an un-namespaced inbox {@code <region>_<agent>} belongs to: the tenant bound to that name in the peer trust
     * registry, or this broker's own tenant for its own controller; null when unknown (a same-region write to it is then
     * refused).
     */
    String boundTenantOf(String agentPath) {
        String[] p = CrescoNames.splitPath(agentPath);
        if (p == null) return null;
        CrescoIdentity own = ownIdentity;
        if (own != null && p[0].equals(own.getRegion()) && p[1].equals(own.getAgent())) return own.getTenant();
        return peers.boundTenant(p[0], p[1]);
    }

    /** Verified network connections up now (tests). */
    int liveVerifiedCount() { return liveVerified.size(); }

    /** Quarantined connections up now. */
    public int quarantinedConnectionCount() { return quarantinedConnections.get(); }

    private static boolean isAdvisory(Message m) {
        ActiveMQDestination d = m.getDestination();
        return d != null && d.isTopic() && d.getPhysicalName().startsWith("ActiveMQ.Advisory.");
    }

    /**
     * An advisory about a consumer, producer or destination (ConsumerInfo carries the destination AND the selector) reaches
     * a tenant only when that destination is the tenant's to read; advisories about connections and sessions (usernames,
     * client ids) and removals carry no tenant scope and reach no tenant. Forwarders, this JVM's own connections and
     * SUPERUSER see every advisory; a temporary destination's create/remove advisory (which ActiveMQ clients rely on, and
     * which names no tenant data) reaches everyone.
     */
    private boolean advisoryAllowed(ConnectionContext ctx, CrescoSecurityContext sc, Message message, Object ds) {
        if (isForwarder(ctx, sc) || isVm(ctx)) return true;
        CrescoIdentity who = (sc != null && sc.verified) ? sc.identity : null;
        if (who != null && roleOf(who) == TenantPolicy.Role.SUPERUSER) return true;
        ActiveMQDestination about = ds instanceof ConsumerInfo ? ((ConsumerInfo) ds).getDestination()
                : ds instanceof ProducerInfo ? ((ProducerInfo) ds).getDestination()
                : ds instanceof DestinationInfo ? ((DestinationInfo) ds).getDestination() : null;
        if (about == null) return withheld(message, who, "an advisory about another connection carries no tenant scope");
        if (ds instanceof DestinationInfo && about.isTemporary()) return true;
        ActiveMQDestination[] each = about.isComposite() ? about.getCompositeDestinations() : new ActiveMQDestination[]{about};
        for (ActiveMQDestination d : each) {
            TenantPolicy.Decision t = TenantPolicy.check(who, d.getPhysicalName(), TenantPolicy.Access.READ, sharedPrefixes, roleOf(who),
                    crossTenantSinks);
            if (!t.allowed) return withheld(message, who, "an advisory about another tenant's destination (" + t.reason + ")");
        }
        return true;
    }

    /**
     * Connections that forward recipient-bound messages without being their recipient: broker-internal contexts, verified
     * broker bridges, and the local (vm://) side of a bridge (its demand subscriptions are network subscriptions). This
     * controller's own vm:// connection is NOT one: it is the controller's identity. Any other vm:// connection has no identity.
     */
    private boolean isForwarder(ConnectionContext ctx, CrescoSecurityContext sc) {
        try {
            if (ctx == null || ctx.getConnector() == null) return true;
            if (ctx.isNetworkConnection()) return isTrustedBridge(ctx);
            return sc != null && sc.bridgeLocal && isVm(ctx);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean withheld(Message message, CrescoIdentity who, String reason) {
        long n = withheld.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - lastWithheldLog > 10_000L) {            // one line per 10 s, with the running count
            lastWithheldLog = now;
            logger.warn("WITHHELD message on '" + (message.getDestination() == null ? "?" : message.getDestination().getPhysicalName())
                    + "' from " + who + " : " + reason + " (" + n + " withheld so far)");
        }
        return false;
    }

    private static String remoteOf(ConnectionContext ctx) {
        try { return ctx.getConnection().getRemoteAddress(); } catch (Exception e) { return "?"; }
    }

    /** Messages sent by connections without a client certificate, discarded so far. */
    public long discardedCount() {
        return discarded.get();
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
            if (quarantined(context, sc)) {
                if (quarantinedConnections.incrementAndGet() > quarantineMaxConnections) {
                    quarantinedConnections.decrementAndGet();
                    throw new SecurityException("Cresco: too many connections without a verified identity (at most "
                            + quarantineMaxConnections + ")");
                }
                sc.quarantineSlot.set(true);
            }
            // the broker never calls removeConnection for a connection whose add fails (a bridge refused below, a client id
            // in use, ...): what this connection holds here is given back at once, or every failed attempt would leak it
            String liveId = sc.verified && sc.certificate && sc.identity != null && context.getConnectionId() != null
                    ? context.getConnectionId().getValue() : null;
            ConnectionContext liveBefore = null;
            boolean added = false;
            try {
                if (liveId != null) liveBefore = liveVerified.put(liveId, context);
                admit(context, info, sc);
                added = true;
            } finally {
                if (!added) {
                    if (sc.quarantineSlot.compareAndSet(true, false)) quarantinedConnections.decrementAndGet();
                    if (liveId != null) {
                        // a second ConnectionInfo for a connection that is up: that connection stays as it was recorded
                        if (liveBefore != null) liveVerified.replace(liveId, context, liveBefore);
                        else liveVerified.remove(liveId, context);
                    }
                }
            }
        }

        private void admit(ConnectionContext context, ConnectionInfo info, CrescoSecurityContext sc) throws Exception {
            sc.inJvm = isVm(context);
            context.setSecurityContext(sc);
            // every connection is judged per message, vm:// included: the in-JVM connector is created lazily and does
            // not inherit the broker's policy, and this controller's own vm:// connection is not exempt (see above)
            // (a connection with a certificate may be a bridge: what is dispatched to it is judged by bridgeRefusal)
            if (recipientBinding || sc.anonymous || sc.certificate) context.setMessageAuthorizationPolicy(CrescoAuthorizationBroker.this);
            if (sc.anonymous) {
                long now = System.currentTimeMillis();
                if (now - lastAnonymousLog > 10_000L) {
                    lastAnonymousLog = now;
                    logger.warn("A network connection from " + remoteOf(context) + (sc.certificate
                            ? " presented a certificate that carries no verified identity here"
                            : " presented no client certificate (username '" + info.getUserName() + "' is not proof of anything)")
                            + ": it has NO identity on this secured broker, so its subscriptions receive nothing and its sends are"
                            + " discarded (logged at most every 10 s)");
                }
            }
            if (sc.certificate && sc.identity != null) info.setUserName(sc.identity.toX500Name());
            // Bridge claim (BrokerInfo before ConnectionInfo, as every real bridge does): judge it now so an impostor
            // is refused outright with a clear reason instead of merely losing the exemption.
            if (context.isNetworkConnection()) {
                if (sc.certificate) {
                    if (peers.isBridgePeer(sc.identity, sc.leaf)) {
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
                    logger.warn("A broker bridge connected without a client certificate: its claim cannot be verified, so it is "
                            + (allowUnverifiedBridges ? "exempt only because broker_allow_unverified_bridges=true" : "NOT exempt from tenant authorization")
                            + " -- turn on broker_require_client_auth (logged once)");
                }
            }
            super.addConnection(context, info);
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            enforce(context, info.getDestination(), TenantPolicy.Access.READ);
            CrescoSecurityContext sc = stateOf(context);
            boundQuarantined(context, sc, info.getDestination(), true);
            if (sc != null && sc.verified && sc.identity != null) peers.touchIdentity(sc.identity);
            if (quarantined(context, sc)) {
                // a quarantined subscription receives nothing; it must not hold anything either: no durable subscription
                // (the store would keep every message for it) and no exclusivity (it would take a queue from its consumers)
                if (info.getSubscriptionName() != null) {
                    throw new SecurityException("Cresco: a connection without a client certificate cannot hold a durable subscription");
                }
                info.setExclusive(false);
                info.setPriority((byte) 0);
            }
            if (sc != null && info.isNetworkSubscription() && isVm(context)) sc.bridgeLocal = true;   // a bridge's local side
            // a process's stdin is its executor's: a dataplane subscription that is not this controller's own (its dataplane
            // applies the rule per bundle) or a bridge's never sees it (StdinGuard)
            if (!isForwarder(context, sc) && !(sc != null && sc.localController) && StdinGuard.isDataplaneTopic(info.getDestination())) {
                String why = StdinGuard.refusal(info.getSelector());
                if (why != null) {
                    logger.warn("DENY READ '" + info.getDestination().getPhysicalName() + "' for " + identityOf(context) + " : " + why);
                    throw new SecurityException("Cresco: " + why);
                }
                try {
                    info.setSelector(StdinGuard.narrow(info.getSelector()));
                } catch (IllegalArgumentException malformed) {
                    logger.warn("DENY READ '" + info.getDestination().getPhysicalName() + "' for " + identityOf(context) + " : " + malformed.getMessage());
                    throw new SecurityException("Cresco: " + malformed.getMessage());
                }
            }
            // the early refusal (a typed error for the client) is for network clients and this controller's own connection
            // (wsapi clients); any other in-JVM connection is judged per message only
            boolean judged = sc != null && (sc.localController || !isVm(context));
            if (recipientBinding && info.getSelector() != null && judged && !isForwarder(context, sc)) {
                CrescoIdentity id = (sc != null && sc.verified) ? sc.identity : null;
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
            boundQuarantined(context, stateOf(context), info.getDestination(), false);
            super.addProducer(context, info);
        }

        @Override
        public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            try {
                super.removeConsumer(context, info);
            } finally {
                inboxConsumerLeft(info == null ? null : info.getDestination());
            }
        }

        @Override
        public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
            try {
                super.removeConnection(context, info, error);
            } finally {
                CrescoSecurityContext sc = stateOf(context);
                if (sc != null && sc.quarantineSlot.compareAndSet(true, false)) quarantinedConnections.decrementAndGet();
                if (context != null && context.getConnectionId() != null) liveVerified.remove(context.getConnectionId().getValue());
            }
        }

        @Override
        public org.apache.activemq.broker.region.Destination addDestination(ConnectionContext context, ActiveMQDestination destination,
                                                                             boolean createIfTemporary) throws Exception {
            if (destination != null && !isTrustedInfra(context)) {
                CrescoSecurityContext sc = stateOf(context);
                if (quarantined(context, sc)) {
                    if (destination.isComposite() && !allAdvisory(destination)) refuseAdmin(context, "create", destination, "composite destination");
                    boolean advisory = destination.isComposite() ? allAdvisory(destination)
                            : destination.getPhysicalName().startsWith("ActiveMQ.Advisory.");
                    boolean exists = advisory || !getDestinations(destination).isEmpty();   // advisory topics are the broker's
                    if (!exists && quarantineCreatedDestinations.incrementAndGet() > quarantineMaxDestinations) {
                        quarantineCreatedDestinations.decrementAndGet();
                        refuseAdmin(context, "create", destination, "connections without a verified identity have created "
                                + quarantineMaxDestinations + " destinations, the most they may");
                    }
                } else if (destination.isComposite() && !allAdvisory(destination)) {
                    refuseAdmin(context, "create", destination, "composite destinations are for infrastructure only");
                } else if (!destination.isTemporary()) {
                    // creating a destination is using it: the identity must be able to read or write it
                    CrescoIdentity id = identityOf(context);
                    ActiveMQDestination[] each = destination.isComposite() ? destination.getCompositeDestinations() : new ActiveMQDestination[]{destination};
                    for (ActiveMQDestination d : each) {
                        if (d.isTemporary()) continue;
                        String name = d.getPhysicalName();
                        TenantPolicy.Decision r = TenantPolicy.check(id, name, TenantPolicy.Access.READ, sharedPrefixes, roleOf(id), crossTenantSinks, CrescoAuthorizationBroker.this::boundTenantOf);
                        TenantPolicy.Decision w = r.allowed ? r
                                : TenantPolicy.check(id, name, TenantPolicy.Access.WRITE, sharedPrefixes, roleOf(id), crossTenantSinks, CrescoAuthorizationBroker.this::boundTenantOf);
                        if (!w.allowed) refuseAdmin(context, "create", d, w.reason);
                    }
                }
            }
            return super.addDestination(context, destination, createIfTemporary);
        }

        @Override
        public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
            ActiveMQDestination d = info == null ? null : info.getDestination();
            if (d != null && !d.isTemporary() && (bridgeCarrier(context, stateOf(context)) || !isTrustedInfra(context))) {
                String why = administerRefusal(context, d);
                if (why != null) refuseAdmin(context, "create", d, why);
            } else if (d != null && d.isTemporary() && quarantined(context, stateOf(context))) {
                refuseAdmin(context, "create", d, "a connection without a verified identity creates no temporary destination");
            }
            super.addDestinationInfo(context, info);
        }

        @Override
        public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
            ActiveMQDestination d = info == null ? null : info.getDestination();
            String why = administerRefusal(context, d);
            if (why != null) refuseAdmin(context, "remove", d, why);
            super.removeDestinationInfo(context, info);
        }

        @Override
        public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
            String why = administerRefusal(context, destination);
            if (why != null) refuseAdmin(context, "remove", destination, why);
            super.removeDestination(context, destination, timeout);
        }

        @Override
        public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
            // a durable subscription is named by (client id, name): only the connection with that client id removes it
            if (!isTrustedInfra(context)) {
                CrescoSecurityContext sc = stateOf(context);
                String mine = context == null ? null : context.getClientId();
                if (quarantined(context, sc) || info == null || mine == null || !mine.equals(info.getClientId())) {
                    String who = identityOf(context) != null ? identityOf(context).toString() : "an unidentified connection";
                    logger.warn("DENY remove durable subscription " + (info == null ? "?" : info.getClientId() + "/" + info.getSubscriptionName())
                            + " for " + who + " : it is not that connection's");
                    throw new SecurityException("Cresco: a durable subscription can be removed only by its own connection");
                }
            }
            super.removeSubscription(context, info);
        }

        @Override
        public void stop() throws Exception {
            java.util.concurrent.ScheduledExecutorService ex = seenRefresher;
            seenRefresher = null;
            if (ex != null) ex.shutdownNow();
            super.stop();
        }

        @Override
        public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
            ConnectionContext ctx = (producerExchange != null) ? producerExchange.getConnectionContext() : null;
            if (quarantined(ctx, stateOf(ctx))) {
                long n = discarded.incrementAndGet();
                if (n == 1 || n % 1000 == 0) {
                    logger.warn("DISCARDED a message to '" + (messageSend.getDestination() == null ? "?" : messageSend.getDestination().getPhysicalName())
                            + "' from a connection without a client certificate (" + n + " discarded so far)");
                }
                return;
            }
            CrescoSecurityContext sc = stateOf(ctx);
            if (bridgeCarrier(ctx, sc)) {
                // what a bridge brings in: only its peer's own traffic. Discarded, not refused: an error on a bridge's
                // connection takes the whole bridge down, and with it the traffic it may carry
                String why = bridgeRefusal(ctx, sc, messageSend.getDestination(), TenantPolicy.Access.WRITE);
                if (why != null) {
                    bridgeOutOfScope(sc, messageSend.getDestination(), TenantPolicy.Access.WRITE, why);
                    return;
                }
            }
            enforce(ctx, messageSend.getDestination(), TenantPolicy.Access.WRITE);
            if (sc != null && sc.verified && sc.certificate && sc.identity != null) peers.touchIdentity(sc.identity);
            stampCaller(ctx, sc, messageSend);
            super.send(producerExchange, messageSend);
        }
    }

    // ---- verified callers (docs/caller-authorization.md sections 1-2) ----

    private final AtomicLong forgedStamps = new AtomicLong();
    private final AtomicLong legacyStamps = new AtomicLong();
    private volatile long lastForgedLog;

    /** Declared callers this broker stripped because the connection may not declare them. */
    public long forgedStampCount() { return forgedStamps.get(); }

    /** Messages from senders that declare no caller at all (nodes without stamping), stamped {@code agent:...}. */
    public long legacyStampCount() { return legacyStamps.get(); }

    /**
     * The caller a message carries past this broker, written as its {@code cresco_caller*} properties:
     * <ul>
     *   <li>from a forwarder (broker-internal, a verified bridge, a bridge's local side): kept -- the first broker set it;</li>
     *   <li>from this controller's own vm:// connection: kept -- the controller stamps what it originates and relays only
     *       what its consumer took from these properties;</li>
     *   <li>from a verified network node: what it declares, when {@link CallerStamps#acceptDeclared} accepts it for that
     *       node; a forged declaration is stripped (counted, logged); a sender that declares nothing at all (a node without
     *       stamping) is that node with minimal rights: {@code agent:<tenant>/<region>/<agent>}, role {@code user};</li>
     *   <li>from anything else (another in-JVM connection, no verified identity): no caller.</li>
     * </ul>
     */
    void stampCaller(ConnectionContext ctx, CrescoSecurityContext sc, Message m) throws java.io.IOException {
        if (isForwarder(ctx, sc)) {
            bridgeScope(ctx, sc, m);
            // forwarded over a verified bridge: first hand only from the bridge's own controller (its own messages)
            boolean internal = ctx == null || ctx.getConnector() == null;
            attestDirect(m, internal ? null : bridgePeerOf(ctx, sc));
            return;
        }
        if (sc != null && sc.localController) {
            attestDirect(m, ownIdentity);                     // this controller's own messages (it also relays others')
            return;
        }
        Object raw = m.getProperty(CallerStamps.P_CALLER);
        CrescoIdentity id = (sc != null && sc.verified && !isVm(ctx)) ? sc.identity : null;
        userWriter(m, id);
        if (id == null) {
            if (raw != null && !"".equals(raw)) forged(m, null, raw, "a connection without a verified node identity");
            setCaller(m, null);
            attestDirect(m, null);
            return;
        }
        if (raw == null) {
            legacyStamps.incrementAndGet();
            io.cresco.library.security.Caller legacy = io.cresco.library.security.Caller.parse(
                    "agent:" + id.getTenant() + "/" + id.getRegion() + "/" + id.getAgent(), id.getTenant(), io.cresco.library.security.Caller.ROLE_USER);
            setCaller(m, legacy);
            nodeAlias(m, id, legacy);
            attestDirect(m, null);                            // a node without stamping signs nothing either: the window's rules
            return;
        }
        if ("".equals(raw)) {
            setCaller(m, null);
            attestDirect(m, null);
            return;
        }
        Object t = m.getProperty(CallerStamps.P_TENANT), r = m.getProperty(CallerStamps.P_ROLES);
        io.cresco.library.security.Caller declared = io.cresco.library.security.Caller.parse(String.valueOf(raw),
                t == null ? null : String.valueOf(t), r == null ? null : String.valueOf(r));
        String why = declared == null ? "a malformed stamp" : CallerStamps.acceptDeclared(id, declared, superuserTenants);
        if (why != null) {
            forged(m, id, raw, why);
            setCaller(m, null);
            attestDirect(m, null);
            return;
        }
        setCaller(m, declared);
        nodeAlias(m, id, declared);
        attestDirect(m, id);
    }

    /**
     * {@link CallerStamps#P_DIRECT}, which only THIS broker writes (whatever a sender put there is replaced at every hop):
     * the node that delivered the message here -- {@code deliverer}: a verified network node, this controller, or the
     * verified controller at the other end of a bridge -- when it says it is the message's origin ({@link
     * CallerStamps#P_ORIGIN} names it) and the message still carries a stamp. A consumer of this broker reads it as "the
     * stamp came first hand from that node" ({@link MsgSigning}: an unsigned message is accepted only so). Otherwise empty.
     */
    private void attestDirect(Message m, CrescoIdentity deliverer) throws java.io.IOException {
        Object origin = m.getProperty(CallerStamps.P_ORIGIN), caller = m.getProperty(CallerStamps.P_CALLER);
        String d = "";
        if (deliverer != null && origin instanceof String && caller instanceof String && !((String) caller).isEmpty()
                && origin.equals(deliverer.getRegion() + "_" + deliverer.getAgent())) {
            d = deliverer.getTenant() + "/" + deliverer.getRegion() + "/" + deliverer.getAgent();
        }
        if (d.isEmpty() && m.getProperty(CallerStamps.P_DIRECT) == null) return;      // nothing to attest or to clear
        m.setProperty(CallerStamps.P_DIRECT, d);
    }

    // ---- inbox consumers leaving (the end of a direct link) ----

    private volatile java.util.function.Consumer<String> inboxLeftListener;

    /**
     * Told the node ({@code region_agent}) each time a consumer of that node's inbox queue leaves this broker: the node
     * stopped or moved, its connection broke, or the bridge that carried its inbox went. Set by the controller that owns
     * this broker (MsgSigning stops sending that node unsigned messages over the direct link).
     */
    public void setInboxLeftListener(java.util.function.Consumer<String> listener) { this.inboxLeftListener = listener; }

    /** The node whose inbox the queue {@code dest} is ({@code [T.<tenant>.]<region>_<agent>}), or null (a topic, a temporary queue). */
    static String inboxNode(ActiveMQDestination dest) {
        if (dest == null || !dest.isQueue() || dest.isTemporary() || dest.isComposite()) return null;
        String name = dest.getPhysicalName();
        if (name == null) return null;
        if (io.cresco.library.security.TenantNamespace.isNamespaced(name)) {
            int dot = name.indexOf('.', 2);                          // past "T.<tenant>"
            if (dot < 0 || dot + 1 >= name.length()) return null;
            name = name.substring(dot + 1);
        }
        return name.isEmpty() ? null : name;
    }

    private void inboxConsumerLeft(ActiveMQDestination dest) {
        java.util.function.Consumer<String> l = inboxLeftListener;
        if (l == null) return;
        try {
            String node = inboxNode(dest);
            if (node != null) l.accept(node);
        } catch (RuntimeException ignore) {
            // a listener never fails the removal of a consumer
        }
    }

    // ---- region aliases (GaiaKeep gfs#26): the former region a node answers at, from its own verified registration ----

    private volatile java.util.function.BiFunction<String, String, String> aliasLookup;
    private final java.util.concurrent.ConcurrentHashMap<String, Object[]> aliasCache = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long ALIAS_CACHE_MS = 30_000L;

    /**
     * Where this broker reads the {@code region_alias} of a node attached to it: (region, agent) -> the alias its region's
     * registry recorded from the node's own verified registration, or null. Set by the region controller that owns this
     * broker; unset, no network node carries an alias.
     */
    public void setRegionAliasLookup(java.util.function.BiFunction<String, String, String> lookup) {
        this.aliasLookup = lookup;
        aliasCache.clear();
    }

    /** {@code region/agent}'s recorded alias, cached for 30 s (so a message costs a map lookup, not a registry read). */
    String recordedAlias(String region, String agent) {
        java.util.function.BiFunction<String, String, String> f = aliasLookup;
        if (f == null) return null;
        String k = region + "_" + agent;
        long now = System.currentTimeMillis();
        Object[] hit = aliasCache.get(k);
        if (hit != null && (Long) hit[1] > now) return (String) hit[0];
        String a;
        try {
            a = io.cresco.library.security.Caller.validAlias(f.apply(region, agent), region);
        } catch (RuntimeException e) {
            a = null;
        }
        if (aliasCache.size() > 100_000) aliasCache.clear();
        aliasCache.put(k, new Object[]{a, now + ALIAS_CACHE_MS});
        return a;
    }

    /** A node's own stamp (controller, plugin, legacy agent) carries the alias its region recorded; an API user's none. */
    private void nodeAlias(Message m, CrescoIdentity id, io.cresco.library.security.Caller c) throws java.io.IOException {
        if (c == null || id == null || c.getKind() == io.cresco.library.security.Caller.Kind.API) return;
        String a = recordedAlias(id.getRegion(), id.getAgent());
        if (a != null) m.setProperty(CallerStamps.P_ALIAS, a);
    }

    // ---- where a bridge's traffic may go: its peer's own tenant's and region's ----

    private static final int BRIDGE_NOT_A_PEER = 1, BRIDGE_ALL = 2, BRIDGE_OWN = 3;
    private static final Object BRIDGE_ALLOWED = new Object();
    private final AtomicLong bridgeWithheld = new AtomicLong(), bridgeDiscarded = new AtomicLong();
    private volatile long lastBridgeScopeLog;

    /** Messages a bridge demanded but may not be given (withheld from it), so far. */
    public long bridgeWithheldCount() { return bridgeWithheld.get(); }

    /** Messages a bridge brought in for a destination its peer may not write (discarded), so far. */
    public long bridgeDiscardedCount() { return bridgeDiscarded.get(); }

    /**
     * Does this connection carry a bridge's traffic for a peer that presented a certificate? An inbound bridge (a verified
     * network connection, and the in-JVM channel its duplex side forwards through), and the local side of any bridge: ActiveMQ
     * opens that vm:// connection with the REMOTE broker's certificates as its transport context, so its identity here is
     * the controller at the other end. No other in-JVM connection presents a certificate.
     */
    private boolean bridgeCarrier(ConnectionContext ctx, CrescoSecurityContext sc) {
        if (sc == null || !sc.certificate || ctx == null) return false;
        if (sc.inJvm) return true;
        try {
            return ctx.isNetworkConnection() && isTrustedBridge(ctx);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * What the peer of a bridge connection is, looked at again at most every second (a message costs two field reads):
     * not a verified controller that completed discovery with this node (nothing crosses); this node's parent (a region's
     * global: the fabric below it is its subtree) or a controller of a superuser tenant (everything, as before); or
     * another controller -- a region below this global, a peer region -- whose bridge carries its OWN tenant's and
     * region's traffic only.
     */
    private int bridgeReach(CrescoSecurityContext sc) {
        long now = System.currentTimeMillis();
        int scope = sc.bridgeReach;
        if (scope != 0 && now - sc.bridgeReachAt < 1000L) return scope;
        CrescoIdentity peer = sc.verified ? sc.identity : null;
        if (peer == null || !peers.isBridgePeer(peer, sc.leaf)) {
            scope = BRIDGE_NOT_A_PEER;
        } else {
            peers.touchBridgePeer(peer);
            PeerIdentity.Expected p = parent.get();
            boolean isParent = p != null && peer.getRegion().equals(p.region) && peer.getAgent().equals(p.agent);
            scope = isParent || superuserTenants.contains(peer.getTenant()) ? BRIDGE_ALL : BRIDGE_OWN;
        }
        sc.bridgeReach = scope;
        sc.bridgeReachAt = now;
        return scope;
    }

    /**
     * Why the bridge on this connection may not carry {@code dest} -- {@code READ}: a message leaving over it (its peer's
     * demand); {@code WRITE}: a message it brings in -- or null. A bridge was exempt from tenant policy in both directions:
     * any controller that could bridge could subscribe {@code T.<another tenant>.>} or write another tenant's inboxes.
     * <ul>
     *   <li>this node's parent and a superuser tenant's controller carry everything;</li>
     *   <li>any other controller carries what {@link TenantPolicy} gives its own identity (its tenant's {@code T.<tenant>.}
     *       subtree, the fabric-shared destinations, named cross-tenant sinks; it reads advisories), and besides: it is
     *       given the messages addressed to an inbox of its OWN region under any tenant's name (a node of another tenant
     *       wrote to one of its nodes), and it may bring in a message for an inbox under a SUPERUSER tenant's name (its
     *       region's answers to the operator's nodes; only controllers and the operator's nodes consume those);</li>
     *   <li>a peer that is not a verified controller that completed discovery with this node carries nothing.</li>
     * </ul>
     * With peer_identity_mode warn or off nothing is refused, as before.
     */
    private String bridgeRefusal(ConnectionContext ctx, CrescoSecurityContext sc, ActiveMQDestination dest, TenantPolicy.Access access) {
        if (dest == null || peerMode != PeerIdentity.Mode.ENFORCE) return null;
        int scope = bridgeReach(sc);
        if (scope == BRIDGE_ALL) return null;
        if (scope == BRIDGE_NOT_A_PEER) {
            return "the bridge's peer " + (sc.identity == null ? "has no verified identity here" : sc.identity
                    + " is not a controller that completed discovery with this node");
        }
        if (dest.isComposite()) {
            for (ActiveMQDestination d : dest.getCompositeDestinations()) {
                String why = bridgeRefusal(ctx, sc, d, access);
                if (why != null) return why;
            }
            return null;
        }
        if (dest.isTemporary()) return null;
        String name = dest.getPhysicalName(), key = (access == TenantPolicy.Access.READ ? "r" : "w") + (dest.isQueue() ? "q:" : "t:") + name;
        Object cached = sc.bridgeVerdicts.get(key);
        if (cached != null) return cached == BRIDGE_ALLOWED ? null : (String) cached;
        CrescoIdentity peer = sc.identity;
        TenantPolicy.Decision d = TenantPolicy.check(peer, name, access, sharedPrefixes, TenantPolicy.Role.TENANT, crossTenantSinks,
                CrescoAuthorizationBroker.this::boundTenantOf);
        String why = d.allowed ? null : "the bridge of " + peer + " carries its own tenant's and region's traffic only (" + d.reason + ")";
        if (why != null && dest.isQueue() && io.cresco.library.security.TenantNamespace.isNamespaced(name)) {
            String[] node = CrescoNames.splitPath(ActiveBroker.bridgedInboxPath(name));
            if (node != null && access == TenantPolicy.Access.READ && node[0].equals(peer.getRegion())) why = null;
            if (node != null && access == TenantPolicy.Access.WRITE
                    && superuserTenants.contains(io.cresco.library.security.TenantNamespace.tenantOf(name))) why = null;
        }
        if (sc.bridgeVerdicts.size() > 4096) sc.bridgeVerdicts.clear();
        sc.bridgeVerdicts.put(key, why == null ? BRIDGE_ALLOWED : why);
        return why;
    }

    /** Count (and log, at most every 10 s) a message a bridge may not carry; false, for {@link #isAllowedToConsume}. */
    private boolean bridgeOutOfScope(CrescoSecurityContext sc, ActiveMQDestination dest, TenantPolicy.Access access, String why) {
        long n = access == TenantPolicy.Access.READ ? bridgeWithheld.incrementAndGet() : bridgeDiscarded.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - lastBridgeScopeLog > 10_000L) {
            lastBridgeScopeLog = now;
            logger.warn((access == TenantPolicy.Access.READ ? "WITHHELD from a bridge a message on '" : "DISCARDED a message a bridge brought in for '")
                    + (dest == null ? "?" : dest.getPhysicalName()) + "': " + why + " (" + n + " so far)");
        }
        return false;
    }

    // ---- what a bridge may carry (review Phase 3 F2) ----

    private static final Object NO_PEER = new Object();
    private final AtomicLong outOfScopeStamps = new AtomicLong();
    /** The bridges this broker opened: connector name -> the controller at the other end. */
    private final java.util.concurrent.ConcurrentHashMap<String, PeerIdentity.Expected> outboundBridges = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile java.util.function.Supplier<PeerIdentity.Expected> parent = () -> null;

    /** Stamps a bridge carried outside what it may carry, stripped so far. */
    public long outOfScopeStampCount() { return outOfScopeStamps.get(); }

    /** This node's parent controller (a region's global), whose bridge carries the whole fabric below it; null for none. */
    public void setParent(java.util.function.Supplier<PeerIdentity.Expected> parent) {
        this.parent = parent == null ? () -> null : parent;
    }

    /** A bridge this broker opened ({@code ActiveBroker}): what arrives over its local side came from {@code peer}. */
    public void registerOutboundBridge(String connectorName, PeerIdentity.Expected peer) {
        if (connectorName != null && peer != null && peer.isComplete()) outboundBridges.put(connectorName, peer);
    }

    public void unregisterOutboundBridge(String connectorName) {
        if (connectorName != null) outboundBridges.remove(connectorName);
    }

    /**
     * The controller a forwarding connection carries messages from: a verified bridge peer (an inbound bridge, its
     * certificate), or the controller this broker opened the bridge to (the local side of an outbound bridge, by its
     * connector name, which ActiveMQ puts at the head of the local connection's client id). Null when unknown.
     */
    private CrescoIdentity bridgePeerOf(ConnectionContext ctx, CrescoSecurityContext sc) {
        if (sc == null) return null;
        Object cached = sc.bridgePeer;
        if (cached != null) return cached == NO_PEER ? null : (CrescoIdentity) cached;
        CrescoIdentity peer = null;
        try {
            if (ctx.isNetworkConnection()) {
                peer = (sc.verified && sc.certificate) ? sc.identity : null;
            } else {
                String cid = ctx.getClientId();
                if (cid != null) {
                    for (java.util.Map.Entry<String, PeerIdentity.Expected> e : outboundBridges.entrySet()) {
                        if (cid.startsWith(e.getKey())) {
                            PeerIdentity.Expected x = e.getValue();
                            String t = peers.boundTenant(x.region, x.agent);
                            if (t == null) t = ownTenant;
                            peer = CrescoIdentity.of(t, x.region, x.agent, null);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            peer = null;
        }
        sc.bridgePeer = peer == null ? NO_PEER : peer;
        return peer;
    }

    /**
     * A stamp a forwarding connection carries is kept only when its bridge peer may carry it ({@link #bridgeCarryRefusal});
     * otherwise it is stripped (counted, logged). Broker-internal contexts carry what they carry.
     */
    private void bridgeScope(ConnectionContext ctx, CrescoSecurityContext sc, Message m) throws java.io.IOException {
        if (ctx == null || ctx.getConnector() == null || sc == null) return;     // broker-internal
        Object raw = m.getProperty(CallerStamps.P_CALLER);
        if (raw == null || "".equals(raw)) return;
        CrescoIdentity peer = bridgePeerOf(ctx, sc);
        String why;
        io.cresco.library.security.Caller declared = null;
        if (peer == null) {
            if (ctx.isNetworkConnection() && !sc.certificate && allowUnverifiedBridges) return;   // migration flag: as before
            why = "the bridge carrying it has no verified controller identity";
        } else {
            Object t = m.getProperty(CallerStamps.P_TENANT), r = m.getProperty(CallerStamps.P_ROLES);
            declared = io.cresco.library.security.Caller.parse(String.valueOf(raw), t == null ? null : String.valueOf(t),
                    r == null ? null : String.valueOf(r));
            PeerIdentity.Expected p = parent.get();
            boolean isParent = p != null && peer.getRegion().equals(p.region) && peer.getAgent().equals(p.agent);
            why = declared == null ? "a malformed stamp" : bridgeCarryRefusal(peer, isParent, declared, superuserTenants);
        }
        if (why == null) return;
        outOfScopeStamps.incrementAndGet();
        forged(m, peer, raw, "carried over the bridge of " + (peer == null ? "an unknown controller" : peer) + ": " + why);
        setCaller(m, null);
    }

    /**
     * May the bridge of controller {@code peer} carry {@code c}? Only a principal it may legitimately carry:
     * <ul>
     *   <li>of its own tenant, unless {@code peer} is in a superuser tenant (then of any tenant);</li>
     *   <li>a node (controller, plugin, agent) within its subtree: its own region's nodes, or -- for this node's parent
     *       (a region's global) -- any node below it; an API user (no region) of an admitted tenant;</li>
     *   <li>with the roles of its kind ({@code controller:} infra, {@code plugin:} plugin, {@code agent:} user, {@code api:}
     *       admin/user); never anonymous.</li>
     * </ul>
     * @return null when it may, else why not
     */
    static String bridgeCarryRefusal(CrescoIdentity peer, boolean peerIsParent, io.cresco.library.security.Caller c,
                                     java.util.Set<String> superuserTenants) {
        if (peer == null || peer.getTenant() == null || peer.getRegion() == null) return "no verified bridge peer";
        if (c == null || c.isAnonymous()) return "anonymous never crosses a bridge of a secured fabric";
        boolean superPeer = superuserTenants != null && superuserTenants.contains(peer.getTenant());
        if (!superPeer && !peer.getTenant().equals(c.getTenant())) {
            return "a principal of tenant " + c.getTenant() + " over a bridge of tenant " + peer.getTenant();
        }
        java.util.Set<String> roles = c.getRoles();
        switch (c.getKind()) {
            case API:
                for (String role : roles) {
                    if (!io.cresco.library.security.Caller.ROLE_ADMIN.equals(role) && !io.cresco.library.security.Caller.ROLE_USER.equals(role)) {
                        return "an API stamp with role " + role;
                    }
                }
                return null;
            case CONTROLLER:
                if (!roles.equals(java.util.Collections.singleton(io.cresco.library.security.Caller.ROLE_INFRA))) return "a controller stamp with roles " + c.getRolesCsv();
                break;
            case PLUGIN:
                if (!roles.equals(java.util.Collections.singleton(io.cresco.library.security.Caller.ROLE_PLUGIN))) return "a plugin stamp with roles " + c.getRolesCsv();
                break;
            case AGENT:
                if (!roles.equals(java.util.Collections.singleton(io.cresco.library.security.Caller.ROLE_USER))) return "an agent stamp with roles " + c.getRolesCsv();
                break;
            default:
                return "a " + c.getKind().name().toLowerCase(java.util.Locale.ROOT) + " stamp never crosses a bridge";
        }
        if (peerIsParent) return null;                       // the fabric below this node's global is the global's subtree
        return peer.getRegion().equals(c.getRegion()) ? null
                : "a node of region " + c.getRegion() + " over the bridge of region " + peer.getRegion() + " (outside its subtree)";
    }

    /**
     * The API user a dataplane message names as its writer ({@code cresco_writer}, set by wsapi on its node): kept only
     * from a verified node that may send for that user (its own tenant's users; any tenant's from a superuser tenant),
     * removed otherwise.
     */
    private void userWriter(Message m, CrescoIdentity id) throws java.io.IOException {
        Object w = m.getProperty(CallerStamps.P_WRITER), wr = m.getProperty(CallerStamps.P_WRITER_ROLES);
        if (w == null && wr == null) return;
        io.cresco.library.security.Caller user = null;
        if (w != null && wr != null && String.valueOf(w).startsWith("api:") && String.valueOf(w).indexOf('/') > 4) {
            String p = String.valueOf(w);
            user = io.cresco.library.security.Caller.parse(p, p.substring(4, p.indexOf('/')), String.valueOf(wr));
        }
        if (user != null && id != null && CallerStamps.acceptDeclared(id, user, superuserTenants) == null) return;
        if (w != null && !"".equals(w)) forged(m, id, w, "a writer the connection may not declare");
        m.removeProperty(CallerStamps.P_WRITER);
        m.removeProperty(CallerStamps.P_WRITER_ROLES);
    }

    private static void setCaller(Message m, io.cresco.library.security.Caller c) throws java.io.IOException {
        m.setProperty(CallerStamps.P_CALLER, c == null ? "" : c.getPrincipal());
        m.setProperty(CallerStamps.P_TENANT, c == null ? "" : c.getTenant());
        m.setProperty(CallerStamps.P_ROLES, c == null ? "" : c.getRolesCsv());
        m.setProperty(CallerStamps.P_ALIAS, "");             // a node's alias is set from its region's record only (nodeAlias)
    }

    private void forged(Message m, CrescoIdentity from, Object declared, String why) {
        long n = forgedStamps.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - lastForgedLog > 10_000L) {
            lastForgedLog = now;
            logger.warn("STRIPPED a forged caller '" + declared + "' on a message to '"
                    + (m.getDestination() == null ? "?" : m.getDestination().getPhysicalName()) + "' from " + from + ": " + why
                    + " (" + n + " stripped so far; the message travels without a caller)");
        }
    }
}
