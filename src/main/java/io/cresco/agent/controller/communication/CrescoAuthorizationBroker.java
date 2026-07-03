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
import org.apache.activemq.security.SecurityContext;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
 *       filtering is done by the bridge's included/excluded destinations, not here.</li>
 *   <li><b>External client</b> — its identity is taken from the connection principal (the JMS username
 *       {@code tenant|region|agent}, or a certificate subject DN under mutual TLS) and checked against
 *       {@link TenantPolicy}. A client with no resolvable identity is denied.</li>
 * </ul>
 * Gated by {@code broker_security_enabled} at the wiring site ({@code ActiveBroker}); when off, the
 * plugin is never installed and the broker behaves exactly as before.
 */
public class CrescoAuthorizationBroker implements BrokerPlugin {

    private final CLogger logger;
    private final Set<String> sharedPrefixes;
    private final boolean logAllow;

    public CrescoAuthorizationBroker(PluginBuilder plugin) {
        this.logger = plugin.getLogger(CrescoAuthorizationBroker.class.getName(), CLogger.Level.Info);
        String shared = plugin.getConfig().getStringParam("broker_shared_destinations",
                "agent.event,region.event,global.event");
        this.sharedPrefixes = new HashSet<>();
        for (String s : shared.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) this.sharedPrefixes.add(t);
        }
        this.logAllow = plugin.getConfig().getBooleanParam("broker_security_log_allow", false);
        logger.info("Cresco tenant authorization active. shared destinations=" + this.sharedPrefixes);
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
        String u = null;
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
            if (ctx.isNetworkConnection()) return true;      // broker-to-broker bridge
            if (ctx.getConnector() == null) return true;     // broker-internal
            Connection c = ctx.getConnection();
            String ra = (c != null) ? c.getRemoteAddress() : null;
            if (ra != null && ra.startsWith("vm")) return true; // local in-JVM controller
        } catch (Exception ignore) {
            // fall through -> not trusted -> enforced
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
        TenantPolicy.Decision d = TenantPolicy.check(id, name, access, sharedPrefixes);
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

    private final class AuthFilter extends BrokerFilter {
        AuthFilter(Broker next) { super(next); }

        @Override
        public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
            // Cryptographic identity binding: when mutual TLS captured a validated client certificate
            // chain (the broker's trust managers already vouched for it during the handshake), derive
            // the principal from the certificate DN and make it authoritative. This overrides any
            // client-asserted username, so a client cannot spoof its tenant/region/agent — it would
            // need the private key of a certificate the broker trusts.
            try {
                Object tc = info.getTransportContext();
                if (tc instanceof X509Certificate[]) {
                    X509Certificate[] chain = (X509Certificate[]) tc;
                    if (chain.length > 0) {
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
            super.addConnection(context, info);
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            enforce(context, info.getDestination(), TenantPolicy.Access.READ);
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
