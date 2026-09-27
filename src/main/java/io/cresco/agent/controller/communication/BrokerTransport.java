package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transport options shared by every broker connection the controller makes: the broker's connector,
 * agent/region client connections (failover URIs) and network bridges.
 *
 * <p><b>Socket buffers (measured on the UK DGX, 2026-09-26):</b> Linux caps an explicit SO_RCVBUF /
 * SO_SNDBUF at net.core.rmem_max / wmem_max (256 KiB / 208 KiB on the DGX compute nodes), and an
 * explicit set turns kernel autotuning off for that socket (autotuning would grow to tcp_rmem max,
 * 6 MiB there). ActiveMQ sets them AFTER connect, so the old defaults pinned every client socket at
 * ActiveMQ's 64 KiB and the broker's at the capped 2 MiB request. With 0 ActiveMQ skips the setsockopt
 * and the kernel autotunes: one 4 MiB stream through the hub went 197 -> 449 MB/s, and a
 * request/response exchange 73 -> 176 MB/s (before the NIO transport fix, which adds the rest).
 * A positive value still pins the buffers, for deployments that need to bound per-socket memory.
 */
public final class BrokerTransport {

    private static final AtomicBoolean INSTALLED = new AtomicBoolean();

    private BrokerTransport() { }

    /** socketBufferSize for the broker's transport connector (0 = kernel autotuning). */
    public static int connectorSocketBufferSize(PluginBuilder plugin) {
        return plugin.getConfig().getIntegerParam("activemq_socket_buffer_size", 0);
    }

    /**
     * Query string for a client transport URI inside failover:(...) or static:(...), e.g.
     * "?verifyHostName=false&socketBufferSize=0". activemq_client_transport_options (default empty)
     * appends raw ActiveMQ transport options, e.g. "tcpNoDelay=true", except secret-looking ones
     * (see {@link #clientOptions}).
     */
    public static String clientQuery(PluginBuilder plugin, String transport) {
        StringBuilder q = new StringBuilder();
        if (transport != null && transport.contains("ssl")) q.append("verifyHostName=false");
        int sb = plugin.getConfig().getIntegerParam("activemq_client_socket_buffer_size", 0);
        q.append(q.length() > 0 ? "&" : "").append("socketBufferSize=").append(sb);   // 0 -> ActiveMQ skips setsockopt
        java.util.List<String> dropped = new java.util.ArrayList<>();
        String extra = clientOptions(plugin.getConfig().getStringParam("activemq_client_transport_options", ""), dropped);
        if (!extra.isEmpty()) q.append('&').append(extra);
        if (!dropped.isEmpty()) {
            plugin.getLogger(BrokerTransport.class.getName(), CLogger.Level.Info).warn(
                    "activemq_client_transport_options: not passing secret-looking option(s) " + dropped + " in broker URIs:"
                    + " ActiveMQ logs broker URIs verbatim, and its tcp/ssl/nio/nio+ssl client transports accept no such"
                    + " option (they refuse it as an invalid connect parameter); broker TLS material comes from Cresco's"
                    + " certificate manager");
        }
        return "?" + q;
    }

    /**
     * The URI to hand to ActiveMQ, with every secret-looking query parameter removed (names are added to
     * {@code dropped}): the last check before a URI reaches ActiveMQ, whose FailoverTransport and bridge
     * logs print URIs verbatim. Handles composite URIs (failover:(a?x=1,b?y=2)?z=3). Returns the same string
     * when nothing is removed.
     */
    public static String withoutSecretParams(String uri, java.util.List<String> dropped) {
        if (uri == null || uri.indexOf('?') < 0) return uri;
        StringBuilder out = new StringBuilder(uri.length());
        boolean removed = false;
        int i = 0, n = uri.length();
        while (i < n) {
            char c = uri.charAt(i);
            if (c != '?') { out.append(c); i++; continue; }
            int j = i + 1;
            while (j < n && "(),".indexOf(uri.charAt(j)) < 0) j++;   // a query ends at ( ) , or the end
            StringBuilder kept = new StringBuilder();
            for (String p : uri.substring(i + 1, j).split("&", -1)) {
                if (p.isEmpty()) continue;
                int eq = p.indexOf('=');
                String name = eq < 0 ? p : p.substring(0, eq);
                if (io.cresco.agent.db.ConfigRedaction.isSecretKey(name)) {
                    removed = true;
                    if (dropped != null) dropped.add(name);
                    continue;
                }
                kept.append(kept.length() > 0 ? "&" : "").append(p);
            }
            if (kept.length() > 0) out.append('?').append(kept);
            i = j;
        }
        return removed ? out.toString() : uri;
    }

    /** {@link #withoutSecretParams}, logging (names only) anything it had to remove. */
    public static String forActiveMQ(String uri, CLogger logger) {
        java.util.List<String> dropped = new java.util.ArrayList<>();
        String clean = withoutSecretParams(uri, dropped);
        if (!dropped.isEmpty() && logger != null) {
            logger.warn("removed secret-looking parameter(s) " + dropped + " from a broker URI before handing it to"
                    + " ActiveMQ (it logs URIs verbatim); TLS secrets come from Cresco's certificate manager");
        }
        return clean;
    }

    /**
     * The raw activemq_client_transport_options (name=value pairs joined by '&') without any option whose
     * name is secret-looking ({@link io.cresco.agent.db.ConfigRedaction#isSecretKey}: keyStorePassword,
     * trustStorePassword, password, *secret*, ...). Their names (never values) are added to {@code dropped}.
     * Those options must never reach a broker URI: ActiveMQ's own FailoverTransport and network-bridge logs
     * ("Failed to connect to [nio+ssl://...?...]", "Transport ... failed") print URIs verbatim through a
     * logger Cresco's redaction does not see, and none of the client transports Cresco uses accepts one.
     */
    static String clientOptions(String raw, java.util.List<String> dropped) {
        if (raw == null || raw.isBlank()) return "";
        StringBuilder kept = new StringBuilder();
        for (String opt : raw.trim().split("&")) {
            String o = opt.trim();
            if (o.startsWith("?")) o = o.substring(1);
            if (o.isEmpty()) continue;
            int eq = o.indexOf('=');
            String name = eq < 0 ? o : o.substring(0, eq);
            if (io.cresco.agent.db.ConfigRedaction.isSecretKey(name)) {
                if (dropped != null) dropped.add(name);
                continue;
            }
            kept.append(kept.length() > 0 ? "&" : "").append(o);
        }
        return kept.toString();
    }

    /**
     * Install the fixed broker-side nio+ssl transport ({@link CrescoNioSslTransportFactory}) for this
     * JVM's embedded ActiveMQ, once, before the broker's connector binds. activemq_nio_ssl_fixed=false
     * keeps ActiveMQ's stock NIOSSLTransport.
     */
    public static void installTransportFactories(PluginBuilder plugin, CLogger logger) {
        CrescoNioSslTransportFactory.writeBatchBytes =
                Math.max(0, plugin.getConfig().getIntegerParam("activemq_nio_ssl_write_batch", 256 * 1024));
        CrescoNioSslTransportFactory.readBufferBytes =
                Math.max(0, plugin.getConfig().getIntegerParam("activemq_nio_ssl_read_buffer", 256 * 1024));
        if (!plugin.getConfig().getBooleanParam("activemq_nio_ssl_fixed", true)) return;
        if (INSTALLED.compareAndSet(false, true)) {
            TransportFactory.registerTransportFactory("nio+ssl", new CrescoNioSslTransportFactory());
            if (logger != null) {
                logger.info("nio+ssl broker transport: selector-wait writes (batch " + CrescoNioSslTransportFactory.writeBatchBytes
                        + " B), batched TLS reads (" + CrescoNioSslTransportFactory.readBufferBytes + " B)");
            }
        }
    }
}
