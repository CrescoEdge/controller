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
     * appends raw ActiveMQ transport options, e.g. "tcpNoDelay=true".
     */
    public static String clientQuery(PluginBuilder plugin, String transport) {
        StringBuilder q = new StringBuilder();
        if (transport != null && transport.contains("ssl")) q.append("verifyHostName=false");
        int sb = plugin.getConfig().getIntegerParam("activemq_client_socket_buffer_size", 0);
        q.append(q.length() > 0 ? "&" : "").append("socketBufferSize=").append(sb);   // 0 -> ActiveMQ skips setsockopt
        String extra = plugin.getConfig().getStringParam("activemq_client_transport_options", "");
        if (extra != null && !extra.isBlank()) q.append('&').append(extra.trim());
        return "?" + q;
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
