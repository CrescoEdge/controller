package io.cresco.agent.controller.communication;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * Broker-layer HOP TRACING for data-plane traffic (e.g. stunnel tunnel bytes).
 *
 * <p>Data-plane bytes ride a shared topic ({@code global.event}) and are demand-forwarded by ActiveMQ's
 * network-of-brokers — the Cresco controllers at TRANSIT nodes never see those bytes, only the brokers do.
 * So the only place to record which brokers a message crosses is inside the broker itself. This filter, on
 * every {@link BrokerFilter#send}, appends THIS broker's node identity ({@code region_agent}) to a
 * {@code cresco_hops} message property — but ONLY for messages explicitly marked {@code cresco_trace=1}
 * (so untraced fabric traffic pays nothing). As the message is demand-forwarded broker→broker, each hop
 * appends itself, so it arrives at the consumer carrying the full ordered broker path it traversed. The
 * endpoint (e.g. the stunnel plugin) reads {@code cresco_hops} and PUSHES it out as a subscribable trace —
 * complete, streaming, end-to-end path visibility with no polling.
 *
 * <p>Gated by {@code net_trace_hops} (default on). The per-message cost is a cheap destination-name check;
 * property (un)marshalling only happens for messages that carry the trace marker.
 */
public class CrescoTraceBroker implements BrokerPlugin {

    static final String P_TRACE = "cresco_trace";
    static final String P_HOPS = "cresco_hops";

    private final CLogger logger;
    private final String nodePath;   // region_agent of THIS broker

    public CrescoTraceBroker(PluginBuilder plugin, String nodePath) {
        this.logger = plugin.getLogger(CrescoTraceBroker.class.getName(), CLogger.Level.Info);
        this.nodePath = nodePath;
        logger.info("Cresco hop-trace active on broker " + nodePath
                + " (stamps " + P_HOPS + " on " + P_TRACE + " data-plane messages)");
    }

    @Override
    public Broker installPlugin(Broker next) { return new TraceFilter(next); }

    private final class TraceFilter extends BrokerFilter {
        TraceFilter(Broker next) { super(next); }

        @Override
        public void send(ProducerBrokerExchange producerExchange, Message m) throws Exception {
            try {
                // cheap pre-check: only data-plane topics carry traceable tunnel bytes
                ActiveMQDestination d = (m != null) ? m.getDestination() : null;
                if (d != null) {
                    String name = d.getPhysicalName();
                    if (name != null && name.startsWith("global.event") && m.getProperty(P_TRACE) != null) {
                        Object cur = m.getProperty(P_HOPS);
                        String prev = (cur == null) ? null : cur.toString();
                        // append this broker once (guard against the same broker's send firing twice)
                        if (prev == null) {
                            m.setProperty(P_HOPS, nodePath);
                        } else if (!prev.endsWith(nodePath)) {
                            m.setProperty(P_HOPS, prev + "," + nodePath);
                        }
                    }
                }
            } catch (Exception ignore) {
                // tracing must never break delivery
            }
            super.send(producerExchange, m);
        }
    }
}
