package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;

public class TCPDiscoveryEngineHandler extends ChannelInboundHandlerAdapter {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private DiscoveryProcessor discoveryProcessor;
    private DiscoveryAuth.Msg hello, challenge;             // this connection's v2 exchange
    /** Where requests are answered (off the I/O thread, in turn per address); null: on the calling thread (tests). */
    private final DiscoveryJobs jobs;
    /** A request of this connection is queued or being answered: the next frame is read only after its answer was written. */
    private final java.util.concurrent.atomic.AtomicBoolean inFlight = new java.util.concurrent.atomic.AtomicBoolean();

    public TCPDiscoveryEngineHandler(ControllerEngine controllerEngine) {
        this(controllerEngine, null);
    }

    public TCPDiscoveryEngineHandler(ControllerEngine controllerEngine, DiscoveryJobs jobs) {
        this.jobs = jobs;
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(TCPDiscoveryEngineHandler.class.getName(),CLogger.Level.Info);
        this.gson = new Gson();
        this.discoveryProcessor = new DiscoveryProcessor(controllerEngine);

    }

    /**
     * A request frame, read by an I/O thread. Answering it checks certificates, may sign, and records a joiner durably
     * (a disk sync): none of that runs here. It is queued under the peer's address ({@link DiscoveryJobs}); when the
     * queue would make it wait too long the peer is told at once that this responder is busy and when to come back.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (jobs == null) {
            answer(ctx, msg, null);
            return;
        }
        if (!inFlight.compareAndSet(false, true)) {
            // no discovery exchange sends a second frame before the first was answered
            DiscoveryGuard.warnLimited(logger, "discovery-tcp-refused", "Discovery: closed a connection from " + remote(ctx)
                    + ": it sent a request before its previous one was answered");
            ctx.close();
            return;
        }
        DiscoveryGuard.Admission.working(ctx, true);
        long retryMs = jobs.submit(remote(ctx), msg instanceof String ? ((String) msg).length() : 0, () -> {
            Request rq = new Request();
            try {
                // a peer that gave up while its request waited is not answered: no work for a connection that is gone
                if (ctx.channel().isActive()) answer(ctx, msg, rq);
            } finally {
                if (!rq.released) written(ctx);                  // nothing was written for it (refused, or nothing to answer)
            }
        });
        if (retryMs > 0) {
            DiscoveryGuard.warnLimited(logger, "discovery-busy", "Discovery: busy: told " + remote(ctx) + " to come back in " + retryMs
                    + " ms (" + jobs.queued() + " request(s) of " + jobs.addressesWaiting() + " address(es) wait for " + jobs.workers()
                    + " worker(s); " + jobs.busy() + " told so far; discovery_workers, discovery_max_wait_ms)");
            written(ctx);
            ctx.writeAndFlush(busyFrame(gson, plugin, retryMs, "the responder's queue is full")).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        }
    }

    /** One queued request: whether the connection was already released for its next frame (by the thread answering it). */
    private static final class Request { boolean released; }

    /** This connection's request is answered (or given up): its next frame may be read, and it may be idle again. */
    private void written(ChannelHandlerContext ctx) {
        DiscoveryGuard.Admission.working(ctx, false);
        inFlight.set(false);
    }

    /** Before the answer of {@code rq} is written (the peer's next frame follows it): release the connection, once. */
    private void release(ChannelHandlerContext ctx, Request rq) {
        if (rq == null || rq.released) return;
        rq.released = true;
        written(ctx);
    }

    /**
     * The frame that tells a peer this responder is busy and after how many milliseconds to ask again: a v2 message of step
     * {@code busy} (a v2 client reads it) and the same time as a plain parameter (a client that sent a locator reads that).
     * It is not authenticated -- nothing of an exchange exists yet -- and tells a peer only to wait.
     */
    static String busyFrame(Gson gson, PluginBuilder plugin, long retryMs, String why) {
        DiscoveryAuth.Msg b = new DiscoveryAuth.Msg();
        b.step = "busy";
        b.retry = retryMs;
        b.reason = why;
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Discovery busy.");
        me.setParam(DiscoveryAuth.PARAM, gson.toJson(b));
        me.setParam(DiscoveryAuth.BUSY_PARAM, String.valueOf(retryMs));
        return gson.toJson(me);
    }

    private void answer(ChannelHandlerContext ctx, Object msg, Request rq) {

        DiscoveryNode discoveryNode = null;
        try {
            // pre-auth: the frame is bounded by DiscoveryWire, the gunzipped node by DiscoveryGuard (never the
            // 128 MiB MsgEvent.getCompressedParam cap)
            MsgEvent rme = gson.fromJson((String)msg, MsgEvent.class);
            if (rme != null && rme.getParam(DiscoveryAuth.PARAM) != null) {
                readV2(ctx, rme.getParam(DiscoveryAuth.PARAM), rq);
                return;
            }
            discoveryNode = DiscoveryGuard.discoveryNode(gson, rme);
        } catch (Exception ex) {
            DiscoveryGuard.warnLimited(logger, "discovery-tcp-refused", "Discovery: refused a request from "
                    + remote(ctx) + ": " + ex.getMessage());
            if (ctx != null) ctx.close();
            return;
        }

        if (discoveryNode != null) {

            String localAddress = ((InetSocketAddress)ctx.channel().localAddress()).getAddress().getHostAddress();
            int localPort = ((InetSocketAddress)ctx.channel().localAddress()).getPort();
            String remoteAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
            int remotePort = ((InetSocketAddress)ctx.channel().remoteAddress()).getPort();


            discoveryNode = discoveryProcessor.processIncomingBroadCastDiscovery(discoveryNode, localAddress, localPort, remoteAddress, remotePort);

            MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Static discovery response.");
            me.setCompressedParam("discovery_node",gson.toJson(discoveryNode));

            if (jobs != null) {
                release(ctx, rq);
                ctx.writeAndFlush(gson.toJson(me));              // (not an I/O thread: nothing flushes for us)
            } else {
                ctx.write(gson.toJson(me));
            }


            //MsgEvent me = gson.fromJson((String)msg, MsgEvent.class);
            //    MsgEvent rme = processMessage(me);
            //ctx.write(gson.toJson(rme));
        }

    }

    private void readV2(ChannelHandlerContext ctx, String json, Request rq) {
        String remote = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        DiscoveryAuth.Msg m;
        try {
            m = gson.fromJson(json, DiscoveryAuth.Msg.class);
        } catch (Exception e) {
            ctx.close();
            return;
        }
        DiscoveryAuth.Msg reply = null;
        boolean last = true;
        if (m != null && "hello".equals(m.step) && hello == null) {
            reply = discoveryProcessor.serverChallenge(m, remote);
            if (reply != null) { hello = m; challenge = reply; last = false; }
        } else if (m != null && "proof".equals(m.step) && hello != null && challenge != null) {
            reply = discoveryProcessor.serverResult(hello, challenge, m, remote);
            hello = null; challenge = null;                  // one proof per challenge
        }
        if (reply == null) { ctx.close(); return; }
        MsgEvent me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Discovery v2.");
        me.setParam(DiscoveryAuth.PARAM, gson.toJson(reply));
        release(ctx, rq);                                        // before the write: the peer's next frame follows it
        if (last) ctx.writeAndFlush(gson.toJson(me)).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        else ctx.writeAndFlush(gson.toJson(me));
    }

    private static String remote(ChannelHandlerContext ctx) {
        return ctx == null ? "?" : DiscoveryGuard.addressKey(ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0");
        ctx.flush();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0.0");
        //ctx.fireChannelInactive();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 0.1");
        //ctx.close();
        //System.out.println("channelReadComplete Thread" + Thread.currentThread() + " 1");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // pre-auth input (an oversized or refused frame): one rate-limited line, no stack trace per peer frame
        DiscoveryGuard.warnLimited(logger, "discovery-tcp-error", "Discovery: closed a connection from " + remote(ctx)
                + ": " + cause.getClass().getSimpleName() + ": " + cause.getMessage());
        logger.debug("TCPDiscoveryEngineHandler.exceptionCaught", cause);
        ctx.close();
    }


}
