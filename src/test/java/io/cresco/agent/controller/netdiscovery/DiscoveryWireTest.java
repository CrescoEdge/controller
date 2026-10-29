package io.cresco.agent.controller.netdiscovery;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The discovery port is unauthenticated: its decoder must deserialize nothing but a String, on the server and the
 * client side, whatever class descriptor a peer sends -- and still interoperate with old nodes, which send Strings with
 * Netty's ObjectEncoder and read with Netty's ObjectDecoder.
 */
class DiscoveryWireTest {

    /** A class whose readObject must never run. */
    public static class Canary implements Serializable {
        private static final long serialVersionUID = 1L;
        static volatile int fired;
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            fired++;
        }
    }

    /** A serializable InvocationHandler whose readObject must never run (the proxy payload). */
    public static class CanaryHandler implements InvocationHandler, Serializable {
        private static final long serialVersionUID = 1L;
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            Canary.fired++;
        }
        @Override public Object invoke(Object proxy, Method method, Object[] args) { return null; }
    }

    /** Netty's compact stream with FULL class descriptors. */
    static final class FatOut extends ObjectOutputStream {
        FatOut(OutputStream o) throws IOException { super(o); }
        @Override protected void writeStreamHeader() throws IOException { writeByte(STREAM_VERSION); }
        @Override protected void writeClassDescriptor(ObjectStreamClass d) throws IOException { write(0); super.writeClassDescriptor(d); }
    }

    @BeforeEach
    void reset() { Canary.fired = 0; }

    private static ByteBuf fatFrame(Object o) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (FatOut out = new FatOut(body)) { out.writeObject(o); }
        byte[] b = body.toByteArray();
        return Unpooled.wrappedBuffer(ByteBuffer.allocate(4 + b.length).putInt(b.length).put(b).flip());
    }

    /** What Netty's ObjectEncoder (an old node, or an attacker using it) puts on the wire: thin class descriptors. */
    private static ByteBuf nettyFrame(Object o) {
        EmbeddedChannel enc = new EmbeddedChannel(new ObjectEncoder());
        enc.writeOutbound(o);
        return enc.readOutbound();
    }

    private static Object decode(ByteBuf frame) {
        EmbeddedChannel ch = new EmbeddedChannel();
        DiscoveryWire.install(ch.pipeline());
        try {
            ch.writeInbound(frame);
            return ch.readInbound();
        } catch (Exception refused) {
            return refused;
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static Object proxyPayload() {
        return Proxy.newProxyInstance(DiscoveryWireTest.class.getClassLoader(), new Class<?>[]{Runnable.class}, new CanaryHandler());
    }

    private static Object urldns() throws Exception {
        HashMap<URL, Integer> m = new HashMap<>();
        m.put(new URL("http://127.0.0.1/"), 1);        // URLDNS shape: URL.hashCode on deserialization resolves the host
        return m;
    }

    private static Object nestedHashSets() {
        Set<Object> root = new HashSet<>(), s1 = root, s2 = new HashSet<>();
        for (int i = 0; i < 40; i++) {                  // a "billion laughs" of HashSets: 2^40 hashCode calls to read
            Set<Object> t1 = new HashSet<>(), t2 = new HashSet<>();
            t1.add("x");
            s1.add(t1); s1.add(t2); s2.add(t1); s2.add(t2);
            s1 = t1; s2 = t2;
        }
        return root;
    }

    @Test
    void noClassIsDeserializedWhateverTheDescriptor() throws Exception {
        for (Object payload : new Object[]{new Canary(), proxyPayload()}) {
            assertFalse(decode(fatFrame(payload)) instanceof String);
            assertFalse(decode(nettyFrame(payload)) instanceof String);
        }
        assertEquals(0, Canary.fired, "a class's readObject ran on the discovery port");
    }

    @Test
    void gadgetShapedPayloadsAreRefusedCheaply() throws Exception {
        for (Object payload : new Object[]{urldns(), nestedHashSets()}) {
            long t0 = System.nanoTime();
            Object r1 = decode(fatFrame(payload)), r2 = decode(nettyFrame(payload));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(r1 instanceof Exception && r2 instanceof Exception, "a " + payload.getClass().getSimpleName() + " payload decoded");
            assertTrue(ms < 2_000, "refusing a gadget payload took " + ms + " ms");
        }
    }

    @Test
    void oldNodesStillInteroperate() throws Exception {
        String small = "{\"discovery_node\":\"x\"}";
        String large = "x".repeat(200_000);                 // > 64 KiB: TC_LONGSTRING
        assertEquals(small, decode(nettyFrame(small)), "a String from an old node's ObjectEncoder did not decode");
        assertEquals(large, decode(nettyFrame(large)));
        // what we send, read by an old node's ObjectDecoder
        EmbeddedChannel ours = new EmbeddedChannel();
        DiscoveryWire.install(ours.pipeline());
        ours.writeOutbound(small);
        ByteBuf wire = ours.readOutbound();
        EmbeddedChannel old = new EmbeddedChannel(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
        old.writeInbound(wire);
        assertEquals(small, old.readInbound());
    }

    @Test
    void oversizedAndMalformedFramesFailClosed() throws Exception {
        ByteBuf huge = Unpooled.buffer().writeInt(DiscoveryWire.MAX_OBJECT_BYTES + 1).writeBytes(new byte[64]);
        assertTrue(decode(huge) instanceof Exception, "an oversized frame was accepted");
        ByteBuf trailing = nettyFrame("ok");
        int len = trailing.getInt(0);
        ByteBuf withTrailer = Unpooled.buffer().writeInt(len + 3).writeBytes(trailing.slice(4, len)).writeBytes(new byte[]{1, 2, 3});
        assertTrue(decode(withTrailer) instanceof Exception, "trailing bytes after the string were accepted");
        ByteBuf wrongHeader = Unpooled.buffer().writeInt(3).writeBytes(new byte[]{9, 9, 9});
        assertTrue(decode(wrongHeader) instanceof Exception);
    }

    @Test
    void theServerSideRefusesOverARealSocket() throws Exception {
        EventLoopGroup g = new NioEventLoopGroup(1);
        LinkedBlockingQueue<Object> got = new LinkedBlockingQueue<>();
        CountDownLatch closed = new CountDownLatch(1);
        try {
            Channel server = new ServerBootstrap().group(g).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel ch) {
                    DiscoveryWire.install(ch.pipeline());              // exactly what TCPDiscoveryEngine installs
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) { got.add(msg); }
                        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
                        @Override public void channelInactive(ChannelHandlerContext ctx) { closed.countDown(); }
                    });
                }
            }).bind("127.0.0.1", 0).sync().channel();
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            try (Socket s = new Socket("127.0.0.1", port)) {
                ByteBuf f = fatFrame(new Canary());
                byte[] b = new byte[f.readableBytes()];
                f.readBytes(b);
                s.getOutputStream().write(b);
                s.getOutputStream().flush();
                assertTrue(closed.await(5, TimeUnit.SECONDS), "the server did not close the connection");
            }
            assertTrue(got.isEmpty(), "the server passed an object on: " + got);
            assertEquals(0, Canary.fired);
            server.close().sync();
        } finally {
            g.shutdownGracefully().sync();
        }
    }

    @Test
    void theClientSideRefusesOverARealSocket() throws Exception {
        EventLoopGroup g = new NioEventLoopGroup(1);
        LinkedBlockingQueue<Object> got = new LinkedBlockingQueue<>();
        try (ServerSocket evil = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            Thread t = new Thread(() -> {
                try (Socket c = evil.accept()) {
                    ByteBuf f = fatFrame(proxyPayload());
                    byte[] b = new byte[f.readableBytes()];
                    f.readBytes(b);
                    c.getOutputStream().write(b);
                    c.getOutputStream().flush();
                    Thread.sleep(500);
                } catch (Exception ignore) { }
            });
            t.start();
            Channel client = new Bootstrap().group(g).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel ch) {
                    DiscoveryWire.install(ch.pipeline());              // exactly what TCPDiscoveryStatic installs
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) { got.add(msg); }
                        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
                    });
                }
            }).connect("127.0.0.1", evil.getLocalPort()).sync().channel();
            assertTrue(client.closeFuture().await(5, TimeUnit.SECONDS), "the client did not close the connection");
            t.join(5000);
            assertTrue(got.isEmpty(), "the client passed an object on: " + got);
            assertEquals(0, Canary.fired);
        } finally {
            g.shutdownGracefully().sync();
        }
    }
}
