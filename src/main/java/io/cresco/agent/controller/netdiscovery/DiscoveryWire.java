package io.cresco.agent.controller.netdiscovery;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

/**
 * The TCP discovery wire: one Java-serialized {@code String} (JSON inside) per frame, framed as Netty's
 * {@code ObjectEncoder} frames it (4-byte length, then Netty's "compact" serialization stream: a one-byte header and
 * the object). The discovery port is unauthenticated and open on every controller.
 *
 * <p><b>Security.</b> Netty's {@code ObjectDecoder} deserializes whatever class a peer names; even with a restrictive
 * {@code ClassResolver} its {@code CompactObjectInputStream.resolveClass} catches the resolver's
 * {@code ClassNotFoundException} and falls back to {@code ObjectInputStream.resolveClass}, so a full class descriptor
 * still loads any class on the classpath and runs its {@code readObject} -- before any authentication. This decoder never
 * resolves a class at all: a {@code String} is {@code TC_STRING} on the wire and needs no class descriptor, so
 * {@link StringOnlyInputStream} refuses every class descriptor, proxy descriptor and class lookup, a serial filter
 * allows nothing but {@code String} (no arrays, depth 1), the frame is capped at {@link #MAX_OBJECT_BYTES}, and anything
 * that is not exactly one {@code String} fails the decode (the channel is closed). The encoder is unchanged, so the wire
 * is the same: old nodes (Netty {@code ObjectEncoder}/{@code ObjectDecoder}) and this decoder interoperate.
 */
final class DiscoveryWire {

    private DiscoveryWire() { }

    /**
     * Largest discovery frame accepted. A certificate exchange is a few KiB; the cap also bounds what one connection can
     * make the listener buffer before the frame is complete (see {@link DiscoveryGuard} for the connection limits).
     */
    static final int MAX_OBJECT_BYTES = 256 * 1024;

    /** Netty's compact stream header: one byte, ObjectStreamConstants.STREAM_VERSION. */
    private static final int STREAM_VERSION = 5;

    /** The decode refusal: a class was named on the discovery wire. */
    static final class RefusedClassException extends SecurityException {
        RefusedClassException(String what) {
            super("discovery accepts only java.lang.String; refused " + what);
        }
    }

    /**
     * For any code that still builds Netty's {@code ObjectDecoder}: refuses every class with an UNCHECKED exception, which
     * Netty does not catch (it catches {@code ClassNotFoundException} and falls back to the default loader). Strings need
     * no class lookup, so nothing legitimate is refused. Prefer {@link #install}.
     */
    static final ClassResolver STRINGS_ONLY = className -> {
        throw new RefusedClassException("class " + className);
    };

    /** Install the discovery codec (String-only decoder, Netty's encoder) on a server or client pipeline. */
    static void install(ChannelPipeline p) {
        p.addLast(new ObjectEncoder(), new StringOnlyDecoder());
    }

    /** Netty's length-field framing, then {@link #readString}. */
    static final class StringOnlyDecoder extends LengthFieldBasedFrameDecoder {
        StringOnlyDecoder() {
            super(MAX_OBJECT_BYTES, 0, 4, 0, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) return null;
            try {
                return readString(frame);
            } finally {
                frame.release();
            }
        }
    }

    /** Exactly one String from a compact-stream frame body, or an exception. */
    static String readString(ByteBuf frame) throws IOException {
        try (StringOnlyInputStream in = new StringOnlyInputStream(new ByteBufInputStream(frame))) {
            Object o;
            try {
                o = in.readObject();
            } catch (ClassNotFoundException e) {
                throw new RefusedClassException("a class lookup");
            }
            if (!(o instanceof String)) throw new RefusedClassException("a non-String object");
            if (in.available() > 0 || frame.isReadable()) throw new StreamCorruptedException("trailing bytes after the discovery string");
            return (String) o;
        }
    }

    private static final ObjectInputFilter FILTER = info -> {
        Class<?> c = info.serialClass();
        if (c != null && c != String.class) return ObjectInputFilter.Status.REJECTED;
        if (info.arrayLength() >= 0) return ObjectInputFilter.Status.REJECTED;
        if (info.depth() > 1 || info.references() > 1 || info.streamBytes() > MAX_OBJECT_BYTES) return ObjectInputFilter.Status.REJECTED;
        return ObjectInputFilter.Status.ALLOWED;
    };

    /** Reads Netty's compact stream but resolves no class at all. */
    static final class StringOnlyInputStream extends ObjectInputStream {
        StringOnlyInputStream(InputStream in) throws IOException {
            super(in);
            setObjectInputFilter(FILTER);
        }

        @Override
        protected void readStreamHeader() throws IOException {
            int version = readByte() & 0xFF;
            if (version != STREAM_VERSION) throw new StreamCorruptedException("unsupported discovery stream version " + version);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException {
            throw new InvalidClassException("discovery accepts only java.lang.String; refused a class descriptor");
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException {
            throw new InvalidClassException(desc == null ? "?" : desc.getName(), "discovery accepts only java.lang.String");
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException {
            throw new InvalidClassException("proxy", "discovery accepts only java.lang.String");
        }
    }
}
