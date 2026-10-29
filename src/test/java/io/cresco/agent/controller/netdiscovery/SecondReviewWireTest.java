package io.cresco.agent.controller.netdiscovery;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.serialization.ObjectDecoder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** Second adversarial review: the discovery wire's "Strings only" class resolver. */
class SecondReviewWireTest {

    /** Any class on the classpath with a readObject: its side effect shows it was deserialized. */
    public static class Canary implements Serializable {
        private static final long serialVersionUID = 1L;
        static volatile boolean fired;
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            fired = true;
        }
    }

    /** Netty's compact stream format, but with a FAT (full) class descriptor instead of the thin class-name one. */
    static final class FatDescriptorOut extends ObjectOutputStream {
        FatDescriptorOut(OutputStream o) throws IOException { super(o); }
        @Override protected void writeStreamHeader() throws IOException { writeByte(STREAM_VERSION); }
        @Override protected void writeClassDescriptor(ObjectStreamClass d) throws IOException { write(0); super.writeClassDescriptor(d); }
    }

    @Test
    void theDiscoveryDecoderDeserializesNothingButStrings() throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (FatDescriptorOut out = new FatDescriptorOut(body)) { out.writeObject(new Canary()); }
        byte[] b = body.toByteArray();
        ByteBuffer frame = ByteBuffer.allocate(4 + b.length).putInt(b.length).put(b);
        frame.flip();
        // exactly the decoder TCPDiscoveryEngine (server, unauthenticated port) and TCPDiscoveryStatic install
        EmbeddedChannel ch = new EmbeddedChannel(new ObjectDecoder(DiscoveryWire.MAX_OBJECT_BYTES, DiscoveryWire.STRINGS_ONLY));
        Object decoded = null;
        try { ch.writeInbound(Unpooled.wrappedBuffer(frame)); decoded = ch.readInbound(); } catch (Exception e) { System.out.println("decode threw " + e); }
        System.out.println("STRINGS_ONLY decoder produced: " + (decoded == null ? null : decoded.getClass().getName())
                + "; Canary.readObject ran = " + Canary.fired);
        assertFalse(Canary.fired, "an unauthenticated peer had an arbitrary Serializable class deserialized (readObject ran) despite STRINGS_ONLY");
    }
}
