package io.cresco.agent.controller.communication;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.NIOOutputStream;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.nio.NIOSSLTransportFactory;
import org.apache.activemq.transport.nio.NIOSSLTransportServer;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * The broker side of the {@code nio+ssl} connector, with the two throughput defects of ActiveMQ
 * 6.2.7's {@link NIOSSLTransport} fixed. Installed for the {@code nio+ssl} scheme by
 * {@link BrokerTransport#installTransportFactories} (config {@code activemq_nio_ssl_fixed}, default
 * true). The client side of {@code nio+ssl} is ActiveMQ's blocking SslTransport either way; only the
 * broker's accepted connections change.
 *
 * <p>Measured cross-host on the UK DGX (IPoIB, 4 MiB messages, one connection, eval/dgx/dpbench in
 * GaiaKeep/gfs wip/dataplane): stock 449 MB/s streaming, write fix 474, write + read fix 838; eight
 * connections 2810 -> 4202 MB/s.
 * <ul>
 *   <li><b>Write:</b> NIOOutputStream.write(ByteBuffer) sleeps 1, 2, 4 ... 1000 ms whenever a
 *   non-blocking write returns 0 (send buffer full), and writes one 16 KiB TLS record per syscall. The
 *   broker's dispatch threads spent 39-41% of their samples in that sleep. Here a write that returns
 *   0 waits for OP_WRITE on a private selector, and many TLS records go out per syscall.</li>
 *   <li><b>Read:</b> secureRead reads one packet-sized TLS record per syscall, then waits for the next
 *   selector wakeup; one NIO worker stayed ~95% busy at 474 MB/s. Here, after the handshake, one read
 *   fills up to {@code activemq_nio_ssl_read_buffer} bytes of records, and later calls unwrap them
 *   without a syscall. The handshake keeps the stock path.</li>
 * </ul>
 */
public class CrescoNioSslTransportFactory extends NIOSSLTransportFactory {

    /** Bytes of TLS records packed into one channel write. */
    static volatile int writeBatchBytes = 256 * 1024;
    /** Bytes of TLS records one channel read may bring in (0 = stock one-record reads). */
    static volatile int readBufferBytes = 256 * 1024;

    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory ssf) throws IOException, URISyntaxException {
        final SSLContext ctx = context;
        return new NIOSSLTransportServer(ctx, this, location, ssf) {
            @Override
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                FixedTransport t = new FixedTransport(format, socket, null, null, null);
                if (ctx != null) t.setSslContext(ctx);
                t.setNeedClientAuth(isNeedClientAuth());
                t.setWantClientAuth(isWantClientAuth());
                return t;
            }
        };
    }

    @Override
    public TcpTransport createTransport(WireFormat wf, Socket socket, SSLEngine engine, TcpTransport.InitBuffer ib, ByteBuffer in) throws IOException {
        return new FixedTransport(wf, socket, engine, ib, in);
    }

    /** The accepted-connection transport: stock NIOSSLTransport with the fixed output stream and batched reads. */
    static final class FixedTransport extends NIOSSLTransport {

        FixedTransport(WireFormat wf, Socket s, SSLEngine e, TcpTransport.InitBuffer ib, ByteBuffer in) throws IOException {
            super(wf, s, e, ib, in);
        }

        @Override
        protected void initializeStreams() throws IOException {
            super.initializeStreams();
            SelectorWaitOutputStream o = new SelectorWaitOutputStream(channel);
            o.setEngine(sslEngine);
            this.dataOut = new DataOutputStream(o);
            this.buffOut = o;
        }

        @Override
        protected synchronized int secureRead(ByteBuffer plain) throws Exception {
            int want = readBufferBytes;
            if (want <= 0 || handshakeInProgress
                    || sslEngine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                return super.secureRead(plain);
            }
            if (inputBuffer.capacity() < want) {                 // at rest: write mode, data in [0, position)
                ByteBuffer big = ByteBuffer.allocate(Math.max(want, 2 * sslSession.getPacketBufferSize()));
                inputBuffer.flip();
                big.put(inputBuffer);
                inputBuffer = big;
            }
            // read only when no complete record is buffered: a full buffer can never stall the loop
            if (inputBuffer.position() == 0 || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                int n = channel.read(inputBuffer);
                if (n == -1) {
                    sslEngine.closeInbound();
                    if (inputBuffer.position() == 0 || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) return -1;
                } else if (n == 0) {
                    return 0;
                }
            }
            plain.clear();
            inputBuffer.flip();
            SSLEngineResult res;
            do {                                                 // records without application bytes are skipped
                res = sslEngine.unwrap(inputBuffer, plain);
            } while (res.getStatus() == SSLEngineResult.Status.OK && res.bytesProduced() == 0 && inputBuffer.hasRemaining()
                    && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING);
            status = res.getStatus();
            handshakeStatus = res.getHandshakeStatus();
            if (status == SSLEngineResult.Status.CLOSED) {
                sslEngine.closeInbound();
                return -1;
            }
            inputBuffer.compact();
            plain.flip();
            if (plain.remaining() == 0 && status == SSLEngineResult.Status.OK && inputBuffer.position() > 0) {
                status = SSLEngineResult.Status.BUFFER_UNDERFLOW;  // read next time instead of spinning
            }
            return plain.remaining();
        }
    }

    /**
     * NIOOutputStream whose write(ByteBuffer) waits for the socket to become writable instead of
     * sleeping, and packs many TLS records into one channel write. Every buffered path of the parent
     * ends in write(ByteBuffer), so this is the only method that changes.
     */
    static final class SelectorWaitOutputStream extends NIOOutputStream {
        private final SocketChannel ch;
        private SSLEngine eng;
        private volatile long ts = -1;
        private Selector sel;
        private ByteBuffer out;

        SelectorWaitOutputStream(SocketChannel ch) {
            super(ch);
            this.ch = ch;
        }

        @Override public void setEngine(SSLEngine engine) { super.setEngine(engine); this.eng = engine; }
        @Override public boolean isWriting() { return ts > 0; }
        @Override public long getWriteTimestamp() { return ts; }

        private void wrapInto(ByteBuffer data, ByteBuffer dst, int pkt) throws IOException {
            do {                                                 // at least once: a 0-byte write flushes handshake data
                SSLEngineResult r = eng.wrap(data, dst);
                if (r.getStatus() == SSLEngineResult.Status.CLOSED) throw new EOFException("SSLEngine closed");
                if (r.getStatus() != SSLEngineResult.Status.OK || (r.bytesConsumed() == 0 && r.bytesProduced() == 0)) break;
            } while (data.hasRemaining() && dst.remaining() >= pkt);
        }

        private void awaitWritable() throws IOException {
            if (sel == null) {
                sel = Selector.open();
                ch.register(sel, SelectionKey.OP_WRITE);
            }
            sel.select(1000);
            sel.selectedKeys().clear();
        }

        @Override
        protected synchronized void write(ByteBuffer data) throws IOException {
            ByteBuffer plain;
            int pkt = 0;
            if (eng != null) {
                pkt = eng.getSession().getPacketBufferSize();
                int cap = Math.max(pkt, writeBatchBytes);
                if (out == null || out.capacity() < cap) out = ByteBuffer.allocateDirect(cap);
                plain = out;
                plain.clear();
                wrapInto(data, plain, pkt);
                plain.flip();
            } else {
                plain = data;
            }
            ts = System.currentTimeMillis();
            try {
                while (plain.hasRemaining()) {
                    if (ch.write(plain) == 0) awaitWritable();
                    if (eng != null && !plain.hasRemaining() && data.hasRemaining()) {
                        plain.clear();
                        wrapInto(data, plain, pkt);
                        plain.flip();
                    }
                }
            } finally {
                ts = -1;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (sel != null) try { sel.close(); } catch (Exception ignore) { }
            }
        }
    }
}
