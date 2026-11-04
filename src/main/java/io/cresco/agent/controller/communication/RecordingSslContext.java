package io.cresco.agent.controller.communication;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 * The TLS context of this node's OUTBOUND broker bridges: the real context, whose client sockets record themselves in
 * {@link OutboundTlsConnections} when EVERY handshake completes -- a full one, and a resumed one (a TLS 1.3 session
 * ticket), which never calls the trust manager. A bridge that reconnected by resuming its session is therefore still
 * known, and a revocation of its peer (its key, its name, or a CA in its chain) closes it (round-2 review N5).
 */
final class RecordingSslContext extends SSLContext {

    private RecordingSslContext(Spi spi, SSLContext real) {
        super(spi, real.getProvider(), real.getProtocol());
    }

    /** {@code real}, recording its client sockets in {@code outbound} under {@code endpoint}. */
    static SSLContext wrap(SSLContext real, OutboundTlsConnections outbound, String endpoint) {
        return new RecordingSslContext(new Spi(real, outbound, endpoint), real);
    }

    private static final class Spi extends SSLContextSpi {
        private final SSLContext real;
        private final OutboundTlsConnections outbound;
        private final String endpoint;

        Spi(SSLContext real, OutboundTlsConnections outbound, String endpoint) {
            this.real = real; this.outbound = outbound; this.endpoint = endpoint;
        }

        @Override protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr) throws java.security.KeyManagementException {
            real.init(km, tm, sr);
        }
        @Override protected SSLSocketFactory engineGetSocketFactory() { return new Recording(real.getSocketFactory(), outbound, endpoint); }
        @Override protected SSLServerSocketFactory engineGetServerSocketFactory() { return real.getServerSocketFactory(); }
        @Override protected SSLEngine engineCreateSSLEngine() { return real.createSSLEngine(); }
        @Override protected SSLEngine engineCreateSSLEngine(String host, int port) { return real.createSSLEngine(host, port); }
        @Override protected SSLSessionContext engineGetServerSessionContext() { return real.getServerSessionContext(); }
        @Override protected SSLSessionContext engineGetClientSessionContext() { return real.getClientSessionContext(); }
        @Override protected SSLParameters engineGetDefaultSSLParameters() { return real.getDefaultSSLParameters(); }
        @Override protected SSLParameters engineGetSupportedSSLParameters() { return real.getSupportedSSLParameters(); }
    }

    /** A client socket factory whose sockets record their peer at every completed handshake. */
    private static final class Recording extends SSLSocketFactory {
        private final SSLSocketFactory real;
        private final OutboundTlsConnections outbound;
        private final String endpoint;

        Recording(SSLSocketFactory real, OutboundTlsConnections outbound, String endpoint) {
            this.real = real; this.outbound = outbound; this.endpoint = endpoint;
        }

        private Socket watch(Socket s) {
            if (s instanceof SSLSocket) {
                HandshakeCompletedListener l = e -> {
                    try {
                        Certificate[] peer = e.getPeerCertificates();
                        X509Certificate[] chain = new X509Certificate[peer.length];
                        for (int i = 0; i < peer.length; i++) chain[i] = (X509Certificate) peer[i];
                        outbound.record(e.getSocket(), chain, endpoint);
                    } catch (Exception ignore) {
                        // no peer certificate: nothing to revoke it by
                    }
                };
                ((SSLSocket) s).addHandshakeCompletedListener(l);
            }
            return s;
        }

        @Override public String[] getDefaultCipherSuites() { return real.getDefaultCipherSuites(); }
        @Override public String[] getSupportedCipherSuites() { return real.getSupportedCipherSuites(); }
        @Override public Socket createSocket() throws IOException { return watch(real.createSocket()); }
        @Override public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
            return watch(real.createSocket(s, host, port, autoClose));
        }
        @Override public Socket createSocket(String host, int port) throws IOException { return watch(real.createSocket(host, port)); }
        @Override public Socket createSocket(String host, int port, InetAddress local, int localPort) throws IOException {
            return watch(real.createSocket(host, port, local, localPort));
        }
        @Override public Socket createSocket(InetAddress host, int port) throws IOException { return watch(real.createSocket(host, port)); }
        @Override public Socket createSocket(InetAddress host, int port, InetAddress local, int localPort) throws IOException {
            return watch(real.createSocket(host, port, local, localPort));
        }
    }
}
