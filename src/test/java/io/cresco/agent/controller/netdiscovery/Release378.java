package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;

import java.security.cert.X509Certificate;

/**
 * The secret's discovery exchange exactly as a node of release 1.3.378 runs it: the MACs of the discovery secret and
 * nothing else -- a plain nonce, no signature by any key. It is also all that a holder of the secret WITHOUT a
 * certificate's private key can send: every certificate here may be somebody else's (they are public).
 *
 * <p>This is NOT the released code: it is written with THIS build's classes ({@link DiscoveryAuth}'s MAC, key derivation
 * and transcript, which the proof left as release 1.3.378 has them), so it shows what this build does when it is spoken to
 * as that release speaks -- not that the released jar speaks so. The released jar itself is exercised by the live
 * suites (rolling updates from the released agent, with and without fixed agent names).
 */
public final class Release378 {

    private static final Gson GSON = new Gson();

    private Release378() { }

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return m == null ? null : GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    /** Joins {@code server} as {@code region/agent}, presenting {@code chain}: the responder's RESULT (null: not answered). */
    public static DiscoveryAuth.Msg join(ControllerEngine server, DiscoveryType type, String secret, String region, String agent, X509Certificate[] chain) {
        DiscoveryProcessor s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = new DiscoveryAuth.Msg();
        hello.step = "hello";
        hello.type = type.name();
        hello.nc = DiscoveryAuth.nonce();
        hello.creg = region;
        hello.cagt = agent;
        hello.ccert = server.getCertificateManager().getJsonFromCerts(chain);
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "10.37.8.1");
        if (ch == null) return null;
        byte[] key = DiscoveryAuth.key(secret, "handshake|" + hello.type);
        X509Certificate sleaf = server.getCertificateManager().getCertsfromJson(ch.scert)[0];
        DiscoveryAuth.Msg proof = new DiscoveryAuth.Msg();
        proof.step = "proof";
        proof.type = hello.type;
        proof.mac = DiscoveryAuth.mac(key, "C", DiscoveryAuth.transcript(hello, ch, chain[0], sleaf, true));
        return s.serverResult(copy(hello), ch, copy(proof), "10.37.8.1");
    }

    /** {@code node}, were it of release 1.3.378, joins {@code server} under its own name with its own chain. */
    public static DiscoveryAuth.Msg join(ControllerEngine node, ControllerEngine server, DiscoveryType type, String secret) {
        return join(server, type, secret, node.cstate.getRegion(), node.cstate.getAgent(), node.getCertificateManager().getPublicCertificate());
    }

    /**
     * Answers {@code client}'s exchange of {@code type} AS {@code as} -- with its public chain -- from {@code fromIp}, naming
     * {@code brokerPort}: what the client takes from it (where it would connect), or null when it refused the answer.
     */
    public static DiscoveryNode answer(ControllerEngine client, ControllerEngine as, DiscoveryType type, String secret, String fromIp, int brokerPort) {
        DiscoveryProcessor c = new DiscoveryProcessor(client);
        DiscoveryAuth.Msg hello = c.clientHello(type);
        if (hello == null) return null;
        X509Certificate cleaf = client.getCertificateManager().getCertsfromJson(hello.ccert)[0];
        X509Certificate sleaf = as.getCertificateManager().getPublicCertificate()[0];
        DiscoveryAuth.Msg ch = new DiscoveryAuth.Msg();
        ch.step = "challenge";
        ch.type = hello.type;
        ch.nc = hello.nc;
        ch.ns = DiscoveryAuth.nonce();
        ch.sreg = as.cstate.getRegion();
        ch.sagt = as.cstate.getAgent();
        ch.scert = as.getCertificateManager().getJsonFromCerts(as.getCertificateManager().getPublicCertificate());
        ch.sport = brokerPort;
        byte[] key = DiscoveryAuth.key(secret, "handshake|" + hello.type);
        ch.mac = DiscoveryAuth.mac(key, "S1", DiscoveryAuth.transcript(hello, ch, cleaf, sleaf, false));
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), fromIp);
        if (proof == null) return null;
        DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
        r.step = "result";
        r.type = hello.type;
        r.ok = true;
        r.rcert = ch.scert;
        r.mac = DiscoveryAuth.mac(key, "S2", DiscoveryAuth.transcript(hello, ch, cleaf, sleaf, true), DiscoveryAuth.resultDigest(r));
        return c.clientResult(hello, copy(ch), copy(r), fromIp, 32005);
    }
}
