package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;

import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/**
 * Binds a TLS connection to the Cresco identity the connection was meant to reach.
 *
 * <p><b>Why.</b> Broker connections dial an IP address and a (possibly dynamic) port learned from discovery,
 * so TLS hostname verification ({@code verifyHostName}) cannot work and is off. Before this class, the only
 * check was the JDK PKIX trust manager over the node's truststore: it accepts ANY certificate that chains to
 * ANY anchor in the truststore. Every peer that ever exchanged certificates with this node (and, under the
 * regional CA, every node its CA enrolled) holds such a certificate, so any of them could answer a connection
 * meant for the regional controller, the global, or a bridged region, and the connection was accepted.
 *
 * <p><b>What it checks.</b> After PKIX succeeds, the leaf certificate must name the expected peer
 * ({@code CN=<agent>, OU=<region>}, read strictly by {@link CrescoNames#identityOf}), and that name must be bound
 * to the certificate ({@link PeerTrustRegistry#verifiedIdentity}): its key is the one recorded for the name (per-node
 * trust: learned in authenticated discovery; regional CA: enrolled), or it was issued by the CA recorded for that
 * name's region and tenant. A trusted chain alone is not enough: under per-node trust every peer generated its own
 * root CA and the node trusts all of them, so a peer that kept its CA key could mint any name.
 *
 * <p>The tenant is compared too: CAs are scoped per tenant, so a certificate that names the expected region and agent
 * under ANOTHER tenant (issued by that tenant's CA) is another identity. The expected tenant is the one verified for the
 * peer in discovery -- carried by {@link Expected}, else what the registry holds for the peer's region
 * ({@link PeerTrustRegistry#regionTenant}) -- or a superuser tenant (the operator's own). What a verified peer may then
 * read and write is the broker's job ({@link TenantPolicy}).
 */
public final class PeerIdentity {

    /** Config key: {@code enforce} (default), {@code warn} or {@code off}. */
    public static final String MODE_KEY = "peer_identity_mode";

    private PeerIdentity() { }

    /** How a failed binding is handled. */
    public enum Mode {
        /** Refuse the connection (fail closed). The default. */
        ENFORCE,
        /** Log what would be refused, accept the connection (for a staged roll-out). */
        WARN,
        /** No identity binding: PKIX only, the behaviour before this check existed. */
        OFF;

        /** Parse a config value; anything unrecognised is ENFORCE (fail closed). */
        public static Mode parse(String v) {
            if (v == null) return ENFORCE;
            switch (v.trim().toLowerCase(Locale.ROOT)) {
                case "off": case "false": case "disabled": return OFF;
                case "warn": case "log": case "audit": return WARN;
                default: return ENFORCE;
            }
        }
    }

    /** The peer a connection is meant to reach: its region and agent name, and its tenant when the caller holds it. */
    public static final class Expected {
        /** The tenant verified for the peer in discovery; null: what the registry holds for the peer's region. */
        public final String tenant;
        public final String region;
        public final String agent;

        public Expected(String region, String agent) {
            this(null, region, agent);
        }

        public Expected(String tenant, String region, String agent) {
            this.tenant = tenant;
            this.region = region;
            this.agent = agent;
        }

        public String path() { return region + "_" + agent; }

        /** Both names present and valid (CrescoNames grammar). */
        public boolean isComplete() { return CrescoNames.isValid(region) && CrescoNames.isValid(agent); }

        @Override public boolean equals(Object o) {
            if (!(o instanceof Expected)) return false;
            Expected e = (Expected) o;
            return Objects.equals(tenant, e.tenant) && Objects.equals(region, e.region) && Objects.equals(agent, e.agent);
        }

        @Override public int hashCode() { return Objects.hash(tenant, region, agent); }

        @Override public String toString() { return (tenant == null ? "" : "tenant=" + tenant + ", ") + "region=" + region + ", agent=" + agent; }
    }

    /** The outcome of {@link #verify}. */
    public static final class Verdict {
        public final boolean ok;
        public final String reason;
        public final CrescoIdentity presented;

        private Verdict(boolean ok, String reason, CrescoIdentity presented) {
            this.ok = ok;
            this.reason = reason;
            this.presented = presented;
        }

        static Verdict ok(CrescoIdentity presented, String reason) { return new Verdict(true, reason, presented); }

        static Verdict refuse(CrescoIdentity presented, String reason) { return new Verdict(false, reason, presented); }
    }

    /**
     * Does {@code chain} (already PKIX-validated, leaf first) belong to {@code expected}? Pure; never throws.
     *
     * @param pins the keys learned for identities over authenticated discovery; null = no pins
     */
    public static Verdict verify(Expected expected, X509Certificate[] chain, PeerTrustRegistry pins) {
        if (expected == null || !expected.isComplete()) {
            return Verdict.refuse(null, "no expected peer identity was recorded for this connection");
        }
        if (chain == null || chain.length == 0 || chain[0] == null) {
            return Verdict.refuse(null, "the peer presented no certificate");
        }
        X509Certificate leaf = chain[0];
        CrescoIdentity presented = CrescoNames.identityOf(leaf);
        if (presented == null) {
            return Verdict.refuse(null, "the certificate names no valid Cresco region/agent (subject '"
                    + leaf.getSubjectX500Principal().getName() + "')");
        }
        if (!expected.region.equals(presented.getRegion()) || !expected.agent.equals(presented.getAgent())) {
            return Verdict.refuse(presented, "the certificate names region=" + presented.getRegion()
                    + ", agent=" + presented.getAgent());
        }
        if (expected.tenant != null && !expected.tenant.equals(presented.getTenant())
                && (pins == null || !pins.isSuperuserTenant(presented.getTenant()))) {
            return Verdict.refuse(presented, "the certificate names the expected region and agent under tenant " + presented.getTenant()
                    + ", not the tenant " + expected.tenant + " verified for this peer in discovery");
        }
        if (pins == null) {
            return Verdict.ok(presented, "identity matches (name only: no registry)");
        }
        String regionTenant = expected.tenant != null ? null : pins.regionTenant(expected.region);
        if (regionTenant != null && !regionTenant.equals(presented.getTenant()) && !pins.isSuperuserTenant(presented.getTenant())) {
            return Verdict.refuse(presented, "the certificate names the expected region and agent under tenant " + presented.getTenant()
                    + ", but region " + expected.region + " is tenant " + regionTenant + "'s here");
        }
        String[] why = new String[1];
        CrescoIdentity bound = pins.verifiedIdentity(chain, why);
        if (bound == null) {
            return Verdict.refuse(presented, why[0]);
        }
        return Verdict.ok(presented, "identity matches and its key or issuing CA is the one recorded for it");
    }

    /** SHA-256 over the leaf's SubjectPublicKeyInfo, hex. */
    public static String spkiFingerprint(X509Certificate cert) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(cert.getPublicKey().getEncoded());
            return HexFormat.of().formatHex(d);
        } catch (Exception e) {
            return "unavailable";
        }
    }

    static String shortFp(String fp) {
        return fp == null ? "none" : (fp.length() > 16 ? fp.substring(0, 16) : fp);
    }
}
