package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.CrescoIdentity;
import io.cresco.library.utilities.CLogger;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.joda.time.DateTime;

import javax.net.ssl.*;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

public class CertificateManager {

    private KeyStore keyStore;
    private volatile KeyStore trustStore;
    private char[] keyStorePassword;
    private String keyStoreFilePath;
    private char[] trustStorePassword;
    private String trustStoreFilePath;
    private String keyStoreAlias;
    //private X509Certificate signingCertificate;
    //private PrivateKey signingKey;
    private int YEARS_VALID = 3;
    private X509Certificate[] chain;
    private CLogger logger;
    // Regional-CA (distributed-trust Option C): set on a region/global controller acting as an issuing
    // authority. When present this node can sign enrolling nodes' leaf certificates (issueEnrollment).
    private RegionCA regionCA;
    // What discovery taught us about peers (pinned keys, bridge-capable controllers): binds TLS connections to
    // the identity they were opened to (PeerIdentity). In memory; re-learned by the next discovery exchange.
    private PeerTrustRegistry peerTrust = new PeerTrustRegistry();

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;

    // CNSA-1.0 RSA floor via the single crypto policy seam (was a factorable 512-bit clamp).
    private int keySize = io.cresco.library.crypto.CrescoCrypto.MIN_RSA_KEY_BITS;
    private boolean certificateSaveFailureEncountered = false;

    public CertificateManager(ControllerEngine controllerEngine) {
        long startTime = System.currentTimeMillis();

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(CertificateManager.class.getName(), CLogger.Level.Info);
        if (ProvisionedIdentity.enabled(plugin.getConfig())) {
            // a PRE-PROVISIONED node: its identity and the fabric's anchors come from the operator's PKI; nothing is generated,
            // enrolled or learned on first use. A provisioning that cannot be used stops the controller (never a fallback).
            initProvisioned();
            logger.info("CertificateManager Init (pre-provisioned): " + (System.currentTimeMillis() - startTime) + " ms");
            return;
        }
        try {

            // how far certificates are backdated: the clock lag every certificate tolerates (P4)
            CertValidity.setBackdateHours(plugin.getConfig().getIntegerParam("identity_cert_backdate_hours", 24));
            peerTrust = openPeerTrust();

            int minKeySize = io.cresco.library.crypto.CrescoCrypto.MIN_RSA_KEY_BITS;
            keySize = plugin.getConfig().getIntegerParam("messagekeysize", minKeySize);
            if (keySize < minKeySize) {
                logger.warn("messagekeysize " + keySize + " is below the CNSA-1.0 RSA floor; using " + minKeySize);
                keySize = minKeySize;
            }

            this.keyStoreAlias = this.controllerEngine.cstate.getAgentPath();

            String keyStorePasswordStr = plugin.getConfig().getStringParam("keystorepwd");
            keyStoreFilePath = plugin.getConfig().getStringParam("keystorefile");
            String trustStorePasswordStr = plugin.getConfig().getStringParam("truststorepwd");
            trustStoreFilePath = plugin.getConfig().getStringParam("truststorefile");
            if (keyStorePasswordStr != null && keyStoreFilePath != null &&
                    trustStorePasswordStr != null && trustStoreFilePath != null &&
                    !keyStoreFilePath.equals(trustStoreFilePath)) {
                //never log store passwords, even at debug
                logger.debug("keyStorePassword set: {}", keyStorePasswordStr != null);
                logger.debug("keyStoreFilePath: {}", keyStoreFilePath);
                logger.debug("trustStorePassword set: {}", trustStorePasswordStr != null);
                logger.debug("trustStoreFilePath: {}", trustStoreFilePath);
                keyStorePassword = keyStorePasswordStr.toCharArray();
                trustStorePassword = trustStorePasswordStr.toCharArray();
                if (Files.exists(Paths.get(keyStoreFilePath)) &&
                        Files.exists(Paths.get(trustStoreFilePath)) &&
                        loadKeyAndTrustStore()) {
                    logger.info("Existing key store and trust store loaded");
                } else {

                    //build directory structure if needed
                    Path keyStorePath = Paths.get(keyStoreFilePath).getParent();
                    if(keyStorePath != null) {
                        if(!keyStorePath.toFile().exists()) {
                            Files.createDirectories(keyStorePath);
                        }
                    }
                    Path trustStorePath = Paths.get(trustStoreFilePath).getParent();
                    if(trustStorePath != null) {
                        if(!trustStorePath.toFile().exists()) {
                            Files.createDirectories(trustStorePath);
                        }
                    }

                    logger.info("Key store or trust store do not exists or are invalid, (re)creating");
                    keyStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
                    keyStore.load(null, null);


                    trustStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
                    trustStore.load(null, null);

                    generateCertChain();

                    //trust self
                    replaceTrustStoreEntries(keyStoreAlias, getPublicCertificate());
                }
            } else {
                //keyStoreAlias = UUID.randomUUID().toString();
                keyStorePassword = UUID.randomUUID().toString().toCharArray();

                keyStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
                keyStore.load(null, null);


                trustStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
                trustStore.load(null, null);

                // the node's key survives restarts (identity bindings at peers and CAs follow the key)
                if (!loadNodeIdentity()) {
                    generateCertChain();
                    saveNodeIdentity();
                }

                //trust self
                replaceTrustStoreEntries(keyStoreAlias, getPublicCertificate());
            }
            // CAs recorded for regions before a restart stay trusted for TLS (their scope is in the registry)
            for (X509Certificate ca : peerTrust.recordedCas()) {
                trustStore.setCertificateEntry("recordedCA-" + PeerIdentity.spkiFingerprint(ca).substring(0, 16), ca);
            }

            startRenewalCheck();
            if (chain != null && chain[0] != null) {
                logger.info("This node's identity key: " + PeerIdentity.spkiFingerprint(chain[0])
                        + " (SHA-256 of its SubjectPublicKeyInfo: what a rebind token or statement names)");
            }

            // Export the public key
            exportPublicKey();
            // Import and public keys from external agents
            importPublicKeysFromDirectory();

            logger.info("CertificateManager Init: " + (System.currentTimeMillis() - startTime) + " ms");


        } catch(Exception ex) {
            logger.error("CertificateManager.<init> CertificateChainGeneration() Error: " + ex.getMessage(), ex);
        }

    }

    // ---------------------------------------------------------------- pre-provisioned identity (security_profile=production)

    /** Non-null: this node's identity is pre-provisioned by the operator's PKI (nothing is generated or enrolled). */
    private volatile ProvisionedIdentity provisioned;
    private volatile ProvisionedIdentity.Revocations operatorRevocations = ProvisionedIdentity.Revocations.NONE;
    /** The node's private key as last loaded (a provisioned keystore can be replaced at run time). */
    private volatile PrivateKey currentIdentityKey;
    /**
     * Pre-provisioned: the chain and key in force, published together (one volatile write) so nothing -- a TLS handshake,
     * a signature -- ever pairs a reloaded chain with the previous key or the reverse. Null on a node that makes its own.
     */
    private volatile NodeIdentityKeyManager.Identity presented;
    /** A self-made identity's chain as last presented (its key is the keystore's, which renewal keeps). */
    private volatile NodeIdentityKeyManager.Identity selfMade;
    private static final java.util.concurrent.atomic.AtomicLong IDENTITY_GEN = new java.util.concurrent.atomic.AtomicLong();
    private volatile String provisionedAlarm;
    private volatile boolean provisionedAlarmCritical;
    private java.util.concurrent.ScheduledExecutorService provisionedWatcher;

    /** Is this node's identity pre-provisioned by the operator's PKI? */
    public boolean isProvisioned() { return provisioned != null; }

    /**
     * Is this node the fabric's GLOBAL controller by the operator's PKI (SAN cresco:role:global, issued by a CA allowed to
     * issue it)? Pre-provisioned, only it answers GLOBAL discovery.
     */
    public boolean isProvisionedGlobal() {
        ProvisionedIdentity p = provisioned;
        return p != null && p.global;
    }

    /**
     * Why this node, configured for a pre-provisioned identity, must NOT start, or null (also null when it is not
     * provisioned): its keystore, password file, trust anchors or revocation file cannot be used, its chain does not validate
     * to the anchors now, it is revoked by the operator, or it names another identity than this node is configured as.
     */
    public static String provisioningRefusal(io.cresco.library.plugin.Config config) {
        if (!ProvisionedIdentity.enabled(config)) return null;
        try {
            Path ta = ProvisionedIdentity.path(config, "trust_anchors");
            ProvisionedIdentity.Revocations rev = readOperatorRevocations(config, ProvisionedIdentity.readAnchors(ta));
            ProvisionedIdentity p = ProvisionedIdentity.load(config, rev);
            return identityMismatch(config, p);
        } catch (ProvisionedIdentity.ProvisioningException e) {
            return e.getMessage();
        }
    }

    /** The identity {@code tenant/region/agent} the provisioned certificate names (to name the node after it), or null. */
    public static String[] provisionedNames(io.cresco.library.plugin.Config config) {
        if (!ProvisionedIdentity.enabled(config)) return null;
        try {
            Path ta = ProvisionedIdentity.path(config, "trust_anchors");
            ProvisionedIdentity p = ProvisionedIdentity.load(config, readOperatorRevocations(config, ProvisionedIdentity.readAnchors(ta)));
            return new String[]{p.identity.getTenant(), p.identity.getRegion(), p.identity.getAgent()};
        } catch (ProvisionedIdentity.ProvisioningException e) {
            return null;
        }
    }

    private static String identityMismatch(io.cresco.library.plugin.Config config, ProvisionedIdentity p) {
        String tenant = SecuritySettings.tenant(config), region = config.getStringParam("regionname"), agent = config.getStringParam("agentname");
        CrescoIdentity id = p.identity;
        if (!id.getTenant().equals(tenant)) return "the certificate names tenant " + id.getTenant() + " but this node is configured for tenant " + tenant
                + " (tenant_id)";
        if (region != null && !region.equals(id.getRegion())) return "the certificate names region " + id.getRegion() + " but regionname is " + region;
        if (agent != null && !agent.equals(id.getAgent())) return "the certificate names agent " + id.getAgent() + " but agentname is " + agent;
        if (config.getBooleanParam("is_global", false) || config.getBooleanParam("is_region", false)) {
            if (!p.controller) return "this node is configured as a controller but its certificate does not carry the SAN URI "
                    + ProvisionedIdentity.URI_ROLE + ProvisionedIdentity.ROLE_CONTROLLER;
        }
        if (config.getBooleanParam("is_global", false) && !p.global) return "this node is configured as the GLOBAL controller (is_global) but its"
                + " certificate does not carry the SAN URI " + ProvisionedIdentity.URI_ROLE + ProvisionedIdentity.ROLE_GLOBAL
                + " (cresco-pki.sh node ... --global)";
        return null;
    }

    private static ProvisionedIdentity.Revocations readOperatorRevocations(io.cresco.library.plugin.Config config,
                                                                          java.util.Collection<X509Certificate> cas)
            throws ProvisionedIdentity.ProvisioningException {
        String f = config.getStringParam("revocation_file");
        long grace = Math.max(0L, config.getLongParam("revocation_crl_grace_seconds", 0L)) * 1000L;
        return (f == null || f.isBlank()) ? ProvisionedIdentity.Revocations.NONE
                : ProvisionedIdentity.Revocations.read(Paths.get(f.trim()), cas, grace);
    }

    /**
     * Pre-provisioned, as the responder of a discovery exchange: why a joiner presenting {@code chain} is refused, or null.
     * Its chain must validate to the PKI and name the identity it claims; an AGENT joiner must be of this controller's region
     * and tenant; a REGION/GLOBAL joiner must be a controller (SAN cresco:role:controller) of another region.
     */
    public String provisionedJoinerRefusal(X509Certificate[] chain, io.cresco.agent.controller.netdiscovery.DiscoveryType type,
                                           String region, String agent) {
        String[] why = new String[1];
        CrescoIdentity id = peerTrust.verifiedIdentity(chain, why);
        if (id == null) return why[0];
        if (!id.getRegion().equals(region) || !id.getAgent().equals(agent)) return "its certificate names " + id + ", not " + region + "/" + agent;
        boolean controllerJoin = type == io.cresco.agent.controller.netdiscovery.DiscoveryType.REGION
                || type == io.cresco.agent.controller.netdiscovery.DiscoveryType.GLOBAL;
        String myRegion = controllerEngine.cstate.getRegion(), myTenant = SecuritySettings.tenant(plugin.getConfig());
        if (controllerJoin) {
            if (!ProvisionedIdentity.isController(chain[0])) return "a " + type + " joiner must be a controller (SAN "
                    + ProvisionedIdentity.URI_ROLE + ProvisionedIdentity.ROLE_CONTROLLER + ")";
            if (id.getRegion().equals(myRegion)) return "a joining controller claims this node's own region";
            return null;
        }
        if (!id.getRegion().equals(myRegion) || !id.getTenant().equals(myTenant)) {
            return "an agent of " + id.getTenant() + "/" + id.getRegion() + " cannot join region " + myRegion + " (tenant " + myTenant + ")";
        }
        if (superuserTenants().contains(id.getTenant())) return "tenant " + id.getTenant() + " is a superuser tenant";
        return null;
    }

    /**
     * Pre-provisioned: the controller presenting {@code chain} completed an authenticated REGION/GLOBAL discovery exchange
     * with this node (it joined, or this node joined it; the checks above passed). It is recorded with its tenant
     * ({@link PeerTrustRegistry#allowBridgePeer(String, String, String)}): its region's names belong to that tenant here
     * from now on. Why it cannot be (the region is another tenant's here), or null.
     */
    public String provisionedControllerLearned(X509Certificate[] chain) {
        CrescoIdentity id = chain == null || chain.length == 0 || chain[0] == null ? null : CrescoNames.identityOf(chain[0]);
        if (id == null) return "its certificate names no valid identity";
        return peerTrust.allowBridgePeer(id.getTenant(), id.getRegion(), id.getAgent());
    }

    /**
     * Pre-provisioned, as the client of a discovery exchange: why the responder presenting {@code chain} (answering as
     * {@code region/agent}) is refused, or null. It must be a controller of the PKI that names itself as it answered, and
     * (under {@code controller_trust_anchors}) chain to one of those too.
     */
    public String provisionedResponderRefusal(X509Certificate[] chain, io.cresco.agent.controller.netdiscovery.DiscoveryType type,
                                              String region, String agent) {
        String[] why = new String[1];
        CrescoIdentity id = peerTrust.verifiedIdentity(chain, why);
        if (id == null) return why[0];
        if (!id.getRegion().equals(region) || !id.getAgent().equals(agent)) return "its certificate names " + id + ", not " + region + "/" + agent;
        if (!ProvisionedIdentity.isController(chain[0])) return "it is not a controller of the operator's PKI (SAN "
                + ProvisionedIdentity.URI_ROLE + ProvisionedIdentity.ROLE_CONTROLLER + ")";
        String myRegion = controllerEngine.cstate.getRegion();
        boolean controllerExchange = type == io.cresco.agent.controller.netdiscovery.DiscoveryType.REGION
                || type == io.cresco.agent.controller.netdiscovery.DiscoveryType.GLOBAL;
        if (controllerExchange && region.equals(myRegion)) return "a controller of this node's own region answered a " + type + " exchange";
        if (type == io.cresco.agent.controller.netdiscovery.DiscoveryType.GLOBAL && !ProvisionedIdentity.isGlobal(chain[0])) {
            return "it answered a GLOBAL exchange but is not the fabric's global controller (its certificate does not carry the SAN URI "
                    + ProvisionedIdentity.URI_ROLE + ProvisionedIdentity.ROLE_GLOBAL + ")";
        }
        if (!controllerExchange && myRegion != null && !region.equals(myRegion)) return "it answered as region " + region + " but this node is of region " + myRegion;
        if (!controllerExchange && !id.getTenant().equals(SecuritySettings.tenant(plugin.getConfig()))) return "its tenant " + id.getTenant() + " is not this node's";
        return controllerAnchorRefusal(chain, null);
    }

    /** Load the provisioned identity, or throw (the controller does not start). */
    private void initProvisioned() {
        try {
            CertValidity.setBackdateHours(plugin.getConfig().getIntegerParam("identity_cert_backdate_hours", 24));
            java.util.List<X509Certificate> anchors = ProvisionedIdentity.readAnchors(ProvisionedIdentity.path(plugin.getConfig(), "trust_anchors"));
            ProvisionedIdentity.Revocations rev = readOperatorRevocations(plugin.getConfig(), anchors);
            ProvisionedIdentity p = ProvisionedIdentity.load(plugin.getConfig(), rev);
            String mismatch = identityMismatch(plugin.getConfig(), p);
            String region = controllerEngine.cstate.getRegion(), agent = controllerEngine.cstate.getAgent();
            if (mismatch == null && region != null && !region.equals(p.identity.getRegion())) mismatch = "the certificate names region "
                    + p.identity.getRegion() + " but this node is " + region;
            if (mismatch == null && agent != null && !agent.equals(p.identity.getAgent())) mismatch = "the certificate names agent "
                    + p.identity.getAgent() + " but this node is " + agent;
            if (mismatch != null) throw new ProvisionedIdentity.ProvisioningException(mismatch);
            keyStoreAlias = controllerEngine.cstate.getAgentPath();
            if (keyStoreAlias == null) keyStoreAlias = p.identity.getRegion() + "_" + p.identity.getAgent();
            keyStorePassword = UUID.randomUUID().toString().toCharArray();
            String rollback = rev.rollbackRefusal(recordedCrlNumbers());
            if (rollback != null) throw new ProvisionedIdentity.ProvisioningException("revocation_file: " + rollback);
            keyStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
            keyStore.load(null, null);
            KeyStore trust = anchorStore(anchors);
            peerTrust = openPeerTrust();
            publishProvisioned(p, anchors, rev, trust);
            recordCrlNumbers(rev);
            bindOwnIdentity();
            provisioned = p;
            logger.info("PRE-PROVISIONED identity " + p.identity + (p.controller ? " (controller)" : "") + " from " + plugin.getConfig().getStringParam("identity_keystore")
                    + ": " + ProvisionedIdentity.describe(p.chain[0]) + "; " + anchors.size() + " trust anchor(s); operator revocations: " + rev.summary()
                    + ". Enrollment, discovery-secret binding and self-made certificates are OFF.");
            logger.info("This node's identity key: " + PeerIdentity.spkiFingerprint(chain[0]));
            startProvisionedWatcher();
        } catch (ProvisionedIdentity.ProvisioningException e) {
            logger.error("REFUSING TO START: the pre-provisioned identity cannot be used: " + e.getMessage());
            throw new IllegalStateException("pre-provisioned identity: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("REFUSING TO START: the pre-provisioned identity cannot be installed: " + e.getMessage(), e);
            throw new IllegalStateException("pre-provisioned identity: " + e.getMessage(), e);
        }
    }

    /** A new trust store holding {@code anchors} only (built aside: the one in force is never emptied or half-filled). */
    private static KeyStore anchorStore(java.util.List<X509Certificate> anchors) throws Exception {
        KeyStore ts = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        ts.load(null, null);
        for (X509Certificate a : anchors) ts.setCertificateEntry("anchor#" + certFingerprint(a), a);
        return ts;
    }

    /**
     * Put a provisioning in force: everything was read, validated and built beforehand ({@code p}, {@code anchors},
     * {@code rev}, {@code trust}), so this only publishes -- the registry's anchors and revocations first (one write: the
     * strictest view takes effect at once), then the trust store (a reference swap), then the chain and key together (one
     * write). No reader ever sees a half-loaded state: an empty trust store, a chain without its key, new anchors with an old
     * revocation list.
     */
    private synchronized void publishProvisioned(ProvisionedIdentity p, java.util.List<X509Certificate> anchors,
                                                 ProvisionedIdentity.Revocations rev, KeyStore trust) throws Exception {
        if (peerTrust != null) {
            peerTrust.setPki(anchors, rev);
            // this node's region is its own tenant's: no other tenant's certificate names it here (PKI scopes are per tenant)
            CrescoIdentity me = CrescoNames.identityOf(p.chain[0]);
            if (me != null) peerTrust.setOwnRegion(me.getTenant(), me.getRegion());
        }
        operatorRevocations = rev;
        this.trustStore = trust;
        presented = new NodeIdentityKeyManager.Identity(p.chain.clone(), p.key, IDENTITY_GEN.incrementAndGet());
        storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, p.key, p.chain);
        this.chain = p.chain;
        this.currentIdentityKey = p.key;
    }

    /**
     * Watch the provisioned files (every {@code identity_keystore_reload_seconds}, 30): a replaced keystore (the operator's
     * renewal), trust anchors or revocation file is loaded, validated and put in force WITHOUT dropping a valid connection
     * (the key managers present the new chain and key at the next handshake); one that does not validate is refused and the
     * current identity kept (CRITICAL). The certificate's expiry is alarmed (WARN within its renewal window, CRITICAL in its
     * last tenth or last week).
     */
    private void startProvisionedWatcher() {
        long secs = Math.max(1, plugin.getConfig().getIntegerParam("identity_keystore_reload_seconds", 30));
        provisionedWatcher = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cresco-provisioned-identity");
            t.setDaemon(true);
            return t;
        });
        provisionedWatcher.scheduleWithFixedDelay(() -> {
            try { reloadProvisionedIfChanged(); } catch (Throwable t) { logger.error("provisioned identity check: " + t.getMessage()); }
        }, secs, secs, java.util.concurrent.TimeUnit.SECONDS);
        checkProvisionedExpiry();
    }

    /**
     * Re-read the provisioned files when any changed. Returns true when a new identity, anchors or revocations were put in
     * force.
     */
    public synchronized boolean reloadProvisionedIfChanged() {
        ProvisionedIdentity cur = provisioned;
        if (cur == null) return false;
        io.cresco.library.plugin.Config cfg = plugin.getConfig();
        boolean changed = false;
        try {
            Path ks = ProvisionedIdentity.path(cfg, "identity_keystore"), pw = ProvisionedIdentity.path(cfg, "identity_keystore_password_file"),
                    ta = ProvisionedIdentity.path(cfg, "trust_anchors");
            String rf = cfg.getStringParam("revocation_file");
            Path rp = rf == null || rf.isBlank() ? null : Paths.get(rf.trim());
            boolean idChanged = ProvisionedIdentity.mtime(ks) != cur.keystoreMtime || ProvisionedIdentity.mtime(pw) != cur.passwordMtime
                    || ProvisionedIdentity.mtime(ta) != cur.anchorsMtime;
            boolean revChanged = rp != null ? (!Files.exists(rp) || ProvisionedIdentity.mtime(rp) != operatorRevocations.mtime)
                    : operatorRevocations != ProvisionedIdentity.Revocations.NONE;
            // the D6 file rules hold at every tick, not only when a file changed: a directory opened to others since is alarmed
            String exposed = null;
            try {
                ProvisionedIdentity.checkFile(ks, true, "identity_keystore");
                ProvisionedIdentity.checkFile(pw, true, "identity_keystore_password_file");
                ProvisionedIdentity.checkFile(ta, false, "trust_anchors");
                if (rp != null && Files.exists(rp, java.nio.file.LinkOption.NOFOLLOW_LINKS)) ProvisionedIdentity.checkFile(rp, false, "revocation_file");
            } catch (ProvisionedIdentity.ProvisioningException e) {
                exposed = e.getMessage();
            }
            if (exposed != null) {
                setProvisionedAlarm(EXPOSED + exposed + "); no replacement is loaded until they are owner-only again", true);
                return false;
            }
            if (provisionedAlarm != null && provisionedAlarm.startsWith(EXPOSED)) setProvisionedAlarm(null, false);
            if (!idChanged && !revChanged) { checkProvisionedExpiry(); return false; }
            // one consistent set of files: read them all, then check none changed while they were read (the operator replaces
            // the keystore and its password file, or the anchors and the CRL, one after the other); else wait for the next tick
            long[] before = provisionedMtimes(ks, pw, ta, rp);
            java.util.List<X509Certificate> anchors = ProvisionedIdentity.readAnchors(ta);
            ProvisionedIdentity.Revocations rev = readOperatorRevocations(cfg, anchors);
            ProvisionedIdentity p = ProvisionedIdentity.load(cfg, rev);
            if (!java.util.Arrays.equals(before, provisionedMtimes(ks, pw, ta, rp))) {
                logger.info("Pre-provisioned identity files changed while they were read: loading them at the next check");
                return false;
            }
            if (!p.identity.equals(cur.identity)) {
                throw new ProvisionedIdentity.ProvisioningException("the new keystore names " + p.identity + ", not this node's " + cur.identity);
            }
            String rollback = rev.rollbackRefusal(recordedCrlNumbers());
            if (rollback != null) throw new ProvisionedIdentity.ProvisioningException("revocation_file: " + rollback);
            boolean newKey = !p.chain[0].getPublicKey().equals(cur.chain[0].getPublicKey());
            KeyStore trust = anchorStore(anchors);
            publishProvisioned(p, anchors, rev, trust);
            recordCrlNumbers(rev);
            if (newKey) bindOwnIdentity();
            provisioned = p;
            changed = true;
            setProvisionedAlarm(null, false);
            logger.info("Pre-provisioned identity RELOADED: " + ProvisionedIdentity.describe(p.chain[0]) + (newKey ? " with a NEW key" : " (same key)")
                    + "; " + anchors.size() + " trust anchor(s); operator revocations: " + rev.summary() + "; live connections kept (the next"
                    + " handshake presents it)");
            try { if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) { }
            // revocations the operator just listed close what holds them now
            onKeysRevoked(java.util.List.of());
        } catch (Exception e) {
            setProvisionedAlarm("the replaced pre-provisioned identity files were REFUSED (" + e.getMessage() + "); this node keeps its current"
                    + " identity " + ProvisionedIdentity.describe(cur.chain[0]), true);
        }
        checkProvisionedExpiry();
        return changed;
    }

    private static long[] provisionedMtimes(Path ks, Path pw, Path ta, Path rp) {
        long[] out = new long[4];
        Path[] ps = {ks, pw, ta, rp};
        for (int i = 0; i < ps.length; i++) {
            try { out[i] = ps[i] == null || !Files.exists(ps[i]) ? -1 : ProvisionedIdentity.mtime(ps[i]); } catch (Exception e) { out[i] = -2; }
        }
        return out;
    }

    /** Rollback protection of the operator's CRLs: the highest CRL number put in force per issuer, in the identity directory. */
    private Path crlNumbersFile() {
        return identityDir().resolve("crl-numbers");
    }

    /** The CRL numbers recorded (issuer DN -> number); empty when none were. An unreadable record fails closed. */
    java.util.Map<String, java.math.BigInteger> recordedCrlNumbers() throws ProvisionedIdentity.ProvisioningException {
        java.util.Map<String, java.math.BigInteger> out = new java.util.HashMap<>();
        Path f = crlNumbersFile();
        if (!Files.exists(f)) return out;
        try {
            for (String line : Files.readAllLines(f, java.nio.charset.StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) continue;
                int tab = line.indexOf('\t');
                if (tab <= 0) throw new IOException("a malformed line");
                out.put(line.substring(tab + 1), new java.math.BigInteger(line.substring(0, tab).trim()));
            }
        } catch (Exception e) {
            throw new ProvisionedIdentity.ProvisioningException("the CRL rollback record " + f + " is unreadable (" + e.getMessage()
                    + "); restore it, or remove it only if every CRL in revocation_file is the newest its CA issued");
        }
        return out;
    }

    /** Record the CRL numbers now in force (never lower than recorded; an issuer no longer trusted at all is dropped). */
    private void recordCrlNumbers(ProvisionedIdentity.Revocations rev) {
        try {
            java.util.Map<String, java.math.BigInteger> had = recordedCrlNumbers();
            java.util.Map<String, java.math.BigInteger> next = new java.util.TreeMap<>();
            for (java.util.Map.Entry<String, java.math.BigInteger> e : had.entrySet()) {
                if (rev.caNames.contains(e.getKey()) || rev.numbers.containsKey(e.getKey())) next.put(e.getKey(), e.getValue());
            }
            for (java.util.Map.Entry<String, java.math.BigInteger> e : rev.numbers.entrySet()) next.merge(e.getKey(), e.getValue(), java.math.BigInteger::max);
            if (next.equals(new java.util.TreeMap<>(had))) return;
            StringBuilder b = new StringBuilder("# highest CRL number in force per issuer (rollback protection); number<TAB>issuer\n");
            for (java.util.Map.Entry<String, java.math.BigInteger> e : next.entrySet()) b.append(e.getValue()).append('\t').append(e.getKey()).append('\n');
            Files.createDirectories(crlNumbersFile().getParent());
            PeerTrustRegistry.writeAtomically(crlNumbersFile(), b.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Could not record the CRL numbers in force (" + e.getMessage() + "): rollback protection holds only until restart");
        }
    }

    /** The operator's CRLs: CRITICAL once one is out of date (its CA's certificates are refused), WARN in its last week. */
    private void checkCrlFreshness() {
        ProvisionedIdentity.Revocations rev = operatorRevocations;
        if (rev == null || rev.nextUpdates.isEmpty()) return;
        long now = System.currentTimeMillis();
        java.util.List<String> stale = rev.staleIssuers(now);
        long next = rev.earliestNextUpdate();
        String text = !stale.isEmpty() ? "the operator's CRL of " + stale + " is OUT OF DATE: certificates of that CA are refused until "
                + plugin.getConfig().getStringParam("revocation_file") + " is refreshed (cresco-pki.sh crl)"
                : next > 0 && next - now < 7 * 24 * 3600_000L ? "the operator's CRL is due for refresh by " + new java.util.Date(next)
                + " (after that, certificates of its CA are refused): refresh " + plugin.getConfig().getStringParam("revocation_file") : null;
        long gap = !stale.isEmpty() ? 600_000L : 3_600_000L;
        if (text != null && (!text.equals(lastCrlAlarm) || now - lastCrlAlarmAt > gap)) {
            lastCrlAlarm = text;
            lastCrlAlarmAt = now;
            if (!stale.isEmpty()) logger.error("IDENTITY: " + text); else logger.warn("IDENTITY: " + text);
        } else if (text == null) {
            lastCrlAlarm = null;
        }
    }

    private volatile String lastCrlAlarm;
    private volatile long lastCrlAlarmAt;

    private void checkProvisionedExpiry() {
        ProvisionedIdentity p = provisioned;
        if (p == null) return;
        checkCrlFreshness();
        if (provisionedAlarm != null && (provisionedAlarm.startsWith("the replaced") || provisionedAlarm.startsWith(EXPOSED))) return;   // stands until fixed
        long now = System.currentTimeMillis(), na = p.chain[0].getNotAfter().getTime();
        long life = intendedLife(p.chain[0].getNotBefore().getTime(), na), left = na - now;
        if (left <= 0) setProvisionedAlarm("this node's pre-provisioned certificate EXPIRED on " + p.chain[0].getNotAfter() + "; replace "
                + plugin.getConfig().getStringParam("identity_keystore"), true);
        else if (left <= Math.min(7 * 24 * 3600_000L, Math.max(life / 10, 3600_000L))) setProvisionedAlarm("this node's pre-provisioned certificate"
                + " expires on " + p.chain[0].getNotAfter() + "; replace " + plugin.getConfig().getStringParam("identity_keystore") + " (it is reloaded"
                + " without a restart)", true);
        else if (chainNeedsRenewal(p.chain, now, renewBeforeMs())) setProvisionedAlarm("this node's pre-provisioned certificate expires on "
                + p.chain[0].getNotAfter() + "; replace " + plugin.getConfig().getStringParam("identity_keystore") + " before then", false);
        else setProvisionedAlarm(null, false);
    }

    private static final String EXPOSED = "the pre-provisioned identity files are no longer protected (";

    private void setProvisionedAlarm(String text, boolean critical) {
        String old = provisionedAlarm;
        provisionedAlarm = text;
        provisionedAlarmCritical = critical;
        if (text != null && !text.equals(old)) { if (critical) logger.error("IDENTITY: " + text); else logger.warn("IDENTITY: " + text); }
    }

    // ---------------------------------------------------------------- persistent identity material

    /** Directory for this node's identity material: node_identity_dir, else <agent data dir>/security. */
    private Path identityDir() {
        String d = plugin.getConfig().getStringParam("node_identity_dir");
        if (d == null || d.isBlank()) {
            String base = null;
            try { base = plugin.getAgentService().getAgentDataDirectory(); } catch (Exception ignore) { }
            if (base == null) base = System.getProperty("cresco_data_location", "cresco-data");
            d = Paths.get(base, "security").toString();
        }
        return Paths.get(d);
    }

    private static java.util.List<String> csv(String v) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (v == null) return out;
        for (String x : v.split(",")) if (!x.trim().isEmpty()) out.add(x.trim());
        return out;
    }

    private PeerTrustRegistry openPeerTrust() {
        if (!plugin.getConfig().getBooleanParam("peer_trust_persist", true)) {
            PeerTrustRegistry mem = new PeerTrustRegistry().withRebind(csv(plugin.getConfig().getStringParam("peer_identity_rebind", "")));
            mem.setKeyReplacedListener(this::onKeyReplaced);
            mem.setRevokedBatchListener(this::onKeysRevoked);
            mem.setSuperuserTenants(superuserTenants());
            return mem;
        }
        String f = plugin.getConfig().getStringParam("peer_trust_file");
        Path file = (f == null || f.isBlank()) ? identityDir().resolve("peer-trust.json") : Paths.get(f);
        PeerTrustRegistry.Options o = new PeerTrustRegistry.Options();
        o.rebind = csv(plugin.getConfig().getStringParam("peer_identity_rebind", ""));
        o.revoke = csv(plugin.getConfig().getStringParam("peer_bridge_revoke", ""));
        o.revokeIdentities = csv(plugin.getConfig().getStringParam("peer_identity_revoke", ""));
        o.bridgePruneDays = plugin.getConfig().getIntegerParam("peer_bridge_prune_days", 30);
        o.bindingPruneDays = plugin.getConfig().getIntegerParam("peer_binding_prune_days", 90);
        // the registry is created on this node's first start, before its persistent identity: when that identity exists, a
        // registry that is missing entirely was deleted, and is sealed rather than silently started empty
        o.expectExisting = plugin.getConfig().getBooleanParam("node_identity_persist", true)
                && plugin.getConfig().getStringParam("keystorefile") == null
                && Files.exists(identityDir().resolve("node-identity.p12"));
        PeerTrustRegistry.secureDirectory(identityDir(), m -> logger.error(m));
        // one writer per store: a CertificateManager re-created in this JVM (regionInit after a global loss) shares the
        // registry the broker already holds instead of opening a second writer on the same files
        PeerTrustRegistry reg = PeerTrustRegistry.shared(file, o, m -> logger.error(m));
        reg.setKeyReplacedListener(this::onKeyReplaced);
        reg.setRevokedBatchListener(this::onKeysRevoked);
        reg.setSuperuserTenants(superuserTenants());
        return reg;
    }

    /** The TLS connections this node opened (bridges, client connections), closed when their peer is revoked. */
    private final OutboundTlsConnections outbound = new OutboundTlsConnections();

    /** The outbound connections this node opened, as recorded by their trust managers and handshakes. */
    public OutboundTlsConnections outboundConnections() {
        if (!outboundRefusalSet) {
            outboundRefusalSet = true;
            outbound.setRefusal(c -> peerTrust == null ? null : peerTrust.revocationRefusal(c));
        }
        return outbound;
    }
    private volatile boolean outboundRefusalSet;

    /** A key was revoked (see {@link #onKeysRevoked}). */
    void onKeyRevoked(String keyFp) {
        onKeysRevoked(java.util.List.of(keyFp));
    }

    /**
     * Keys were revoked (one batch): remove every truststore anchor carrying them; have the broker close every connection to
     * it whose peer is refused now (TLS and the broker refuse new ones already); and close every connection THIS node
     * opened -- bridges included -- whose peer is refused now.
     */
    void onKeysRevoked(java.util.Collection<String> keyFps) {
        dropAnchorsOfKeys(new java.util.HashSet<>(keyFps));
        // message authentication: every credential verified anew, the sessions and trust-bundle entries of what is now refused
        // dropped (each message is checked against the revocations as they stand in any case)
        try {
            MsgSigning ms = controllerEngine == null ? null : controllerEngine.msgSigningIfCreated();
            if (ms != null) ms.onRevocations();
        } catch (Exception ex) {
            logger.error("message signatures: applying revocations: " + ex.getMessage());
        }
        try {
            if (controllerEngine.getBroker() != null) controllerEngine.getBroker().closeRevokedConnections();
        } catch (Exception ex) {
            logger.error("closing connections of revoked keys: " + ex.getMessage());
        }
        try {
            java.util.List<String> closed = outbound.closeRefused(peerTrust::revocationRefusal);
            for (String c : closed) logger.warn("CLOSED the connection this node opened to " + c);
        } catch (Exception ex) {
            logger.error("closing outbound connections of revoked keys: " + ex.getMessage());
        }
    }

    /**
     * Remove every truststore entry carrying one of {@code keyFps} that is revoked for the name its certificate carries (an
     * entry a region listed applies to that region's names only: the same key anchored for another region's node stays);
     * refresh the broker's trust when any was removed.
     */
    private void dropAnchorsOfKeys(java.util.Set<String> keyFps) {
        try {
            if (trustStore == null || keyFps.isEmpty()) return;
            java.util.List<String> drop = new java.util.ArrayList<>();
            synchronized (trustStore) {
                for (String alias : java.util.Collections.list(trustStore.aliases())) {
                    Certificate c = trustStore.getCertificate(alias);
                    if (c == null) continue;
                    String fp = PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded());
                    if (!keyFps.contains(fp)) continue;
                    CrescoIdentity named = c instanceof X509Certificate ? CrescoNames.identityOf((X509Certificate) c) : null;
                    if (peerTrust == null || peerTrust.isRevokedFor(fp, named == null ? null : named.getRegion())) drop.add(alias);
                }
                for (String alias : drop) trustStore.deleteEntry(alias);
            }
            if (!drop.isEmpty()) {
                logger.warn("Removed " + drop.size() + " truststore anchor(s) carrying revoked keys: " + drop);
                saveKeyAndTrustStore();
                if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
            }
        } catch (Exception ex) {
            logger.error("dropAnchorsOfKeys: " + ex.getMessage());
        }
    }

    /**
     * Why a controller presenting {@code chain} may not be joined under {@code controller_trust_anchors} (SHA-256 of a
     * certificate's DER or of its public key, hex; comma separated), or null. Empty setting: any controller that passes the
     * authenticated exchange may be joined the first time (and is pinned from then on).
     */
    public String controllerAnchorRefusal(X509Certificate[] chain) {
        return controllerAnchorRefusal(chain, null);
    }

    /**
     * As {@link #controllerAnchorRefusal(X509Certificate[])}: the responder's {@code chain} must have a validated path from
     * its LEAF to a listed anchor ({@link AnchorPath}: issuer/subject, CA basic constraints, signatures, validity; other
     * certificates it carries are ignored). Under the regional CA the exchange also returns {@code issued} -- this node's
     * identity issued by the responder's CA -- and that leaf must have been issued by a CA ON the validated path, so a
     * responder that copied the anchored controller's (public) chain cannot hand out identities under a CA of its own.
     */
    public String controllerAnchorRefusal(X509Certificate[] chain, X509Certificate[] issued) {
        java.util.Set<String> anchors = controllerAnchors();
        if (anchors.isEmpty()) return null;
        java.util.Date now = new java.util.Date();
        java.util.List<X509Certificate> path = AnchorPath.path(chain, anchors, now);
        if (path == null) {
            return "its certificate does not chain to anything listed in controller_trust_anchors (a validated path from its leaf;"
                    + " a listed certificate merely carried along does not count)";
        }
        if (issued != null && issued.length > 0 && issued[0] != null) {
            boolean onPath = false;
            // a federating controller returns its CA itself: it must BE a CA on the path (same key)
            for (int i = 1; i < path.size() && !onPath; i++) {
                if (path.get(i).getPublicKey().equals(issued[0].getPublicKey())) onPath = true;
            }
            for (int i = 1; i < path.size() && !onPath; i++) {
                X509Certificate ca = path.get(i);
                if (!issued[0].getIssuerX500Principal().equals(ca.getSubjectX500Principal())) continue;
                try { issued[0].verify(ca.getPublicKey()); onPath = true; } catch (Exception ignore) { }
            }
            // a responder whose own leaf is the anchor (no CA on the path) issues under the CA in its chain
            if (!onPath && path.size() == 1 && chain.length > 1) {
                for (X509Certificate c : chain) {
                    if (c == null || c == chain[0] || c.getBasicConstraints() < 0) continue;
                    try {
                        chain[0].verify(c.getPublicKey());
                        if (c.getPublicKey().equals(issued[0].getPublicKey())) onPath = true;
                        else if (issued[0].getIssuerX500Principal().equals(c.getSubjectX500Principal())) { issued[0].verify(c.getPublicKey()); onPath = true; }
                    } catch (Exception ignore) { }
                }
            }
            if (!onPath) return "the identity it issued was not issued by the CA on its anchored path (controller_trust_anchors)";
        }
        return null;
    }

    /** {@code controller_trust_anchors}, normalised (lowercase hex, no colons). */
    private java.util.Set<String> controllerAnchors() {
        java.util.Set<String> want = new java.util.HashSet<>();
        for (String a : csv(plugin.getConfig().getStringParam("controller_trust_anchors", ""))) want.add(a.replace(":", "").toLowerCase(java.util.Locale.ROOT));
        return want;
    }

    /**
     * Does {@code chain} carry a certificate or key listed in {@code controller_trust_anchors} (a non-empty setting)? Then
     * the operator vouched for it: a joined controller that re-keyed under such an anchor is followed without a rebind.
     */
    public boolean controllerAnchorVouches(X509Certificate[] chain) {
        return !controllerAnchors().isEmpty() && controllerAnchorRefusal(chain, null) == null;
    }

    private volatile String rebindStatementCache;
    private volatile long rebindStatementMtime = -1;

    /**
     * The operator-signed controller rebind statement this controller serves in discovery ({@code <identity dir>/
     * controller-rebind.json}, see {@link RebindStatement}), or null. Re-read when the file changes.
     */
    public String controllerRebindStatement() {
        try {
            Path f = identityDir().resolve("controller-rebind.json");
            long m = Files.exists(f) ? Files.getLastModifiedTime(f).toMillis() : 0;
            if (m != rebindStatementMtime) {
                String s = null;
                if (m != 0 && Files.size(f) <= 16_384) s = Files.readString(f).trim();
                rebindStatementCache = s == null || s.isEmpty() ? null : s;
                rebindStatementMtime = m;
                if (rebindStatementCache != null) logger.info("Serving the controller rebind statement " + f + " in discovery");
            }
            return rebindStatementCache;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Why {@code statement} does not vouch that the controller {@code region/agent} this node pinned now holds {@code keyFp}
     * ({@code controller_rebind_signers}: SHA-256 of the operator public keys trusted to sign one), or null when it does.
     */
    public String rebindStatementRefusal(String statement, String region, String agent, String keyFp) {
        return RebindStatement.refusal(statement, region, agent, keyFp, csv(plugin.getConfig().getStringParam("controller_rebind_signers", "")),
                System.currentTimeMillis());
    }

    private volatile PrivateKey identityKeyCache;
    private volatile java.security.PublicKey identityKeyFor;

    /**
     * This node's identity private key (the key its leaf certificate carries), read from the keystore once and again only
     * when the leaf's public key changes (renewal keeps the key). For per-message signing (MsgSigning).
     */
    public PrivateKey identityPrivateKey() throws Exception {
        NodeIdentityKeyManager.Identity pi = presented;
        if (pi != null) return pi.key;
        X509Certificate[] c = getPublicCertificate();
        java.security.PublicKey pub = (c == null || c.length == 0) ? null : c[0].getPublicKey();
        PrivateKey k = identityKeyCache;
        if (k != null && pub != null && pub.equals(identityKeyFor)) return k;
        k = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
        identityKeyCache = k;
        identityKeyFor = pub;
        return k;
    }

    /** Sign {@code data} with this node's identity key (the key its leaf certificate carries). */
    public byte[] signWithIdentity(byte[] data) throws Exception {
        NodeIdentityKeyManager.Identity pi = presented;
        PrivateKey k = pi != null ? pi.key : (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
        Signature s = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(k.getAlgorithm()));
        s.initSign(k);
        s.update(data);
        return s.sign();
    }

    /**
     * A name's key was replaced in the registry: remove every truststore entry its old chain added (named by the peer's
     * {@code <region>_<agent>} path) and every entry carrying the old key, so the old chain is no anchor any more. TLS
     * already refuses the old key (BoundKeyTrustManager asks the registry); this removes the anchor too, and refreshes the
     * broker's trust.
     */
    /**
     * The registry bound {@code name} ({@code region/agent}) to another key than {@code oldKeyFp} (an operator's rebind, a
     * controller this node follows that re-keyed, this node's own identity replaced): the truststore anchors of the key
     * before go ({@link #dropReplacedAnchors}), and message authentication stops serving what it verified on that key
     * ({@link MsgSigning#keyChanged}: cached credentials, sessions, the trust-bundle entry).
     */
    void onKeyReplaced(String name, String oldKeyFp) {
        dropReplacedAnchors(name, oldKeyFp);
        try {
            int s = name == null ? -1 : name.indexOf('/');
            if (s <= 0 || controllerEngine == null) return;
            String region = name.substring(0, s), agent = name.substring(s + 1);
            String present = peerTrust == null ? null : peerTrust.pinnedKey(new PeerIdentity.Expected(region, agent));
            if (present != null) controllerEngine.msgSigning().keyChanged(region, agent, present);
        } catch (Exception ex) {
            logger.error("onKeyReplaced(" + name + "): " + ex.getMessage());
        }
    }

    void dropReplacedAnchors(String name, String oldKeyFp) {
        try {
            if (trustStore == null || name == null) return;
            String prefix = name.replace('/', '_').replaceAll("[^A-Za-z0-9._-]", "_").toLowerCase(java.util.Locale.ROOT) + "#";
            java.util.List<String> drop = new java.util.ArrayList<>();
            synchronized (trustStore) {
                for (String alias : java.util.Collections.list(trustStore.aliases())) {
                    Certificate c = trustStore.getCertificate(alias);
                    boolean sameKey = c != null && oldKeyFp != null && oldKeyFp.equals(PeerTrustRegistry.fingerprint(c.getPublicKey().getEncoded()));
                    if (alias.toLowerCase(java.util.Locale.ROOT).startsWith(prefix) || sameKey) drop.add(alias);
                }
                for (String alias : drop) trustStore.deleteEntry(alias);
            }
            if (!drop.isEmpty()) {
                logger.warn("Removed " + drop.size() + " truststore anchor(s) of " + name + " (its key " + PeerIdentity.shortFp(oldKeyFp)
                        + " was replaced): " + drop);
                saveKeyAndTrustStore();
                if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
            }
        } catch (Exception ex) {
            logger.error("dropReplacedAnchors(" + name + "): " + ex.getMessage());
        }
    }

    /**
     * Reserve this node's own identity in its registry (role self) so no joiner is ever bound to it. Called by a
     * controller before it answers a discovery exchange, and when its region CA re-issues its identity.
     */
    public void bindOwnIdentity() {
        try {
            if (chain == null || chain[0] == null) return;
            CrescoIdentity me = CrescoNames.identityOf(chain[0]);
            if (me == null) return;
            PeerTrustRegistry.BindResult r = peerTrust.bind(me.getTenant(), me.getRegion(), me.getAgent(), chain[0].getPublicKey(),
                    "own", PeerTrustRegistry.ROLE_SELF, true);
            if (!r.ok) logger.error("Could not reserve this node's own identity " + me.getAgentPath() + ": " + r.reason);
        } catch (Exception ex) {
            logger.error("bindOwnIdentity: " + ex.getMessage());
        }
    }

    private boolean persistIdentity() {
        return plugin.getConfig().getBooleanParam("node_identity_persist", true) && keyStoreFilePath == null;
    }

    /** Password for an identity store: a random value in an owner-only file next to it, created on first use. */
    private char[] storePassword(Path store) throws IOException {
        Path pass = store.resolveSibling(store.getFileName() + ".pass");
        if (!Files.exists(pass)) {
            byte[] r = new byte[32];
            new SecureRandom().nextBytes(r);
            // owner-only from creation, fsynced, renamed atomically, directory fsynced
            PeerTrustRegistry.writeAtomically(pass, java.util.HexFormat.of().formatHex(r).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
        return Files.readString(pass).trim().toCharArray();
    }

    private void writeStore(Path store, String alias, PrivateKey key, X509Certificate[] certs) throws Exception {
        writeStore(store, alias, key, certs, storePassword(store));
    }

    private void writeStore(Path store, String alias, PrivateKey key, X509Certificate[] certs, char[] pw) throws Exception {
        KeyStore ks = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        ks.load(null, null);
        ks.setKeyEntry(alias, key, pw, certs);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        ks.store(out, pw);
        PeerTrustRegistry.writeAtomically(store, out.toByteArray());
    }

    private Object[] readStore(Path store, String alias) throws Exception {
        if (!Files.exists(store)) return null;
        // reading never makes a password: a store whose password file is gone is unreadable, not "new"
        if (!Files.exists(store.resolveSibling(store.getFileName() + ".pass"))) {
            throw new IOException("its password file " + store.getFileName() + ".pass is missing");
        }
        return readStore(store, alias, storePassword(store));
    }

    private Object[] readStore(Path store, String alias, char[] pw) throws Exception {
        if (!Files.exists(store)) return null;
        KeyStore ks = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        try (FileInputStream in = new FileInputStream(store.toFile())) {
            ks.load(in, pw);
        }
        Key k = ks.getKey(alias, pw);
        Certificate[] c = ks.getCertificateChain(alias);
        if (!(k instanceof PrivateKey) || c == null || c.length == 0) return null;
        X509Certificate[] x = new X509Certificate[c.length];
        for (int i = 0; i < c.length; i++) x[i] = (X509Certificate) c[i];
        return new Object[]{k, x};
    }

    /** Save this node's key and chain (node_identity_persist, default true; not when keystorefile is configured). */
    private synchronized void saveNodeIdentity() {
        if (!persistIdentity()) return;
        try {
            PrivateKey k = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (k != null && chain != null) writeStore(identityDir().resolve("node-identity.p12"), "node", k, chain);
        } catch (Exception ex) {
            logger.error("Could not save this node's identity key (it will be new after a restart, and peers that bound"
                    + " the old key will refuse it): " + ex.getMessage());
        }
    }

    /** Load the persisted key; re-issue the leaf with it when it names another identity. */
    private boolean loadNodeIdentity() {
        if (!persistIdentity()) return false;
        try {
            Object[] kc = readStore(identityDir().resolve("node-identity.p12"), "node");
            if (kc == null) return false;
            PrivateKey key = (PrivateKey) kc[0];
            X509Certificate[] stored = (X509Certificate[]) kc[1];
            CrescoIdentity have = CrescoNames.identityOf(stored[0]);
            String region = controllerEngine.cstate.getRegion(), agent = controllerEngine.cstate.getAgent();
            // a leaf a region CA issued is renewed by that CA (re-enrollment, or this node's own CA), never by a self-made chain
            boolean regionIssued = stored.length > 1 && stored[1] != null && stored[1].getBasicConstraints() >= 0
                    && PeerTrustRegistry.caScopeOf(stored[1]) != null;
            if (have != null && have.getRegion().equals(region) && have.getAgent().equals(agent) && !regionIssued
                    && chainNeedsRenewal(stored, System.currentTimeMillis(), renewBeforeMs())) {
                // the key is kept (every binding follows it); only the certificates are re-issued
                logger.warn("This node's persisted certificate chain " + validityText(stored) + "; renewing it with the same key");
                generateCertChain(new KeyPair(stored[0].getPublicKey(), key));
                saveNodeIdentity();
            } else if (have != null && have.getRegion().equals(region) && have.getAgent().equals(agent)) {
                this.chain = stored;
                storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, key, stored);
            } else {
                generateCertChain(new KeyPair(stored[0].getPublicKey(), key));
                saveNodeIdentity();
            }
            logger.info("Loaded this node's persisted identity key (" + PeerIdentity.shortFp(PeerIdentity.spkiFingerprint(chain[0])) + ")");
            return true;
        } catch (Exception ex) {
            logger.error("Persisted node identity unreadable (" + ex.getMessage() + "); making a new key -- peers that bound"
                    + " the old one will refuse this node until it is re-enrolled (peer_identity_rebind)");
            return false;
        }
    }

    // ---------------------------------------------------------------- renewal

    /** At least this long before expiry a certificate is renewed ({@code identity_renew_before_days}, default 30). */
    private long renewBeforeMs() {
        return Math.max(1, plugin.getConfig().getIntegerParam("identity_renew_before_days", 30)) * 24L * 3600 * 1000;
    }

    /**
     * Does any certificate in {@code chain} need renewal at {@code now}: expired, not yet valid, or with less than
     * {@link #renewalWindow} of its lifetime left -- a FRACTION of its own lifetime: a third, or {@code minBeforeMs} if that
     * is more, but never more than half. A 3-year chain renews a year ahead; a 7-day region-issued leaf at half-life (so it
     * is never "due" at birth, and a failed re-enrollment has half the lifetime to be retried).
     */
    static boolean chainNeedsRenewal(X509Certificate[] chain, long now, long minBeforeMs) {
        if (chain == null || chain.length == 0 || chain[0] == null) return true;
        for (X509Certificate c : chain) {
            if (c == null) continue;
            long nb = c.getNotBefore().getTime(), na = c.getNotAfter().getTime();
            if (now < nb || now >= na) return true;
            if (na - now <= renewalWindow(intendedLife(nb, na), minBeforeMs)) return true;
        }
        return false;
    }

    /**
     * A certificate's INTENDED lifetime: notAfter - notBefore without the backdate (identity_cert_backdate_hours) that every
     * Cresco certificate carries for peers whose clocks lag. Counting the backdate as lifetime made a leaf of a day or less
     * due the moment it was issued (round-2 review N6). A certificate shorter than the backdate was not backdated by it (an
     * older or foreign issuer): its whole span counts.
     */
    static long intendedLife(long notBefore, long notAfter) {
        long span = notAfter - notBefore, backdate = CertValidity.backdateMs();
        return span > backdate ? span - backdate : span;
    }

    /** How long before its expiry a certificate living {@code lifeMs} is renewed: max(life/3, floor), at most life/2. */
    static long renewalWindow(long lifeMs, long minBeforeMs) {
        return Math.min(Math.max(lifeMs / 3, minBeforeMs), lifeMs / 2);
    }

    private static String validityText(X509Certificate[] chain) {
        long first = Long.MAX_VALUE;
        for (X509Certificate c : chain) if (c != null) first = Math.min(first, c.getNotAfter().getTime());
        return (first <= System.currentTimeMillis() ? "EXPIRED on " : "expires on ") + new java.util.Date(first);
    }

    private java.util.concurrent.ScheduledExecutorService renewal;

    /** Check this node's chain and region CA daily ({@code identity_renew_check_hours}, default 24) and renew them when due. */
    /**
     * Validity of the leaves a region CA issues ({@code identity_leaf_validity_hours}, default 168 = 7 days). Short-lived:
     * an identity that is no longer re-enrolled (a node gone, or refused) lapses on its own; a live node re-enrolls when a
     * third of the lifetime is left.
     */
    long leafValidityMs() {
        return Math.max(1, plugin.getConfig().getIntegerParam("identity_leaf_validity_hours", 168)) * 3600L * 1000;
    }

    private void startRenewalCheck() {
        long minutes = Math.max(1, plugin.getConfig().getIntegerParam("identity_renew_check_minutes", 60));
        renewal = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cresco-identity-renewal");
            t.setDaemon(true);
            return t;
        });
        renewal.scheduleWithFixedDelay(() -> {
            try { renewIfDue(); } catch (Throwable t) { logger.error("identity renewal check: " + t.getMessage()); }
        }, minutes, minutes, java.util.concurrent.TimeUnit.MINUTES);
    }

    /** Is this chain's leaf issued by a region CA this node does not hold (an enrollment it must renew by re-enrolling)? */
    private boolean issuedByRemoteRegionCa(X509Certificate[] c) {
        if (c == null || c.length < 2 || c[1] == null || regionCA != null) return false;
        return PeerTrustRegistry.caScopeOf(c[1]) != null && c[1].getBasicConstraints() >= 0;
    }

    /** Stop this manager's background work (the shared peer trust registry stays open: the broker uses it). */
    public void close() {
        if (renewal != null) renewal.shutdownNow();
        if (provisionedWatcher != null) provisionedWatcher.shutdownNow();
    }

    /**
     * Renew what is due, keeping every key: the region CA (a new root re-signs the same CA key), then this node's chain: under
     * its region CA when it holds one; by RE-ENROLLING with the controller it joined when a remote region CA issued it; else
     * (per-node trust) a fresh self-rooted chain for the same key. Truststore entries of this node's own identity and CA are
     * REPLACED, never accumulated. Returns true when anything was renewed.
     *
     * <p>A region-issued leaf is never replaced by a self-made chain (its region refuses one, and it would keep a key alive
     * that its region stopped vouching for). When re-enrollment keeps failing the node keeps serving its last valid leaf and
     * says so: WARN, CRITICAL in the last tenth of the leaf's life, retrying every {@code identity_reenroll_retry_minutes}
     * (5); at expiry the leaf lapses -- every verifier refuses it, and the region's broker closes the connection -- and the
     * node keeps trying, coming back by itself at the first re-enrollment that succeeds (the region re-enrolls a bound,
     * unrevoked key whatever its last leaf's validity). Connectivity is kept as long as the identity is valid; it is never
     * extended past what the region vouched for.
     */
    public boolean renewIfDue() throws Exception {
        if (provisioned != null) { checkProvisionedExpiry(); return false; }   // the operator renews: a replaced keystore is reloaded
        Object local = renewLocally();
        if (local != RE_ENROLL) return (Boolean) local;
        // Re-enrollment runs an authenticated discovery exchange, and the exchange's own handler takes this manager's lock
        // (it may re-issue the leaf for a new name): it must never be called with the lock held -- that was a self-deadlock
        // that stopped every later renewal of the node (seen live with renewal checks every minute).
        if (!reEnrolling.compareAndSet(false, true)) return false;
        try {
            logger.info("This node's region-issued leaf " + validityText(chain) + "; re-enrolling with its regional controller");
            boolean reEnrolled = false;
            String failure = null;
            try { reEnrolled = controllerEngine.reEnroll(); } catch (Exception ex) { failure = ex.getMessage(); }
            synchronized (this) {
                if (reEnrolled) {
                    reEnrollFailures = 0;
                    setIdentityAlarm(null, false);
                    return true;
                }
                reEnrollFailures++;
                long now = System.currentTimeMillis();
                long left = chain[0].getNotAfter().getTime() - now, life = intendedLife(chain[0].getNotBefore().getTime(), chain[0].getNotAfter().getTime());
                boolean critical = left <= life / 10;
                String text = (left <= 0 ? "this node's region-issued identity EXPIRED on " + chain[0].getNotAfter() + ": its region refuses it until"
                        + " a re-enrollment succeeds" : "this node's region-issued identity " + validityText(chain) + " and re-enrollment with its"
                        + " regional controller failed " + reEnrollFailures + " time(s)" + (failure == null ? "" : " (" + failure + ")")
                        + "; the last valid leaf is kept in use") + "; retrying" + (critical ? " every " + reEnrollRetryMinutes() + " min" : " at every renewal check");
                setIdentityAlarm(text, critical);
                if (critical) scheduleRenewalRetry();
                return false;
            }
        } finally {
            reEnrolling.set(false);
        }
    }

    /** {@link #renewIfDue}'s answer when the node's leaf must be renewed by re-enrolling (done without this manager's lock). */
    private static final Object RE_ENROLL = new Object();
    private final java.util.concurrent.atomic.AtomicBoolean reEnrolling = new java.util.concurrent.atomic.AtomicBoolean();

    /** With the lock: renew what this node renews itself; {@link #RE_ENROLL} when its leaf must be re-enrolled; else renewed or not. */
    private synchronized Object renewLocally() throws Exception {
        long now = System.currentTimeMillis(), before = renewBeforeMs();
        boolean renewed = false;
        if (regionCA != null && chainNeedsRenewal(regionCA.caChain(), now, before)) {
            logger.warn("This node's region CA " + validityText(regionCA.caChain()) + "; renewing it with the same key");
            regionCA = regionCA.renewed(keySize, YEARS_VALID);
            saveRegionCA();
            replaceTrustStoreEntries("localRegionCA-" + controllerEngine.cstate.getRegion(), new X509Certificate[]{ regionCA.caCert() });
            renewed = true;
        }
        if (chain != null && (renewed || chainNeedsRenewal(chain, now, before))) {
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey == null) return renewed;
            if (regionCA != null) {
                CrescoIdentity me = CrescoNames.identityOf(chain[0]);
                if (me != null) {
                    X509Certificate[] fresh = regionCA.enrollFor(chain[0].getPublicKey(), me.getTenant(), me.getRegion(), me.getAgent(), leafValidityMs());
                    this.chain = fresh;
                    storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, fresh);
                }
            } else if (issuedByRemoteRegionCa(chain)) {
                // a leaf our region's CA issued: re-enroll over the authenticated exchange (a fresh short-lived leaf, same key),
                // outside this lock; never renewed with a self-made chain
                return RE_ENROLL;
            } else {
                logger.warn("This node's certificate chain " + validityText(chain) + "; renewing it with the same key");
                generateCertChain(new KeyPair(chain[0].getPublicKey(), myKey));
            }
            saveNodeIdentity();
            replaceTrustStoreEntries(keyStoreAlias, getPublicCertificate());
            renewed = true;
        }
        if (renewed) {
            try { if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) { }
            logger.info("Renewed this node's identity (same key): " + validityText(chain) + "; truststore " + trustStoreSize() + " entries");
        }
        return renewed;
    }

    /** How many entries the truststore holds (logged at renewal: it must not grow). */
    int trustStoreSize() {
        try { return trustStore == null ? 0 : trustStore.size(); } catch (Exception e) { return -1; }
    }

    private volatile int reEnrollFailures;
    private final java.util.concurrent.atomic.AtomicBoolean retryScheduled = new java.util.concurrent.atomic.AtomicBoolean();

    private int reEnrollRetryMinutes() {
        return Math.max(1, plugin.getConfig().getIntegerParam("identity_reenroll_retry_minutes", 5));
    }

    /** One extra renewal check soon (the last tenth of a region-issued leaf's life, re-enrollment failing). */
    private void scheduleRenewalRetry() {
        if (renewal == null || !retryScheduled.compareAndSet(false, true)) return;
        try {
            renewal.schedule(() -> {
                retryScheduled.set(false);
                try { renewIfDue(); } catch (Throwable t) { logger.error("identity renewal retry: " + t.getMessage()); }
            }, reEnrollRetryMinutes(), java.util.concurrent.TimeUnit.MINUTES);
        } catch (Exception e) {
            retryScheduled.set(false);
        }
    }

    // ---------------------------------------------------------------- identity health (reported by IdentityHealthCheck)

    private volatile String identityAlarm;
    private volatile boolean identityAlarmCritical;
    private volatile String clockAlarm;
    private volatile boolean clockAlarmCritical;
    private final java.util.Map<String, String> skewedPeers = new java.util.concurrent.ConcurrentHashMap<>();
    /** How long a clock-skew report stands without being heard again (15 min). */
    static final long SKEW_REPORT_TTL_MS = 15 * 60_000L;

    private void setIdentityAlarm(String text, boolean critical) {
        String old = identityAlarm;
        identityAlarm = text;
        identityAlarmCritical = critical;
        if (text == null) {
            if (old != null) logger.info("Identity renewal OK again (" + validityText(chain) + ")");
        } else if (!text.equals(old)) {
            if (critical) logger.error("IDENTITY: " + text); else logger.warn("IDENTITY: " + text);
        }
    }

    /**
     * A peer's clock reading {@code peerTs} (its currentTimeMillis, e.g. a pong's remote_ts) arrived now: record the skew.
     * Beyond half the certificate backdate it is reported (WARN), beyond the backdate CRITICAL (certificates one side issues
     * are not yet valid for the other).
     */
    public void noteClock(long peerTs, String peer) {
        if (peerTs <= 0 || peer == null) return;
        long skew = peerTs - System.currentTimeMillis();
        String text = CertValidity.skewText(skew, peer);
        if (text == null) {
            if (skewedPeers.remove(peer) != null) logger.info("Clock of " + peer + " agrees with this node's again");
        } else {
            boolean critical = Math.abs(skew) >= CertValidity.backdateMs();
            String level = (critical ? "CRITICAL|" : "WARN|") + System.currentTimeMillis() + "|" + text;
            String old = skewedPeers.size() < 1024 || skewedPeers.containsKey(peer) ? skewedPeers.put(peer, level) : null;
            if (old == null || !old.startsWith(critical ? "CRITICAL" : "WARN")) {
                if (critical) logger.error("CLOCK SKEW: " + text); else logger.warn("CLOCK SKEW: " + text);
            }
        }
    }

    /**
     * This node's identity health: {status, text} with status OK, WARN or CRITICAL. CRITICAL: the peer trust registry cannot
     * lock its store (fenced), is sealed, or holds revocations in force that are not recorded; this node's region-issued
     * identity is in its last tenth (or expired) and re-enrollment fails; a clock is skewed past the certificate backdate.
     */
    public String[] identityHealth() {
        java.util.List<String> crit = new java.util.ArrayList<>(), warn = new java.util.ArrayList<>();
        PeerTrustRegistry r = peerTrust;
        if (r != null) {
            if (r.lockFailure() != null) crit.add("peer trust registry FENCED: " + r.lockFailure() + " (nothing new is bound)");
            if (r.isSealed()) crit.add("peer trust registry SEALED: " + r.sealReason());
            if (r.revocationDurability() != null) crit.add(r.revocationDurability());
        }
        String ia = identityAlarm;
        if (ia != null) (identityAlarmCritical ? crit : warn).add(ia);
        String pa = provisionedAlarm;
        if (pa != null) (provisionedAlarmCritical ? crit : warn).add(pa);
        String rcf = regionCaFailure;
        if (rcf != null) crit.add("REGION CA UNAVAILABLE: " + rcf + " (no identity is issued; restore region-ca.p12 or set region_ca_allow_new)");
        if (chain != null && chain[0] != null) {
            long now = System.currentTimeMillis();
            if (now >= chain[0].getNotAfter().getTime()) crit.add("this node's certificate EXPIRED on " + chain[0].getNotAfter());
            else if (now < chain[0].getNotBefore().getTime()) crit.add("this node's certificate is NOT YET VALID (" + chain[0].getNotBefore()
                    + "): this node's clock is behind its issuer's by more than the certificate backdate");
        }
        // a skew report ages out when that peer is not heard from again (a decommissioned node, a one-off): a peer pings
        // every few seconds, so a report older than SKEW_REPORT_TTL_MS is stale
        long nowMs = System.currentTimeMillis();
        for (java.util.Map.Entry<String, String> e : new java.util.ArrayList<>(skewedPeers.entrySet())) {
            String[] v = e.getValue().split("\\|", 3);
            long at = v.length == 3 ? Long.parseLong(v[1]) : 0;
            if (nowMs - at > SKEW_REPORT_TTL_MS) { skewedPeers.remove(e.getKey(), e.getValue()); continue; }
            ("CRITICAL".equals(v[0]) ? crit : warn).add(v.length == 3 ? v[2] : e.getValue());
        }
        if (!crit.isEmpty()) { crit.addAll(warn); return new String[]{"CRITICAL", String.join("; ", crit)}; }
        if (!warn.isEmpty()) return new String[]{"WARN", String.join("; ", warn)};
        return new String[]{"OK", chain == null || chain[0] == null ? "no identity yet" : "identity " + validityText(chain)};
    }

    /**
     * Replace the truststore entries named {@code <prefix>#...} by {@code certs} (entries are named by fingerprint): this
     * node's own chain or CA after a renewal or re-enrollment REPLACES the previous one instead of piling up. Returns true
     * when the set of entries changed.
     */
    public boolean replaceTrustStoreEntries(String prefix, Certificate[] certs) {
        if (trustStore == null || certs == null) return false;
        try {
            String p = ((prefix == null || prefix.isBlank()) ? "cert" : prefix.replaceAll("[^A-Za-z0-9._-]", "_")).toLowerCase(java.util.Locale.ROOT) + "#";
            java.util.Set<String> keep = new java.util.HashSet<>();
            for (Certificate c : certs) {
                if (c == null) continue;
                keep.add(p + java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(c.getEncoded())));
            }
            java.util.List<String> stale = new java.util.ArrayList<>();
            synchronized (trustStore) {
                for (String alias : java.util.Collections.list(trustStore.aliases())) {
                    if (alias.toLowerCase(java.util.Locale.ROOT).startsWith(p) && !keep.contains(alias.toLowerCase(java.util.Locale.ROOT))) stale.add(alias);
                }
                for (String alias : stale) trustStore.deleteEntry(alias);
            }
            java.util.List<String> added = addCertificatesToTrustStore(prefix, certs);
            if (!stale.isEmpty() && added.isEmpty()) saveKeyAndTrustStore();
            return !stale.isEmpty() || !added.isEmpty();
        } catch (Exception ex) {
            logger.error("replaceTrustStoreEntries(" + prefix + "): " + ex.getMessage());
            return false;
        }
    }

    /**
     * The public-key directory ({@code public_key_directory}): where this node exports its own certificate and from which it
     * imports operator-placed trust anchors. Default {@code <agent data dir>/public-keys} (absolute).
     */
    private Path publicKeyDirectory() {
        String d = plugin.getConfig().getStringParam("public_key_directory");
        if (d == null || d.isBlank()) return identityDir().getParent() == null ? Paths.get("public-keys").toAbsolutePath()
                : identityDir().getParent().resolve("public-keys");
        return Paths.get(d);
    }

    private void exportPublicKey() {
        try {
            Path dir = publicKeyDirectory();
            if (!dir.isAbsolute()) dir = dir.toAbsolutePath();
            PeerTrustRegistry.mkdirs(dir);
            String certFileName = controllerEngine.cstate.getAgentPath() + ".cer";
            PeerTrustRegistry.writeAtomically(dir.resolve(certFileName), getPublicCertificate()[0].getEncoded());
        } catch (Exception ex) {
            logger.error("Failed to export public key: " + ex.getMessage());
        }
    }

    /** SHA-256 of a certificate's DER encoding, lowercase hex (the form {@code public_key_trusted_fingerprints} lists). */
    static String certFingerprint(Certificate c) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(c.getEncoded()));
    }

    /**
     * Audit D6: import operator-placed trust anchors from the public-key directory -- only certificates whose SHA-256
     * fingerprint is listed in {@code public_key_trusted_fingerprints} (comma separated, hex, ':' allowed), only from an
     * ABSOLUTE directory that neither group nor others may write, and only regular files (no symlinks) that neither group nor
     * others may write. Before, every {@code .cer} in a relative {@code public-keys} directory became a trust anchor: whoever
     * could drop a file there (or chose the working directory) chose this node's trust.
     *
     * @return the fingerprints imported
     */
    java.util.List<String> importPublicKeysFromDirectory() {
        java.util.List<String> imported = new java.util.ArrayList<>();
        java.util.Set<String> allow = new java.util.HashSet<>();
        for (String f : csv(plugin.getConfig().getStringParam("public_key_trusted_fingerprints", ""))) {
            allow.add(f.replace(":", "").toLowerCase(java.util.Locale.ROOT));
        }
        if (allow.isEmpty()) return imported;                     // nothing is trusted from the directory unless listed
        Path dir = publicKeyDirectory();
        if (!dir.isAbsolute()) {
            logger.error("public_key_directory '" + dir + "' is not an absolute path: no trust anchor is imported from it");
            return imported;
        }
        if (!Files.isDirectory(dir, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return imported;
        String loose = looseWriter(dir);
        if (loose != null) {
            logger.error("public_key_directory " + dir + " " + loose + ": no trust anchor is imported from it (make it owner-only, chmod 700)");
            return imported;
        }
        try (java.util.stream.Stream<Path> files = Files.list(dir)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                if (!f.getFileName().toString().endsWith(".cer") || !Files.isRegularFile(f, java.nio.file.LinkOption.NOFOLLOW_LINKS)) continue;
                String why = looseWriter(f);
                if (why != null) { logger.error("Not importing " + f + ": it " + why); continue; }
                try (InputStream in = Files.newInputStream(f)) {
                    Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(in);
                    String fp = certFingerprint(cert);
                    if (!allow.contains(fp)) {
                        logger.debug("public-key directory: " + f.getFileName() + " (" + fp + ") is not in public_key_trusted_fingerprints; not imported");
                        continue;
                    }
                    trustStore.setCertificateEntry("imported#" + fp, cert);
                    imported.add(fp);
                    logger.info("Imported trust anchor " + f.getFileName() + " (" + fp + ") from " + dir + " (listed in public_key_trusted_fingerprints)");
                } catch (Exception e) {
                    logger.error("Failed to import certificate " + f.getFileName() + ": " + e.getMessage());
                }
            }
        } catch (Exception ex) {
            logger.error("Error importing public keys: " + ex.getMessage());
        }
        return imported;
    }

    /** Why {@code p} is writable by someone other than its owner (or owned by another user), or null. */
    private static String looseWriter(Path p) {
        try {
            java.util.Set<java.nio.file.attribute.PosixFilePermission> perms =
                    Files.getPosixFilePermissions(p, java.nio.file.LinkOption.NOFOLLOW_LINKS);
            if (perms.contains(java.nio.file.attribute.PosixFilePermission.GROUP_WRITE)
                    || perms.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE)) {
                return "is writable by group or others (" + java.nio.file.attribute.PosixFilePermissions.toString(perms) + ")";
            }
            String owner = Files.getOwner(p, java.nio.file.LinkOption.NOFOLLOW_LINKS).getName(), me = System.getProperty("user.name");
            if (!owner.equals(me) && !owner.equals("root")) return "is owned by " + owner + ", not this node's user " + me;
            return null;
        } catch (UnsupportedOperationException notPosix) {
            return "has no POSIX permissions to check";
        } catch (IOException e) {
            return "cannot be checked (" + e.getMessage() + ")";
        }
    }

    /**
     * Add {@code certs} to the truststore. Each entry is named by the certificate's own SHA-256 fingerprint (after a
     * caller-chosen prefix), never by its subject: a subject is whatever the certificate's maker wrote, so naming by it let
     * a peer's certificate that copied another's subject REPLACE that anchor (e.g. a region CA), cutting off everyone it
     * vouched for. An existing entry is never overwritten; the same certificate twice is one entry.
     *
     * @return the aliases newly added
     */
    public java.util.List<String> addCertificatesToTrustStore(String alias, Certificate[] certs) {
        java.util.List<String> added = new java.util.ArrayList<>();
        if (certs == null) return added;
        try {
            String prefix = (alias == null || alias.isBlank()) ? "cert" : alias.replaceAll("[^A-Za-z0-9._-]", "_");
            for (Certificate cert : certs) {
                if (cert == null) continue;
                String fp = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(cert.getEncoded()));
                String name = (prefix + "#" + fp).toLowerCase(java.util.Locale.ROOT);
                if (trustStore.containsAlias(name)) continue;
                logger.debug("addCertificatesToTrustStore: adding " + name);
                trustStore.setCertificateEntry(name, cert);
                added.add(name);
            }
            if (!added.isEmpty()) saveKeyAndTrustStore();
        } catch (Exception ex) {
            logger.error("addCertificatesToTrustStore() : error " + ex.getMessage());
        }
        return added;
    }

    public X509Certificate[] getPublicCertificate() {
        NodeIdentityKeyManager.Identity pi = presented;
        if (pi != null) return pi.chain;
        return chain;
        //return certs;
    }

    private void generateCertChain() {
        generateCertChain(null);
    }

    /**
     * (Re)make this node's self-rooted chain [leaf, intermediate, root]. With {@code leafKeyPair} the leaf keeps that
     * key: identity bindings (pins, a CA's name-to-key records) follow the key, so re-issuing a leaf for a new name
     * must not change it.
     */
    private void generateCertChain(KeyPair leafKeyPair) {

        try {
            // Create self signed Root CA certificate

            String agentName = "agent-" + UUID.randomUUID().toString();
            String pluginName = "plugin-" + UUID.randomUUID().toString();
            String functionName = "message-" + UUID.randomUUID().toString();

            KeyPair rootCAKeyPair = generateKeyPair();

            X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                    new X500Name("CN=rootCA-" + agentName), // issuer authority
                    CertValidity.serial(), //serial number of certificate
                    CertValidity.notBefore(), // start of validity (backdated for peers whose clocks are behind)
                    CertValidity.notAfterYears(YEARS_VALID),
                    //new DateTime(2025, 12, 31, 0, 0, 0, 0).toDate(), //end of certificate validity
                    new X500Name("CN=rootCA-" + agentName), // subject name of certificate
                    rootCAKeyPair.getPublic()); // public key of certificate
            // key usage restrictions
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
            // every signature suite below is the crypto policy's choice (SHA384withRSA under the default cnsa1);
            // certificates are made before the node holds the fabric policy, so CrescoCrypto uses the effective one
            X509Certificate rootCA = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).
                            build(rootCAKeyPair.getPrivate()))); // private key of signing authority , here it is self signed


            //create Intermediate CA cert signed by Root CA
            KeyPair intermedCAKeyPair = generateKeyPair();
            builder = new JcaX509v3CertificateBuilder(
                    rootCA, // here rootCA is issuer authority
                    CertValidity.serial(), CertValidity.notBefore(),
                    CertValidity.notAfterYears(YEARS_VALID),
                    new X500Name("CN=IntermedCA-" + pluginName), intermedCAKeyPair.getPublic());
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
            X509Certificate intermedCA = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).
                            build(rootCAKeyPair.getPrivate())));// private key of signing authority , here it is signed by rootCA

            //create end user cert signed by Intermediate CA
            KeyPair endUserCertKeyPair = (leafKeyPair != null) ? leafKeyPair : generateKeyPair();
            // Identity-bearing leaf: bind tenant/region/agent into the subject DN so the broker (and
            // any peer) can cryptographically identify this node from its certificate rather than a
            // spoofable connection username. Falls back to the legacy UUID CN before identity is known.
            String leafDN;
            String idTenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
            String idRegion = controllerEngine.cstate.getRegion();
            String idAgent = controllerEngine.cstate.getAgent();
            X500Name leafName;
            CrescoIdentity own = CrescoIdentity.of(idTenant, idRegion, idAgent, null);
            if (CrescoNames.isValid(own)) {
                leafName = CrescoNames.x500(own);                      // from the attribute values, never a string DN
                leafDN = leafName.toString();
                logger.info("Issuing identity-bearing leaf certificate: " + leafDN);
            } else {
                if (idRegion != null && idAgent != null) {
                    logger.error("This node's identity cannot be put in a certificate: "
                            + firstNonNull(CrescoNames.whyInvalid("tenant_id", idTenant), CrescoNames.whyInvalid("region", idRegion),
                            CrescoNames.whyInvalid("agent", idAgent)) + ". Peers that bind connections to identities (peer_identity_mode=enforce) will refuse it.");
                }
                leafDN = "CN=endUserCert-" + functionName;
                leafName = new X500Name(leafDN);
            }
            builder = new JcaX509v3CertificateBuilder(
                    intermedCA, //here intermedCA is issuer authority
                    CertValidity.serial(), CertValidity.notBefore(),
                    CertValidity.notAfterYears(YEARS_VALID),
                    leafName, endUserCertKeyPair.getPublic());
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
            X509Certificate endUserCert = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).
                            build(intermedCAKeyPair.getPrivate())));// private key of signing authority , here it is signed by intermedCA


            chain = new X509Certificate[3];
            chain[0]=endUserCert;
            chain[1]=intermedCA;
            chain[2]=rootCA;


            //Store the certificate chain
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, endUserCertKeyPair.getPrivate(), chain);

            //Reload the keystore and display key and certificate chain info
            //loadCertChain(alias, keyStorePassword, keystore);
            //Clear the keystore
            //clearKeyStore(alias, password, keystore);
            //signingCertificate = intermedCA;
            //signingKey = intermedCAKeyPair.getPrivate();


        } catch (Exception ex) {
            logger.error("CertificateManager.generateCertChain failure", ex);
        }

    }

    private static String firstNonNull(String... s) {
        for (String x : s) if (x != null) return x;
        return null;
    }

    private KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA");
        kpGen.initialize(keySize, new SecureRandom());
        return kpGen.generateKeyPair();
    }

    /**
     * This node's TLS trust managers: PKIX over the truststore, or a chain whose leaf key is bound to the identity it names
     * in the peer trust registry ({@link BoundKeyTrustManager}) -- so bound peers are still trusted after a restart, when
     * the in-memory truststore no longer holds their self-rooted chains.
     */
    public TrustManager[] getTrustManagers() {
        TrustManager[] trustManagers = null;
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            trustManagers = trustManagerFactory.getTrustManagers();
            for (int i = 0; i < trustManagers.length; i++) {
                if (trustManagers[i] instanceof X509TrustManager) {
                    trustManagers[i] = new BoundKeyTrustManager((X509TrustManager) trustManagers[i], peerTrust, logger);
                }
            }
        } catch(Exception ex) {
            logger.error("getTrustManagers Error : " + ex.getMessage());
        }
        return trustManagers;
    }

    /**
     * The node's current trust manager (built per call, so it sees every addition): PKIX over its truststore, or a
     * registry-bound key (see {@link #getTrustManagers}).
     */
    public X509TrustManager currentPkixTrustManager() {
        TrustManager[] tms = getTrustManagers();
        if (tms != null) {
            for (TrustManager tm : tms) {
                if (tm instanceof X509TrustManager) return (X509TrustManager) tm;
            }
        }
        return null;
    }

    public PeerTrustRegistry getPeerTrust() {
        return peerTrust;
    }

    /** {@code peer_identity_mode}: enforce (default) | warn | off. */
    public PeerIdentity.Mode peerIdentityMode() {
        return PeerIdentity.Mode.parse(plugin.getConfig().getStringParam(PeerIdentity.MODE_KEY, "enforce"));
    }

    /**
     * Trust managers for ONE outbound broker connection meant to reach {@code expected}: PKIX over the current
     * truststore, then the certificate must name {@code expected} (and carry its pinned key when one was learned).
     * With {@code peer_identity_mode=off} this is the plain PKIX trust managers, as before.
     */
    public TrustManager[] clientTrustManagers(PeerIdentity.Expected expected, String endpoint) {
        PeerIdentity.Mode mode = peerIdentityMode();
        if (mode == PeerIdentity.Mode.OFF) {
            return getTrustManagers();
        }
        return new TrustManager[]{ new PeerIdentityTrustManager(this::currentPkixTrustManager, expected, peerTrust, mode, logger, endpoint, outboundConnections()) };
    }

    /**
     * Make our own leaf name our CURRENT identity. The leaf is made when the node
     * starts, before an agent joins a region, so an agent that was not configured with the region it later joins
     * carries {@code OU=<its start-up region>} while the fabric (and its inbox, and stream recipients) know it by
     * the region it joined. Called before our certificate goes out in a discovery exchange; re-issues the chain only
     * when the names differ, keeping the leaf key (identity bindings follow the key). A node that holds a region CA
     * stamps its own identity, so this is a no-op there; under the regional CA a joining agent's leaf is replaced
     * by the one its region issues at enrollment anyway.
     *
     * @return true when the leaf was re-issued
     */
    public synchronized boolean ensureLeafIdentity() {
        if (provisioned != null) return false;                         // a provisioned identity is never re-issued here
        try {
            if (regionCA != null) return false;                        // a CA holder's identity is its own CA's to stamp
            String region = controllerEngine.cstate.getRegion();
            String agent = controllerEngine.cstate.getAgent();
            String tenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
            if (region == null || agent == null || chain == null || chain[0] == null) return false;
            CrescoIdentity have = CrescoNames.identityOf(chain[0]);
            // under the regional CA the issuer stamps the tenant, so only region/agent must match there
            boolean regionalCa = plugin.getConfig().getBooleanParam("security_regional_ca", false);
            if (have != null && region.equals(have.getRegion()) && agent.equals(have.getAgent())
                    && (regionalCa || tenant.equals(have.getTenant()))) {
                return false;
            }
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            generateCertChain(myKey != null ? new KeyPair(chain[0].getPublicKey(), myKey) : null);   // same key, new name
            saveNodeIdentity();
            replaceTrustStoreEntries(keyStoreAlias, getPublicCertificate());
            try { if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            try { if (controllerEngine.getActiveClient() != null) controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
            logger.info("Re-issued own certificate for the current identity " + CrescoIdentity.fromCertificate(chain[0])
                    + " (it named " + have + ")");
            return true;
        } catch (Exception ex) {
            logger.error("ensureLeafIdentity error: " + ex.getMessage(), ex);
            return false;
        }
    }

    /** The identity the key managers present now: the provisioned pair, else the current self-made chain (keystore key). */
    NodeIdentityKeyManager.Identity presentedIdentity() {
        NodeIdentityKeyManager.Identity p = presented;
        if (p != null) return p;
        X509Certificate[] c = chain;
        NodeIdentityKeyManager.Identity s = selfMade;
        if (c == null) return null;
        if (s == null || s.chain != c) {
            s = new NodeIdentityKeyManager.Identity(c, null, IDENTITY_GEN.incrementAndGet());
            selfMade = s;
        }
        return s;
    }

    public KeyManager[] getKeyManagers() {
        KeyManager[] keyManagers = null;
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keyStorePassword);
            keyManagers = keyManagerFactory.getKeyManagers();
            // always present the node's identity as a client, even when the server's CA hints do not name its issuer
            // (a restarted server under per-node trust): see NodeIdentityKeyManager
            for (int i = 0; i < keyManagers.length; i++) {
                if (keyManagers[i] instanceof X509KeyManager) {
                    // the CURRENT chain: a connection factory made before a renewal or re-enrollment presents the new leaf
                    // when it reconnects (same key), not the snapshot it was made with
                    keyManagers[i] = new NodeIdentityKeyManager((X509KeyManager) keyManagers[i], () -> keyStoreAlias)
                            .withCurrentChain(() -> chain).withCurrentKey(() -> currentIdentityKey)
                            .withCurrentIdentity(this::presentedIdentity);
                }
            }
        } catch(Exception ex) {
            logger.error("getKeyManagers Error : " + ex.getMessage());
        }
        return keyManagers;
    }

    private void storeKeyAndCertificateChain(String alias, char[] password, Key key, X509Certificate[] chain) throws Exception{


        //KeyStore keyStore=KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        //keyStore=KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        //keyStore.load(null,null);
        keyStore.setKeyEntry(alias, key, password, chain);

        //trustStore.setKeyEntry(alias, key, password, chain);

        //trustStore = keyStore;
        //TODO trying to to save
        //keyStore.store(new FileOutputStream(keystore),password);

    }

    private String getStringFromCert(X509Certificate cert) {
        String certString = null;
        try {
            certString = Base64.getEncoder().encodeToString(cert.getEncoded());

        } catch(Exception ex) {
            logger.error("getStringfromCert : error " + ex.getMessage(), ex);
        }
        return certString;
    }

    private X509Certificate getCertfromString(String certString) {
        X509Certificate cert= null;
        try {
            /*
            byte[] valueDecoded = Base64.decodeBase64(bytesEncoded);
System.out.println("Decoded value is " + new String(valueDecoded));
             */
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            InputStream inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(certString));
            cert = (X509Certificate)certificateFactory.generateCertificate(inputStream);
            if (inputStream != null) {
                inputStream.close();
            }
        } catch(Exception ex) {
            // peer input, often before authentication: one line, no stack trace
            logger.warn("getCertfromString: not a certificate: " + ex.getClass().getSimpleName());
            logger.debug("getCertfromString", ex);
        }
        return cert;
    }

    /** Most certificates a chain from a peer may carry (leaf, issuing CA, root). */
    public static final int MAX_CHAIN_CERTS = 3;

    /**
     * A peer's certificate chain from its JSON form (an array of up to {@link #MAX_CHAIN_CERTS} certificates), or
     * {@code null} if it is malformed or longer. Peer input, often before authentication: a refusal is one line, never a
     * stack trace.
     */
    public X509Certificate[] getCertsfromJson(String jsonCerts) {
        X509Certificate[] certs = null;
        try {

            Gson gc = new Gson();
            String[] certImportList = gc.fromJson(jsonCerts,String[].class);
            if (certImportList == null) return null;
            if (certImportList.length > MAX_CHAIN_CERTS) {
                logger.warn("getCertsfromJson: refused a chain of " + certImportList.length + " certificates (at most "
                        + MAX_CHAIN_CERTS + ")");
                return null;
            }
            certs = new X509Certificate[MAX_CHAIN_CERTS];
            for(int i = 0; i<certImportList.length; i++) {
                certs[i] = getCertfromString(certImportList[i]);
            }

        } catch(Exception ex) {
            logger.warn("getCertsfromJson: refused a malformed certificate chain: " + ex.getClass().getSimpleName());
            logger.debug("getCertsfromJson", ex);
            certs = null;
        }
        return certs;
    }

    private String[] getStringsFromCerts(X509Certificate[] certs) {
        String[] certStrings = null;
        try {
            certStrings = new String[certs.length];

            for(int i = 0; i < certs.length; i++) {
                certStrings[i] = getStringFromCert(certs[i]);
            }

        } catch(Exception ex) {
            logger.error("getStringsfromCerts : error " + ex.getMessage(), ex);
        }
        return certStrings;
    }

    public String getJsonFromCerts(X509Certificate[] certs) {
        String certJson = null;
        try {
            String[] certArray = getStringsFromCerts(certs);
            Gson gc = new Gson();
            certJson = gc.toJson(certArray);

        } catch(Exception ex) {
            logger.error("getJsonFromCerts : error " + ex.getMessage(), ex);
        }
        return certJson;
    }

    // ----- Regional-CA enrollment (distributed-trust Option C) -----

    /** True if this controller is an issuing authority (holds a region CA). */
    public boolean isRegionCA() {
        return regionCA != null;
    }

    /**
     * Make this controller a region CA: generate the CA, trust it, and re-issue this controller's OWN
     * identity under it so peers that trust the region CA cert also trust this controller's broker.
     * Idempotent. Called at startup when {@code security_regional_ca} is enabled on a region/global.
     */
    public synchronized void ensureRegionCA() {
        if (regionCA != null) {
            return;
        }
        if (provisioned != null) {
            if (!regionCaOffLogged) {
                regionCaOffLogged = true;
                logger.info("security_regional_ca is ignored: this node's identity is pre-provisioned (the operator's PKI issues every identity)");
            }
            return;
        }
        // a region CA that existed but cannot be loaded is retried at most once a minute (an operator may restore it)
        if (regionCaFailure != null && System.currentTimeMillis() - regionCaFailureAt < 60_000L) return;
        try {
            String tenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
            String region = controllerEngine.cstate.getRegion();
            String agent = controllerEngine.cstate.getAgent();
            regionCA = loadRegionCA(tenant, region);
            if (regionCA == null && regionCaFailure != null) {
                // never a silent new CA: every agent's identity and every peer's record of this region name the old one
                regionCaFailureAt = System.currentTimeMillis();
                logger.error("REGION CA UNAVAILABLE: " + regionCaFailure + ". This controller issues NO identities (joining agents are"
                        + " refused, enrolled agents cannot re-enroll) until it is recovered. To recover: restore " + regionCaStore()
                        + " and " + regionCaStore().getFileName() + ".pass from a backup (picked up within a minute, no restart); or, to start"
                        + " a NEW region CA deliberately, set region_ca_allow_new=true for one start -- then every agent re-enrolls and every"
                        + " peer that recorded this region's CA (its global, peer regions) must rebind the region (the line '" + region
                        + "' in its peer-trust.json.rebind).");
                return;
            }
            if (regionCA == null) {
                regionCA = RegionCA.generate(tenant, region, keySize, YEARS_VALID);
                saveRegionCA();
                if (regionCaFailureAt != 0) logger.warn("A NEW region CA was generated for region " + region + " (region_ca_allow_new)");
            }
            regionCaFailure = null;
            replaceTrustStoreEntries("localRegionCA-" + region, new X509Certificate[]{ regionCA.caCert() });
            // revocations of keys this CA enrolled can expire once every leaf it issued for them has
            peerTrust.setIssuedLeafLifetime(leafValidityMs());
            String scope = peerTrust.trustCa(regionCA.caCert(), region, tenant, "own");
            if (scope != null) {
                logger.error("This node's region CA is not the one recorded for region " + region + ": " + scope);
            }
            // self-enroll: replace our self-signed leaf with one signed by our own region CA (same key)
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey != null && getPublicCertificate() != null && getPublicCertificate()[0] != null) {
                PeerTrustRegistry.BindResult own = peerTrust.bind(tenant, region, agent, getPublicCertificate()[0].getPublicKey(), "own",
                        PeerTrustRegistry.ROLE_SELF, true);
                if (!own.ok) logger.error("This controller's own identity " + region + "/" + agent + " is bound to another key: " + own.reason);
                X509Certificate[] selfChain = regionCA.enrollFor(getPublicCertificate()[0].getPublicKey(), tenant, region, agent, leafValidityMs());
                this.chain = selfChain;
                storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, selfChain);
                saveNodeIdentity();
            }
            try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            logger.info("Region CA established for region '" + region + "' (tenant=" + tenant + "); own identity re-issued under it");
        } catch (Exception ex) {
            logger.error("ensureRegionCA error: " + ex.getMessage(), ex);
        }
    }

    private Path regionCaStore() {
        return identityDir().resolve("region-ca.p12");
    }

    /** {@code region_ca_password_file}: the operator's owner-only file whose first line encrypts the region CA's key at rest. */
    static final String REGION_CA_PASSWORD_FILE = "region_ca_password_file";

    /**
     * The password the region CA's store is encrypted with: the first line of {@code region_ca_password_file} (owner-only,
     * kept where the operator keeps secrets: a mounted secret, another volume), or null when it is not set -- then the
     * store's own random password beside it (region-ca.p12.pass), which protects the key no better than the file permissions
     * of that one directory, and is WARNed about.
     */
    private char[] operatorRegionCaPassword() throws Exception {
        String f = plugin.getConfig().getStringParam(REGION_CA_PASSWORD_FILE);
        if (f == null || f.isBlank()) return null;
        Path p = Paths.get(f.trim());
        ProvisionedIdentity.checkFile(p, true, REGION_CA_PASSWORD_FILE);
        String line = Files.readAllLines(p, java.nio.charset.StandardCharsets.UTF_8).stream().findFirst().orElse("").trim();
        if (line.length() < 16) throw new IOException(REGION_CA_PASSWORD_FILE + " " + p + " holds fewer than 16 characters");
        return line.toCharArray();
    }

    private volatile boolean regionCaPlainWarned;

    private void saveRegionCA() {
        if (!plugin.getConfig().getBooleanParam("node_identity_persist", true)) return;
        try {
            char[] op = operatorRegionCaPassword();
            Path store = regionCaStore(), sibling = store.resolveSibling(store.getFileName() + ".pass");
            if (op != null) {
                writeStore(store, "region-ca", regionCA.caPrivateKey(), regionCA.caChain(), op);
                // the key is now encrypted under the operator's password: the password that sat beside it goes
                if (Files.deleteIfExists(sibling)) logger.info("The region CA's key is encrypted under " + REGION_CA_PASSWORD_FILE
                        + "; its former password file " + sibling + " was removed");
            } else {
                writeStore(store, "region-ca", regionCA.caPrivateKey(), regionCA.caChain());
                if (!regionCaPlainWarned) {
                    regionCaPlainWarned = true;
                    logger.warn("WARNING: the region CA's private key is encrypted only under the random password beside it ("
                            + sibling.getFileName() + "): set " + REGION_CA_PASSWORD_FILE + " (an owner-only file kept elsewhere) to"
                            + " encrypt it at rest");
                }
            }
        } catch (Exception ex) {
            logger.error("Could not save the region CA (a restart will make a new one, which peers that recorded this CA refuse): " + ex.getMessage());
        }
    }

    private volatile boolean regionCaOffLogged;

    /** Why the region CA that existed cannot be loaded (none is generated in its place; CRITICAL), or null. */
    private volatile String regionCaFailure;
    private volatile long regionCaFailureAt;

    /** Why this region's CA is unavailable (it existed and cannot be loaded), or null. */
    public String regionCaFailure() { return regionCaFailure; }

    /**
     * Load the persisted region CA. Null with {@link #regionCaFailure} set when one EXISTED -- its store is present, or this
     * node's registry records its own CA for the region -- but cannot be used (unreadable, its key or chain missing, made for
     * another region or tenant): a new CA is then NOT generated unless {@code region_ca_allow_new=true}. Null without a
     * failure on a first start (no CA ever) or with node_identity_persist=false.
     */
    private RegionCA loadRegionCA(String tenant, String region) {
        regionCaFailure = null;
        if (!plugin.getConfig().getBooleanParam("node_identity_persist", true)) return null;
        boolean allowNew = plugin.getConfig().getBooleanParam("region_ca_allow_new", false);
        Path store = regionCaStore();
        boolean existed = Files.exists(store) || Files.exists(store.resolveSibling(store.getFileName() + ".pass"))
                || peerTrust.ownCaRecorded(region);
        String failure;
        try {
            char[] op = operatorRegionCaPassword();
            Path sibling = store.resolveSibling(store.getFileName() + ".pass");
            Object[] kc;
            boolean migrate = false;
            if (op == null) {
                kc = readStore(store, "region-ca");
            } else {
                try {
                    kc = readStore(store, "region-ca", op);
                } catch (Exception wrongPassword) {
                    // a store made before region_ca_password_file was set: open it with its own password once, re-encrypt it
                    if (!Files.exists(sibling)) throw new IOException("it does not open with " + REGION_CA_PASSWORD_FILE + " ("
                            + wrongPassword.getMessage() + ")");
                    kc = readStore(store, "region-ca");
                    migrate = kc != null;
                }
            }

            if (kc == null) {
                if (!existed) return null;                           // a first start: the CA is made now
                failure = Files.exists(store) ? store + " holds no region CA key and chain"
                        : store + " is missing although this region's CA existed (its registry records it)";
            } else {
                X509Certificate[] c = (X509Certificate[]) kc[1];
                String[] scope = PeerTrustRegistry.caScopeOf(c[0]);
                if (scope == null || !scope[0].equals(region) || !scope[1].equals(tenant) || c.length < 2) {
                    failure = "the persisted region CA " + store + " is for " + (scope == null ? "?" : scope[1] + "/" + scope[0]) + ", not "
                            + tenant + "/" + region;
                } else {
                    logger.info("Loaded the persisted region CA for region " + region);
                    RegionCA ca = RegionCA.restore(c[1], new KeyPair(c[0].getPublicKey(), (PrivateKey) kc[0]), c[0]);
                    if (migrate) {
                        regionCA = ca;
                        saveRegionCA();
                        logger.warn("The region CA's key was RE-ENCRYPTED under " + REGION_CA_PASSWORD_FILE + " (it was under the"
                                + " password beside it, which is removed)");
                    }
                    if (chainNeedsRenewal(c, System.currentTimeMillis(), renewBeforeMs())) {
                        logger.warn("The persisted region CA for region " + region + " " + validityText(c) + "; renewing it with the same key");
                        ca = ca.renewed(keySize, YEARS_VALID);
                        regionCA = ca;
                        saveRegionCA();
                    }
                    if (op == null && !regionCaPlainWarned) {
                        regionCaPlainWarned = true;
                        logger.warn("WARNING: the region CA's private key is encrypted only under the random password beside it ("
                                + sibling.getFileName() + "): set " + REGION_CA_PASSWORD_FILE + " (an owner-only file kept elsewhere) to"
                                + " encrypt it at rest");
                    }
                    return ca;
                }
            }
        } catch (Exception ex) {
            failure = "the persisted region CA " + store + " is unreadable (" + ex.getMessage() + ")";
        }
        if (allowNew) {
            logger.error(failure + "; region_ca_allow_new=true: a NEW region CA is made (every peer that recorded the old one must rebind"
                    + " region " + region + ")");
            return null;
        }
        regionCaFailure = failure;
        return null;
    }

    /**
     * Issue a region-CA-signed leaf for an enrolling node. The joiner presents its self-signed cert
     * (identity-bearing DN + public key); we take ONLY its public key and STAMP the identity from the
     * authenticated join (a regional CA forces its own tenant; a global preserves the region's tenant).
     * Returns the region-signed chain [leaf, regionCA, root] as JSON for the joiner to install.
     */
    public String issueEnrollment(String joinerCertJson) {
        return issueEnrollment(joinerCertJson, null);
    }

    /**
     * As {@link #issueEnrollment(String)}, for a joiner that came in over a discovery exchange of {@code joinType}.
     * The issuing controller decides the identity, so it refuses identities the joiner cannot hold:
     * <ul>
     *   <li>an AGENT joins THIS region, so its certificate is stamped with this controller's region whatever its own
     *       self-signed certificate claimed (an agent belongs to the region it joined);</li>
     *   <li>nobody is issued this controller's own identity, and a joining region/global controller cannot take this
     *       controller's region name: either would let the joiner answer for this controller (or its region) on a
     *       TLS connection that checks the certificate's identity (PeerIdentity).</li>
     * </ul>
     */
    public String issueEnrollment(String joinerCertJson, io.cresco.agent.controller.netdiscovery.DiscoveryType joinType) {
        if (provisioned != null) {
            logger.error("Enrollment REFUSED: this node is pre-provisioned; identities are issued by the operator's PKI, never at a join");
            return null;
        }
        try {
            ensureRegionCA();
            if (regionCA == null) {
                if (regionCaFailure != null) logger.error("Enrollment REFUSED: this region's CA is unavailable (" + regionCaFailure + ")");
                return null;
            }
            X509Certificate[] joinerCerts = getCertsfromJson(joinerCertJson);
            if (joinerCerts == null || joinerCerts.length == 0 || joinerCerts[0] == null) {
                logger.error("issueEnrollment: no joiner certificate");
                return null;
            }
            // The joiner's claim, read strictly: every name in the Cresco grammar (no spaces, '#', '_' or specials that
            // a DN parser would normalise into somebody else's name), or nothing is issued.
            boolean controllerJoin = joinType == io.cresco.agent.controller.netdiscovery.DiscoveryType.REGION
                    || joinType == io.cresco.agent.controller.netdiscovery.DiscoveryType.GLOBAL;
            // an agent's claimed region and tenant are replaced by this CA's, so only its agent name must be present
            CrescoIdentity claimed = controllerJoin ? CrescoNames.identityOf(joinerCerts[0]) : CrescoNames.claimOf(joinerCerts[0]);
            if (claimed == null) {
                logger.error("Enrollment REFUSED: the joiner's certificate names no valid Cresco identity (subject '"
                        + joinerCerts[0].getSubjectX500Principal().getName() + "'); names must be "
                        + "1-64 of A-Z a-z 0-9 . - starting and ending with a letter or digit");
                return null;
            }
            String myRegion = controllerEngine.cstate.getRegion();
            String myAgent = controllerEngine.cstate.getAgent();
            String myTenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
            if (controllerJoin) {
                return federateController(joinerCerts, claimed, myRegion, myAgent);
            }

            // An AGENT joins THIS region: its certificate names this region and this CA's tenant (a region CA vouches for
            // its own region and tenant only), whatever its self-signed certificate claimed.
            String tenant = myTenant;
            String region = myRegion;
            String agent = claimed.getAgent();
            if (!region.equals(claimed.getRegion()) || !tenant.equals(claimed.getTenant())) {   // (absent counts as differing)
                logger.info("Enrollment: agent " + agent + " claimed " + claimed.getTenant() + "/" + claimed.getRegion()
                        + "; stamping this CA's " + tenant + "/" + region + " (it joined here)");
            }
            if (myAgent != null && myAgent.equals(agent)) {
                logger.error("Enrollment REFUSED: a joiner asked for this controller's own identity region=" + region + ", agent=" + agent);
                return null;
            }
            String agentRefusal = agentJoinRefusal(tenant, region, agent);
            if (agentRefusal != null) {
                logger.error("Enrollment REFUSED: " + region + "/" + agent + ": " + agentRefusal);
                return null;
            }
            // one key per name: a second key for a name this CA already issued is refused unless the operator rebinds it
            java.security.PublicKey joinerKey = joinerCerts[0].getPublicKey();
            PeerTrustRegistry.BindResult free = peerTrust.canBind(tenant, region, agent, joinerKey);
            if (!free.ok) {
                logger.error("Enrollment REFUSED: identity " + region + "/" + agent + " is taken: " + free.reason);
                return null;
            }
            X509Certificate[] signed = regionCA.enrollFor(joinerKey, tenant, region, agent, leafValidityMs());
            // run the guards again on the name AS ISSUED (what every verifier will read), not on the strings we passed in
            CrescoIdentity issued = CrescoNames.identityOf(signed[0]);
            if (issued == null || !tenant.equals(issued.getTenant()) || !region.equals(issued.getRegion()) || !agent.equals(issued.getAgent())
                    || (myRegion.equals(issued.getRegion()) && myAgent != null && myAgent.equals(issued.getAgent()))) {
                logger.error("Enrollment REFUSED: the certificate as issued names " + issued + ", not tenant=" + tenant
                        + ", region=" + region + ", agent=" + agent + " (or names this controller)");
                return null;
            }
            PeerTrustRegistry.BindResult bound = peerTrust.bind(tenant, region, agent, joinerKey, "enrolled");
            if (!bound.ok) {
                logger.error("Enrollment REFUSED: " + region + "/" + agent + ": " + bound.reason);
                return null;
            }
            logger.info("Enrolled node " + region + "_" + agent + " (tenant=" + tenant + ") under region CA");
            return getJsonFromCerts(signed);
        } catch (Exception ex) {
            logger.error("issueEnrollment error: " + ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Why a CA that a federating controller returned may not be recorded, or null: it must be a valid CA certificate now,
     * name the region of the controller that answered ({@code peerRegion}), never this node's own region (whose CA is this
     * node's, or the one it joined), and a tenant that is not a superuser tenant ({@code broker_superuser_tenants}) and is
     * federated here ({@code security_federated_tenants}, empty = any). The same rules a responder applies to a joining
     * controller ({@link #federateController}).
     */
    public String federatedCaRefusal(X509Certificate peerCa, String peerRegion) {
        String[] scope = PeerTrustRegistry.caScopeOf(peerCa);
        if (scope == null) return "it names no valid region (OU) and tenant (O)";
        if (peerCa.getBasicConstraints() < 0) return "it is not a CA certificate";
        try { peerCa.checkValidity(); } catch (Exception e) { return "it is not valid now (" + e.getMessage() + ")"; }
        if (peerRegion == null || !peerRegion.equals(scope[0])) return "it names region " + scope[0] + ", not the answering controller's " + peerRegion;
        String myRegion = controllerEngine.cstate.getRegion();
        if (scope[0].equals(myRegion)) return "it claims this node's own region " + myRegion;
        if (superuserTenants().contains(scope[1])) return "it is for the superuser tenant " + scope[1] + " (every name it signed would be SUPERUSER here)";
        java.util.List<String> tenants = csv(plugin.getConfig().getStringParam("security_federated_tenants", ""));
        if (!tenants.isEmpty() && !tenants.contains(scope[1])) return "its tenant " + scope[1] + " is not in security_federated_tenants " + tenants;
        return null;
    }

    /** Tenants whose network clients get the cross-tenant SUPERUSER role (broker_superuser_tenants). */
    private java.util.Set<String> superuserTenants() {
        return new java.util.HashSet<>(csv(plugin.getConfig().getStringParam("broker_superuser_tenants", "cresco-system")));
    }

    /**
     * Why an agent may not be bound as {@code tenant/region/agent} at this controller, or null. An agent proves only that it
     * holds the agent discovery secret -- fabric membership, not a name -- so it is bound only inside this controller's own
     * region and tenant, never to a controller name (this node's own, or one bound as a controller), and never in a
     * superuser tenant (those identities are provisioned by an operator, not self-claimed).
     */
    public String agentJoinRefusal(String tenant, String region, String agent) {
        String myRegion = controllerEngine.cstate.getRegion(), myAgent = controllerEngine.cstate.getAgent();
        String myTenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
        if (myRegion == null || !myRegion.equals(region)) return "an agent can only join this controller's region " + myRegion + ", not " + region;
        if (!myTenant.equals(tenant)) return "an agent can only join in this controller's tenant " + myTenant + ", not " + tenant;
        if (superuserTenants().contains(tenant)) return "tenant " + tenant + " is a superuser tenant; agents cannot claim it";
        if (agent.equals(myAgent) || peerTrust.isReserved(region, agent)) return "the name " + region + "/" + agent + " is a controller's";
        return null;
    }

    /**
     * Per-node trust: why the controller presenting {@code chain} in a REGION/GLOBAL discovery exchange (a joiner here, or
     * the responder this node joins) may not be bound as a controller, or null. The region and global discovery secrets
     * prove membership, not a name or a tenant, and the tenant is read from the certificate the peer made for itself: a
     * controller in a superuser tenant ({@code broker_superuser_tenants}: SUPERUSER at every broker that binds it) is never
     * bound first come. The operator lists it by key or certificate in {@code controller_trust_anchors} on this node.
     */
    public String controllerJoinRefusal(X509Certificate[] chain) {
        CrescoIdentity id = chain == null || chain.length == 0 || chain[0] == null ? null : CrescoNames.identityOf(chain[0]);
        if (id == null) return "its certificate names no valid identity";
        if (!superuserTenants().contains(id.getTenant())) return null;
        if (controllerAnchorVouches(chain)) return null;
        return "it claims the superuser tenant " + id.getTenant() + ", and a controller of a superuser tenant is never bound first"
                + " come: list its key or certificate (SHA-256, hex; its key is " + PeerIdentity.spkiFingerprint(chain[0])
                + ") in controller_trust_anchors on this node";
    }

    /**
     * A region or global controller joining over a REGION/GLOBAL discovery exchange is not re-issued: it presents its
     * own region CA chain {@code [leaf, itsRegionCA, root]}, and the two CAs are recorded as each other's peers, each
     * scoped to its own region and tenant (a region CA vouches only for its own region's names). Refused when the
     * joiner's leaf was not issued by its CA, the CA does not name the joiner's region and tenant, the region is this
     * controller's, the tenant is not one this controller federates ({@code security_federated_tenants}, empty = any),
     * or another CA / key is already recorded for that region / controller identity.
     *
     * @return our CA chain {@code [ourCA, ourRoot]} for the joiner to record, or null when refused
     */
    private String federateController(X509Certificate[] joinerCerts, CrescoIdentity claimed, String myRegion, String myAgent) {
        String region = claimed.getRegion(), agent = claimed.getAgent(), tenant = claimed.getTenant();
        if (region.equals(myRegion)) {
            logger.error("Enrollment REFUSED: a joining controller (" + region + "/" + agent + ") asked for this controller's region '" + region + "'");
            return null;
        }
        if (superuserTenants().contains(tenant)) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + " is in the superuser tenant " + tenant
                    + " (its CA would make every name it signs SUPERUSER here)");
            return null;
        }
        java.util.List<String> tenants = csv(plugin.getConfig().getStringParam("security_federated_tenants", ""));
        if (!tenants.isEmpty() && !tenants.contains(tenant)) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + " is tenant " + tenant
                    + ", which is not in security_federated_tenants " + tenants);
            return null;
        }
        if (joinerCerts.length < 2 || joinerCerts[1] == null) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + " presented no region CA"
                    + " (security_regional_ca must be on for every controller of a regional-CA fabric)");
            return null;
        }
        X509Certificate joinerCa = joinerCerts[1];
        String[] scope = PeerTrustRegistry.caScopeOf(joinerCa);
        try {
            joinerCa.checkValidity();
            joinerCerts[0].checkValidity();
            if (joinerCa.getBasicConstraints() < 0) throw new IllegalArgumentException("its CA certificate is not a CA");
            if (!joinerCerts[0].getIssuerX500Principal().equals(joinerCa.getSubjectX500Principal())) throw new IllegalArgumentException("its leaf was not issued by its CA");
            joinerCerts[0].verify(joinerCa.getPublicKey());
            if (joinerCerts.length > 2 && joinerCerts[2] != null) joinerCa.verify(joinerCerts[2].getPublicKey());
        } catch (Exception e) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + ": " + e.getMessage());
            return null;
        }
        if (scope == null || !scope[0].equals(region) || !scope[1].equals(tenant)) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + " presented a CA for "
                    + (scope == null ? "no valid region" : scope[1] + "/" + scope[0]) + ", not its own " + tenant + "/" + region);
            return null;
        }
        PeerTrustRegistry.BindResult free = peerTrust.canBind(tenant, region, agent, joinerCerts[0].getPublicKey());
        if (!free.ok) {
            logger.error("Enrollment REFUSED: controller identity " + region + "/" + agent + " is taken: " + free.reason);
            return null;
        }
        Object caBefore = peerTrust.caState(region);
        String why = peerTrust.trustCa(joinerCa, region, tenant, "federated");
        if (why != null) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + ": " + why);
            return null;
        }
        PeerTrustRegistry.BindResult bound = peerTrust.bind(tenant, region, agent, joinerCerts[0].getPublicKey(), "federated",
                PeerTrustRegistry.ROLE_CONTROLLER, false);
        if (!bound.ok) {
            // the controller's identity is refused (revoked, tombstoned, bound to another key): nothing of this exchange stays
            if (peerTrust.caState(region) != caBefore) peerTrust.restoreCa(region, caBefore);
            logger.error("Enrollment REFUSED: controller identity " + region + "/" + agent + ": " + bound.reason);
            return null;
        }
        replaceTrustStoreEntries("peerCA-" + region, joinerCerts.length > 2 && joinerCerts[2] != null
                ? new X509Certificate[]{joinerCa, joinerCerts[2]} : new X509Certificate[]{joinerCa});
        try { if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
        logger.info("Federated with controller " + region + "/" + agent + " (tenant=" + tenant + "): recorded its region CA, scoped to "
                + region + "; returning this region's CA");
        return getJsonFromCerts(regionCA.caChain());
    }

    /**
     * Install a region-CA-signed identity received at enrollment: keep our private key, swap our cert
     * chain to the region-signed one, trust the issuing CA (which rides inside the chain), and refresh
     * the live broker + client SSL contexts so the new identity and anchor take effect immediately.
     */
    public boolean installEnrollment(String chainJson) {
        return installEnrollment(chainJson, null);
    }

    /**
     * Install what a regional-CA controller returned in a discovery exchange with the controller of {@code peerRegion}:
     * either our identity re-issued by that region's CA {@code [ourLeaf, regionCA, root]} (we joined as an agent), or
     * that controller's own CA chain {@code [itsCA, root]} (we are a controller federating with it). The CA is recorded
     * scoped to the region it names, which must be {@code peerRegion} (when given) -- and for our re-issued identity,
     * the region the leaf names.
     */
    public boolean installEnrollment(String chainJson, String peerRegion) {
        return installEnrollment(chainJson, peerRegion, null);
    }

    /**
     * As {@link #installEnrollment(String, String)} for an exchange of {@code type}: an AGENT exchange must return our own
     * identity re-issued by the region we joined; only a REGION/GLOBAL exchange may return the peer controller's CA. A
     * client PINS its peer's CA at the first join: another CA for that region later is refused unless the operator
     * authorises one rebind of the region on this node (holding a discovery secret does not make anyone that region's CA).
     */
    public boolean installEnrollment(String chainJson, String peerRegion, io.cresco.agent.controller.netdiscovery.DiscoveryType type) {
        if (provisioned != null) {
            logger.error("installEnrollment REFUSED: this node is pre-provisioned; it installs no identity or CA from a peer");
            return false;
        }
        try {
            X509Certificate[] signedChain = getCertsfromJson(chainJson);
            if (signedChain == null || signedChain.length == 0 || signedChain[0] == null) {
                logger.error("installEnrollment: empty enrollment chain");
                return false;
            }
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey == null) {
                logger.error("installEnrollment: no private key for alias " + keyStoreAlias);
                return false;
            }
            java.security.cert.Certificate ourCert = keyStore.getCertificate(keyStoreAlias);
            if (ourCert != null && !ourCert.getPublicKey().equals(signedChain[0].getPublicKey())) {
                if (type != io.cresco.agent.controller.netdiscovery.DiscoveryType.REGION
                        && type != io.cresco.agent.controller.netdiscovery.DiscoveryType.GLOBAL) {
                    logger.error("installEnrollment REFUSED: " + (type == null ? "a legacy" : "an " + type) + " exchange with " + peerRegion
                            + " returned a CA instead of this node's re-issued identity (only a REGION/GLOBAL federation returns a CA)");
                    return false;
                }
                // the peer controller's own CA chain: validated in full BEFORE anything is recorded, then recorded for its
                // region only (never this node's own region, never a superuser or unlisted tenant), keeping our identity
                X509Certificate peerCa = signedChain[0];
                String refusal = federatedCaRefusal(peerCa, peerRegion);
                if (refusal != null) {
                    logger.error("installEnrollment REFUSED: the CA returned by " + peerRegion + ": " + refusal);
                    return false;
                }
                String[] scope = PeerTrustRegistry.caScopeOf(peerCa);
                String why = peerTrust.trustCa(peerCa, scope[0], scope[1], "federated", false);
                if (why != null) {
                    logger.error("installEnrollment REFUSED: " + why);
                    return false;
                }
                replaceTrustStoreEntries("peerCA-" + scope[0], signedChain);
                try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
                try { controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
                logger.info("Federated with region " + scope[0] + " (tenant=" + scope[1] + "): recorded its CA, kept our own identity");
                return true;
            }
            // our identity, issued by the region we joined
            CrescoIdentity mine = CrescoNames.identityOf(signedChain[0]);
            if (mine == null || signedChain.length < 2 || signedChain[1] == null) {
                logger.error("installEnrollment REFUSED: the returned certificate names no valid identity or has no issuing CA");
                return false;
            }
            X509Certificate issuer = signedChain[1];
            String[] scope = PeerTrustRegistry.caScopeOf(issuer);
            try {
                signedChain[0].verify(issuer.getPublicKey());
            } catch (Exception e) {
                logger.error("installEnrollment REFUSED: the returned identity was not signed by the CA that came with it");
                return false;
            }
            if (scope == null || !scope[0].equals(mine.getRegion()) || !scope[1].equals(mine.getTenant())
                    || (peerRegion != null && !peerRegion.equals(mine.getRegion()))) {
                logger.error("installEnrollment REFUSED: identity " + mine + " issued by a CA for " + (scope == null ? "?" : scope[1] + "/" + scope[0])
                        + (peerRegion != null ? " while joining region " + peerRegion : ""));
                return false;
            }
            String myAgent = controllerEngine.cstate.getAgent();
            if (myAgent != null && !myAgent.equals(mine.getAgent())) {
                logger.error("installEnrollment REFUSED: the region issued this node the identity " + mine + ", which is not its name " + myAgent);
                return false;
            }
            if (superuserTenants().contains(mine.getTenant())) {
                logger.error("installEnrollment REFUSED: the region issued this node an identity in the superuser tenant " + mine.getTenant()
                        + " (superuser identities are provisioned by an operator, never issued at a join)");
                return false;
            }
            try { signedChain[0].checkValidity(); issuer.checkValidity(); } catch (Exception e) {
                long now = System.currentTimeMillis();
                boolean early = now < signedChain[0].getNotBefore().getTime();
                if (early) noteClock(signedChain[0].getNotBefore().getTime() + CertValidity.backdateMs(), "the controller of region " + mine.getRegion());
                logger.error("installEnrollment REFUSED: the returned identity or its CA is not valid now: " + e.getMessage()
                        + (early ? " -- this node's clock is BEHIND its region's by more than the certificate backdate"
                        + " (identity_cert_backdate_hours); fix the clocks (NTP)" : ""));
                return false;
            }
            String why = peerTrust.trustCa(issuer, scope[0], scope[1], "joined", false);
            if (why != null) {
                logger.error("installEnrollment REFUSED: " + why);
                return false;
            }
            // a RE-enrollment (same key, same issuing CA key) changes nothing a live connection depends on: the key managers
            // present the current chain at the next handshake and the trust managers read the truststore per handshake, so
            // nothing is torn down. Only a new key or a new issuer re-initialises the connections.
            X509Certificate[] prev = this.chain;
            boolean sameKeyAndIssuer = prev != null && prev.length > 1 && prev[0] != null && prev[1] != null
                    && prev[0].getPublicKey().equals(signedChain[0].getPublicKey())
                    && prev[1].getPublicKey().equals(issuer.getPublicKey());
            this.chain = signedChain;
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, signedChain);
            saveNodeIdentity();
            boolean anchorsChanged = replaceTrustStoreEntries("issuingCA-" + scope[0], signedChain.length > 2 && signedChain[2] != null
                    ? new X509Certificate[]{ issuer, signedChain[2] } : new X509Certificate[]{ issuer });
            if (anchorsChanged || !sameKeyAndIssuer) {
                try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            }
            if (!sameKeyAndIssuer) {
                try { controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
                logger.info("Installed region-CA-signed identity: " + mine);
            } else {
                logger.info("Re-enrolled: " + mine + " " + validityText(signedChain) + " (same key and CA; live connections kept; truststore "
                        + trustStoreSize() + " entries)");
            }
            reEnrollFailures = 0;
            setIdentityAlarm(null, false);
            return true;
        } catch (Exception ex) {
            logger.error("installEnrollment error: " + ex.getMessage(), ex);
            return false;
        }
    }

    public void saveKeyAndTrustStore() {
        if (keyStoreFilePath == null || trustStoreFilePath == null || keyStoreFilePath.equals(trustStoreFilePath) || certificateSaveFailureEncountered)
            return;
        logger.debug("saveKeyAndTrustStore({},{})", keyStoreFilePath, trustStoreFilePath);
        Path keyStoreFilePathObj = Paths.get(keyStoreFilePath);
        try {
            Files.createDirectories(keyStoreFilePathObj.getParent());
        } catch (IOException e) {
            logger.warn("Failed to create key store parent directory: {}, retaining certificates only in memory",
                    keyStoreFilePathObj.getParent());
            certificateSaveFailureEncountered = true;
            return;
        }
        Path trustStoreFilePathObj = Paths.get(trustStoreFilePath);
        try {
            Files.createDirectories(trustStoreFilePathObj.getParent());
        } catch (IOException e) {
            logger.warn("Failed to create trust store parent directory: {}, retaining certificates only in memory",
                    trustStoreFilePathObj.getParent());
            certificateSaveFailureEncountered = true;
            return;
        }
        try (FileOutputStream keyStoreOut = new FileOutputStream(keyStoreFilePath);
             FileOutputStream trustStoreOut = new FileOutputStream(trustStoreFilePath)) {
            keyStore.store(keyStoreOut, keyStorePassword);
            trustStore.store(trustStoreOut, trustStorePassword);
        } catch (IOException e) {
            logger.error("CertificateManager.saveKeyAndTrustStore IOException", e);
        } catch (CertificateException e) {
            logger.error("CertificateManager.saveKeyAndTrustStore CertificateException", e);
        } catch (NoSuchAlgorithmException e) {
            logger.error("CertificateManager.saveKeyAndTrustStore NoSuchAlgorithmException", e);
        } catch (KeyStoreException e) {
            logger.error("CertificateManager.saveKeyAndTrustStore KeyStoreException", e);
        }
    }

    public boolean loadKeyAndTrustStore() {
        if (keyStoreFilePath == null || trustStoreFilePath == null)
            return false;
        logger.debug("loadKeyAndTrustStore({},{})", keyStoreFilePath, trustStoreFilePath);
        try (FileInputStream keyStoreIn = new FileInputStream(keyStoreFilePath);
             FileInputStream trustStoreIn = new FileInputStream(trustStoreFilePath)) {
            logger.trace("Generating blank key store object");
            keyStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
            logger.trace("Loading existing key store: {}", Paths.get(keyStoreFilePath).toAbsolutePath());
            keyStore.load(keyStoreIn, keyStorePassword);
            if (keyStore == null) {
                logger.warn("Failed to load existing key store file with provided password");
                return false;
            }
            logger.trace("Checking for alias [{}] in key store", keyStoreAlias);
            if (!keyStore.containsAlias(keyStoreAlias)) {
                logger.warn("Alias [{}] does not appear in key store, load failed", keyStoreAlias);
                return false;
            }
            logger.trace("Generating blank trust store object");
            trustStore = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
            logger.trace("Loading existing trust store: {}", Paths.get(trustStoreFilePath).toAbsolutePath());
            trustStore.load(trustStoreIn, trustStorePassword);
            Certificate[] keyStoreCertChain = keyStore.getCertificateChain(keyStoreAlias);
            if (keyStoreCertChain == null) {
                logger.warn("Certificate chain for alias [{}] does not appear in key store, load failed",
                        keyStoreAlias);
                return false;
            }
            logger.trace("Loading [{}] certificates from key store alias [{}]", keyStoreCertChain.length,
                    keyStoreAlias);
            chain = new X509Certificate[keyStoreCertChain.length];
            for (int i = 0; i < keyStoreCertChain.length; i++)
                chain[i] = (X509Certificate) keyStoreCertChain[i];
            return true;
        } catch (IOException e) {
            logger.error("CertificateManager.loadKeyAndTrustStore IOException", e);
            return false;
        } catch (CertificateException e) {
            logger.error("CertificateManager.loadKeyAndTrustStore CertificateException", e);
            return false;
        } catch (NoSuchAlgorithmException e) {
            logger.error("CertificateManager.loadKeyAndTrustStore NoSuchAlgorithmException", e);
            return false;
        } catch (KeyStoreException e) {
            logger.error("CertificateManager.loadKeyAndTrustStore KeyStoreException", e);
            return false;
        } catch (Exception e) {
            logger.error("CertificateManager.loadKeyAndTrustStore Exception", e);
            return false;
        }
    }

    //unused

    public void updateSSL(X509Certificate[] cert, String alias) {


        try {
            //broker.getSslContext().addTrustManager();

            TrustManager[] trustmanagers = getTrustManagers();
            if (trustmanagers != null) {
                for (TrustManager tm : trustmanagers) {
                    if (tm instanceof X509TrustManager) {
                        //((X509TrustManager) tm).checkClientTrusted(cert, alias);
                        //((X509TrustManager) tm).checkServerTrusted(cert, alias);

                        ((X509TrustManager) tm).checkClientTrusted(cert, "RSA");
                        ((X509TrustManager) tm).checkServerTrusted(cert, "RSA");

                        //((X509TrustManager) tm).checkClientTrusted(null, null);
                        //((X509TrustManager) tm).checkServerTrusted(null, null);

                        //trustmanagers[i] = new TrustManagerDecorator(
                        //        (X509TrustManager) tm, trustStrategy);
                    }
                }
            }

        } catch(Exception ex) {
            logger.error("updateSSL error " + ex.getMessage());
        }
    }

    public void addCertificatesToKeyStore(String alias, Certificate[] certs) {
        try {
            for(Certificate cert:certs){
                //logger.error(cert.toString());
                //PublicKey publicKey = cert.getPublicKey();

                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                //trustStore.setCertificateEntry(UUID.randomUUID().toString(),cert);
                keyStore.setCertificateEntry(alias,cert);

                //logger.error(publicKeyString);
            }
        } catch (Exception ex) {
            logger.error("addCertificatesToTrustStore() : error " + ex.getMessage());
        }
    }

    /*
    private void generateCertChain2() {
        try{


            //Generate ROOT certificate
            CertAndKeyGen keyGen=new CertAndKeyGen("RSA","SHA256WithRSA",null);
            keyGen.generate(keySize);
            PrivateKey rootPrivateKey=keyGen.getPrivateKey();

            X509Certificate rootCertificate = keyGen.getSelfCertificate(new X500Name("CN=ROOT-" + keyStoreAlias), (long) (365 * 24 * 60 * 60) * YEARS_VALID);


            rootCertificate   = createSignedCertificate(rootCertificate,rootCertificate,rootPrivateKey);

            chain = new X509Certificate[1];
            chain[0]=rootCertificate;

            //String alias = "mykey";
            //String keystore = "testkeys.jks";

            //Store the certificate chain
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, rootPrivateKey, chain);

            //Reload the keystore and display key and certificate chain info
            //loadCertChain(alias, keyStorePassword, keystore);
            //Clear the keystore
            //clearKeyStore(alias, password, keystore);
            signingCertificate = rootCertificate;
            signingKey = rootPrivateKey;

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
*/
    public TrustManager getTrustManager() {
        TrustManager trustManager = null;
        try {
            trustManager = getTrustManagers()[0];
            if(trustManager == null) {
                logger.error("TRUST MANAGER NULL!!!");
            }
        } catch(Exception ex) {
            logger.error("getTrustManager Error : " + ex.getMessage());
        }
        return trustManager;
    }

    public KeyManager getKeyManager() {
        KeyManager keyManager = null;
        try {
            keyManager = getKeyManagers()[0];
        } catch(Exception ex) {
            logger.error("getKeyManager Error : " + ex.getMessage());
        }
        return keyManager;
    }

    public Certificate[] getPublicCertificates() {
        Certificate[] certs = null;
        try {
            Key key = keyStore.getKey(keyStoreAlias, keyStorePassword);

            if (key instanceof PrivateKey) {

                certs = keyStore.getCertificateChain(keyStoreAlias);

            }
        } catch(Exception ex) {
            logger.error("getCertificates() : error " + ex.getMessage());
        }
        return certs;
    }

    public Certificate[] getCertificates() {
        Certificate[] certs = null;
        try {
            Key key = keyStore.getKey(keyStoreAlias, keyStorePassword);

            if (key instanceof PrivateKey) {

                certs = keyStore.getCertificateChain(keyStoreAlias);

            }
        } catch(Exception ex) {
            logger.error("getCertificates() : error " + ex.getMessage());
        }
        return certs;
    }

    public void loadTrustStoreCertChain(String alias) throws Exception{

        Key key=trustStore.getKey(alias, keyStorePassword);

        if(key instanceof PrivateKey){
            //logger.error("Get private key : ");
            logger.error(key.toString());

            Certificate[] certs=trustStore.getCertificateChain(alias);
            logger.error("Certificate chain length : "+certs.length);
            for(Certificate cert:certs){
                logger.error(cert.toString());
                PublicKey publicKey = cert.getPublicKey();

                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
                //trustStore.setCertificateEntry(cert.toString(),cert);
                //keyStore.setCertificateEntry(cert.toString(),cert);

                logger.error(publicKeyString);
            }
        }else{

            logger.error("Key is not private key");
            Certificate cert = trustStore.getCertificate(alias);
            if(cert != null) {


                logger.error(cert.toString());
                PublicKey publicKey = cert.getPublicKey();

                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
                //trustStore.setCertificateEntry(cert.toString(),cert);
                //keyStore.setCertificateEntry(cert.toString(),cert);

                logger.error(publicKeyString);
            } else {
                logger.error("cert null");
            }

        }
    }

    public void loadKeyStoreCertChain(String alias) throws Exception{

        Key key=keyStore.getKey(alias, keyStorePassword);

        if(key instanceof PrivateKey){
            //logger.error("Get private key : ");
            logger.error(key.toString());

            Certificate[] certs=keyStore.getCertificateChain(alias);
            logger.error("Certificate chain length : "+certs.length);
            for(Certificate cert:certs){
                logger.error(cert.toString());
                PublicKey publicKey = cert.getPublicKey();
                String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                //trustStore.setCertificateEntry(cert.toString(),cert);
                //keyStore.setCertificateEntry(cert.toString(),cert);

                logger.error(publicKeyString);
            }
        }else{

            logger.error("Key is not private key");
            Certificate cert = keyStore.getCertificate(alias);
            if(cert != null) {
                logger.error(cert.toString());
                PublicKey publicKey = cert.getPublicKey();
                String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                //trustStore.setCertificateEntry(cert.toString(),cert);
                //keyStore.setCertificateEntry(cert.toString(),cert);

                logger.error(publicKeyString);
            } else {
                logger.error("cert null");
            }

        }
    }

    private void loadAndDisplayChain(String alias,char[] password, String keystore) throws Exception{
        //Reload the keystore
        KeyStore keyStore=KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        keyStore.load(new FileInputStream(keystore),password);

        Key key=keyStore.getKey(alias, password);

        if(key instanceof PrivateKey){
            logger.error("Get private key : ");
            logger.error(key.toString());

            Certificate[] certs=keyStore.getCertificateChain(alias);
            logger.error("Certificate chain length : "+certs.length);
            for(Certificate cert:certs){
                logger.error(cert.toString());
                PublicKey publicKey = cert.getPublicKey();
                String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
                //String publicKeyString = Base64.encodeBase64String(publicKey.getEncoded());
                trustStore.setCertificateEntry(cert.toString(),cert);
                logger.error(publicKeyString);
            }
        }else{
            logger.error("Key is not private key");
        }
    }

    private  void clearKeyStore(String alias,char[] password, String keystore) throws Exception{
        KeyStore keyStore=KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        keyStore.load(new FileInputStream(keystore),password);
        keyStore.deleteEntry(alias);
        keyStore.store(new FileOutputStream(keystore),password);
    }

    /*
    private X509Certificate createSignedCertificate(X509Certificate cetrificate,X509Certificate issuerCertificate,PrivateKey issuerPrivateKey){
        try{
            Principal issuer = issuerCertificate.getSubjectDN();
            String issuerSigAlg = issuerCertificate.getSigAlgName();

            byte[] inCertBytes = cetrificate.getTBSCertificate();
            X509CertInfo info = new X509CertInfo(inCertBytes);

            //various versions of java work diff
            try{
                //info.set( X509CertInfo.SUBJECT, new CertificateSubjectName( owner ) );
                info.set( X509CertInfo.ISSUER, new CertificateIssuerName( (X500Name) issuer ) );
            } catch(Exception e){
                //info.set( X509CertInfo.SUBJECT, owner );
                info.set( X509CertInfo.ISSUER, issuer );
            }


            //info.set(X509CertInfo.ISSUER, new CertificateIssuerName((X500Name) issuer));

            //No need to add the BasicContraint for leaf cert
            if(!cetrificate.getSubjectDN().getName().equals("CN=TOP")){
                CertificateExtensions exts=new CertificateExtensions();
                BasicConstraintsExtension bce = new BasicConstraintsExtension(true, -1);
                exts.set(BasicConstraintsExtension.NAME,new BasicConstraintsExtension(false, bce.getExtensionValue()));
                info.set(X509CertInfo.EXTENSIONS, exts);
            }

            X509CertImpl outCert = new X509CertImpl(info);
            outCert.sign(issuerPrivateKey, issuerSigAlg);

            return outCert;
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

 */

      /*
    private void generateCertChain() {
        try{

            //Generate ROOT certificate
            CertAndKeyGen keyGen=new CertAndKeyGen("RSA","SHA256WithRSA",null);
            keyGen.generate(keySize);
            PrivateKey rootPrivateKey=keyGen.getPrivateKey();

            KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
            // DRBG (SP 800-90A) via the crypto seam, was SHA1PRNG; CNSA RSA floor, was hardcoded 2048.
            gen.initialize(io.cresco.library.crypto.CrescoCrypto.MIN_RSA_KEY_BITS,
                    io.cresco.library.crypto.CrescoCrypto.secureRandom());
            KeyPair keyPair = gen.generateKeyPair();

            //X509Certificate rootCertificate = keyGen.getSelfCertificate(new X500Name("CN=ROOT"), (long) (365 * 24 * 60 * 60) * YEARS_VALID);
            X509Certificate rootCertificate = keyGen.getSelfCertificate(new X500Name("CN=ROOT, OU=myTeam, O=MyOrg, L=Lexington, ST=KY, C=US"), (long) (365 * 24 * 60 * 60) * YEARS_VALID);

            //Generate intermediate certificate
            CertAndKeyGen keyGen1=new CertAndKeyGen("RSA","SHA256WithRSA",null);
            keyGen1.generate(keySize);
            PrivateKey middlePrivateKey=keyGen1.getPrivateKey();

            X509Certificate middleCertificate = keyGen1.getSelfCertificate(new X500Name("CN=MIDDLE, OU=myTeam, O=MyOrg, L=Lexington, ST=KY, C=US"), (long) (365 * 24 * 60 * 60) * YEARS_VALID);

            //Generate leaf certificate
            CertAndKeyGen keyGen2=new CertAndKeyGen("RSA","SHA256WithRSA",null);
            keyGen2.generate(keySize);
            PrivateKey topPrivateKey=keyGen2.getPrivateKey();

            X509Certificate topCertificate = keyGen2.getSelfCertificate(new X500Name("CN=TOP, OU=myTeam, O=MyOrg, L=Lexington, ST=KY, C=US"), (long) (365 * 24 * 60 * 60) * YEARS_VALID);


            rootCertificate   = createSignedCertificate(rootCertificate,rootCertificate,rootPrivateKey);
            middleCertificate = createSignedCertificate(middleCertificate,rootCertificate,rootPrivateKey);
            topCertificate    = createSignedCertificate(topCertificate,middleCertificate,middlePrivateKey);

            chain = new X509Certificate[3];
            chain[0]=topCertificate;
            chain[1]=middleCertificate;
            chain[2]=rootCertificate;

            //String alias = "mykey";
            //String keystore = "testkeys.jks";

            //Store the certificate chain
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, topPrivateKey, chain);

            //Reload the keystore and display key and certificate chain info
            //loadCertChain(alias, keyStorePassword, keystore);
            //Clear the keystore
            //clearKeyStore(alias, password, keystore);
            signingCertificate = middleCertificate;
            signingKey = middlePrivateKey;

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
*/

      /*
    public X509Certificate[] getCertificate(String alias) {
        X509Certificate[] certChain = null;
        try {
            //Generate leaf certificate
            CertAndKeyGen keyGen2=new CertAndKeyGen("RSA","SHA256WithRSA",null);
            keyGen2.generate(keySize);
            PrivateKey topPrivateKey=keyGen2.getPrivateKey();
            X509Certificate topCertificate = keyGen2.getSelfCertificate(new X500Name("CN=TOP"), (long) (365 * 24 * 60 * 60) * YEARS_VALID);
            Certificate cert = createSignedCertificate(topCertificate,signingCertificate,signingKey);
            //keyStore.setCertificateEntry(alias,cert);
            //trustStore.setCertificateEntry(alias,cert);



            certChain = chain.clone();
            certChain[0] = (X509Certificate)cert;

            storeKeyAndCertificateChain(alias, keyStorePassword, topPrivateKey, certChain);


        } catch(Exception ex) {
            logger.error("getCertificate : error " + ex.getMessage());
        }

        return  certChain;
    }

     */


}
