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
    private KeyStore trustStore;
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
        try {

            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
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
                    addCertificatesToTrustStore(keyStoreAlias, getPublicCertificate());
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
                addCertificatesToTrustStore(keyStoreAlias, getPublicCertificate());
            }
            // CAs recorded for regions before a restart stay trusted for TLS (their scope is in the registry)
            for (X509Certificate ca : peerTrust.recordedCas()) {
                trustStore.setCertificateEntry("recordedCA-" + PeerIdentity.spkiFingerprint(ca).substring(0, 16), ca);
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
            return new PeerTrustRegistry().withRebind(csv(plugin.getConfig().getStringParam("peer_identity_rebind", "")));
        }
        String f = plugin.getConfig().getStringParam("peer_trust_file");
        Path file = (f == null || f.isBlank()) ? identityDir().resolve("peer-trust.json") : Paths.get(f);
        return PeerTrustRegistry.open(file,
                csv(plugin.getConfig().getStringParam("peer_identity_rebind", "")),
                csv(plugin.getConfig().getStringParam("peer_bridge_revoke", "")),
                plugin.getConfig().getIntegerParam("peer_bridge_prune_days", 30),
                m -> logger.error(m));
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
            Files.createDirectories(pass.getParent());
            Path tmp = pass.resolveSibling(pass.getFileName() + ".tmp");
            Files.writeString(tmp, java.util.HexFormat.of().formatHex(r));
            try { Files.setPosixFilePermissions(tmp, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); } catch (Exception ignore) { }
            Files.move(tmp, pass, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        return Files.readString(pass).trim().toCharArray();
    }

    private void writeStore(Path store, String alias, PrivateKey key, X509Certificate[] certs) throws Exception {
        char[] pw = storePassword(store);
        KeyStore ks = KeyStore.getInstance(io.cresco.library.crypto.CrescoCrypto.KEYSTORE_TYPE);
        ks.load(null, null);
        ks.setKeyEntry(alias, key, pw, certs);
        Path tmp = store.resolveSibling(store.getFileName() + ".tmp");
        try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
            ks.store(out, pw);
            out.getFD().sync();
        }
        try { Files.setPosixFilePermissions(tmp, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); } catch (Exception ignore) { }
        Files.move(tmp, store, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private Object[] readStore(Path store, String alias) throws Exception {
        if (!Files.exists(store)) return null;
        char[] pw = storePassword(store);
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
            if (have != null && have.getRegion().equals(region) && have.getAgent().equals(agent)) {
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

    private void exportPublicKey() {
        try {
            String publicKeyDirectoryPath = plugin.getConfig().getStringParam("public_key_directory", "cresco-data/public-keys");
            File publicKeyDirectory = new File(publicKeyDirectoryPath);
            if (!publicKeyDirectory.exists()) {
                publicKeyDirectory.mkdirs();
            }
            String certFileName = controllerEngine.cstate.getAgentPath() + ".cer";
            File certFile = new File(publicKeyDirectory, certFileName);
            try (FileOutputStream fos = new FileOutputStream(certFile)) {
                fos.write(getPublicCertificate()[0].getEncoded());
            }
        } catch (Exception ex) {
            logger.error("Failed to export public key: " + ex.getMessage());
        }
    }

    private void importPublicKeysFromDirectory() {
        try {
            String publicKeyDirectoryPath = plugin.getConfig().getStringParam("public_key_directory", "cresco-data/public-keys");
            File publicKeyDirectory = new File(publicKeyDirectoryPath);
            if (publicKeyDirectory.exists() && publicKeyDirectory.isDirectory()) {
                File[] certFiles = publicKeyDirectory.listFiles();
                if (certFiles != null) {
                    for (File certFile : certFiles) {
                        if (certFile.isFile()) {
                            try {
                                String alias = certFile.getName().replace(".cer", "");
                                if (!trustStore.containsAlias(alias)) {
                                    try (FileInputStream fis = new FileInputStream(certFile)) {
                                        CertificateFactory cf = CertificateFactory.getInstance("X.509");
                                        Certificate cert = cf.generateCertificate(fis);
                                        trustStore.setCertificateEntry(alias, cert);
                                        logger.info("Imported public key: " + alias);
                                    }
                                } else {
                                    logger.debug("Certificate with alias '" + alias + "' already exists in the truststore. Skipping.");
                                }
                            } catch (Exception e) {
                                logger.error("Failed to import certificate " + certFile.getName() + ": " + e.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("Error importing public keys: " + ex.getMessage());
        }
    }

    public void addCertificatesToTrustStore(String alias, Certificate[] certs) {
        try {
            for(Certificate cert:certs){

                if (cert instanceof X509Certificate) {
                    X509Certificate x509cert = (X509Certificate) cert;

                    // Get subject
                    Principal principal = x509cert.getSubjectDN();
                    //set alias to subject name to prevent overwriting in the cert chain
                    alias = principal.getName();
                }

                logger.debug("addCertificatesToTrustStore: ADDING ALIAS: " + alias + " to truststore");
                trustStore.setCertificateEntry(alias,cert);

            }
            saveKeyAndTrustStore();
        } catch (Exception ex) {
            logger.error("addCertificatesToTrustStore() : error " + ex.getMessage());
        }
    }

    public X509Certificate[] getPublicCertificate() {
        //X509Certificate[] certs = new X509Certificate[1];
        //certs[0] = chain[0];
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
                    BigInteger.valueOf(new Random().nextInt()), //serial number of certificate
                    DateTime.now().toDate(), // start of validity
                    new DateTime().plusYears(YEARS_VALID).toDate(),
                    //new DateTime(2025, 12, 31, 0, 0, 0, 0).toDate(), //end of certificate validity
                    new X500Name("CN=rootCA-" + agentName), // subject name of certificate
                    rootCAKeyPair.getPublic()); // public key of certificate
            // key usage restrictions
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
            // every signature suite below is the crypto policy's choice (SHA384withRSA under the default cnsa1);
            // certificates are made before the node holds the fabric policy, so CrescoCrypto uses the effective one
            X509Certificate rootCA = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).setProvider("BC").
                            build(rootCAKeyPair.getPrivate()))); // private key of signing authority , here it is self signed


            //create Intermediate CA cert signed by Root CA
            KeyPair intermedCAKeyPair = generateKeyPair();
            builder = new JcaX509v3CertificateBuilder(
                    rootCA, // here rootCA is issuer authority
                    BigInteger.valueOf(new Random().nextInt()), DateTime.now().toDate(),
                    new DateTime().plusYears(YEARS_VALID).toDate(),
                    new X500Name("CN=IntermedCA-" + pluginName), intermedCAKeyPair.getPublic());
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
            X509Certificate intermedCA = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).setProvider("BC").
                            build(rootCAKeyPair.getPrivate())));// private key of signing authority , here it is signed by rootCA

            //create end user cert signed by Intermediate CA
            KeyPair endUserCertKeyPair = (leafKeyPair != null) ? leafKeyPair : generateKeyPair();
            // Identity-bearing leaf: bind tenant/region/agent into the subject DN so the broker (and
            // any peer) can cryptographically identify this node from its certificate rather than a
            // spoofable connection username. Falls back to the legacy UUID CN before identity is known.
            String leafDN;
            String idTenant = plugin.getConfig().getStringParam("tenant_id", "default");
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
                    BigInteger.valueOf(new Random().nextInt()), DateTime.now().toDate(),
                    new DateTime().plusYears(YEARS_VALID).toDate(),
                    leafName, endUserCertKeyPair.getPublic());
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
            X509Certificate endUserCert = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm("RSA")).setProvider("BC").
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
        KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA", "BC");
        kpGen.initialize(keySize, new SecureRandom());
        return kpGen.generateKeyPair();
    }

    public TrustManager[] getTrustManagers() {
        TrustManager[] trustManagers = null;
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            trustManagers = trustManagerFactory.getTrustManagers();

        } catch(Exception ex) {
            logger.error("getTrustManagers Error : " + ex.getMessage());
        }
        return trustManagers;
    }

    /** The node's current PKIX trust manager over its truststore (built per call, so it sees every addition). */
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
        return new TrustManager[]{ new PeerIdentityTrustManager(this::currentPkixTrustManager, expected, peerTrust, mode, logger, endpoint) };
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
        try {
            if (regionCA != null) return false;                        // a CA holder's identity is its own CA's to stamp
            String region = controllerEngine.cstate.getRegion();
            String agent = controllerEngine.cstate.getAgent();
            String tenant = plugin.getConfig().getStringParam("tenant_id", "default");
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
            addCertificatesToTrustStore(keyStoreAlias, getPublicCertificate());
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

    public KeyManager[] getKeyManagers() {
        KeyManager[] keyManagers = null;
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keyStorePassword);
            keyManagers = keyManagerFactory.getKeyManagers();

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
            logger.error("getCertsfromString : error " + ex.getMessage(), ex);
        }
        return cert;
    }

    public X509Certificate[] getCertsfromJson(String jsonCerts) {
        X509Certificate[] certs = null;
        try {

            Gson gc = new Gson();
            String[] certImportList = gc.fromJson(jsonCerts,String[].class);
            certs = new X509Certificate[3];
            for(int i = 0; i<certImportList.length; i++) {
                certs[i] = getCertfromString(certImportList[i]);
            }

        } catch(Exception ex) {
            logger.error("getCertsfromJson : error " + ex.getMessage(), ex);
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
        try {
            String tenant = plugin.getConfig().getStringParam("tenant_id", "default");
            String region = controllerEngine.cstate.getRegion();
            String agent = controllerEngine.cstate.getAgent();
            regionCA = loadRegionCA(tenant, region);
            if (regionCA == null) {
                regionCA = RegionCA.generate(tenant, region, keySize, YEARS_VALID);
                saveRegionCA();
            }
            addCertificatesToTrustStore("localRegionCA-" + region, new X509Certificate[]{ regionCA.caCert() });
            String scope = peerTrust.trustCa(regionCA.caCert(), region, tenant, "own");
            if (scope != null) {
                logger.error("This node's region CA is not the one recorded for region " + region + ": " + scope);
            }
            // self-enroll: replace our self-signed leaf with one signed by our own region CA (same key)
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey != null && getPublicCertificate() != null && getPublicCertificate()[0] != null) {
                PeerTrustRegistry.BindResult own = peerTrust.bind(tenant, region, agent, getPublicCertificate()[0].getPublicKey(), "own");
                if (!own.ok) logger.error("This controller's own identity " + region + "/" + agent + " is bound to another key: " + own.reason);
                X509Certificate[] selfChain = regionCA.enroll(getPublicCertificate()[0].getPublicKey(), tenant, region, agent, YEARS_VALID);
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

    private void saveRegionCA() {
        if (!plugin.getConfig().getBooleanParam("node_identity_persist", true)) return;
        try {
            writeStore(regionCaStore(), "region-ca", regionCA.caPrivateKey(), regionCA.caChain());
        } catch (Exception ex) {
            logger.error("Could not save the region CA (a restart will make a new one, which peers that recorded this CA refuse): " + ex.getMessage());
        }
    }

    private RegionCA loadRegionCA(String tenant, String region) {
        if (!plugin.getConfig().getBooleanParam("node_identity_persist", true)) return null;
        try {
            Object[] kc = readStore(regionCaStore(), "region-ca");
            if (kc == null) return null;
            X509Certificate[] c = (X509Certificate[]) kc[1];
            String[] scope = PeerTrustRegistry.caScopeOf(c[0]);
            if (scope == null || !scope[0].equals(region) || !scope[1].equals(tenant) || c.length < 2) {
                logger.error("The persisted region CA is for " + (scope == null ? "?" : scope[0] + "/" + scope[1]) + ", not " + region + "/" + tenant + "; making a new one");
                return null;
            }
            logger.info("Loaded the persisted region CA for region " + region);
            return RegionCA.restore(c[1], new KeyPair(c[0].getPublicKey(), (PrivateKey) kc[0]), c[0]);
        } catch (Exception ex) {
            logger.error("Persisted region CA unreadable (" + ex.getMessage() + "); making a new one");
            return null;
        }
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
        try {
            ensureRegionCA();
            if (regionCA == null) {
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
            String myTenant = plugin.getConfig().getStringParam("tenant_id", "default");
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
            // one key per name: a second key for a name this CA already issued is refused unless the operator rebinds it
            java.security.PublicKey joinerKey = joinerCerts[0].getPublicKey();
            PeerTrustRegistry.BindResult free = peerTrust.canBind(tenant, region, agent, joinerKey);
            if (!free.ok) {
                logger.error("Enrollment REFUSED: identity " + region + "/" + agent + " is taken: " + free.reason);
                return null;
            }
            X509Certificate[] signed = regionCA.enroll(joinerKey, tenant, region, agent, YEARS_VALID);
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
        String why = peerTrust.trustCa(joinerCa, region, tenant, "federated");
        if (why != null) {
            logger.error("Enrollment REFUSED: joining controller " + region + "/" + agent + ": " + why);
            return null;
        }
        peerTrust.bind(tenant, region, agent, joinerCerts[0].getPublicKey(), "federated");
        addCertificatesToTrustStore("peerCA-" + region, joinerCerts.length > 2 && joinerCerts[2] != null
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
                // the peer controller's own CA chain: record it for its region, keep our identity
                X509Certificate peerCa = signedChain[0];
                String[] scope = PeerTrustRegistry.caScopeOf(peerCa);
                if (scope == null || peerCa.getBasicConstraints() < 0 || (peerRegion != null && !peerRegion.equals(scope[0]))) {
                    logger.error("installEnrollment REFUSED: the CA returned by " + peerRegion + " names region "
                            + (scope == null ? "?" : scope[0]) + " (or is not a CA)");
                    return false;
                }
                String why = peerTrust.trustCa(peerCa, scope[0], scope[1], "federated");
                if (why != null) {
                    logger.error("installEnrollment REFUSED: " + why);
                    return false;
                }
                addCertificatesToTrustStore("peerCA-" + scope[0], signedChain);
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
            String why = peerTrust.trustCa(issuer, scope[0], scope[1], "joined");
            if (why != null) {
                logger.error("installEnrollment REFUSED: " + why);
                return false;
            }
            this.chain = signedChain;
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, signedChain);
            saveNodeIdentity();
            addCertificatesToTrustStore("issuingCA-" + scope[0], signedChain.length > 2 && signedChain[2] != null
                    ? new X509Certificate[]{ issuer, signedChain[2] } : new X509Certificate[]{ issuer });
            try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            try { controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
            logger.info("Installed region-CA-signed identity: " + mine);
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
