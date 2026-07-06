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

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;

    private int keySize = 2048;
    private boolean certificateSaveFailureEncountered = false;

    public CertificateManager(ControllerEngine controllerEngine) {
        long startTime = System.currentTimeMillis();

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(CertificateManager.class.getName(), CLogger.Level.Info);
        try {

            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            keySize = plugin.getConfig().getIntegerParam("messagekeysize",2048);
            if (keySize < 512) {
                logger.warn("Message key sizes (messagekeysize) of <512 are not currently supported, using 512 bits");
                keySize = 512;
            }

            this.keyStoreAlias = this.controllerEngine.cstate.getAgentPath();

            String keyStorePasswordStr = plugin.getConfig().getStringParam("keystorepwd");
            keyStoreFilePath = plugin.getConfig().getStringParam("keystorefile");
            String trustStorePasswordStr = plugin.getConfig().getStringParam("truststorepwd");
            trustStoreFilePath = plugin.getConfig().getStringParam("truststorefile");
            if (keyStorePasswordStr != null && keyStoreFilePath != null &&
                    trustStorePasswordStr != null && trustStoreFilePath != null &&
                    !keyStoreFilePath.equals(trustStoreFilePath)) {
                logger.debug("keyStorePasswordStr: {}", keyStorePasswordStr);
                logger.debug("keyStoreFilePath: {}", keyStoreFilePath);
                logger.debug("trustStorePasswordStr: {}", trustStorePasswordStr);
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
                    keyStore = KeyStore.getInstance("jks");
                    keyStore.load(null, null);


                    trustStore = KeyStore.getInstance("jks");
                    trustStore.load(null, null);

                    generateCertChain();

                    //trust self
                    addCertificatesToTrustStore(keyStoreAlias, getPublicCertificate());
                }
            } else {
                //keyStoreAlias = UUID.randomUUID().toString();
                keyStorePassword = UUID.randomUUID().toString().toCharArray();

                keyStore = KeyStore.getInstance("jks");
                keyStore.load(null, null);


                trustStore = KeyStore.getInstance("jks");
                trustStore.load(null, null);

                generateCertChain();

                //trust self
                addCertificatesToTrustStore(keyStoreAlias, getPublicCertificate());
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
            X509Certificate rootCA = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").
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
                    .build(new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").
                            build(rootCAKeyPair.getPrivate())));// private key of signing authority , here it is signed by rootCA

            //create end user cert signed by Intermediate CA
            KeyPair endUserCertKeyPair = generateKeyPair();
            // Identity-bearing leaf: bind tenant/region/agent into the subject DN so the broker (and
            // any peer) can cryptographically identify this node from its certificate rather than a
            // spoofable connection username. Falls back to the legacy UUID CN before identity is known.
            String leafDN;
            String idTenant = plugin.getConfig().getStringParam("tenant_id", "default");
            String idRegion = controllerEngine.cstate.getRegion();
            String idAgent = controllerEngine.cstate.getAgent();
            if (idRegion != null && idAgent != null) {
                leafDN = io.cresco.library.security.CrescoIdentity.of(idTenant, idRegion, idAgent, null).toX500Name();
                logger.info("Issuing identity-bearing leaf certificate: " + leafDN);
            } else {
                leafDN = "CN=endUserCert-" + functionName;
            }
            builder = new JcaX509v3CertificateBuilder(
                    intermedCA, //here intermedCA is issuer authority
                    BigInteger.valueOf(new Random().nextInt()), DateTime.now().toDate(),
                    new DateTime().plusYears(YEARS_VALID).toDate(),
                    new X500Name(leafDN), endUserCertKeyPair.getPublic());
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
            builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
            X509Certificate endUserCert = new JcaX509CertificateConverter().getCertificate(builder
                    .build(new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").
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


        //KeyStore keyStore=KeyStore.getInstance("jks");
        //keyStore=KeyStore.getInstance("jks");
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
            regionCA = RegionCA.generate(tenant, region, keySize, YEARS_VALID);
            addCertificatesToTrustStore("localRegionCA-" + region, new X509Certificate[]{ regionCA.caCert() });
            // self-enroll: replace our self-signed leaf with one signed by our own region CA
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey != null && getPublicCertificate() != null && getPublicCertificate()[0] != null) {
                X509Certificate[] selfChain = regionCA.enroll(getPublicCertificate()[0].getPublicKey(), tenant, region, agent, YEARS_VALID);
                this.chain = selfChain;
                storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, selfChain);
            }
            try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            logger.info("Region CA established for region '" + region + "' (tenant=" + tenant + "); own identity re-issued under it");
        } catch (Exception ex) {
            logger.error("ensureRegionCA error: " + ex.getMessage(), ex);
        }
    }

    /**
     * Issue a region-CA-signed leaf for an enrolling node. The joiner presents its self-signed cert
     * (identity-bearing DN + public key); we take ONLY its public key and STAMP the identity from the
     * authenticated join (a regional CA forces its own tenant; a global preserves the region's tenant).
     * Returns the region-signed chain [leaf, regionCA, root] as JSON for the joiner to install.
     */
    public String issueEnrollment(String joinerCertJson) {
        try {
            ensureRegionCA();
            if (regionCA == null) {
                return null;
            }
            X509Certificate[] joinerCerts = getCertsfromJson(joinerCertJson);
            if (joinerCerts == null || joinerCerts[0] == null) {
                logger.error("issueEnrollment: no joiner certificate");
                return null;
            }
            CrescoIdentity claimed = CrescoIdentity.fromCertificate(joinerCerts[0]);
            String tenant = (claimed != null && claimed.getTenant() != null) ? claimed.getTenant()
                    : plugin.getConfig().getStringParam("tenant_id", "default");
            String region = (claimed != null && claimed.getRegion() != null) ? claimed.getRegion() : "unknown";
            String agent  = (claimed != null && claimed.getAgent()  != null) ? claimed.getAgent()  : "unknown";
            // Stricter: a purely-regional CA stamps ITS tenant (an agent cannot cross tenants). A global
            // federates multiple tenants, so it preserves the joining region's tenant.
            boolean isGlobal = controllerEngine.cstate.isGlobalController();
            // W2 PEER FEDERATION: when enabled, two independently-rooted REGIONS trust each other as EQUALS
            // rather than one subordinating the other. The joiner presents its OWN region-CA chain
            // ([leaf, joinerRegionCA, joinerRoot]); we add that CA to OUR truststore (so we trust the peer's
            // own-signed identities), and we PRESERVE the joiner's tenant/region/agent instead of re-stamping
            // it under our tenant. This is the bilateral cross-trust that removes the need for a common
            // global as trust broker. Default off -> legacy subordinating behaviour unchanged.
            boolean peerFederation = plugin.getConfig().getBooleanParam("security_peer_federation", false);
            boolean joinerIsRegionPeer = (claimed != null && claimed.getAgent() != null
                    && joinerCerts.length > 1 && joinerCerts[1] != null);
            if (peerFederation && !isGlobal && joinerIsRegionPeer) {
                // trust the peer's own CA (+ root) — bilateral: the peer likewise trusts ours via the chain
                // we return below, which installEnrollment adds to its truststore.
                addCertificatesToTrustStore("peerCA-" + joinerCerts[1].getSerialNumber(),
                        joinerCerts.length > 2 && joinerCerts[2] != null
                                ? new X509Certificate[]{ joinerCerts[1], joinerCerts[2] }
                                : new X509Certificate[]{ joinerCerts[1] });
                try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
                logger.info("PEER-FEDERATION: cross-trusted peer region " + region + " (tenant=" + tenant
                        + ") as an EQUAL — added its CA to truststore, preserved its identity (no subordination).");
                // return OUR CA chain so the peer trusts us too (bilateral), WITHOUT re-signing the peer.
                return getJsonFromCerts(regionCA.caChain());
            }
            if (!isGlobal) {
                tenant = plugin.getConfig().getStringParam("tenant_id", tenant);
            }
            X509Certificate[] signed = regionCA.enroll(joinerCerts[0].getPublicKey(), tenant, region, agent, YEARS_VALID);
            logger.info("Enrolled node " + region + "_" + agent + " (tenant=" + tenant + ") under region CA");
            return getJsonFromCerts(signed);
        } catch (Exception ex) {
            logger.error("issueEnrollment error: " + ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Install a region-CA-signed identity received at enrollment: keep our private key, swap our cert
     * chain to the region-signed one, trust the issuing CA (which rides inside the chain), and refresh
     * the live broker + client SSL contexts so the new identity and anchor take effect immediately.
     */
    public boolean installEnrollment(String chainJson) {
        try {
            X509Certificate[] signedChain = getCertsfromJson(chainJson);
            if (signedChain == null || signedChain[0] == null) {
                logger.error("installEnrollment: empty enrollment chain");
                return false;
            }
            PrivateKey myKey = (PrivateKey) keyStore.getKey(keyStoreAlias, keyStorePassword);
            if (myKey == null) {
                logger.error("installEnrollment: no private key for alias " + keyStoreAlias);
                return false;
            }
            // W2 PEER FEDERATION: if the returned bundle is the PEER's CA (its leaf public key does not match
            // OUR key), it is a cross-trust bundle, not a re-issue of our identity. Add it to the truststore
            // and KEEP our own identity — the equal-peer relationship (no subordination).
            try {
                java.security.cert.Certificate ourCert = keyStore.getCertificate(keyStoreAlias);
                if (ourCert != null && !ourCert.getPublicKey().equals(signedChain[0].getPublicKey())) {
                    addCertificatesToTrustStore("peerCA-" + signedChain[0].getSerialNumber(), signedChain);
                    try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
                    try { controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
                    logger.info("PEER-FEDERATION: installed peer CA bundle (kept our own identity, no subordination): "
                            + CrescoIdentity.fromCertificate(signedChain[0]));
                    return true;
                }
            } catch (Exception ignore) { /* fall through to normal enrollment install */ }
            this.chain = signedChain;
            storeKeyAndCertificateChain(keyStoreAlias, keyStorePassword, myKey, signedChain);
            // trust the issuing CA (chain[1]) and its root (chain[2]) — the O(regions) trust anchors
            if (signedChain.length > 1 && signedChain[1] != null) {
                addCertificatesToTrustStore("issuingCA-" + signedChain[1].getSerialNumber(),
                        signedChain.length > 2 && signedChain[2] != null
                                ? new X509Certificate[]{ signedChain[1], signedChain[2] }
                                : new X509Certificate[]{ signedChain[1] });
            }
            try { controllerEngine.getBroker().updateTrustManager(); } catch (Exception ignore) {}
            try { controllerEngine.getActiveClient().refreshTrust(); } catch (Exception ignore) {}
            logger.info("Installed region-CA-signed identity: " + CrescoIdentity.fromCertificate(signedChain[0]));
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
            keyStore = KeyStore.getInstance("jks");
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
            trustStore = KeyStore.getInstance("jks");
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
        KeyStore keyStore=KeyStore.getInstance("jks");
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
        KeyStore keyStore=KeyStore.getInstance("jks");
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
            gen.initialize(2048, SecureRandom.getInstance("SHA1PRNG"));
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
