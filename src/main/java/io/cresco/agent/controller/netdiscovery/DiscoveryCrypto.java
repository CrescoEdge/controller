package io.cresco.agent.controller.netdiscovery;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.utilities.CLogger;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * Validators on discovery's locator messages (DISCOVER / DISCOVERED, and the legacy certify exchange): proof that the
 * sender holds the discovery secret of that type. They locate peers; trust is established only by the discovery v2
 * handshake ({@link DiscoveryAuth}).
 *
 * <p>Format {@code v2:<base64(iv || AES-256-GCM ciphertext+tag)>}, a fresh 96-bit IV per message and a key derived from
 * the secret with PBKDF2-HMAC-SHA-384 -- never the same token twice. The legacy format (AES/ECB under a SHA-1-derived
 * 128-bit key: a deterministic, replayable token) is produced and accepted only with {@code discovery_auth=compat}, for a
 * rolling upgrade.
 */
public class DiscoveryCrypto {
    private CLogger logger;
    private static final String ALGORITHM = "AES";
    private static final String V2 = "v2:";
    private final boolean legacy;

    public DiscoveryCrypto(ControllerEngine controllerEngine) {

        this.logger = controllerEngine.getPluginBuilder().getLogger(DiscoveryCrypto.class.getName(),CLogger.Level.Info);
        this.legacy = DiscoveryAuth.Mode.parse(controllerEngine.getPluginBuilder().getConfig()
                .getStringParam(DiscoveryAuth.MODE_KEY, "v2")) == DiscoveryAuth.Mode.COMPAT;
    }

    private static Key gcmKey(final String secKey) {
        return new SecretKeySpec(Arrays.copyOf(DiscoveryAuth.key(secKey, "validator"), 32), ALGORITHM);
    }

    private Key generateKeyFromString(final String secKey) throws Exception {
        //String SALT = "MrSaltyManBaby";
        String SALT = "jG0vixSqlM8bCf";
        byte[] keyVal = (SALT + secKey).getBytes(StandardCharsets.UTF_8);
        MessageDigest sha = MessageDigest.getInstance("SHA-1");
        keyVal = sha.digest(keyVal);
        keyVal = Arrays.copyOf(keyVal, 16); // use only first 128 bit
        final Key key = new SecretKeySpec(keyVal, ALGORITHM);
        return key;
    }

    public String encrypt(final String valueEnc, final String secKey) {
        try {
            if (legacy) {
                final Cipher c = Cipher.getInstance(ALGORITHM);
                c.init(Cipher.ENCRYPT_MODE, generateKeyFromString(secKey));
                return Base64.getEncoder().encodeToString(c.doFinal(valueEnc.getBytes(StandardCharsets.UTF_8)));
            }
            byte[] iv = new byte[12];
            new SecureRandom().nextBytes(iv);
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, gcmKey(secKey), new GCMParameterSpec(128, iv));
            byte[] ct = c.doFinal(valueEnc.getBytes(StandardCharsets.UTF_8));
            byte[] out = new byte[iv.length + ct.length];
            System.arraycopy(iv, 0, out, 0, iv.length);
            System.arraycopy(ct, 0, out, iv.length, ct.length);
            return V2 + Base64.getEncoder().encodeToString(out);
        } catch(Exception ex) {
            logger.error("DiscoveryCrypto.encrypt", ex);
            return null;
        }
    }

    public String decrypt(final String encryptedValue, final String secretKey) {
        if (encryptedValue == null || secretKey == null) return null;
        try {
            if (encryptedValue.startsWith(V2)) {
                byte[] in = Base64.getDecoder().decode(encryptedValue.substring(V2.length()));
                if (in.length < 12 + 16) return null;
                Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
                c.init(Cipher.DECRYPT_MODE, gcmKey(secretKey), new GCMParameterSpec(128, in, 0, 12));
                return new String(c.doFinal(in, 12, in.length - 12), StandardCharsets.UTF_8);
            }
            if (!legacy) {
                logger.debug("legacy discovery validator refused (discovery_auth=v2)");
                return null;
            }
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.DECRYPT_MODE, generateKeyFromString(secretKey));
            return new String(c.doFinal(Base64.getDecoder().decode(encryptedValue)), StandardCharsets.UTF_8);
        } catch (javax.crypto.BadPaddingException bx) {             // incl. AEADBadTagException: another secret
            logger.debug("configured secret cannot decrypt encrypted value");
            return null;
        } catch(Exception ex) {
            logger.debug("DiscoveryCrypto.decrypt: " + ex.getMessage());
            return null;
        }
    }
}
