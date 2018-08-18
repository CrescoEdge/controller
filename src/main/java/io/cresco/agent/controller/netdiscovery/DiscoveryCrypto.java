package io.cresco.agent.controller.netdiscovery;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.utilities.CLogger;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

public class DiscoveryCrypto {
    private CLogger logger;
    private static final String ALGORITHM = "AES";

    public DiscoveryCrypto(ControllerEngine controllerEngine) {

        this.logger = controllerEngine.getPluginBuilder().getLogger(DiscoveryCrypto.class.getName(),CLogger.Level.Info);
    }

    private Key generateKeyFromString(final String secKey) throws Exception {
        //String SALT = "MrSaltyManBaby";
        String SALT = "jG0vixSqlM8bCf";
        byte[] keyVal = (SALT + secKey).getBytes("UTF-8");
        MessageDigest sha = MessageDigest.getInstance("SHA-1");
        keyVal = sha.digest(keyVal);
        keyVal = Arrays.copyOf(keyVal, 16); // use only first 128 bit
        final Key key = new SecretKeySpec(keyVal, ALGORITHM);
        return key;
    }

    public String encrypt(final String valueEnc, final String secKey) {

        String encryptedValue = null;

        try {
            //logger.info("dec: [" + valueEnc + "] key:[" + secKey + "]");

            final Key key = generateKeyFromString(secKey);
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.ENCRYPT_MODE, key);
            final byte[] encValue = c.doFinal(valueEnc.getBytes());
            //encryptedValue = new BASE64Encoder().encode(encValue);
            encryptedValue = Base64.getEncoder().encodeToString(encValue);
            //logger.info("enc: [" + encryptedValue + "] key:[" + secKey + "]");

        } catch(Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }

        return encryptedValue;
    }

    public String decrypt(final String encryptedValue, final String secretKey) {

        String decryptedValue = null;

        try {
            //logger.info("enc: [" + encryptedValue + "] key:[" + secretKey + "]");
            final Key key = generateKeyFromString(secretKey);
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.DECRYPT_MODE, key);
            //final byte[] decorVal = new BASE64Decoder().decodeBuffer(encryptedValue);
            final byte[] decorVal = Base64.getDecoder().decode(encryptedValue);
            //byte[] valueDecoded= Base64.decodeBase64(bytesEncoded );
            final byte[] decValue = c.doFinal(decorVal);
            decryptedValue = new String(decValue);
            //logger.info("dec: [" + decryptedValue + "] key:[" + secretKey + "]");

        } catch(javax.crypto.BadPaddingException bx) {
            logger.debug(bx.getMessage() + " bad password error");
            bx.printStackTrace();
            logger.info("bad!");
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }

        return decryptedValue;
    }
}
