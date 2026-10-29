package io.cresco.agent.controller.communication;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;

/**
 * Validity and serial numbers for every certificate Cresco makes. Certificates are (re)issued at discovery time and used
 * at once by peers whose clocks may be behind: notBefore is backdated by {@link #BACKDATE_MS} so a peer up to an hour
 * slow still accepts a certificate issued "now" (common CA practice). Serials are 63 random bits, always positive.
 */
public final class CertValidity {

    private CertValidity() { }

    /** How far notBefore is set in the past. */
    public static final long BACKDATE_MS = 3_600_000L;

    private static final SecureRandom RNG = new SecureRandom();

    public static Date notBefore() {
        return new Date(System.currentTimeMillis() - BACKDATE_MS);
    }

    public static Date notAfterYears(int years) {
        return new Date(System.currentTimeMillis() + years * 365L * 24 * 3600 * 1000);
    }

    public static BigInteger serial() {
        return new BigInteger(63, RNG).add(BigInteger.ONE);
    }
}
