package io.cresco.agent.controller.communication;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;

/**
 * Validity and serial numbers for every certificate Cresco makes. Certificates are (re)issued at discovery time and used
 * at once by peers whose clocks may be behind: notBefore is backdated by {@link #backdateMs()} so a peer whose clock lags
 * by less than that still accepts a certificate issued "now". The backdate is {@code identity_cert_backdate_hours}
 * (default 24, at least 1): with short-lived region-issued leaves a node whose clock lagged more than the old fixed hour
 * was locked out of every certificate its region issued. Clock skew larger than this is detected and reported (see
 * {@link #skewText}). Serials are 63 random bits, always positive.
 */
public final class CertValidity {

    private CertValidity() { }

    /** The default backdate (and the clock lag every certificate tolerates). */
    public static final long DEFAULT_BACKDATE_MS = 24L * 3_600_000L;

    private static volatile long backdateMs = DEFAULT_BACKDATE_MS;

    private static final SecureRandom RNG = new SecureRandom();

    /** How far notBefore is set in the past. */
    public static long backdateMs() { return backdateMs; }

    /** Set from {@code identity_cert_backdate_hours} (at least 1 hour). */
    public static void setBackdateHours(int hours) {
        backdateMs = Math.max(1, hours) * 3_600_000L;
    }

    public static Date notBefore() {
        return new Date(System.currentTimeMillis() - backdateMs);
    }

    public static Date notAfterYears(int years) {
        return new Date(System.currentTimeMillis() + years * 365L * 24 * 3600 * 1000);
    }

    public static BigInteger serial() {
        return new BigInteger(63, RNG).add(BigInteger.ONE);
    }

    /**
     * A description of the clock offset {@code skewMs} (peer clock minus this node's), or null when it is harmless: a peer
     * whose clock is ahead by more than the backdate issues certificates this node sees as not yet valid (and the other way
     * round).
     */
    public static String skewText(long skewMs, String peer) {
        long abs = Math.abs(skewMs);
        if (abs < backdateMs / 2) return null;
        return "the clock of " + peer + " is " + (abs / 60_000L) + " min " + (skewMs > 0 ? "AHEAD of" : "BEHIND") + " this node's"
                + (abs >= backdateMs ? " -- more than the certificate backdate (" + (backdateMs / 3_600_000L) + " h): certificates one side"
                + " issues are NOT YET VALID for the other; fix the clocks (NTP)" : " (half the certificate backdate or more; fix the clocks)");
    }
}
