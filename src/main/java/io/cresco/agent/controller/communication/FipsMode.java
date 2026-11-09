package io.cresco.agent.controller.communication;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.HexFormat;
import java.util.Set;

/**
 * {@code security_profile=production} runs in FIPS 140-3 approved-only mode: the agent's CryptoBoot sets it up before any
 * bundle starts (docs/production-deployment.md, section 6). The controller does not take that on trust -- a node launched
 * some other way, or a JVM whose providers were changed after the boot, would sign, verify and agree keys with whatever
 * the JVM offers -- so before a production node starts, the controller checks it itself and refuses to start otherwise:
 * <ul>
 *   <li>every JCA provider installed is one FIPS mode leaves: the validated modules (Bouncy Castle FIPS {@code BCFIPS}, its
 *       {@code BCJSSE}, ACCP-FIPS behind {@code CrescoACCP-FIPS}), Cresco's names over them ({@code CrescoFIPS}) and the
 *       JDK's TLS protocol engine ({@code SunJSSE}), whose every cryptographic operation is then a module's;</li>
 *   <li>Bouncy Castle FIPS is in approved-only mode and its power-on self-tests passed ({@code FipsStatus.isReady});</li>
 *   <li>ACCP-FIPS, where installed, is the FIPS build, loaded and healthy;</li>
 *   <li>a non-approved algorithm is refused (MD5 is not available), and an approved one answers its known answer
 *       (SHA-384 of "abc", FIPS 180-4).</li>
 * </ul>
 */
public final class FipsMode {

    private FipsMode() { }

    /** The JCA providers FIPS approved-only mode leaves (the agent's FipsProviders.install). */
    static final Set<String> ALLOWED = Set.of("CrescoACCP-FIPS", "BCFIPS", "BCJSSE", "CrescoFIPS", "SunJSSE");
    private static final String SHA384_ABC = "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7";

    /** Why this JVM is not in FIPS 140-3 approved-only mode, or null when it is. */
    public static String refusal() {
        return refusal(java.security.Security.getProviders());
    }

    static String refusal(Provider[] providers) {
        if (providers == null || providers.length == 0) return "no JCA provider is installed";
        Provider bc = null, accp = null;
        for (Provider p : providers) {
            if (!ALLOWED.contains(p.getName())) {
                return "the JCA provider " + p.getName() + " is installed, which is not a FIPS 140-3 validated module (FIPS mode"
                        + " leaves only " + new java.util.TreeSet<>(ALLOWED) + ")";
            }
            if (p.getName().equals("BCFIPS")) bc = p;
            if (p.getName().equals("CrescoACCP-FIPS")) accp = p;
        }
        if (bc == null) return "Bouncy Castle FIPS (BCFIPS) is not installed";
        ClassLoader cl = bc.getClass().getClassLoader();
        try {
            Class<?> reg = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar", true, cl);
            if (!Boolean.TRUE.equals(reg.getMethod("isInApprovedOnlyMode").invoke(null))) {
                return "Bouncy Castle FIPS is not in approved-only mode";
            }
            Class<?> status = Class.forName("org.bouncycastle.crypto.fips.FipsStatus", true, cl);
            if (!Boolean.TRUE.equals(status.getMethod("isReady").invoke(null))) {
                return "the Bouncy Castle FIPS module's self-tests did not pass: " + status.getMethod("getStatusMessage").invoke(null);
            }
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            Throwable c = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
            return "the Bouncy Castle FIPS module cannot be checked (" + c + ")";
        }
        if (accp != null) {
            try {
                Provider p = (Provider) Class.forName("com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider", true,
                        ClassLoader.getSystemClassLoader()).getField("INSTANCE").get(null);
                Object loadErr = p.getClass().getMethod("getLoadingError").invoke(p);
                if (loadErr != null) return "ACCP-FIPS did not load: " + loadErr;
                p.getClass().getMethod("assertHealthy").invoke(p);
                if (!Boolean.TRUE.equals(p.getClass().getMethod("isFips").invoke(p))) return "ACCP is installed, and it is not its FIPS build";
            } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                Throwable c = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
                return "ACCP-FIPS is not healthy (" + c + ")";
            }
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return "a non-approved algorithm is available (MD5, from " + md5.getProvider().getName() + ")";
        } catch (NoSuchAlgorithmException refused) {
            // as it must be
        }
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-384");
            String got = HexFormat.of().formatHex(sha.digest("abc".getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
            if (!SHA384_ABC.equals(got)) return "SHA-384 from " + sha.getProvider().getName() + " fails its known answer";
            if (!ALLOWED.contains(sha.getProvider().getName())) return "SHA-384 is served by " + sha.getProvider().getName();
        } catch (Exception e) {
            return "SHA-384 is not available (" + e + ")";
        }
        return null;
    }

    /** One line naming what serves crypto (for the start-up log). */
    public static String summary() {
        StringBuilder b = new StringBuilder();
        for (Provider p : java.security.Security.getProviders()) b.append(b.length() == 0 ? "" : ", ").append(p.getName());
        return b.toString();
    }
}
