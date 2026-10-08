package io.cresco.agent.core;

import io.cresco.agent.db.ConfigRedaction;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Secrets off the JVM command line (GaiaKeep OUT-03b). A {@code -Dparam=value} is visible to every
 * local user in {@code ps}/{@code /proc/<pid>/cmdline}; this lets the agent take a secret parameter
 * from a file or the environment instead, ahead of {@code -D}:
 *
 * <ol>
 *   <li>{@code <param>_file}: a path, given as {@code -D<param>_file}, {@code CRESCO_<PARAM>_FILE} or
 *       in agent.ini; the file must pass {@link OwnerOnlyFile} (owner-only 0600/0400, safe directory)
 *       or the agent refuses to start;</li>
 *   <li>the environment variable {@code CRESCO_<PARAM>} (upper case; the exact-case
 *       {@code CRESCO_<param>} the library already reads is accepted too);</li>
 *   <li>{@code -D<param>} and agent.ini, as before.</li>
 * </ol>
 *
 * The resolved value goes into the agent config map. The library Config reads {@code -D} before
 * the map on every lookup, so when a file or environment value wins, a {@code -D} copy of the same
 * parameter is cleared from the system properties (and a warning says to take it off the command
 * line). A secret still given only by {@code -D} keeps working, with a warning.
 *
 * <p>Secret parameters are {@link #AGENT_SECRET_PARAMS} plus any parameter whose name
 * {@link ConfigRedaction#isSecretKey looks secret} and that has a {@code _file} or
 * {@code CRESCO_} environment source.
 */
public final class SecretSources {

    /** The agent's own secret parameters (read through the controller's plugin config). */
    public static final List<String> AGENT_SECRET_PARAMS = Collections.unmodifiableList(Arrays.asList(
            "keystorepwd", "truststorepwd", "db_password", "broker_security_secret",
            "discovery_secret_agent", "discovery_secret_region", "discovery_secret_global"));

    /**
     * Families whose {@code <name>_file} their owner reads with its own rules, so this resolver must not read it too
     * (the owner would see the secret twice and refuse to start): db_key (DBEngine's key file, OUT-03a) and gfs's key
     * families (KeySources: key-record files plus {@code _env} indirection). Every parameter of a family
     * ({@code gfs_secret_env}, {@code gfs_secret_previous_files}, ...) keeps the plain lookup. The same list as the
     * library's SecretParams.NOT_SOURCES.
     */
    static final Set<String> NOT_SOURCES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
            "db_key", "gfs_secret", "core_master_key", "tape_media_key", "gfs_pkcs11_pin")));

    static boolean inExcludedFamily(String param) {
        for (String family : NOT_SOURCES) if (param.equals(family) || param.startsWith(family + "_")) return true;
        return false;
    }

    static final String ENV_PREFIX = "CRESCO_";
    static final String FILE_SUFFIX = "_file";

    /** A {@code <param>_file} source is configured but unusable: the agent must not start. */
    public static final class SecretSourceException extends IllegalStateException {
        SecretSourceException(String msg, Throwable cause) { super(msg, cause); }
    }

    private SecretSources() {}

    static String envName(String param) {
        return ENV_PREFIX + param.toUpperCase(Locale.ROOT);
    }

    /** Is this a parameter we resolve: a listed agent secret, or a secret-looking name. */
    static boolean isSecretParam(String param) {
        return param != null && !param.isEmpty() && !param.toLowerCase(Locale.ROOT).endsWith(FILE_SUFFIX)
                && !inExcludedFamily(param)
                && (AGENT_SECRET_PARAMS.contains(param) || ConfigRedaction.isSecretKey(param));
    }

    /** Every parameter that has a file/env source, plus the listed agent secrets. */
    static Set<String> candidates(Map<String, Object> config, Map<String, String> env, Properties sys) {
        Set<String> names = new LinkedHashSet<>(AGENT_SECRET_PARAMS);
        List<String> keys = new ArrayList<>();
        for (Object k : config.keySet()) keys.add(String.valueOf(k));
        for (Object k : sys.keySet()) keys.add(String.valueOf(k));
        for (String k : keys) {
            if (k.toLowerCase(Locale.ROOT).endsWith(FILE_SUFFIX)) {
                String p = k.substring(0, k.length() - FILE_SUFFIX.length());
                if (isSecretParam(p)) names.add(p);
            }
        }
        for (String e : env.keySet()) {
            if (!e.startsWith(ENV_PREFIX) || e.length() == ENV_PREFIX.length()) continue;
            String p = e.substring(ENV_PREFIX.length());
            if (p.toLowerCase(Locale.ROOT).endsWith(FILE_SUFFIX)) p = p.substring(0, p.length() - FILE_SUFFIX.length());
            // the exact-case form (CRESCO_db_password) names the parameter; the upper-case form maps to lower case
            String param = p.equals(p.toUpperCase(Locale.ROOT)) ? p.toLowerCase(Locale.ROOT) : p;
            if (isSecretParam(param)) names.add(param);
        }
        return names;
    }

    /**
     * Resolve the secret parameters into {@code config}. {@code sys} is the live system properties
     * in production (a {@code -D} that loses is cleared from it). Returns log lines (never values) for
     * the caller to emit once its logger exists. Throws SecretSourceException on an unusable file.
     */
    public static List<String> resolve(Map<String, Object> config, Map<String, String> env, Properties sys) {
        List<String> notes = new ArrayList<>();
        for (String p : candidates(config, env, sys)) {
            String value = null;
            String source = null;

            String filePath = sys.getProperty(p + FILE_SUFFIX);
            String fileFrom = "-D" + p + FILE_SUFFIX;
            if (filePath == null) { filePath = env.get(envName(p) + "_FILE"); fileFrom = envName(p) + "_FILE"; }
            if (filePath == null) { filePath = env.get(ENV_PREFIX + p + FILE_SUFFIX); fileFrom = ENV_PREFIX + p + FILE_SUFFIX; }
            if (filePath == null && config.get(p + FILE_SUFFIX) != null) {
                filePath = String.valueOf(config.get(p + FILE_SUFFIX));
                fileFrom = p + FILE_SUFFIX + " (agent config)";
            }
            if (filePath != null && !filePath.isBlank()) {
                try {
                    value = OwnerOnlyFile.read(Paths.get(filePath.trim()), p + FILE_SUFFIX);
                } catch (OwnerOnlyFile.UnsafeFileException e) {
                    throw new SecretSourceException("refusing to start: secret " + p + " from " + fileFrom + ": " + e.getMessage(), e);
                }
                source = "file " + filePath.trim() + " (" + fileFrom + ")";
            } else if (env.get(envName(p)) != null) {
                value = env.get(envName(p));
                source = "environment " + envName(p);
            } else if (env.get(ENV_PREFIX + p) != null) {
                value = env.get(ENV_PREFIX + p);
                source = "environment " + ENV_PREFIX + p;
            }

            boolean onCommandLine = sys.getProperty(p) != null;
            if (source != null) {
                config.put(p, value);
                notes.add("secret " + p + " taken from " + source);
                if (onCommandLine) {
                    sys.remove(p);
                    notes.add("WARN: -D" + p + " ignored and cleared (" + source + " wins); take it off the JVM command line");
                }
                if (!source.equals("environment " + ENV_PREFIX + p) && env.get(ENV_PREFIX + p) != null) {
                    notes.add("WARN: " + ENV_PREFIX + p + " is also set; the library Config reads it ahead of the agent config, unset it");
                }
            } else if (onCommandLine && AGENT_SECRET_PARAMS.contains(p)) {
                notes.add("WARN: secret " + p + " is on the JVM command line (-D" + p + ", visible to local users);"
                        + " use " + p + FILE_SUFFIX + " (an 0600 file) or " + envName(p));
            }
        }
        return notes;
    }
}
