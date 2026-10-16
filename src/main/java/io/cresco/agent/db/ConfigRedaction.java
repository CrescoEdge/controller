package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Keeps secret-looking config values out of everything the controller sends elsewhere: the
 * watchdog and state exports to the regional and global controllers, and the plugininfo,
 * listplugins and listpluginsbytype replies, the agent-level pluginlist reply, and the pipeline
 * replies getgpipeline, getgpipelineexport and getinodestatus. Plugin config is stored in plaintext Derby and was
 * shipped upstream whole, so a plugin's key or password reached every controller above it and any
 * wsapi client of the global controller (GaiaKeep OUT-03).
 *
 * <p>An iNode's config (configparams) is also kept out of the pluginadd and addplugin replies (they no longer
 * echo it), and out of log lines: {@link #redactText} redacts JSON "name": value pairs at any escape depth and
 * withholds a compressed configparams whole, so a logged message map or config JSON carries no secret.
 *
 * <p>Only the copies that leave are redacted. The agent's own database keeps the full config,
 * because the agent restarts its plugins from it. Nothing upstream reads these values: the exports
 * feed status, placement and repo lookups, which use ids, names and non-secret settings.
 */
public final class ConfigRedaction {

    /**
     * A config key is secret when it contains secret, password, passphrase or token, or ends in _key
     * (case-insensitive), or has pin as a whole word: delimited by start, end, '_', '.' or '-' in any
     * case (pin, HSM_PIN, pkcs11.pin), or as a camelCase word (hsmPin, userPIN, pinCode). ping_interval,
     * mapping, spinlock and pinned are not caught.
     */
    public static final Pattern SECRET_KEY = Pattern.compile(
            "(?i:secret|password|passphrase|token|_key$)"
            + "|(?i:(?:^|[_.-])pin(?:[_.-]|$))"
            + "|(?:^|[_.-])(?:pin|Pin)(?=[A-Z0-9])"
            + "|[a-z0-9](?:Pin|PIN)(?![a-z])");
    public static final String REDACTED = "[REDACTED]";

    /**
     * A name=value parameter embedded in a value or a log line: a URI query or ActiveMQ transport options
     * (activemq_client_transport_options, a failover:(nio+ssl://h:p?keyStorePassword=...) URI), a JDBC URL
     * (;password=...). The value runs to the next separator ActiveMQ and JDBC URLs use: & ; , ) whitespace,
     * or a quote. A value containing one of those cannot be carried in such a URL unescaped anyway.
     */
    private static final Pattern EMBEDDED_PARAM = Pattern.compile("([A-Za-z0-9_.\\-]+)=([^&;,)\\s\"']*)");

    private static final Gson GSON = new Gson();

    private ConfigRedaction() {}

    public static boolean isSecretKey(String key) { return key != null && SECRET_KEY.matcher(key).find(); }

    /**
     * A "name": value pair inside JSON text, at any escape depth: a JSON document logged whole, or one embedded
     * as a string in another ({\"password\":\"x\"}). Group 1 is the backslashes escaping the key's quote, so the
     * value's closing quote is found at the same depth (see {@link #valueEnd}).
     */
    private static final Pattern JSON_KEY = Pattern.compile("(\\\\*)\"([A-Za-z0-9_.\\-]+)\\\\*\"\\s*:\\s*");
    /** A bare JSON scalar (number, true, false, null). */
    private static final Pattern JSON_SCALAR = Pattern.compile("[-+0-9.eEa-zA-Z]+");
    /** The compressed-param prefix: MsgEvent.setCompressedParam is base64 of gzip, which starts with H4sI. */
    private static final String GZIP_BASE64 = "H4sI";

    /**
     * True for a plugin-config carrier whose value is compressed (MsgEvent.setCompressedParam): configparams
     * travels gzip-base64 in pluginadd and addplugin messages, so a logged message map shows it as
     * configparams=H4sI... That is the whole config, secrets included, one base64 decode away; it is withheld
     * as a unit because it cannot be redacted key by key without decompressing.
     */
    static boolean isCompressedConfig(String name, String value) {
        return "configparams".equalsIgnoreCase(name) && value != null && value.startsWith(GZIP_BASE64);
    }

    /**
     * The text with the value of every embedded secret-looking parameter replaced by {@link #REDACTED}:
     * name=value parameters ({@link #isSecretKey}: keyStorePassword, trustStorePassword, password, any
     * *Password* or *secret*, token, passphrase, *_key, pin) and "name": value pairs of JSON text at any escape
     * depth. A compressed configparams value ({@link #isCompressedConfig}) is withheld whole. Everything else
     * is kept, so an option string, URI or JSON document stays readable. Stable: redacting twice gives the same
     * text. Used for config values whose own key is not secret (activemq_client_transport_options) and for every
     * log line (CLoggerImpl, DataPlaneLogger), where a broker URI, a message map or a JSON config would
     * otherwise appear.
     */
    public static String redactText(String text) {
        if (text == null) return null;
        String out = redactNameValues(text);
        return out.indexOf('"') < 0 ? out : redactJsonPairs(out);
    }

    private static String redactNameValues(String text) {
        if (text.indexOf('=') < 0) return text;
        java.util.regex.Matcher m = EMBEDDED_PARAM.matcher(text);
        StringBuilder sb = null;
        int last = 0, pos = 0;
        while (pos < text.length() && m.find(pos)) {
            if (!isSecretKey(m.group(1)) && !isCompressedConfig(m.group(1), m.group(2))) {
                pos = m.end(1) + 1;          // look inside a non-secret value too: brokerURL=x?password=...
                continue;
            }
            pos = m.end(2);                  // a secret value is taken whole, even when it contains '=' or '?'
            if (m.group(2).isEmpty() || REDACTED.equals(m.group(2))) continue;
            if (sb == null) sb = new StringBuilder(text.length());
            sb.append(text, last, m.start(2)).append(REDACTED);
            last = m.end(2);
        }
        return sb == null ? text : sb.append(text, last, text.length()).toString();
    }

    private static String redactJsonPairs(String text) {
        java.util.regex.Matcher m = JSON_KEY.matcher(text);
        StringBuilder sb = null;
        int last = 0, pos = 0;
        while (pos < text.length() && m.find(pos)) {
            pos = m.end();
            String key = m.group(2);
            int depth = m.group(1).length();
            String quote = "\\".repeat(depth) + "\"";
            int vs = m.end();
            boolean quoted = text.startsWith(quote, vs);
            int start, end;
            if (quoted) {
                start = vs + quote.length();
                end = valueEnd(text, start, depth);          // -1: unterminated, taken to the end of the text
            } else {
                java.util.regex.Matcher s = JSON_SCALAR.matcher(text).region(vs, text.length());
                if (!s.lookingAt()) continue;
                start = vs;
                end = s.end();
            }
            String value = text.substring(start, end < 0 ? text.length() : end);
            boolean secret = isSecretKey(key) || (quoted && isCompressedConfig(key, value));
            if (!secret || value.isEmpty() || REDACTED.equals(value)) {
                if (!quoted && end > 0) pos = end;
                continue;                                  // a non-secret string value is searched too (nested JSON)
            }
            if (sb == null) sb = new StringBuilder(text.length());
            // a bare scalar becomes a string at the same depth, so the JSON stays well formed
            sb.append(text, last, start).append(quoted ? REDACTED : quote + REDACTED + quote);
            last = end < 0 ? text.length() : end;
            pos = last;
        }
        return sb == null ? text : sb.append(text, last, text.length()).toString();
    }

    /**
     * Where a JSON string value that opened at {@code start} closes, at escape depth {@code depth} (the number
     * of backslashes before its opening quote: 0 in a plain document, 1 inside a JSON string, 3 inside that).
     * A quote preceded by k backslashes closes the value when k - depth is a whole number of escaped
     * backslashes at that depth (2(depth+1) each); a quote inside the value carries 2depth+1 instead.
     * Returns the index of the closing backslash-run's start, or -1 if the value never closes.
     */
    static int valueEnd(String text, int start, int depth) {
        for (int i = start; i < text.length(); i++) {
            if (text.charAt(i) != '"') continue;
            int k = 0;
            while (i - k - 1 >= start && text.charAt(i - k - 1) == '\\') k++;
            if (k >= depth && (k - depth) % (2 * (depth + 1)) == 0) return i - depth;
        }
        return -1;
    }

    /**
     * The throwable to log in place of t: t itself when neither it nor a cause carries a secret-looking
     * embedded parameter (an ActiveMQ "Could not connect to broker URL: ...?keyStorePassword=..." message),
     * else a copy whose messages are redacted, keeping every stack trace and the cause chain.
     */
    public static Throwable redactThrowable(Throwable t) {
        if (t == null) return null;
        boolean dirty = false;
        Throwable c = t;
        for (int depth = 0; c != null && !dirty && depth < 16; depth++) {   // bounded: a cause chain can loop
            String s = c.toString();
            dirty = !s.equals(redactText(s));
            c = (c.getCause() == c) ? null : c.getCause();
        }
        return dirty ? copyRedacted(t, 0) : t;
    }

    private static Throwable copyRedacted(Throwable t, int depth) {
        Throwable cause = (t.getCause() != null && t.getCause() != t && depth < 16) ? copyRedacted(t.getCause(), depth + 1) : null;
        RedactedThrowable r = new RedactedThrowable(redactText(t.toString()), cause);
        r.setStackTrace(t.getStackTrace());
        return r;
    }

    /** A logged stand-in for a throwable whose message carried a secret: prints as the original class and redacted message. */
    static final class RedactedThrowable extends Throwable {
        private final String text;
        RedactedThrowable(String text, Throwable cause) { super(text, cause, false, true); this.text = text; }
        @Override public String toString() { return text; }
    }

    /**
     * A configparams JSON object with every secret value replaced. Anything that is not a JSON object
     * is withheld entirely ("{}"): what cannot be read cannot be shown to be safe.
     */
    public static String redactJson(String configJson) {
        if (configJson == null) return null;
        try {
            JsonElement e = JsonParser.parseString(configJson);
            if (!e.isJsonObject()) return "{}";
            JsonObject o = e.getAsJsonObject();
            for (String k : o.keySet()) {
                JsonElement v = o.get(k);
                if (isSecretKey(k)) o.add(k, new JsonPrimitive(REDACTED));
                else if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString()) redactStringIn(o, k, v.getAsString());
            }
            return GSON.toJson(o);
        } catch (RuntimeException unreadable) {
            return "{}";
        }
    }

    /** A copy of a flat config map with every secret value replaced. */
    public static Map<String, String> redactMap(Map<String, String> m) {
        if (m == null) return null;
        Map<String, String> out = new HashMap<>(m);
        for (Map.Entry<String, String> e : out.entrySet()) {
            if (isSecretKey(e.getKey())) e.setValue(REDACTED);
            else if (e.getValue() != null) e.setValue(redactText(e.getValue()));   // e.g. activemq_client_transport_options
        }
        return out;
    }

    /** Replaces o[k] when its string value carries a secret-looking embedded parameter. */
    private static void redactStringIn(JsonObject o, String k, String value) {
        String r = redactText(value);
        if (!r.equals(value)) o.add(k, new JsonPrimitive(r));
    }

    /**
     * A pipeline (gpipeline) JSON document with every secret value replaced, at any depth: node
     * params are plugin config, and an embedded configparams or params string is redacted as config
     * too. What cannot be parsed is withheld ("{}"). The getgpipelineexport reply is meant for
     * re-deployment, so a redacted export needs its secrets supplied again; that is intended.
     */
    public static String redactPipelineJson(String json) {
        if (json == null) return null;
        try {
            JsonElement e = JsonParser.parseString(json);
            if (!e.isJsonObject()) return "{}";
            redactTree(e);
            return GSON.toJson(e);
        } catch (RuntimeException unreadable) {
            return "{}";
        }
    }

    private static void redactTree(JsonElement e) {
        if (e.isJsonArray()) {
            com.google.gson.JsonArray a = e.getAsJsonArray();
            for (int i = 0; i < a.size(); i++) {
                JsonElement x = a.get(i);
                if (x.isJsonPrimitive() && x.getAsJsonPrimitive().isString()) a.set(i, new JsonPrimitive(redactText(x.getAsString())));
                else redactTree(x);
            }
        } else if (e.isJsonObject()) {
            JsonObject o = e.getAsJsonObject();
            for (String k : new java.util.ArrayList<>(o.keySet())) {
                JsonElement v = o.get(k);
                if (isSecretKey(k)) o.add(k, new JsonPrimitive(REDACTED));
                else if (("configparams".equals(k) || "params".equals(k)) && v.isJsonPrimitive() && v.getAsJsonPrimitive().isString())
                    o.add(k, new JsonPrimitive(redactJson(v.getAsString())));
                else if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString()) redactStringIn(o, k, v.getAsString());
                else redactTree(v);
            }
        }
    }

    /** A copy of an iNode status map (getinodestatus) with its params / configparams redacted. */
    public static Map<String, String> redactINode(Map<String, String> inode) {
        if (inode == null) return null;
        Map<String, String> out = redactMap(inode);
        for (String k : new String[]{"params", "configparams"}) if (inode.get(k) != null) out.put(k, redactJson(inode.get(k)));
        return out;
    }

    /** A copy of an exported rnode/anode/pnode row with its configparams redacted. */
    public static Map<String, String> redactNode(Map<String, String> node) {
        if (node == null) return null;
        Map<String, String> out = new HashMap<>(node);
        for (Map.Entry<String, String> e : out.entrySet()) {
            if ("configparams".equals(e.getKey())) e.setValue(redactJson(e.getValue()));
            else if (e.getValue() != null) e.setValue(redactText(e.getValue()));
        }
        return out;
    }
}
