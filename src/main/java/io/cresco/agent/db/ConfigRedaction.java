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

    private static final Gson GSON = new Gson();

    private ConfigRedaction() {}

    public static boolean isSecretKey(String key) { return key != null && SECRET_KEY.matcher(key).find(); }

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
            for (String k : o.keySet()) if (isSecretKey(k)) o.add(k, new JsonPrimitive(REDACTED));
            return GSON.toJson(o);
        } catch (RuntimeException unreadable) {
            return "{}";
        }
    }

    /** A copy of a flat config map with every secret value replaced. */
    public static Map<String, String> redactMap(Map<String, String> m) {
        if (m == null) return null;
        Map<String, String> out = new HashMap<>(m);
        for (Map.Entry<String, String> e : out.entrySet()) if (isSecretKey(e.getKey())) e.setValue(REDACTED);
        return out;
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
            for (JsonElement x : e.getAsJsonArray()) redactTree(x);
        } else if (e.isJsonObject()) {
            JsonObject o = e.getAsJsonObject();
            for (String k : new java.util.ArrayList<>(o.keySet())) {
                JsonElement v = o.get(k);
                if (isSecretKey(k)) o.add(k, new JsonPrimitive(REDACTED));
                else if (("configparams".equals(k) || "params".equals(k)) && v.isJsonPrimitive() && v.getAsJsonPrimitive().isString())
                    o.add(k, new JsonPrimitive(redactJson(v.getAsString())));
                else redactTree(v);
            }
        }
    }

    /** A copy of an iNode status map (getinodestatus) with its params / configparams redacted. */
    public static Map<String, String> redactINode(Map<String, String> inode) {
        if (inode == null) return null;
        Map<String, String> out = redactMap(inode);
        for (String k : new String[]{"params", "configparams"}) if (out.get(k) != null) out.put(k, redactJson(out.get(k)));
        return out;
    }

    /** A copy of an exported rnode/anode/pnode row with its configparams redacted. */
    public static Map<String, String> redactNode(Map<String, String> node) {
        if (node == null) return null;
        Map<String, String> out = new HashMap<>(node);
        if (out.containsKey("configparams")) out.put("configparams", redactJson(out.get("configparams")));
        return out;
    }
}
