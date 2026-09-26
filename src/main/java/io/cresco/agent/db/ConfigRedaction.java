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
 * listplugins and listpluginsbytype replies. Plugin config is stored in plaintext Derby and was
 * shipped upstream whole, so a plugin's key or password reached every controller above it and any
 * wsapi client of the global controller (GaiaKeep OUT-03).
 *
 * <p>Only the copies that leave are redacted. The agent's own database keeps the full config,
 * because the agent restarts its plugins from it. Nothing upstream reads these values: the exports
 * feed status, placement and repo lookups, which use ids, names and non-secret settings.
 */
public final class ConfigRedaction {

    /**
     * A config key is secret when it contains secret, password, passphrase or token, ends in _key,
     * or has pin as a whole underscore-separated word (so ping_interval and mapping are not caught).
     * Case-insensitive.
     */
    public static final Pattern SECRET_KEY = Pattern.compile("(?i)(secret|password|passphrase|token|_key$|(^|_)pin(_|$))");
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

    /** A copy of an exported rnode/anode/pnode row with its configparams redacted. */
    public static Map<String, String> redactNode(Map<String, String> node) {
        if (node == null) return null;
        Map<String, String> out = new HashMap<>(node);
        if (out.containsKey("configparams")) out.put("configparams", redactJson(out.get("configparams")));
        return out;
    }
}
