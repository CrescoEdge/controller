package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Secret-looking plugin config values never leave the agent in an export or a reply. */
class ConfigRedactionTest {

    static final Gson GSON = new Gson();

    @Test
    void secretLookingKeysMatch() {
        for (String k : new String[]{"gfs_secret", "GFS_SECRET_FILE", "core_master_key", "cresco_service_key", "db_password",
                "ssl_passphrase", "api_token", "tokenizer_model", "pin", "hsm_pin", "gfs_pkcs11_pin_file", "PIN_env", "discovery_secret_agent",
                "hsmPin", "userPIN", "pinCode", "hsmPinFile", "pkcs11.pin", "token-pin", "HSM_PIN", "Pin", "PIN", "API_KEY"})
            assertTrue(ConfigRedaction.isSecretKey(k), k);
        for (String k : new String[]{"pluginname", "jarfile", "md5", "version", "gfs_roles", "index_addr", "ping_interval_ms",
                "mapping", "spinlock", "keyspace", "key_count", "site_id", "location", "inode_id", "pipeline",
                "PING_TIMEOUT", "pinned", "wrapPinned", "shipping", "SNAPPING", "pinger", "keepAlive"})
            assertFalse(ConfigRedaction.isSecretKey(k), k);
    }

    @Test
    void configparamsKeepEverythingButSecretValues() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pluginname", "io.cresco.gfs");
        cfg.put("gfs_secret", "gfs-federation-secret-0001");
        cfg.put("core_master_key", "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff");
        cfg.put("pledge_bytes", 50000000000L);
        String out = ConfigRedaction.redactJson(GSON.toJson(cfg));
        assertFalse(out.contains("gfs-federation-secret-0001"));
        assertFalse(out.contains("00112233445566778899"));
        Map<String, Object> back = GSON.fromJson(out, new TypeToken<Map<String, Object>>() {}.getType());
        assertEquals(ConfigRedaction.REDACTED, back.get("gfs_secret"));
        assertEquals(ConfigRedaction.REDACTED, back.get("core_master_key"));
        assertEquals("io.cresco.gfs", back.get("pluginname"));
        assertEquals(5.0E10, ((Number) back.get("pledge_bytes")).doubleValue());
        assertEquals(out, ConfigRedaction.redactJson(out), "stable: the diff-gated export does not churn");
    }

    @Test
    void unreadableConfigIsWithheldEntirely() {
        assertEquals("{}", ConfigRedaction.redactJson("gfs_secret=abc"));
        assertEquals("{}", ConfigRedaction.redactJson("[\"gfs_secret\",\"abc\"]"));
        assertEquals("{}", ConfigRedaction.redactJson("{\"gfs_secret\": \"abc\""));
        assertNull(ConfigRedaction.redactJson(null));
    }

    @Test
    void anExportedRowIsACopyWithItsConfigRedacted() {
        Map<String, String> row = new HashMap<>();
        row.put("plugin_id", "plugin/3");
        row.put("status_code", "10");
        row.put("configparams", "{\"pluginname\":\"io.cresco.gfs\",\"gfs_secret\":\"s3cr3t-value\"}");
        Map<String, String> out = ConfigRedaction.redactNode(row);
        assertFalse(out.get("configparams").contains("s3cr3t-value"));
        assertEquals("plugin/3", out.get("plugin_id"));
        assertTrue(row.get("configparams").contains("s3cr3t-value"), "the agent's own row is untouched");
        Map<String, String> flat = new HashMap<>(Map.of("pluginid", "p", "cresco_service_key", "k", "region", "r"));
        Map<String, String> red = ConfigRedaction.redactMap(flat);
        assertEquals(ConfigRedaction.REDACTED, red.get("cresco_service_key"));
        assertEquals("p", red.get("pluginid"));
        assertEquals("k", flat.get("cresco_service_key"));
    }
}
