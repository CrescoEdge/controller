package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.db.ConfigRedaction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The agent-level pluginlist reply (AgentExecutor.pluginList -> PluginAdmin.getPluginList) carries no plugin secret. */
class PluginListRedactionTest {

    @Test
    void everyRowOfThePluginListIsRedacted() {
        Map<String, Map<String, String>> db = new HashMap<>();
        Map<String, String> gfs = new HashMap<>();
        gfs.put("pluginid", "plugin/1");
        gfs.put("pluginname", "io.cresco.gfs");
        gfs.put("configparams", "{\"pluginname\":\"io.cresco.gfs\",\"gfs_secret\":\"s3cr3t-A\",\"core_master_key\":\"s3cr3t-B\","
                + "\"gfs_pkcs11_pin_env\":\"s3cr3t-C\",\"hsmPin\":\"s3cr3t-D\",\"gfs_roles\":\"index\"}");
        db.put("plugin/1", gfs);
        Map<String, String> other = new HashMap<>();
        other.put("pluginid", "plugin/2");
        other.put("configparams", "not json: password=s3cr3t-E");
        db.put("plugin/2", other);

        String json = PluginAdmin.redactedRows(List.of("plugin/1", "plugin/2", "plugin/gone"), db::get);
        assertFalse(json.contains("s3cr3t"), json);
        List<Map<String, String>> rows = new Gson().fromJson(json, new TypeToken<List<Map<String, String>>>() {}.getType());
        assertEquals(2, rows.size(), "a plugin with no row is skipped, as before");
        Map<String, Object> cfg = new Gson().fromJson(rows.get(0).get("configparams"), new TypeToken<Map<String, Object>>() {}.getType());
        assertEquals(ConfigRedaction.REDACTED, cfg.get("gfs_secret"));
        assertEquals(ConfigRedaction.REDACTED, cfg.get("hsmPin"));
        assertEquals("index", cfg.get("gfs_roles"), "non-secret settings stay");
        assertEquals("io.cresco.gfs", rows.get(0).get("pluginname"));
        assertEquals("{}", rows.get(1).get("configparams"), "unreadable config is withheld");
        assertTrue(db.get("plugin/1").get("configparams").contains("s3cr3t-A"), "the agent's own row is untouched");
    }
}
