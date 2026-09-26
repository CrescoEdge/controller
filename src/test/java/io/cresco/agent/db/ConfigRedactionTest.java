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

    @Test
    void pipelineRepliesCarryNoSecretValuesAtAnyDepth() {
        String gpipeline = "{\"pipeline_id\":\"resource-1\",\"pipeline_name\":\"gfs\",\"nodes\":["
                + "{\"type\":\"dummy\",\"node_name\":\"idx\",\"node_id\":\"n0\",\"params\":{\"pluginname\":\"io.cresco.gfs\","
                + "\"gfs_secret\":\"s3cr3t-value\",\"core_master_key_file\":\"/k/m.key\",\"hsmPin\":\"1234\","
                + "\"configparams\":\"{\\\"db_password\\\":\\\"hunter2\\\",\\\"site_id\\\":\\\"s1\\\"}\"}}],"
                + "\"edges\":[{\"edge_id\":\"e0\",\"node_from\":\"n0\",\"node_to\":\"n1\",\"extra\":{\"api_token\":\"tok\"}}]}";
        String out = ConfigRedaction.redactPipelineJson(gpipeline);
        for (String secret : new String[]{"s3cr3t-value", "1234", "hunter2", "\"tok\""}) assertFalse(out.contains(secret), secret + " in " + out);
        for (String kept : new String[]{"io.cresco.gfs", "/k/m.key", "resource-1", "n0", "s1", "e0"}) assertTrue(out.contains(kept), kept + " lost from " + out);
        assertEquals(out, ConfigRedaction.redactPipelineJson(out), "stable");
        assertEquals("{}", ConfigRedaction.redactPipelineJson("{\"nodes\":["));
        assertEquals("{}", ConfigRedaction.redactPipelineJson("[1,2]"));
        assertNull(ConfigRedaction.redactPipelineJson(null));
    }

    @Test
    void anINodeStatusMapHasItsParamsRedacted() {
        Map<String, String> inode = new HashMap<>();
        inode.put("inode_id", "i1");
        inode.put("status_code", "10");
        inode.put("params", "{\"pluginname\":\"io.cresco.gfs\",\"gfs_secret\":\"s3cr3t-value\"}");
        Map<String, String> out = ConfigRedaction.redactINode(inode);
        assertFalse(out.get("params").contains("s3cr3t-value"));
        assertTrue(out.get("params").contains("io.cresco.gfs"));
        assertEquals("i1", out.get("inode_id"));
        assertTrue(inode.get("params").contains("s3cr3t-value"), "the global database row is untouched");
        inode.put("params", "not json gfs_secret=abc");
        assertEquals("{}", ConfigRedaction.redactINode(inode).get("params"));
    }

    // activemq_client_transport_options is not a secret-looking key, but its value is raw ActiveMQ transport
    // options appended to every broker URI, so a keystore password put there must not leave in an export,
    // a reply, or a log line (CLoggerImpl runs every message and throwable through redactText/redactThrowable).
    static final String OPTS = "keyStorePassword=ks-pw-1&trustStorePassword=ts-pw-2&tcpNoDelay=true&password=pw-3"
            + "&jms.clientSecret=sec-4&keyStoreKeyPassword=kk-5&wireFormat.maxInactivityDuration=30000";
    static final String[] OPT_SECRETS = {"ks-pw-1", "ts-pw-2", "pw-3", "sec-4", "kk-5"};

    /** The option string inside a redacted configparams JSON (Gson escapes '=' and '&' in the raw text). */
    static String optsIn(String json) {
        for (String secret : OPT_SECRETS) assertFalse(json.contains(secret), secret + " in " + json);
        Map<String, Object> m = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
        return (String) m.get("activemq_client_transport_options");
    }

    static void assertNoOptSecrets(String s) {
        for (String secret : OPT_SECRETS) assertFalse(s.contains(secret), secret + " in " + s);
        assertTrue(s.contains("tcpNoDelay=true"), "non-secret options kept: " + s);
        assertTrue(s.contains("wireFormat.maxInactivityDuration=30000"), "non-secret options kept: " + s);
        assertTrue(s.contains("keyStorePassword=" + ConfigRedaction.REDACTED), s);
    }

    @Test
    void transportOptionValuesAreRedactedInEveryExportAndReply() {
        assertFalse(ConfigRedaction.isSecretKey("activemq_client_transport_options"), "the key itself is not secret-looking");

        // a flat config map (region/plugin replies)
        Map<String, String> cfg = new HashMap<>(Map.of("activemq_client_transport_options", OPTS, "activemq_transport", "nio+ssl"));
        Map<String, String> red = ConfigRedaction.redactMap(cfg);
        assertNoOptSecrets(red.get("activemq_client_transport_options"));
        assertEquals("nio+ssl", red.get("activemq_transport"));
        assertEquals(OPTS, cfg.get("activemq_client_transport_options"), "the agent's own config is untouched");

        // the agent's configparams JSON (watchdog/state export), and an exported row carrying it
        String json = GSON.toJson(Map.of("activemq_client_transport_options", OPTS, "pluginname", "io.cresco.agent"));
        String out = ConfigRedaction.redactJson(json);
        assertNoOptSecrets(optsIn(out));
        assertTrue(out.contains("io.cresco.agent"));
        assertEquals(out, ConfigRedaction.redactJson(out), "stable: the diff-gated export does not churn");
        Map<String, String> row = new HashMap<>(Map.of("agent_id", "a1", "configparams", json, "note", "brokerURL=x?password=pw-3"));
        Map<String, String> rowOut = ConfigRedaction.redactNode(row);
        assertNoOptSecrets(optsIn(rowOut.get("configparams")));
        assertFalse(rowOut.get("note").contains("pw-3"));
        assertEquals("a1", rowOut.get("agent_id"));

        // a pipeline document: an option string at depth, in an object and in an array
        String pipe = "{\"pipeline_id\":\"p1\",\"nodes\":[{\"params\":{\"pluginname\":\"x\",\"opts\":\"" + OPTS
                + "\"}}],\"extra\":[\"" + OPTS + "\"]}";
        String pipeOut = ConfigRedaction.redactPipelineJson(pipe);
        for (String secret : OPT_SECRETS) assertFalse(pipeOut.contains(secret), secret + " in " + pipeOut);
        assertTrue(pipeOut.contains("tcpNoDelay\\u003dtrue") || pipeOut.contains("tcpNoDelay=true"), pipeOut);
        assertEquals(pipeOut, ConfigRedaction.redactPipelineJson(pipeOut), "stable");

        // an iNode status map
        Map<String, String> inode = new HashMap<>(Map.of("inode_id", "i1", "params", json));
        assertNoOptSecrets(optsIn(ConfigRedaction.redactINode(inode).get("params")));
    }

    @Test
    void aBrokerUriInALogLineKeepsItsShapeButNotItsSecrets() {
        String uri = "failover:(nio+ssl://10.0.0.1:32010?verifyHostName=false&socketBufferSize=0&" + OPTS
                + ",nio+ssl://10.0.0.2:32010?trustStorePassword=ts-pw-2)?maxReconnectAttempts=5&initialReconnectDelay=5000";
        String line = ConfigRedaction.redactText("Connection to URI [" + uri + "] started successfully.");
        assertNoOptSecrets(line);
        for (String kept : new String[]{"nio+ssl://10.0.0.1:32010?verifyHostName=false&socketBufferSize=0&",
                ",nio+ssl://10.0.0.2:32010?trustStorePassword=" + ConfigRedaction.REDACTED + ")",
                "?maxReconnectAttempts=5&initialReconnectDelay=5000] started successfully."})
            assertTrue(line.contains(kept), kept + " lost from " + line);
        assertEquals(line, ConfigRedaction.redactText(line), "stable");

        // other separators and secret-looking names: JDBC ';', a service key, a token, a PIN, whitespace
        String misc = ConfigRedaction.redactText("jdbc:derby:db;user=cresco;password=hunter2 cresco_service_key=k-6 api_token=t-7 hsm_pin=1234 port=8282");
        for (String secret : new String[]{"hunter2", "k-6", "t-7", "1234"}) assertFalse(misc.contains(secret), secret + " in " + misc);
        assertTrue(misc.contains("user=cresco") && misc.contains("port=8282"), misc);

        // nothing to redact: the same instance back (the logger's fast path)
        String plain = "socketBufferSize=0&tcpNoDelay=true ping_interval=5 mapping=a";
        assertSame(plain, ConfigRedaction.redactText(plain));
        assertNull(ConfigRedaction.redactText(null));
        assertEquals("password=", ConfigRedaction.redactText("password="), "an empty value has nothing to hide");
    }

    @Test
    void aThrowableCarryingABrokerUrlIsLoggedRedacted() {
        Exception root = new java.net.ConnectException("Connection refused");
        Exception e = new IllegalStateException("Could not connect to broker URL: nio+ssl://h:32010?" + OPTS + ". Reason: " + root, root);
        Throwable r = ConfigRedaction.redactThrowable(e);
        assertNotSame(e, r);
        java.io.StringWriter sw = new java.io.StringWriter();
        r.printStackTrace(new java.io.PrintWriter(sw));
        String printed = sw.toString();
        for (String secret : OPT_SECRETS) assertFalse(printed.contains(secret), secret + " in " + printed);
        assertTrue(printed.startsWith("java.lang.IllegalStateException: Could not connect to broker URL: nio+ssl://h:32010?"), printed);
        assertTrue(printed.contains("Caused by: java.net.ConnectException: Connection refused"), printed);
        assertArrayEquals(e.getStackTrace(), r.getStackTrace(), "the stack trace is kept");

        // a secret only in the cause
        Throwable r2 = ConfigRedaction.redactThrowable(new RuntimeException("connect failed", new IllegalArgumentException("Invalid connect parameters: {trustStorePassword=ts-pw-2}")));
        java.io.StringWriter sw2 = new java.io.StringWriter();
        r2.printStackTrace(new java.io.PrintWriter(sw2));
        assertFalse(sw2.toString().contains("ts-pw-2"), sw2.toString());
        assertTrue(sw2.toString().contains("java.lang.RuntimeException: connect failed"));

        // a clean throwable passes through unchanged; null stays null; a looping cause chain terminates
        Exception clean = new RuntimeException("socketBufferSize=0");
        assertSame(clean, ConfigRedaction.redactThrowable(clean));
        assertNull(ConfigRedaction.redactThrowable(null));
        Exception a = new RuntimeException("a"), b = new RuntimeException("b password=pw-3", a);
        a.initCause(b);
        assertFalse(ConfigRedaction.redactThrowable(a).getCause().toString().contains("pw-3"));
    }
}
