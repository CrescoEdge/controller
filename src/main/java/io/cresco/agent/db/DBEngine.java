package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.data.DataPlaneLogger;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;


public class DBEngine {

    private DataSource ds;
    private Gson gson;
    private Type type;
    private DBType dbType = DBType.EMBEDDED;
    private PoolableConnectionFactory poolableConnectionFactory;
    private ObjectPool<PoolableConnection> connectionPool;
    private PoolingDataSource<PoolableConnection> dataSource;

    private CLogger logger;

    private List<String> tablesNames;

    private PluginBuilder pluginBuilder;

    private String dbPath;

    public DBEngine(PluginBuilder plugin) {

        try {

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {
                Path path = Paths.get(cresco_data_location, "derbydb-home");
                System.setProperty("derby.system.home",path.toAbsolutePath().normalize().toString());
            } else {
                System.setProperty("derby.system.home", new File("cresco-data/derbydb-home/").getAbsolutePath());
            }

            //to remove derby.log file
            //System.setProperty("derby.stream.error.field", "DerbyUtil.DEV_NULL");
            //to set the location of the derby.log file
            //System.setProperty("derby.stream.error.file","cresco-data/derby-log/derby.log");

            this.pluginBuilder = plugin;
            this.logger = pluginBuilder.getLogger(DBEngine.class.getName(),CLogger.Level.Info);

            logger.info("Init DB");

            tablesNames = new ArrayList<>();
            tablesNames.add("inodekpi");
            tablesNames.add("vnode");
            tablesNames.add("inode");
            tablesNames.add("resourcenode");
            tablesNames.add("tenantnode");
            tablesNames.add("pluginof");
            tablesNames.add("pnode");
            tablesNames.add("agentof");
            tablesNames.add("anode");
            tablesNames.add("rnode");

            this.gson = new Gson();
            this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
            }.getType();


            String defaultDBName = "cresco-controller-db";
            String dbName = plugin.getConfig().getStringParam("db_name", defaultDBName);

            dbPath = plugin.getPluginDataDirectory() + "/derbydb-home/" + dbName;

            String dbDriver = plugin.getConfig().getStringParam("db_driver", "org.apache.derby.jdbc.EmbeddedDriver");

            //String dbDriver = plugin.getConfig().getStringParam("db_driver","org.hsqldb.jdbcDriver");
            //if (dbDriver.contains("mysql")) {
            //    dbType = DBType.MYSQL;
            //}


            String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc", "jdbc:derby:" + dbPath + ";create=true");
            //String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc","jdbc:hsqldb:" + "database/" + dbName + ";create=true");

            String dbUserName = plugin.getConfig().getStringParam("db_username");
            String dbPassword = plugin.getConfig().getStringParam("db_password");

            //org.apache.derby.jdbc.EmbeddedDriver embeddedDriver = new EmbeddedDriver();
            //logger.info("Init DB 0.6 Class " + embeddedDriver.getClass().toString());

            Class.forName(dbDriver).newInstance();

            if ((dbUserName != null) && (dbPassword != null)) {
                ds = setupDataSource(dbConnectionString, dbUserName, dbPassword);
            } else {
                ds = setupDataSource(dbConnectionString);
            }

            //Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            //ds = setupDataSource("jdbc:derby:demo;create=true");

            //Class.forName("com.mysql.cj.jdbc.Driver");
            //ds = setupDataSource("jdbc:mysql://localhost/cresco?characterEncoding=UTF-8","root", "nopassword");

            if (dbType == DBType.EMBEDDED) {

                if (dbName.equals(defaultDBName)) {
                    File dbsource = Paths.get(dbPath).toFile();
                    //File dbsource = new File(defaultDBName);
                    if (dbsource.exists()) {
                        //delete(dbsource);
                    } else {
                        //dbsource.mkdir();
                        initDB();
                        addTenant(0, "default tenant");
                    }
                }
            }


            if (dbType == DBType.MYSQL) {
                if (!checkSchema()) {
                    initDB();

                    addTenant(0, "default tenant");
                }
            }


            /*
            if (!checkSchema()) {


                initDB();

            addTenant(0, "default tenant");
            }
            */

            //do something here to clean up schema

            //addResource("sysinfo_resource","Performance Metrics",0,0,"added by DBEngine by default", null);

            //Class.forName("com.mysql.cj.jdbc.Driver");
            //ds = setupDataSource("jdbc:mysql://localhost/cresco?characterEncoding=UTF-8","root", "nopassword");


            /*
            ds = new BasicDataSource();
            ds.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
            ds.setUrl("jdbc:derby:demo;create=true");
            */

        } catch (Exception ex) {
            if(logger != null) {
                logger.error("DBEngine.DBEngine()", ex);
            } else {
                ex.printStackTrace();
            }
        }
    }

    public boolean shutdown() {
        boolean isShutdown = false;
        try {

            //shutdown connections
            dataSource.close();
            connectionPool.close();

            try {
                //String shutdownString = "jdbc:derby:" + dbPath + ";shutdown=true";
                //String dropString = "jdbc:derby:" + dbPath + ";drop=true";
                //String dropString = "jdbc:derby:" + dbPath + ";drop=true";
                //DriverManager.getConnection(dropString);
                //DriverManager.getConnection("jdbc:derby:memory:eh;drop=true");
                DriverManager.getConnection("jdbc:derby:;shutdown=true");

            } catch (SQLException e) {
                if (e.getErrorCode() == 50000) {
                    isShutdown = true;

                } else if (e.getErrorCode() == 45000) {
                    isShutdown = true;

                }
                else {
                    logger.error("DBEngine.shutdown() SQL error code: {}", e.getErrorCode(), e);
                }
            }

            //unload drivers
            //DriverManager.getConnection("jdbc:derby:;shutdown=true");

            // Deregister every registered Derby JDBC driver so the engine can unload cleanly.
            // Enumerate rather than instantiating named driver classes: Derby 10.15+ reorganized
            // the org.apache.derby.jdbc.* layout (AutoloadedDriver is no longer a public class).
            Enumeration<Driver> derbyDrivers = DriverManager.getDrivers();
            while (derbyDrivers.hasMoreElements()) {
                Driver drv = derbyDrivers.nextElement();
                if (drv.getClass().getName().startsWith("org.apache.derby.")) {
                    try { DriverManager.deregisterDriver(drv); }
                    catch (SQLException sqle) { logger.error("DBEngine.shutdown() driver deregistration", sqle); }
                }
            }

        }
        catch (Exception ex) {
            logger.error("DBEngine.shutdown()", ex);
        }
        return isShutdown;
    }

    public boolean checkSchema() {
        boolean isOk = true;
        try {
            List<String> existingTables = new ArrayList<>();

            try (Connection conn = ds.getConnection()) {
                DatabaseMetaData md = conn.getMetaData();

                ResultSet rs = md.getTables(null, null, "%", null);
                while (rs.next()) {
                    existingTables.add(rs.getString(3).toLowerCase());
                    //System.out.println(rs.getString(3));
                }

                rs.close();
                conn.close();

                for (String table : tablesNames) {
                    if (!existingTables.contains(table)) {
                        //System.out.println("TABLE DOES NOT EXIST: " + table);
                        return false;
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("DBEngine.checkSchema() Schema is invalid", ex);
        }
        return isOk;
    }

    // Columns of pnode that remote "list plugins by type" requests may filter on. The column
    // name arrives over the wire (action_plugintype_id) and JDBC cannot bind an identifier as
    // a parameter, so getPluginListMapByType() validates it against this hardcoded allowlist.
    private static final Set<String> PLUGIN_TYPE_COLUMNS = new HashSet<>(Arrays.asList(
            "plugin_id", "status_desc", "pluginname", "jarfile", "version", "md5"));

    // Tables this class itself creates and drops. dropTable() concatenates the name into DDL,
    // which cannot be parameterized, so it refuses anything outside this hardcoded allowlist.
    private static final Set<String> MANAGED_TABLES = new HashSet<>(Arrays.asList(
            "inodekpi", "vnode", "inode", "resourcenode", "tenantnode", "cstate",
            "pluginof", "pnode", "aconfig", "agentof", "anode", "rnode"));

    // Statement-era string concatenation rendered a null Java String as the four-character
    // literal "null" inside SQL quotes. Preserve that exact stored/matched value under
    // PreparedStatement parameters so the injection fix changes no observable behavior.
    private static String textParam(String value) {
        return value == null ? "null" : value;
    }

    // Binds positional parameters with typed setters for the multi-branch query builders.
    private static void bindParams(PreparedStatement stmt, List<Object> queryParams) throws SQLException {
        for (int i = 0; i < queryParams.size(); i++) {
            Object queryParam = queryParams.get(i);
            if (queryParam instanceof Integer) {
                stmt.setInt(i + 1, (Integer) queryParam);
            } else if (queryParam instanceof Long) {
                stmt.setLong(i + 1, (Long) queryParam);
            } else {
                stmt.setString(i + 1, textParam((String) queryParam));
            }
        }
    }

    public boolean nodeUpdateStatus(String mode, String region_watchdog_update, String agent_watchdog_update, String plugin_watchdog_update, String regionconfigs, String agentconfigs, String pluginconfigs) {

        // Start pessimistic and report success only when every entry applied cleanly. Callers use
        // this to answer agent_enable (is_registered): the old always-true return told a
        // re-registering agent it was registered while its regional rows stayed LOST.
        boolean isUpdated = false;
        boolean entryFailed = false;

        boolean isRegionUpdate = false;
        boolean isAgentUpdate = false;

        try {

            if(mode.equals("REGION")) {
                isRegionUpdate = true;
            } else if(mode.equals("AGENT")) {
                isAgentUpdate = true;
            }


            if(region_watchdog_update != null) {
                updateWatchDogTS(region_watchdog_update, null, null);
            }

            if(agent_watchdog_update != null) {
                updateWatchDogTS(null, agent_watchdog_update, null);
            }

            if(plugin_watchdog_update != null) {
                updateWatchDogTS(null, null, plugin_watchdog_update);
            }

            if(regionconfigs != null) {

                Map<String,List<Map<String,String>>> regionConfigMap = gson.fromJson(regionconfigs,type);
                for (Map.Entry<String, List<Map<String,String>>> entry : regionConfigMap.entrySet()) {

                    List<Map<String, String>> regionList = entry.getValue();

                    for(Map<String,String> regionMap : regionList) {
                        String region_id = regionMap.get("region_id");
                        // per-entry guard + try/catch: one malformed entry (e.g. nulls from a
                        // defective peer export) must not abort the rest of the batch
                        try {
                            String status_code = regionMap.get("status_code");
                            String status_desc = regionMap.get("status_desc");
                            String watchdog_period = regionMap.get("watchdog_period");
                            //String watchdog_ts = agentMap.get("watchdog_ts");
                            String configparams = regionMap.get("configparams");

                            if(region_id == null || status_code == null || watchdog_period == null) {
                                logger.warn("nodeUpdateStatus() skipping malformed region entry: {}", regionMap);
                                entryFailed = true;
                                continue;
                            }

                            if(!nodeExist(region_id,null,null)) {

                                //logger.debug("addNodeFromUpdate add [" + de.getParams() + "]");
                                addRNode(region_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);

                            } else {
                                //logger.debug("addNodeFromUpdate update [" + de.getParams() + "]");
                                updateNode(region_id,null,null,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);
                            }
                        } catch (Exception ex) {
                            logger.error("nodeUpdateStatus() region entry failed for region_id: " + region_id, ex);
                            entryFailed = true;
                        }

                    }
                }

            }

            if(agentconfigs != null) {

                Map<String,List<Map<String,String>>> agentConfigMap = gson.fromJson(agentconfigs,type);

                for (Map.Entry<String, List<Map<String,String>>> entry : agentConfigMap.entrySet()) {
                    String region_id = entry.getKey();

                    List<String> removeAgentList = null;

                    if(isRegionUpdate) {
                        removeAgentList = getNodeList(region_id,null);
                    }

                    List<Map<String,String>> agentList = entry.getValue();
                    for(Map<String,String> agentMap : agentList) {
                        String agent_id = agentMap.get("agent_id");
                        try {
                            // a known agent must never be swept into the not-in-update removal
                            // below just because its entry was malformed
                            if(isRegionUpdate && agent_id != null) {
                                removeAgentList.remove(agent_id);
                            }

                            String status_code = agentMap.get("status_code");
                            String status_desc = agentMap.get("status_desc");
                            String watchdog_period = agentMap.get("watchdog_period");
                            //String watchdog_ts = agentMap.get("watchdog_ts");
                            String configparams = agentMap.get("configparams");

                            if(agent_id == null || status_code == null || watchdog_period == null) {
                                logger.warn("nodeUpdateStatus() skipping malformed agent entry: {}", agentMap);
                                entryFailed = true;
                                continue;
                            }

                            if(!nodeExist(region_id,agent_id,null)) {

                                //logger.debug("addNodeFromUpdate add [" + de.getParams() + "]");
                                addANode(agent_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);

                            } else {
                                //logger.debug("addNodeFromUpdate update [" + de.getParams() + "]");
                                updateNode(region_id,agent_id,null,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);
                            }

                            if(!assoicateANodetoRNodeExist(region_id,agent_id)) {
                                assoicateANodetoRNode(region_id, agent_id);
                            }
                        } catch (Exception ex) {
                            logger.error("nodeUpdateStatus() agent entry failed for agent_id: " + agent_id, ex);
                            entryFailed = true;
                        }

                    }

                    //remove any agents not in the update
                    if(removeAgentList != null) {
                        for (String agent_id : removeAgentList) {
                            removeNode(region_id, agent_id, null);
                        }
                    }

                }
            }

            if(pluginconfigs != null) {

                Map<String,List<Map<String,String>>> pluginConfigMap = gson.fromJson(pluginconfigs,type);

                for (Map.Entry<String, List<Map<String,String>>> entry : pluginConfigMap.entrySet()) {

                    String agent_id = entry.getKey();

                    String region_id = getRNodeFromAnode(agent_id);

                    if(region_id != null) {

                    List<String> removePluginList = getNodeList(region_id,agent_id);

                    List<Map<String,String>> pluginList = entry.getValue();

                    for(Map<String,String> pluginMap : pluginList) {


                        String plugin_id = pluginMap.get("plugin_id");
                        try {
                            // a known plugin must never be swept into the not-in-update removal
                            // below just because its entry was malformed
                            if(plugin_id != null) {
                                removePluginList.remove(plugin_id);
                            }

                            String status_code = pluginMap.get("status_code");
                            String status_desc = pluginMap.get("status_desc");
                            String watchdog_period = pluginMap.get("watchdog_period");
                            //String watchdog_ts = pluginMap.get("watchdog_ts");
                            String pluginname = pluginMap.get("pluginname");
                            String jarfile = pluginMap.get("jarfile");
                            String version = pluginMap.get("version");
                            String md5 = pluginMap.get("md5");
                            String configparams = pluginMap.get("configparams");
                            String persistence_code = pluginMap.get("persistence_code");

                            if(plugin_id == null || status_code == null || watchdog_period == null) {
                                logger.warn("nodeUpdateStatus() skipping malformed plugin entry: {}", pluginMap);
                                entryFailed = true;
                                continue;
                            }

                            if(!nodeExist(null,null, plugin_id)) {
                                int status = addPNode(agent_id,plugin_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),pluginname,jarfile,version,md5,configparams,Integer.parseInt(persistence_code));
                            } else {
                                updateNode(null, null, plugin_id, Integer.parseInt(status_code), status_desc, Integer.parseInt(watchdog_period), System.currentTimeMillis(), configparams);
                            }

                            if(!assoicatePNodetoANodeExist(agent_id,plugin_id)) {
                                assoicatePNodetoANode(agent_id,plugin_id);
                            }
                        } catch (Exception ex) {
                            logger.error("nodeUpdateStatus() plugin entry failed for plugin_id: " + plugin_id, ex);
                            entryFailed = true;
                        }

                    }

                    //remove any plugins not in the update
                    for(String plugin_id : removePluginList) {
                        removeNode(region_id,agent_id,plugin_id);
                    }

                    } else {
                        logger.error("nodeUpdateStatus() WHY DOES AGENT: {} HAVE NO REGION!", agent_id, new Throwable());
                    }

                }

            }

            isUpdated = !entryFailed;

        } catch (Exception ex) {
            logger.error("DBEngine.nodeUpdateStatus()", ex);
            isUpdated = false;
        }
        return isUpdated;
    }

    public void updateNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {

            String stmtString = null;
            String nodeId = null;

            if (((region != null) && (agent != null) && (plugin != null)) || ((region == null) && (agent == null) && (plugin != null))) {
                //add plugin metadata where it exist

                String pluginname = "unknown";
                String jarfile = "unknown";
                String version = "unknown";
                String md5 = "unknown";

                if (configparams != null) {
                    Type type = new TypeToken<Map<String, String>>() {
                    }.getType();
                    Map<String, String> configMap = gson.fromJson(configparams, type);

                    if (configMap.containsKey("pluginname")) {
                        pluginname = configMap.get("pluginname");
                    }
                    if (configMap.containsKey("jarfile")) {
                        jarfile = configMap.get("jarfile");
                    }
                    if (configMap.containsKey("version")) {
                        version = configMap.get("version");
                    }
                    if (configMap.containsKey("md5")) {
                        md5 = configMap.get("md5");
                    }

                }

                stmtString = "UPDATE pnode SET status_code=?, status_desc=?, watchdog_period=?" +
                        ", watchdog_ts=?, configparams=? " +
                        "WHERE plugin_id=?";
                nodeId = plugin;

            } else if (((region != null) && (agent != null) && (plugin == null)) || ((region == null) && (agent != null) && (plugin == null))) {
                stmtString = "UPDATE anode SET status_code=?, status_desc=?, watchdog_period=?" +
                        ", watchdog_ts=?, configparams=? " +
                        "WHERE agent_id=?";
                nodeId = agent;

            } else if ((region != null) && (agent == null) && (plugin == null)) {
                stmtString = "UPDATE rnode SET status_code=?, status_desc=?, watchdog_period=?" +
                        ", watchdog_ts=?, configparams=? " +
                        "WHERE region_id=?";
                nodeId = region;
            }

            try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                stmt.setInt(1, status_code);
                stmt.setString(2, textParam(status_desc));
                stmt.setInt(3, watchdog_period);
                stmt.setLong(4, watchdog_ts);
                stmt.setString(5, textParam(configparams));
                stmt.setString(6, textParam(nodeId));

                stmt.executeUpdate();
                stmt.close();
            }

            conn.close();
        } catch(Exception ex) {
            logger.error("DBEngine.updateNode()", ex);
        }

    }

    public Map<String,String> getDBExport(boolean regions, boolean agents, boolean plugins, String region_id, String agent_id, String plugin_id) {

        Map<String,String> exportMap = null;

        try {

            exportMap = new HashMap<>();


            if(regions) {

                Map<String, List<Map<String, String>>> regionMap = new HashMap<>();
                List<Map<String, String>> regionList = new ArrayList<>();
                List<String> tmpRegionList = null;
                if(region_id == null) {
                    tmpRegionList = getNodeList(null,null);
                } else {
                    tmpRegionList = new ArrayList<>();
                    tmpRegionList.add(region_id);
                }

                for(String tmp_region_id : tmpRegionList) {
                    Map<String, String> rNode = getRNodeStrict(tmp_region_id);
                    // skip rows that vanished between the list query and the node fetch — a null
                    // (or formerly nulls-filled) entry poisons the peer's nodeUpdateStatus
                    if (rNode != null) {
                        regionList.add(rNode);
                    }
                }
                regionMap.put(pluginBuilder.getRegion(), regionList);

                exportMap.put("regionconfigs",gson.toJson(regionMap));

            }

            if(agents) {

                Map<String, List<Map<String, String>>> agentMap = new HashMap<>();

                if((region_id != null) && (agent_id != null)) {
                    List<Map<String, String>> agentList = new ArrayList<>();
                    Map<String, String> aNode = getANodeStrict(agent_id);
                    if (aNode != null) {
                        agentList.add(aNode);
                    }
                    agentMap.put(region_id, agentList);

                } else {

                    List<String> tmpRegionList = getNodeList(null,null);
                    for(String tmp_region_id : tmpRegionList) {
                        List<Map<String, String>> agentList = new ArrayList<>();
                        List<String> tmpAgentList = getNodeList(tmp_region_id, null);
                        for(String tmp_agent_id : tmpAgentList) {
                            Map<String, String> aNode = getANodeStrict(tmp_agent_id);
                            if (aNode != null) {
                                agentList.add(aNode);
                            }
                        }
                        agentMap.put(tmp_region_id, agentList);
                    }

                }
                exportMap.put("agentconfigs",gson.toJson(agentMap));

            }

            if(plugins) {

                Map<String, List<Map<String, String>>> pluginMap = new HashMap<>();

                if((region_id != null) && (agent_id != null)) {

                    List<Map<String, String>> pluginList = new ArrayList<>();
                    List<String> tmpPluginList = getNodeList(region_id, agent_id);
                    for (String pluginId : tmpPluginList) {
                        Map<String, String> pNode = getPNodeStrict(pluginId);
                        if (pNode != null) {
                            pluginList.add(pNode);
                        }
                    }
                    pluginMap.put(agent_id, pluginList);
                } else {

                    List<String> tmpRegionList = getNodeList(null,null);
                    for(String tmp_region_id : tmpRegionList) {
                        List<String> tmpAgentList = getNodeList(tmp_region_id, null);
                        for(String tmp_agent_id : tmpAgentList) {
                            List<String> tmpPluginList = getNodeList(tmp_region_id, tmp_agent_id);
                            List<Map<String, String>> pluginList = new ArrayList<>();
                            for(String tmp_plugin_id : tmpPluginList) {
                                Map<String, String> pNode = getPNodeStrict(tmp_plugin_id);
                                if (pNode != null) {
                                    pluginList.add(pNode);
                                }
                            }
                            pluginMap.put(tmp_agent_id, pluginList);
                        }
                    }
                }
                exportMap.put("pluginconfigs",gson.toJson(pluginMap));
            }

        } catch (Exception ex) {
            // Return null on ANY error: a partial export shipped upstream makes the peer's
            // not-in-update sweep delete live nodes. Callers skip the tick / retry instead.
            logger.error("DBEngine.getDBExport()", ex);
            exportMap = null;
        }


        return exportMap;
    }

    public void addCStateEvent(long config_ts, String current_mode, String current_desc, String global_region, String global_agent, String regional_region, String regional_agent, String local_region, String local_agent) {

        try (Connection conn = ds.getConnection()) {

            String stmtString = "INSERT INTO cstate values (?,?,?,?,?,?,?,?,?)";

            try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                stmt.setLong(1, config_ts);
                stmt.setString(2, textParam(current_mode));
                stmt.setString(3, textParam(current_desc));
                stmt.setString(4, textParam(global_region));
                stmt.setString(5, textParam(global_agent));
                stmt.setString(6, textParam(regional_region));
                stmt.setString(7, textParam(regional_agent));
                stmt.setString(8, textParam(local_region));
                stmt.setString(9, textParam(local_agent));

                stmt.executeUpdate();
                stmt.close();
            }

            conn.close();

        } catch(Exception ex) {
            logger.error("DBEngine.addCStateEvent()", ex);
        }

    }

    public void updateRNode(String region, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {

            String stmtString = "UPDATE rnode SET status_code=?, status_desc=?, watchdog_period=?" +
                    ", watchdog_ts=?, configparams=? " +
                    "WHERE region_id=?";

            try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                stmt.setInt(1, status_code);
                stmt.setString(2, textParam(status_desc));
                stmt.setInt(3, watchdog_period);
                stmt.setLong(4, watchdog_ts);
                stmt.setString(5, textParam(configparams));
                stmt.setString(6, textParam(region));

                stmt.executeUpdate();
                stmt.close();
            }

            conn.close();
        } catch(Exception ex) {
            logger.error("DBEngine.updateRNode()", ex);
        }

    }

    public void updateANode(String agent, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {

            String stmtString = "UPDATE anode SET status_code=?, status_desc=?, watchdog_period=?" +
                    ", watchdog_ts=?, configparams=? " +
                    "WHERE agent_id=?";

            try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                stmt.setInt(1, status_code);
                stmt.setString(2, textParam(status_desc));
                stmt.setInt(3, watchdog_period);
                stmt.setLong(4, watchdog_ts);
                stmt.setString(5, textParam(configparams));
                stmt.setString(6, textParam(agent));

                stmt.executeUpdate();
                stmt.close();
            }

            conn.close();
        } catch(Exception ex) {
            logger.error("DBEngine.updateANode()", ex);
        }

    }

    public List<String> getVNodeResourceList(String resourceId) {
        List<String> inodeResourceList = null;

        try {

            inodeResourceList = new ArrayList<>();

            String queryString = "SELECT vnode_id FROM vnode WHERE resource_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resourceId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        while (rs.next()) {
                            String node = rs.getString(1);
                            if (!inodeResourceList.contains(node)) {
                                inodeResourceList.add(node);
                            }
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getVNodeResourceList()", ex);
        }
        return inodeResourceList;
    }

    public List<String> getINodeResourceList(String resourceId) {
        List<String> inodeResourceList = null;

        try {

            inodeResourceList = new ArrayList<>();

            String queryString = "SELECT inode_id FROM inode WHERE resource_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resourceId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        while (rs.next()) {
                            String node = rs.getString(1);
                            if (!inodeResourceList.contains(node)) {
                                inodeResourceList.add(node);
                            }
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getINodeResourceList()", ex);
        }
        return inodeResourceList;
    }

    public int getINodeStatus(String inodeId) {
        int status_code = -1;
        try {

            String queryString = "SELECT status_code FROM inode WHERE inode_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(inodeId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        rs.next();
                        status_code = rs.getInt(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getINodeStatus()", ex);
        }
        return status_code;
    }

    public int setINodeStatusCode(String inodeId, int status_code, String status_desc) {
        int queryReturn = -1;
        try {

            String queryString = "UPDATE inode SET status_code=?, status_desc=?"
                    + " WHERE inode_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setInt(1, status_code);
                    stmt.setString(2, textParam(status_desc));
                    stmt.setString(3, textParam(inodeId));

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.setINodeStatusCode()", ex);
        }
        return queryReturn;
    }

    public int getPNodePersistenceCode(String pluginId) {
        int status_code = -1;
        try {

            String queryString = "SELECT persistence_code FROM pnode " +
                    "WHERE plugin_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(pluginId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        rs.next();
                        status_code = rs.getInt(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getPNodePersistenceCode()", ex);
        }
        return status_code;
    }

    // The three node getters distinguish "row missing" (null) from "query failed" (throws, strict
    // variants): a map of nulls or an error-as-absent both poison peers — the former NFEs the
    // peer's nodeUpdateStatus, the latter makes its not-in-update sweep delete live nodes.
    // getDBExport uses the strict variants; the lenient public wrappers return null on error for
    // callers where a dropped read is tolerable (health checks, display paths).

    private Map<String,String> getPNodeStrict(String pluginId) throws Exception {
        Map<String,String> pNodeMap = null;

        String queryString = "SELECT * FROM pnode " +
                "WHERE plugin_id=?";

        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                stmt.setString(1, textParam(pluginId));

                try(ResultSet rs = stmt.executeQuery()) {

                    if (rs.next()) {
                        pNodeMap = new HashMap<>();
                        pNodeMap.put("plugin_id", rs.getString("plugin_id"));
                        pNodeMap.put("status_code", rs.getString("status_code"));
                        pNodeMap.put("status_desc", rs.getString("status_desc"));
                        pNodeMap.put("watchdog_period", rs.getString("watchdog_period"));
                        //pNodeMap.put("watchdog_ts", rs.getString("watchdog_ts"));
                        pNodeMap.put("pluginname", rs.getString("pluginname"));
                        pNodeMap.put("version", rs.getString("version"));
                        pNodeMap.put("jarfile", rs.getString("jarfile"));
                        pNodeMap.put("md5", rs.getString("md5"));
                        pNodeMap.put("configparams", rs.getString("configparams"));
                        pNodeMap.put("persistence_code", rs.getString("persistence_code"));
                    } else {
                        logger.warn("DBEngine.getPNode() no pnode row for plugin_id: {}", pluginId);
                    }
                }
            }
        }
        return pNodeMap;
    }

    public Map<String,String> getPNode(String pluginId) {
        try {
            return getPNodeStrict(pluginId);
        } catch(Exception ex) {
            logger.error("DBEngine.getPNode()", ex);
            return null;
        }
    }

    private Map<String,String> getRNodeStrict(String regionId) throws Exception {
        Map<String,String> aNodeMap = null;

        String queryString = "SELECT * FROM rnode " +
                "WHERE region_id=?";

        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                stmt.setString(1, textParam(regionId));

                try(ResultSet rs = stmt.executeQuery()) {

                    // Missing row -> null, never a map of nulls (see getPNodeStrict).
                    if (rs.next()) {
                        aNodeMap = new HashMap<>();
                        aNodeMap.put("region_id", rs.getString("region_id"));
                        aNodeMap.put("status_code", rs.getString("status_code"));
                        aNodeMap.put("status_desc", rs.getString("status_desc"));
                        aNodeMap.put("watchdog_period", rs.getString("watchdog_period"));
                        //aNodeMap.put("watchdog_ts", rs.getString("watchdog_ts"));
                        aNodeMap.put("configparams", rs.getString("configparams"));
                    } else {
                        logger.warn("DBEngine.getRNode() no rnode row for region_id: {}", regionId);
                    }
                }
            }
        }
        return aNodeMap;
    }

    public Map<String,String> getRNode(String regionId) {
        try {
            return getRNodeStrict(regionId);
        } catch(Exception ex) {
            logger.error("DBEngine.getRNode()", ex);
            return null;
        }
    }

    private Map<String,String> getANodeStrict(String agentId) throws Exception {
        Map<String,String> aNodeMap = null;

        String queryString = "SELECT * FROM anode " +
                "WHERE agent_id=?";

        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                stmt.setString(1, textParam(agentId));

                try(ResultSet rs = stmt.executeQuery()) {

                    // Missing row -> null, never a map of nulls: an empty anode export shipped
                    // over the wire is what NFE'd the regional nodeUpdateStatus and left
                    // re-registering agents stuck LOST (2026-08-15 incident).
                    if (rs.next()) {
                        aNodeMap = new HashMap<>();
                        aNodeMap.put("agent_id", rs.getString("agent_id"));
                        aNodeMap.put("status_code", rs.getString("status_code"));
                        aNodeMap.put("status_desc", rs.getString("status_desc"));
                        aNodeMap.put("watchdog_period", rs.getString("watchdog_period"));
                        //aNodeMap.put("watchdog_ts", rs.getString("watchdog_ts"));
                        aNodeMap.put("configparams", rs.getString("configparams"));
                    } else {
                        logger.warn("DBEngine.getANode() no anode row for agent_id: {}", agentId);
                    }
                }
            }
        }
        return aNodeMap;
    }

    public Map<String,String> getANode(String agentId) {
        try {
            return getANodeStrict(agentId);
        } catch(Exception ex) {
            logger.error("DBEngine.getANode()", ex);
            return null;
        }
    }

    public String getRNodeFromAnode(String agentId) {
        String configParams = null;
        try {

            String queryString = "SELECT region_id FROM agentOf WHERE agent_id = ?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(agentId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        if (rs.next()) {
                            configParams = rs.getString(1);
                        } else {
                            logger.error("getRNodeFromAnode() WHY IS RESULT SET EMPTY THIS NULL: AGENT_ID = " + agentId);
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getRNodeFromAnode()", ex);
        }

        return configParams;
    }

    public String getNodeConfigParams(String regionId, String agentId, String pluginId) {
        String configParams = null;
        try {

            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT P.configparams FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND P.PLUGIN_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID";
                queryParams.add(regionId);
                queryParams.add(agentId);
                queryParams.add(pluginId);

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT A.configparams FROM ANODE A, RNODE R, AGENTOF O " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = O.REGION_ID " +
                        "AND O.AGENT_ID = A.AGENT_ID ";
                queryParams.add(regionId);
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT configparams " + "FROM rnode " +
                        "WHERE region_id = ?";
                queryParams.add(regionId);
            }

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        rs.next();
                        configParams = rs.getString(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getNodeConfigParams()", ex);
        }
        return configParams;
    }

    public int getNodeCount(String regionId, String agentId) {
        int count = -2;
        try {

            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT count(P.PLUGIN_ID) FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID";
                queryParams.add(regionId);
                queryParams.add(agentId);


            } else if((regionId != null) && (agentId == null)) {
                //region

                queryString = "SELECT count(A.agent_id) FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID = ? AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID ";
                queryParams.add(regionId);

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT count(region_id) FROM rnode ";
            }

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        rs.next();
                        count = rs.getInt(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getNodeCount()", ex);
        }
        return count;
    }

    public String getResourceNodeSubmission(String resource_id) {
        String submission = null;
        try {

            String queryString = "SELECT submission FROM resourcenode WHERE resource_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resource_id));

                    try(ResultSet rs = stmt.executeQuery()) {

                        rs.next();
                        submission = rs.getString(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getResourceNodeSubmission()", ex);
        }
        return submission;
    }

    public int setResourceNodeStatus(String resourceId, int status_code, String status_desc) {
        int queryReturn = -1;
        try {

            String queryString = "UPDATE resourcenode SET status_code=?, status_desc=?"
                    + " WHERE resource_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setInt(1, status_code);
                    stmt.setString(2, textParam(status_desc));
                    stmt.setString(3, textParam(resourceId));

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }


        } catch(Exception ex) {
            logger.error("DBEngine.setResourceNodeStatus()", ex);
        }
        return queryReturn;
    }

    public int updateResource(String resourceId, int status_code, String status_desc, String submission) {
        int queryReturn = -1;
        try {

            String queryString = "UPDATE resourcenode SET status_code=?, status_desc=?, submission=?"
                    + " WHERE resource_id=?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setInt(1, status_code);
                    stmt.setString(2, textParam(status_desc));
                    stmt.setString(3, textParam(submission));
                    stmt.setString(4, textParam(resourceId));

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.updateResource()", ex);
        }
        return queryReturn;
    }

    public void addRNode(String region, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {

            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                String insertRNodeString = "insert into rnode (region_id,status_code,status_desc,watchdog_period,watchdog_ts,configparams) " +
                        "values (?,?,?,?,?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(insertRNodeString)) {

                    stmt.setString(1, textParam(region));
                    stmt.setInt(2, status_code);
                    stmt.setString(3, textParam(status_desc));
                    stmt.setInt(4, watchdog_period);
                    stmt.setLong(5, watchdog_ts);
                    stmt.setString(6, textParam(configparams));

                    stmt.executeUpdate();
                    conn.commit();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.addRNode()", ex);
        }

    }

    private void cleanANodesfromRNode(String region_id) {

        try {

            List<String> agentList = getNodeList(region_id,null);
            for(String agent_id : agentList) {
                cleanPnodesFromAnode(region_id, agent_id);
            }


        } catch(Exception ex) {
            logger.error("DBEngine.cleanANodesfromRNode()", ex);
        }

    }

    private void cleanPnodesFromAnode(String region_id, String agent_id) {

        try {

            List<String> pluginList = getNodeList(region_id,agent_id);
            for(String plugin_id : pluginList) {
                removeNode(region_id,agent_id,plugin_id);
            }


        } catch(Exception ex) {
            logger.error("DBEngine.cleanPnodesFromAnode()", ex);
        }

    }

    public void addANode(String agent, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {
            try (Connection conn = ds.getConnection()) {

                conn.setAutoCommit(false);

                String insertANodeString = "insert into anode (agent_id,status_code,status_desc,watchdog_period,watchdog_ts,configparams) " +
                        "values (?,?,?,?,?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(insertANodeString)) {

                    stmt.setString(1, textParam(agent));
                    stmt.setInt(2, status_code);
                    stmt.setString(3, textParam(status_desc));
                    stmt.setInt(4, watchdog_period);
                    stmt.setLong(5, watchdog_ts);
                    stmt.setString(6, textParam(configparams));

                    stmt.executeUpdate();
                    conn.commit();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.addANode()", ex);
        }

    }

    public boolean assoicateANodetoRNodeExist(String regionId, String agentId) {
        boolean exist = false;
        try {

            //agent
            String queryString = "SELECT COUNT(1) " + "FROM agentof " +
                        "WHERE region_id = ? " +
                        "AND agent_id = ?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(regionId));
                    stmt.setString(2, textParam(agentId));

                    try(ResultSet rs = stmt.executeQuery()) {

                        if (rs.next()) {
                            exist = rs.getBoolean(1);
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.assoicateANodetoRNodeExist()", ex);
        }
        return exist;
    }

    public boolean assoicatePNodetoANodeExist(String agentId, String pluginId) {
        boolean exist = false;
        try {

            //agent
            String queryString = "SELECT COUNT(1) " + "FROM pluginof " +
                    "WHERE agent_id = ? " +
                    "AND plugin_id = ?";

            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(agentId));
                    stmt.setString(2, textParam(pluginId));

                    try(ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            exist = rs.getBoolean(1);
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.assoicatePNodetoANodeExist()", ex);
        }
        return exist;
    }

    public void assoicateANodetoRNode(String region, String agent) {

        if(!assoicateANodetoRNodeExist(region,agent)) {
                try(Connection conn = ds.getConnection()) {

                    String insertANodeToRNode = "insert into agentof (region_id, agent_id) " +
                            "values (?,?)";

                    try (PreparedStatement stmt = conn.prepareStatement(insertANodeToRNode)) {

                        stmt.setString(1, textParam(region));
                        stmt.setString(2, textParam(agent));

                        stmt.executeUpdate();

                        stmt.close();
                    }

                    conn.close();
            } catch (Exception ex) {
                logger.error("DBEngine.assoicateANodetoRNode()", ex);
            }
        }

    }

    public void assoicatePNodetoANode(String agent, String plugin) {

        if(!assoicatePNodetoANodeExist(agent,plugin)) {

            try(Connection conn = ds.getConnection()) {

                String insertANodeToRNode = "insert into pluginof (agent_id, plugin_id) " +
                        "values (?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(insertANodeToRNode)) {

                    stmt.setString(1, textParam(agent));
                    stmt.setString(2, textParam(plugin));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            } catch (Exception ex) {
                logger.error("DBEngine.assoicatePNodetoANode()", ex);
            }
        }

    }

    public void updatePNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code) {


        try(Connection conn = ds.getConnection()) {

            String insertPNodeString = "UPDATE pnode SET status_code=?, status_desc=?, watchdog_period=?" +
                    ", watchdog_ts=?, configparams=? " +
                    "WHERE plugin_id=?";

            try (PreparedStatement stmt = conn.prepareStatement(insertPNodeString)) {

                stmt.setInt(1, status_code);
                stmt.setString(2, textParam(status_desc));
                stmt.setInt(3, watchdog_period);
                stmt.setLong(4, watchdog_ts);
                stmt.setString(5, textParam(configparams));
                stmt.setString(6, textParam(plugin));

                stmt.executeUpdate();
                //force update of pnode, so the next command does not fail.

                stmt.close();
            }

            conn.close();
        } catch(Exception ex) {
            logger.error("DBEngine.updatePNode()", ex);
        }

    }

    public int addPNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code) {

        int status = -1;

        try(Connection conn = ds.getConnection()) {

                conn.setAutoCommit(false);

            String insertPNodeString = "insert into pnode (plugin_id,status_code,status_desc,watchdog_period,watchdog_ts,pluginname,jarfile,version,md5,configparams,persistence_code) " +
                    "values (?,?,?,?,?,?,?,?,?,?,?)";

            String insertPNodeToANode = "insert into pluginof (agent_id, plugin_id) " +
                    "values (?,?)";

            try (PreparedStatement stmt = conn.prepareStatement(insertPNodeString)) {

                stmt.setString(1, textParam(plugin));
                stmt.setInt(2, status_code);
                stmt.setString(3, textParam(status_desc));
                stmt.setInt(4, watchdog_period);
                stmt.setLong(5, watchdog_ts);
                stmt.setString(6, textParam(pluginname));
                stmt.setString(7, textParam(jarfile));
                stmt.setString(8, textParam(version));
                stmt.setString(9, textParam(md5));
                stmt.setString(10, textParam(configparams));
                stmt.setInt(11, persistence_code);

                status = stmt.executeUpdate();
                //force update of pnode, so the next command does not fail.
                conn.commit();

                stmt.close();
            }

            try (PreparedStatement stmt = conn.prepareStatement(insertPNodeToANode)) {

                stmt.setString(1, textParam(agent));
                stmt.setString(2, textParam(plugin));

                status = status + stmt.executeUpdate();

                conn.commit();

                stmt.close();
            }

            conn.close();
        } catch(Exception ex) {
            logger.error("DBEngine.addPNode()", ex);
        }
        return status;
    }


    public int updateINodeAssignment(String inodeId, int status_code, String status_desc, String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = "UPDATE inode SET status_code=?, status_desc=?" +
                    ", region_id=?, agent_id=?, plugin_id=?" +
                    " WHERE inode_id=?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setInt(1, status_code);
                    stmt.setString(2, textParam(status_desc));
                    stmt.setString(3, textParam(regionId));
                    stmt.setString(4, textParam(agentId));
                    stmt.setString(5, textParam(pluginId));
                    stmt.setString(6, textParam(inodeId));

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.updateINodeAssignment()", ex);
        }
        return queryReturn;
    }

    public boolean inodeKPIExist(String inodeId) {
        boolean exist = false;
        try {

            //region
            String queryString = "SELECT COUNT(1) " + "FROM inodekpi " +
                    "WHERE inode_id = ?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(inodeId));

                    try(ResultSet rs = stmt.executeQuery()) {
                        rs.next();
                        exist = rs.getBoolean(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("inodeKPIExist()", ex);
        }
        return exist;
    }


    public void addInodeKPI(String inodeId, String kpiparams) {

        try {
            try(Connection conn = ds.getConnection()) {

                String stmtString = "insert into inodekpi (inode_id, kpiparams) " +
                        "values (?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                    stmt.setString(1, textParam(inodeId));
                    stmt.setString(2, textParam(kpiparams));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.addInodeKPI()", ex);
        }

    }

    public int updateInodeKPI(String inodeId, String kpiparams) {
        int queryReturn = -1;
        try {

            String queryString = "UPDATE inodekpi SET kpiparams=?"
                    + " WHERE inode_id=?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(kpiparams));
                    stmt.setString(2, textParam(inodeId));

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.updateInodeKPI()", ex);
        }
        return queryReturn;
    }

    public void addInode(String inodeId, String resourceId, int statusCode, String statusDesc, String configparams) {

        try {
            try(Connection conn = ds.getConnection()) {

                String stmtString = "insert into inode (inode_id, resource_id, status_code, status_desc, configparams) " +
                        "values (?,?,?,?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                    stmt.setString(1, textParam(inodeId));
                    stmt.setString(2, textParam(resourceId));
                    stmt.setInt(3, statusCode);
                    stmt.setString(4, textParam(statusDesc));
                    stmt.setString(5, textParam(configparams));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.addInode()", ex);
        }

    }

    public void addVnode(String vnodeId, String resourceId, String inodeId, String configparams) {

        try {
            try(Connection conn = ds.getConnection()) {

                String stmtString = "insert into vnode (vnode_id, resource_id, inode_id, configparams) " +
                        "values (?,?,?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                    stmt.setString(1, textParam(vnodeId));
                    stmt.setString(2, textParam(resourceId));
                    stmt.setString(3, textParam(inodeId));
                    stmt.setString(4, textParam(configparams));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.addVnode()", ex);
        }

    }

    public int getResourceNodeStatus(String resource_id) {
        int status_code = -1;
        try {

            String queryString = "SELECT status_code FROM resourcenode WHERE resource_id=?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resource_id));

                    try(ResultSet rs = stmt.executeQuery()) {
                        rs.next();
                        status_code = rs.getInt(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getResourceNodeStatus()", ex);
        }
        return status_code;
    }

    public Map<String,String> getInodeMap(String inode_id) {
        Map<String,String> inodeMap = new HashMap<>();
        try
        {

            String queryString = "SELECT * FROM inode WHERE inode_id=?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(inode_id));

                    try(ResultSet rs = stmt.executeQuery()) {

                        if (rs.next()) {
                            inodeMap.put("inode_id", rs.getString("inode_id"));
                            inodeMap.put("resource_id", rs.getString("resource_id"));


                            inodeMap.put("region_id", rs.getString("region_id"));
                            inodeMap.put("agent_id", rs.getString("agent_id"));
                            inodeMap.put("plugin_id", rs.getString("plugin_id"));

                            inodeMap.put("status_code", rs.getString("status_code"));
                            inodeMap.put("status_desc", rs.getString("status_desc"));

                            inodeMap.put("params", rs.getString("configparams"));

                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        }
        catch(Exception ex)
        {
            logger.error("DBEngine.getInodeMap()", ex);
        }

        return inodeMap;
    }

    public List<String> getINodeKPIList(String regionId, String agentId) {
        List<String> inodeKPIList = null;
        try
        {
            inodeKPIList = new ArrayList<>();
            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null)) {
                //agent
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id = ? AND agent_id = ?)";
                queryParams.add(regionId);
                queryParams.add(agentId);
            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id = ?)";
                queryParams.add(regionId);
            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id IS NOT NULL AND agent_id IS NOT NULL)";
            }
            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            inodeKPIList.add(rs.getString(1));
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        }
        catch(Exception ex)
        {
            logger.error("DBEngine.getINodeKPIList()", ex);
        }

        return inodeKPIList;
    }

    public Map<String,String> getResourceNodeStatusMap(String resource_id) {
        Map<String,String> statusMap = new HashMap<>();
        try
        {

            String queryString = "SELECT resource_name, tenant_id, status_code, status_desc FROM resourcenode WHERE resource_id=?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resource_id));

                    try(ResultSet rs = stmt.executeQuery()) {
                        rs.next();

                        statusMap.put("pipeline_id", resource_id);
                        statusMap.put("pipeline_name", rs.getString("resource_name"));
                        statusMap.put("tenant_id", rs.getString("tenant_id"));
                        statusMap.put("status_code", rs.getString("status_code"));
                        statusMap.put("status_desc", rs.getString("status_desc"));

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        }
        catch(Exception ex)
        {
            logger.error("DBEngine.getResourceNodeStatusMap()", ex);
        }

        return statusMap;
    }

    public void initDB() {

//ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent

        String largeFieldType = "clob";

        if(dbType == DBType.MYSQL) {
            largeFieldType = "blob";
        }


        String createRNode = "CREATE TABLE rnode" +
                "(" +
                "   region_id varchar(43) primary key NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   configparams " + largeFieldType +
                ")";

        String createANode = "CREATE TABLE anode" +
                "(" +
                //"   region_id varchar(43) NOT NULL," +
                "   agent_id varchar(42) primary key NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   configparams " + largeFieldType +
                //"   FOREIGN KEY (region_id) REFERENCES rnode(region_id) " +
                ")";

        String createAgentOf = "CREATE TABLE agentof" +
                "(" +
                "   region_id varchar(43) NOT NULL," +
                "   agent_id varchar(42) NOT NULL," +
                "   FOREIGN KEY (region_id) REFERENCES rnode(region_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (agent_id) REFERENCES anode(agent_id) ON DELETE CASCADE" +
                ")";

        String createPNode = "CREATE TABLE pnode" +
                "(" +
                //"   region_id varchar(43) NOT NULL," +
                //"   agent_id varchar(42) NOT NULL," +
                "   plugin_id varchar(43) primary key NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   pluginname varchar(255)," +
                "   jarfile varchar(255)," +
                "   version varchar(255)," +
                "   md5 varchar(255)," +
                "   configparams " + largeFieldType + "," +
                "   persistence_code int DEFAULT 0" +
                //"   FOREIGN KEY (region_id) REFERENCES rnode(region_id), " +
                //"   FOREIGN KEY (agent_id) REFERENCES anode(agent_id), " +
                //"   CONSTRAINT pNodeID PRIMARY KEY (region_id, agent_id, plugin_id)" +
                ")";

        String createPluginOf = "CREATE TABLE pluginof" +
                "(" +
                "   agent_id varchar(42) NOT NULL," +
                "   plugin_id varchar(43) NOT NULL," +
                "   FOREIGN KEY (agent_id) REFERENCES anode(agent_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (plugin_id) REFERENCES pnode(plugin_id) ON DELETE CASCADE " +
                ")";

        /*
        String createCState = "CREATE TABLE cstate" +
                "(" +
                "   config_ts bigint unique NOT NULL," +
                "   current_mode varchar(43) NOT NULL," +
                "   current_desc varchar(255)," +
                "   global_region varchar(43)," +
                "   global_agent varchar(42)," +
                "   regional_region varchar(43)," +
                "   regional_agent varchar(42)," +
                "   local_region varchar(43)," +
                "   local_agent varchar(42)," +
                "   FOREIGN KEY (global_region) REFERENCES rnode(region_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (global_agent) REFERENCES anode(agent_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (regional_region) REFERENCES rnode(region_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (regional_agent) REFERENCES anode(agent_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (local_region) REFERENCES rnode(region_id) ON DELETE CASCADE, " +
                "   FOREIGN KEY (local_agent) REFERENCES anode(agent_id) ON DELETE CASCADE " +
                ")";
        */

        String createCState = "CREATE TABLE cstate" +
                "(" +
                "   config_ts bigint unique NOT NULL," +
                "   current_mode varchar(43) NOT NULL," +
                "   current_desc varchar(255)," +
                "   global_region varchar(43)," +
                "   global_agent varchar(42)," +
                "   regional_region varchar(43)," +
                "   regional_agent varchar(42)," +
                "   local_region varchar(43)," +
                "   local_agent varchar(42)," +
                "   FOREIGN KEY (local_region) REFERENCES rnode(region_id) ON DELETE CASCADE," +
                "   FOREIGN KEY (local_agent) REFERENCES anode(agent_id) ON DELETE CASCADE" +
                ")";



        String createTenantNode = "CREATE TABLE tenantnode" +
                "(" +
                "   tenant_id int primary key NOT NULL," +
                "   tenantname varchar(255)" +
                ")";

        String createResourceNode = "CREATE TABLE resourcenode" +
                "(" +
                "   resource_id varchar(45) primary key NOT NULL," +
                "   resource_name varchar(255)," +
                "   tenant_id int," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   submission " + largeFieldType + "," +
                "   FOREIGN KEY (tenant_id) REFERENCES tenantnode(tenant_id)" +
                ")";

        String createVnode = "CREATE TABLE vnode" +
                "(" +
                "   vnode_id varchar(42) primary key NOT NULL," +
                "   resource_id varchar(45) NOT NULL," +
                "   inode_id varchar(42)," +
                "   configparams " + largeFieldType + "," +
                "   FOREIGN KEY (resource_id) REFERENCES resourcenode(resource_id)" +
                ")";

        String createInode = "CREATE TABLE inode" +
                "(" +
                "   inode_id varchar(42) primary key NOT NULL," +
                "   resource_id varchar(45) NOT NULL," +
                "   region_id varchar(43)," +
                "   agent_id varchar(42)," +
                "   plugin_id varchar(43)," +
                "   status_code int NOT NULL," +
                "   status_desc varchar(255) NOT NULL," +
                "   configparams " + largeFieldType + " NOT NULL," +
                "   kpiparams " + largeFieldType + "," +
                "   FOREIGN KEY (resource_id) REFERENCES resourcenode(resource_id)" +
                ")";

        String createInodeKPI = "CREATE TABLE inodekpi" +
                "(" +
                //"   inodekpi_id varchar(42) primary key NOT NULL," +
                "   inode_id varchar(42)," +
                "   kpiparams " + largeFieldType +
                //"   FOREIGN KEY (inode_id) REFERENCES inode(inode_id)" +
                ")";

        /*
        for(String table : tablesNames) {
                dropTable(table);

        }
        */


        if(dbType == DBType.MYSQL) {
            if (tableExist("inodekpi")) {
                dropTable("inodekpi");
            }

            if (tableExist("vnode")) {
                dropTable("vnode");
            }

            if (tableExist("inode")) {
                dropTable("inode");
            }

            if (tableExist("resourcenode")) {
                dropTable("resourcenode");
            }

            if (tableExist("tenantnode")) {
                dropTable("tenantnode");
            }

            if (tableExist("cstate")) {
                dropTable("cstate");
            }

            if (tableExist("pluginof")) {
                dropTable("pluginof");
            }

            if (tableExist("pnode")) {
                dropTable("pnode");
            }

            if(tableExist("aconfig")) {
                dropTable("aconfig");
            }

            if(tableExist("agentof")) {
                dropTable("agentof");
            }

            if (tableExist("anode")) {
                dropTable("anode");
            }

            if (tableExist("rnode")) {
                dropTable("rnode");
            }

        }


        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    stmt.executeUpdate(createRNode);
                    stmt.executeUpdate(createANode);
                    stmt.executeUpdate(createAgentOf);
                    stmt.executeUpdate(createPNode);
                    stmt.executeUpdate(createPluginOf);
                    stmt.executeUpdate(createCState);
                    stmt.executeUpdate(createTenantNode);
                    stmt.executeUpdate(createResourceNode);
                    stmt.executeUpdate(createInode);
                    stmt.executeUpdate(createVnode);
                    stmt.executeUpdate(createInodeKPI);
                    
                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.initDB()", ex);
        }

        // Indexes for the association tables' join/FK columns and the status/watchdog lookup
        // columns used by getNodeList/getStaleNodeList/getNodeStatusCodeMap and the KPI joins.
        // Derby has no CREATE INDEX IF NOT EXISTS: an already-exists failure (SQLState X0Y32)
        // is ignored the same way re-created tables are tolerated; anything else is logged.
        String[][] indexStatements = new String[][]{
                {"idx_agentof_region_id", "CREATE INDEX idx_agentof_region_id ON agentof (region_id)"},
                {"idx_agentof_agent_id", "CREATE INDEX idx_agentof_agent_id ON agentof (agent_id)"},
                {"idx_pluginof_agent_id", "CREATE INDEX idx_pluginof_agent_id ON pluginof (agent_id)"},
                {"idx_pluginof_plugin_id", "CREATE INDEX idx_pluginof_plugin_id ON pluginof (plugin_id)"},
                {"idx_rnode_status_code", "CREATE INDEX idx_rnode_status_code ON rnode (status_code)"},
                {"idx_anode_status_code", "CREATE INDEX idx_anode_status_code ON anode (status_code)"},
                {"idx_pnode_status_code", "CREATE INDEX idx_pnode_status_code ON pnode (status_code)"},
                {"idx_inode_resource_id", "CREATE INDEX idx_inode_resource_id ON inode (resource_id)"},
                {"idx_inode_region_agent", "CREATE INDEX idx_inode_region_agent ON inode (region_id, agent_id)"},
                {"idx_vnode_resource_id", "CREATE INDEX idx_vnode_resource_id ON vnode (resource_id)"},
                {"idx_inodekpi_inode_id", "CREATE INDEX idx_inodekpi_inode_id ON inodekpi (inode_id)"}
        };

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    for (String[] indexStatement : indexStatements) {
                        try {
                            stmt.executeUpdate(indexStatement[1]);
                        } catch (SQLException sqle) {
                            if (!"X0Y32".equals(sqle.getSQLState())) {
                                logger.error("DBEngine.initDB() create index failed: " + indexStatement[0], sqle);
                            }
                        }
                    }

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.initDB() index creation", ex);
        }
    }


    private boolean tableExist(String tableName)  {
        boolean exist = false;

        try {

            String queryString = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = ?";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(tableName));

                    try(ResultSet rs = stmt.executeQuery()) {
                        rs.next();
                        exist = rs.getBoolean(1);

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        }
        catch (SQLException sqle) {
            //eat SQL exception, if tables does not exist this will throw
            logger.error("DBEngine.tableExist() SQL EXCEPTION", sqle);
        }
        catch(Exception ex) {
            logger.error("DBEngine.tableExist()", ex);
        }
        return exist;
    }
    //

    private int dropTable(String tableName) {
        int result = -1;
        try {

            // A table name cannot be bound as a JDBC parameter; restrict this DDL to the
            // hardcoded set of tables this class manages.
            if((tableName == null) || (!MANAGED_TABLES.contains(tableName.toLowerCase(Locale.ROOT)))) {
                logger.error("DBEngine.dropTable() refusing non-allowlisted table: " + tableName);
                return result;
            }

            String stmtString = "DROP TABLE " + tableName;

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    result = stmt.executeUpdate(stmtString);

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.dropTable()", ex);
        }
        return result;
    }

    public boolean nodeExist(String regionId, String agentId, String pluginId) {
        boolean exist = false;
        try {


            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId == null) && (agentId == null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT COUNT(1) " + "FROM pnode " +
                        "WHERE plugin_id = ?";
                queryParams.add(pluginId);

            } if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT COUNT(1) " + "FROM pnode " +
                        "WHERE plugin_id = ?";
                queryParams.add(pluginId);

            } else if((regionId == null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT COUNT(1) " + "FROM anode " +
                        "WHERE agent_id = ?";
                queryParams.add(agentId);

            }else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT COUNT(1) " + "FROM anode " +
                        "WHERE agent_id = ?";
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT COUNT(1) " + "FROM rnode " +
                        "WHERE region_id = ?";
                queryParams.add(regionId);
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        if (rs.next()) {
                            exist = rs.getBoolean(1);
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.nodeExist()", ex);
        }
        return exist;
    }

    public void reassoicateANodes(String originalRegionId, String originalAgentId, String regionId, String agentId) {

        try {
            try(Connection conn = ds.getConnection()) {

                String queryString = "UPDATE AGENTOF " +
                        "SET region_id = ?, agent_id = ? " +
                        "WHERE region_id = ? AND agent_id = ?";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(regionId));
                    stmt.setString(2, textParam(agentId));
                    stmt.setString(3, textParam(originalRegionId));
                    stmt.setString(4, textParam(originalAgentId));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.reassoicateANodes()", ex);
        }

    }

    public void reassoicatePNodes(String originalAgentId, String agentId) {

        try {
            try(Connection conn = ds.getConnection()) {

                String queryString = "UPDATE PLUGINOF " +
                        "SET agent_id = ? " +
                        "WHERE agent_id = ?";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(agentId));
                    stmt.setString(2, textParam(originalAgentId));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.reassoicatePNodes()", ex);
        }
        //return isRemoved;
    }

    public void purgeTransientPNodes(String regionId, String agentId) {
        //boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {

                String queryString = "DELETE FROM PNODE P WHERE NOT EXISTS ( " +
                        "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID " +
                        "AND P.PERSISTENCE_CODE > 9 )";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(regionId));
                    stmt.setString(2, textParam(agentId));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.purgeTransientPNodes()", ex);
        }
        //return isRemoved;
    }

    public boolean removeINode(String inodeId) {
        boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {

                String queryString = "DELETE FROM inode " +
                        "WHERE inode_id = ?";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(inodeId));

                    if (stmt.executeUpdate() == 1) {
                        isRemoved = true;
                    }

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.removeINode()", ex);
        }
        return isRemoved;
    }

    public boolean removeVNode(String vnodeId) {
        boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {

                String queryString = "DELETE FROM vnode " +
                        "WHERE vnode_id = ?";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(vnodeId));

                    if (stmt.executeUpdate() == 1) {
                        isRemoved = true;
                    }

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.removeVNode()", ex);
        }
        return isRemoved;
    }

    public boolean removeResource(String resourceId) {
        boolean isRemoved = false;
        try {

            List<String> nodeList = getINodeResourceList(resourceId);

            for(String inode_id : nodeList) {
                removeINode(inode_id);
            }
            nodeList = getVNodeResourceList(resourceId);

            for(String vnode_id : nodeList) {
                removeVNode(vnode_id);
            }

            try(Connection conn = ds.getConnection()) {

                String queryString = "DELETE FROM resourcenode " +
                        "WHERE resource_id = ?";

                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(resourceId));

                    if (stmt.executeUpdate() == 1) {
                        isRemoved = true;
                    }

                    stmt.close();
                }

                conn.close();
            }

        }
        catch(Exception ex) {
            //logger.error("removePipeline " + ex.getMessage());
            logger.error("DBEngine.removeResource()", ex);
        }
        return isRemoved;
    }

    public Map<String,String> getCSTATE(String config_ts) {
        Map<String,String> cstateMap = null;
        try {

            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if(config_ts != null) {
                queryString = "SELECT CONFIG_TS, CURRENT_MODE, CURRENT_DESC, GLOBAL_REGION, " +
                        "GLOBAL_AGENT, REGIONAL_REGION, REGIONAL_AGENT, LOCAL_REGION, LOCAL_AGENT " +
                        "FROM CSTATE WHERE CONFIG_TS = ?";
                queryParams.add(Long.parseLong(config_ts));

            } else {
                queryString = "SELECT CONFIG_TS, CURRENT_MODE, CURRENT_DESC, GLOBAL_REGION, " +
                        "GLOBAL_AGENT, REGIONAL_REGION, REGIONAL_AGENT, LOCAL_REGION, LOCAL_AGENT " +
                        "FROM CSTATE WHERE (CONFIG_TS) IN " +
                        "( SELECT MAX(CONFIG_TS) " +
                        "  FROM CSTATE )";
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        if (rs.next()) {
                            cstateMap = new HashMap<>();
                            cstateMap.put("config_ts", rs.getString(1));
                            cstateMap.put("current_mode", rs.getString(2));
                            cstateMap.put("current_desc", rs.getString(3));
                            cstateMap.put("global_region", rs.getString(4));
                            cstateMap.put("global_agent", rs.getString(5));
                            cstateMap.put("regional_region", rs.getString(6));
                            cstateMap.put("regional_agent", rs.getString(7));
                            cstateMap.put("local_region", rs.getString(8));
                            cstateMap.put("local_agent", rs.getString(9));
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }


        } catch (Exception ex) {
            logger.error("DBEngine.getCSTATE()", ex);
        }
        return cstateMap;
    }


    public boolean removeNode(String regionId, String agentId, String pluginId) {

        //System.out.println("regionId: " + regionId + " agentId: " + agentId + " pluginId:c" + pluginId);

        boolean isRemoved = false;
        try {
            String queryString = null;
            List<Object> queryParams = new ArrayList<>();
            //DELETE FROM table_name WHERE condition;
            if ((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "DELETE FROM PNODE WHERE PLUGIN_ID IN ( " +
                        "SELECT P.PLUGIN_ID " +
                        "FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND P.PLUGIN_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID)";
                queryParams.add(regionId);
                queryParams.add(agentId);
                queryParams.add(pluginId);

            } else if ((regionId == null) && (agentId == null) && (pluginId != null)) {
                //plugin
                queryString = "DELETE FROM pnode " +
                        "WHERE plugin_id = ?";
                queryParams.add(pluginId);

            } else if ((regionId == null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "DELETE FROM anode " +
                        "WHERE region_id = ? and agent_id = ?";
                queryParams.add(regionId);
                queryParams.add(agentId);

            } else if ((regionId != null) && (agentId != null) && (pluginId == null)) {

                //first remove agent plugins
                cleanPnodesFromAnode(regionId, agentId);

                //agent
                queryString = "DELETE FROM ANODE WHERE AGENT_ID IN ( " +
                        "SELECT A.AGENT_ID " +
                        "FROM ANODE A, RNODE R, AGENTOF AO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID)";
                queryParams.add(regionId);
                queryParams.add(agentId);

            } else if ((regionId != null) && (agentId == null) && (pluginId == null)) {
                //first remove agents and plugins from region
                cleanANodesfromRNode(regionId);

                //region
                queryString = "DELETE FROM rnode " +
                        "WHERE region_id = ?";
                queryParams.add(regionId);
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    //System.out.println("QUERY: " + queryString);

                    int result = stmt.executeUpdate();


                    //System.out.println("RESULT : " + result);
                    if (result < 2) {
                        isRemoved = true;
                    }

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.removeNode()", ex);
        }
        return isRemoved;
    }

    public int updateWatchDogTS(String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId == null) && (agentId == null) && (pluginId != null)) {
                //plugin

                queryString = "UPDATE pnode SET watchdog_ts = ?"
                        + " WHERE plugin_id=?";
                queryParams.add(System.currentTimeMillis());
                queryParams.add(pluginId);

            } else if((regionId == null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET watchdog_ts = ?"
                        + " WHERE agent_id=?";
                queryParams.add(System.currentTimeMillis());
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET watchdog_ts = ?"
                        + " WHERE region_id=?";
                queryParams.add(System.currentTimeMillis());
                queryParams.add(regionId);
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.updateWatchDogTS()", ex);
        }
        return queryReturn;
    }

    public List<String> getNodeList(String regionId, String agentId) {

        List<String> nodeList = null;
        try {


            nodeList = new ArrayList<>();
            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID " +
                        "AND P.STATUS_CODE = 10";
                queryParams.add(regionId);
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT A.agent_id FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID = ? AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID "
                        + "AND A.STATUS_CODE = 10";
                queryParams.add(regionId);

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM rnode WHERE STATUS_CODE = 10";
            }

            if(queryString != null) {

                try(Connection conn = ds.getConnection()) {
                    try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                        bindParams(stmt, queryParams);

                        try(ResultSet rs = stmt.executeQuery()) {

                            while (rs.next()) {
                                String node = rs.getString(1);
                                if (!nodeList.contains(node)) {
                                    nodeList.add(node);
                                }
                            }

                            rs.close();
                        }

                        stmt.close();
                    }

                    conn.close();
                }
            } else {
                logger.error("getNodeList(regionId = {} , agentId= {})", regionId, agentId, new Throwable());
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getNodeList()", ex);
        }
        return nodeList;
    }


    public List<Map<String,String>> getPluginListMapByType(String actionPluginTypeId, String actionPluginTypeValue) {
        List<Map<String,String>> configMapList = null;
        try {

            configMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            // The column name arrives over the wire (action_plugintype_id) and JDBC cannot
            // bind an identifier as a parameter, so validate it against the hardcoded
            // allowlist of pnode columns before it touches the SQL text.
            String typeColumn = (actionPluginTypeId == null) ? null : actionPluginTypeId.toLowerCase(Locale.ROOT);
            if((typeColumn == null) || (!PLUGIN_TYPE_COLUMNS.contains(typeColumn))) {
                logger.error("DBEngine.getPluginListMapByType() rejected non-allowlisted pnode column: " + actionPluginTypeId);
                return configMapList;
            }

            String queryString = "SELECT R.REGION_ID, A.AGENT_ID, P.PLUGIN_ID, P.CONFIGPARAMS " +
                    "FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                    "WHERE P." + typeColumn + " = ? " +
                    "AND R.REGION_ID = AO.REGION_ID " +
                    "AND AO.AGENT_ID = A.AGENT_ID " +
                    "AND A.AGENT_ID = PO.AGENT_ID " +
                    "AND PO.PLUGIN_ID = P.PLUGIN_ID";

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    stmt.setString(1, textParam(actionPluginTypeValue));

                    try(ResultSet rs = stmt.executeQuery()) {

                        while (rs.next()) {
                            String configParamString = rs.getString("configparams");
                            Map<String, String> configMap = gson.fromJson(configParamString, type);
                            configMap.put("region", rs.getString("region_id"));
                            configMap.put("agent", rs.getString("agent_id"));
                            configMap.put("pluginid", rs.getString("plugin_id"));
                            configMapList.add(configMap);
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getPluginListMapByType()", ex);
        }

        return configMapList;
    }

    public Map<String,Integer> getNodeStatusCodeMap(String regionId, String agentId) {

        String queryString = null;

        Map<String,Integer> nodeMap = null;
        try {

            nodeMap = new HashMap<>();

            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID, P.status_code FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND P.PLUGIN_ID = PO.PLUGIN_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID ";
                queryParams.add(regionId);
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null)) {
                //region

                queryString = "SELECT A.agent_id, A.status_code FROM ANODE A, RNODE R, AGENTOF O " +
                        "WHERE R.REGION_ID = ? " +
                        "AND R.REGION_ID = O.REGION_ID " +
                        "AND O.AGENT_ID = A.AGENT_ID " +
                        "AND R.REGION_ID = O.REGION_ID ";
                queryParams.add(regionId);

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id, status_code FROM rnode ";
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        while (rs.next()) {
                            String key = rs.getString(1);
                            if (!nodeMap.containsKey(key)) {
                                nodeMap.put(key, rs.getInt(2));
                            }
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getNodeStatusCodeMap() QUERY STRING: [{}]", queryString, ex);
        }
        return nodeMap;
    }

    public List<String> getStaleNodeList(String regionId, String agentId, int periodMultiplier) {

        List<String> nodeList = null;
        try {


            nodeList = new ArrayList<>();
            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID = ? " +
                        "AND A.AGENT_ID = ? " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND P.status_code=10 and ((? - P.watchdog_ts) > (P.watchdog_period * ?))";
                queryParams.add(regionId);
                queryParams.add(agentId);
                queryParams.add(System.currentTimeMillis());
                queryParams.add(periodMultiplier);

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT A.agent_id FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID = ? AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID "
                        + "AND A.status_code=10 and ((? - A.watchdog_ts) > (A.watchdog_period * ?))";
                queryParams.add(regionId);
                queryParams.add(System.currentTimeMillis());
                queryParams.add(periodMultiplier);

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM rnode "
                        + "WHERE status_code=10 and ((? - watchdog_ts) > (watchdog_period * ?))";
                queryParams.add(System.currentTimeMillis());
                queryParams.add(periodMultiplier);
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    try(ResultSet rs = stmt.executeQuery()) {

                        while (rs.next()) {
                            String node = rs.getString(1);
                            if (!nodeList.contains(node)) {
                                nodeList.add(node);
                            }
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getStaleNodeList()", ex);
        }
        return nodeList;
    }

    public int setNodeStatusCode(String regionId, String agentId, String pluginId, int status_code, String status_desc) {



        int queryReturn = -1;
        try {

            String queryString = null;
            List<Object> queryParams = new ArrayList<>();

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin

                queryString = "UPDATE pnode SET status_code=?, status_desc=?"
                        + " WHERE plugin_id=?";
                queryParams.add(status_code);
                queryParams.add(status_desc);
                queryParams.add(pluginId);

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET status_code=?, status_desc=?"
                        + " WHERE agent_id=?";
                queryParams.add(status_code);
                queryParams.add(status_desc);
                queryParams.add(agentId);

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET status_code=?, status_desc=?"
                        + " WHERE region_id=?";
                queryParams.add(status_code);
                queryParams.add(status_desc);
                queryParams.add(regionId);
            }

            try(Connection conn = ds.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement(queryString)) {

                    bindParams(stmt, queryParams);

                    queryReturn = stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.setNodeStatusCode()", ex);
        }
        return queryReturn;
    }

    public void addResource(String resourceId, String resourceName, int tenantId, int statusCode, String statusDesc, String submission) {

        try {


            try(Connection conn = ds.getConnection()) {

                String stmtString = "insert into resourcenode (resource_id, resource_name, tenant_id, status_code, status_desc, submission) " +
                        "values (?,?,?,?,?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                    stmt.setString(1, textParam(resourceId));
                    stmt.setString(2, textParam(resourceName));
                    stmt.setInt(3, tenantId);
                    stmt.setInt(4, statusCode);
                    stmt.setString(5, textParam(statusDesc));
                    stmt.setString(6, textParam(submission));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.addResource()", ex);
        }

    }

    public void addTenant(int tenantId, String tenantName) {

        try {
            try(Connection conn = ds.getConnection()) {

                String stmtString = "insert into tenantnode (tenant_id,tenantname) " +
                        "values (?,?)";

                try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {

                    stmt.setInt(1, tenantId);
                    stmt.setString(2, textParam(tenantName));

                    stmt.executeUpdate();

                    stmt.close();
                }

                conn.close();
            }
        } catch(Exception ex) {
            logger.error("DBEngine.addTenant()", ex);
        }

    }

    public List<String> getResourceNodeList() {
        List<String> nodeList = null;
        try {

            nodeList = new ArrayList<>();
            String queryString = null;

            queryString = "SELECT resource_id FROM resourcenode ";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            String node = rs.getString(1);
                            if (!nodeList.contains(node)) {
                                nodeList.add(node);
                            }
                        }

                        rs.close();
                    }

                    stmt.close();
                }

                conn.close();
            }

        } catch(Exception ex) {
            logger.error("DBEngine.getResourceNodeList()", ex);
        }
        return nodeList;
    }


    public DataSource setupDataSource(String connectURI) {
        return setupDataSource(connectURI,null,null);
    }

    public DataSource setupDataSource(String connectURI, String login, String password) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //

        ConnectionFactory connectionFactory = null;

        if((login == null) && (password == null)) {
            connectionFactory = new DriverManagerConnectionFactory(connectURI, null);
        } else {
            connectionFactory = new DriverManagerConnectionFactory(connectURI,
                    login, password);
        }


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);



        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);



        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public byte[] dataCompress(byte[] dataToCompress) {

        byte[] compressedData;
        try {
            ByteArrayOutputStream byteStream =
                    new ByteArrayOutputStream(dataToCompress.length);
            try {
                GZIPOutputStream zipStream =
                        new GZIPOutputStream(byteStream);
                try {
                    zipStream.write(dataToCompress);
                }
                finally {
                    zipStream.close();
                }
            } finally {
                byteStream.close();
            }
            compressedData = byteStream.toByteArray();
        } catch(Exception e) {
            logger.error("DBEngine.dataCompress()", e);
            return null;
        }
        return compressedData;
    }

}