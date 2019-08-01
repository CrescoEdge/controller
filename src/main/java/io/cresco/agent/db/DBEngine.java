package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.plugin.PluginBuilder;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class DBEngine {

    private DataSource ds;
    private Gson gson;
    private Type type;
    private DBType dbType = DBType.EMBEDDED;

    private List<String> tablesNames;

    private PluginBuilder pluginBuilder;

    public DBEngine(PluginBuilder plugin) {

        try {

            this.pluginBuilder = plugin;

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

            String dbDriver = plugin.getConfig().getStringParam("db_driver", "org.apache.derby.jdbc.EmbeddedDriver");
            //String dbDriver = plugin.getConfig().getStringParam("db_driver","org.hsqldb.jdbcDriver");
            if (dbDriver.contains("mysql")) {
                dbType = DBType.MYSQL;
            }


            String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc", "jdbc:derby:" + dbName + ";create=true");
            //String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc","jdbc:hsqldb:" + "database/" + dbName + ";create=true");


            String dbUserName = plugin.getConfig().getStringParam("db_username");
            String dbPassword = plugin.getConfig().getStringParam("db_password");

            Class.forName(dbDriver);

            if ((dbUserName != null) && (dbPassword != null)) {
                ds = setupDataSource(dbConnectionString, dbUserName, dbPassword);
            } else {
                ds = setupDataSource(dbConnectionString);
            }

            //Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            //ds = setupDataSource("jdbc:derby:demo;create=true");

            //Class.forName("com.mysql.cj.jdbc.Driver");
            //ds = setupDataSource("jdbc:mysql://localhost/cresco?characterEncoding=UTF-8","root", "codeman01");

            if (dbType == DBType.EMBEDDED) {
                if (dbName.equals(defaultDBName)) {
                    File dbsource = new File(defaultDBName);
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
            //ds = setupDataSource("jdbc:mysql://localhost/cresco?characterEncoding=UTF-8","root", "codeman01");


            /*
            ds = new BasicDataSource();
            ds.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
            ds.setUrl("jdbc:derby:demo;create=true");
            */

        } catch (Exception ex) {
            ex.printStackTrace();
        }
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
            System.out.println("Schema is invalid");
        }
        return isOk;
    }

    public boolean nodeUpdateStatus(String mode, String region_watchdog_update, String agent_watchdog_update, String plugin_watchdog_update, String regionconfigs, String agentconfigs, String pluginconfigs) {

        boolean isUpdated = true;

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
                        String status_code = regionMap.get("status_code");
                        String status_desc = regionMap.get("status_desc");
                        String watchdog_period = regionMap.get("watchdog_period");
                        //String watchdog_ts = agentMap.get("watchdog_ts");
                        String configparams = regionMap.get("configparams");

                        if(!nodeExist(region_id,null,null)) {

                            //logger.debug("addNodeFromUpdate add [" + de.getParams() + "]");
                            addRNode(region_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);

                        } else {
                            //logger.debug("addNodeFromUpdate update [" + de.getParams() + "]");
                            updateNode(region_id,null,null,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);
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
                        String status_code = agentMap.get("status_code");
                        String status_desc = agentMap.get("status_desc");
                        String watchdog_period = agentMap.get("watchdog_period");
                        //String watchdog_ts = agentMap.get("watchdog_ts");
                        String configparams = agentMap.get("configparams");

                        if(!nodeExist(region_id,agent_id,null)) {

                            //logger.debug("addNodeFromUpdate add [" + de.getParams() + "]");
                            addANode(agent_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);

                        } else {
                            //logger.debug("addNodeFromUpdate update [" + de.getParams() + "]");
                            updateNode(region_id,agent_id,null,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),configparams);
                            //remove from removeAgentList
                            if(isRegionUpdate) {
                                removeAgentList.remove(agent_id);
                            }
                        }

                        if(!assoicateANodetoRNodeExist(region_id,agent_id)) {
                            assoicateANodetoRNode(region_id, agent_id);
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

                        if(!nodeExist(null,null, plugin_id)) {
                            int status = addPNode(agent_id,plugin_id,Integer.parseInt(status_code),status_desc,Integer.parseInt(watchdog_period),System.currentTimeMillis(),pluginname,jarfile,version,md5,configparams,Integer.parseInt(persistence_code));
                        } else {
                            updateNode(null, null, plugin_id, Integer.parseInt(status_code), status_desc, Integer.parseInt(watchdog_period), System.currentTimeMillis(), configparams);
                            removePluginList.remove(plugin_id);
                        }

                        if(!assoicatePNodetoANodeExist(agent_id,plugin_id)) {
                            assoicatePNodetoANode(agent_id,plugin_id);
                        }

                    }

                    //remove any plugins not in the update
                    for(String plugin_id : removePluginList) {
                        removeNode(region_id,agent_id,plugin_id);
                    }

                    } else {
                        System.out.println("WHY DOES AGENT: " + agent_id + " HAVE NO REGION!");
                        Thread.dumpStack();
                    }

                }

            }

            isUpdated = true;

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isUpdated;
    }

    public void updateNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {

                String stmtString = null;

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

                    stmtString = "UPDATE pnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                            ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                            "WHERE plugin_id='" + plugin + "'";

                } else if (((region != null) && (agent != null) && (plugin == null)) || ((region == null) && (agent != null) && (plugin == null))) {
                    stmtString = "UPDATE anode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                            ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                            "WHERE agent_id='" + agent + "'";

                } else if ((region != null) && (agent == null) && (plugin == null)) {
                    stmtString = "UPDATE rnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                            ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                            "WHERE region_id='" + region + "'";
                }

                stmt.executeUpdate(stmtString);
            }

        } catch(Exception ex) {
            ex.printStackTrace();
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
                    regionList.add(getRNode(tmp_region_id));
                }
                regionMap.put(pluginBuilder.getRegion(), regionList);

                exportMap.put("regionconfigs",gson.toJson(regionMap));

            }

            if(agents) {

                Map<String, List<Map<String, String>>> agentMap = new HashMap<>();

                if((region_id != null) && (agent_id != null)) {
                    List<Map<String, String>> agentList = new ArrayList<>();
                    agentList.add(getANode(agent_id));
                    agentMap.put(region_id, agentList);

                } else {

                    List<String> tmpRegionList = getNodeList(null,null);
                    for(String tmp_region_id : tmpRegionList) {
                        List<Map<String, String>> agentList = new ArrayList<>();
                        List<String> tmpAgentList = getNodeList(tmp_region_id, null);
                        for(String tmp_agent_id : tmpAgentList) {
                            agentList.add(getANode(tmp_agent_id));
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
                        pluginList.add(getPNode(pluginId));
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
                                pluginList.add(getPNode(tmp_plugin_id));
                            }
                            pluginMap.put(tmp_agent_id, pluginList);
                        }
                    }
                }
                exportMap.put("pluginconfigs",gson.toJson(pluginMap));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }


        return exportMap;
    }

    public void addCStateEvent(long config_ts, String current_mode, String current_desc, String global_region, String global_agent, String regional_region, String regional_agent, String local_region, String local_agent) {

        try (Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String stmtString = null;

                stmtString = "INSERT INTO cstate values (" + config_ts + ",'" + current_mode + "','" + current_desc + "','" + global_region + "','" + global_agent + "','" + regional_region + "','" + regional_agent + "','" + local_region + "','" + local_agent + "')";

                stmt.executeUpdate(stmtString);
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void updateRNode(String region, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String stmtString = null;


                stmtString = "UPDATE rnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                        ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                        "WHERE region_id='" + region + "'";


                stmt.executeUpdate(stmtString);
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void updateANode(String agent, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try (Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String stmtString = null;


                stmtString = "UPDATE anode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                        ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                        "WHERE agent_id='" + agent + "'";


                stmt.executeUpdate(stmtString);
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public List<String> getVNodeResourceList(String resourceId) {
        List<String> inodeResourceList = null;

        try {

            inodeResourceList = new ArrayList<>();
            String queryString = null;

            queryString = "SELECT vnode_id FROM vnode WHERE resource_id='" + resourceId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    ResultSet rs = stmt.executeQuery(queryString);

                    while (rs.next()) {
                        String node = rs.getString(1);
                        if (!inodeResourceList.contains(node)) {
                            inodeResourceList.add(node);
                        }
                    }

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return inodeResourceList;
    }

    public List<String> getINodeResourceList(String resourceId) {
        List<String> inodeResourceList = null;

        try {

            inodeResourceList = new ArrayList<>();
            String queryString = null;

            queryString = "SELECT inode_id FROM inode WHERE resource_id='" + resourceId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    ResultSet rs = stmt.executeQuery(queryString);

                    while (rs.next()) {
                        String node = rs.getString(1);
                        if (!inodeResourceList.contains(node)) {
                            inodeResourceList.add(node);
                        }
                    }

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return inodeResourceList;
    }


    public int getINodeStatus(String inodeId) {
        int status_code = -1;
        try {

            String queryString = null;

            queryString = "SELECT status_code FROM inode WHERE inode_id='" + inodeId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    ResultSet rs = stmt.executeQuery(queryString);

                    rs.next();
                    status_code = rs.getInt(1);

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return status_code;
    }

    public int setINodeStatusCode(String inodeId, int status_code, String status_desc) {
        int queryReturn = -1;
        try {

            String queryString = null;

            queryString = "UPDATE inode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                    + " WHERE inode_id='" + inodeId + "'";


            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    queryReturn = stmt.executeUpdate(queryString);

                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public int getPNodePersistenceCode(String pluginId) {
        int status_code = -1;
        try {

            String queryString = null;

            queryString = "SELECT persistence_code FROM pnode " +
                    "WHERE plugin_id='" + pluginId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {


                    ResultSet rs = stmt.executeQuery(queryString);

                    rs.next();
                    status_code = rs.getInt(1);

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return status_code;
    }

    public Map<String,String> getPNode(String pluginId) {
        Map<String,String> pNodeMap = null;
        try {

            pNodeMap = new HashMap<>();

            String queryString = null;

            queryString = "SELECT * FROM pnode " +
                    "WHERE plugin_id='" + pluginId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    ResultSet rs = stmt.executeQuery(queryString);

                    rs.next();
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

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return pNodeMap;
    }

    public Map<String,String> getRNode(String regionId) {
        Map<String,String> aNodeMap = null;
        try {

            aNodeMap = new HashMap<>();

            String queryString = null;

            queryString = "SELECT * FROM rnode " +
                    "WHERE region_id='" + regionId + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    ResultSet rs = stmt.executeQuery(queryString);

                    rs.next();
                    aNodeMap.put("region_id", rs.getString("region_id"));
                    aNodeMap.put("status_code", rs.getString("status_code"));
                    aNodeMap.put("status_desc", rs.getString("status_desc"));
                    aNodeMap.put("watchdog_period", rs.getString("watchdog_period"));
                    //aNodeMap.put("watchdog_ts", rs.getString("watchdog_ts"));
                    aNodeMap.put("configparams", rs.getString("configparams"));

                    rs.close();
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return aNodeMap;
    }

    public Map<String,String> getANode(String agentId) {
        Map<String,String> aNodeMap = null;
        try {

            aNodeMap = new HashMap<>();

            String queryString = null;

            queryString = "SELECT * FROM anode " +
                    "WHERE agent_id='" + agentId + "'";

            try (Connection conn = ds.getConnection()) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            rs.next();
            aNodeMap.put("agent_id", rs.getString("agent_id"));
            aNodeMap.put("status_code", rs.getString("status_code"));
            aNodeMap.put("status_desc", rs.getString("status_desc"));
            aNodeMap.put("watchdog_period", rs.getString("watchdog_period"));
            //aNodeMap.put("watchdog_ts", rs.getString("watchdog_ts"));
            aNodeMap.put("configparams", rs.getString("configparams"));

            rs.close();
            stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return aNodeMap;
    }

    public int setPNodePersistenceCode(String plugin, int persistence_code) {
        int queryReturn = -1;
        try {

            String queryString = null;


            queryString = "UPDATE pnode SET persistence_code=" + persistence_code + "' " +
                    "WHERE plugin_id='" + plugin + "'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public String getRNodeFromAnode(String agentId) {
        String configParams = null;
        try {

            String queryString = "SELECT region_id FROM agentOf WHERE agent_id = '" + agentId +"'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                if (rs.next()) {
                    configParams = rs.getString(1);
                } else {
                    System.out.println("WHY IS RESULT SET EMPTY THIS NULL: AGENT_ID = " + agentId);
                }

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }

        return configParams;
    }

    public String getNodeConfigParams(String regionId, String agentId, String pluginId) {
        String configParams = null;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT P.configparams FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID ='" + regionId + "' " +
                        "AND A.AGENT_ID = '" + agentId + "' " +
                        "AND P.PLUGIN_ID = '" + pluginId + "' " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID";

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT A.configparams FROM ANODE A, RNODE R, AGENTOF O " +
                        "WHERE R.REGION_ID ='" + regionId + "' " +
                        "AND A.AGENT_ID = '" + agentId + "' " +
                        "AND R.REGION_ID = O.REGION_ID " +
                        "AND O.AGENT_ID = A.AGENT_ID ";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT configparams " + "FROM rnode " +
                        "WHERE region_id = '" + regionId + "'";
            }

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                rs.next();
                configParams = rs.getString(1);

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }
        return configParams;
    }

    public int getNodeCount(String regionId, String agentId) {
        int count = -2;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT count(P.PLUGIN_ID) FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID ='" + regionId + "' " +
                        "AND A.AGENT_ID = '" + agentId + "' " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID";


            } else if((regionId != null) && (agentId == null)) {
                //region

                queryString = "SELECT count(A.agent_id) FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID ='" + regionId + "' AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID ";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT count(region_id) FROM rnode ";
            }

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                rs.next();
                count = rs.getInt(1);

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return count;
    }

    public String getResourceNodeSubmission(String resource_id) {
        String submission = null;
        try {

            String queryString = null;

            queryString = "SELECT submission FROM resourcenode WHERE resource_id='" + resource_id + "'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                rs.next();
                submission = rs.getString(1);


                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return submission;
    }

    public int setResourceNodeStatus(String resourceId, int status_code, String status_desc) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE resourcenode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                    + " WHERE resource_id='" + resourceId + "'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public int updateResource(String resourceId, int status_code, String status_desc, String submission) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE resourcenode SET status_code=" + status_code + ", status_desc='" + status_desc + "', submission='" + submission + "'"
                    + " WHERE resource_id='" + resourceId + "'";

            try (Connection conn = ds.getConnection()) {
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }


    public void addRNode(String region, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {

            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                Statement stmt = conn.createStatement();

                String insertRNodeString = "insert into rnode (region_id,status_code,status_desc,watchdog_period,watchdog_ts,configparams) " +
                        "values ('" + region + "'," + status_code + ",'" + status_desc + "'," +
                        watchdog_period + "," + watchdog_ts + ",'" +
                        configparams + "')";

                stmt.executeUpdate(insertRNodeString);
                stmt.close();
                conn.commit();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }


    private void cleanANodesfromRNode(String region_id) {

        try {

            List<String> agentList = getNodeList(region_id,null);
            for(String agent_id : agentList) {
                cleanPnodesFromAnode(region_id, agent_id);
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    private void cleanPnodesFromAnode(String region_id, String agent_id) {

        try {

            List<String> pluginList = getNodeList(region_id,agent_id);
            for(String plugin_id : pluginList) {
                removeNode(region_id,agent_id,plugin_id);
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void addANode(String agent, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {
            try (Connection conn = ds.getConnection()) {

                conn.setAutoCommit(false);

                Statement stmt = conn.createStatement();

                String insertANodeString = "insert into anode (agent_id,status_code,status_desc,watchdog_period,watchdog_ts,configparams) " +
                        "values ('" + agent + "'," + status_code + ",'" + status_desc + "'," +
                        watchdog_period + "," + watchdog_ts + ",'" +
                        configparams + "')";

                stmt.executeUpdate(insertANodeString);

                stmt.close();

                conn.commit();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }


    public boolean assoicateANodetoRNodeExist(String regionId, String agentId) {
        boolean exist = false;
        try {


            String queryString = null;

            //agent
            queryString = "SELECT COUNT(1) " + "FROM agentof " +
                        "WHERE region_id = '" + regionId + "'" +
                        "AND agent_id = '" + agentId + "'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();


                ResultSet rs = stmt.executeQuery(queryString);

                if (rs.next()) {
                    exist = rs.getBoolean(1);
                }
                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean assoicatePNodetoANodeExist(String agentId, String pluginId) {
        boolean exist = false;
        try {


            String queryString = null;

            //agent
            queryString = "SELECT COUNT(1) " + "FROM pluginof " +
                    "WHERE agent_id = '" + agentId + "'" +
                    "AND plugin_id = '" + pluginId + "'";

            try (Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();


                ResultSet rs = stmt.executeQuery(queryString);

                if (rs.next()) {
                    exist = rs.getBoolean(1);
                }
                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public void assoicateANodetoRNode(String region, String agent) {

        if(!assoicateANodetoRNodeExist(region,agent)) {
                try(Connection conn = ds.getConnection()) {

                    Statement stmt = conn.createStatement();

                    String insertANodeToRNode = "insert into agentof (region_id, agent_id) " +
                            "values ('" + region + "','" + agent + "')";

                    stmt.executeUpdate(insertANodeToRNode);
                    stmt.close();

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    public void assoicatePNodetoANode(String agent, String plugin) {

        if(!assoicatePNodetoANodeExist(agent,plugin)) {

            try(Connection conn = ds.getConnection()) {

                    Statement stmt = conn.createStatement();

                    String insertANodeToRNode = "insert into pluginof (agent_id, plugin_id) " +
                            "values ('" + agent + "','" + plugin + "')";

                    stmt.executeUpdate(insertANodeToRNode);
                    stmt.close();

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    public void updatePNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code) {


        try(Connection conn = ds.getConnection()) {

                Statement stmt = conn.createStatement();

                String insertPNodeString = "UPDATE pnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                    ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                    "WHERE plugin_id='" + plugin + "'";


                stmt.executeUpdate(insertPNodeString);
                //force update of pnode, so the next command does not fail.

                stmt.close();



        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public int addPNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code) {

        int status = -1;

        try(Connection conn = ds.getConnection()) {

                conn.setAutoCommit(false);

                Statement stmt = conn.createStatement();

                String insertPNodeString = "insert into pnode (plugin_id,status_code,status_desc,watchdog_period,watchdog_ts,pluginname,jarfile,version,md5,configparams,persistence_code) " +
                        "values ('" + plugin + "'," + status_code + ",'" + status_desc + "'," +
                        watchdog_period + "," + watchdog_ts + ",'" +
                        pluginname + "','" + jarfile + "','" + version + "','" + md5 + "','" +
                        configparams + "'," + persistence_code + ")";

                String insertPNodeToANode = "insert into pluginof (agent_id, plugin_id) " +
                        "values ('" + agent + "','" + plugin + "')";


                status = stmt.executeUpdate(insertPNodeString);
                //force update of pnode, so the next command does not fail.
                conn.commit();

                status = status + stmt.executeUpdate(insertPNodeToANode);


                stmt.close();

                conn.commit();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return status;
    }


    public int updateINodeAssignment(String inodeId, int status_code, String status_desc, String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE inode SET status_code=" + status_code + ", status_desc='" + status_desc +
                    "', region_id='" + regionId +"', agent_id='" + agentId + "', plugin_id='" + pluginId + "'" +
                    " WHERE inode_id='" + inodeId + "'";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public boolean inodeKPIExist(String inodeId) {
        boolean exist = false;
        try {

            String queryString = null;


            //region
            queryString = "SELECT COUNT(1) " + "FROM inodekpi " +
                    "WHERE inode_id = '" + inodeId + "'";

            try(Connection conn = ds.getConnection()) {
            Statement stmt = conn.createStatement();


            ResultSet rs = stmt.executeQuery(queryString);
            rs.next();
            exist = rs.getBoolean(1);

            rs.close();
            stmt.close();
            }

        } catch(Exception ex) {
            //ex.printStackTrace();
            System.out.println("inodeKPIExist()");
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());

        }
        return exist;
    }


    public void addInodeKPI(String inodeId, String kpiparams) {

        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String stmtString = null;

                stmtString = "insert into inodekpi (inode_id, kpiparams) " +
                        "values ('" + inodeId + "','" + kpiparams + "')";

                stmt.executeUpdate(stmtString);
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public int updateInodeKPI(String inodeId, String kpiparams) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE inodekpi SET kpiparams='" + kpiparams +"'"
                    + " WHERE inode_id='" + inodeId + "'";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public void addInode(String inodeId, String resourceId, int statusCode, String statusDesc, String configparams) {

        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String stmtString = null;

                stmtString = "insert into inode (inode_id, resource_id, status_code, status_desc, configparams) " +
                        "values ('" + inodeId + "','" + resourceId + "'," + statusCode + ",'" + statusDesc + "','" + configparams + "')";

                stmt.executeUpdate(stmtString);
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void addVnode(String vnodeId, String resourceId, String inodeId, String configparams) {

        try {
            try(Connection conn = ds.getConnection()) {

                Statement stmt = conn.createStatement();
                String stmtString = null;

                stmtString = "insert into vnode (vnode_id, resource_id, inode_id, configparams) " +
                        "values ('" + vnodeId + "','" + resourceId + "','" + inodeId + "','" + configparams + "')";

                stmt.executeUpdate(stmtString);
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public int getResourceNodeStatus(String resource_id) {
        int status_code = -1;
        try {

            String queryString = null;

            queryString = "SELECT status_code FROM resourcenode WHERE resource_id='" + resource_id + "'";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                rs.next();
                status_code = rs.getInt(1);

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return status_code;
    }

    public Map<String,String> getInodeMap(String inode_id) {
        Map<String,String> inodeMap = new HashMap<>();
        Statement stmt = null;
        try
        {

            String queryString = null;

            queryString = "SELECT * FROM inode WHERE inode_id='" + inode_id + "'";

            try(Connection conn = ds.getConnection()) {
                stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

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
                stmt.close();
            }

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return inodeMap;
    }

    public List<String> getINodeKPIList(String regionId, String agentId) {
        List<String> inodeKPIList = null;
        try
        {
            inodeKPIList = new ArrayList<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id = '" + regionId + "' AND agent_id = '" + agentId + "')";
            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id = '" + regionId + "')";
            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT inodekpi.kpiparams, inode.region_id, inode.agent_id FROM inodekpi " +
                        "INNER JOIN inode ON inodekpi.inode_id = inode.inode_id " +
                        "WHERE (region_id IS NOT NULL AND agent_id IS NOT NULL)";
            }
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);
                while (rs.next()) {
                    inodeKPIList.add(rs.getString(1));
                }

                rs.close();
                stmt.close();
            }

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return inodeKPIList;
    }

    public Map<String,String> getResourceNodeStatusMap(String resource_id) {
        Map<String,String> statusMap = new HashMap<>();
        try
        {

            String queryString = null;

            queryString = "SELECT resource_name, tenant_id, status_code, status_desc FROM resourcenode WHERE resource_id='" + resource_id + "'";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);
                rs.next();

                statusMap.put("pipeline_id", resource_id);
                statusMap.put("pipeline_name", rs.getString("resource_name"));
                statusMap.put("tenant_id", rs.getString("tenant_id"));
                statusMap.put("status_code", rs.getString("status_code"));
                statusMap.put("status_desc", rs.getString("status_desc"));

                rs.close();
                stmt.close();
            }

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
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
                Statement stmt = conn.createStatement();

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
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }


    private boolean tableExist(String tableName)  {
        boolean exist = false;

        Statement stmt = null;
        ResultSet rs = null;

        try {

            String queryString = null;

            queryString = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = N'" + tableName + "'";

            try(Connection conn = ds.getConnection()) {
                stmt = conn.createStatement();

                rs = stmt.executeQuery(queryString);
                rs.next();
                exist = rs.getBoolean(1);

                rs.close();
                stmt.close();
            }

        }
        catch (SQLException sqle) {
            //eat SQL exception, if tables does not exist this will throw
            System.out.println("SQL EXCEPTIO : "  + sqle.getMessage());
            sqle.printStackTrace();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    private int dropTable(String tableName) {
        int result = -1;
        try {

            String stmtString = null;

            stmtString = "DROP TABLE " + tableName;

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public boolean nodeExist(String regionId, String agentId, String pluginId) {
        boolean exist = false;
        try {


            String queryString = null;

            if((regionId == null) && (agentId == null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT COUNT(1) " + "FROM pnode " +
                        "WHERE plugin_id = '" + pluginId + "'";

            } if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT COUNT(1) " + "FROM pnode " +
                        "WHERE plugin_id = '" + pluginId + "'";

            } else if((regionId == null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT COUNT(1) " + "FROM anode " +
                        "WHERE agent_id = '" + agentId + "'";

            }else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT COUNT(1) " + "FROM anode " +
                        "WHERE agent_id = '" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT COUNT(1) " + "FROM rnode " +
                        "WHERE region_id = '" + regionId + "'";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();


                ResultSet rs = stmt.executeQuery(queryString);

                if (rs.next()) {
                    exist = rs.getBoolean(1);
                }
                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public void reassoicateANodes(String originalRegionId, String originalAgentId, String regionId, String agentId) {

        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "UPDATE AGENTOF " +
                        "SET region_id = '" + regionId + "', agent_id = '" + agentId + "'" +
                        "WHERE region_id = '" + originalRegionId + "' AND agent_id = '" + originalAgentId + "'";

                stmt.executeUpdate(queryString);

                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void reassoicatePNodes(String originalAgentId, String agentId) {

        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "UPDATE PLUGINOF " +
                        "SET agent_id = '" + agentId + "' " +
                        "WHERE agent_id = '" + originalAgentId + "'";

                stmt.executeUpdate(queryString);

                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        //return isRemoved;
    }

    public void purgeTransientPNodes(String regionId, String agentId) {
        //boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "DELETE FROM PNODE P WHERE NOT EXISTS ( " +
                        "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PLUGINOF PO " +
                        "WHERE R.REGION_ID = '" + regionId + "' " +
                        "AND A.AGENT_ID = '" + agentId + "' " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID " +
                        "AND P.PERSISTENCE_CODE > 9 )";


                stmt.executeUpdate(queryString);

                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        //return isRemoved;
    }

    public boolean removeINode(String inodeId) {
        boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "DELETE FROM inode " +
                        "WHERE inode_id = '" + inodeId + "'";


                if (stmt.executeUpdate(queryString) == 1) {
                    isRemoved = true;
                }
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return isRemoved;
    }

    public boolean removeVNode(String vnodeId) {
        boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "DELETE FROM vnode " +
                        "WHERE vnode_id = '" + vnodeId + "'";


                if (stmt.executeUpdate(queryString) == 1) {
                    isRemoved = true;
                }
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
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
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;

                queryString = "DELETE FROM resourcenode " +
                        "WHERE resource_id = '" + resourceId + "'";


                if (stmt.executeUpdate(queryString) == 1) {
                    isRemoved = true;
                }
                stmt.close();
            }

        }
        catch(Exception ex) {
            //logger.error("removePipeline " + ex.getMessage());
            ex.printStackTrace();
        }
        return isRemoved;
    }

    public Map<String,String> getCSTATE(String config_ts) {
        Map<String,String> cstateMap = null;
        try {

            String queryString = null;

            if(config_ts != null) {
                queryString = "SELECT CONFIG_TS, CURRENT_MODE, CURRENT_DESC, GLOBAL_REGION, " +
                        "GLOBAL_AGENT, REGIONAL_REGION, REGIONAL_AGENT, LOCAL_REGION, LOCAL_AGENT " +
                        "FROM CSTATE WHERE CONFIG_TS = " + config_ts;

            } else {
                queryString = "SELECT CONFIG_TS, CURRENT_MODE, CURRENT_DESC, GLOBAL_REGION, " +
                        "GLOBAL_AGENT, REGIONAL_REGION, REGIONAL_AGENT, LOCAL_REGION, LOCAL_AGENT " +
                        "FROM CSTATE WHERE (CONFIG_TS) IN " +
                        "( SELECT MAX(CONFIG_TS) " +
                        "  FROM CSTATE )";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);


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
                stmt.close();
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return cstateMap;
    }


    public boolean removeNode(String regionId, String agentId, String pluginId) {

        boolean isRemoved = false;
        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String queryString = null;
                //DELETE FROM table_name WHERE condition;
                if ((regionId != null) && (agentId != null) && (pluginId != null)) {
                    //plugin
                    queryString = "DELETE FROM PNODE WHERE PLUGIN_ID IN ( " +
                            "SELECT P.PLUGIN_ID " +
                            "FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                            "WHERE R.REGION_ID = '" + regionId + "' " +
                            "AND A.AGENT_ID = '" + agentId + "' " +
                            "AND P.PLUGIN_ID = '" + pluginId + "' " +
                            "AND R.REGION_ID = AO.REGION_ID " +
                            "AND AO.AGENT_ID = A.AGENT_ID " +
                            "AND A.AGENT_ID = PO.AGENT_ID " +
                            "AND PO.PLUGIN_ID = P.PLUGIN_ID)";

                } else if ((regionId == null) && (agentId == null) && (pluginId != null)) {
                    //plugin
                    queryString = "DELETE FROM pnode " +
                            "WHERE plugin_id = '" + pluginId + "'";

                } else if ((regionId == null) && (agentId != null) && (pluginId == null)) {
                    //agent
                    queryString = "DELETE FROM anode " +
                            "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "'";

                } else if ((regionId != null) && (agentId != null) && (pluginId == null)) {

                    //first remove agent plugins
                    cleanPnodesFromAnode(regionId, agentId);

                    //agent
                    queryString = "DELETE FROM ANODE WHERE AGENT_ID IN ( " +
                            "SELECT A.AGENT_ID " +
                            "FROM ANODE A, RNODE R, AGENTOF AO " +
                            "WHERE R.REGION_ID = '" + regionId + "' " +
                            "AND A.AGENT_ID = '" + agentId + "' " +
                            "AND R.REGION_ID = AO.REGION_ID " +
                            "AND AO.AGENT_ID = A.AGENT_ID)";

                } else if ((regionId != null) && (agentId == null) && (pluginId == null)) {
                    //first remove agents and plugins from region
                    cleanANodesfromRNode(regionId);

                    //region
                    queryString = "DELETE FROM rnode " +
                            "WHERE region_id = '" + regionId + "'";
                }

                //System.out.println("QUERY: " + queryString);

                int result = stmt.executeUpdate(queryString);


                //System.out.println("RESULT : " + result);
                if (result < 2) {
                    isRemoved = true;
                }
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return isRemoved;
    }

    public int updateWatchDogTS(String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = null;

            if((regionId == null) && (agentId == null) && (pluginId != null)) {
                //plugin

                queryString = "UPDATE pnode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE plugin_id='" + pluginId + "'";

            } else if((regionId == null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE region_id='" + regionId + "'";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public List<String> getNodeList(String regionId, String agentId) {

        List<String> nodeList = null;
        try {


            nodeList = new ArrayList<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                        "WHERE R.REGION_ID ='" + regionId + "' " +
                        "AND A.AGENT_ID = '" + agentId + "' " +
                        "AND R.REGION_ID = AO.REGION_ID " +
                        "AND AO.AGENT_ID = A.AGENT_ID " +
                        "AND A.AGENT_ID = PO.AGENT_ID " +
                        "AND PO.PLUGIN_ID = P.PLUGIN_ID";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT A.agent_id FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID ='" + regionId + "' AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID ";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM rnode ";
            }

            if(queryString != null) {

                try(Connection conn = ds.getConnection()) {
                    Statement stmt = conn.createStatement();

                    ResultSet rs = stmt.executeQuery(queryString);

                    while (rs.next()) {
                        String node = rs.getString(1);
                        if (!nodeList.contains(node)) {
                            nodeList.add(node);
                        }
                    }

                    rs.close();
                    stmt.close();
                }
            } else {
                System.out.println("getNodeList(regionId = " + regionId + " , agentId= " + agentId + ")");
                Thread.dumpStack();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeList;
    }


    public List<Map<String,String>> getPluginListMapByType(String actionPluginTypeId, String actionPluginTypeValue) {
        List<Map<String,String>> configMapList = null;
        try {

            configMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //plugin
            //queryString = "SELECT region_id, agent_id, plugin_id, configparams " + "FROM pnode " +
            //        "WHERE " + actionPluginTypeId + " = '" + actionPluginTypeValue + "'";

            queryString = "SELECT R.REGION_ID, A.AGENT_ID, P.PLUGIN_ID, P.CONFIGPARAMS " +
                    "FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO " +
                    "WHERE P." + actionPluginTypeId + " = '" + actionPluginTypeValue + "' " +
                    "AND R.REGION_ID = AO.REGION_ID " +
                    "AND AO.AGENT_ID = A.AGENT_ID " +
                    "AND A.AGENT_ID = PO.AGENT_ID " +
                    "AND PO.PLUGIN_ID = P.PLUGIN_ID";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                while (rs.next()) {
                    String configParamString = rs.getString("configparams");
                    Map<String, String> configMap = gson.fromJson(configParamString, type);
                    configMap.put("region", rs.getString("region_id"));
                    configMap.put("agent", rs.getString("agent_id"));
                    configMap.put("pluginid", rs.getString("plugin_id"));
                    configMapList.add(configMap);
                }

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return configMapList;
    }

    public Map<String,Integer> getNodeStatusCodeMap(String regionId, String agentId) {


        Map<String,Integer> nodeMap = null;
        try {

            nodeMap = new HashMap<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID, P.status_code FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO" +
                        "WHERE R.REGION_ID ='" + regionId + "'" +
                        "AND A.AGENT_ID = '" + agentId + "'" +
                        "AND R.REGION_ID = AO.REGION_ID" +
                        "AND AO.AGENT_ID = A.AGENT_ID" +
                        "AND A.AGENT_ID = PO.AGENT_ID";

            } else if((regionId != null) && (agentId == null)) {
                //region

                queryString = "SELECT A.agent_id, A.status_code FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID ='" + regionId + "' AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id, status_code FROM rnode ";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                while (rs.next()) {
                    String key = rs.getString(1);
                    if (!nodeMap.containsKey(key)) {
                        nodeMap.put(key, rs.getInt(2));
                    }
                }

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeMap;
    }

    public List<String> getStaleNodeList(String regionId, String agentId, int periodMultiplier) {

        List<String> nodeList = null;
        try {


            nodeList = new ArrayList<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent

                queryString = "SELECT P.PLUGIN_ID FROM ANODE A, RNODE R, AGENTOF AO, PNODE P, PLUGINOF PO" +
                        "WHERE R.REGION_ID ='" + regionId + "'" +
                        "AND A.AGENT_ID = '" + agentId + "'" +
                        "AND R.REGION_ID = AO.REGION_ID" +
                        "AND AO.AGENT_ID = A.AGENT_ID" +
                        "AND A.AGENT_ID = PO.AGENT_ID" +
                        "AND P.status_code=10 and ((" + System.currentTimeMillis() + " - P.watchdog_ts) > (P.watchdog_period *  " + periodMultiplier + "))";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT A.agent_id FROM ANODE A, RNODE R, AGENTOF O "
                        + "WHERE R.REGION_ID ='" + regionId + "' AND R.REGION_ID = O.REGION_ID AND O.AGENT_ID = A.AGENT_ID "
                        + "AND A.status_code=10 and ((" + System.currentTimeMillis() + " - A.watchdog_ts) > (A.watchdog_period * " + periodMultiplier + "))";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM rnode "
                        + "WHERE status_code=10 and ((" + System.currentTimeMillis() + " - watchdog_ts) > (watchdog_period * " + periodMultiplier + "))";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                while (rs.next()) {
                    String node = rs.getString(1);
                    if (!nodeList.contains(node)) {
                        nodeList.add(node);
                    }
                }

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeList;
    }

    public int setNodeStatusCode(String regionId, String agentId, String pluginId, int status_code, String status_desc) {



        int queryReturn = -1;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin

                queryString = "UPDATE pnode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                        + " WHERE plugin_id='" + pluginId + "'";

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                        + " WHERE agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                        + " WHERE region_id='" + regionId + "'";
            }

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                queryReturn = stmt.executeUpdate(queryString);

                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public void addResource(String resourceId, String resourceName, int tenantId, int statusCode, String statusDesc, String submission) {

        try {


            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String stmtString = null;

                stmtString = "insert into resourcenode (resource_id, resource_name, tenant_id, status_code, status_desc, submission) " +
                        "values ('" + resourceId + "','" + resourceName + "'," + tenantId + "," + statusCode + ",'" + statusDesc + "','" + submission + "')";


                stmt.executeUpdate(stmtString);
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void addTenant(int tenantId, String tenantName) {

        try {
            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();
                String stmtString = null;

                stmtString = "insert into tenantnode (tenant_id,tenantname) " +
                        "values (" + tenantId + ",'" + tenantName + "')";

                stmt.executeUpdate(stmtString);
                stmt.close();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public List<String> getResourceNodeList() {
        List<String> nodeList = null;
        try {

            nodeList = new ArrayList<>();
            String queryString = null;

            queryString = "SELECT resource_id FROM resourcenode ";

            try(Connection conn = ds.getConnection()) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(queryString);

                while (rs.next()) {
                    String node = rs.getString(1);
                    if (!nodeList.contains(node)) {
                        nodeList.add(node);
                    }
                }

                rs.close();
                stmt.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeList;
    }


    public static DataSource setupDataSource(String connectURI) {
        return setupDataSource(connectURI,null,null);
    }

    public static DataSource setupDataSource(String connectURI, String login, String password) {
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
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);



        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);



        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
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
            return null;
        }
        return compressedData;
    }

}