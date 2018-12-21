package io.cresco.agent.controller.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DBEngine {

    private DataSource ds;
    private Gson gson;
    private PluginBuilder plugin;
    private CLogger logger;
    private ControllerEngine controllerEngine;


    public DBEngine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DBEngine.class.getName(),CLogger.Level.Info);

        try {

            this.gson = new Gson();

            String defaultDBName = "cresco-controller";
            String dbName  = plugin.getConfig().getStringParam("db_name",defaultDBName);

            if(dbName.equals(defaultDBName)) {
                File dbsource = new File(dbName);
                if (dbsource.exists()) {
                    delete(dbsource);
                }
            }

            String dbDriver = plugin.getConfig().getStringParam("db_driver","org.apache.derby.jdbc.EmbeddedDriver");
            String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc","jdbc:derby:" + dbName + ";create=true");

            String dbUserName = plugin.getConfig().getStringParam("db_username");
            String dbPassword = plugin.getConfig().getStringParam("db_password");

            Class.forName(dbDriver);

            if((dbUserName != null) && (dbPassword != null)) {
                ds = setupDataSource(dbConnectionString,dbUserName, dbPassword);
            } else {
                ds = setupDataSource(dbConnectionString);
            }

            //Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            //ds = setupDataSource("jdbc:derby:demo;create=true");

            //Class.forName("com.mysql.cj.jdbc.Driver");
            //ds = setupDataSource("jdbc:mysql://localhost/cresco?characterEncoding=UTF-8","root", "codeman01");


            initDB();

            addTenant(0,"default tenant");
            addResource("sysinfo_resource","Performance Metrics",0,0,"added by DBEngine by default", null);


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



    public void updateNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            if((region != null) && (agent != null) && (plugin != null)) {
                //add plugin metadata where it exist

                String pluginname = "unknown";
                String jarfile = "unknown";
                String version = "unknown";
                String md5 = "unknown";

                if(configparams != null) {
                    Type type = new TypeToken<Map<String, String>>(){}.getType();
                    Map<String,String> configMap = gson.fromJson(configparams, type);

                    if(configMap.containsKey("pluginname")) {
                        pluginname = configMap.get("pluginname");
                    }
                    if(configMap.containsKey("jarfile")) {
                        jarfile = configMap.get("jarfile");
                    }
                    if(configMap.containsKey("version")) {
                        version = configMap.get("version");
                    }
                    if(configMap.containsKey("md5")) {
                        md5 = configMap.get("md5");
                    }

                }

                stmtString = "UPDATE pnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                        ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                        "WHERE region_id='" + region + "' AND agent_id='" + agent + "' AND plugin_id='" + plugin + "'";

            } else if((region != null) && (agent != null) && (plugin == null)) {
                stmtString = "UPDATE anode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                        ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                        "WHERE region_id='" + region + "' AND agent_id='" + agent + "'";

            } else if((region != null) && (agent == null) && (plugin == null)) {
                stmtString = "UPDATE rnode SET status_code=" + status_code + ", status_desc='" + status_desc + "', watchdog_period=" + watchdog_period +
                        ", watchdog_ts=" + watchdog_ts + ", configparams='" + configparams + "' " +
                        "WHERE region_id='" + region + "'";
            }

            System.out.println("SQL: [" + stmtString + "]");

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                inodeResourceList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                inodeResourceList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                status_code = rs.getInt(1);
            }

            rs.close();
            stmt.close();
            conn.close();

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


            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public String getNodeConfigParams(String regionId, String agentId, String pluginId) {
        String configParams = null;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT configparams " + "FROM pnode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "' and plugin_id = '" + pluginId + "'";
            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT configparams " + "FROM anode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT configparams " + "FROM rnode " +
                        "WHERE region_id = '" + regionId + "'";
            }


            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();


            ResultSet rs = stmt.executeQuery(queryString);

            rs.next();
            configParams = rs.getString(1);

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return configParams;
    }

    public int getNodeCount(String regionId, String agentId) {
        int count = -2;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent
                queryString = "SELECT count(plugin_id) FROM pnode "
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT count(agent_id) FROM anode "
                        + " WHERE region_id='" + regionId + "'";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT count(region_id) FROM anode ";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            rs.next();
            count = rs.getInt(1);

            rs.close();
            stmt.close();
            conn.close();

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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                submission = rs.getString(1);
            }

            rs.close();
            stmt.close();
            conn.close();

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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public int updateResource(String resourceId, int status_code, String status_desc, String submission) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE resourcenode SET status_code=" + status_code + ", status_desc='" + status_desc + "', submission='" + submission +"'"
                    + " WHERE resource_id='" + resourceId + "'";

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public void addNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            if((region != null) && (agent != null) && (plugin != null)) {
                //add plugin metadata where it exist

                String pluginname = "unknown";
                String jarfile = "unknown";
                String version = "unknown";
                String md5 = "unknown";

                if(configparams != null) {
                    Type type = new TypeToken<Map<String, String>>(){}.getType();
                    Map<String,String> configMap = gson.fromJson(configparams, type);

                    if(configMap.containsKey("pluginname")) {
                        pluginname = configMap.get("pluginname");
                    }
                    if(configMap.containsKey("jarfile")) {
                        jarfile = configMap.get("jarfile");
                    }
                    if(configMap.containsKey("version")) {
                        version = configMap.get("version");
                    }
                    if(configMap.containsKey("md5")) {
                        md5 = configMap.get("md5");
                    }

                }

                stmtString = "insert into pnode (region_id,agent_id,plugin_id,status_code,status_desc,watchdog_period,watchdog_ts,pluginname,jarfile,version,md5,configparams) " +
                        "values ('" + region + "','" + agent + "','" + plugin + "'," + status_code + ",'" + status_desc + "'," +
                        watchdog_period + "," + watchdog_ts + ",'" +
                        pluginname + "','" + jarfile + "','" + version + "','" + md5 + "','" +
                        configparams + "')";
            } else if((region != null) && (agent != null) && (plugin == null)) {
                stmtString = "insert into anode (region_id,agent_id,status_code,status_desc,watchdog_period,watchdog_ts,configparams) " +
                        "values ('" + region + "','" + agent + "'," + status_code + ",'" + status_desc + "'," + watchdog_period + "," + watchdog_ts + ",'" + configparams + "')";
            } else if((region != null) && (agent == null) && (plugin == null)) {
                stmtString= "insert into rnode (region_id,status_code,status_desc,watchdog_period, watchdog_ts,configparams) " +
                        "values ('" + region + "'," + status_code + ",'" + status_desc + "'," + watchdog_period + "," + watchdog_ts + ",'" + configparams + "')";
            }
            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public int updateINodeAssignment(String inodeId, int status_code, String status_desc, String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE inode SET status_code=" + status_code + ", status_desc='" + status_desc +
                    "', region_id='" + regionId +"', agent_id='" + agentId + "', plugin_id='" + pluginId + "'" +
                    " WHERE inode_id='" + inodeId + "'";

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

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


            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();


            ResultSet rs = stmt.executeQuery(queryString);
            rs.next();
            exist = rs.getBoolean(1);

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public String getInodeKPI(String inodeId) {
        String inodeKPI = null;
        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String queryString = null;

            queryString = "select kpiparams from inodekpi where inode_id='" + inodeId + "'";


            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                inodeKPI = rs.getString(1);
            }

            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return inodeKPI;
    }

    public void addInodeKPI(String inodeId, String kpiparams) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            stmtString = "insert into inodekpi (inode_id, kpiparams) " +
                    "values ('" + inodeId + "','" + kpiparams + "')";

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public void addInode(String inodeId, String resourceId, int statusCode, String statusDesc, String configparams) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            stmtString = "insert into inode (inode_id, resource_id, status_code, status_desc, configparams) " +
                    "values ('" + inodeId + "','" + resourceId + "'," + statusCode + ",'" + statusDesc + "','" + configparams + "')";

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void addVnode(String vnodeId, String resourceId, String inodeId, String configparams) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            stmtString = "insert into vnode (vnode_id, resource_id, inode_id, configparams) " +
                    "values ('" + vnodeId + "','" +  resourceId + "','" + inodeId + "','" + configparams  + "')";

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public int getResourceNodeStatus(String resource_id) {
        int status_code = -1;
        try {

            String queryString = null;

            queryString = "SELECT status_code FROM resourcenode WHERE resource_id='" + resource_id + "'";

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                status_code = rs.getInt(1);
            }

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return status_code;
    }

    public Map<String,String> getInodeMap(String inode_id) {
        Map<String,String> inodeMap = new HashMap<>();
        try
        {

            String queryString = null;

            queryString = "SELECT region_id, agent_id, plugin_id FROM inode WHERE inode_id='" + inode_id + "'";

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);
            rs.next();

            inodeMap.put("region_id",rs.getString("region_id"));
            inodeMap.put("agent_id",rs.getString("agent_id"));
            inodeMap.put("plugin_id",rs.getString("plugin_id"));

            rs.close();
            stmt.close();
            conn.close();

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
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);
            while(rs.next()) {
                inodeKPIList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);
            rs.next();

            statusMap.put("pipeline_id",resource_id);
            statusMap.put("pipeline_name",rs.getString("resource_name"));
            statusMap.put("tenant_id",rs.getString("tenant_id"));
            statusMap.put("status_code",rs.getString("status_code"));
            statusMap.put("status_desc",rs.getString("status_desc"));

            rs.close();
            stmt.close();
            conn.close();

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return statusMap;
    }

    public void initDB() {

        String createRNode = "CREATE TABLE rnode" +
                "(" +
                "   region_id varchar(43) primary key NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   configparams varchar(255)" +
                ")";

        String createANode = "CREATE TABLE anode" +
                "(" +
                "   region_id varchar(43) NOT NULL," +
                "   agent_id varchar(42) primary key NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   configparams varchar(255)," +
                "   FOREIGN KEY (region_id) REFERENCES rnode(region_id) " +
                ")";

        String createPNode = "CREATE TABLE pnode" +
                "(" +
                "   region_id varchar(43) NOT NULL," +
                "   agent_id varchar(42) NOT NULL," +
                "   plugin_id varchar(43) NOT NULL," +
                "   status_code int," +
                "   status_desc varchar(255)," +
                "   watchdog_period int," +
                "   watchdog_ts bigint," +
                "   pluginname varchar(255)," +
                "   jarfile varchar(255)," +
                "   version varchar(255)," +
                "   md5 varchar(255)," +
                "   configparams blob," +
                "   FOREIGN KEY (region_id) REFERENCES rnode(region_id), " +
                "   FOREIGN KEY (agent_id) REFERENCES anode(agent_id), " +
                "   CONSTRAINT pNodeID PRIMARY KEY (region_id, agent_id, plugin_id)" +
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
                "   submission blob," +
                "   FOREIGN KEY (tenant_id) REFERENCES tenantnode(tenant_id)" +
                ")";

        String createVnode = "CREATE TABLE vnode" +
                "(" +
                "   vnode_id varchar(42) primary key NOT NULL," +
                "   resource_id varchar(45) NOT NULL," +
                "   inode_id varchar(42)," +
                "   configparams blob," +
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
                "   configparams blob NOT NULL," +
                "   kpiparams blob," +
                "   FOREIGN KEY (resource_id) REFERENCES resourcenode(resource_id)" +
                ")";

        String createInodeKPI = "CREATE TABLE inodekpi" +
                "(" +
                //"   inodekpi_id varchar(42) primary key NOT NULL," +
                "   inode_id varchar(43)," +
                "   kpiparams blob" +
                //"   FOREIGN KEY (inode_id) REFERENCES inode(inode_id)" +
                ")";


        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(createRNode);
            stmt.executeUpdate(createANode);
            stmt.executeUpdate(createPNode);

            stmt.executeUpdate(createTenantNode);
            stmt.executeUpdate(createResourceNode);
            stmt.executeUpdate(createInode);
            stmt.executeUpdate(createVnode);

            stmt.executeUpdate(createInodeKPI);


            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public boolean nodeExist(String regionId, String agentId, String pluginId) {
        boolean exist = false;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "SELECT COUNT(1) " + "FROM pnode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "' and plugin_id = '" + pluginId + "'";
            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "SELECT COUNT(1) " + "FROM anode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "SELECT COUNT(1) " + "FROM rnode " +
                        "WHERE region_id = '" + regionId + "'";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();


            ResultSet rs = stmt.executeQuery(queryString);
            rs.next();
            exist = rs.getBoolean(1);

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean removeINode(String inodeId) {
        boolean isRemoved = false;
        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String queryString = null;
            //DELETE FROM table_name WHERE condition;

                queryString = "DELETE FROM inode " +
                        "WHERE inode_id = '" + inodeId + "'";


            if(stmt.executeUpdate(queryString) == 1) {
                isRemoved = true;
            }
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return isRemoved;
    }

    public boolean removeVNode(String vnodeId) {
        boolean isRemoved = false;
        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String queryString = null;
            //DELETE FROM table_name WHERE condition;

            queryString = "DELETE FROM vnode " +
                    "WHERE vnode_id = '" + vnodeId + "'";


            if(stmt.executeUpdate(queryString) == 1) {
                isRemoved = true;
            }
            stmt.close();
            conn.close();
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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String queryString = null;
            //DELETE FROM table_name WHERE condition;

            queryString = "DELETE FROM resourcenode " +
                    "WHERE resource_id = '" + resourceId + "'";


            if(stmt.executeUpdate(queryString) == 1) {
                isRemoved = true;
            }
            stmt.close();
            conn.close();



        }
        catch(Exception ex) {
            //logger.error("removePipeline " + ex.getMessage());
            ex.printStackTrace();
        }
        return isRemoved;
    }


    public boolean removeNode(String regionId, String agentId, String pluginId) {
        boolean isRemoved = false;
        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String queryString = null;
            //DELETE FROM table_name WHERE condition;
            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin
                queryString = "DELETE FROM pnode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "' and plugin_id = '" + pluginId + "'";
            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "DELETE FROM anode " +
                        "WHERE region_id = '" + regionId + "' and agent_id = '" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "DELETE FROM rnode " +
                        "WHERE region_id = '" + regionId + "'";
            }

            int result = stmt.executeUpdate(queryString);

            //System.out.println("RESULT : " + result);
            if(result < 2) {
                isRemoved = true;
            }
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return isRemoved;
    }

    public int updateWatchDogTS(String regionId, String agentId, String pluginId) {
        int queryReturn = -1;
        try {

            String queryString = null;

            if((regionId != null) && (agentId != null) && (pluginId != null)) {
                //plugin

                queryString = "UPDATE pnode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "' and plugin_id='" + pluginId + "'";

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET watchdog_ts = + " + System.currentTimeMillis()
                        + " WHERE region_id='" + regionId + "'";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

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
                queryString = "SELECT plugin_id FROM pnode "
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT agent_id FROM anode "
                        + " WHERE region_id='" + regionId + "'";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM anode ";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                nodeList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeList;
    }

    public List<String> getPNodeListByType(String regionId, String agentId) {
        List<String> nodeList = null;
        try {

            nodeList = new ArrayList<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent
                queryString = "SELECT plugin_id FROM pnode "
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT agent_id FROM anode "
                        + " WHERE region_id='" + regionId + "'";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM anode ";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                nodeList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

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
            queryString = "SELECT region_id, agent_id, plugin_id, configparams " + "FROM pnode " +
                    "WHERE " + actionPluginTypeId + " = '" + actionPluginTypeValue + "'";


            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while(rs.next()) {
                String configParamString = rs.getString("configparams");
                Map<String,String> configMap = gson.fromJson(configParamString, type);
                configMap.put("region",rs.getString("region_id"));
                configMap.put("agent",rs.getString("agent_id"));
                configMap.put("pluginid",rs.getString("plugin_id"));
                configMapList.add(configMap);
            }

            /*
            String region = perfMap.get("region");
                String agent = perfMap.get("agent");
                String pluginID = perfMap.get("pluginid");

             */
            rs.close();
            stmt.close();
            conn.close();

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
                queryString = "SELECT plugin_id, status_code FROM pnode "
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT agent_id, status_code FROM anode "
                        + " WHERE region_id='" + regionId + "'";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id, status_code FROM anode ";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                nodeMap.put(rs.getString(1),rs.getInt(2));
            }

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeMap;
    }

    public List<String> getStaleNodeList(String regionId, String agentId) {
        List<String> nodeList = null;
        try {


            nodeList = new ArrayList<>();
            String queryString = null;

            if((regionId != null) && (agentId != null)) {
                //agent
                queryString = "SELECT plugin_id FROM pnode "
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "' and status_code=10 and ((" + System.currentTimeMillis() + " - watchdog_ts) > (watchdog_period * 3))";

            } else if((regionId != null) && (agentId == null)) {
                //region
                queryString = "SELECT agent_id FROM anode "
                        + " WHERE region_id='" + regionId + "' and status_code=10 and ((" + System.currentTimeMillis()  + " - watchdog_ts) > (watchdog_period * 3))";

            }
            else if((regionId == null) && (agentId == null)) {
                //global
                queryString = "SELECT region_id FROM anode "
                        + "WHERE status_code=10 and ((" + System.currentTimeMillis() + " - watchdog_ts) > (watchdog_period * 3))";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                nodeList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

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
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "' and plugin_id='" + pluginId + "'";

            } else if((regionId != null) && (agentId != null) && (pluginId == null)) {
                //agent
                queryString = "UPDATE anode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                        + " WHERE region_id='" + regionId + "' and agent_id='" + agentId + "'";

            } else if((regionId != null) && (agentId == null) && (pluginId == null)) {
                //region
                queryString = "UPDATE rnode SET status_code=" + status_code + ", status_desc='" + status_desc + "'"
                        + " WHERE region_id='" + regionId + "'";
            }

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            queryReturn = stmt.executeUpdate(queryString);

            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public void addResource(String resourceId, String resourceName, int tenantId, int statusCode, String statusDesc, String submission) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            stmtString = "insert into resourcenode (resource_id, resource_name, tenant_id, status_code, status_desc, submission) " +
                    "values ('" + resourceId + "','" + resourceName + "'," + tenantId + "," + statusCode + ",'" + statusDesc + "','" + submission + "')";

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void addTenant(int tenantId, String tenantName) {

        try {
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            String stmtString = null;

            stmtString = "insert into tenantnode (tenant_id,tenantname) " +
                    "values (" + tenantId + ",'" + tenantName + "')";

            stmt.executeUpdate(stmtString);
            stmt.close();
            conn.close();
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

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(queryString);

            while (rs.next()) {
                nodeList.add(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeList;
    }

    public void printnodes() {
        try {

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("SELECT * FROM pnode");


            // print out query result
            while (rs.next()) {
                System.out.printf("%s\t%s\t%s\n", rs.getString("region_id"), rs.getString("agent_id"), rs.getString("plugin_id"));
            }
            rs.close();

            rs = stmt.executeQuery("SELECT * FROM anode");

            // print out query result
            while (rs.next()) {
                System.out.printf("%s\t%s\n", rs.getString("region_id"), rs.getString("agent_id"));
            }
            rs.close();

            rs = stmt.executeQuery("SELECT * FROM rnode");

            // print out query result
            while (rs.next()) {
                System.out.printf("%s\n", rs.getString("region_id"));
            }
            rs.close();


            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
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


    public void connectionToDerby() throws SQLException {
        // -------------------------------------------
        // URL format is
        // jdbc:derby:<local directory to save data>
        // -------------------------------------------
        String dbUrl = "jdbc:derby:demo;create=true";
        //conn = DriverManager.getConnection(dbUrl);
    }

    public void printDB() {
        try {

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();

            //System.out.println("NumActive: " + ds.getNumActive());
            //System.out.println("NumIdle: " + ds.getNumIdle());

            //Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM users");

            // print out query result
            while (rs.next()) {
                //System.out.printf("%d\t%s\n", rs.getInt("id"), rs.getString("name"));
                //System.out.print(".");
                String s = rs.getString("name");
            }
            rs.close();
            stmt.close();
            conn.close();

        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public void normalDbUsage() throws SQLException {
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("Create table users (id int primary key, name varchar(36))");
        stmt.close();
        conn.close();

    }

    public void addRecord(int number, String name) {
        try {
            //System.out.print("+");
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("insert into users values (" + number + ",'" + name + "')");
            stmt.close();
            conn.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
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

    public byte[] compressString(String str) {

        byte[] dataToCompress = str.getBytes(StandardCharsets.UTF_8);
        return dataCompress(dataToCompress);
    }

    public String uncompressString(String compressedData) {
        if (compressedData == null)
            return null;
        try {
            byte[] exportDataRawCompressed = DatatypeConverter.parseBase64Binary(compressedData);
            InputStream iss = new ByteArrayInputStream(exportDataRawCompressed);
            InputStream is = new GZIPInputStream(iss);
            return new Scanner(is,"UTF-8").useDelimiter("\\A").next();
        } catch (IOException e) {
            return null;
        }
    }

}
