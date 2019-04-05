package io.cresco.agent.controller.agentcontroller;


import com.google.gson.Gson;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.app.gEdge;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import javafx.beans.binding.IntegerBinding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class PluginNode {

    private DBInterfaceImpl gdb;
    private PluginBuilder plugin;
    private Gson gson;

    //private Logger logger =
    private String pluginID;
    private String name;
    private String version;
    private String MD5;
    private String jarPath;
    private boolean active = false;
    private int status_code = 3;
    private String status_desc = "Plugin Configuration Created";
    private long watchdog_ts = 0;
    private int watchdog_period = 0;
    private long runtime = 0;
    private long bundleID;

    private PluginService pluginService;
    private Map<String,Object> configMap;
    private List<gEdge> edgeList;

    //private String inode_id;
    //private String resource_id;

    /*
    status_code = 3; //agentcontroller init
    status_code = 7; //Plugin instance could not be started
    status_code = 8; //agentcontroller disabled
    status_code = 9; //Plugin Bundle could not be installed or started
    status_code = 10; //started and working
    status_code = 40; //WATCHDOG check STALE
    status_code = 41; //Missing status parameter
    status_code = 50; //WATCHDOG check LOST
    status_code = 80; //failed to start
    status_code = 90; //Exception on timeout shutdown
    status_code = 91; //Exception on timeout verification to confirm down
    status_code = 92; //timeout on disable verification
     */

    public String getConfigValue(String configKey) {

        String strValue = null;
        Object value = configMap.get(configKey);
        if(value != null) {
            strValue = (String)value;
        }
        //return configMap.get(configKey);
        /*
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + ":" + value);
        }
        */
        return strValue;
    }


    public PluginNode(PluginBuilder plugin, DBInterfaceImpl gdb, long bundleID, String pluginID, Map<String,Object> configMap, List<gEdge> edgeList) throws IOException {
        this.plugin = plugin;
        this.gdb = gdb;
        this.gson = new Gson();
        this.bundleID = bundleID;
        this.pluginID = pluginID;
        this.name = (String)configMap.get("pluginname");
        this.version = (String)configMap.get("version");
        this.MD5 = (String)configMap.get("md5");
        this.jarPath = (String)configMap.get("jarfile");
        this.configMap = configMap;
        this.edgeList = edgeList;

        /*
        URL url = getClass().getClassLoader().getResource(jarPath);
        Manifest manifest = null;
        if(url != null) {
            manifest = new JarInputStream(getClass().getClassLoader().getResourceAsStream(jarPath)).getManifest();
            MD5 = plugin.getMD5(getClass().getClassLoader().getResourceAsStream(url.getPath()));

        } else {
            //url = new File(jarPath).toURI().toURL();
            manifest = new JarInputStream(new FileInputStream(new File(this.jarPath))).getManifest();
            MD5 = plugin.getMD5(jarPath);

        }

        Attributes mainAttributess = manifest.getMainAttributes();
        name = mainAttributess.getValue("Bundle-SymbolicName");
        version = mainAttributess.getValue("Bundle-Version");


        //persistance code < 9, keep config and try to restart plugin on next restart
        int persistence_code = 0;

        if(configMap.containsKey("persistence_code")) {
            persistence_code = Integer.parseInt(configMap.get("persistence_code").toString());
        }
        */

        //System.out.println("NAME: " + name);
        //System.out.println("VERSION: " + version);
        //System.out.println("JAR PATH: " + jarPath);
        //System.out.println("MD5: " + MD5);

        //public void addNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {
        //
        //create DB entry for plugin

        int persistence_code = 0;

        if(configMap.containsKey("persistence_code")) {
            persistence_code = Integer.parseInt(configMap.get("persistence_code").toString());
        }

        gdb.addPNode(plugin.getAgent(), pluginID,status_code,status_desc,watchdog_period,watchdog_ts,name,jarPath,version,MD5, gson.toJson(configMap), persistence_code);

    }

    public long getBundleID() {
        return bundleID;
    }

    public Map<String, Object> getConfigMap() {
        return configMap;
    }

    public Map<String, String> exportParamMap() {
        //return configMap;
        Map<String, String> paramMap = new HashMap<>();

        Iterator it = configMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            paramMap.put((String) pair.getKey(), (String) pair.getValue());
        }
        return paramMap;
    }

    public List<gEdge> getEdgeList() {
        return edgeList;
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public long getWatchdogTimer() {
        return watchdog_period;
    }

    public void setWatchDogTS(long watchdog_ts) {
        this.watchdog_ts = watchdog_ts;
    }

    public long getWatchdogTS() {
        return watchdog_ts;
    }

    public void setWatchDogTimer(long watchdogtimer) {
        this.watchdog_period = watchdog_period;
    }

    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public int getStatus_code() {return status_code;}

    public void setStatus(int status_code, String status_desc) {
        this.status_code = status_code;
        this.status_desc = status_desc;
        //update code
        gdb.setNodeStatusCode(plugin.getRegion(),plugin.getAgent(),pluginID,status_code,status_desc);
    }

    public String getStatus_desc() {return this.status_desc;}

    public PluginService getPluginService() {
        return this.pluginService;
    }

    public void setPluginService(PluginService pluginService) {
        this.pluginService = pluginService;
        this.active = true;
    }

    /*
    public String getInodeId() {return inode_id;}

    public void setInodeId(String inode_id) {
        this.inode_id = inode_id;
    }

    public String getResourceId() {return resource_id;}

    public void setResourceId(String resource_id) {
        this.resource_id = resource_id;
    }
    */

    public boolean getActive() {
        return active;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        sb.append("\"id\":\"");
        sb.append(pluginID);
        sb.append("\",");

        sb.append("\"jar\":\"");
        sb.append(jarPath);
        sb.append("\",");

        sb.append("\"name\":\"");
        sb.append(name);
        sb.append("\",");

        sb.append("\"version\":\"");
        sb.append(version);
        sb.append("\",");

        sb.append("\"active\":");
        sb.append(active);

        sb.append("}");
        return sb.toString();
    }
}
