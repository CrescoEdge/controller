package io.cresco.agent.controller.agentcontroller;


import io.cresco.library.plugin.PluginService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class PluginNode {
    //private Logger logger =
    private String pluginID;
    private String jarPath;
    private String pluginName;
    private String name;
    private String version;
    private boolean active = false;
    private int status_code = 3;
    private String status_desc = "Plugin Configuration Created";
    private long watchdog_ts = 0;
    private long watchdogtimer = 0;
    private long runtime = 0;
    private long bundleID;

    private PluginService pluginService;
    private Map<String,Object> configMap;

    //private String inode_id;
    //private String resource_id;

    /*
    status_code = 3; //agentcontroller init
    status_code = 7; //Plugin instance could not be started
    status_code = 8; //agentcontroller disabled
    status_code = 9; //Plugin Bundle could not be installed or started
    status_code = 10; //started and working
    status_code = 40; //WATCHDOG check failed with agent
    status_code = 41; //Missing status parameter
    status_code = 80; //failed to start
    status_code = 90; //Exception on timeout shutdown
    status_code = 91; //Exception on timeout verification to confirm down
    status_code = 92; //timeout on disable verification
     */

    public PluginNode(long bundleID, String pluginID, String pluginName, String jarPath, Map<String,Object> configMap) throws IOException {

        this.bundleID = bundleID;
        this.pluginID = pluginID;
        this.jarPath = jarPath;
        this.configMap = configMap;

        URL url = getClass().getClassLoader().getResource(jarPath);
        Manifest manifest = null;
        if(url != null) {
            manifest = new JarInputStream(getClass().getClassLoader().getResourceAsStream(jarPath)).getManifest();
        } else {
            //url = new File(jarPath).toURI().toURL();
            manifest = new JarInputStream(new FileInputStream(new File(this.jarPath))).getManifest();
        }

        Attributes mainAttributess = manifest.getMainAttributes();
        name = mainAttributess.getValue("Bundle-SymbolicName");
        version = mainAttributess.getValue("Bundle-Version");


        //URL url = new File(jarPath).toURI().toURL();
        //URLClassLoader loader = new URLClassLoader(new URL[] {new File(jarPath).toURI().toURL()}, this.getClass().getClassLoader());
        //ResourceFinder finder = new ResourceFinder("META-INF/services", loader, url);

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
        return watchdogtimer;
    }

    public void setWatchDogTS(long watchdog_ts) {
        this.watchdog_ts = watchdog_ts;
    }

    public long getWatchdogTS() {
        return watchdog_ts;
    }

    public void setWatchDogTimer(long watchdogtimer) {
        this.watchdogtimer = watchdogtimer;
    }

    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public int getStatus_code() {return status_code;}

    public void setStatus_code(int status_code) {
        this.status_code = status_code;
    }

    public void setStatus_desc(String status_desc) {
        this.status_desc = status_desc;
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
