package io.cresco.agent.core;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

import java.io.File;
import java.util.*;

public class Config {

    private HierarchicalINIConfiguration iniConfObj;

    public Config(String configFile) throws ConfigurationException {
        iniConfObj = new HierarchicalINIConfiguration(configFile);
        iniConfObj.setDelimiterParsingDisabled(true);
        iniConfObj.setAutoSave(true);

    }

    public String getPluginConfigString() {
        SubnodeConfiguration sObj = iniConfObj.getSection("general");
        //final Map<String,String> result=new TreeMap<String,String>();
        StringBuilder sb = new StringBuilder();
        final Iterator it = sObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final String value = sObj.getString(key.toString());
            //result.put(key.toString(),value);
            sb.append(key.toString()).append("=").append(value).append(",");

        }
        return sb.toString().substring(0, sb.length() - 1);
        //return result;
    }

    public Map<String,Object> getConfigMap() {
        return getConfigMap("general");
    }

    public Map<String,Object> getConfigMap(String section) {

        Map<String,Object> configMap = new HashMap<>();

        SubnodeConfiguration sObj = iniConfObj.getSection(section);
        //final Map<String,String> result=new TreeMap<String,String>();
        //StringBuilder sb = new StringBuilder();
        final Iterator it = sObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final Object value = sObj.getString(key.toString());
            configMap.put(key.toString(),value);
            //result.put(key.toString(),value);
            //sb.append(key.toString() + "=" + value + ",");

        }
        //return sb.toString().substring(0, sb.length() - 1);
        //return result;
        return  configMap;
    }

    public List<String> getPluginList(int isEnabled) {
        //isEnabled : 0=disabled , 1 enabled

        List<String> enabledPlugins = new ArrayList<>();
        SubnodeConfiguration sObj = iniConfObj.getSection("plugins");
        Iterator it = sObj.getKeys();
        while (it.hasNext()) {
            Object key = it.next();
            int value = 0;
            value = Integer.parseInt(sObj.getString(key.toString()));
            if (value == isEnabled) {
                enabledPlugins.add(key.toString());
            }
        }
        return enabledPlugins;
    }

    public int getIntParams(String section, String param) {
        int return_param = -1;
        try {
            SubnodeConfiguration sObj = iniConfObj.getSection(section);
            return_param = Integer.parseInt(sObj.getString(param));
        } catch (Exception ex) {
            System.out.println("AgentEngine : Config : Error : " + ex.toString());
        }
        return return_param;
    }

    public String getStringParams(String section, String param) {
        String return_param = null;
        try {
            SubnodeConfiguration sObj = iniConfObj.getSection(section);
            return_param = sObj.getString(param);
        } catch (Exception ex) {
            System.out.println("AgentEngine : Config : Error : " + ex.toString());
        }
        return return_param;
    }

    public String getPluginPath() {
        SubnodeConfiguration sObj = iniConfObj.getSection("general");
        String pluginPath = sObj.getString("pluginpath");
        if (!pluginPath.endsWith(System.getProperty("file.separator"))) {
            pluginPath = pluginPath + System.getProperty("file.separator");
        }
        return pluginPath;
    }

    public String getLogPath() {
        SubnodeConfiguration sObj = iniConfObj.getSection("general");
        String logPath = sObj.getString("logpath");
        if (logPath == null)
            logPath = "./logs";
        if (logPath.endsWith("/") || logPath.endsWith("\\\\"))
            logPath = logPath.substring(0, logPath.length() - 1);
        return new File(logPath).getAbsolutePath();
    }

    public String getPluginConfigFile() {
        SubnodeConfiguration sObj = iniConfObj.getSection("general");
        return sObj.getString("plugin_config_file");
    }



}