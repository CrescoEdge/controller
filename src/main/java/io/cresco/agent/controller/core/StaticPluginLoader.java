package io.cresco.agent.controller.core;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.core.Config;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.*;

public class StaticPluginLoader implements Runnable  {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Config config;
    private Type hashMaptype;
    private Gson gson;

    public StaticPluginLoader(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        hashMaptype = new TypeToken<Map<String, Object>>(){}.getType();
        gson = new Gson();

        try {

            File localPluginFile = new File("plugin.ini");
            if(localPluginFile.isFile()) {
                this.config = new Config(localPluginFile.getAbsolutePath());
            }

            if(this.config == null) {

                String pluginConfigFileName = plugin.getConfig().getStringParam("plugin_config_file","conf/plugins.ini");
                File pluginConfigFile  = new File(pluginConfigFileName);
                if (pluginConfigFile.isFile()) {
                    this.config = new Config(pluginConfigFile.getAbsolutePath());
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }


    }


    public void run() {

        boolean isStaticInit = false;

            try {

                while(!isStaticInit) {

                    if(controllerEngine.cstate.isActive()) {

                        //pull the list of all plugins
                        List<String> pluginList = controllerEngine.getGDB().getNodeList(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent());
                        List<Map<String,Object>> systemPluginConfigList = new ArrayList<>();

                        for(String pluginId : pluginList) {
                            if (controllerEngine.getGDB().getPNodePersistenceCode(pluginId) == 20) {
                                Map<String,Object> pluginMap = getPluginConfigMap(pluginId);
                                //removing specific version information, in cases where config exist for system plugin, but plugin version has been updated.
                                pluginMap.remove("version");
                                pluginMap.remove("md5");
                                systemPluginConfigList.add(pluginMap);
                            }
                        }



                        if (config != null) {

                            for (String tmpPluginID : config.getPluginList(1)) {

                                try {
                                    Map<String, Object> map = config.getConfigMap(tmpPluginID);

                                    if ((map.containsKey("pluginname"))) {
                                            String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                    } else {
                                        logger.error("Bad Config Found " + map.toString());
                                    }

                                } catch (Exception exe) {
                                    exe.printStackTrace();
                                }
                            }

                            isStaticInit = true;
                        } else {
                            //why not load this sucker here...
                            logger.debug("No plugin config!");

                            if(controllerEngine.cstate.isGlobalController()) {

                                //load repo
                                if(plugin.getConfig().getBooleanParam("enable_repo",true)) {

                                    String pluginName = "io.cresco.repo";

                                    Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                    if(map == null) {


                                        map = getPluginConfigMap(pluginName);

                                        if (map == null) {
                                            map = new HashMap<>();
                                            map.put("pluginname", pluginName);
                                            map.put("jarfile", "repo-1.1-SNAPSHOT.jar");
                                            map.put("persistence_code", "20");
                                            map.put("inode_id", generatePluginId(pluginName));
                                        }
                                    }
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                                //load wsapi
                                if (plugin.getConfig().getBooleanParam("enable_wsapi", true)) {

                                    String pluginName = "io.cresco.wsapi";

                                    Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                    if(map == null) {

                                        map = getPluginConfigMap(pluginName);

                                        if (map == null) {
                                            map = new HashMap<>();
                                            map.put("pluginname", pluginName);
                                            map.put("jarfile", "wsapi-1.1-SNAPSHOT.jar");
                                            map.put("persistence_code", "20");
                                            map.put("inode_id", generatePluginId(pluginName));
                                        }
                                    }
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                                //load dashboard
                                //dashboard is deprecated v1.1
                                /*
                                if (plugin.getConfig().getBooleanParam("enable_dashboard", true)) {

                                    String pluginName = "io.cresco.dashboard";

                                    Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                    if(map == null) {

                                        map = getPluginConfigMap(pluginName);

                                        if (map == null) {
                                            map = new HashMap<>();
                                            map.put("pluginname", pluginName);
                                            map.put("jarfile", "dashboard-1.1-SNAPSHOT.jar");
                                            map.put("persistence_code", "20");
                                            map.put("inode_id", generatePluginId(pluginName));
                                        }
                                    }
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                                 */


                            } else {

                                //load repo if requested
                                if(plugin.getConfig().getBooleanParam("enable_repo",false)) {

                                    String pluginName = "io.cresco.repo";

                                    Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                    if(map == null) {

                                        map = getPluginConfigMap(pluginName);

                                        if (map == null) {
                                            map = new HashMap<>();
                                            map.put("pluginname", pluginName);
                                            map.put("jarfile", "repo-1.1-SNAPSHOT.jar");
                                            map.put("persistence_code", "20");
                                            map.put("inode_id", generatePluginId(pluginName));
                                        }
                                    }

                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                                //load wsapi
                                if (plugin.getConfig().getBooleanParam("enable_wsapi", false)) {

                                    String pluginName = "io.cresco.wsapi";

                                    Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                    if(map == null) {

                                        map = getPluginConfigMap(pluginName);

                                        if (map == null) {
                                            map = new HashMap<>();
                                            map.put("pluginname", pluginName);
                                            map.put("jarfile", "wsapi-1.1-SNAPSHOT.jar");
                                            map.put("persistence_code", "20");
                                            map.put("inode_id", generatePluginId(pluginName));
                                        }
                                    }
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                            }
                            isStaticInit = true;
                        }

                        /*
                        if(!controllerEngine.getPluginAdmin().pluginTypeActive("io.cresco.cdp")) {
                            //load cep plugin
                            if (plugin.getConfig().getBooleanParam("enable_cep", true)) {
                                //logger.info("Starting CDP : Status Active: " + controllerEngine.cstate.isActive() + " Status State: " + controllerEngine.cstate.getControllerState());
                                String pluginName = "io.cresco.cep";

                                Map<String, Object> map = null;
                                map = getPluginConfigMap(pluginName);

                                if(map == null) {
                                    map = new HashMap<>();
                                    map.put("pluginname", pluginName);
                                    map.put("jarfile", "cep-1.0-SNAPSHOT.jar");
                                    map.put("persistence_code", "20");
                                    map.put("inode_id", pluginName);
                                }
                                String pluginId = controllerEngine.getPluginAdmin().addPlugin(map);


                            }
                        }
                         */

                        if(!controllerEngine.getPluginAdmin().pluginTypeActive("io.cresco.sysinfo")) {
                            //load sysinfo
                            if (plugin.getConfig().getBooleanParam("enable_sysinfo", true)) {

                                //logger.info("Starting SYSINFO : Status Active: " + controllerEngine.cstate.isActive() + " Status State: " + controllerEngine.cstate.getControllerState());

                                String pluginName = "io.cresco.sysinfo";

                                Map<String, Object> map = getPluginConfigMapbyName(systemPluginConfigList,pluginName);

                                if(map == null) {

                                    map = getPluginConfigMap(pluginName);

                                    if (map == null) {

                                        map = new HashMap<>();
                                        map.put("pluginname", pluginName);
                                        map.put("jarfile", "sysinfo-1.1-SNAPSHOT.jar");
                                        map.put("persistence_code", "20");
                                        map.put("inode_id", generatePluginId(pluginName));
                                    }
                                }
                                String pluginId = controllerEngine.getPluginAdmin().addPlugin(map);

                            }

                        }

                            for (String pluginId : pluginList) {

                                try {
                                    //check if plugin is already running already
                                    if (!controllerEngine.getPluginAdmin().pluginExist(pluginId)) {
                                        //check if plugin should be running
                                        if (controllerEngine.getGDB().getPNodePersistenceCode(pluginId) == 10) {

                                            //start new plugin
                                            Map<String, Object> configMap = getPluginConfigMap(pluginId);
                                            if (configMap != null) {
                                                String pluginID = controllerEngine.getPluginAdmin().addPlugin(configMap);
                                            }
                                        }
                                    }

                                } catch (Exception ex) {
                                    logger.error("Failed to restart plugin: " + pluginId);
                                    ex.printStackTrace();
                                }

                            }
                        //list might be large clear them
                        pluginList.clear();
                        systemPluginConfigList.clear();


                    } else {
                        if(controllerEngine.cstate.isFailed()) {
                            logger.error("Failed shutting down");
                            isStaticInit = true;
                        } else {
                            Thread.sleep(100);
                            logger.trace("Status : " + controllerEngine.cstate.getControllerState());
                        }
                    }
                }

            } catch(Exception ex) {

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                //String sStackTrace = sw.toString(); // stack trace as a string
                //System.out.println(sStackTrace);

                //ex.printStackTrace();
                logger.error(sw.toString());
            }

    }

    private Map<String, Object> getPluginConfigMapbyName(List<Map<String,Object>> systemPluginConfigList, String pluginName) {
        Map<String, Object> configMap = null;
        try {
            for(Map<String,Object> tmpConfigMap: systemPluginConfigList) {
                String tmpPluginName = (String)tmpConfigMap.get("pluginname");

                if(tmpPluginName.equals(pluginName)) {
                    configMap = tmpConfigMap;
                    break;
                }
            }


        } catch (Exception ex) {
            logger.error("getPluginConfigMapbyName() " + ex.getMessage());
        }

        return configMap;
    }

    private String generatePluginId(String pluginName) {
        return "system-" + UUID.randomUUID().toString();
    }

    private Map<String, Object> getPluginConfigMap(String pluginId) {
        Map<String, Object> configMap = null;
        try {
            if(controllerEngine.getGDB().nodeExist(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent(), pluginId)) {
                String configParams = controllerEngine.getGDB().getPluginInfo(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent(), pluginId);
                if (configParams != null) {
                    configMap = gson.fromJson(configParams, hashMaptype);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return configMap;
    }



}
