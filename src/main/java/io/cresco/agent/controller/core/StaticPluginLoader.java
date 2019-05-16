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
import java.net.URL;
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

                        /*
                        logger.error("WAITING 30 SEC to LOAD STATICS");
                        Thread.sleep(30000);
                        logger.error("STARTING to LOAD STATICS");
                        */

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
                                    Map<String, Object> map = new HashMap<>();
                                    map.put("pluginname", "io.cresco.repo");
                                    map.put("jarfile", "repo-1.0-SNAPSHOT.jar");
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                                //load dashboard

                                //if(controllerEngine.getPluginAdmin().serviceExist("org.osgi.service.http.HttpService")) {

                                if (plugin.getConfig().getBooleanParam("enable_dashboard", true)) {
                                    Map<String, Object> map = new HashMap<>();
                                    map.put("pluginname", "io.cresco.dashboard");
                                    map.put("jarfile", "dashboard-1.0-SNAPSHOT.jar");
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }
                                //} else {
                                //    logger.info("HttpService : Does not exist : Console Disabled.");
                                //}

                            } else {

                                //load repo if requested
                                if(plugin.getConfig().getBooleanParam("enable_repo",false)) {
                                    Map<String, Object> map = new HashMap<>();
                                    map.put("pluginname", "io.cresco.repo");
                                    map.put("jarfile", "repo-1.0-SNAPSHOT.jar");
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                }

                            }
                            isStaticInit = true;
                        }

                        if(!controllerEngine.getPluginAdmin().pluginTypeActive("io.cresco.cdp")) {
                            //load cep plugin
                            if (plugin.getConfig().getBooleanParam("enable_cep", true)) {
                                logger.info("Starting CDP : Status Active: " + controllerEngine.cstate.isActive() + " Status State: " + controllerEngine.cstate.getControllerState());

                                Map<String, Object> map = new HashMap<>();
                                map.put("pluginname", "io.cresco.cep");
                                map.put("jarfile", "cep-1.0-SNAPSHOT.jar");

                                String pluginId = controllerEngine.getPluginAdmin().addPlugin(map);


                            }
                        }

                        if(!controllerEngine.getPluginAdmin().pluginTypeActive("io.cresco.sysinfo")) {
                            //load sysinfo
                            if (plugin.getConfig().getBooleanParam("enable_sysinfo", true)) {
                                logger.info("Starting SYSINFO : Status Active: " + controllerEngine.cstate.isActive() + " Status State: " + controllerEngine.cstate.getControllerState());

                                Map<String, Object> map = new HashMap<>();
                                map.put("pluginname", "io.cresco.sysinfo");
                                map.put("jarfile", "sysinfo-1.0-SNAPSHOT.jar");

                                String pluginId = controllerEngine.getPluginAdmin().addPlugin(map);

                            }

                        }

                            //pull the list of all plugins
                            List<String> pluginList = controllerEngine.getGDB().getNodeList(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent());
                            for (String pluginId : pluginList) {

                                try {

                                    //check if plugin is already running already
                                    if (!controllerEngine.getPluginAdmin().pluginExist(pluginId)) {
                                        //check if plugin should be running
                                        if (controllerEngine.getGDB().getPNodePersistenceCode(pluginId) > 9) {
                                            //start new plugin
                                            String configParams = controllerEngine.getGDB().getPluginInfo(controllerEngine.cstate.getRegion(), controllerEngine.cstate.getAgent(), pluginId);
                                            if (configParams != null) {
                                                Map<String, Object> map = gson.fromJson(configParams, hashMaptype);
                                                String pluginID = controllerEngine.getPluginAdmin().addPlugin(map);
                                            }
                                        }
                                    }

                                } catch (Exception ex) {
                                    logger.error("Failed to restart plugin: " + pluginId);
                                    ex.printStackTrace();
                                }

                            }



                    }
                    logger.trace("Status : " + controllerEngine.cstate.getControllerState());
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



}
