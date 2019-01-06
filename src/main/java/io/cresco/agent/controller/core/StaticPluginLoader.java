package io.cresco.agent.controller.core;

import com.google.gson.Gson;
import io.cresco.agent.core.Config;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.Constants;

import java.io.File;
import java.net.URL;
import java.util.*;

public class StaticPluginLoader implements Runnable  {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Config config;
    private boolean internalModInit = false;
    private Gson gson;


    public StaticPluginLoader(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
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

                        if (config != null) {

                            for (String tmpPluginID : config.getPluginList(1)) {

                                try {
                                    Map<String, Object> map = config.getConfigMap(tmpPluginID);

                                    if ((map.containsKey("pluginname") && (map.containsKey("jarfile")))) {
                                        String pluginName = (String) map.get("pluginname");
                                        String baseJarFile = (String) map.get("jarfile");
                                        String jarFile = null;
                                        String pluginPath = plugin.getConfig().getStringParam("pluginpath");

                                        //try and find file in conf
                                        if (pluginPath != null) {
                                            if (pluginPath.endsWith("/")) {
                                                jarFile = pluginPath + baseJarFile;
                                            } else {
                                                jarFile = pluginPath + "/" + baseJarFile;
                                            }
                                        }

                                        //try and find file in local root repo
                                        if (jarFile == null) {
                                            File pluginRepo = new File("repo");
                                            if(pluginRepo.isDirectory()) {
                                                String pluginRepoPath = pluginRepo.getAbsolutePath() + "/" + baseJarFile;
                                                System.out.println("repo path : " + pluginRepoPath);
                                                File jarFileRepo = new File(pluginRepoPath);
                                                if(jarFileRepo.isFile()) {
                                                    jarFile = pluginRepoPath + "/";
                                                }
                                            }
                                        }

                                        //try and find it embedded
                                        if(jarFile == null) {
                                            URL bundleURL = getClass().getClassLoader().getResource(baseJarFile);
                                            if(bundleURL != null) {
                                                jarFile = baseJarFile;
                                            }
                                        }

                                        //logger.error("name: " + pluginName + " jar: " + jarFile + " path:" + pluginPath);

                                        if(jarFile == null) {
                                            logger.error("pluginPath = null");
                                        } else {
                                            String pluginID = controllerEngine.getPluginAdmin().addPlugin(pluginName, jarFile, map);
                                            logger.info("STATIC LOADED : pluginID: " + pluginID + " pluginName: " + pluginName + " jarName: " + jarFile);
                                        }
                                    } else {
                                        logger.error("Bad Jar Path");
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
                                    String pluginID = controllerEngine.getPluginAdmin().addPlugin((String) map.get("pluginname"), (String) map.get("jarfile"), map);
                                }
                                //load global

                                if(controllerEngine.getPluginAdmin().serviceExist("org.osgi.service.http.HttpService")) {

                                    if (plugin.getConfig().getBooleanParam("enable_web", true)) {
                                        Map<String, Object> map = new HashMap<>();
                                        map.put("pluginname", "io.cresco.dashboard");
                                        map.put("jarfile", "dashboard-1.0-SNAPSHOT.jar");
                                        String pluginID = controllerEngine.getPluginAdmin().addPlugin((String) map.get("pluginname"), (String) map.get("jarfile"), map);

                                    }
                                } else {
                                    logger.info("HttpService : Does not exist : Console Disabled.");
                                }

                            }
                            isStaticInit = true;
                        }
                        if(!controllerEngine.getPluginAdmin().pluginTypeActive("io.cresco.sysinfo")) {
                            //load sysinfo
                            if (plugin.getConfig().getBooleanParam("enable_sysinfo", true)) {
                                logger.info("Starting SYSINFO : Status Active: " + controllerEngine.cstate.isActive() + " Status State: " + controllerEngine.cstate.getControllerState());

                                String inodeId = UUID.randomUUID().toString();
                                String resourceId = "sysinfo_resource";

                                Map<String, Object> map = new HashMap<>();
                                map.put("pluginname", "io.cresco.sysinfo");
                                map.put("jarfile", "sysinfo-1.0-SNAPSHOT.jar");
                                //map.put("inode_id", inodeId);
                                //map.put("resource_id","sysinfo_resource");

                                String pluginId = controllerEngine.getPluginAdmin().addPlugin((String) map.get("pluginname"), (String) map.get("jarfile"), map);

                                //Manually create inode configuration allowing KPI storage
                                //String inodeId = UUID.randomUUID().toString();
                               // String inodeId = "sysinfo_inode";
                                //todo fix this when agents get a DB
                                /*
                                if(controllerEngine.cstate.isRegionalController()) {
                                    controllerEngine.getGDB().addINode(resourceId, inodeId, 10, "iNode added by Controller StaticPlugin Loader.", gson.toJson(map));
                                    controllerEngine.getGDB().updateINodeAssignment(inodeId, 10, "iNode Active.", plugin.getRegion(), plugin.getAgent(), pluginId);
                                }
                                */
                            }

                        }

                    }
                    logger.trace("Status : " + controllerEngine.cstate.getControllerState());
                    Thread.sleep(1000);
                }

            } catch(Exception ex) {
                ex.printStackTrace();
            }

    }



}
