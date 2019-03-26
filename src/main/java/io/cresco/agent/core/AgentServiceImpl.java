package io.cresco.agent.core;


import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


@Component(
        service = { AgentService.class} ,
        immediate = true,
        reference=@Reference(name="ConfigurationAdmin", service=ConfigurationAdmin.class)
)

public class AgentServiceImpl implements AgentService {

    private ControllerEngine controllerEngine;
    private ControllerState controllerState;
    private AgentState agentState;
    private PluginBuilder plugin;
    private PluginAdmin pluginAdmin;
    private DBInterfaceImpl gdb;
    private CLogger logger;

    //this needs to be pulled from Config
    private String ENV_PREFIX = "CRESCO_";


    public AgentServiceImpl() {


    }

    @Activate
    void activate(BundleContext context) {


            this.controllerState = new ControllerState();
            this.agentState = new AgentState(controllerState);


        try {

            String agentConfig = System.getProperty("agentConfig");

            if(agentConfig == null) {
                agentConfig = "conf/agent.ini";
            }

            Map<String,Object> map = null;

            File configFile  = new File(agentConfig);
            Config config = null;
            if(configFile.isFile()) {

                //Agent Config
                config = new Config(configFile.getAbsolutePath());
                map = config.getConfigMap();

            }

            String configMsg = "Property > Env";

            if(config == null) {
                map = new HashMap<>();
            } else {
                configMsg = "Property > Env > " + configFile;
            }

            /*
            //take all the system env varables with CRESCO and put them into the config
            Map<String,String> envMap = System.getenv();
            for (Map.Entry<String, String> entry : envMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if(key.contains(ENV_PREFIX)) {
                    key = key.replace(ENV_PREFIX, "").toLowerCase().trim();
                    map.put(key,value);
                }
            }

            //take all input property names and add to the config
            Properties properties = System.getProperties();
            for(String propertyNames : properties.stringPropertyNames()) {
                map.put(propertyNames,properties.getProperty(propertyNames));
            }

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key + ":" + value);
            }
            */

            //create plugin
            plugin = new PluginBuilder(this, this.getClass().getName(), context, map);

            //create database
            gdb = new DBInterfaceImpl(plugin);

            //create admin
            pluginAdmin = new PluginAdmin(plugin, agentState, gdb, context);

            logger = plugin.getLogger("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);
            pluginAdmin.setLogLevel("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);

            logger.info("");
            logger.info("       ________   _______      ________   ________   ________   ________");
            logger.info("      /  _____/  /  ___  |    /  _____/  /  _____/  /  _____/  /  ___   /");
            logger.info("     /  /       /  /__/  /   /  /__     /  /___    /  /       /  /  /  /");
            logger.info("    /  /       /  __   /    /  ___/    /____   /  /  /       /  /  /  /");
            logger.info("   /  /____   /  /  |  |   /  /____   _____/  /  /  /____   /  /__/  /");
            logger.info("  /_______/  /__/   |__|  /_______/  /_______/  /_______/  /________/");
            logger.info("");
            logger.info("      Configuration Source : {}", configMsg);
            //logger.info("      Plugin Configuration File: {}", config.getPluginConfigFile());
            logger.info("");

            logger.info("Controller Starting Init");

            controllerEngine = new ControllerEngine(controllerState, plugin, pluginAdmin, gdb);

            //core init needs to go here
            if(controllerEngine.coreInit()) {
                logger.info("Controller Completed Core-Init");
            } else {
                logger.error("Controlled Failed Core-Init : Exiting");
            }

            //setup role init
            if(controllerEngine.commInit()) {
                logger.info("Controller Completed Init");

            } else {
                logger.error("Controlled Failed Init");
            }

            while(!controllerEngine.cstate.isActive()) {
                logger.info("Waiting for controller to become active...");
                Thread.sleep(1000);
            }

            plugin.setIsActive(true);

        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    @Deactivate
    void deactivate(BundleContext context) {
        if(logger != null) {
            logger.info("Deactivate Controller");
        }

        if(plugin != null) {
            plugin.setIsActive(false);
        }

        if(controllerEngine != null) {
            controllerEngine.closeCommunications();
        }

        if(gdb != null) {
            gdb.shutdown();
        }

    }

    @Modified
    void modified(BundleContext context, Map<String,Object> map) {
        logger.info("Modified Config Map PluginID:" + map.get("pluginID"));
    }

    @Override
    public void setLogLevel(String logId, CLogger.Level level) {

        if(pluginAdmin != null) {
            pluginAdmin.setLogLevel(logId, level);
        }

    }

    @Override
    public AgentState getAgentState() {
        return agentState;
    }


    @Override
    public void msgOut(String id, MsgEvent msg) {
        try {
            controllerEngine.msgIn(msg);
        } catch(Exception ex) {
            logger.error(msg.printHeader());
            logger.error(msg.getParams().toString());

            ex.printStackTrace();
        }
    }

    @Override
    public DataPlaneService getDataPlaneService() {
        return controllerEngine.getDataPlaneService();
    }



}