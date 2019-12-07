package io.cresco.agent.core;


import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.ControllerStatePersistanceImp;
import io.cresco.agent.db.DBEngine;
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
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;


@Component(
        service = {AgentService.class} ,
        immediate = true,
        reference=@Reference(name="ConfigurationAdmin", service=ConfigurationAdmin.class)
)

public class AgentServiceImpl implements AgentService {

    private ControllerEngine controllerEngine;
    private ControllerState controllerState;
    private AgentState agentState;
    private PluginBuilder plugin;
    private PluginAdmin pluginAdmin;
    private DBEngine dbe;
    private DBInterfaceImpl gdb;
    private CLogger logger;

    //this needs to be pulled from Config
    private String ENV_PREFIX = "CRESCO_";


    public AgentServiceImpl() {


    }


    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, CLogger.Level level) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName);


    }

    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName);

    }

    private Map<String,Object> initAgentConfigMap() {
        Map<String, Object> configParams = null;
        try {

            configParams = new HashMap<>();

            String agentConfig = System.getProperty("agentConfig");

            if (agentConfig == null) {
                agentConfig = "conf/agent.ini";
            }

            File configFile = new File(agentConfig);
            Config config = null;
            if (configFile.isFile()) {

                //Agent Config
                config = new Config(configFile.getAbsolutePath());
                configParams = config.getConfigMap();

            }

            /*
            String configMsg = "Property > Env";

            if (config == null) {
                configParams = new HashMap<>();
            } else {
                configMsg = "Property > Env > " + configFile;
            }
            */


            String platform = System.getenv("CRESCO_PLATFORM");
            if (platform == null) {

                if(config != null) {
                    platform = config.getStringParams("general", "platform");
                }

                if (platform == null) {
                    platform = "unknown";
                }
            }

            configParams.put("platform", platform);
            //enableMsg.setParam("platform", platform);

            String environment = System.getenv("CRESCO_ENVIRONMENT");
            if (environment == null) {

                if(config != null) {
                    environment = config.getStringParams("general", "environment");
                }

                if (environment == null) {
                    try {
                        environment = System.getProperty("os.name");
                    } catch (Exception ex) {
                        environment = "unknown";
                    }
                }
            }
            //enableMsg.setParam("environment", environment);
            configParams.put("environment", environment);

            String location = System.getenv("CRESCO_LOCATION");
            if(location == null) {

                if(config != null) {
                    location = config.getStringParams("general", "location");
                }
            }
            if (location == null) {

                try {
                    location = InetAddress.getLocalHost().getHostName();
                    if (location != null) {
                        //logger.info("Location set: " + location);
                    }
                } catch (Exception ex) {
                    //logger.error("getLocalHost() Failed : " + ex.getMessage());
                }

                if (location == null) {
                    try {

                        String osType = System.getProperty("os.name").toLowerCase();
                        if (osType.equals("windows")) {
                            location = System.getenv("COMPUTERNAME");
                        } else if (osType.equals("linux")) {
                            location = System.getenv("HOSTNAME");
                        }

                        if (location != null) {
                            //logger.info("Location set env: " + location);
                        }

                    } catch (Exception exx) {
                        //do nothing
                        //logger.error("Get System Env Failed : " + exx.getMessage());
                    }
                }
            }
            if (location == null) {
                location = "unknown";
            }
            //enableMsg.setParam("location", location);
            configParams.put("location", location);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
        return configParams;
    }

    @Override
    public String getAgentDataDirectory() {
        String agentDataDirectory = null;
        try {
            if(plugin != null) {
                agentDataDirectory = plugin.getConfig().getStringParam("agent_data_directory", FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString());
            } else {
                agentDataDirectory = FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            if(logger != null) {
                logger.error(ex.getMessage());
            }
        }
        return agentDataDirectory;
    }

    @Activate
    void activate(BundleContext context) {

    //AgentServiceImpl agentService = this;

        Map<String,Object> configParams = initAgentConfigMap();

        //create plugin
        plugin = new PluginBuilder(this, this.getClass().getName(), context, configParams);

        //starting database
        dbe = new DBEngine(plugin);

        //create controller database implementation
        gdb = new DBInterfaceImpl(plugin, dbe);

        //create controller state persistance
        ControllerStatePersistanceImp controllerStatePersistanceImp = new ControllerStatePersistanceImp(plugin,dbe);

        //control state
        controllerState = new ControllerState(controllerStatePersistanceImp);

        //agent state
        agentState = new AgentState(controllerState);

        //create admin
        pluginAdmin = new PluginAdmin(this, plugin, agentState, gdb, context);

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
        //logger.info("      Configuration Source : {}", configMsg);
        //logger.info("      Plugin Configuration File: {}", config.getPluginConfigFile());
        logger.info("");

        logger.info("Controller Starting Init");

        controllerEngine = new ControllerEngine(controllerState, plugin, pluginAdmin, gdb);

        (new Thread() {
        public void run() {
            try {

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
                    logger.info("Controller State = " + controllerEngine.cstate.getControllerState().toString());

                    Thread.sleep(1000);
                }

                plugin.setIsActive(true);


            } catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }).start();

    }

    @Deactivate
    void deactivate(BundleContext context) {

        if(logger != null) {
            logger.info("Starting Controller Shutdown");
        }

        if(controllerEngine != null) {
            //controllerEngine.closeCommunications();

            switch (controllerEngine.cstate.getControllerState()) {

                case STANDALONE_INIT:
                    controllerEngine.cstate.setStandaloneShutdown("Shutdown Called");
                    break;
                case STANDALONE:
                    controllerEngine.cstate.setStandaloneShutdown("Shutdown Called");
                    break;
                case AGENT:
                    controllerEngine.cstate.setAgentShutdown("Shutdown Called");
                    break;
                case REGION_GLOBAL:
                    controllerEngine.cstate.setRegionShutdown("Shutdown Called");
                    break;
                case GLOBAL:
                    controllerEngine.cstate.setGlobalShutdown("Shutdown Called");
                    break;

                default:
                    if(logger != null) {
                        logger.error("INVALID MODE : " + controllerEngine.cstate.getControllerState());
                    }
                    break;
            }

        }

        if(plugin != null) {
            plugin.setIsActive(false);
        }

        if(gdb != null) {
            gdb.shutdown();
        }

        if(logger != null) {
            logger.info("Controller Shutdown Completed");
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