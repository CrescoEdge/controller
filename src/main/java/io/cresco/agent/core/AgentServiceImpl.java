package io.cresco.agent.core;

import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.data.DataPlaneLogger;
import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.*;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;


@Component(
        service = {AgentService.class} ,
        immediate = true,
        reference=@Reference(name="ConfigurationAdmin", service=ConfigurationAdmin.class)
)

public class AgentServiceImpl implements AgentService {

    private ControllerEngine controllerEngine;
    private ControllerStateImp cstate;
    private AgentState agentState;
    private PluginBuilder plugin;
    private PluginAdmin pluginAdmin;
    private DBEngine dbe;
    private DBInterfaceImpl gdb;
    private CLogger logger;
    private DataPlaneLogger dataPlaneLogger;

    //this needs to be pulled from Config
    private String ENV_PREFIX = "CRESCO_";


    public AgentServiceImpl() {


    }


    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, CLogger.Level level) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName, dataPlaneLogger);


    }

    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName, dataPlaneLogger);

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

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {

                if(plugin != null) {
                    agentDataDirectory = plugin.getConfig().getStringParam("agent_data_directory", cresco_data_location);
                } else {
                    agentDataDirectory = cresco_data_location;
                }

            } else {

                if(plugin != null) {
                    agentDataDirectory = plugin.getConfig().getStringParam("agent_data_directory", FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString());
                } else {
                    agentDataDirectory = FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString();
                }

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

        //create dataplane logger
        dataPlaneLogger = new DataPlaneLogger(plugin);

        //starting database
        dbe = new DBEngine(plugin);

        //create controller database implementation
        gdb = new DBInterfaceImpl(plugin, dbe);

        //control state
        cstate = new ControllerStateImp(plugin, dbe);

        //agent state
        agentState = new AgentState(cstate);

        //create admin
        pluginAdmin = new PluginAdmin(this, plugin, agentState, gdb, context, dataPlaneLogger);

        logger = plugin.getLogger("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);
        //setLogLevel("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);
        //pluginAdmin.setLogLevel("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);

        logger.info("");
        logger.info("       ________   _______      ________   ________   ________   ________");
        logger.info("      /  _____/  /  ___  |    /  _____/  /  _____/  /  _____/  /  ___   /");
        logger.info("     /  /       /  /__/  /   /  /__     /  /___    /  /       /  /  /  /");
        logger.info("    /  /       /  __   /    /  ___/    /____   /  /  /       /  /  /  /");
        logger.info("   /  /____   /  /  |  |   /  /____   _____/  /  /  /____   /  /__/  /");
        logger.info("  /_______/  /__/   |__|  /_______/  /_______/  /_______/  /________/");
        logger.info("");
        logger.info("");

        logger.info("Controller Version: " + getControllerVersion());

        controllerEngine = new ControllerEngine(cstate, plugin, pluginAdmin, gdb);

        (new Thread() {
        public void run() {
            try {

                //core init needs to go here
                if(controllerEngine.start()) {
                    logger.info("Controller Completed Startup");
                } else {
                    logger.error("Controlled Failed Startup : Exiting");
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

            if(!controllerEngine.stop()) {
                if(logger != null) {
                    logger.error("deactivate() ControllerEngine stop() was dirty!");
                }
            }
            if(dbe != null) {
                dbe.shutdown();

            }

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

        //set log level for files
        if(pluginAdmin != null) {
            pluginAdmin.setLogLevel(logId, level);
        }

    }

    public String getControllerVersion() {
        String version = null;
        try{

            String jarURLString = new File(AgentServiceImpl.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            if(jarURLString.contains("!/")) {
                jarURLString = "jar:file://" + jarURLString;
                URL inputURL = new URL(jarURLString);
                JarInputStream jarStream = null;

                try {

                    if (inputURL != null) {
                        JarURLConnection conn = (JarURLConnection) inputURL.openConnection();
                        InputStream in = conn.getInputStream();

                        jarStream = new JarInputStream(in);
                        Manifest mf = jarStream.getManifest();
                        //Manifest mf = conn.getManifest();
                        if (mf != null) {
                            Attributes mainAttribs = mf.getMainAttributes();
                            if (mainAttribs != null) {
                                version = mainAttribs.getValue("Bundle-Version");
                            }
                        }
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                } finally {

                    if(jarStream != null) {
                        jarStream.close();
                    }
                }


            } else {
                version = plugin.getPluginVersion(jarURLString);
            }

        }
        catch(Exception ex)
        {
            ex.printStackTrace();

        }
        return version;
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