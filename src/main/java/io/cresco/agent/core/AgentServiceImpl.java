package io.cresco.agent.core;


import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.*;

import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.File;
import java.util.HashMap;
import java.util.Map;


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
    private CLogger logger;

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
            String env = System.getProperty(param);
            if(env == null) {
                env = System.getenv(ENV_PREFIX + param);
            }
            */

            plugin = new PluginBuilder(this, this.getClass().getName(), context, map);

            this.pluginAdmin = new PluginAdmin(plugin, agentState,context);

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

            controllerEngine = new ControllerEngine(controllerState, plugin, pluginAdmin);


            //logger.info("Controller Init");
            if(controllerEngine.commInit()) {
                logger.info("Controller Completed Init");

                //todo Add dataplan stuff here
                //setDataPlane

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
    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString) {
        return controllerEngine.getDataPlaneService().addMessageListener(topicType,messageListener,selectorString);
    }

    @Override
    public boolean sendMessage(TopicType topicType, TextMessage textMessage) {
        return controllerEngine.getDataPlaneService().sendMessage(topicType,textMessage);
    }


}