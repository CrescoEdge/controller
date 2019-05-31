package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AgentExecutor implements Executor {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;


    public AgentExecutor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        logger = plugin.getLogger(AgentExecutor.class.getName(),CLogger.Level.Info);
        gson = new Gson();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {

        switch (incoming.getParam("action")) {

            case "enable":
                //enablePlugin(incoming);
                break;

            case "disable":
                //disablePlugin(incoming);
                break;
            case "pluginadd":
                return pluginAdd(incoming);

            case "pluginremove":
                return pluginRemove(incoming);

            default:
                logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                logger.error(incoming.getParams().toString());
                break;
        }

        return null;
    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        //System.out.println("INCOMING INFO MESSAGE FOR AGENT FROM " + incoming.getSrcPlugin() + " setting new desc");

        if(incoming.getParams().containsKey("print")) {
            logger.error("Plugin: " + incoming.getSrcPlugin() + " out: " + incoming.getParam("print"));
        }
        /*
            String pluginName = "io.cresco.skeleton";
            String jarFile = "/Users/cody/IdeaProjects/skeleton/target/skeleton-1.0-SNAPSHOT.jar";
            Map<String, Object> map = new HashMap<>();

            map.put("pluginname", pluginName);
            map.put("jarfile", jarFile);

            controllerEngine.getPluginAdmin().addPlugin(pluginName, jarFile, map);
        */
        incoming.setParam("desc","to-plugin-agent-rpc");
        return incoming;
    }

    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

            switch (incoming.getParam("action")) {

                case "getlog":
                    return getLog(incoming);

                default:
                    logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                    logger.error(incoming.getParams().toString());
                    break;
            }
            return null;
        }

    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }

    private MsgEvent getLog(MsgEvent ce) {
        try {

            Path filePath = Paths.get("log/log.out");
            ce.addFile(filePath.toAbsolutePath().toString());

            return ce;


        } catch(Exception ex) {

            logger.error("getlog Error: " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

        return null;
    }

    private MsgEvent pluginAdd(MsgEvent ce) {

        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            Map<String, String> hm = gson.fromJson(ce.getCompressedParam("configparams"), type);

            //todo persistance should be set by the application not here
            hm.put("persistence_code","10");

            Map<String,Object> map = new HashMap<>();

            for (Map.Entry<String, String> entry : hm.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                map.put(key,value);
            }

            String pluginId = null;

            if(ce.getParam("edges") != null) {
                pluginId = controllerEngine.getPluginAdmin().addPlugin(map, ce.getCompressedParam("edges"));
            } else {
                pluginId = controllerEngine.getPluginAdmin().addPlugin(map);
            }

            if(pluginId != null) {

                Map<String, String> statusMap = controllerEngine.getPluginAdmin().getPluginStatus(pluginId);
                ce.setParam("status_code", statusMap.get("status_code"));
                ce.setParam("status_desc", statusMap.get("status_desc"));
                ce.setParam("pluginid", pluginId);

            } else {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin Bundle could not be installed or started!");
            }

            return ce;


        } catch(Exception ex) {

            logger.error("pluginadd Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);


        }

        return null;
    }

    private MsgEvent pluginRemove(MsgEvent ce) {

        try {
            String plugin = ce.getParam("pluginid");
            if(plugin == null) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin NULL");

            } else {
                logger.info("disabling plugin : " + plugin);
                boolean isDisabled = controllerEngine.getPluginAdmin().stopPlugin(plugin);

                if (isDisabled) {

                    ce.setParam("status_code", "7");
                    ce.setParam("status_desc", "Plugin Removed");

                } else {
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "Plugin Could Not Be Removed");
                }
            }

        } catch(Exception ex) {
            logger.error("pluginremove Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Removed Exception");
        }
        return ce;
    }


}