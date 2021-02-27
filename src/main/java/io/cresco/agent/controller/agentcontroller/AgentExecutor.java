package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.core.CoreState;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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

            case "pluginadd":
                return pluginAdd(incoming);

            case "pluginremove":
                return pluginRemove(incoming);

            case "pluginupload":
                return pluginUpdate(incoming);

            case "setloglevel":
                return setLogLevel(incoming);

            case "getislogdp":
                return getDPLogIsEnabled(incoming);

            case "setlogdp":
                return  setDPLogIsEnabled(incoming);

            case "controllerupdate":
                updateController(incoming);
                break;

            case "restartcontroller":
                restartController();
                break;

            case "restartframework":
                restartFramework();
                break;

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

        if(incoming.getParams().containsKey("print")) {
            logger.error("Plugin: " + incoming.getSrcPlugin() + " out: " + incoming.getParam("print"));
        }

        incoming.setParam("desc","to-plugin-agent-rpc");
        return incoming;
    }

    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

            switch (incoming.getParam("action")) {

                case "getlog":
                    return getLog(incoming);
                case "getfileinfo":
                    return getFileInfo(incoming);
                case "getfiledata":
                    return getFileData(incoming);

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

            Path filePath = Paths.get("cresco-data/cresco-logs/main.log");
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

    private MsgEvent getFileData (MsgEvent ce) {
        try {

            if(ce.paramsContains("filepath") && ce.paramsContains("skiplength") && ce.paramsContains("partsize")) {

                Path filePath = Paths.get(ce.getParam("filepath"));
                if (filePath.toFile().exists()) {
                    if (filePath.toFile().isFile()) {

                        long skipLength = Long.parseLong(ce.getParam("skiplength"));
                        int partsize = Integer.parseInt(ce.getParam("partsize"));

                        try (InputStream inputStream = new FileInputStream(filePath.toFile())) {
                            byte[] databyte = new byte[partsize];
                            long skipSize = inputStream.skip(skipLength);
                            long readSize = inputStream.read(databyte);
                            inputStream.close();
                            ce.setCompressedDataParam("payload",databyte);
                            ce.setParam("status","10");
                            ce.setParam("status_desc","wrote data part");

                        } catch (Exception e) {
                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);
                            String sStackTrace = sw.toString(); // stack trace as a string
                            logger.error(sStackTrace);

                            ce.setParam("status","9");
                            ce.setParam("status_desc","inputStream failure");
                        }

                    } else {
                        ce.setParam("status","9");
                        ce.setParam("status_desc","path is not a file");
                    }
                } else {
                    ce.setParam("status","9");
                    ce.setParam("status_desc","file does not exist");
                }
            } else {
                ce.setParam("status","9");
                ce.setParam("status_desc","no filepath | skiplength | partsize given");
            }

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

            ce.setParam("status","9");
            ce.setParam("status_desc","getFileData() failure");
        }
        return ce;
    }

    private MsgEvent getFileInfo (MsgEvent ce) {
        try {

            if(ce.paramsContains("filepath")) {

                Path filePath = Paths.get(ce.getParam("filepath"));
                if (filePath.toFile().exists()) {
                    if (filePath.toFile().isFile()) {
                        ce.setParam("status","10");
                        ce.setParam("status_desc","file found");
                        ce.setParam("md5", plugin.getMD5(filePath.toFile().getAbsolutePath()));
                        ce.setParam("size", String.valueOf(filePath.toFile().length()));
                    } else {
                        ce.setParam("status","9");
                        ce.setParam("status_desc","path is not a file");
                    }
                } else {
                    ce.setParam("status","9");
                    ce.setParam("status_desc","file does not exist");
                }
            } else {
                ce.setParam("status","9");
                ce.setParam("status_desc","no file path given");
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

            ce.setParam("status","9");
            ce.setParam("status_desc","getFileInfo() failure");
        }
        return ce;
    }

    private void restartController() {

        try {

            logger.error("Controller Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.restartController();

        } catch(Exception ex) {

            logger.error("restartController " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

    }

    private void updateController(MsgEvent me) {

        try {

            String jar_file_path = me.getParam("jar_file_path");
            logger.error("Controller Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.updateController(jar_file_path);

        } catch(Exception ex) {

            logger.error("restartController " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

    }

    private void restartFramework() {

        try {

            logger.error("Framework Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.restartFramework();

        } catch(Exception ex) {

            logger.error("restartController " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

    }

    private MsgEvent pluginAdd(MsgEvent ce) {

        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("configparams");
            logger.trace("pluginAdd configParamsJson: " + configParamsJson);
            Map<String, String> hm = gson.fromJson(configParamsJson, type);

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

            ce.setParam("error",sStackTrace);


        }

        return null;
    }

    private MsgEvent pluginRemove(MsgEvent ce) {

        try {
            String pluginId = ce.getParam("pluginid");
            if(pluginId == null) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin NULL");

            } else {
                logger.info("disabling plugin : " + pluginId);
                boolean isDisabled = controllerEngine.getPluginAdmin().stopPlugin(pluginId);

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
            ce.setParam("status_desc", "Plugin Could Not Be Removed Exception [" + ex.getMessage() + "]");
        }
        return ce;
    }

    private MsgEvent pluginUpdate(MsgEvent ce) {

        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("configparams");
            logger.trace("pluginAdd configParamsJson: " + configParamsJson);
            Map<String, String> hm = gson.fromJson(configParamsJson, type);
            byte[] jarData = ce.getDataParam("jardata");

            boolean isUpdated = controllerEngine.getPluginAdmin().pluginUpdate(hm, jarData);

            ce.removeParam("jardata");
            ce.setParam("is_updated", String.valueOf(isUpdated));

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

            ce.setParam("error",sStackTrace);


        }

        return null;
    }


    private MsgEvent getDPLogIsEnabled(MsgEvent ce) {

        try {
            String sessionId = ce.getParam("session_id");
            if(sessionId == null) {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "islogDP session_id NULL");
            } else {
                boolean isSet = controllerEngine.getPluginAdmin().logDPIsEnabled(sessionId);
                ce.setParam("islogdp", String.valueOf(isSet));
                ce.setParam("status_code", "7");
                ce.setParam("status_desc", "islogDP Get");
            }


        } catch(Exception ex) {
            logger.error("getDPLogIsEnabled Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "logDP Could Not Get Exception");
        }
        return ce;
    }

    private MsgEvent setDPLogIsEnabled(MsgEvent ce) {

        try {
            String logDPString = ce.getParam("setlogdp");
            String sessionId = ce.getParam("session_id");

            if((logDPString == null) || (sessionId == null)) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "setlogdp NULL OR session_id NULL");

            } else {

                boolean logDP = Boolean.parseBoolean(logDPString);
                boolean isSet = controllerEngine.getPluginAdmin().logDPSetEnabled(sessionId,logDP);

                if (isSet) {

                    ce.setParam("status_code", "7");
                    ce.setParam("status_desc", "logDP Set");

                } else {
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "logDP Could Not Be Set");
                }
            }

        } catch(Exception ex) {
            logger.error("setDPLogIsEnabled Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "logDP Could Not Be Set Exception");
        }
        return ce;
    }

    private MsgEvent removeLogLevel(MsgEvent ce) {

        try {
            String baseClassName = ce.getParam("baseclassname");
            String sessionId = ce.getParam("session_id");

            if((baseClassName == null) || (sessionId == null)) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "baseClassName NULL OR session_id NULL");

            } else {

                boolean isSet = controllerEngine.getPluginAdmin().removeLogLevel(sessionId, baseClassName);

                if (isSet) {

                    ce.setParam("status_code", "7");
                    ce.setParam("status_desc", "LogLevel Removed");

                } else {
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "LogLevel Could Not Be Removed");
                }
            }

        } catch(Exception ex) {
            logger.error("setLogLevel Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "baseClassName LogLevel Could Not Be Removed Exception");
        }
        return ce;
    }

    private MsgEvent setLogLevel(MsgEvent ce) {

        try {
            String baseClassName = ce.getParam("baseclassname");
            String loglevelString = ce.getParam("loglevel");
            String sessionId = ce.getParam("session_id");

            CLogger.Level loglevel = CLogger.Level.valueOf(loglevelString);
            if((baseClassName == null) || (sessionId == null)) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "baseClassName NULL OR session_id NULL");

            } else {

                boolean isSet = controllerEngine.getPluginAdmin().setDPLogLevel(sessionId, baseClassName,loglevel);

                if (isSet) {

                    ce.setParam("status_code", "7");
                    ce.setParam("status_desc", "LogLevel Set");

                } else {
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "LogLevel Could Not Be Set");
                }
            }

        } catch(Exception ex) {
            logger.error("setLogLevel Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "baseClassName LogLevel Could Not Be Set Exception");
        }
        return ce;
    }


}