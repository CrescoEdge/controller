package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.core.Config;
import io.cresco.library.core.CoreState;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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

            case "pluginadd":
                return pluginAdd(incoming);

            case "pluginremove":
                return pluginRemove(incoming);

            case "pluginlist":
                return pluginList(incoming);

            case "pluginstatus":
                return pluginStatus(incoming);

            case "pluginupload":
                return pluginUpload(incoming);

            case "pluginrepopull":
                return pluginRepoPull(incoming);

            case "setloglevel":
                return setLogLevel(incoming);

            case "getislogdp":
                return getDPLogIsEnabled(incoming);

            case "setlogdp":
                return  setDPLogIsEnabled(incoming);

            case "controllerupdate":
                updateController(incoming);
                break;

            case "stopcontroller":
                stopController();
                break;

            case "restartcontroller":
                restartController();
                break;

            case "restartframework":
                restartFramework();
                break;

            case "killjvm":
                killJVM();
                break;

            case "cepadd":
                return cepAdd(incoming);

            case "getagentinfo":
                return getAgentInfo(incoming);

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
                case "getcontrollerstatus":
                    return getControllerStatus(incoming);
                case "iscontrolleractive":
                    return isControllerActive(incoming);
                case "getbroadcastdiscovery":
                    return getBroadcastDiscovery(incoming);


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

            Path filePath = null;

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {
                filePath = Paths.get(cresco_data_location, "cresco-logs","main.log");
            } else {
                filePath = Paths.get("cresco-data", "cresco-logs","main.log");
            }

            if(ce.paramsContains("action_inmessage")) {
                ce.setCompressedDataParam("log",java.nio.file.Files.readAllBytes(filePath));
            } else{
                ce.addFile(filePath.toAbsolutePath().toString());
            }
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

    private MsgEvent getBroadcastDiscovery(MsgEvent ce) {

        try {
            logger.error("prediscover");
            List<DiscoveryNode> discovery_list = controllerEngine.getPerfMonitorNet().getNetworkDiscoveryList();

            for(DiscoveryNode dn : discovery_list) {
                logger.error(gson.toJson(dn));
            }
            logger.error("post discover");
            ce.setParam("broadcast_discovery","data");

        } catch (Exception ex) {
            logger.error("getBroadcastDiscovery " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

            ce.setParam("error",sStackTrace);

            ce.setParam("broadcast_discovery","unknown");

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

    private void stopController() {

        try {

            logger.error("Controller Stop Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.stopController();

        } catch(Exception ex) {

            logger.error("stopController " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

    }

    private void restartController() {

        try {

            logger.info("restartController() Controller Restart Started");
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

            //create a version record so that updated controller will be used on restart
            File confDir = new File("conf");
            if(!confDir.exists()) {
                confDir.mkdir();
            }
            String versionConfig = "conf/version.ini";
            File versionFile = new File(versionConfig);
            if(versionFile.exists()) {
                versionFile.delete();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(versionConfig));
            writer.write("[io.cresco.controller]" +  System.lineSeparator());
            writer.write("jarfile=\"" + jar_file_path + "\"" +  System.lineSeparator());
            writer.close();

            logger.info("updateController() Controller Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            boolean isUpdated = coreState.updateController(jar_file_path);
            if(isUpdated) {
                logger.info("updateController() Controller Updated");
            } else {
                logger.error("updateController() Controller Update Failed");
            }

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

    private void killJVM() {

        try {

            logger.error("Killing JVM");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.killJVM();

        } catch(Exception ex) {

            logger.error("killJVM " + ex.getMessage());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

        }

    }


    private MsgEvent getControllerStatus(MsgEvent ce) {

        try {

            ce.setParam("controller_status", String.valueOf(controllerEngine.cstate.getControllerState()));

        } catch(Exception ex) {

            logger.error("getControllerStatus Error: " + ex.getMessage());


            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

            ce.setParam("error",sStackTrace);

            ce.setParam("controller_status","unknown");
        }

        return ce;
    }


    private MsgEvent isControllerActive(MsgEvent ce) {

        try {

            ce.setParam("is_controller_active", String.valueOf(controllerEngine.cstate.isActive()));

        } catch(Exception ex) {

            logger.error("isControllerActive: " + ex.getMessage());


            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);

            ce.setParam("error",sStackTrace);

            ce.setParam("is_controller_active",Boolean.FALSE.toString());
        }

        return ce;
    }


    private MsgEvent cepAdd(MsgEvent ce) {

        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("cepparams");
            logger.error(configParamsJson);
            logger.trace("addCEP configParamsJson: " + configParamsJson);
            Map<String, String> params = gson.fromJson(configParamsJson, type);
            logger.error(params.toString());
            String input_stream = params.get("input_stream");
            String input_stream_desc = params.get("input_stream_desc");
            String output_stream = params.get("output_stream");
            String output_stream_desc = params.get("output_stream_desc");
            String query = params.get("query");

            String cepid = plugin.getAgentService().getDataPlaneService().createCEP(input_stream, input_stream_desc, output_stream,output_stream_desc, query);
            if(cepid != null) {

                ce.setParam("status_code", "10");
                ce.setParam("status_desc", "CEP Active");
                ce.setParam("cepid", cepid);

            } else {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "CEP could not be started!");
            }

            return ce;


        } catch(Exception ex) {

            logger.error("cepadd Error: " + ex.getMessage());
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

            //remove jar data on responce
            if(ce.paramsContains("jardata")) {
                ce.removeParam("jardata");
            }

            //return ce;


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

        return ce;
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

    private MsgEvent pluginList(MsgEvent ce) {

        try {
            String pluginList = controllerEngine.getPluginAdmin().getPluginList();

            if (pluginList != null) {
                ce.setCompressedParam("plugin_list",pluginList);
                ce.setParam("status_code", "10");
                ce.setParam("status_desc", "Plugins Listed");

            } else {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugins Could Not Be Listed");
            }

        } catch(Exception ex) {
            logger.error("pluginlist Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugins Could Not Be Listed Exception [" + ex.getMessage() + "]");
        }
        return ce;
    }

    private MsgEvent pluginStatus(MsgEvent ce) {

        try {

            String pluginId = ce.getParam("pluginid");
            Map<String, String> statusMap = controllerEngine.getPluginAdmin().getPluginStatus(pluginId);

            if(statusMap != null) {

                ce.setParam("status_code", statusMap.get("status_code"));
                ce.setParam("status_desc", statusMap.get("status_desc"));
                ce.setParam("pluginid", pluginId);
                ce.setCompressedParam("plugin_status", gson.toJson(statusMap));

            } else {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin Status could not be determined!");
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


    private MsgEvent pluginUpload(MsgEvent ce) {

        boolean isUpdated = false;
        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("configparams");
            logger.trace("pluginAdd configParamsJson: " + configParamsJson);
            Map<String, String> hm = gson.fromJson(configParamsJson, type);
            byte[] jarData = ce.getDataParam("jardata");

            if(ce.paramsContains("jardata")) {
                logger.error("JAR FOUND");
            }

            String jarPath = controllerEngine.getPluginAdmin().pluginUpdate(hm, jarData);
            if(jarPath != null) {
                isUpdated = true;
                ce.setParam("jar_file_path",jarPath);
            }

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

        ce.removeParam("configparams");
        ce.removeParam("jardata");
        ce.setParam("is_updated", String.valueOf(isUpdated));
        return ce;
    }

    private MsgEvent pluginRepoPull(MsgEvent ce) {

        boolean isUpdated = false;
        try {

            Type type = new TypeToken<Map<String, Object>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("configparams");
            logger.trace("pluginAdd configParamsJson: " + configParamsJson);
            Map<String, Object> hm = gson.fromJson(configParamsJson, type);


            Map<String,Object> validated_list = controllerEngine.getPluginAdmin().remotePluginMap(hm);
            ce.setCompressedParam("configparams",gson.toJson(validated_list));
            isUpdated = true;

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

        ce.setParam("is_updated", String.valueOf(isUpdated));
        return ce;
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

    private MsgEvent getAgentInfo(MsgEvent ce) {

        try {

            //ce.setCompressedParam("agent-config",plugin.getConfig().getConfigAsJSON());
            ce.setParam("agent-data", plugin.getConfig().getStringParam("cresco_data_location","cresco-data"));

        } catch(Exception ex) {
            logger.error("getAgentInfo Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "getAgentInfo Could Not Get Exception");
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
            String sessionId = ce.getParam("session_id");
            String baseClassName = null;
            if (ce.paramsContains("baseclassname")) {
                baseClassName = ce.getParam("baseclassname");
            }

            String loglevelString = ce.getParam("loglevel");

            CLogger.Level loglevel = CLogger.Level.valueOf(loglevelString);
            if((baseClassName == null) || (sessionId == null)) {

                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "one or more NULL baseClassName: "+ baseClassName + " OR session_id: " + sessionId);

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