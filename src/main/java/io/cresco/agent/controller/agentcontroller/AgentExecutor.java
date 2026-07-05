package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.capability.CapabilityInventory;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.agent.core.Config;
import io.cresco.library.capability.*;
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

@CrescoCapabilities(namespace = "agent", target = "agent", routingParams = {"region", "agent"},
        summary = "Agent controller: manages plugins on a single agent node, controller lifecycle, log/file access, CEP rules, and agent-local queries.")
@CrescoActions({
    // --- CONFIG (agent-local mutations) ---
    @CrescoAction(name = "pluginadd", type = "CONFIG", summary = "Add/start a plugin bundle on this agent.", why = "Deploy a plugin locally.", params = {@CrescoParam(name = "configparams", required = true, compressed = true, type = "object"), @CrescoParam(name = "edges", compressed = true, type = "object")}, returns = {@CrescoReturn(name = "status_code"), @CrescoReturn(name = "pluginid")}),
    @CrescoAction(name = "pluginremove", type = "CONFIG", summary = "Stop/remove a plugin on this agent.", why = "Undeploy a plugin locally.", params = @CrescoParam(name = "pluginid", required = true), returns = @CrescoReturn(name = "status_code")),
    @CrescoAction(name = "pluginlist", type = "CONFIG", summary = "List all plugins on this agent.", why = "Discover locally-deployed plugins.", returns = @CrescoReturn(name = "plugin_list", type = "object", compressed = true)),
    @CrescoAction(name = "pluginstatus", type = "CONFIG", summary = "Get the status of one plugin on this agent.", why = "Health-check a local plugin.", params = @CrescoParam(name = "pluginid", required = true), returns = @CrescoReturn(name = "plugin_status", type = "object", compressed = true)),
    @CrescoAction(name = "pluginupload", type = "CONFIG", summary = "Upload a plugin JAR and register it.", why = "Provision a plugin artifact onto this agent.", params = {@CrescoParam(name = "configparams", required = true, compressed = true, type = "object"), @CrescoParam(name = "jardata", type = "binary", required = true)}, returns = @CrescoReturn(name = "is_updated", type = "boolean")),
    @CrescoAction(name = "pluginrepopull", type = "CONFIG", summary = "Validate a set of plugins against the repo.", why = "Ensure required plugin artifacts are available before deploy.", params = @CrescoParam(name = "configparams", required = true, compressed = true, type = "object"), returns = @CrescoReturn(name = "is_updated", type = "boolean")),
    @CrescoAction(name = "setloglevel", type = "CONFIG", summary = "Set the log level for a class/session (Trace/Debug/Info/Warn/Error).", why = "Adjust logging verbosity at runtime.", params = {@CrescoParam(name = "session_id", required = true), @CrescoParam(name = "loglevel", required = true), @CrescoParam(name = "baseclassname")}, returns = @CrescoReturn(name = "status_code")),
    @CrescoAction(name = "getislogdp", type = "CONFIG", summary = "Query whether dataplane log streaming is enabled for a session.", why = "Check live log-stream state.", params = @CrescoParam(name = "session_id", required = true), returns = @CrescoReturn(name = "islogdp", type = "boolean")),
    @CrescoAction(name = "setlogdp", type = "CONFIG", summary = "Enable/disable dataplane log streaming for a session.", why = "Turn live log streaming on/off.", params = {@CrescoParam(name = "session_id", required = true), @CrescoParam(name = "setlogdp", type = "boolean", required = true)}, returns = @CrescoReturn(name = "status_code")),
    @CrescoAction(name = "controllerupdate", type = "CONFIG", summary = "Stage a controller JAR for the next restart.", why = "In-place controller upgrade.", params = @CrescoParam(name = "jar_file_path", required = true)),
    @CrescoAction(name = "stopcontroller", type = "CONFIG", summary = "Stop this agent's controller (async).", why = "Graceful controller shutdown."),
    @CrescoAction(name = "restartcontroller", type = "CONFIG", summary = "Restart this agent's controller (async).", why = "Recover/refresh the controller."),
    @CrescoAction(name = "restartframework", type = "CONFIG", summary = "Restart the OSGi framework (async).", why = "Full agent framework restart."),
    @CrescoAction(name = "killjvm", type = "CONFIG", summary = "Kill the agent JVM (async).", why = "Hard stop of the agent process."),
    @CrescoAction(name = "cepadd", type = "CONFIG", summary = "Add a Complex-Event-Processing rule to the dataplane.", why = "Install a streaming query over dataplane events.", params = @CrescoParam(name = "cepparams", required = true, compressed = true, type = "object", description = "{input_stream,input_stream_desc,output_stream,output_stream_desc,query}"), returns = @CrescoReturn(name = "cepid", description = "id of the created CEP")),
    @CrescoAction(name = "cepremove", type = "CONFIG", summary = "Remove a Complex-Event-Processing rule by id.", why = "Tear down a streaming query and free its resources.", params = @CrescoParam(name = "cepid", required = true, description = "id returned by cepadd"), returns = @CrescoReturn(name = "status_code", description = "10 removed")),
    @CrescoAction(name = "cepinput", type = "CONFIG", summary = "Feed a single JSON event into a CEP input stream.", why = "Inject an event into a streaming query over RPC (dataplane feed alternative).", params = {@CrescoParam(name = "cep_input_stream", required = true, description = "the CEP input stream name"), @CrescoParam(name = "cep_payload", required = true, compressed = true, type = "object", description = "the JSON event payload")}, returns = @CrescoReturn(name = "status_code", description = "10 accepted")),
    @CrescoAction(name = "getagentinfo", type = "CONFIG", summary = "Return the agent's data-directory path.", why = "Locate the agent's on-disk data location.", returns = @CrescoReturn(name = "agent-data", description = "data directory path")),
    // --- EXEC (agent-local reads) ---
    @CrescoAction(name = "getlog", summary = "Fetch this agent's main log (compressed inline or as a file).", why = "Retrieve agent logs for diagnostics.", params = @CrescoParam(name = "action_inmessage", type = "boolean", description = "true=compress into reply; else attach file"), returns = @CrescoReturn(name = "log", type = "binary", compressed = true)),
    @CrescoAction(name = "getfileinfo", summary = "Return metadata (md5, size) for a file on this agent.", why = "Prepare a chunked file transfer / verify a file.", params = @CrescoParam(name = "filepath", required = true), returns = {@CrescoReturn(name = "md5"), @CrescoReturn(name = "size", type = "integer")}),
    @CrescoAction(name = "getfiledata", summary = "Stream a chunk of a file on this agent (seek+read).", why = "Transfer large files in parts.", params = {@CrescoParam(name = "filepath", required = true), @CrescoParam(name = "skiplength", type = "integer", required = true), @CrescoParam(name = "partsize", type = "integer", required = true)}, returns = @CrescoReturn(name = "payload", type = "binary", compressed = true)),
    @CrescoAction(name = "getcontrollerstatus", summary = "Return this agent's controller state code.", why = "Check controller lifecycle state.", returns = @CrescoReturn(name = "controller_status", type = "integer")),
    @CrescoAction(name = "iscontrolleractive", summary = "Return whether this agent's controller is active.", why = "Readiness check.", returns = @CrescoReturn(name = "is_controller_active", type = "boolean")),
    @CrescoAction(name = "getbroadcastdiscovery", summary = "Return this agent's network discovery list.", why = "Inspect discovered neighbors.", returns = @CrescoReturn(name = "broadcast_discovery", type = "object")),
    @CrescoAction(name = "listagents", summary = "List agents in this agent's region.", why = "Local discovery of sibling agents.", params = @CrescoParam(name = "action_region"), returns = @CrescoReturn(name = "agentslist", type = "object", compressed = true)),
    @CrescoAction(name = "getmetricinventory", summary = "Return this node's unified metric inventory (node scope).", why = "Node-local metrics; the controller fan-out calls this on each agent.", params = {@CrescoParam(name = "action_include_plugins", type = "boolean"), @CrescoParam(name = "action_include_resource", type = "boolean")}, returns = @CrescoReturn(name = "metricinventory", type = "object")),
    @CrescoAction(name = "gethealthinventory", summary = "Return this node's health inventory (all Felix HealthCheck results, node scope).", why = "Node-local health; the parallel of getmetricinventory for the central health system.", returns = @CrescoReturn(name = "healthinventory", type = "object", description = "{node, aggregate, checks:[{name,status,rawStatus,message,tags,lastRunTs}]}")),
    @CrescoAction(name = "getcapabilities", summary = "Return the agent controller's self-describing capability document.", why = "Discovery of the agent-local API.", returns = @CrescoReturn(name = "capabilities", type = "object")),
    @CrescoAction(name = "getcapabilityinventory", summary = "Return this node's capability inventory (node scope): controller tiers + local plugins + OSGi surface.", why = "Node-local capability catalog; the controller fan-out calls this on each agent.", params = {@CrescoParam(name = "action_include_plugins", type = "boolean"), @CrescoParam(name = "action_include_osgi", type = "boolean")}, returns = @CrescoReturn(name = "capabilityinventory", type = "object"))
})
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

            case "cepremove":
                return cepRemove(incoming);

            case "cepinput":
                return cepInput(incoming);

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
                case "listagents":
                    return listAgents(incoming);
                case "getmetricinventory":
                    return getMetricInventory(incoming);
                case "gethealthinventory":
                    return getHealthInventory(incoming);
                case "getcapabilities":
                    return CapabilityResponder.respond(incoming, this);
                case "getcapabilityinventory":
                    return getCapabilityInventory(incoming);

                default:
                    logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                    logger.error(incoming.getParams().toString());
                    break;
            }
            return null;
        }

    // B-2 unified metrics: every node's controller answers getmetricinventory (node scope) so the
    // global/region fan-out in PerfControllerMonitor can aggregate the whole mesh. Mirrors the global
    // handler; scope is forced to node here (a leaf agent does not re-fan-out).
    private MsgEvent getMetricInventory(MsgEvent ce) {
        try {
            if (controllerEngine.getPerfControllerMonitor() != null) {
                boolean incPlugins = !"false".equalsIgnoreCase(ce.getParam("action_include_plugins"));
                boolean incResource = "true".equalsIgnoreCase(ce.getParam("action_include_resource"));
                ce.setParam("metricinventory",
                        controllerEngine.getPerfControllerMonitor().getMetricInventory("node", incPlugins, incResource));
                ce.setParam("status", "10");
            } else {
                ce.setParam("status", "9");
                ce.setParam("status_desc", "measurements disabled (enable_controllermon=false)");
            }
        } catch (Exception ex) {
            ce.setParam("error", ex.getMessage());
            logger.error("getMetricInventory() " + ex.getMessage());
        }
        return ce;
    }

    // Central health: return this node's health inventory (every discovered Felix HealthCheck's
    // snapshot). The queryable parallel of getMetricInventory — metrics AND health are now both
    // readable over MsgEvent, node-scoped here (a leaf agent does not re-fan-out).
    private MsgEvent getHealthInventory(MsgEvent ce) {
        try {
            ce.setParam("healthinventory",
                    io.cresco.agent.controller.health.HealthInventory.node(controllerEngine));
            ce.setParam("status", "10");
        } catch (Exception ex) {
            ce.setParam("error", ex.getMessage());
            logger.error("getHealthInventory() " + ex.getMessage());
        }
        return ce;
    }

    // Node-scoped capability inventory for this agent (controller tiers + local plugins + OSGi surface).
    // The controller's region/global fan-out calls this on each agent.
    private MsgEvent getCapabilityInventory(MsgEvent ce) {
        try {
            boolean incPlugins = !"false".equalsIgnoreCase(ce.getParam("action_include_plugins"));
            boolean incOsgi = "true".equalsIgnoreCase(ce.getParam("action_include_osgi"));
            ce.setParam("capabilityinventory",
                    new CapabilityInventory(controllerEngine).getCapabilityInventory("node", incPlugins, incOsgi));
            ce.setParam("status", "10");
        } catch (Exception ex) {
            ce.setParam("error", ex.getMessage());
            logger.error("getCapabilityInventory() " + ex.getMessage());
        }
        return ce;
    }


    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }

    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }

    /**
     * Query to list all agents (action_region=null) or agents in a specific region (action_region=[region]
     * @param ce MsgEvent.Type.EXEC, action=listagents, action_region=[optional region]
     *           if action_region=null all agents are listed
     * @return creates "agentslist", in compressed json format
     * @see GlobalExecutor#executeEXEC(MsgEvent)
     */
    private MsgEvent listAgents(MsgEvent ce) {

        try {
            String actionRegionAgents = null;

            if(ce.getParam("action_region") != null) {
                actionRegionAgents = ce.getParam("action_region");
            }

            ce.setCompressedParam("agentslist",gson.toJson(controllerEngine.getGDB().getAgentList(actionRegionAgents)));
            logger.trace("list agents return : " + ce.getParams().toString());
        }
        catch(Exception ex) {
            ce.setParam("error", ex.getMessage());
        }

        return ce;
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

            logger.error("getlog Error: " + ex.getMessage(), ex);

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
                            logger.error("getFileData() inputStream ", e);

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

            logger.error("getFileData() ", ex);

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
            logger.error("getBroadcastDiscovery " + ex.getMessage(), ex);

            ce.setParam("error", ex.getMessage());

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
            logger.error("getFileInfo() ", ex);

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

            logger.error("stopController " + ex.getMessage(), ex);

        }

    }

    private void restartController() {

        try {

            logger.info("restartController() Controller Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.restartController();

        } catch(Exception ex) {

            logger.error("restartController " + ex.getMessage(), ex);

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

            logger.error("restartController " + ex.getMessage(), ex);

        }

    }

    private void restartFramework() {

        try {

            logger.error("Framework Restart Started");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.restartFramework();

        } catch(Exception ex) {

            logger.error("restartController " + ex.getMessage(), ex);

        }

    }

    private void killJVM() {

        try {

            logger.error("Killing JVM");
            CoreState coreState = controllerEngine.getPluginAdmin().getCoreState();
            coreState.killJVM();

        } catch(Exception ex) {

            logger.error("killJVM " + ex.getMessage(), ex);

        }

    }


    private MsgEvent getControllerStatus(MsgEvent ce) {

        try {

            ce.setParam("controller_status", String.valueOf(controllerEngine.cstate.getControllerState()));

        } catch(Exception ex) {

            logger.error("getControllerStatus Error: " + ex.getMessage(), ex);

            ce.setParam("error", ex.getMessage());

            ce.setParam("controller_status","unknown");
        }

        return ce;
    }


    private MsgEvent isControllerActive(MsgEvent ce) {

        try {

            ce.setParam("is_controller_active", String.valueOf(controllerEngine.cstate.isActive()));

        } catch(Exception ex) {

            logger.error("isControllerActive: " + ex.getMessage(), ex);

            ce.setParam("error", ex.getMessage());

            ce.setParam("is_controller_active",Boolean.FALSE.toString());
        }

        return ce;
    }


    private MsgEvent cepAdd(MsgEvent ce) {

        try {

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            String configParamsJson = ce.getCompressedParam("cepparams");
            logger.trace("addCEP configParamsJson: " + configParamsJson);
            Map<String, String> params = gson.fromJson(configParamsJson, type);
            logger.debug("addCEP params: " + params);
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

            logger.error("cepadd Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            ce.setParam("error", ex.getMessage());


        }

        return null;
    }

    private MsgEvent cepRemove(MsgEvent ce) {
        try {
            String cepId = ce.getParam("cepid");
            if (cepId == null) {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "cepid NULL");
            } else {
                boolean removed = plugin.getAgentService().getDataPlaneService().removeCEP(cepId);
                ce.setParam("status_code", removed ? "10" : "9");
                ce.setParam("status_desc", removed ? "CEP removed" : "CEP could not be removed");
            }
        } catch (Exception ex) {
            logger.error("cepremove Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "cepremove exception [" + ex.getMessage() + "]");
        }
        return ce;
    }

    private MsgEvent cepInput(MsgEvent ce) {
        try {
            String streamName = ce.getParam("cep_input_stream");
            String payload = ce.getCompressedParam("cep_payload");
            if (streamName == null || payload == null) {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "cep_input_stream or cep_payload NULL");
            } else {
                plugin.getAgentService().getDataPlaneService().inputCEP(streamName, payload);
                ce.setParam("status_code", "10");
                ce.setParam("status_desc", "event accepted");
            }
        } catch (Exception ex) {
            logger.error("cepinput Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "cepinput exception [" + ex.getMessage() + "]");
        }
        return ce;
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

            logger.error("pluginadd Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            ce.setParam("error", ex.getMessage());

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

            logger.error("pluginadd Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            ce.setParam("error", ex.getMessage());


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

            logger.error("pluginadd Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            ce.setParam("error", ex.getMessage());

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

            logger.error("pluginadd Error: " + ex.getMessage(), ex);
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");

            ce.setParam("error", ex.getMessage());


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