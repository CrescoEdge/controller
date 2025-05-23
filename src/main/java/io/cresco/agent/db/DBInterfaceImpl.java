package io.cresco.agent.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.gNode;
import io.cresco.library.app.gPayload;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DBInterfaceImpl implements DBInterface {

    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine dbe; // The DBEngine instance

    private Gson gson;
    private Type type;
    private Type mapType;

    public BlockingQueue<String> importQueue;

    public DBInterfaceImpl(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(DBInterfaceImpl.class.getName(),CLogger.Level.Info);
        this.dbe = dbe; // Assign the DBEngine instance
        //this.dbe = new DBEngine(plugin);

        this.importQueue = new LinkedBlockingQueue<>();
        this.gson = new Gson();
        this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        mapType = new TypeToken<Map<String, String>>(){}.getType();

    }

    // --- NEW WRAPPER METHOD ---
    /**
     * Updates the watchdog timestamp for the specified node.
     * @param regionId The region ID (null if updating agent or plugin).
     * @param agentId The agent ID (null if updating region or plugin).
     * @param pluginId The plugin ID (null if updating region or agent).
     * @return The number of rows updated (should be 1 if successful, 0 otherwise).
     */
    public int updateWatchDogTS(String regionId, String agentId, String pluginId) {
        try {
            // Call the method in the underlying DBEngine instance
            return dbe.updateWatchDogTS(regionId, agentId, pluginId);
        } catch (Exception ex) {
            logger.error("updateWatchDogTS wrapper error: {}", ex.getMessage(), ex);
            return -1; // Indicate error
        }
    }
    // --- END NEW WRAPPER METHOD ---


    public Map<String,String> getInodeMap(String inodeId) {
        return dbe.getInodeMap(inodeId);
    }

    public int setINodeStatusCode(String inodeId, int status_code, String status_desc) {
        return dbe.setINodeStatusCode(inodeId,status_code,status_desc);
    }

    public gPayload getPipelineObj(String pipelineId) {
        gPayload gpay = null;
        try {

            gpay = gson.fromJson(dbe.getResourceNodeSubmission(pipelineId), gPayload.class);

        }
        catch(Exception ex) {
            logger.error("getPipelineObj " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        return gpay;
    }

    public boolean nodeExist(String region, String agent, String plugin) {
        return dbe.nodeExist(region,agent,plugin);
    }

    public void addPNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code) {

        if(dbe.nodeExist(null,null, plugin)) {
            dbe.updatePNode(agent, plugin, status_code, status_desc, watchdog_period, watchdog_ts, pluginname, jarfile, version, md5, configparams, persistence_code);
        } else {
            dbe.addPNode(agent, plugin, status_code, status_desc, watchdog_period, watchdog_ts, pluginname, jarfile, version, md5, configparams, persistence_code);
        }
    }


    // Replace the existing nodeUpdate method in java/io/cresco/agent/db/DBInterfaceImpl.java
// with the following:

    @Override
    public boolean nodeUpdate(MsgEvent de) {
        boolean wasProcessedCorrectly = false; // Default to false, indicates if any valid action was taken

        try {
            // Extract parameters relevant to watchdog timestamp updates
            String region_watchdog_update = de.getParam("region_watchdog_update");
            String agent_watchdog_update = de.getParam("agent_watchdog_update");
            String plugin_watchdog_update = de.getParam("plugin_watchdog_update");
            int timestampUpdatesPerformed = 0;

            // Attempt to update watchdog timestamps if corresponding parameters are present
            if (region_watchdog_update != null) {
                if (dbe.updateWatchDogTS(region_watchdog_update, null, null) > 0) {
                    timestampUpdatesPerformed++;
                    logger.trace("Watchdog TS updated for region: {}", region_watchdog_update);
                } else {
                    logger.warn("Failed to update watchdog TS for region: {}", region_watchdog_update);
                }
            }
            if (agent_watchdog_update != null) { // This matches the error log context
                if (dbe.updateWatchDogTS(null, agent_watchdog_update, null) > 0) {
                    timestampUpdatesPerformed++;
                    logger.trace("Watchdog TS updated for agent: {}", agent_watchdog_update);
                } else {
                    logger.warn("Failed to update watchdog TS for agent: {}", agent_watchdog_update);
                }
            }
            if (plugin_watchdog_update != null) {
                if (dbe.updateWatchDogTS(null, null, plugin_watchdog_update) > 0) {
                    timestampUpdatesPerformed++;
                    logger.trace("Watchdog TS updated for plugin: {}", plugin_watchdog_update);
                } else {
                    logger.warn("Failed to update watchdog TS for plugin: {}", plugin_watchdog_update);
                }
            }

            // If any timestamp was successfully updated, consider the core watchdog function successful.
            if (timestampUpdatesPerformed > 0) {
                wasProcessedCorrectly = true;
            }

            // Now, handle the 'mode' parameter and call to dbe.nodeUpdateStatus
            // The error "[InterfaceImpl] nodeUpdate() node mode found!" implies 'mode' is present.
            String mode = de.getParam("mode");

            if (mode != null) {
                logger.debug("Mode '{}' found in MsgEvent (Type: {}). Processing with dbe.nodeUpdateStatus.", mode, de.getMsgType().name());

                String regionconfigs = de.paramsContains("regionconfigs") ? de.getCompressedParam("regionconfigs") : null;
                String agentconfigs = de.paramsContains("agentconfigs") ? de.getCompressedParam("agentconfigs") : null;
                String pluginconfigs = de.paramsContains("pluginconfigs") ? de.getCompressedParam("pluginconfigs") : null;

                boolean statusUpdateCallSuccess = dbe.nodeUpdateStatus(mode, region_watchdog_update, agent_watchdog_update, plugin_watchdog_update, regionconfigs, agentconfigs, pluginconfigs);

                if (statusUpdateCallSuccess) {
                    // If dbe.nodeUpdateStatus also reports success, then all intended operations worked.
                    wasProcessedCorrectly = true;
                } else {
                    // dbe.nodeUpdateStatus returned false. This indicates an issue with the 'mode' processing or config parts.
                    logger.warn("nodeUpdate: Mode '{}' present, but dbe.nodeUpdateStatus returned false. Timestamp updates success: {}. Msg: {}",
                            mode, (timestampUpdatesPerformed > 0), de.getParams());
                    // If timestamp updates didn't happen AND this also failed, then the whole operation is a failure.
                    // If timestamps *did* succeed, wasProcessedCorrectly remains true from the earlier check.
                    if (timestampUpdatesPerformed == 0) {
                        wasProcessedCorrectly = false;
                    }
                }
            } else { // mode is null
                if (!wasProcessedCorrectly) { // Only an issue if timestamps also weren't updated
                    logger.warn("nodeUpdate() called with no 'mode' and no specific watchdog parameters. Msg: {}", de.getParams());
                }
            }
        } catch (Exception ex) {
            logger.error("DBInterfaceImpl.nodeUpdate() Exception: {}", ex.getMessage(), ex);
            wasProcessedCorrectly = false; // Ensure false on any exception
        }

        logger.debug("DBInterfaceImpl.nodeUpdate returning: {} for MsgType: {}, Mode: {}, AgentWatchdog: {}",
                wasProcessedCorrectly, de.getMsgType().name(), de.getParam("mode"), de.getParam("agent_watchdog_update"));
        return wasProcessedCorrectly;
    }

    public String getPipeline(String pipelineId) {
        return dbe.getResourceNodeSubmission(pipelineId);
    }

    public Map<String, NodeStatusType> getEdgeHealthStatus(String region, String agent, String pluginId) {

        Map<String, NodeStatusType> nodeStatusMap = null;
        try {
            nodeStatusMap = new HashMap<>();

            Map<String,Integer> nodeStatusMapTmp = dbe.getNodeStatusCodeMap(region,agent);

            for (Map.Entry<String, Integer> pair : nodeStatusMapTmp.entrySet()) {

                String node = pair.getKey();
                int status_code = pair.getValue();

                switch (status_code) {
                    case 3:  nodeStatusMap.put(node, NodeStatusType.STARTING);
                        break;
                    case 7:  nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 8:  nodeStatusMap.put(node, NodeStatusType.DISABLED);
                        break;
                    case 9:  nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 10:  nodeStatusMap.put(node, NodeStatusType.ACTIVE);
                        break;
                    case 40:  nodeStatusMap.put(node, NodeStatusType.STALE);
                        break;
                    case 41:  nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 50:  nodeStatusMap.put(node, NodeStatusType.LOST);
                        break;
                    case 80:  nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 90: nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 91: nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    case 92: nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                    default: nodeStatusMap.put(node, NodeStatusType.ERROR);
                        break;
                }
            }

            int periodMultiplier = plugin.getConfig().getIntegerParam("period_multiplier",10);
            List<String> pendingStaleList = dbe.getStaleNodeList(region,agent, periodMultiplier);
            for(String node : pendingStaleList) {
                if(nodeStatusMap.containsKey(node)) {
                    nodeStatusMap.put(node, NodeStatusType.PENDINGSTALE);
                }
            }

        }
        catch(Exception ex) {
            //logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return nodeStatusMap;
    }

    public int setNodeStatusCode(String regionId, String agentId, String pluginId, int status_code, String status_desc) {
        return dbe.setNodeStatusCode(regionId,agentId,pluginId,status_code,status_desc);
    }

    public String getPipelineInfo(String pipeline_action) {
        String queryReturn = null;
        try
        {
            Map<String,List<Map<String,String>>> queryMap;

            queryMap = new HashMap<>();
            List<Map<String,String>> pipelineArray = new ArrayList<>();
            List<String> pipelines = null;
            if(pipeline_action != null) {
                pipelines = new ArrayList<>();
                pipelines.add(pipeline_action);
            } else {
                pipelines = dbe.getResourceNodeList();
            }

            for(String pipelineId :pipelines) {
                Map<String,String> pipelineMap = dbe.getResourceNodeStatusMap(pipelineId);
                if(!pipelineMap.isEmpty()) {
                    pipelineArray.add(pipelineMap);
                }
            }
            queryMap.put("pipelines",pipelineArray);

            queryReturn = gson.toJson(queryMap);
            //queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress((gson.toJson(queryMap))));

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return queryReturn;

    }

    public gPayload createPipelineRecord(String tenant_id, String gPayload) {

        gPayload gpay = null;
        try {
            gpay = gson.fromJson(gPayload, gPayload.class);

            gpay.pipeline_id = "resource-" + UUID.randomUUID().toString();
            gpay.status_code = "3";
            gpay.status_desc = "Record added to DB.";

            dbe.addResource(gpay.pipeline_id,gpay.pipeline_name,Integer.parseInt(tenant_id),3,"Record added to DB.",gson.toJson(gpay));

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        return gpay;
    }

    public String addINode(String resourceId, String inodeId, int status_code, String status_desc, String configparams) {
        dbe.addInode(inodeId,resourceId,status_code,status_desc, configparams);
        return null;
    }

    public gPayload createPipelineNodes(gPayload gpay) {

        try
        {
            //create nodes and links for pipeline records
            HashMap<String,String> iNodeHm = new HashMap<>();
            HashMap<String,String> vNodeHm = new HashMap<>();

            HashMap<String,String> incomingToINodeMap = new HashMap<>();

            for(gNode node : gpay.nodes)
            {
                try
                {
                    String originalInodeId = node.node_id;

                    //add resource_id to configs
                    node.params.put("resource_id",gpay.pipeline_id);

                    String vnodeId = "vnode-" + UUID.randomUUID().toString();
                    String inodeId = "inode-" + UUID.randomUUID().toString();

                    //create clean vnode first
                    dbe.addVnode(vnodeId,gpay.pipeline_id,inodeId,gson.toJson(node.params));
                    vNodeHm.put(node.node_id, vnodeId);

                    //create anotated inode next
                    dbe.addInode(inodeId,gpay.pipeline_id,3,"Added inode to database.",gson.toJson(node.params));
                    iNodeHm.put(node.node_id, inodeId);

                    node.node_id = inodeId;
                    node.params.put("inode_id",node.node_id);

                    incomingToINodeMap.put(originalInodeId, node.node_id);


                }
                catch(Exception ex)
                {
                    logger.error("createPipelineNodes() Nodes: " + ex.toString());
                    StringWriter errors = new StringWriter();
                    ex.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
            }

            for(gEdge edge : gpay.edges) {
                try
                {

                    edge.edge_id = "edge=" + UUID.randomUUID().toString();
                    if(incomingToINodeMap.containsKey(edge.node_from)) {
                        edge.node_from = incomingToINodeMap.get(edge.node_from);
                    }

                    if(incomingToINodeMap.containsKey(edge.node_to)) {
                        edge.node_to = incomingToINodeMap.get(edge.node_to);
                    }

                    /*
                    if((edge.node_from != null) && (edge.node_to != null)) {
                        if ((vNodeHm.containsKey(edge.node_from)) && (vNodeHm.containsKey(edge.node_to))) {

                            //create edge between vnodes
                            logger.debug("From vID : " + edge.node_from + " TO vID: " + edge.node_to);
                            String edge_id = createVEdge(vNodeHm.get(edge.node_from), vNodeHm.get(edge.node_to));
                            edge.edge_id = edge_id;

                            logger.debug("vedgeid: " + edge_id + " from: " + vNodeHm.get(edge.node_from) + " to:" + vNodeHm.get(edge.node_to));

                            //create edge between inodes
                            String iedge_id = createIEdge(iNodeHm.get(edge.node_from), iNodeHm.get(edge.node_to));
                            logger.debug("iedgeid: " + iedge_id + " from: " + iNodeHm.get(edge.node_from) + " to:" + iNodeHm.get(edge.node_to));

                            //assign vEdge ID
                            logger.debug("pre edge.node_from=" + edge.node_from);
                            logger.debug("pre edge.node_to=" + edge.node_to);

                            edge.edge_id = iedge_id;

                            edge.node_from = iNodeHm.get(edge.node_from);
                            edge.node_to = iNodeHm.get(edge.node_to);

                            logger.debug("post edge.node_from=" + edge.node_from);
                            logger.debug("post edge.node_to=" + edge.node_to);

                            //logger.debug("edge.node_from inode: " + eNodeHm.get(edge.node_from));
                            //logger.debug("edge.node_to inode: " + eNodeHm.get(edge.node_to));

                        }

                    }
                    */
                    //iEdge
                }
                catch(Exception ex)
                {
                    logger.error("createPipelineNodes() Edges: " + ex.toString());
                    StringWriter errors = new StringWriter();
                    ex.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
            }


            //return gpay;
            gpay.status_code = "3";
            gpay.status_desc = "Pipeline Nodes Created.";
            updatePipeline(gpay);
            //setPipelineStatus(gpay.pipeline_id,"3","Pipeline Nodes Created.");
            return gpay;
        }
        catch(Exception ex)
        {
            logger.error("createPipelineNodes(): " + ex.toString());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        gpay.status_code = "1";
        gpay.status_desc = "Failed to create Pipeline.";
        //setPipelineStatus(gpay.pipeline_id,"1", "Failed to create Pipeline.");
        updatePipeline(gpay);
        return gpay;
    }

    public boolean setPipelineStatus(String pipelineId, String status_code, String status_desc) {
        return dbe.setResourceNodeStatus(pipelineId, Integer.parseInt(status_code), status_desc) == 1;

    }

    public boolean updatePipeline(gPayload gpay) {
        try
        {

            String pipelineId = gpay.pipeline_id;
            String status_code = gpay.status_code;
            String status_desc = gpay.status_desc;
            String submission = gson.toJson(gpay);
            dbe.updateResource(pipelineId,Integer.parseInt(status_code),status_desc,submission);
            return true;
        }
        catch(Exception ex)
        {
            logger.error("updatePipeline Error: " + ex.toString());
        }

        return false;
    }

    public boolean updateINodeAssignment(String inodeId, int status_code, String status_desc, String regionId, String agentId, String pluginId) {
        try
        {
            dbe.updateINodeAssignment(inodeId,status_code,status_desc,regionId,agentId,pluginId);
            logger.debug("MAP : " + dbe.getInodeMap(inodeId).toString());

            return true;
        }
        catch(Exception ex)
        {
            logger.error("updatePipeline Error: " + ex.toString());
        }

        return false;
    }

    public String getAgentList(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> regionList;
            if(actionRegion != null) {
                regionList = new ArrayList<>();
                regionList.add(actionRegion);
            } else {
                regionList = dbe.getNodeList(null,null);
            }
            for(String region : regionList) {

                List<String> agentList = dbe.getNodeList(region, null);

                if (agentList != null) {
                    for (String agent : agentList) {
                        Map<String, String> regionMap = new HashMap<>();
                        //logger.trace("Agent : " + region);
                        regionMap.put("agent_id", agent);
                        regionMap.put("region_id", region);
                        regionMap.put("plugins", String.valueOf(dbe.getNodeCount(region, agent)));
                        Map<String,String> aNode = dbe.getANode(agent);
                        regionMap.put("status_desc", aNode.get("status_desc"));
                        try {

                            try {
                                String configParamString  = dbe.getNodeConfigParams(region,agent,null);
                                Type type = new TypeToken<Map<String, String>>() {
                                }.getType();
                                Map<String, String> params = gson.fromJson(configParamString, type);
                                regionMap.put("location",params.get("location"));
                                regionMap.put("platform",params.get("platform"));
                                regionMap.put("environment",params.get("environment"));

                            } catch(Exception ex) {
                                logger.error(ex.getMessage());
                                StringWriter errors = new StringWriter();
                                ex.printStackTrace(new PrintWriter(errors));
                                logger.error(errors.toString());
                                regionMap.put("location","unknown");
                                regionMap.put("platform","unknown");
                                regionMap.put("environment","unknown");
                            }


                        } catch (Exception ex) {
                            logger.error("getAgentList() Map " + ex.toString());
                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            ex.printStackTrace(pw);
                            logger.error(sw.toString()); //
                        }
                        regionArray.add(regionMap);
                    }
                }
                queryMap.put("agents", regionArray);
            }
            queryReturn = gson.toJson(queryMap);

        }
        catch(Exception ex)
        {
            logger.error("getAgentList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public int getINodeStatus(String inodeId) {
        return dbe.getINodeStatus(inodeId);
    }

    public int getPNodePersistenceCode(String plugin) { return dbe.getPNodePersistenceCode(plugin); }

    public int setPNodePersistenceCode(String plugin, int persistence_code) {
        return dbe.setPNodePersistenceCode(plugin,persistence_code);
    }

    public Map<String,List<pNode>> getPluginListRepoSet() {

        AtomicBoolean lockRepoList = new AtomicBoolean();

        Map<String,List<pNode>> pluginRepoMap = null;
        try
        {
            List<String> repoList = getPluginListRepoInventory();

            if(repoList != null) {

                logger.debug("getPluginListRepoInventory() SIZE " + repoList.size());

                pluginRepoMap = new HashMap<>();

                for (String repoJSON : repoList) {
                    Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(repoJSON, type);
                    List<Map<String, String>> tmpPluginsList = myRepoMap.get("plugins");
                    List<Map<String, String>> tmpServerList = myRepoMap.get("server");

                    if(tmpPluginsList != null) {

                        for (Map<String, String> plugin : tmpPluginsList) {
                            String name = plugin.get("pluginname");
                            String jarfile = plugin.get("jarfile");
                            String md5 = plugin.get("md5");
                            String version = plugin.get("version");

                            synchronized (lockRepoList) {

                                if (!pluginRepoMap.containsKey(name)) {
                                    List<pNode> nodeList = new ArrayList<>();
                                    pNode node = new pNode(name, jarfile, md5, version, tmpServerList);
                                    nodeList.add(node);
                                    pluginRepoMap.put(name, nodeList);
                                } else {
                                    List<pNode> nodeList = new ArrayList<>(pluginRepoMap.get(name));

                                    for (Iterator<pNode> iterator = pluginRepoMap.get(name).iterator(); iterator.hasNext(); ) {
                                        pNode node = iterator.next();
                                        if (node.isEqual(name, jarfile, md5, version)) {
                                            nodeList.get(nodeList.indexOf(node)).addRepos(tmpServerList);
                                        } else {
                                            pNode newnode = new pNode(name, jarfile, md5, version, tmpServerList);
                                            nodeList.add(newnode);
                                        }
                                    }

                                    pluginRepoMap.put(name, nodeList);
                                }

                            }
                        }

                    }

                }
            }

        }
        catch(Exception ex)
        {
            logger.error("getPluginListRepoSet() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return pluginRepoMap;
    }

    public List<Map<String,String>> getPluginListMapByType(String actionPluginTypeId, String actionPluginTypeValue) {
        List<Map<String,String>> configMapList = null;

        try
        {
            configMapList = dbe.getPluginListMapByType(actionPluginTypeId,actionPluginTypeValue);

        }
        catch(Exception ex)
        {
            logger.error("getPluginListByType() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return  configMapList;
    }

    public String getPluginListByType(String actionPluginTypeId, String actionPluginTypeValue) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            queryMap.put("plugins", dbe.getPluginListMapByType(actionPluginTypeId,actionPluginTypeValue));
            queryReturn = gson.toJson(queryMap);

        }
        catch(Exception ex)
        {
            logger.error("getPluginListByType() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public List<String> getPluginListRepoInventory() {
        List<String> repoList = null;
        try
        {
            repoList = new ArrayList<>();

            //get the list of all global repo plugins
            MsgEvent requestRepos  = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            requestRepos.setParam("action","listpluginsbytype");
            requestRepos.setParam("action_plugintype_id","pluginname");
            requestRepos.setParam("action_plugintype_value","io.cresco.repo");

            MsgEvent responseRepos = plugin.sendRPC(requestRepos);

            String repoPluginsJSON = null;

            if(requestRepos != null) {
                repoPluginsJSON = responseRepos.getCompressedParam("pluginsbytypelist");
            }

            //String repoPluginsJSON = getPluginListByType("pluginname","io.cresco.repo");

            //query all repo plugins
            if(repoPluginsJSON != null) {
                Map<String, List<Map<String, String>>> myMap = gson.fromJson(repoPluginsJSON, type);

                for (Map<String, String> perfMap : myMap.get("plugins")) {

                    String region = perfMap.get("region");
                    String agent = perfMap.get("agent");
                    String pluginID = perfMap.get("pluginid");

                    MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                    request.setParam("action", "repolist");

                    MsgEvent response = plugin.sendRPC(request);
                    if(response != null) {
                        if (response.getParam("repolist") != null) {
                            String repoListJson = response.getCompressedParam("repolist");
                            repoList.add(repoListJson);
                        } else {
                            logger.error("NO RESPONSE FROM REPO LIST MISSING PARAM");
                        }
                    } else {
                        logger.error("NO RESPONSE FROM REPO LIST MISSING MESSAGE");
                    }
                }
            }

        }
        catch(Exception ex)
        {
            logger.error("getPluginListByType() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return repoList;
    }

    public String getRegionList() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();
            List<String> regionList = dbe.getNodeList(null,null);

            if(regionList != null) {
                for (String region : regionList) {
                    Map<String,String> regionMap = new HashMap<>();
                    logger.trace("Region : " + region);
                    List<String> agentList = dbe.getNodeList(region, null);
                    regionMap.put("name",region);
                    if(agentList != null) {
                        regionMap.put("agents", String.valueOf(agentList.size()));
                    } else {
                        regionMap.put("agents", "0");
                    }
                    regionArray.add(regionMap);
                }
            }
            queryMap.put("regions",regionArray);

            //queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            queryReturn = gson.toJson(queryMap);

        }
        catch(Exception ex)
        {
            logger.error("getRegionList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public String getPluginListRepo() {
        String returnString = null;
        try
        {
            Map<String,List<Map<String,String>>> myRepoMapReturn = new HashMap<>();

            List<Map<String,String>> pluginsList = new ArrayList<>();
            List<String> repoList = getPluginListRepoInventory();

            for(String repoJSON : repoList) {
                Map<String,List<Map<String,String>>> myRepoMap = gson.fromJson(repoJSON, type);
                if(myRepoMap != null) {
                    if (myRepoMap.containsKey("plugins")) {
                        List<Map<String, String>> tmpPluginsList = myRepoMap.get("plugins");
                        pluginsList.addAll(tmpPluginsList);
                    }
                }
            }

            myRepoMapReturn.put("plugins",pluginsList);
            returnString = gson.toJson(myRepoMapReturn);
        }
        catch(Exception ex)
        {
            logger.error("getPluginListByType() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return returnString;
    }

    public String getPluginList(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> regionList;
            if(actionRegion != null) {
                regionList = new ArrayList<>();
                regionList.add(actionRegion);
            } else {
                regionList = dbe.getNodeList(null,null);
            }
            for(String region : regionList) {
                List<String> agentList;
                if(actionAgent != null) {
                    agentList = new ArrayList<>();
                    agentList.add(actionAgent);
                } else {
                    agentList = dbe.getNodeList(region,null);
                }
                //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
                //Map<String,String> rMap = new HashMap<String,String>();
                if (agentList != null) {
                    for (String agent : agentList) {
                        logger.trace("Agent : " + region);
                        List<String> pluginList = dbe.getNodeList(region, agent);
                        for(String plugin : pluginList) {
                            Map<String, String> regionMap = new HashMap<>();
                            regionMap.put("name", plugin);
                            regionMap.put("region", region);
                            regionMap.put("agent", agent);

                            try {
                                String configParamString = dbe.getNodeConfigParams(region, agent, plugin);
                                Map<String,String> configMap = gson.fromJson(configParamString,mapType);
                                regionMap.putAll(configMap);
                            }catch (Exception ex) {
                                logger.error("Could not get plugin configMap");
                            }

                            regionArray.add(regionMap);
                        }
                    }
                }
                queryMap.put("plugins", regionArray);
            }
            //queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            queryReturn = gson.toJson(queryMap);

        }
        catch(Exception ex)
        {
            logger.error("getAgentList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public String getPluginInfo(String actionRegion, String actionAgent, String actionPlugin) {
        String queryReturn = null;
        try
        {
            queryReturn = dbe.getNodeConfigParams(actionRegion,actionAgent,actionPlugin);
        }
        catch(Exception ex)
        {
            logger.error("getPluginInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public Map<String,String> getPNode(String pluginId) {
        return dbe.getPNode(pluginId);
    }

    public Map<String,String> getANode(String agentId) {
        return dbe.getANode(agentId);
    }

    public Map<String,String> getRNode(String regionId) {
        return dbe.getRNode(regionId);
    }

    public int getPipelineStatusCode(String pipelineId) {
        return dbe.getResourceNodeStatus(pipelineId);
    }

    public boolean removeNode(String region, String agent, String plugin) {
        return dbe.removeNode(region,agent,plugin);
    }

    public boolean removePipeline(String pipelineId) {
        return dbe.removeResource(pipelineId);
    }


    public boolean updateKPI(String region, String agent, String pluginId, String resource_id, String inode_id, Map<String,String> params) {
        boolean isUpdated = false;

        logger.debug("resource_id:" + resource_id + " inode_id:" + inode_id + " params:" + params.toString());

        try {

            if(params != null) {
                if(params.get("perf") != null) {
                    if (!dbe.inodeKPIExist(inode_id)) {
                        dbe.addInodeKPI(inode_id, params.get("perf"));
                    } else {
                        dbe.updateInodeKPI(inode_id, params.get("perf"));
                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return isUpdated;
    }

    public String getGPipeline(String actionPipelineId) {
        String queryReturn = null;
        try
        {
            gPayload gpay = getPipelineObj(actionPipelineId);
            if(gpay != null) {
                Map<String, String> pipeStatMap = dbe.getResourceNodeStatusMap(actionPipelineId);
                gpay.status_code = pipeStatMap.get("status_code");
                gpay.status_desc = pipeStatMap.get("status_desc");

                List<gNode> nodes = new ArrayList<>(gpay.nodes);
                gpay.nodes.clear();

                for(gNode node : nodes) {
                    Map<String,String> inodeParams = dbe.getInodeMap(node.node_id);

                    //String status_code = String.valueOf(dbe.getINodeStatus(node.node_id));

                    //String regionId = inodeParams.get("region_id");
                    //String agentId = inodeParams.get("agent_id");
                    //String pluginId = inodeParams.get("plugin_id");

                    node.params.put("region_id",inodeParams.get("region_id"));
                    node.params.put("agent_id",inodeParams.get("agent_id"));
                    node.params.put("plugin_id",inodeParams.get("plugin_id"));

                    node.params.put("status_code",inodeParams.get("status_code"));
                    node.params.put("status_desc",inodeParams.get("status_desc"));

                    node.params.put("inode_id",inodeParams.get("inode_id"));
                    node.params.put("resource_id",inodeParams.get("resource_id"));



                    //logger.error("MAP: " + inodeParams.toString());

/*
                    inodeMap.put("inode_id", rs.getString("inode_id"));
                    inodeMap.put("resource_id", rs.getString("resource_id"));


                    inodeMap.put("region_id", rs.getString("region_id"));
                    inodeMap.put("agent_id", rs.getString("agent_id"));
                    inodeMap.put("plugin_id", rs.getString("plugin_id"));

                    inodeMap.put("status_code", rs.getString("status_code"));
                    inodeMap.put("status_desc", rs.getString("status_desc"));

                    */
                    //String status_code = inodeParams.get("status_code");
                    /*
                    String status_desc = inodeParams.get("status_desc");
                    String params = inodeParams.get("configparams");
                    String inode_id = inodeParams.get("inode_id");
                    String resource_id = inodeParams.get("resource_id");

                    node.params.put("status_code", status_code);
                    node.params.put("status_desc", status_desc);
                    node.params.put("params", params);
                    node.params.put("inode_id", inode_id);
                    node.params.put("resource_id", resource_id);
                    */

                    gpay.nodes.add(node);

                }


                /*
                int nodeSize = gpay.nodes.size();
                for (int i = 0; i < nodeSize; i++) {
                    gpay.nodes.get(i).params.clear();

                    //String inodeNodeid = dbe.getInodeMap(gpay.nodes.get(i).node_id) dba.getINodeNodeId(gpay.nodes.get(i).node_id);
                    Map<String,String> inodeParams = dbe.getInodeMap(gpay.nodes.get(i).node_id);

                    String status_code = inodeParams.get("status_code");
                    String status_desc = inodeParams.get("status_desc");
                    String params = inodeParams.get("configparams");
                    String inode_id = inodeParams.get("inode_id");
                    String resource_id = inodeParams.get("resource_id");

                    //String status_code = agentcontroller.getGDB().dba.getINodeParam(gpay.nodes.get(i).node_id, "status_code");
                    //String status_desc = agentcontroller.getGDB().dba.getINodeParam(gpay.nodes.get(i).node_id, "status_desc");
                    gpay.nodes.get(i).params.put("status_code", status_code);
                    gpay.nodes.get(i).params.put("status_desc", status_desc);
                    gpay.nodes.get(i).params.put("params", params);
                    gpay.nodes.get(i).params.put("inode_id", inode_id);
                    gpay.nodes.get(i).params.put("resource_id", resource_id);

                }
                */



                //logger.error("json: " + gson.toJson(gpay));

                queryReturn = gson.toJson(gpay);
            }

        } catch(Exception ex) {
            logger.error("getGPipeline() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return queryReturn;

    }

    public String getGPipelineExport(String actionPipelineId) {
        String queryReturn = null;
        try
        {
            queryReturn = getPipeline(actionPipelineId);
            //if(returnGetGpipeline != null) {
            //    queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress(returnGetGpipeline));
            //}

        } catch(Exception ex) {
            logger.error("getGPipelineExport() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return queryReturn;

    }

    public List<String> getNodeList(String region, String agent) {
        //logger.error("List<String> getNodeList(String region, String agent, String plugin)");
        return dbe.getNodeList(region,agent);
    }

    public boolean removeNode(MsgEvent de) {
        boolean wasRemoved = false;
        try {

            String unregister_region = de.getParam("unregister_region_id");
            String unregister_agent = de.getParam("unregister_agent_id");
            String unregister_plugin = de.getParam("unregister_plugin_id");

            dbe.removeNode(unregister_region, unregister_agent, unregister_plugin);

            wasRemoved = true;

        } catch (Exception ex) {
            logger.error("Boolean removeNode(MsgEvent de) " + ex.getMessage());
        }

        return wasRemoved;
    }

    public Map<String,String> getDBExport(boolean regions, boolean agents, boolean plugins, String region_id, String agent_id, String plugin_id) {
        return dbe.getDBExport(regions,agents,plugins,region_id,agent_id,plugin_id);
    }

    /*
    public String getIsAssignedInfo(String resourceid,String inodeid, boolean isResourceMetric) {

        String queryReturn = null;
        try
        {

            Map<String,String> inodeMap = getInodeMap(inodeid);

            //logger.error("inode: " + inodeid + " region_id:" + inodeMap.get("region_id") + " agent_id:" + inodeMap.get("agent_id") + " plugin_id:" + inodeMap.get("plugin_id"));


            queryReturn = controllerEngine.getPerfControllerMonitor().getKPIInfo(inodeMap.get("region_id"),inodeMap.get("agent_id"),inodeMap.get("plugin_id"));

            //logger.error("KPI STRING: " + kpiString);


        } catch(Exception ex) {
            logger.error("getIsAssignedInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }
    */



    //complete

    public String getIsAssignedInfo(String resourceid,String inodeid, boolean isResourceMetric) {
        logger.error("getIsAssignedInfo not implemented inode: " + inodeid + " resouce_id:" + resourceid);
        return null;
    }

    public void shutdown() {
        dbe.shutdown();
        logger.info("DB Engine Shutdown");

    }

    public Map<String,String> paramStringToMap(String param) {
        Map<String,String> params = null;
        logger.error("Map<String,String> paramStringToMap(String param)");
        return params;
    }

    public Map<String,String> getResourceTotal() {
        Map<String,String> resourceTotal = null;
        logger.error("Map<String,String> getResourceTotal()");
        return resourceTotal;
    }

    private String getGlobalNetResourceInfo() {
        String queryReturn = null;
        logger.error("String getGlobalNetResourceInfo()");
        return queryReturn;

    }

    public String addINodeResource(String resource_id, String inode_id) {
        logger.error("String addINodeResource(String resource_id, String inode_id)");
        return null;
    }

    public String getNetResourceInfo() {
        String queryReturn = null;
        logger.error("String getNetResourceInfo()");
        return queryReturn;

    }

    public Map<String,String> getResourceTotal2() {
        Map<String,String> resourceTotal = null;
        logger.error("Map<String,String> getResourceTotal2()");
        return resourceTotal;
    }

    public Map<String, NodeStatusType> getNodeStatus(String region, String agent, String plugin) {

        Map<String, NodeStatusType> nodeStatusMap = null;
        logger.error("Map<String,NodeStatusType> getNodeStatus(String region, String agent, String plugin)");
        return nodeStatusMap;
    }

    public boolean setEdgeParam(String edgeId, String paramKey, String paramValue) {
        logger.error("boolean setEdgeParam(String edgeId, String paramKey, String paramValue)");
        return false;
    }

    public Map<String,String> getEdgeParamsNoTx(String edgeId) {
        logger.error("Map<String,String> getEdgeParamsNoTx(String edgeId)");
        return null;
    }

    public Map<String,String> getNodeParams(String node_id) {
        logger.error("Map<String,String> getNodeParams(String node_id)");
        return null;
    }

    public String getINodeParams(String iNode_id) {
        logger.error("String getINodeParams(String iNode_id)");
        return null;
    }

    public String getINodeParam(String inode_id, String param) {
        logger.error("String getINodeParam(String inode_id, String param)");
        return null;
    }

    public Map<String,String> getpNodeINode(String iNode_id) {
        logger.error("Map<String,String> getpNodeINode(String iNode_id)");
        return null;
    }

    public List<String> getANodeFromIndex(String indexName, String indexValue) {
        logger.error("List<String> getANodeFromIndex(String indexName, String indexValue)");
        return null;
    }

    public boolean setINodeParam(String inode_id, String paramKey, String paramValue) {
        logger.error("setINodeParam(String inode_id, String paramKey, String paramValue)");
        return false;
    }

    public String addEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className, Map<String,String> paramMap) {
        logger.error("String addEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className, Map<String,String> paramMap)");
        return null;
    }

    public String getNodeId(String region, String agent, String plugin) {
        logger.error("String getNodeId(String region, String agent, String plugin)");
        return  null;
    }

    public String addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin) {
        logger.error("String addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin)");
        return null;
    }

    public String getResourceEdgeId(String resource_id, String inode_id) {
        logger.error("String getResourceEdgeId(String resource_id, String inode_id)");
        return null;
    }
    public String getIsAssignedParam(String edge_id,String param_name) {
        logger.error("String getIsAssignedParam(String edge_id,String param_name)");
        return null;
    }

    public boolean setDBImport(String exportData) {
        logger.error("boolean setDBImport(String exportData)");
        return false;
    }

}
