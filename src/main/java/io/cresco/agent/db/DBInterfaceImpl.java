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
    private DBEngine dbe;

    private Gson gson;
    private Type type;

    public BlockingQueue<String> importQueue;


    public DBInterfaceImpl(PluginBuilder plugin, DBEngine dbe) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(DBInterfaceImpl.class.getName(),CLogger.Level.Info);
        this.dbe = dbe;
        //this.dbe = new DBEngine(plugin);

        this.importQueue = new LinkedBlockingQueue<>();
        this.gson = new Gson();
        this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

    }


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

    /*
    public void addNode(String region, String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String configparams) {
        dbe.addNode(region,agent,plugin, status_code,status_desc,watchdog_period,watchdog_ts,configparams);
    }
    */

    public boolean addNodeFromUpdate(MsgEvent de) {
        boolean wasAdded = false;

        try {

            String region = de.getParam("region_name");
            String agent = de.getParam("agent_name");
            String plugin = de.getParam("plugin_id");

            logger.debug("Adding Node: " + de.getParams().toString());

            if(region != null) {
                if(!dbe.nodeExist(region,null,null)) {
                    //fixme take into account current state
                    //add region, this will need to be more complex in future
                    dbe.addNode(region,null,null,0,"Region added by Agent",0, 0, null);
                }

                //if region update, otherwise ignore
                if((region != null) && (agent == null)) {
                    dbe.updateNode(region,null,null,0,"Region added by Agent",0, 0, null);
                }

            }

            //Is Agent
            if((region != null) && (agent != null) && (plugin == null)) {

                if(!dbe.nodeExist(region,agent,null)) {
                    //fixme take into account current state
                    //add region, this will need to be more complex in future
                    dbe.addNode(region,agent,null,0,"Agent added by Agent",Integer.parseInt(de.getParam("watchdogtimer")),System.currentTimeMillis(),de.getParam("configparams"));
                } else {
                    dbe.updateNode(region,agent,null,0,"Agent added by Agent",Integer.parseInt(de.getParam("watchdogtimer")),System.currentTimeMillis(),de.getParam("configparams"));
                }

                if (de.getParam("pluginconfigs") != null) {
                    List<Map<String, String>> configMapList = new Gson().fromJson(de.getCompressedParam("pluginconfigs"),
                            new TypeToken<List<Map<String, String>>>() {
                            }.getType());

                    //Add Plugin Information
                    for (Map<String, String> configMap : configMapList) {
                        String pluginId = configMap.get("pluginid");
                        String status_code = configMap.get("status_code");
                        String status_desc = configMap.get("status_desc");
                        String configparams = configMap.get("configparams");

                        logger.debug("Adding Sub-Node: " + configMap.toString());

                        if(!nodeExist(region,agent,pluginId)) {
                            //todo plugins need to list their watchdog peroid
                            dbe.addNode(region, agent, pluginId, Integer.parseInt(status_code), status_desc, Integer.parseInt(de.getParam("watchdogtimer")), System.currentTimeMillis(), configparams);
                        } else {
                            dbe.updateNode(region, agent, pluginId, Integer.parseInt(status_code), status_desc, Integer.parseInt(de.getParam("watchdogtimer")), System.currentTimeMillis(), configparams);
                        }
                    }
                }

                throw new NullPointerException("demo");
            }

            wasAdded = true;

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("GraphDBUpdater : addNode ERROR : " + ex.toString());
        }

        return wasAdded;
    }

    public String getPipeline(String pipelineId) {
        return dbe.getResourceNodeSubmission(pipelineId);
    }

    public boolean watchDogUpdate(MsgEvent de) {
        boolean wasUpdated = false;

        try {

            //logger.error("src_region:" + de.getSrcRegion() + " src_agent:" + de.getSrcAgent() + " src_plugin:" + de.getSrcPlugin());
            //logger.error(de.getParams().toString());
            String region = de.getParam("region_name");
            String agent = de.getParam("agent_name");
            String pluginId = de.getParam("plugin_id");


            logger.debug("watchdog() region=" + region + " agent=" + agent + " agentcontroller=" + pluginId);

            if(dbe.nodeExist(region,agent,pluginId)) {


                dbe.updateWatchDogTS(region,agent,pluginId);

                if((region != null) && (agent != null) && (pluginId == null)) {

                    //add agentcontroller configs for agent
                    if (de.getParam("pluginconfigs") != null) {

                        List<Map<String, String>> configMapList = new Gson().fromJson(de.getCompressedParam("pluginconfigs"),
                                new TypeToken<List<Map<String, String>>>() {
                                }.getType());

                        //build a list of plugins on record for an agent
                        List<String> pluginRemoveList = dbe.getNodeList(region,agent);

                        for (Map<String, String> configMap : configMapList) {
                            String subpluginId = configMap.get("pluginid");

                            //remove agentcontroller from remove list of new config exist
                            pluginRemoveList.remove(subpluginId);

                            //add new nodes not currently on the system, add node will check that they exist
                            if(!dbe.nodeExist(region,agent,subpluginId)) {

                                String status_code = configMap.get("status_code");
                                String status_desc = configMap.get("status_desc");
                                String configparams = configMap.get("configparams");

                                logger.debug("subpluginId:" + subpluginId + " status_code=" + status_code + " status_desc=" + status_desc + " watchdogtimer=" + configMap.get("watchdogtimer") + " configMap: " + configMap.toString());

                                //todo plugins need to list their watchdog peroid
                                //dbe.addNode(region,agent,subpluginId,Integer.parseInt(status_code),status_desc,Integer.parseInt(configMap.get("watchdogtimer")),System.currentTimeMillis(),configparams);
                            }
                        }

                        //remove nodes on the pluginRemoveList, they are no longer on the agent
                        for(String removePlugin : pluginRemoveList) {
                            dbe.removeNode(region,agent,removePlugin);
                        }
                    }
                }


                wasUpdated = true;
            }
            else {
                logger.error("watchdog() nodeID does not exist for region:" + region + " agent:" + agent + " pluginId:" + pluginId);
                logger.error(de.getMsgType().toString() + " [" + de.getParams().toString() + "]");

            }

        } catch (Exception ex) {
            logger.error(": watchDogUpdate ERROR : " + ex.toString());
            ex.printStackTrace();
        }
        return wasUpdated;
    }

    public Map<String, NodeStatusType> getEdgeHealthStatus(String region, String agent, String plugin) {

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

            List<String> pendingStaleList = dbe.getStaleNodeList(region,agent);
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

            for(gNode node : gpay.nodes)
            {
                try
                {
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


                }
                catch(Exception ex)
                {
                    logger.error("createPipelineNodes() Nodes: " + ex.toString());
                    //todo remove this
                    StringWriter errors = new StringWriter();
                    ex.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
            }

            for(gEdge edge : gpay.edges) {
                try
                {
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
                    //todo remove this
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
            //todo remove this
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
        if(dbe.setResourceNodeStatus(pipelineId, Integer.parseInt(status_code),status_desc) == 1) {
            return true;
        } else {
            return false;
        }

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
                        regionMap.put("name", agent);
                        regionMap.put("region", region);
                        regionMap.put("plugins", String.valueOf(dbe.getNodeCount(region, agent)));

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

            pluginRepoMap = new HashMap<>();

            for(String repoJSON : repoList) {
                Map<String,List<Map<String,String>>> myRepoMap = gson.fromJson(repoJSON, type);
                List<Map<String,String>> tmpPluginsList = myRepoMap.get("plugins");
                List<Map<String,String>> tmpServerList = myRepoMap.get("server");


                for(Map<String,String> plugin : tmpPluginsList) {
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
                            List<pNode> nodeList = new ArrayList<>();
                            nodeList.addAll(pluginRepoMap.get(name));

                            for (Iterator<pNode> iterator = pluginRepoMap.get(name).iterator(); iterator.hasNext();) {
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
            String repoPluginsJSON = getPluginListByType("pluginname","io.cresco.repo");

            Map<String,List<Map<String,String>>> myMap = gson.fromJson(repoPluginsJSON, type);

            for(Map<String,String> perfMap : myMap.get("plugins")) {

                String region = perfMap.get("region");
                String agent = perfMap.get("agent");
                String pluginID = perfMap.get("pluginid");

                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                request.setParam("action", "repolist");

                MsgEvent response = plugin.sendRPC(request);
                if(response.getParam("repolist") != null) {
                    repoList.add(response.getCompressedParam("repolist"));
                } else {
                    logger.error("NO RESPONSE FROM REPO LIST");
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

                List<gNode> nodes = new ArrayList<>();
                nodes.addAll(gpay.nodes);
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
            String returnGetGpipeline = getPipeline(actionPipelineId);
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
        logger.error("inode: " + inodeid + " resouce_id:" + resourceid);
        return null;
    }

    public void shutdown() {
        logger.error("shutdown()");
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

    public void submitDBImport(String exportData) {
        logger.error("void submitDBImport(String exportData)");
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

    public boolean removeNode(MsgEvent de) {
        boolean wasRemoved = false;
        logger.error("Boolean removeNode(MsgEvent de)");
        return wasRemoved;
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

    public String getDBExport() {
        logger.error("String getDBExport()");
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