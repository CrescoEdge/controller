package io.cresco.agent.controller.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.app.gPayload;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalscheduler.pNode;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.xml.bind.DatatypeConverter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DBInterface {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private DBEngine gde;
    public DBBaseFunctions gdb;
    public DBApplicationFunctions dba;

    private Gson gson;
    private Type type;

    private Thread DBManagerThread;
    private BlockingQueue<String> importQueue;

    //NMS Added no-arg constructor and builder-style stuff for testing
    public DBInterface(){}

    public DBInterface controllerEngine(ControllerEngine toAdd){
        this.controllerEngine = toAdd;
        return this;
    }

    public DBInterface pluginBuilder(PluginBuilder toAdd){
        this.plugin = toAdd;
        return this;
    }

    public DBInterface logger(CLogger toAdd){
        this.logger = toAdd;
        return this;
    }

    public DBInterface importQueue(BlockingQueue<String> toAdd){
        this.importQueue = toAdd;
        return this;
    }

    public DBInterface dbEngine(DBEngine toAdd){
        this.gde = toAdd;
        return this;
    }
    public DBInterface dbBaseFunctions(DBBaseFunctions toAdd){
        this.gdb = toAdd;
        return this;
    }
    public DBInterface dbApplicationFunctions(DBApplicationFunctions toAdd){
        this.dba = toAdd;
        return this;
    }
    public DBInterface gson(Gson toAdd){
        this.gson = toAdd;
        return this;
    }
    public DBInterface gsonTypeToken(TypeToken toAdd){
        this.type = toAdd.getType();
        return this;
    }

    public DBInterface dbManagerThread(DBManager toAdd){
        this.DBManagerThread =new Thread(toAdd);
        return this;
    }

    public void startDBManagerThread(){
        this.DBManagerThread.start();
    }


    public DBInterface(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DBInterface.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(DBInterface.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        //this.agentcontroller = agentcontroller;
        this.importQueue = new LinkedBlockingQueue<>();
        this.gde = new DBEngine(controllerEngine);
        this.gdb = new DBBaseFunctions(controllerEngine,gde);
        this.dba = new DBApplicationFunctions(controllerEngine,gde);
        this.gson = new Gson();
        this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        //DB manager
        logger.debug("Starting DB Manager");
        logger.debug("Starting Broker Manager");
        this.DBManagerThread = new Thread(new DBManager(controllerEngine, importQueue));
        this.DBManagerThread.start();
    }

    /*
    public Thread getDBManagerThread() {
        return DBManagerThread;
    }

    public void setDBManagerThread(Thread activeDBManagerThread) {
        this.DBManagerThread = activeDBManagerThread;
    }
    */

    public Map<String,String> paramStringToMap(String param) {
        Map<String,String> params = null;
        try
        {

            params = new HashMap<String,String>();
            String[] pstr = param.split(",");
            for(String str : pstr)
            {
                String[] pstrs = str.split("=");
                params.put(pstrs[0], pstrs[1]);
            }
        }
        catch(Exception ex)
        {
            logger.error("paramStringToMap " + ex.toString());
        }
        return params;
    }

    public Map<String,String> getResourceTotal() {
        Map<String,String> resourceTotal = null;
        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;
        long region_count = 0;
        long agent_count = 0;
        long plugin_count = 0;

        try
        {


            resourceTotal = new HashMap<>();
            List<String> sysInfoEdgeList = controllerEngine.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
            for(String edgeID : sysInfoEdgeList) {

                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);
                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                for (String fspair : edgeParams.get("fs-map").split(",")) {
                    String[] fskey = fspair.split(":");
                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                }
            }

            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    region_count++;
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
                    if (agentList != null) {
                        for (String agent : agentList) {
                            agent_count++;
                            logger.trace("Agent : " + agent);

                            List<String> pluginList = gdb.getNodeList(region, agent, null);
                            if (pluginList != null) {

                                boolean isRecorded = false;
                                for (String pluginId : pluginList) {
                                    logger.trace("Plugin : " + plugin);
                                    plugin_count++;
                                }
                            }
                        }
                    }
                }
            }

            //logger.trace("Regions :" + region_count);
            //logger.trace("Agents :" + agent_count);
            //logger.trace("Plugins : " + plugin_count);
            logger.trace("Total CPU core count : " + cpu_core_count);
            logger.trace("Total Memory Available : " + memoryAvailable);
            logger.trace("Total Memory Total : " + memoryTotal);
            logger.trace("Total Disk Available : " + diskAvailable);
            logger.trace("Total Disk Total : " + diskTotal);
            resourceTotal.put("regions",String.valueOf(region_count));
            resourceTotal.put("agents",String.valueOf(agent_count));
            resourceTotal.put("plugins",String.valueOf(plugin_count));
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));

        }
        catch(Exception ex)
        {
            logger.error("getResourceTotal() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return resourceTotal;
    }

    public String getRegionList() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();
            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    Map<String,String> regionMap = new HashMap<>();
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
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

    public void submitDBImport(String exportData) {
        importQueue.offer(exportData);
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
                regionList = gdb.getNodeList(null,null,null);
            }
            for(String region : regionList) {

                List<String> agentList = gdb.getNodeList(region, null, null);
                //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
                //Map<String,String> rMap = new HashMap<String,String>();
                if (agentList != null) {
                    for (String agent : agentList) {
                        Map<String, String> regionMap = new HashMap<>();
                        logger.trace("Agent : " + region);
                        List<String> pluginList = gdb.getNodeList(region, agent, null);
                        regionMap.put("name", agent);
                        regionMap.put("region", region);
                        regionMap.put("plugins", String.valueOf(pluginList.size()));
                        //TODO there must be a better way to do this
                        try {
                            String nodeId = gdb.getNodeId(region, agent, null);
                            Map<String, String> params = gdb.getNodeParams(nodeId);
                            regionMap.put("location",params.get("location"));
                            regionMap.put("platform",params.get("platform"));
                            regionMap.put("environment",params.get("environment"));
                        } catch (Exception ex) {
                            //do nothing
                        }
                        regionArray.add(regionMap);
                    }
                }
                queryMap.put("agents", regionArray);
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

            //Test
            /*
            Map<String,List<pNode>> testMap = getPluginListRepoSet();
            for (Map.Entry<String, List<pNode>> entry : testMap.entrySet()) {
                String key = entry.getKey();
                List<pNode> value = entry.getValue();
                logger.error("name: " + key);
                for(pNode node : value) {
                    logger.error("\t jarfile: " + node.jarfile);
                    logger.error("\t md5 hash: " + node.md5);
                    logger.error("\t version: " +node.version);
                    logger.error("\t date: " +node.getBuildTime().toString());
                    for(Map<String,String> server : node.repoServers) {
                        logger.error("\t\t protocol: " + server.get("protocol"));
                        logger.error("\t\t ip: " + server.get("ip"));
                        logger.error("\t\t port: " + server.get("port"));
                    }
                }

            }
            */

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

    public Map<String,List<pNode>> getPluginListRepoSet() {

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

                    if(!pluginRepoMap.containsKey(name)) {
                        List<pNode> nodeList = new ArrayList<>();
                        pNode node = new pNode(name,jarfile,md5,version, tmpServerList);
                        nodeList.add(node);
                        pluginRepoMap.put(name,nodeList);
                    } else {
                        List<pNode> nodeList = pluginRepoMap.get(name);
                        for(pNode node : nodeList) {
                            if(node.isEqual(name,jarfile,md5,version)) {
                                nodeList.get(nodeList.indexOf(node)).addRepos(tmpServerList);
                            } else {
                                pNode newnode = new pNode(name,jarfile,md5,version, tmpServerList);
                                nodeList.add(newnode);
                            }
                        }
                        pluginRepoMap.put(name,nodeList);
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

        return pluginRepoMap;
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

                /*
                MsgEvent request = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Plugin List by Repo");
                request.setParam("src_region", plugin.getRegion());
                request.setParam("src_agent", plugin.getAgent());
                request.setParam("src_plugin", plugin.getPluginID());
                request.setParam("dst_region", region);
                request.setParam("dst_agent", agent);
                request.setParam("dst_plugin", perfMap.get("agentcontroller"));
                request.setParam("action", "repolist");
                */
                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                request.setParam("action", "repolist");

                MsgEvent response = plugin.sendRPC(request);

                repoList.add(response.getCompressedParam("repolist"));
                /*
                Map<String,List<Map<String,String>>> myRepoMap = gson.fromJson(response.getCompressedParam("repolist"), type);

                for(Map<String,String> contactMap : myRepoMap.get("server")) {
                    logger.error(contactMap.get("ip"));
                    logger.error(contactMap.get("protocol"));
                    logger.error(contactMap.get("port"));
                }
                */
                //String resource_node_id = agentcontroller.getGDB().dba.getResourceNodeId("sysinfo_resource");
                //String inode_node_id = agentcontroller.getGDB().dba.getINodeNodeId("sysinfo_inode");
                //String plugin_node_id = agentcontroller.getGDB().gdb.getNodeId(region,agent,"agentcontroller/0");
                //String edge_id = agentcontroller.getGDB().dba.getResourceEdgeId("sysinfo_resource", "sysinfo_inode", region, agent, "agentcontroller/0");

            }

            //queryMap.put("plugins", pluginArray);

            //queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            //queryReturn = gson.toJson(queryMap);

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

    public String getPluginListByType(String actionPluginTypeId, String actionPluginTypeValue) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> pluginArray = new ArrayList<>();

            List<String> pluginList;
            if((actionPluginTypeId == null) || (actionPluginTypeValue == null)) {
                pluginList = new ArrayList<>();
                pluginList.add(actionPluginTypeId);
            } else {
                pluginList = gdb.getPNodeFromIndex(actionPluginTypeId,actionPluginTypeValue);
            }
            for(String nodeId : pluginList) {

                Map<String,String> pluginConfig = gdb.getNodeParamsNoTx(nodeId);

                if(pluginConfig != null) {
                    pluginArray.add(pluginConfig);
                }
            }

            queryMap.put("plugins", pluginArray);

            //queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
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
                regionList = gdb.getNodeList(null,null,null);
            }
            for(String region : regionList) {
                List<String> agentList;
                if(actionAgent != null) {
                    agentList = new ArrayList<>();
                    agentList.add(actionAgent);
                } else {
                    agentList = gdb.getNodeList(region,null,null);
                }
                //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
                //Map<String,String> rMap = new HashMap<String,String>();
                if (agentList != null) {
                    for (String agent : agentList) {
                        logger.trace("Agent : " + region);
                        List<String> pluginList = gdb.getNodeList(region, agent, null);
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
            String nodeId = gdb.getNodeId(actionRegion, actionAgent,actionPlugin);
            Map<String,String> nodeParams = gdb.getNodeParams(nodeId);
            queryReturn = nodeParams.get("configparams");
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

    private String getGlobalNetResourceInfo() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        List<DiscoveryNode> dsList = null;
        try
        {
            dsList = new ArrayList<>();

            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> netInfoEdgeList = controllerEngine.getGDB().dba.getIsAssignedEdgeIds("netdiscovery_resource", "netdiscovery_inode");

            for(String edgeID : netInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);

                String network_map_json = gdb.stringUncompress(edgeParams.get("network_map"));


                Type listType = new TypeToken<ArrayList<DiscoveryNode>>(){}.getType();
                List<DiscoveryNode> tmpDsList = new Gson().fromJson(network_map_json, listType);
                dsList.addAll(tmpDsList);

                //MsgEvent me = msgEventFromJson(network_map_json);


                //MsgEvent
                //logger.error(network_map);
                /*
                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                for (String fspair : edgeParams.get("fs-map").split(",")) {
                    String[] fskey = fspair.split(":");
                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                }
                */
            }

            for(DiscoveryNode dn : dsList) {
                logger.info("src_ip {} src_port {} src_region {} src_agent {}",dn.src_ip, dn.src_port, dn.src_region, dn.src_agent);
                logger.info("dst_ip {} dst_port {} dst_region {} dst_agent {}",dn.dst_ip, dn.dst_port, dn.dst_region, dn.dst_agent);
            }


            /*
            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("globalresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            */

        } catch(Exception ex) {
            logger.error("getGlobalNetResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    //NMS change to package private to facilitate testing
    protected String getGlobalResourceInfo() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> sysInfoEdgeList = controllerEngine.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");

            for(String edgeID : sysInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);


                String sysInfoJson= gdb.stringUncompress(edgeParams.get("perf"));

                Type type = new TypeToken<Map<String, List<Map<String, String>>>>() {
                }.getType();

                Map<String,List<Map<String,String>>> myMap = gson.fromJson(sysInfoJson, type);

                cpu_core_count += Long.parseLong(myMap.get("cpu").get(0).get("cpu-logical-count"));

                memoryAvailable += Long.parseLong(myMap.get("mem").get(0).get("memory-available"));
                memoryTotal += Long.parseLong(myMap.get("mem").get(0).get("memory-total"));

                for(Map<String,String> fsMap : myMap.get("fs")) {
                    diskAvailable += Long.parseLong(fsMap.get("available-space"));
                    diskTotal += Long.parseLong(fsMap.get("total-space"));
                }

                /*
                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                for (String fspair : edgeParams.get("fs-map").split(",")) {
                    String[] fskey = fspair.split(":");
                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                }
                */

            }
            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("globalresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            //queryReturn = gson.toJson(queryMap);

} catch(Exception ex) {
            logger.error("getGlobalResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }
    protected String getRegionResourceInfo(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> sysInfoEdgeList = controllerEngine.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");

            for(String edgeID : sysInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);

                if(edgeParams.get("region").toLowerCase().equals(actionRegion.toLowerCase())) {

                    String sysInfoJson= gdb.stringUncompress(edgeParams.get("perf"));

                    Type type = new TypeToken<Map<String, List<Map<String, String>>>>() {
                    }.getType();

                    Map<String,List<Map<String,String>>> myMap = gson.fromJson(sysInfoJson, type);

                    cpu_core_count += Long.parseLong(myMap.get("cpu").get(0).get("cpu-logical-count"));

                    memoryAvailable += Long.parseLong(myMap.get("mem").get(0).get("memory-available"));
                    memoryTotal += Long.parseLong(myMap.get("mem").get(0).get("memory-total"));

                    for(Map<String,String> fsMap : myMap.get("fs")) {
                        diskAvailable += Long.parseLong(fsMap.get("available-space"));
                        diskTotal += Long.parseLong(fsMap.get("total-space"));
                    }
                    /*
                    cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                    memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                    memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                    for (String fspair : edgeParams.get("fs-map").split(",")) {
                        String[] fskey = fspair.split(":");
                        diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                        diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                    }
                    */
                }
            }
            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("regionresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            //queryReturn = gson.toJson(queryMap);

        } catch(Exception ex) {
            logger.error("getRegionResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }
    protected String getAgentResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> sysInfoEdgeList = controllerEngine.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");

            for(String edgeID : sysInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);

                if((edgeParams.get("region").toLowerCase().equals(actionRegion.toLowerCase())) && (edgeParams.get("agent").toLowerCase().equals(actionAgent.toLowerCase()))) {

                    Map<String,String> resourceTotal = new HashMap<>();

                    /*
                    Map<String,String> resourceTotal = new HashMap<>(edgeParams);
                    resourceTotal.remove("dst_region");
                    resourceTotal.remove("src_plugin");
                    resourceTotal.remove("src_region");
                    resourceTotal.remove("src_agent");
                    resourceTotal.remove("agentcontroller");
                    resourceTotal.remove("region");
                    resourceTotal.remove("agent");
                    resourceTotal.remove("inode_id");
                    resourceTotal.remove("resource_id");
                    resourceTotal.remove("routepath");
                    */
                    if(edgeParams.containsKey("perf")) {
                        resourceTotal.put("perf", gdb.stringUncompress(edgeParams.get("perf")));
                        regionArray.add(resourceTotal);
                    }

                }
            }

            queryMap.put("agentresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));
            //queryReturn = gson.toJson(queryMap);

        } catch(Exception ex) {
            logger.error("getAgentResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) {
        String returnString = null;

        try{

            List<Map<String,String>> metricList = new ArrayList<>();
            if((actionRegion != null) && (actionAgent != null) && (actionPluginId != null)) {
                List<String> edgeList = controllerEngine.getGDB().dba.getIsAttachedEdgeId(actionRegion,actionAgent,actionPluginId);
                if(edgeList != null) {
                    for(String edgeId : edgeList) {
                        Map<String, String> edgeParams = dba.getIsAssignedParams(edgeId);
                        if(edgeParams != null) {
                            //String resource_id = edgeParams.get("resource_id");
                            String inode_id = edgeParams.get("inode_id");

                            if(!inode_id.equals("sysinfo_inode") && !inode_id.equals("netdiscovery_inode")) {
                                if(edgeParams.containsKey("perf")) {
                                    Map<String,String> perfMap = new HashMap<>();
                                    perfMap.put("name",inode_id);
                                    perfMap.put("metrics", gdb.stringUncompress(edgeParams.get("perf")));
                                    metricList.add(perfMap);
                                }
                            }
                        }
                    }
                }
            }
            returnString = gson.toJson(metricList);
        } catch(Exception ex) {
            logger.error("getIsAttachedMetrics() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }
        return returnString;
    }

    public String getNetResourceInfo() {
        String queryReturn = null;
        try
        {
                queryReturn = getGlobalNetResourceInfo();

        } catch(Exception ex) {
            logger.error("getNetResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;
        try
        {
            if((actionRegion != null) && (actionAgent != null)) {
                queryReturn = getAgentResourceInfo(actionRegion,actionAgent);
            } else if (actionRegion != null) {
                queryReturn = getRegionResourceInfo(actionRegion);
            } else {
                queryReturn = getGlobalResourceInfo();
            }

        } catch(Exception ex) {
            logger.error("getResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getGPipeline(String actionPipelineId) {
        String queryReturn = null;
        try
        {
            gPayload gpay = controllerEngine.getGDB().dba.getPipelineObj(actionPipelineId);
            if(gpay != null) {
                Map<String, String> pipeStatMap = controllerEngine.getGDB().dba.getPipelineStatus(actionPipelineId);
                gpay.status_code = pipeStatMap.get("status_code");
                gpay.status_desc = pipeStatMap.get("status_desc");

                int nodeSize = gpay.nodes.size();
                for (int i = 0; i < nodeSize; i++) {
                    gpay.nodes.get(i).params.clear();
                    //logger.error("vnode=" + gpay.nodes.get(i).node_id);
                    //String inodeid = agentcontroller.getGDB().dba.getINodefromVNode(gpay.nodes.get(i).node_id);
                    //logger.error("inode=" + inodeid);
                    //String status_code = agentcontroller.getGDB().dba.getINodeParam(inodeid,"status_code");
                    //String status_desc = agentcontroller.getGDB().dba.getINodeParam(inodeid,"status_desc");
                    //gpay.nodes.get(i).params.put("inode_id",inodeid);
                    //logger.error("iNODE: " + gpay.nodes.get(i).node_id);
                    String inodeNodeid = dba.getINodeNodeId(gpay.nodes.get(i).node_id);
                    Map<String,String> inodeParams = controllerEngine.getGDB().gdb.getNodeParamsNoTx(inodeNodeid);

                    String status_code = inodeParams.get("status_code");
                    String status_desc = inodeParams.get("status_desc");
                    String params = inodeParams.get("params");
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

                /*
                int edgeSize = gpay.edges.size();
                for (int i = 0; i < nodeSize; i++) {
                    dba.getIsAssignedParams("df");
                }
                */
                //gdb.getEdgeParamsNoTx()

                //String returnGetGpipeline = gson.toJson(gpay);
                //String returnGetGpipeline = agentcontroller.getGDB().dba.getPipeline(actionPipelineId);
                //queryReturn = DatatypeConverter.printBase64Binary(agentcontroller.getGDB().gdb.stringCompress(returnGetGpipeline));
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
            String returnGetGpipeline = controllerEngine.getGDB().dba.getPipeline(actionPipelineId);
            if(returnGetGpipeline != null) {
                queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress(returnGetGpipeline));
            }

        } catch(Exception ex) {
            logger.error("getGPipelineExport() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return queryReturn;

    }

    public String getIsAssignedInfo(String resourceid,String inodeid, boolean isResourceMetric) {

        String queryReturn = null;
        try
        {
            String isAssignedEdgeId = controllerEngine.getGDB().dba.getResourceEdgeId(resourceid, inodeid);

            if(isResourceMetric) {
                   String resourceMetric = controllerEngine.getGDB().dba.getIsAssignedParam(isAssignedEdgeId,"resource_metric");
                   if(resourceMetric != null) {
                       queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress(resourceMetric));
                   }
            } else {
                Map<String, String> queryMap = controllerEngine.getGDB().dba.getIsAssignedParams(isAssignedEdgeId);
                queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress((gson.toJson(queryMap))));
            }

        } catch(Exception ex) {
            logger.error("getIsAssignedInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

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
                    pipelines = controllerEngine.getGDB().dba.getPipelineIdList();
                }

                for(String pipelineId :pipelines) {
                    Map<String,String> pipelineMap = controllerEngine.getGDB().dba.getPipelineStatus(pipelineId);
                    if(!pipelineMap.isEmpty()) {
                        pipelineArray.add(pipelineMap);
                    }
                }
                queryMap.put("pipelines",pipelineArray);

                queryReturn = DatatypeConverter.printBase64Binary(controllerEngine.getGDB().gdb.stringCompress((gson.toJson(queryMap))));

            } catch(Exception ex) {
            logger.error("getPipelineInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public Map<String,String> getResourceTotal2() {
        Map<String,String> resourceTotal = null;
        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;
        long region_count = 0;
        long agent_count = 0;
        long plugin_count = 0;

        try
        {
            /*
            logger.info("CODY START");
            List<String> sysInfoEdgeList = gdb.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
            for(String edgeID : sysInfoEdgeList) {
                logger.info("ID = " + edgeID);
                //logger.info(gdb.getIsAssignedParam(String edge_id,String param_name)
                String region = gdb.getIsAssignedParam(edgeID,"region");
                String agent = gdb.getIsAssignedParam(edgeID,"agent");
                String pluginID = gdb.getIsAssignedParam(edgeID,"agentcontroller");

                Map<String,String> edgeParams = gdb.getIsAssignedParams(edgeID);
                for (Map.Entry<String, String> entry : edgeParams.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    logger.info("key=" + key + " value=" + value);
                }

                logger.info(region + " " + agent + " " + pluginID);
            }

            logger.info("CODY END");
            */

            //public List<String> getIsAssignedEdgeIds(String resource_id, String inode_id)

            resourceTotal = new HashMap<>();
            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    region_count++;
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
                    if (agentList != null) {
                        for (String agent : agentList) {
                            agent_count++;
                            logger.trace("Agent : " + agent);

                            List<String> pluginList = gdb.getNodeList(region, agent, null);
                            if (pluginList != null) {

                                boolean isRecorded = false;
                                for (String pluginId : pluginList) {
                                    logger.trace("Plugin : " + plugin);
                                    plugin_count++;
                                    if (!isRecorded) {
                                        String pluginConfigparams = gdb.getNodeParam(region, agent, pluginId, "configparams");
                                        logger.trace("configParams : " + pluginConfigparams);
                                        if (pluginConfigparams != null) {
                                            Map<String, String> pMap = paramStringToMap(pluginConfigparams);
                                            if (pMap.get("pluginname").equals("cresco-sysinfo-agentcontroller")) {

                                                String isAssignedEdgeId = dba.getResourceEdgeId("sysinfo_resource", "sysinfo_inode",region,agent,pluginId);
                                                Map<String,String> edgeParams = dba.getIsAssignedParams(isAssignedEdgeId);
                                                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                                                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                                                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                                                for(String fspair : edgeParams.get("fs-map").split(",")) {
                                                    String[] fskey = fspair.split(":");
                                                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                                                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                                                }
                                                /*
                                                System.out.println("region=" + region + " agent=" + agent);
                                                String agent_path = region + "_" + agent;
                                                String agentConfigparams = gdb.getNodeParam(region, agent, null, "configparams");
                                                Map<String, String> aMap = paramStringToMap(agentConfigparams);
                                                //String resourceKey = aMap.get("platform") + "_" + aMap.get("environment") + "_" + aMap.get("location");

                                                for (Map.Entry<String, String> entry : pMap.entrySet()) {
                                                    String key = entry.getKey();
                                                    String value = entry.getValue();
                                                    System.out.println("\t" + key + ":" + value);
                                                }
                                                isRecorded = true;
                                                */
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            logger.trace("Regions :" + region_count);
            logger.trace("Agents :" + agent_count);
            logger.trace("Plugins : " + plugin_count);
            logger.trace("Total CPU core count : " + cpu_core_count);
            logger.trace("Total Memory Available : " + memoryAvailable);
            logger.trace("Total Memory Total : " + memoryTotal);
            logger.trace("Total Disk Available : " + diskAvailable);
            logger.trace("Total Disk Total : " + diskTotal);
            resourceTotal.put("regions",String.valueOf(region_count));
            resourceTotal.put("agents",String.valueOf(agent_count));
            resourceTotal.put("plugins",String.valueOf(plugin_count));
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));

        }
        catch(Exception ex)
        {
            logger.error("getResourceTotal() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return resourceTotal;
    }

    public Map<String,NodeStatusType> getEdgeHealthStatus(String region, String agent, String plugin) {

        Map<String,NodeStatusType> nodeStatusMap = null;
        try {
            nodeStatusMap = new HashMap<>();
            //List<String> queryList = gdb.getNodeIds(region,agent,agentcontroller,false);
            List<String> queryList = gdb.getEdgeHealthIds(region,agent,plugin,false);
            //logger.info("getEdgeHealthStatus : Count : " + queryList.size());

            for(String nodeId : queryList) {
                Map<String,String> params = gdb.getEdgeParamsNoTx(nodeId);
                long watchdog_rate = 5000L;

                if(params.containsKey("watchdogtimer")) {
                    watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
                }

                boolean isPending = false;

                if(params.containsKey("enable_pending")) {
                    if(Boolean.parseBoolean(params.get("enable_pending"))) {
                        isPending = true;
                    }
                }

                if(isPending) {
                    nodeStatusMap.put(nodeId, NodeStatusType.PENDING);
                }
                else if((params.containsKey("watchdog_ts"))  && (params.containsKey("is_active"))) {
                    long watchdog_ts = Long.parseLong(params.get("watchdog_ts"));
                    //long watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
                    boolean isActive = Boolean.parseBoolean(params.get("is_active"));
                    if(isActive) {
                        long watchdog_diff = System.currentTimeMillis() - watchdog_ts;
                        if (watchdog_diff > (watchdog_rate * 3)) {
                            //node is stale
                            nodeStatusMap.put(nodeId, NodeStatusType.STALE);
                            logger.error(nodeId + " is stale");
                        } else {
                            nodeStatusMap.put(nodeId, NodeStatusType.ACTIVE);
                        }
                    }
                    else {
                        logger.error(nodeId + " is lost");
                        nodeStatusMap.put(nodeId, NodeStatusType.LOST);

                    }
                }
                else {
                    //Plugins will trigger this problem, need to fix Cresco Library
                    logger.error(nodeId + " could not find watchdog_ts or watchdog_rate");
                    nodeStatusMap.put(nodeId,NodeStatusType.ERROR);
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return nodeStatusMap;
    }

    public Map<String,NodeStatusType> getNodeStatus(String region, String agent, String plugin) {

        Map<String,NodeStatusType> nodeStatusMap = null;
        try {
            nodeStatusMap = new HashMap<>();
            List<String> queryList = gdb.getNodeIds(region,agent,plugin,false);

            //region queries will not include plugins, make second request if that is what is needed
            /*
            if((region != null) && (agent == null)) {

                for(String agentNodeId : queryList) {
                    agent = gdb.getNodeParam(agentNodeId,"agent");
                    nodeStatusMap.putAll(getNodeStatus(region,agent,null));
                }

            }
            */

            for(String nodeId : queryList) {
                Map<String,String> params = gdb.getNodeParamsNoTx(nodeId);
                long watchdog_rate = 5000L;

                if(params.containsKey("watchdogtimer")) {
                    watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
                }

                boolean isPending = false;

                if(params.containsKey("enable_pending")) {
                    if(Boolean.parseBoolean(params.get("enable_pending"))) {
                        isPending = true;
                    }
                }

                if(isPending) {
                    nodeStatusMap.put(nodeId, NodeStatusType.PENDING);
                }
                else if((params.containsKey("watchdog_ts"))  && (params.containsKey("is_active"))) {
                    long watchdog_ts = Long.parseLong(params.get("watchdog_ts"));
                    //long watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
                    boolean isActive = Boolean.parseBoolean(params.get("is_active"));
                    if(isActive) {
                        long watchdog_diff = System.currentTimeMillis() - watchdog_ts;
                        if (watchdog_diff > (watchdog_rate * 3)) {
                            //node is stale
                            nodeStatusMap.put(nodeId, NodeStatusType.STALE);
                            logger.error(nodeId + " is stale");
                        } else {
                            nodeStatusMap.put(nodeId, NodeStatusType.ACTIVE);
                        }
                    }
                    else {
                        logger.error(nodeId + " is lost");
                        nodeStatusMap.put(nodeId, NodeStatusType.LOST);

                    }
                }
                else {
                    //Plugins will trigger this problem, need to fix Cresco Library
                    logger.error(nodeId + " could not find watchdog_ts or watchdog_rate");
                    nodeStatusMap.put(nodeId,NodeStatusType.ERROR);
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return nodeStatusMap;
    }

    public Boolean addNode(MsgEvent de) {
        Boolean wasAdded = false;

        try {

            /*
            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String agentcontroller = de.getParam("src_plugin");
            */


            String region = de.getParam("region_name");
            String agent = de.getParam("agent_name");
            String plugin = de.getParam("plugin_id");




            de.setParam("is_active",Boolean.TRUE.toString());
            de.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));


            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId == null) {
                nodeId = gdb.addNode(region, agent,plugin);
            }

            gdb.setNodeParams(region,agent,plugin, de.getParams());

                logger.debug("Adding Node: " + de.getParams().toString());

                if((region != null) && (agent != null) && (plugin == null)) {
                    //logger.info("is Agent: Process Plugins");
                    //add agentcontroller configs for agent
                    if (de.getParam("pluginconfigs") != null) {
                        List<Map<String, String>> configMapList = new Gson().fromJson(de.getCompressedParam("pluginconfigs"),
                                new TypeToken<List<Map<String, String>>>() {
                                }.getType());

                        for (Map<String, String> configMap : configMapList) {
                            String pluginId = configMap.get("pluginid");
                            gdb.addNode(region, agent, pluginId);
                            gdb.setNodeParams(region, agent, pluginId, configMap);
                        }
                    }
            }

            wasAdded = true;

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : addNode ERROR : " + ex.toString());
        }
        return wasAdded;
    }

    public Boolean watchDogUpdate(MsgEvent de) {
        Boolean wasUpdated = false;

        try {
            /*
            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String pluginId = de.getParam("src_plugin");
            */
            String region = de.getParam("region_name");
            String agent = de.getParam("agent_name");
            String pluginId = de.getParam("plugin_id");

            String nodeId = gdb.getNodeId(region,agent,pluginId);

            logger.trace("watchdog() region=" + region + " agent=" + agent + " agentcontroller=" + pluginId);

            if(nodeId != null) {
                //update watchdog_ts for local db

                String interval = de.getParam("watchdogtimer");
                if(interval == null) {
                    interval = "5000";
                }
                //setNodeParamsNoTx

                Map<String,String> updateMap = new HashMap<>();
                updateMap.put("watchdogtimer", interval);
                updateMap.put("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                updateMap.put("is_active", Boolean.TRUE.toString());
                updateMap.put("enable_pending", Boolean.FALSE.toString());


                //We no longer use Nodes to store health status, see edgehealth
                //gdb.setNodeParamsNoTx(region, agent, pluginId, updateMap);


                String edgeId = gdb.getEdgeHealthId(region, agent, pluginId);

                if(edgeId != null) {

                    //logger.error("UPDATE EDGE : " + edgeId + " region:" + region + " agent:" + agent + " agentcontroller:" + pluginId);
                    //logger.error(gdb.getEdgeParamsNoTx(edgeId).toString());
                    gdb.setEdgeParamsNoTx(edgeId,updateMap);
                } else {
                    logger.error("Missing Edge Id");
                }


                if((region != null) && (agent != null) && (pluginId == null)) {

                    //add agentcontroller configs for agent
                    if (de.getParam("pluginconfigs") != null) {

                        List<Map<String, String>> configMapList = new Gson().fromJson(de.getCompressedParam("pluginconfigs"),
                                new TypeToken<List<Map<String, String>>>() {
                                }.getType());

                        //build a list of plugins on record for an agent
                        List<String> pluginRemoveList = gdb.getNodeList(region,agent,null);

                        for (Map<String, String> configMap : configMapList) {
                            String subpluginId = configMap.get("pluginid");

                            //remove agentcontroller from remove list of new config exist
                            if(pluginRemoveList.contains(subpluginId)) {
                                pluginRemoveList.remove(subpluginId);
                            }

                            if(gdb.getNodeId(region,agent,subpluginId) == null) {
                                gdb.addNode(region, agent, subpluginId);
                            }
                            gdb.setNodeParams(region, agent, subpluginId, configMap);

                            /*
                            for (Map.Entry<String, String> entry : configMap.entrySet())
                            {
                                System.out.println(entry.getKey() + "/" + entry.getValue());
                                //gdb.addNode(region, agent,agentcontroller);
                                //gdb.setNodeParams(region,agent,agentcontroller, de.getParams());
                            }
                            */

                        }

                        //remove nodes on the pluginRemoveList, they are no longer on the agent
                        for(String removePlugin : pluginRemoveList) {
                            if(!gdb.removeNode(region,agent, removePlugin)) {
                                logger.error("watchDogUpdate Error : Could not remove agentcontroller region:" + region + " agent:" + agent + " agentcontroller:" + removePlugin);
                            }
                        }
                    }
                }

                //check for config update



                //gdb.setNodeParam(region, agent, pluginId, "watchdogtimer", interval);
                //gdb.setNodeParam(region,agent,pluginId, "watchdog_ts", String.valueOf(System.currentTimeMillis()));
                //gdb.setNodeParam(region,agent,pluginId, "is_active", Boolean.TRUE.toString());

                wasUpdated = true;
            }
            else {
                logger.error("watchdog() nodeID does not exist for region:" + region + " agent:" + agent + " pluginId:" + pluginId);
                logger.error(de.getMsgType().toString() + " [" + de.getParams().toString() + "]");
                /*
                if(gdb.getNodeId(region,agent,null) != null) {

                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG,agentcontroller.getRegion(),null,null,"enabled");
                    le.setMsgBody("Get Plugin Inventory From Agent");
                    le.setParam("src_region", agentcontroller.getRegion());
                    le.setParam("dst_region", region);
                    le.setParam("dst_agent", agent);
                    le.setParam("configtype","plugininventory");
                    le.setParam("agentcontroller",pluginId);
                    this.agentcontroller.msgIn(le);
                }
                */

            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : watchDogUpdate ERROR : " + ex.toString());
        }
        return wasUpdated;
    }

    public Boolean removeNode(MsgEvent de) {
        Boolean wasRemoved = false;

        try {

            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String plugin = de.getParam("src_plugin");

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.debug("Removing Node: " + de.getParams().toString());
                gdb.removeNode(region, agent,plugin);
                wasRemoved = true;
            }
            else {
                logger.error("removeNode() MsgEvent region: " + region + " agent:" + agent + " agentcontroller:" + plugin + " does not exist!");
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : removeNode ERROR : " + ex.toString());
        }
        return wasRemoved;
    }

    public Boolean removeNode(String region, String agent, String plugin) {
        Boolean wasRemoved = false;

        try {

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.debug("Removing Node: " + "region: " + region + " agent:" + agent + " agentcontroller:" + plugin);
                gdb.removeNode(region, agent,plugin);
                wasRemoved = true;
            }
            else {
                logger.error("RemoveNode() region: " + region + " agent:" + agent + " agentcontroller:" + plugin + " does not exist!");
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : removeNode ERROR : " + ex.toString());
        }
        return wasRemoved;
    }

}
