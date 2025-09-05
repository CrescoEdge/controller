package io.cresco.agent.db;

import io.cresco.library.app.gPayload;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;

import java.util.List;
import java.util.Map;

public interface DBInterface {

    void shutdown();
    Map<String,String> getResourceTotal();
    Map<String,List<Map<String,String>>> getRegionList();
    Map<String,List<Map<String,String>>> getAgentList(String actionRegion);
    String getPluginListRepo();
    Map<String, List<pNode>> getPluginListRepoSet();
    List<String> getPluginListRepoInventory();
    String getPluginListByType(String actionPluginTypeId, String actionPluginTypeValue);
    String getPluginList(String actionRegion, String actionAgent);
    String getPluginInfo(String actionRegion, String actionAgent, String actionPlugin);
    String getNetResourceInfo();
    String getGPipeline(String actionPipelineId);
    String getGPipelineExport(String actionPipelineId);
    String getIsAssignedInfo(String resourceid, String inodeid, boolean isResourceMetric);
    String getPipelineInfo(String pipeline_action);
    Map<String, NodeStatusType> getEdgeHealthStatus(String region, String agent, String plugin);
    boolean nodeUpdate(MsgEvent de);
    void addPNode(String agent, String plugin, int status_code, String status_desc, int watchdog_period, long watchdog_ts, String pluginname, String jarfile, String version, String md5, String configparams, int persistence_code);
    //public boolean watchDogUpdate(MsgEvent de);
    boolean removeNode(MsgEvent de);
    boolean removeNode(String region, String agent, String plugin);
    Map<String,String> getNodeParams(String node_id);
    String getINodeParam(String inode_id, String param);
    Map<String,String> getpNodeINode(String iNode_id);
    List<String> getANodeFromIndex(String indexName, String indexValue);
    boolean setINodeParam(String inode_id, String paramKey, String paramValue);
    String addEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className, Map<String, String> paramMap);
    String getPipeline(String pipelineId);
    gPayload createPipelineRecord(String tenant_id, String gPayload);
    boolean updateKPI(String region, String agent, String pluginId, String resource_id, String inode_id, Map<String, String> params);
    Map<String,String> getDBExport(boolean regions, boolean agents, boolean plugins, String region_id, String agent_id, String plugin_id);
    gPayload createPipelineNodes(gPayload gpay);
    boolean setPipelineStatus(String pipelineId, String status_code, String status_desc);
    gPayload getPipelineObj(String pipelineId);
    int getPipelineStatusCode(String pipelineId);
    int getINodeStatus(String INodeId);
    boolean removePipeline(String pipelineId);
    List<String> getNodeList(String region, String agent);
    boolean setDBImport(String exportData);
    Map<String, NodeStatusType> getNodeStatus(String region, String agent, String plugin);
    Map<String,String> getEdgeParamsNoTx(String edgeId);
    String getINodeParams(String iNode_id);
    boolean setEdgeParam(String edgeId, String paramKey, String paramValue);
    String addINodeResource(String resource_id, String inode_id);
    String getNodeId(String region, String agent, String plugin);
    String addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin);
    String getResourceEdgeId(String resource_id, String inode_id);
    String getIsAssignedParam(String edge_id, String param_name);
    Map<String,String> paramStringToMap(String param);

}