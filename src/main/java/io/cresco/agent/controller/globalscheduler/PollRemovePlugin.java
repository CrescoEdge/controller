package io.cresco.agent.controller.globalscheduler;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;


public class PollRemovePlugin implements Runnable { 

	private String resource_id  = null;
	private String inode_id = null;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private GlobalHealthWatcher ghw;
	private CLogger logger;

	public PollRemovePlugin(ControllerEngine controllerEngine, String resource_id, String inode_id)
	{
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PollRemovePlugin.class.getName(),CLogger.Level.Info);

        //logger = new CLogger(PollRemovePlugin.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);

		//this.agentcontroller = agentcontroller;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
	}

	public void run() {
        try {

            if(controllerEngine.getGDB().getINodeStatus(inode_id) > 8) {
            String edge_id = controllerEngine.getGDB().getResourceEdgeId(resource_id, inode_id);
            if (edge_id != null) {
                String pnode_node_id = controllerEngine.getGDB().getIsAssignedParam(edge_id, "out");
                if (pnode_node_id != null) {
                    pnode_node_id = pnode_node_id.substring(pnode_node_id.indexOf("[") + 1, pnode_node_id.indexOf("]"));

                    String region = controllerEngine.getGDB().getIsAssignedParam(edge_id, "region");
                    String agent = controllerEngine.getGDB().getIsAssignedParam(edge_id, "agent");
                    String pluginId = controllerEngine.getGDB().getIsAssignedParam(edge_id, "agentcontroller");

                    logger.debug("starting to remove r: " + region + " a:" + agent + " p:" + pluginId);

                    String pnode_node_id_match = controllerEngine.getGDB().getNodeId(region, agent, pluginId);

                    if (pnode_node_id.equals(pnode_node_id_match)) {
                        MsgEvent me = removePlugin(region, agent, pluginId);

                        MsgEvent re = plugin.sendRPC(me);

                        if(re != null) {
                            int statusCode = Integer.parseInt(re.getParam("status_code"));
                            if(statusCode == 7) {
                                boolean isRemovedPlugin = controllerEngine.getGDB().removeNode(region,agent,pluginId);
                            }
                        } else {
                            logger.error("Return remove = null");
                        }


                        int count = 0;
                        boolean isRemoved = false;
                        while ((!isRemoved) && (count < 60)) {
                            if (controllerEngine.getGDB().getNodeId(region, agent, pluginId) == null) {
                                isRemoved = true;
                            }
                            Thread.sleep(1000);
                            count++;
                        }
                        if (isRemoved) {
                            controllerEngine.getGDB().setINodeParam(inode_id, "status_code", "8");
                            controllerEngine.getGDB().setINodeParam(inode_id,"status_desc","iNode Disabled");
                            logger.debug("removed r: " + region + " a:" + agent + " p:" + pluginId + " for inode:" + inode_id);
                        } else {
                            controllerEngine.getGDB().setINodeParam(inode_id, "status_code", "90");
                            controllerEngine.getGDB().setINodeParam(inode_id,"status_desc","iNode unable to verify iNode deactivation!");
                            logger.debug("pollRemovePlugin : unable to verify iNode deactivation! " + controllerEngine.getGDB().getINodeStatus(inode_id));

                        }

                    } else {
                        String errorString = "pnode_node_id mismatch : pnode_node_id " + pnode_node_id + " != " + pnode_node_id_match;
                        logger.error(errorString);
                        controllerEngine.getGDB().setINodeParam(inode_id, "status_code", "91");
                        controllerEngine.getGDB().setINodeParam(inode_id,"status_desc",errorString);
                    }

                } else {
                    controllerEngine.getGDB().setINodeParam(inode_id, "status_code", "92");
                    logger.error("agentcontroller not found for resource_id: " + resource_id + " inode_id:" + inode_id + " setting inode status");
                }

                }
            }
        }
	   catch(Exception ex)
	   {
            logger.error("PollRemovePlugin : " + ex.getMessage());
       }
    }  

	public MsgEvent removePlugin(String region, String agent, String pluginId)
	{
	    MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG,region,agent);
	    /*
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"remove agentcontroller");
		me.setParam("src_region", plugin.getRegion());
		me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        */
        me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("action", "pluginremove");
		me.setParam("pluginid", pluginId);
		return me;	
	}
	
}