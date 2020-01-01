package io.cresco.agent.controller.globalscheduler;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.Map;


public class PollRemovePlugin implements Runnable { 

	private String resource_id  = null;
	private String inode_id = null;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;

	public PollRemovePlugin(ControllerEngine controllerEngine, String resource_id, String inode_id)
	{
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PollRemovePlugin.class.getName(),CLogger.Level.Info);
		this.resource_id = resource_id;
		this.inode_id = inode_id;
	}

	public void run() {
        try {

            //if(controllerEngine.getGDB().getINodeStatus(inode_id) > 8) {

                Map<String,String> inodeMap = controllerEngine.getGDB().getInodeMap(inode_id);
                String region = inodeMap.get("region_id");
                String agent = inodeMap.get("agent_id");
                String pluginId = inodeMap.get("plugin_id");

                if((region != null) && (agent != null) && (pluginId != null)) {

                    MsgEvent me = removePlugin(region, agent, pluginId);
                    //double default RPC timeout
                    //MsgEvent re = plugin.sendRPC(me,6000);
                    MsgEvent re = plugin.sendRPC(me);

                    if (re != null) {
                        int statusCode = Integer.parseInt(re.getParam("status_code"));
                        if (statusCode == 7) {
                            logger.debug("REMOVING inode: " + inode_id + " RPC:" + re.getParams().toString());
                            boolean isRemovedPlugin = controllerEngine.getGDB().removeNode(region,agent,pluginId);

                            if(isRemovedPlugin) {
                                logger.debug("removed r: " + region + " a:" + agent + " p:" + pluginId + " for inode:" + inode_id);
                                controllerEngine.getGDB().setINodeStatusCode(inode_id,8,"iNode Disabled.");
                            } else {
                                logger.error("pollRemovePlugin : unable to verify iNode deactivation! " + inode_id);
                                controllerEngine.getGDB().setINodeStatusCode(inode_id,90,"iNode unable to verify iNode deactivation!");
                            }
                        }
                    } else {
                        controllerEngine.getGDB().setINodeStatusCode(inode_id,91,"iNode unable to verify iNode deactivation!");
                        logger.error("pollRemovePlugin : unable to verify iNode deactivation! " + inode_id);
                    }
                } else {
                    controllerEngine.getGDB().setINodeStatusCode(inode_id,92,"iNode with no Plugin Assignment!");
                    logger.error("iNode with no Plugin Assignment for resource_id: " + resource_id + " inode_id:" + inode_id);
                }

            //}
        }
	   catch(Exception ex)
	   {
            logger.error("PollRemovePlugin : " + ex.getMessage());
       }
    }  

	public MsgEvent removePlugin(String region, String agent, String pluginId)
	{
	    MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG,region,agent);

        me.setParam("action", "pluginremove");
		me.setParam("pluginid", pluginId);
		return me;	
	}
	
}