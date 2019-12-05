package io.cresco.agent.controller.globalscheduler;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PollAddPlugin implements Runnable {

	private String resource_id  = null;
	private String inode_id = null;
	private String region = null;
	private String agent = null;
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private MsgEvent me;

	public PollAddPlugin(ControllerEngine controllerEngine, String resource_id, String inode_id, String region, String agent, MsgEvent me)
	{
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(PollAddPlugin.class.getName(),CLogger.Level.Info);

		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.region = region;
		this.agent = agent;
		this.me = me;

    }
	 public void run() {
         try
	        {
                int count = 0;
	        	String edge_id = null;

	        	//double default RPC timeout
	        	//MsgEvent re = plugin.sendRPC(me,6000);
				MsgEvent re = plugin.sendRPC(me);

				if(re != null) {
					//info returned from agent
					String pluginId = re.getParam("pluginid");
					String status_code_plugin = re.getParam("status_code");
					String status_desc_plugin = re.getParam("status_desc");

					//link inode to
					//controllerEngine.getGDB().addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);


					if((status_code_plugin == null) || (status_desc_plugin == null)) {
						controllerEngine.getGDB().updateINodeAssignment(inode_id, 42,"iNode Missing Status Parameters." , region, agent, pluginId);
					} else {
						if (Integer.parseInt(status_code_plugin) == 10) {
							controllerEngine.getGDB().updateINodeAssignment(inode_id, 10,"iNode Active." , region, agent, pluginId);
						} else {
							controllerEngine.getGDB().updateINodeAssignment(inode_id, Integer.parseInt(status_code_plugin),status_desc_plugin, region, agent, pluginId);
						}
					}
				} else {
					logger.debug("pollAddPlugin : unable to verify iNode activation!  inode_id=" + inode_id);
					controllerEngine.getGDB().setINodeStatusCode(inode_id,40,"iNode Failed Scheduling.");
				}

	        }
		   catch(Exception ex)
		   {
               logger.debug("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!  inode_id=" + inode_id);
			   controllerEngine.getGDB().setINodeStatusCode(inode_id,41,"iNode Failed Scheduling Exception.");

               logger.error("PollAddPlugin: Error " + ex.getMessage());
               StringWriter errors = new StringWriter();
               ex.printStackTrace(new PrintWriter(errors));
               logger.error("PollAddPlugin: Trace " + errors.toString());

           }
	    }

}
