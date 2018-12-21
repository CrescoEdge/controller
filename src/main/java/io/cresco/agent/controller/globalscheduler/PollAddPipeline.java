package io.cresco.agent.controller.globalscheduler;


import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.app.gNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;

public class PollAddPipeline implements Runnable {

    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
    List<gNode> pipelineNodes;
    String pipelineId;
    private Gson gson;

	public PollAddPipeline(ControllerEngine controllerEngine, List<gNode> assignedNodes, String pipelineId)
	{
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PollAddPipeline.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(PollAddPipeline.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
		//this.agentcontroller = agentcontroller;
		//this.assignedNodes = assignedNodes;
		this.pipelineId = pipelineId;
        pipelineNodes = new ArrayList<>(assignedNodes);
        gson = new Gson();
	}
	 public void run() {
	        try 
	        {
                logger.debug("PipelineId " + pipelineId + " Pipeline Starting");
                for(gNode gnode : pipelineNodes) {

                    logger.debug("gnode_id : " + gnode.node_id + " params : " + gnode.params);
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
                    me.setParam("globalcmd", "addplugin");
                    me.setParam("inode_id", gnode.node_id);
                    me.setParam("resource_id", pipelineId);
                    me.setParam("location_region",gnode.params.get("location_region"));
                    me.setParam("location_agent",gnode.params.get("location_agent"));
                    gnode.params.remove("location_region");
                    gnode.params.remove("location_agent");

                    if(gnode.params.containsKey("edges")) {
                        //remove edges from config if they exist
                        me.setCompressedParam("edges", gnode.params.get("edges"));
                        gnode.params.remove("edges");
                    }

                    /*
                    StringBuilder configparms = new StringBuilder();
                    for (Map.Entry<String, String> entry : gnode.params.entrySet())
                    {
                        configparms.append(entry.getKey() + "=" + entry.getValue() + ",");
                        //System.out.println(entry.getKey() + "/" + entry.getValue());
                    }
                    if(configparms.length() > 0) {
                        configparms.deleteCharAt(configparms.length() -1);
                    }
                    */

                    me.setCompressedParam("configparams",gson.toJson(gnode.params));
                    //me.setParam("configparams", configparms.toString());
                    logger.debug("Message [" + me.getParams().toString() + "]");
                    controllerEngine.getGDB().setINodeStatusCode(gnode.node_id, 4,"iNode resources scheduled.");

                    //controllerEngine.getGDB().set setINodeParam(gnode.node_id,"status_code","4");
                    //controllerEngine.getGDB().setINodeParam(gnode.node_id,"status_desc","iNode resources scheduled.");

                    controllerEngine.getResourceScheduleQueue().add(me);

                }

                List<gNode> errorList = new ArrayList<>();
                boolean isScheduling = true;
                while(isScheduling)
	        	{
                    List<gNode> checkList = new ArrayList<>(pipelineNodes);

                    if(checkList.isEmpty()) {
	        	        isScheduling = false;
                    }


                    for(gNode gnode : checkList) {
                        int statusCode = controllerEngine.getGDB().getINodeStatus(gnode.node_id);
                        //int statusCode = Integer.parseInt(controllerEngine.getGDB().getINodeParam(gnode.node_id, "status_code"));
                        logger.debug("PipelineId " + pipelineId + " inode: " + gnode.node_id + " Status " + statusCode);

                        if (statusCode != 4) {
                            //logger.debug("PollAddPipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + controllerEngine.getGDB().dba.getINodeParam(gnode.node_id, "status_code"));
                            pipelineNodes.remove(gnode);
                            if(statusCode != 10) {
                                //String statusDesc = controllerEngine.getGDB().getINodeParam(gnode.node_id, "status_desc");
                                //logger.error("PipelineId " + pipelineId + " Status Code " + statusCode + " Status Desc " + statusDesc);
                                errorList.add(gnode);
                            }
                        }
                    }

                    Thread.sleep(1000);

	        	}
                if(errorList.isEmpty()) {
                    controllerEngine.getGDB().setPipelineStatus(pipelineId, "10", "Pipeline Active");
                    logger.debug("PipelineId " + pipelineId + " Pipeline Active");
                } else {
                    controllerEngine.getGDB().setPipelineStatus(pipelineId, "50", "Pipeline Failed Resource Assignment");
                    logger.error("PipelineId " + pipelineId + " Failed Resource Assignment");
                }

	        }
		   catch(Exception ex)
		   {

               logger.error("PollAddPipeline : " + ex.getMessage());
	            //logger.error("PollAddPipeline : " + agentcontroller.getStringFromError(ex));
	       }
	    }


}
