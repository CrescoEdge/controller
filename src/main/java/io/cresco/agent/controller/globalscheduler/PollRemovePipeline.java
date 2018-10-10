package io.cresco.agent.controller.globalscheduler;



import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.app.gNode;
import io.cresco.library.app.gPayload;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;

public class PollRemovePipeline implements Runnable {

    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
    private List<gNode> pipelineNodes;
    private String pipelineId;
    private gPayload gpay;


	public PollRemovePipeline(ControllerEngine controllerEngine, String pipelineId)
	{
		//this.logger = new CLogger(PollRemovePipeline.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
		//this.agentcontroller = agentcontroller;
		//this.assignedNodes = assignedNodes;
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PollRemovePipeline.class.getName(),CLogger.Level.Info);


        this.pipelineId = pipelineId;
	}
	 public void run() {
	        try {

	            int pipelineStatus = controllerEngine.getGDB().getPipelineStatusCode(pipelineId);

                //logger.error("PIPELINE ID " + pipelineId + " SCHEDILER FOR REMOVAL!!! STATUS " + pipelineStatus);

                //if((pipelineStatus >= 10) && (pipelineStatus < 19)) {
                if((pipelineStatus >= 10) && (pipelineStatus < 19)) {

                    controllerEngine.getGDB().setPipelineStatus(pipelineId, "9", "Pipeline Scheduled for Removal");


					gpay = controllerEngine.getGDB().getPipelineObj(pipelineId);
                    //logger.error("pluginsid : " + pipelineId + " status_code " + controllerEngine.getGDB().dba.getPipelineStatus(pipelineId) + " pipleinId payload:" + gpay.pipeline_id);
                    if (pipelineId.equals(gpay.pipeline_id)) {

						pipelineNodes = new ArrayList<>(gpay.nodes);
						for (gNode gnode : pipelineNodes) {

						    int statusCode = controllerEngine.getGDB().getINodeStatus(gnode.node_id);
						    if((statusCode >= 10) && (statusCode < 19))  { //running somewhere

                                MsgEvent me = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                                me.setParam("globalcmd", "removeplugin");
                                me.setParam("inode_id", gnode.node_id);
                                me.setParam("resource_id", pipelineId);
						        /*
						        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
                                me.setParam("globalcmd", "removeplugin");
                                me.setParam("inode_id", gnode.node_id);
                                me.setParam("resource_id", pipelineId);
                                */

                                //ghw.resourceScheduleQueue.add(me);
                                controllerEngine.getGDB().setINodeParam(gnode.node_id,"status_code","9");
                                controllerEngine.getGDB().setINodeParam(gnode.node_id,"status_desc","iNode Pipeline Scheduled for Removal");

                                controllerEngine.getResourceScheduleQueue().add(me);
                            }
                            else if(statusCode > 19) {
                                controllerEngine.getGDB().setINodeParam(gnode.node_id,"status_code","8");
                                controllerEngine.getGDB().setINodeParam(gnode.node_id,"status_desc","iNode Disabled");
                            }
						}
                    //start watch loop
                        //logger.error("PollRemovePipeline : Start Listen loop");
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
                                if (statusCode != 9) {
                                    if(statusCode == 8) {
                                        logger.debug("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + controllerEngine.getGDB().getINodeParam(gnode.node_id, "status_code"));
                                        pipelineNodes.remove(gnode);
                                    }
                                    if(statusCode > 19) {
                                        errorList.add(gnode);
                                        pipelineNodes.remove(gnode);
                                        logger.error("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + controllerEngine.getGDB().getINodeParam(gnode.node_id, "status_code"));

                                    }
                                }
                            }

                            Thread.sleep(500);

                        }
                        //end watch loop
                        if(errorList.isEmpty()) {
                            controllerEngine.getGDB().removePipeline(pipelineId);
                            logger.debug("pipelineid " + pipelineId + " removed!");
                        } else {
                            controllerEngine.getGDB().setPipelineStatus(pipelineId, "80", "Pipeline Failed Removal");
                            logger.error("PipelineID: " + pipelineId + " Removal Failed!");
                        }
                    }
				}

	        }
		   catch(Exception ex)
		   {
             logger.error("PollAddPipeline : " + ex.getMessage());
               controllerEngine.getGDB().setPipelineStatus(pipelineId, "90", "Pipeline Failed Removal");
               logger.error("PipelineID: " + pipelineId + " Removal Schedule Failed!");
	       }
	    }


}
