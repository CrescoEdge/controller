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
		this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PollRemovePipeline.class.getName(),CLogger.Level.Info);
        this.pipelineId = pipelineId;
	}
	 public void run() {
	        try {

	            int pipelineStatus = controllerEngine.getGDB().getPipelineStatusCode(pipelineId);

                if((pipelineStatus >= 10) && (pipelineStatus < 19)) {

                    controllerEngine.getGDB().setPipelineStatus(pipelineId, "9", "Pipeline Scheduled for Removal");


					gpay = controllerEngine.getGDB().getPipelineObj(pipelineId);

                    if (pipelineId.equals(gpay.pipeline_id)) {

						pipelineNodes = new ArrayList<>(gpay.nodes);
						for (gNode gnode : pipelineNodes) {

						    int statusCode = controllerEngine.getGDB().getINodeStatus(gnode.node_id);
						    if((statusCode >= 10) && (statusCode < 19))  { //running somewhere

                                MsgEvent me = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.CONFIG);
                                me.setParam("globalcmd", "removeplugin");
                                me.setParam("inode_id", gnode.node_id);
                                me.setParam("resource_id", pipelineId);

						        controllerEngine.getGDB().setINodeStatusCode(gnode.node_id,9,"iNode Pipeline Scheduled for Removal");

                                controllerEngine.getResourceScheduleQueue().add(me);
                            }
                            else if(statusCode > 19) {
                                controllerEngine.getGDB().setINodeStatusCode(gnode.node_id,8,"iNode Disabled");
                            }
						}

						//start watch loop
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
                                        logger.debug("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id);
                                        pipelineNodes.remove(gnode);
                                    }
                                    if(statusCode > 19) {
                                        errorList.add(gnode);
                                        pipelineNodes.remove(gnode);
                                        logger.error("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id);

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
