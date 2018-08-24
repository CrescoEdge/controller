package io.cresco.agent.controller.globalscheduler;


import io.cresco.agent.controller.app.gEdge;
import io.cresco.agent.controller.app.gNode;
import io.cresco.agent.controller.app.gPayload;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AppSchedulerEngine implements Runnable {

    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
    private GlobalHealthWatcher ghw;
    private ExecutorService addPipelineExecutor;

    public AppSchedulerEngine(ControllerEngine controllerEngine, GlobalHealthWatcher ghw) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AppSchedulerEngine.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(AppSchedulerEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Debug);
		//this.agentcontroller = agentcontroller;
		this.ghw = ghw;
        addPipelineExecutor = Executors.newFixedThreadPool(100);
	}

    public void run() {
        try
        {

            ghw.AppSchedulerActive = true;
            while (ghw.AppSchedulerActive)
            {
                try
                {
                    gPayload gpay = controllerEngine.getAppScheduleQueue().poll();


                    if(gpay != null) {

                        logger.debug("gPayload.added");

                        gPayload createdPipeline = controllerEngine.getGDB().dba.createPipelineNodes(gpay);

                        if(createdPipeline.status_code.equals("3")) {
                            logger.debug("Created Pipeline Records: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                            //start creating real objects

                            int pipelineStatus = schedulePipeline(gpay.pipeline_id);

                            switch (pipelineStatus) {


                                //all metrics
                                case 1: controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"40","Failed to schedule pipeline resources exception.");
                                    break;
                                case 4: //all is good!
                                    break;
                                        //controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline resources scheduled.");
                                        //moved into schedulePipeline to prevent race condition
                                case 60: controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"60","Failed to schedule pipeline node resources.");
                                    break;
                                case 61: controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"61","Failed to schedule pipeline edge resources.");
                                    break;

                                default:
                                    logger.error("Pipeline Scheduling Failed: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                                    break;
                            }

                        }
                        else
                        {
                            logger.error("Pipeline Creation Failed: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);

                        }

                    }
                    else
                    {
                        Thread.sleep(1000);
                    }
                }
                catch(Exception ex)
                {
                    logger.error("AppSchedulerEngine gPayloadQueue Error: " + ex.toString());
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    ex.printStackTrace(pw);
                    logger.error(sw.toString()); // stack trace as a string
                }
            }
        }
        catch(Exception ex)
        {
            logger.error("AppSchedulerEngine Error: " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); // stack trace as a string
        }
    }

    public int schedulePipeline(String pipeline_id) {
        int scheduleStatus = 1;
        try {
            gPayload gpay = controllerEngine.getGDB().dba.getPipelineObj(pipeline_id);
            logger.debug("checkPipeline started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

            List<String> badINodes = new ArrayList<String>();
            logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
            for (gNode node : gpay.nodes) {
                String iNode_id = node.node_id;

                logger.debug("Checking iNode_id:" + iNode_id);
                controllerEngine.getGDB().dba.addINodeResource(gpay.pipeline_id, iNode_id);

            }

            //Assign location to specific nodes
            Map<String, List<gNode>> schedulemaps = buildNodeMaps(gpay.nodes);
            //printScheduleStats(schedulemaps);

            if (schedulemaps.get("error").size() != 0) {
                logger.error("Error Node assignments = " + schedulemaps.get("error").size());
            }

            if (schedulemaps.get("unassigned").size() != 0) {
                logger.error("Unassigned Node assignments = " + schedulemaps.get("unassigned").size());
            }

            if (schedulemaps.get("noresource").size() != 0) {
                logger.error("Not Enough Resources to Schedule Node assignments = " + schedulemaps.get("noresource").size());
            }

            if ((schedulemaps.get("assigned").size() != 0) && (schedulemaps.get("unassigned").size() == 0) && (schedulemaps.get("error").size() == 0) && (schedulemaps.get("noresource").size() == 0)) {
                logger.debug("Node Scheduling is ready!");
                //nodes are scheduled now work on edges

                List<gNode> assignedNodeList = null;

                if(gpay.edges.size() > 0) {
                    assignedNodeList =  buildEdgeMaps(gpay.edges, schedulemaps.get("assigned"));
                } else {
                    assignedNodeList = schedulemaps.get("assigned");
                }

                if(assignedNodeList.size() == schedulemaps.get("assigned").size()) {
                    //set DB as scheduled
                    logger.debug("Submitting Resource Pipeline for Scheduling " + gpay.pipeline_id);
                    controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id, "4", "Pipeline resources scheduled.");
                    addPipelineExecutor.execute(new PollAddPipeline(controllerEngine, schedulemaps.get("assigned"), gpay.pipeline_id));
                    logger.debug("Submitted Resource Pipeline for Scheduling");
                    scheduleStatus = 4;
                } else {
                    scheduleStatus = 61;
                }
            } else {
                scheduleStatus = 60;
            }
        }
        catch (Exception ex) {
            logger.error("schedulePipeline Error " + ex.getMessage());
        }
        return scheduleStatus;
    }



    public boolean nodeExist(String region, String agent) {
        boolean nodeExist = false;
        try {
            String nodeId = controllerEngine.getGDB().gdb.getNodeId(region,agent,null);
            if(nodeId != null) {
                nodeExist = true;
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return nodeExist;
    }

    public boolean locationExist(String location) {
        boolean isLocation = false;
        //String getINodeId(String resource_id, String inode_id)
        List<String> aNodeLocations = controllerEngine.getGDB().gdb.getANodeFromIndex("location",location);
        if(aNodeLocations.size() > 0) {
            isLocation = true;
        }
        return isLocation;
    }

    public void printScheduleStats(Map<String,List<gNode>> schedulemaps) {
        logger.info("Assigned Nodes : " + schedulemaps.get("assigned").size());
        logger.info("Unassigned Nodes : " + schedulemaps.get("unassigned").size());
        logger.info("Noresource Nodes : " + schedulemaps.get("noresource").size());
        logger.info("Error Nodes : " + schedulemaps.get("error").size());
    }

    private List<gNode> buildEdgeMaps(List<gEdge> edges, List<gNode> nodes) {

        try {

            Map<String,gNode> nodeMap = new HashMap<>();
            for(gNode node : nodes) {
                nodeMap.put(node.node_id,node);
            }

            //verify predicates
            for (gEdge edge : edges) {

                logger.trace("edge_id=" + edge.edge_id + " node_from=" + edge.node_from + " node_to=" + edge.node_to);

                if ((nodeMap.containsKey(edge.node_to)) && (nodeMap.containsKey(edge.node_from))) {
                    //modify nodes

                } else {
                    if ((!nodeMap.containsKey(edge.node_to)) && (nodeMap.containsKey(edge.node_to))) {
                        logger.error("Error Edge assignments = " + edge.edge_id + " missing node_to = " + edge.node_to);
                    } else if ((nodeMap.containsKey(edge.node_to)) && (!nodeMap.containsKey(edge.node_to))) {
                        logger.error("Error Edge assignments = " + edge.edge_id + " missing node_from = " + edge.node_from);
                    } else {
                        logger.error("Error Edge assignments = " + edge.edge_id + " missing node_from = " + edge.node_from + " and missing node_to = " + edge.node_to);
                    }
                }
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodes;
    }

    private Map<String,List<gNode>> buildNodeMaps(List<gNode> nodes) {

        Map<String,List<gNode>> nodeResults = null;
        try {

            nodeResults = new HashMap<>();

            List<gNode> assignedNodes = new ArrayList<>();
            List<gNode> errorNodes = new ArrayList<>();
            List<gNode> unAssignedNodes = new ArrayList<>(nodes);

            //verify predicates
            for (gNode node : nodes) {
                logger.trace("node_id=" + node.node_id + " node_name=" + node.node_name + " type" + node.type + " params" + node.params.toString());
                if (node.params.containsKey("location_region") && node.params.containsKey("location_agent")) {
                    if (nodeExist(node.params.get("location_region"), node.params.get("location_agent"))) {
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    } else {
                        errorNodes.add(node);
                    }
                } else if(node.params.containsKey("location")) {
                    String nodeId = getLocationNodeId(node.params.get("location"));
                    if(nodeId != null) {
                        Map<String,String> nodeMap = controllerEngine.getGDB().gdb.getNodeParams(nodeId);
                        node.params.put("location_region",nodeMap.get("region"));
                        node.params.put("location_agent",nodeMap.get("agent"));
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    }
                }
            }
            nodeResults.put("assigned",assignedNodes);
            nodeResults.put("unassigned", unAssignedNodes);
            nodeResults.put("error",errorNodes);
            nodeResults.put("noresource", new ArrayList<gNode>());

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeResults;
    }

    private String getLocationNodeId(String location) {
        String locationNodeId = null;
        try {
            List<String> aNodeList = controllerEngine.getGDB().gdb.getANodeFromIndex("location", location);
            if(!aNodeList.isEmpty()) {
                locationNodeId = aNodeList.get(0);
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return locationNodeId;
    }

}
