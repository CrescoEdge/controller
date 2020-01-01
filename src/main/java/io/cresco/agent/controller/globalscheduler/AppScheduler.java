package io.cresco.agent.controller.globalscheduler;


import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.gNode;
import io.cresco.library.app.gPayload;
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


public class AppScheduler implements IncomingApp {

    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;

    private ExecutorService addPipelineExecutor;
    private Gson gson;

    public AppScheduler(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AppScheduler.class.getName(),CLogger.Level.Info);

        //this should be a configurable parameter
        //addPipelineExecutor = Executors.newFixedThreadPool(100);
        addPipelineExecutor = Executors.newCachedThreadPool();
        gson = new Gson();
	}

    @Override
    public void incomingMessage(gPayload gpay) {

        try
        {
            if(gpay != null) {

                logger.debug("gPayload.added");

                gPayload createdPipeline = controllerEngine.getGDB().createPipelineNodes(gpay);

                if(createdPipeline.status_code.equals("3")) {
                    logger.debug("Created Pipeline Records: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                    //start creating real objects

                    int pipelineStatus = schedulePipeline(gpay.pipeline_id);

                    switch (pipelineStatus) {

                        //all metrics
                        case 1: controllerEngine.getGDB().setPipelineStatus(gpay.pipeline_id,"40","Failed to schedule pipeline resources exception.");
                            break;
                        case 4: //all is good!
                            break;
                        //controllerEngine.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline resources scheduled.");
                        //moved into schedulePipeline to prevent race condition
                        case 60: controllerEngine.getGDB().setPipelineStatus(gpay.pipeline_id,"60","Failed to schedule pipeline node resources.");
                            break;
                        case 61: controllerEngine.getGDB().setPipelineStatus(gpay.pipeline_id,"61","Failed to schedule pipeline edge resources.");
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

    public int schedulePipeline(String pipeline_id) {
        int scheduleStatus = 1;
        try {
            gPayload gpay = controllerEngine.getGDB().getPipelineObj(pipeline_id);
            logger.debug("checkPipeline started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

            List<String> badINodes = new ArrayList<String>();

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

                if(assignedNodeList != null) {

                    if (assignedNodeList.size() == schedulemaps.get("assigned").size()) {
                        //set DB as scheduled
                        logger.debug("Submitting Resource Pipeline for Scheduling " + gpay.pipeline_id);
                        controllerEngine.getGDB().setPipelineStatus(gpay.pipeline_id, "4", "Pipeline resources scheduled.");
                        //addPipelineExecutor.execute(new PollAddPipeline(controllerEngine, schedulemaps.get("assigned"), gpay.pipeline_id));
                        addPipelineExecutor.execute(new PollAddPipeline(controllerEngine, assignedNodeList, gpay.pipeline_id));
                        logger.debug("Submitted Resource Pipeline for Scheduling");
                        scheduleStatus = 4;
                    } else {
                        scheduleStatus = 61;
                    }
                } else {
                    scheduleStatus = 62;
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

    private List<gNode> buildEdgeMaps(List<gEdge> edges, List<gNode> nodes) {

        List<gNode> nodeEdgeList = null;
        try {

            nodeEdgeList = new ArrayList<>();

            //Generate a list of edges for each related node
            Map<String,List<gEdge>> edgeNodeMap = new HashMap<>();

            Map<String,gNode> nodeMap = new HashMap<>();

            //List<String> nodeIds = new ArrayList<>();
            for(gNode node : nodes) {
                nodeMap.put(node.node_id,node);
            }

            //verify predicates
            for (gEdge edge : edges) {

                logger.debug("edge_id=" + edge.edge_id + " node_from=" + edge.node_from + " node_to=" + edge.node_to);


                if ((nodeMap.containsKey(edge.node_to)) && (nodeMap.containsKey(edge.node_from))) {
                    //modify nodes

                    gNode node_to = nodeMap.get(edge.node_to);
                    gNode node_from = nodeMap.get(edge.node_from);

                    logger.debug("NODE_TO PARAMS: [" + node_to.params + "]");
                    logger.debug("NODE_FROM PARAMS: [" + node_from.params + "]");

                    String ntRegion = node_to.params.get("location_region");
                    String ntAgent = node_to.params.get("location_agent");

                    String nfRegion = node_from.params.get("location_region");
                    String nfAgent = node_from.params.get("location_agent");

                    logger.debug("nfr: " + nfRegion + " nfa:" + nfAgent + " ntr:" + ntRegion + " nta:" + ntAgent);

                    edge.getParamsTo().put("location_region",ntRegion);
                    edge.getParamsTo().put("location_agent",ntAgent);

                    edge.getParamsFrom().put("location_region",nfRegion);
                    edge.getParamsFrom().put("location_agent",nfAgent);


                    if(!edgeNodeMap.containsKey(edge.node_to)) {
                        edgeNodeMap.put(edge.node_to, new ArrayList<>());
                    }
                    if(!edgeNodeMap.containsKey(edge.node_from)) {
                        edgeNodeMap.put(edge.node_from, new ArrayList<>());
                    }

                    //adding list to related nodes
                    edgeNodeMap.get(edge.node_to).add(edge);
                    edgeNodeMap.get(edge.node_from).add(edge);


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

            for(gNode node : nodes) {
                //nodeIds.add(node.node_id);
                if(edgeNodeMap.containsKey(node.node_id)) {

                    String edgePayload = gson.toJson(edgeNodeMap.get(node.node_id));
                    //logger.error("EDGE PAYLOAD: " + edgePayload);
                    node.params.put("edges",edgePayload);
                }


                nodeEdgeList.add(node);
            }

        }
        catch(Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }

        return nodeEdgeList;
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
                    if (controllerEngine.getGDB().nodeExist(node.params.get("location_region"), node.params.get("location_agent"),null)) {
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    } else {
                        errorNodes.add(node);
                    }
                } else if(node.params.containsKey("location")) {
                    String nodeId = getLocationNodeId(node.params.get("location"));
                    if(nodeId != null) {
                        Map<String,String> nodeMap = controllerEngine.getGDB().getNodeParams(nodeId);
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
            List<String> aNodeList = controllerEngine.getGDB().getANodeFromIndex("location", location);
            if(!aNodeList.isEmpty()) {
                locationNodeId = aNodeList.get(0);
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return locationNodeId;
    }

}
