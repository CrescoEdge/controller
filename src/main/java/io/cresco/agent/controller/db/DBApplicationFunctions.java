package io.cresco.agent.controller.db;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import io.cresco.agent.controller.app.gEdge;
import io.cresco.agent.controller.app.gNode;
import io.cresco.agent.controller.app.gPayload;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;


public class DBApplicationFunctions {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private OrientGraphFactory factory;
    //private ODatabaseDocumentTx db;
    private int retryCount;
    //private DBEngine dbe;
    //private OrientGraph odb;
    //private OPartitionedDatabasePool pool;

    public DBApplicationFunctions(ControllerEngine controllerEngine, DBEngine dbe) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DBApplicationFunctions.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(DBApplicationFunctions.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        //this.agentcontroller = agentcontroller;
        //this.pool = dbe.pool;

        this.factory = dbe.factory;
        // this.factory = getFactory();
        //this.db = dbe.db;
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);
        //odb = factory.getTx();

        //create basic application constructs
        initCrescoDB();

        //object
        //OObjectDatabaseTx db = new OObjectDatabaseTx(dbe.pool.acquire());

    }


    private OrientGraphFactory getFactory() {
        OrientGraphFactory getFactory = null;

        try {
            String host = plugin.getConfig().getStringParam("gdb_host");
            String username = plugin.getConfig().getStringParam("gdb_username");
            String password = plugin.getConfig().getStringParam("gdb_password");
            String dbname = plugin.getConfig().getStringParam("gdb_dbname");

            String connection_string = "remote:" + host + "/" + dbname;


            if ((host != null) && (username != null) && (password != null) && (dbname != null)) {
                getFactory = new OrientGraphFactory(connection_string, username, password);
            }
            else {
                getFactory = new OrientGraphFactory("memory:internalDb");
            }
        }
        catch(Exception ex) {
            logger.error("isMemory : " + ex.getMessage());
        }
        return getFactory;

    }

    public boolean removePipeline(String pipelineId) {
        boolean isRemoved = false;
        try {
            getresourceNodeList(pipelineId,null);
            List<String> vNodeList = getNodeIdFromEdge("pipeline", "isVNode", "vnode_id", true, "pipeline_id", pipelineId);
            for(String vNodeId : vNodeList) {
                logger.debug("vnodes " + vNodeId);
                List<String> iNodeList =getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNodeId);
                for(String iNodeId : iNodeList) {
                    logger.debug("inodes " + iNodeId);
                    List<String> eNodeList = getNodeIdFromEdge("inode", "in", "enode_id", false, "inode_id",iNodeId);
                    eNodeList.addAll(getNodeIdFromEdge("inode", "out", "enode_id",true, "inode_id",iNodeId));
                    for(String eNodeId : eNodeList) {
                        logger.debug("enodes " + eNodeId);
                        removeNode(getENodeNodeId(eNodeId));
                    }
                    removeNode(getINodeNodeId(iNodeId));
                }
                removeNode(getVNodeNodeId(vNodeId));
            }
            removeNode(getPipelineNodeId(pipelineId));
            isRemoved = true;
        }
        catch(Exception ex) {
            logger.error("removePipeline " + ex.getMessage());
        }
        return isRemoved;
    }

    /*
    public void clearCache() {
        factory.getDatabase().getLocalCache().invalidate();
    }
*/
    /*
    public List<String> getPipelineINodes(String pipelineId) {
        List<String> iNodeList = null;
        try {
            iNodeList = new ArrayList<>();
            getresourceNodeList(pipelineId,null);
            List<String> vNodeList = getNodeIdFromEdge("pipeline", "isVNode", "vnode_id", true, "pipeline_id", pipelineId);
            for(String vNodeId : vNodeList) {
                logger.debug("vnodes " + vNodeId);
                iNodeList.addAll(getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNodeId));
            }

        }
        catch(Exception ex) {
            logger.error("removePipeline " + ex.getMessage());
        }
        return iNodeList;
    }

    public List<String> removePipeline(String pipelineId) {
        List<String> iNodeList = null;
        try {
            iNodeList = new ArrayList<>();
            getresourceNodeList(pipelineId,null);
            List<String> vNodeList = getNodeIdFromEdge("pipeline", "isVNode", "vnode_id", true, "pipeline_id", pipelineId);
            for(String vNodeId : vNodeList) {
                logger.debug("vnodes " + vNodeId);
                iNodeList.addAll(getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNodeId));
                for(String iNodeId : iNodeList) {
                    logger.debug("inodes " + iNodeId);
                    List<String> eNodeList = getNodeIdFromEdge("inode", "in", "enode_id", false, "inode_id",iNodeId);
                    eNodeList.addAll(getNodeIdFromEdge("inode", "out", "enode_id",true, "inode_id",iNodeId));
                    for(String eNodeId : eNodeList) {
                        logger.debug("enodes " + eNodeId);
                        removeNode(getENodeNodeId(eNodeId));
                    }
                }
                removeNode(getVNodeNodeId(vNodeId));
            }

            //removeNode(getPipelineNodeId(pipelineId));
        }
        catch(Exception ex) {
            logger.error("removePipeline " + ex.getMessage());
        }
        return iNodeList;
    }
       */
    public String getTenantNodeId(String tenantId) {
        String node_id = null;
        OrientGraph graph = null;

        //object

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:Tenant.tenant_id WHERE key = '" + tenantId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            logger.error("getTenantNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getPipelineNodeIdNoTx(String pipelineId) {
        String node_id = null;
        //OrientGraph graph = null;
        OrientGraphNoTx graph = null;
        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            //graph = factory.getTx();
            graph = factory.getNoTx();
            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:Pipeline.pipeline_id WHERE key = '" + pipelineId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }


        }
        catch(Exception ex)
        {
            logger.error("getPipelineNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getPipelineNodeId(String pipelineId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:Pipeline.pipeline_id WHERE key = '" + pipelineId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }


        }
        catch(Exception ex)
        {
            logger.error("getPipelineNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getVNodeNodeId(String vNodeId) {
        String edge_id = null;

        OrientGraph graph = null;


        int count = 0;
        try
        {
            graph = factory.getTx();
            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                String tmp_edge_id = IgetVNodeNodeId(vNodeId);
                if(tmp_edge_id == null) {
                    return null;
                }
                if(!tmp_edge_id.equals("*")) {
                    edge_id = tmp_edge_id;
                }
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("getVNodeNodeId : failed to get node " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("getVNodeNodeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public String IgetVNodeNodeId(String vNodeId) {
        String node_id = null;
        OrientGraph graph = null;
        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:vNode.vnode_id WHERE key = '" + vNodeId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            logger.error("getVNodeID : Error " + ex.toString());
            node_id = "*";
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getINodeNodeId(String iNodeId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.inode_id WHERE key = '" + iNodeId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            logger.error("getINodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getENodeNodeId(String eNodeId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:eNode.enode_id WHERE key = '" + eNodeId + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            logger.error("getENodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getPipeline(String pipelineId) {
        String json = null;
        OrientGraph graph = null;
        try {
            graph = factory.getTx();
            String pipelineNodeId = getPipelineNodeId(pipelineId);
            if (pipelineNodeId != null) {
                //if(getPipelineNodeId(pipelineId != null)
                Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
                if (vPipeline != null) {
                    json = vPipeline.getProperty("submission");
                }
            }
        }
        catch(Exception ex) {
            logger.error("getPipeline " + ex.getMessage());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return json;
    }

    public String createIEdge(String node_from, String node_to) {
        OrientGraph graph = null;
        String edge_id = null;
        int count = 0;
        try
        {
            graph = factory.getTx();
            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IcreateIEdge(node_from,node_to);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createIEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createIEdge : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    String IcreateIEdge(String node_from, String node_to) {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            logger.debug("createIedge from_node: " + node_from + " node_to: " + node_to);
            logger.debug("-I0 ");
            //Vertex iNode_from = odb.getVertexByKey("iNode.node_id", node_from);
            Vertex iNode_from = graph.getVertex(getINodeNodeId(node_from));
            logger.debug("-I1 ");

            //Vertex iNode_to = odb.getVertexByKey("iNode.node_id", node_to);
            Vertex iNode_to = graph.getVertex(getINodeNodeId(node_to));

            logger.debug("-I2 ");

            if((iNode_from != null) && (iNode_to != null))
            {
                logger.debug("iEdge: node_from:" + node_from + " node_to:" + node_to);
            }
            logger.debug("-I3 ");

            Iterable<Edge> fromEdges = iNode_from.getEdges(Direction.OUT, "Out");
            Edge eEdge_from = fromEdges.iterator().next();
            Vertex eNode_from = eEdge_from.getVertex(Direction.IN);
            if(eNode_from != null)
            {
                logger.debug("From eNode_id:" +  eNode_from.getProperty("enode_id") + " iNode_id:" + eNode_from.getProperty("inode_id"));
            }
            logger.debug("-I4 ");

            Iterable<Edge> toEdges = iNode_to.getEdges(Direction.IN, "in");
            Edge eEdge_to = toEdges.iterator().next();
            Vertex eNode_to = eEdge_to.getVertex(Direction.OUT);
            logger.debug("-I5 ");

            if(eNode_to != null)
            {
                logger.debug("To eNode_id:" +  eNode_to.getProperty("enode_id") + " iNode_id:" + eNode_to.getProperty("inode_id"));
            }
            logger.debug("-I6 ");

            Edge iEdge = graph.addEdge(null, eNode_from, eNode_to, "isEConnected");
			/*
			 public Iterable<Edge> getEdges(OrientVertex iDestination,
                               Direction iDirection,
                               String... iLabels)
			 */
            logger.debug("-I7 ");

            //Edge vEdge = odb.addEdge(null, eNode_from, eNode_to, "isIConnected");
            //Edge vEdge = eNode_from.getEdges(arg0, arg1)
            //odb.commit();
            //return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IcreateIedge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;

    }

    String createVEdge(String node_from, String node_to) {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            logger.debug("createVEdge node_from: " + node_from + " node_to:" + node_to);

            logger.debug("-0");
            //Vertex vNode_from = odb.getVertexByKey("vNode.node_id", node_from);
            Vertex vNode_from = graph.getVertex(getVNodeNodeId(node_from));
            logger.debug("-1");
            //Vertex vNode_to = odb.getVertexByKey("vNode.node_id", node_to);
            Vertex vNode_to = graph.getVertex(getVNodeNodeId(node_to));
            logger.debug("-2");
            Edge vEdge = graph.addEdge(null, vNode_from, vNode_to, "isVConnected");
            logger.debug("-3");
            edge_id = UUID.randomUUID().toString();
            logger.debug("-4");
            vEdge.setProperty("edge_id", edge_id);
            logger.debug("-5");
            //odb.commit();
            logger.debug("-6");
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.debug("createVEdge DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.debug("createVedge: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public boolean setPipelineStatus(gPayload gpay) {
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            String pipelineId = gpay.pipeline_id;
            String status_code = gpay.status_code;
            String status_desc = gpay.status_desc;
            String submission = JsonFromgPayLoad(gpay);
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));

            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                vPipeline.setProperty("submission", submission);

                //odb.commit();
                return true;
            }
        }
        catch(Exception ex)
        {
            logger.debug("setPipelineStatus Error: " + ex.toString());
            logger.debug(controllerEngine.getStringFromError(ex));
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;

    }

    public boolean setPipelineStatusNoTx(String pipelineId, String status_code, String status_desc) {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IsetPipelineStatusNoTx(pipelineId,status_code,status_desc);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("removeNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("removeNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IsetPipelineStatusNoTx(String pipelineId, String status_code, String status_desc) {
        //TransactionalGraph graph = null;
        //OrientGraph graph = null;
        OrientGraphNoTx graph = null;
        try
        {
            String pipelineNodeId = getPipelineNodeId(pipelineId);
            //graph = factory.getTx();
            graph = factory.getNoTx();
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(pipelineNodeId);

            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                //graph.commit();
                //odb.commit();
                //return true;
            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
            logger.error("IsetPipelineStatus concurrent " + exc.getMessage());
            StringWriter errors = new StringWriter();
            exc.printStackTrace(new PrintWriter(errors));
            logger.error("IsetPipelineStatus concurrent " + errors.toString());

        }
        catch(Exception ex)
        {
            logger.error("setPipelineStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;

    }

    public boolean setPipelineStatus(String pipelineId, String status_code, String status_desc) {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {
            /*
            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("ADDNODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddNode(region, agent, agentcontroller);
                count++;

            }
            */

            while((!nodeRemoved) && (count != retryCount))
            {

                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    long delay = (long)(Math.random() * 1000);
                    //logger.error("delay: " + delay);
                    Thread.sleep(delay); //random wait to prevent sync error
                }
                nodeRemoved = IsetPipelineStatus(pipelineId,status_code,status_desc);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("removeNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("removeNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IsetPipelineStatus(String pipelineId, String status_code, String status_desc) {
        //TransactionalGraph graph = null;
        boolean isCommited = false;
        OrientGraph graph = null;
        try
        {
            String pipelineNodeId = getPipelineNodeId(pipelineId);
            graph = factory.getTx();
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(pipelineNodeId);

            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                graph.commit();
                //odb.commit();
                //return true;
                isCommited = true;
            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
            logger.error("IsetPipelineStatus concurrent " + exc.getMessage());
            StringWriter errors = new StringWriter();
            exc.printStackTrace(new PrintWriter(errors));
            logger.error("IsetPipelineStatus concurrent " + errors.toString());
        }
        catch(Exception ex)
        {
            logger.error("setPipelineStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                    graph.shutdown();
            }
        }
        //return false;
        return isCommited;
    }

    public int getINodeStatus(String INodeId) {
        int statusCode = -1;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(getINodeNodeId(INodeId));

            if(vPipeline != null)
            {
                String statusString = vPipeline.getProperty("status_code");
                if(statusString != null) {
                    statusCode = Integer.parseInt(statusString);
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getINodeStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return statusCode;

    }

    public int getPipelineStatusCode(String pipelineId) {
        int statusCode = -1;
        OrientGraph graph = null;
        try
        {
            //Map<String,String> statusMap = getPipelineStatus(pipelineId);
            //String statusCodeString = statusMap.get("status_code");
            //if(statusCodeString != null) {
                statusCode = Integer.parseInt(getPipelineStatus(pipelineId).get("status_code"));
            //}

        }
        catch(Exception ex)
        {
            logger.debug("getPipelineStatusCode Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        //return statusCode;
        return statusCode;
    }


    public Map<String,String> getPipelineStatus(String pipelineId) {
        Map<String,String> statusMap = new HashMap<>();
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));

            if(vPipeline != null)
            {

                String nameString = vPipeline.getProperty("pipeline_name");
                if(nameString != null) {
                    statusMap.put("pipeline_name",nameString);
                } else {
                    statusMap.put("pipeline_name","unknown");
                }

                statusMap.put("pipeline_id",pipelineId);

                String descString = vPipeline.getProperty("status_desc");
                if(descString != null) {
                    statusMap.put("status_desc",descString);
                } else {
                    statusMap.put("status_desc","unknown");
                }

                String tenantString = vPipeline.getProperty("tenant_id");
                if(tenantString != null) {
                    statusMap.put("tenant_id",tenantString);
                } else {
                    statusMap.put("tenant_id","unknown");
                }

                String statusString = vPipeline.getProperty("status_code");
                if(statusString != null) {
                    statusMap.put("status_code",statusString);
                } else {
                    statusMap.put("status_code","unknown");
                }

            }
        }
        catch(Exception ex)
        {
            logger.error("setPipelineStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        //return statusCode;
        return statusMap;
    }


    public Map<String,String> getPipelineParam(String pipelineId, String param) {
        //String params = null;
        OrientGraph graph = null;
        Map<String,String> params = null;
        try {
            graph = factory.getTx();

            String node_id = getPipelineNodeId(pipelineId);
            if(node_id != null) {

                Vertex iNode = graph.getVertex(node_id);
                params = getMapFromString(iNode.getProperty(param).toString(), false);
            }
        }
        catch(Exception ex) {
            logger.error("getPipelineParam " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return params;
    }

    public String getINodeParams(String iNode_id) {
        String params = null;
        OrientGraph graph = null;

        try {
            graph = factory.getTx();

            String node_id = getINodeNodeId(iNode_id);
            if(node_id != null) {
                Vertex iNode = graph.getVertex(node_id);
                params = iNode.getProperty("params");
            }
        }
        catch(Exception ex) {
            logger.error("getINodeConfigParams " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return params;
    }

    public gPayload getPipelineObj(String pipelineId) {
        OrientGraph graph = null;
        gPayload gpay = null;
        try {
            graph = factory.getTx();

            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            String pipelineNodeId = getPipelineNodeId(pipelineId);
            if(pipelineNodeId != null) {
                Vertex vPipeline = graph.getVertex(pipelineNodeId);
                if (vPipeline != null) {
                    String json = vPipeline.getProperty("submission");
                    gpay = gPayLoadFromJson(json);
                }
            }
        }
        catch(Exception ex) {
            logger.error("getPipelineObj " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return gpay;

    }

    public String getINodefromVNode(String vNode_id) {
        String edge_id = null;

        int count = 0;
        try
        {
            if(vNode_id != null) {
                while ((edge_id == null) && (count != retryCount)) {
                    if (count > 0) {
                        Thread.sleep((long) (Math.random() * 1000)); //random wait to prevent sync error
                    }
                    String tmp_edge_id = IgetINodefromVNode(vNode_id);
                    if (tmp_edge_id.equals("*")) {
                        return null;
                    }
                    else {
                        edge_id = tmp_edge_id;
                    }
                    count++;

                }

                if ((edge_id == null) && (count == retryCount)) {
                    logger.debug("getINodefromVNode " + count + " retrys");
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getINodefromVNode : Error " + ex.toString());
        }

        return edge_id;
    }

    public String IgetINodefromVNode(String vNode_id) {
        String iNode_id = null;

        //OrientGraph graph = null;
        TransactionalGraph graph = null;
        try
        {
            /*
            List<String> iNodeList = getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNode_id);
            if(iNodeList.size() > 0) {
                iNode_id = iNodeList.get(0);
            }
            */

            graph = factory.getTx();

            //Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);
            Vertex vNode = graph.getVertex(getVNodeNodeId(vNode_id));
            if(vNode != null)
            {
                //json = vPipeline.getProperty("submission");
                Iterable<Edge> vNodeEdges = vNode.getEdges(Direction.OUT, "isINode");
                Iterator<Edge> iter = vNodeEdges.iterator();

                Edge isINode = iter.next();
                Vertex iNode = isINode.getVertex(Direction.IN);
                return iNode.getProperty("inode_id");
            }

        }
        catch(Exception ex)
        {
            logger.error("getINodefromVNode: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return iNode_id;
    }

    public boolean setINodeStatus(String iNode_id, String status_code, String status_desc) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode == null)
            {
                logger.debug("setINodeStatus Error: Node not found!");
                return false;
            }
            else
            {
                iNode.setProperty("status_code", status_code);
                iNode.setProperty("status_desc", status_desc);
                //odb.commit();
                logger.debug("setINodeStatus iNode_id:" + iNode_id + " status_code:" + status_code + " status_desc:" + status_desc);
                return true;
            }

        }
        catch(Exception ex)
        {
            logger.error("setINodeStatus: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;
    }

    public List<String> getPipelineIdList() {
        OrientGraph graph = null;

        List<String> pipelineList = null;
        try
        {
            graph = factory.getTx();

            pipelineList = new ArrayList<>();
            Iterable<Vertex> pipelines = graph.getVerticesOfClass("Pipeline");
            Iterator<Vertex> iter = pipelines.iterator();
            while (iter.hasNext())
            {
                Vertex vPipeline = iter.next();
                String pipelineId = vPipeline.getProperty("pipeline_id");
                //gPayload gpay = gPayLoadFromJson(submission);
                pipelineList.add(pipelineId);
            }
        }
        catch(Exception ex)
        {
            logger.debug("getPipelineIdList Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }

        logger.debug("PipelineIdlist return: " + pipelineList.size());

        return pipelineList;
    }

    public List<gPayload> getPipelineList() {
        OrientGraph graph = null;

        List<gPayload> pipelineList = null;
        try
        {
            graph = factory.getTx();

            pipelineList = new ArrayList<gPayload>();
            Iterable<Vertex> pipelines = graph.getVerticesOfClass("Pipeline");
            Iterator<Vertex> iter = pipelines.iterator();
            while (iter.hasNext())
            {
                Vertex vPipeline = iter.next();
                String submission = vPipeline.getProperty("submission");
                gPayload gpay = gPayLoadFromJson(submission);
                pipelineList.add(gpay);

            }
        }
        catch(Exception ex)
        {
            logger.debug("getPipelineList Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }

        logger.debug("Pipeline list return: " + pipelineList.size());

        return pipelineList;
    }

    public boolean iNodeIsActive(String iNode_id) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));

            if(iNode != null)
            {
                if(iNode.getProperty("inode_id").toString().equals("4"))
                {
                    return true;
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("iNodeIsActive: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;
    }

    public Map<String,String> getMapFromString(String param, boolean isRestricted) {
        Map<String,String> paramMap = null;

        logger.debug("PARAM: " + param);

        try{
            String[] sparam = param.split(",");
            logger.debug("PARAM LENGTH: " + sparam.length);

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                String[] sstr = str.split(":");

                if(isRestricted)
                {
                    paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                }
                else
                {
                    if(sstr.length > 1)
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), URLDecoder.decode(sstr[1], "UTF-8"));
                    }
                    else
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                    }
                }
            }
        }
        catch(Exception ex)
        {
            logger.error("getMapFromString Error: " + ex.toString());
        }

        return paramMap;
    }

    public String getvNodefromINode(String iNode_id) {
        String uINode = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode != null)
            {
                //Get Enode for this iNode
                Iterable<Edge> iNodeEdges = iNode.getEdges(Direction.IN, "isINode");
                Iterator<Edge> iterE = iNodeEdges.iterator();
                Edge isIn = iterE.next();
                Vertex eNode = isIn.getVertex(Direction.OUT);
                logger.debug("vNode: " + eNode.getId().toString());
                uINode = eNode.getId().toString();

            }
            else
            {
                logger.error("getUpstreamNode: Error: Incoming iNode: " + iNode_id + " is null");
            }
        }
        catch(Exception ex)
        {
            logger.error("getUpstreamNode: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return uINode;
    }

    public String getUpstreamNode(String iNode_id) {
        String uINode = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode != null)
            {
                //Get Enode for this iNode
                Iterable<Edge> iNodeEdges = iNode.getEdges(Direction.IN, "in");
                Iterator<Edge> iterE = iNodeEdges.iterator();
                Edge isIn = iterE.next();
                Vertex eNode = isIn.getVertex(Direction.OUT);
                logger.debug("ENode: " + eNode.getId().toString());

                //Get Enode for the upstream iNode
                Iterable<Edge> eNodeEdges = eNode.getEdges(Direction.IN, "isEConnected");
                Iterator<Edge> iterE2 = eNodeEdges.iterator();
                Edge isEConnected = iterE2.next();
                Vertex eNode2 = isEConnected.getVertex(Direction.OUT);
                logger.debug("ENode2: " + eNode2.getId().toString());

                //Get Upstream iNode
                Iterable<Edge> iNodeEdges2 = eNode2.getEdges(Direction.IN, "out");
                Iterator<Edge> iter2 = iNodeEdges2.iterator();
                Edge isOut = iter2.next();
                Vertex iNode2 = isOut.getVertex(Direction.OUT);
                uINode = iNode2.getProperty("inode_id");
            }
            else
            {
                logger.error("getUpstreamNode: Error: Incoming iNode: " + iNode_id + " is null");
            }
        }
        catch(Exception ex)
        {
            logger.error("getUpstreamNode: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return uINode;
    }

    public Map<String,String> getNodeManifest(String iNode_id) {
        Map<String,String> manifest = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode != null)
            {
                manifest = getMapFromString(iNode.getProperty("params").toString(), false);
                //set type in manifest
                manifest.put("inode_id", iNode.getProperty("inode_id").toString());
                manifest.put("node_name", iNode.getProperty("node_name").toString());
                manifest.put("node_type", iNode.getProperty("node_type").toString());
                manifest.put("status_code", iNode.getProperty("status_code").toString());
                manifest.put("status_desc", iNode.getProperty("status_desc").toString());

                //add type-specific information This should be done a better way
                if((manifest.get("node_type").equals("membuffer")) || (manifest.get("node_type").equals("query")))
                {
                    //WALK BACK AND FIND AMQP
                    String upstreamINode_id = getUpstreamNode(iNode_id);
                    if(upstreamINode_id == null)
                    {
                        logger.error("getNodeManifest: Error: null nextNodeID before AMQP Node");
                        return null;
                    }
                    else
                    {
                        //Vertex uINode = odb.getVertexByKey("iNode.node_id", upstreamINode_id);
                        Vertex uINode = graph.getVertex(getINodeNodeId(upstreamINode_id));
                        if(uINode == null)
                        {
                            logger.error("getNodeManifest: Error: null uINode Node");
                            return null;
                        }
                        else
                        {
                            if(uINode.getProperty("status_code") != null)
                            {
                                if(uINode.getProperty("status_code").toString().equals("4"))
                                {
                                    logger.debug("Found Active Upstream iNode:" + upstreamINode_id);
                                    Map<String,String> params = getMapFromString(uINode.getProperty("params").toString(), false);
                                    manifest.put("outExchange", params.get("outExchange"));
                                    manifest.put("amqp_server", params.get("amqp_server"));
                                    manifest.put("amqp_login", params.get("amqp_login"));
                                    manifest.put("amqp_password", params.get("amqp_password"));
                                    return manifest;
                                }
                            }
                            else
                            {
                                logger.debug("Upstream iNode is InActive: " + upstreamINode_id);
                                return null;
                            }

                        }
                    }
                }

            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeManifest: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return manifest;
    }

    public gPayload createPipelineNodes(gPayload gpay) {

        try
        {
            //create nodes and links for pipeline records
            HashMap<String,String> iNodeHm = new HashMap<>();
            HashMap<String,String> vNodeHm = new HashMap<>();

            for(gNode node : gpay.nodes)
            {
                try
                {
                    //add resource_id to configs
                    node.params.put("resource_id",gpay.pipeline_id);
                    //

                    String vNode_id = createVNode(gpay.pipeline_id, node,true);
                    vNodeHm.put(node.node_id, vNode_id);

                    logger.debug("vNode:" + vNode_id + " pipelineNodeID: " + node.node_id + " dbNodeId: " + getVNodeNodeId(vNode_id));

                    String iNode_id = createINode(vNode_id, node);
                    iNodeHm.put(node.node_id, iNode_id);

                    logger.debug("iNode:" + iNode_id + " pipelineNodeID: " + node.node_id + " dbNodeId: " +  getINodeNodeId(iNode_id));

                    //Create iNode I/O Nodes
                    String eNodeIn_id = createENode(iNode_id, true);
                    String eNodeOut_id = createENode(iNode_id, false);
                    logger.debug("eNodeIn:" + getENodeNodeId(eNodeIn_id) + " eNodeOut: " + getENodeNodeId(eNodeOut_id));

                    //assoicateNtoV(vNode_id,iNode_id);
                    //keep the vnodeID
                    node.node_id = iNode_id;

                    //test finding nodes
                    //getNode(node);
                }
                catch(Exception ex)
                {
                    logger.debug("createPipelineNodes Node: " + ex.toString());
                }
            }

            for(gEdge edge : gpay.edges)
            {
                try
                {
                    //createGEdge(edge);
                    //vEdge
                    if((edge.node_from != null) && (edge.node_to != null)) {
                        if ((vNodeHm.containsKey(edge.node_from)) && (vNodeHm.containsKey(edge.node_to))) {

                            //create edge between vnodes
                            logger.debug("From vID : " + edge.node_from + " TO vID: " + edge.node_to);
                            String edge_id = createVEdge(vNodeHm.get(edge.node_from), vNodeHm.get(edge.node_to));
                            edge.edge_id = edge_id;
                            logger.debug("vedgeid: " + edge_id + " from: " + iNodeHm.get(edge.node_from) + " to:" + vNodeHm.get(edge.node_to));

                            //create edge between inodes
                            String iedge_id = createIEdge(iNodeHm.get(edge.node_from), iNodeHm.get(edge.node_to));
                            //assign vEdge ID
                            edge.node_from = vNodeHm.get(edge.node_from);
                            edge.node_to = vNodeHm.get(edge.node_to);
                            logger.debug("iedgeid: " + iedge_id + " from: " + iNodeHm.get(edge.node_from) + " to:" + vNodeHm.get(edge.node_to));

                        }
                    }
                    //iEdge
                }
                catch(Exception ex)
                {
                    logger.debug("createPipelineNodes Edge: " + ex.toString());
                }
            }
            //return gpay;
            gpay.status_code = "3";
            gpay.status_desc = "Pipeline Nodes Created.";
            setPipelineStatus(gpay);
            //lets see what this does
            return gpay;
        }
        catch(Exception ex)
        {
            logger.debug("createPipelineNodes: " + ex.toString());
            //todo remove this
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));

            logger.error(errors.toString());
        }
        gpay.status_code = "1";
        gpay.status_desc = "Failed to create Pipeline.";
        setPipelineStatus(gpay);
        return gpay;
    }

    public String createVNode(String pipelineId, gNode node, boolean isNew) {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IcreateVNode(pipelineId,node,isNew);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createVNode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createVNode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateVNode(String pipelineId, gNode node, boolean isNew) {
        TransactionalGraph graph = null;
        String node_id = null;
        try
        {
            graph = factory.getTx();

            Vertex vNode = graph.addVertex("class:vNode");
            vNode.setProperty("node_name", node.node_name);
            vNode.setProperty("node_type", node.type);
            if(isNew)
            {
                node_id = UUID.randomUUID().toString();
            }
            else
            {
                node_id = node.node_id;
            }
            vNode.setProperty("vnode_id", node_id);

            if(node.params.size() > 0) {
                vNode.setProperty("params",encodeParams(node.params));
            }

            vNode.setProperty("isSource", String.valueOf(node.isSource));
            //graph.commit();

            //logger.debug("Created vNode " + node_id + " Node ID " + vNode.getId().toString() );
            //+ " getvNode = " + getVNodeNodeId(node_id));

            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
            Edge ePipeline = graph.addEdge(null, vPipeline, vNode, "isVNode");
            //graph.commit();

            //    return node_id;
            graph.commit();
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public boolean removeNode(String rid) {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveNode(rid);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("removeNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("removeNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveNode(String rid) {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            if(rid != null) {
                graph = factory.getTx();
                Vertex rNode = graph.getVertex(rid);

                graph.removeVertex(rNode);
                graph.commit();
            }
            nodeRemoved = true;
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
            logger.error("concurrent " + exc.getMessage());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.error("iremoveNode : nodeId=" + rid + " thread_id: " + threadId + " Error " + ex.toString() );
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public String createINode(String iNode_id, gNode node) {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IcreateINode(iNode_id,node);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createINode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createINode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateINode(String iNode_id, gNode node) {

        String node_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            node_id = UUID.randomUUID().toString();

            Vertex iNode = graph.addVertex("class:iNode");
            iNode.setProperty("node_name", node.node_name);
            iNode.setProperty("node_type", node.type);
            iNode.setProperty("inode_id", node_id);
            node.params.put("inode_id",node_id);
            //if(node.params.size() > 0) {
                iNode.setProperty("params",encodeParams(node.params));
                logger.debug("iNode Params: " + encodeParams(node.params));
            //}

            iNode.setProperty("status_code", "0");
            iNode.setProperty("status_desc", "Added to DB");


            //graph.commit();

            logger.debug("Created iNode " + node_id + " Node ID " + iNode.getId().toString());
            //+ " getiNode = " + getINodeNodeId(node_id));


            //Link to vNode
            logger.debug("Connecting to vNode = " + iNode_id + " node ID " + getVNodeNodeId(iNode_id));
            Vertex vNode = graph.getVertex(getVNodeNodeId(iNode_id));
            //Vertex vNode = graph.getVertex(iNode_id);

            Edge eNodeEdge = graph.addEdge(null, vNode, iNode, "isINode");

            graph.commit();

        //    return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String createENode(String iNode_id, boolean isIn) {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IcreateENode(iNode_id,isIn);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createENode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createENode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateENode(String iNode_id, boolean isIn) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            String node_id = UUID.randomUUID().toString();

            Vertex eNode = graph.addVertex("class:eNode");
            eNode.setProperty("enode_id", node_id);
            eNode.setProperty("inode_id", iNode_id);
            graph.commit();

            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));

            if(isIn)
            {
                Edge ePipeline = graph.addEdge(null, eNode, iNode, "in");
            }
            else
            {
                Edge ePipeline = graph.addEdge(null, iNode, eNode, "out");
            }
            //graph.commit();

            logger.debug("Created eNode " + node_id + " Node ID " + eNode.getId().toString() + " geteNode = " + getENodeNodeId(node_id));


            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("createENode DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.error("createENode: " + ex.toString());
        }
        return null;
    }

    String assoicateNtoV(String vNode_id, String iNode_id) {

        String node_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            //Link to vNode
            logger.debug("Connecting to vNode = " + iNode_id + " node ID " + getVNodeNodeId(iNode_id));
            Vertex vNode = graph.getVertex(getVNodeNodeId(vNode_id));
            Vertex iNode = graph.getVertex(getVNodeNodeId(iNode_id));

            Edge eNodeEdge = graph.addEdge(null, vNode, iNode, "isINode");

            //Create iNode I/O Nodes
            String eNode_id = createENode(node_id, true);

            logger.debug("Added new eNode in: " + eNode_id);
            eNode_id = createENode(node_id, false);
            logger.debug("Added new eNode out: " + eNode_id);

            //graph.commit();

            //    return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String encodeParams(Map<String,String> params) {
        String params_string = null;
        try{
            StringBuilder sb = new StringBuilder();

            for (Map.Entry<String, String> entry : params.entrySet())
            {
                String sKey = URLEncoder.encode(entry.getKey(), "UTF-8");
                String sValue = null;
                if(entry.getValue() != null)
                {
                    sValue = URLEncoder.encode(entry.getValue(), "UTF-8");
                }

                sb.append(sKey + ":" + sValue + ",");
            }
            params_string = sb.substring(0, sb.length() -1);

        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
            logger.error("encodeParams: Exception ex:" + ex.toString());
        }
        return params_string;
    }

    public gPayload createPipelineRecord(String tenant_id, String gPayload) {
        {
            gPayload gpay = null;

            int count = 0;
            try
            {

                while((gpay == null) && (count != retryCount))
                {
                    if(count > 0)
                    {
                        Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                    }
                    gpay = IcreatePipelineRecord(tenant_id,gPayload);
                    count++;

                }

                if((gpay == null) && (count == retryCount))
                {
                    logger.debug("createPipelineRecord : Failed to add edge in " + count + " retrys");
                }
            }
            catch(Exception ex)
            {
                logger.debug("createPipelineRecord : Error " + ex.toString());
            }

            return gpay;
        }
    }

    private gPayload IcreatePipelineRecord(String tenant_id, String gPayload) {
        logger.debug("createPipelineRecord...");
        TransactionalGraph graph = null;
        gPayload gpay = null;
        try
        {
            graph = factory.getTx();

            gpay = gPayLoadFromJson(gPayload);

            gpay.pipeline_id = UUID.randomUUID().toString();
            //inject real pipelineID
            gPayload = JsonFromgPayLoad(gpay);


            if(getPipelineNodeId(gpay.pipeline_id) == null) {
                logger.debug("Creating vPipeline");

                Vertex vPipeline = graph.addVertex("class:Pipeline");
                vPipeline.setProperty("pipeline_id", gpay.pipeline_id);
                vPipeline.setProperty("pipeline_name", gpay.pipeline_name);
                vPipeline.setProperty("submission", gPayload);
                vPipeline.setProperty("status_code", "3");
                vPipeline.setProperty("status_desc", "Record added to DB.");
                vPipeline.setProperty("tenant_id", tenant_id);

                String tenantNode = getTenantNodeId(tenant_id);
                Vertex vTenant = graph.getVertex(tenantNode);
                Edge eLives = graph.addEdge(null, vPipeline, vTenant, "isPipeline");

                //graph.commit();
                //odb.commit();
                /*
                if(getPipelineNodeId(gpay.pipeline_id) == null) {
                    logger.error("Pipeline record was not saved to database!");
                }
                else {
                    logger.debug("Post vPipeline commit of node " + getPipelineNodeId(gpay.pipeline_id));
                }
                String tenantNode = getTenantNodeId(tenant_id);
                logger.error("**** CODY tenant_id : " + tenant_id + " " + tenantNode);
                if(tenantNode != null) {
                    Vertex vTenant = graph.getVertex(tenantNode);
                    Edge eLives = graph.addEdge(null, vPipeline, vTenant, "isPipeline");
                }
                graph.commit();
                //odb.commit();

                logger.info(.add Pipeline to Scheduler Queue");
                //agentcontroller.getAppScheduleQueue().add(gpay);
                //return gpay;
                */
                graph.commit();

                if(getPipelineNodeId(gpay.pipeline_id) == null) {
                    logger.error("Pipeline record was not saved to database!");
                }
                else {
                    logger.debug("Post vPipeline commit of node " + getPipelineNodeId(gpay.pipeline_id));
                }
                controllerEngine.getAppScheduleQueue().add(gpay);

            }
            else {
                logger.error("createPipelineRecord : Duplicate pipeline_id!" );
            }

        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
            //logger.error("IsetPipelineStatus concurrent " + exc.getMessage());
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("createPipelineRecord DUPE " + se.toString());
        }
        catch(Exception ex)
        {

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("createPipelineRecord " + ex.toString());

        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return gpay;
    }

    boolean createTenant(String tenantName, String tenantId) {
        TransactionalGraph graph = null;
        try
        {
            graph = factory.getTx();

            Vertex vTenant = graph.addVertex("class:Tenant");
            if(tenantId == null){
                vTenant.setProperty("tenant_id", UUID.randomUUID().toString());
            }
            else {
                vTenant.setProperty("tenant_id", tenantId);
            }
            graph.commit();
            //odb.commit();
            //return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.error(ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;
    }

    public Map<String,String> getNodeIdParametersFromEdge(String nodeClass, String edgeLabel, List<String> properties, boolean in, String key, String value) {
        OrientGraph graph = null;
        Map<String,String> returnProperties = null;
        try
        {
            returnProperties = new HashMap<>();
            if((nodeClass != null) && (edgeLabel != null)) {
                nodeClass = nodeClass.toLowerCase();
                edgeLabel.toLowerCase();

                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();//SELECT rid, expand(outE('isVNode')) from pipeline
                String queryString = null;
                if(in) {
                    queryString = "SELECT rid, expand(outE('" + edgeLabel + "').inV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                } else {
                    queryString = "SELECT rid, expand(inE('" + edgeLabel + "').outV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                }
                logger.debug("querystring " + queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                while (iter.hasNext()) {
                    Vertex v = iter.next();

                    for (String p : properties)
                    {
                        String property = v.getProperty(p).toString();
                        if (property != null) {

                            returnProperties.put(p,property);
                        }
                    }

                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeIdParametersFromEdge : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return returnProperties;
    }

    public List<String> getNodeIdFromEdge(String nodeClass, String edgeLabel, String property, boolean in, String key, String value) {
        String edge_id = null;
        OrientGraph graph = null;
        List<String> edge_list = null;
        try
        {
            if((nodeClass != null) && (edgeLabel != null)) {
                nodeClass = nodeClass.toLowerCase();
                edgeLabel.toLowerCase();

                edge_list = new ArrayList<String>();
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();//SELECT rid, expand(outE('isVNode')) from pipeline
                String queryString = null;
                if(in) {
                    queryString = "SELECT rid, expand(outE('" + edgeLabel + "').inV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                } else {
                    queryString = "SELECT rid, expand(inE('" + edgeLabel + "').outV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                }
                logger.debug("querystring " + queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                while (iter.hasNext()) {
                    Vertex v = iter.next();

                    edge_id = v.getProperty(property).toString();
                    if (edge_id != null) {

                        //edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        logger.debug("parameter = " + edge_id);
                        edge_list.add(edge_id);
                    }

                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeIdFromEdge : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_list;
    }

    public Map<String,String> getpNodeINode(String iNode_id) {
        Map<String,String> nodeMap = null;
        OrientGraph graph = null;
        try
        {
            nodeMap = new HashMap<>();
            if(iNode_id != null) {

                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();//SELECT rid, expand(outE('isVNode')) from pipeline
                String queryString = "SELECT rid, expand(inE('isAssigned').outV()) from inode";

                logger.debug("querystring " + queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                while (iter.hasNext()) {
                    Vertex v = iter.next();

                    String region = v.getProperty("region").toString();
                    String agent = v.getProperty("agent").toString();
                    String plugin = v.getProperty("agentcontroller").toString();

                    if ((region != null) && (agent != null)  && (plugin != null)) {
                        //edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        logger.debug("parameter = " + region + "agent " + "agentcontroller " + plugin);
                        nodeMap.put("region",region);
                        nodeMap.put("agent",agent);
                        nodeMap.put("agentcontroller",plugin);
                    }

                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeIdFromEdge : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeMap;
    }

    public void initCrescoDB() {
        OrientGraph graph = null;

        try
        {
            logger.debug("Create iNode Vertex Class");
            String[] iProps = {"resource_id", "inode_id"}; //Property names
            createVertexClass("iNode", iProps);
            createVertexIndex("iNode","inode_id",true);

            logger.debug("Create Tenant Vertex Class");
            createVertexClass("Tenant");
            createVertexIndex("Tenant", "tenant_id", true);

            logger.debug("Create Pipeline Vertex Class");
            createVertexClass("Pipeline");
            createVertexIndex("Pipeline", "pipeline_id", true);

            logger.debug("Create eNode Vertex Class");
            createVertexClass("eNode");
            createVertexIndex("eNode", "enode_id", true);

            logger.debug("Create vNode Vertex Class");
            createVertexClass("vNode");
            createVertexIndex("vNode", "vnode_id", true);

            logger.debug("Create isVNode Edge Class");
            createEdgeClass("isVNode",null);

            logger.debug("Create isINode Edge Class");
            createEdgeClass("isINode",null);

            logger.debug("Create isVConnected Edge Class");
            createEdgeClass("isVConnected",null);

            logger.debug("Create isPipeline Edge Class");
            createEdgeClass("isPipeline",null);

            logger.debug("Create isEConnected Edge Class");
            createEdgeClass("isEConnected",null);

            logger.debug("Create in Edge Class");
            createEdgeClass("in",null);

            logger.debug("Create out Edge Class");
            createEdgeClass("out",null);

            logger.debug("Create isConnected Edge Class");
            String[] isConnectedProps = {"edge_id"}; //Property names
            //createEdgeClass("isConnected",isConnectedProps);
            createEdgeClass("isConnected",null);

            logger.debug("Create isResource Edge Class");
            String[] isResourceProps = {"edge_id"}; //Property names
            //createEdgeClass("isResource",isResourceProps);
            createEdgeClass("isResource",null);

            logger.debug("Create isReachable Edge Class");
            String[] isReachableProps = {"edge_id"}; //Property names
            //createEdgeClass("isReachable",isReachableProps);
            createEdgeClass("isReachable",null);

            logger.debug("Create isAssigned Edge Class");
            String[] isAssignedProps = {"resource_id","inode_id","region", "agent", "agentcontroller"}; //Property names
            createEdgeClass("isAssigned",isAssignedProps);

            logger.debug("Create resourceNode Vertex Class");
            String[] resourceProps = {"resource_id"}; //Property names
            createVertexClass("resourceNode", resourceProps);

            //create agentcontroller anchor resource and inodes
            String[]  pluginAnchors= {"sysinfo","netdiscovery","container","controllerinfo"}; //Property names
            for(String pAnchor : pluginAnchors)
            {
                String resource_id = pAnchor + "_resource";
                String inode_id = pAnchor + "_inode";

                logger.debug("Creating " + pAnchor + " resource node.");
                if(getResourceNodeId(resource_id) == null)
                {
                    //create resource
                    addResourceNode(resource_id);
                }
                logger.debug("Creating " + pAnchor + " iNode.");
                if(getINodeNodeId(inode_id) == null)
                {
                    //create inode
                    addINode(resource_id, inode_id);
                }
            }

            createTenant("default", "0");

            /*
            graph = factory.getTx();

            graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
                @Override
                public Object call(OrientBaseGraph iArgument) {

                    logger.debug("Create Tenant Vertex Class");
                    createVertexClass("Tenant");
                    createVertexIndex("Tenant", "tenant_id", true);

                    logger.debug("Create Pipeline Vertex Class");
                    createVertexClass("Pipeline");
                    createVertexIndex("Pipeline", "pipeline_id", true);

                    logger.debug("Create eNode Vertex Class");
                    createVertexClass("eNode");
                    createVertexIndex("eNode", "enode_id", true);

                    logger.debug("Create vNode Vertex Class");
                    createVertexClass("vNode");
                    createVertexIndex("vNode", "vnode_id", true);

                    logger.debug("Create isVNode Edge Class");
                    createEdgeClass("isVNode",null);

                    logger.debug("Create isINode Edge Class");
                    createEdgeClass("isINode",null);

                    logger.debug("Create isVConnected Edge Class");
                    createEdgeClass("isVConnected",null);

                    logger.debug("Create isPipeline Edge Class");
                    createEdgeClass("isPipeline",null);

                    logger.debug("Create isEConnected Edge Class");
                    createEdgeClass("isEConnected",null);

                    logger.debug("Create in Edge Class");
                    createEdgeClass("in",null);

                    logger.debug("Create out Edge Class");
                    createEdgeClass("out",null);

                    logger.debug("Create isConnected Edge Class");
                    String[] isConnectedProps = {"edge_id"}; //Property names
                    //createEdgeClass("isConnected",isConnectedProps);
                    createEdgeClass("isConnected",null);

                    logger.debug("Create isResource Edge Class");
                    String[] isResourceProps = {"edge_id"}; //Property names
                    //createEdgeClass("isResource",isResourceProps);
                    createEdgeClass("isResource",null);

                    logger.debug("Create isReachable Edge Class");
                    String[] isReachableProps = {"edge_id"}; //Property names
                    //createEdgeClass("isReachable",isReachableProps);
                    createEdgeClass("isReachable",null);

                    logger.debug("Create isAssigned Edge Class");
                    String[] isAssignedProps = {"resource_id","inode_id","region", "agent"}; //Property names
                    createEdgeClass("isAssigned",isAssignedProps);

                    logger.debug("Create resourceNode Vertex Class");
                    String[] resourceProps = {"resource_id"}; //Property names
                    createVertexClass("resourceNode", resourceProps);

                    logger.debug("Create iNode Vertex Class");
                    String[] iProps = {"resource_id", "inode_id"}; //Property names
                    createVertexClass("iNode", iProps);
                    createVertexIndex("iNode","inode_id",true);


                    //create agentcontroller anchor resource and inodes
                    String[]  pluginAnchors= {"sysinfo","netdiscovery","container"}; //Property names
                    for(String pAnchor : pluginAnchors)
                    {
                        String resource_id = pAnchor + "_resource";
                        String inode_id = pAnchor + "_inode";

                        logger.debug("Creating " + pAnchor + " resource node.");
                        if(getResourceNodeId(resource_id) == null)
                        {
                            //create resource
                            addResourceNode(resource_id);
                        }
                        logger.debug("Creating " + pAnchor + " iNode.");
                        if(getINodeId(resource_id, inode_id) == null)
                        {
                            //create inode
                            addINode(resource_id, inode_id);
                        }
                    }



                    return null;
                }
            });

            createTenant("default", "0");
            */
        }
        catch(Exception ex)
        {
            logger.debug("App initCrescoDB Error: " + ex.toString());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.debug("App initCrescoDB Error: " + errors.toString());

        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
    }

    boolean createVertexClass(String className, String[] props) {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass vt = txGraph.createVertexType(className);
            for(String prop : props)
                vt.createProperty(prop, OType.STRING);
            vt.createIndex(className + ".nodePath", OClass.INDEX_TYPE.UNIQUE, props);
            txGraph.commit();
            if (schema.existsClass(className)) {
                wasCreated = true;
            }
        }
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createEdgeClass(String className, String[] props) {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass et = txGraph.createEdgeType(className);
            if(props != null) {
                for (String prop : props) {
                    et.createProperty(prop, OType.STRING);
                }
                et.createIndex(className + ".edgeProp", OClass.INDEX_TYPE.UNIQUE, props);
            }
            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createVertexClass(String className) {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass vt = txGraph.createVertexType(className);
            txGraph.commit();
            if (schema.existsClass(className)) {
                wasCreated = true;
            }
        }
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createVertexIndex(String className, String indexName, boolean isUnique) {
        boolean wasCreated = false;
        try
        {
            OrientGraphNoTx txGraph = factory.getNoTx();
            OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

            if (schema.existsClass(className))
            {
                OClass vt = txGraph.getVertexType(className);
                if(vt.getProperty(indexName) == null) {
                    vt.createProperty(indexName, OType.STRING);
                }
                if(isUnique)
                {
                    vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.UNIQUE, indexName);
                }
                else
                {
                    vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.NOTUNIQUE, indexName);
                }

                wasCreated = true;
            }

            txGraph.commit();
            txGraph.shutdown();
        }
        catch(Exception ex)
        {
            logger.error("createVertexIndex : Error " + ex.toString());
        }

        return wasCreated;
    }

    public String JsonFromgPayLoad(gPayload gpay) {
        Gson gson = new GsonBuilder().create();
        //gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return gson.toJson(gpay);

    }

    public gPayload gPayLoadFromJson(String json) {
        Gson gson = new GsonBuilder().create();
        gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return me;
    }

    //NEW FROM BASIC

    //READS

    //READS
    /*
    public String getINodeId(String inode_id) {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            String queryString = "SELECT rid FROM INDEX:iNode.inode_id WHERE key = [\"" + inode_id + "\"]";
            logger.error(queryString);
            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getINodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getINodeId(String resource_id, String inode_id) {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.nodePath WHERE key = [\"" + resource_id + "\",\"" + inode_id + "\"]")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getINodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }
*/
    public String getResourceNodeId(String resource_id)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:resourceNode.nodePath WHERE key = '" + resource_id + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getResourceNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public List<String> getIsAttachedEdgeId(String region, String agent, String pluginId) {
        List<String> edgeList = null;
        OrientGraphNoTx graph = null;
        try
        {
            edgeList = new ArrayList<>();
            if((region != null) && (agent != null) && (pluginId != null))
            {
                String nodeId = controllerEngine.getGDB().gdb.getNodeId(region,agent,pluginId);
                if(nodeId != null) {
                    graph = factory.getNoTx();
                    String queryString = "SELECT * from isAssigned where out = \"" + nodeId + "\"";
                    logger.debug(queryString);
                    Iterable<Edge> resultIterator = graph.command(new OCommandSQL(queryString)).execute();
                    Iterator<Edge> iter = resultIterator.iterator();
                    while (iter.hasNext()) {
                        Edge e = iter.next();
                        edgeList.add(e.getId().toString());
                    }
                    logger.debug("getIsAttachedEdgeId region " + region + "agent " + agent + " pluginId " + pluginId + " edgeListCount " + edgeList.size());
                }

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getIsAttachedEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edgeList;
    }

    public String getResourceEdgeId(String resource_id, String inode_id, String region, String agent, String pluginId) {
        String edge_id = null;
        OrientGraph graph = null;
        try
        {
            if((resource_id != null) && (inode_id != null) && (region != null) && (agent != null))
            {



                graph = factory.getTx();
                String queryString = "SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id + "\",\"" + region + "\",\"" + agent + "\",\"" + pluginId +"\"]";
                logger.debug(queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(edge_id != null)
                {
                    edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                }
                logger.debug("getResourceEdgeId resource_id " + resource_id + " inode_id " + inode_id + "  region " + region + "agent " + agent + " pluginId " + pluginId + " edgeid " + edge_id);

                //Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);


            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getResourceEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public String getResourceEdgeId(String resource_id, String inode_id)
    {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            if((resource_id != null) && (inode_id != null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(edge_id != null)
                {
                    edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                }
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getIsAssignedEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public List<String> getIsAssignedEdgeIds(String resource_id, String inode_id)
    {
        String edge_id = null;
        //OrientGraph graph = null;
        OrientGraphNoTx graph = null;
        List<String> edge_list = null;
        try
        {
            if((resource_id != null) && (inode_id != null))
            {
                edge_list = new ArrayList<String>();
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                //graph = factory.getTx();
                graph = factory.getNoTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();

                    if(edge_id != null)
                    {
                        edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        edge_list.add(edge_id);
                    }

                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getIsAssignedEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_list;
    }

    public String getIsAssignedParam(String edge_id,String param_name)
    {
        String param = null;
        OrientGraph graph = null;

        try
        {
            if((edge_id != null) && (param_name != null))
            {
                graph = factory.getTx();
                Edge e = graph.getEdge(edge_id);
                param = e.getProperty(param_name).toString();

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getIsAssignedParam : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return param;
    }

    public Map<String,String> getIsAssignedParams(String edge_id)
    {
        Map<String,String> params = null;
        OrientGraph graph = null;

        try
        {
            if(edge_id != null)
            {
                params = new HashMap<>();
                graph = factory.getTx();
                Edge e = graph.getEdge(edge_id);

                for (String s : e.getPropertyKeys()) {
                    params.put(s,e.getProperty(s).toString());
                }

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getIsAssignedParams : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return params;
    }

    public List<String> getresourceNodeList(String resource_id, String inode_id)
    {
        List<String> node_list = null;
        OrientGraph graph = null;
        try
        {

            if((resource_id == null) && (inode_id == null))
            {
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:resourceNode.nodePath")).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex rNode = graph.getVertex(node_id);
                        node_list.add(rNode.getProperty("resource_id").toString());
                    }
                }

            }
            else if((resource_id != null) && (inode_id == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.nodePath WHERE key = [\"" + resource_id + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex aNode = graph.getVertex(node_id);
                        node_list.add(aNode.getProperty("inode_id").toString());
                    }
                }

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeList : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_list;
    }

    public String getINodeParam(String inode_id, String param)
    {
        String iNode_param = null;
        String node_id = null;
        //OrientGraph graph = null;
        OrientGraphNoTx graph = null;
        try
        {
            //node_id = getINodeId(inode_id);
            node_id = getINodeNodeId(inode_id);
            if(node_id != null)
            {
                //graph = factory.getTx();
                graph = factory.getNoTx();
                Vertex iNode = graph.getVertex(node_id);
                iNode_param = iNode.getProperty(param).toString();
            }

        }
        catch(Exception ex)
        {
            logger.debug("getINodeParam: Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return iNode_param;
    }
/*
    public String getINodeParam(String resource_id,String inode_id, String param)
    {
        String iNode_param = null;
        String node_id = null;
        OrientGraph graph = null;

        try
        {
            node_id = getINodeNodeId(inode_id);
            //node_id = getINodeId(resource_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                iNode_param = iNode.getProperty(param).toString();
            }

        }
        catch(Exception ex)
        {
            logger.debug("getINodeParam: Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return iNode_param;
    }
*/
    //WRITES
/*
    public String addIsConnectedEdge(String resource_id, String inode_id, String region, String agent, String agentcontroller)
    {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IaddIsConnectedEdge(resource_id, inode_id, region, agent, agentcontroller);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addIsAttachedEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addIsAttachedEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddIsConnectedEdge(String inode_id, String region, String agent, String pluginId)
    {
        String edge_id = null;
        TransactionalGraph graph = null;
        try
        {
            edge_id = getResourceEdgeId(resource_id,inode_id,region,agent,pluginId);
            if(edge_id != null)
            {
                //logger.debug("Node already Exist: region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);
            }
            else
            {

                if((resource_id != null) && (inode_id != null) && (region != null) && (agent != null) && (pluginId != null))
                {
                    String inode_node_id = getINodeNodeId(inode_id);
                    String pnode_node_id = agentcontroller.getGDB().gdb.getNodeId(region,agent,pluginId);
                    if((inode_node_id != null) && (pnode_node_id != null))
                    {
                        graph = factory.getTx();

                        Vertex fromV = graph.getVertex(pnode_node_id);
                        Vertex toV = graph.getVertex(inode_node_id);
                        if((fromV != null) && (toV != null))
                        {
                            Edge e = graph.addEdge("class:isAssigned", fromV, toV, "isAssigned");
                            e.setProperty("resource_id", resource_id);
                            e.setProperty("inode_id", inode_id);
                            e.setProperty("region", region);
                            e.setProperty("agent", agent);
                            e.setProperty("agentcontroller", pluginId);
                            graph.commit();
                            edge_id = e.getId().toString();
                        }
                        graph.commit();
                    }
                    else
                    {
                        if(inode_node_id != null)
                        {
                            logger.debug("IaddIsAttachedEdge: iNode does not exist : " + inode_id);
                        }
                        if(pnode_node_id != null)
                        {
                            logger.debug("IaddIsAttachedEdge: pNode does not exist : " + region + agent + pluginId);
                        }
                    }
                }
                else
                {
                    logger.debug("IaddIsAttachedEdge: required input is null : " + resource_id + "," + inode_id + "," + region + "," + agent + "," + agentcontroller);

                }

            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            logger.debug("IaddIsAttachedEdge: ORecordDuplicatedException : " + exc.toString());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            logger.debug("IaddIsAttachedEdge: OConcurrentModificationException : " + exc.toString());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IaddIsAttachedEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;

    }
  */
    public String addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin) {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IaddIsAttachedEdge(resource_id, inode_id, region, agent, plugin);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addIsAttachedEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addIsAttachedEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String pluginId) {
        String edge_id = null;
        TransactionalGraph graph = null;
        try
        {
            edge_id = getResourceEdgeId(resource_id,inode_id,region,agent,pluginId);
            if(edge_id != null)
            {
                //logger.debug("Node already Exist: region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);
            }
            else
            {

                if((resource_id != null) && (inode_id != null) && (region != null) && (agent != null) && (pluginId != null))
                {
                    String inode_node_id = getINodeNodeId(inode_id);
                    String pnode_node_id = controllerEngine.getGDB().gdb.getNodeId(region,agent,pluginId);
                    if((inode_node_id != null) && (pnode_node_id != null))
                    {
                        graph = factory.getTx();

                        Vertex fromV = graph.getVertex(pnode_node_id);
                        Vertex toV = graph.getVertex(inode_node_id);
                        if((fromV != null) && (toV != null))
                        {
                            Edge e = graph.addEdge("class:isAssigned", fromV, toV, "isAssigned");
                            e.setProperty("resource_id", resource_id);
                            e.setProperty("inode_id", inode_id);
                            e.setProperty("region", region);
                            e.setProperty("agent", agent);
                            e.setProperty("agentcontroller", pluginId);
                            graph.commit();
                            edge_id = e.getId().toString();
                        }
                        graph.commit();
                    }
                    else
                    {
                        if(inode_node_id != null)
                        {
                            logger.debug("IaddIsAttachedEdge: iNode does not exist : " + inode_id);
                        }
                        if(pnode_node_id != null)
                        {
                            logger.debug("IaddIsAttachedEdge: pNode does not exist : " + region + agent + pluginId);
                        }
                    }
                }
                else
                {
                    logger.debug("IaddIsAttachedEdge: required input is null : " + resource_id + "," + inode_id + "," + region + "," + agent + "," + plugin);

                }

            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            logger.debug("IaddIsAttachedEdge: ORecordDuplicatedException : " + exc.toString());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            logger.debug("IaddIsAttachedEdge: OConcurrentModificationException : " + exc.toString());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IaddIsAttachedEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;

    }


    public String addINode(String resource_id, String inode_id)
    {
        String node_id = null;

        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddINode(resource_id, inode_id);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addINode : Error " + ex.toString());
        }

        return node_id;
    }

    public String addINodeResource(String resource_id, String inode_id)
    {
        String node_id = null;

        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddINodeResource(resource_id, inode_id);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                logger.debug("addINodeResource : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("addINodeResource : Error " + ex.toString());
        }

        return node_id;
    }

    private String IaddINodeResource(String resource_id, String inode_id)
    {
        String node_id = null;
        String resource_node_id = null;

        OrientGraph graph = null;
        try
        {

            node_id = controllerEngine.getGDB().dba.getINodeNodeId(inode_id);
            if(node_id != null)
            {
                resource_node_id = getResourceNodeId(resource_id);
                if(resource_node_id == null)
                {
                    resource_node_id = addResourceNode(resource_id);
                }

                if(resource_node_id != null)
                {
                    graph = factory.getTx();

                    Vertex fromV = graph.getVertex(node_id);
                    fromV.setProperty("resource_id", resource_id);

                    //ADD EDGE TO RESOURCE
                    Vertex toV = graph.getVertex(resource_node_id);
                    graph.addEdge("class:isResource", fromV, toV, "isResource");
                    graph.commit();
                    node_id = fromV.getId().toString();
                }
            }
            else {
                logger.error("IaddINodeResource inode " + inode_id + " missing!");
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            logger.debug("Error 0 " + exc.getMessage());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            logger.debug("Error 1 " + exc.getMessage());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IaddINode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    private String IaddINode(String resource_id, String inode_id)
    {
        String node_id = null;
        String resource_node_id = null;

        OrientGraph graph = null;
        try
        {
            node_id = getINodeNodeId(inode_id);
            if(node_id != null)
            {
                //logger.debug("Node already Exist: region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);
            }
            else
            {

                resource_node_id = getResourceNodeId(resource_id);
                if(resource_node_id == null)
                {
                    resource_node_id = addResourceNode(resource_id);
                }

                if(resource_node_id != null)
                {
                    graph = factory.getTx();

                    Vertex fromV = graph.addVertex("class:iNode");
                    fromV.setProperty("inode_id", inode_id);
                    fromV.setProperty("resource_id", resource_id);


                    //ADD EDGE TO RESOURCE
                    Vertex toV = graph.getVertex(resource_node_id);
                    graph.addEdge("class:isResource", fromV, toV, "isResource");
                    graph.commit();
                    node_id = fromV.getId().toString();
                }
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            logger.debug("Error 0 " + exc.getMessage());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            logger.debug("Error 1 " + exc.getMessage());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("IaddINode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String addResourceNode(String resource_id) {
        String node_id = null;

        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("ADDNODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddResourceNode(resource_id);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addINode : Error " + ex.toString());
        }

        return node_id;
    }

    private String IaddResourceNode(String resource_id)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            node_id = getResourceNodeId(resource_id);

            if(node_id != null)
            {
                //logger.debug("Node already Exist: region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);
            }
            else
            {
                graph = factory.getTx();
                //add something

                Vertex v = graph.addVertex("class:resourceNode");
                v.setProperty("resource_id", resource_id);
                graph.commit();
                node_id = v.getId().toString();
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("addResourceNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public boolean removeINode(String inode_id)
    {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveINode(inode_id);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("DBEngine : removeINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : removeINode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveINode(String inode_id)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,agentcontroller);
            String node_id = getINodeNodeId(inode_id);
            if(node_id == null)
            {
                logger.debug("Tried to remove missing node : inode_id=" + inode_id);
                nodeRemoved = true;
            }
            else
            {
                graph = factory.getTx();
                Vertex rNode = graph.getVertex(node_id);
                graph.removeVertex(rNode);
                graph.commit();
                nodeRemoved = true;
            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("GrapgDBEngine : removeNode :  thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public boolean removeResourceNode(String resource_id)
    {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveResourceNode(resource_id);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("DBEngine : removeResourceNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : removeResourceNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveResourceNode(String resource_id)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,agentcontroller);
            String node_id = getResourceNodeId(resource_id);
            if(node_id == null)
            {
                logger.debug("Tried to remove missing node : resource_id=" + resource_id);
                nodeRemoved = true;
            }
            else
            {
                //remove iNodes First
                List<String> inodes = getresourceNodeList(resource_id,null);
                if(inodes != null)
                {
                    for(String inode_id : inodes)
                    {
                        removeINode(inode_id);
                    }
                }
                inodes = getresourceNodeList(resource_id,null);
                if(inodes == null)
                {
                    graph = factory.getTx();
                    Vertex resourceNode = graph.getVertex(node_id);
                    graph.removeVertex(resourceNode);
                    graph.commit();
                    nodeRemoved = true;
                }

            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("DBEngine : IremoveResourceNode :  thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public boolean IsetINodeParams(String inode_id, Map<String,String> paramMap)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getINodeNodeId(inode_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext())
                {
                    Map.Entry pairs = (Map.Entry)it.next();
                    iNode.setProperty( pairs.getKey().toString(), pairs.getValue().toString());
                }
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    public boolean setINodeParams(String inode_id, Map<String,String> paramMap)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetINodeParams(inode_id, paramMap);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : setINodeParams : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : setINodeParams : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean setINodeParam(String inode_id, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetINodeParam(inode_id, paramKey, paramValue);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : setINodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : setINodeParam : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetINodeParam(String inode_id, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getINodeNodeId(inode_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                iNode.setProperty( paramKey, paramValue);
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.debug("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    //Updateing KPI
    public boolean updateKPI(String region, String agent, String pluginId, String resource_id, String inode_id, Map<String,String> params) {
        boolean isUpdated = false;
        String edge_id = null;
        try
        {
            //distribute to KPI Broker
            /*
            Map<String,String> perfMap = new HashMap<>();
            perfMap.putAll(params);
            perfMap.put("region", region);
            perfMap.put("agent", agent);
            perfMap.put("agentcontroller", pluginId);
            perfMap.put("resourceid", resource_id);
            perfMap.put("inodeid", inode_id);
            */
            controllerEngine.getKPIProducer().sendMessage(region,agent,pluginId,resource_id,inode_id,params);

            //make sure nodes exist
            String resource_node_id = getResourceNodeId(resource_id);
            String inode_node_id = getINodeNodeId(inode_id);
            String plugin_node_id = controllerEngine.getGDB().gdb.getNodeId(region,agent,pluginId);

            //create node if not seen.. this needs to be changed.
            if(plugin_node_id != null) {
                //plugin_node_id = agentcontroller.getGDB().gdb.addNode(region,agent,pluginId);
                //logger.debug("updateKPI : Added Node" + region + " " + agent + " " + pluginId + " = " + plugin_node_id);


                if ((resource_node_id != null) && (inode_node_id != null) && (plugin_node_id != null)) {
                    //logger.debug("updateKPI resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + agentcontroller + " = " + plugin_node_id);
                    //check if edge is found, if not create it
                    edge_id = getResourceEdgeId(resource_id, inode_id, region, agent, pluginId);
                    if (edge_id == null) {

                        edge_id = addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);
                        logger.debug("updateKPI edge addIsAttachedEdge resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);

                    }
                    //edge_id = getResourceEdgeId(resource_id, inode_id, region, agent);

                    //check again if edge is found
                    if (edge_id != null) {
                        logger.debug("updateKPI edge found resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);

                        edge_id = getResourceEdgeId(resource_id, inode_id, region, agent, pluginId);

                        if (updateEdgeNoTx(edge_id, params)) {
                            isUpdated = true;
                        } else {
                            logger.error("Failed to updatePerf : Failed to update Edge params!");
                        }
                    } else {
                        logger.error("Failed to updatePerf : edge_id not found!");
                    }
                } else {
                    logger.debug("Can't update missing nodes : " + resource_id + "," + inode_id + "," + pluginId);
                    logger.debug("Can't update missing nodes : " + resource_node_id + "," + inode_node_id + "," + plugin_node_id);
                }
            } else {
                logger.debug("Can't update agentcontroller with no agent : " + resource_id + "," + inode_id + "," + region + "," + agent);
            }
        }
        catch(Exception ex)
        {
            logger.debug("Controller : DBEngine : Failed to updatePerf");

        }
        return isUpdated;
    }

    private boolean updateEdgeNoTx(String edge_id, Map<String,String> params) {
        boolean isUpdated = false;
        OrientGraphNoTx graph = null;
        try
        {
            graph = factory.getNoTx();
            Edge edge = graph.getEdge(edge_id);
            if(edge != null)
            {
                for (Map.Entry<String, String> entry : params.entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                graph.commit();
                isUpdated = true;
            }
            else
            {
                logger.error("IupdateEdge: no edge found for edge_id=" + edge_id);
            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            logger.error("IupdateEdge: duplicate exception for edge_id=" + edge_id);
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            logger.error("IupdateEdge: concurrent exception for edge_id=" + edge_id);
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.error("IupdateEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }



}
