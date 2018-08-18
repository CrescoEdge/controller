package io.cresco.agent.controller.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryNode;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class DBBaseFunctions {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private OrientGraphFactory factory;
    private ODatabaseDocumentTx db;
    private int retryCount;

    private Gson gson;
    //private DBEngine dbe;
    public OPartitionedDatabasePool pool;
    private Boolean regionalImportActive = false;

    public String[] aNodeIndexParams = {"platform","environment","location"};
    public String[] pNodeIndexParams = {"pluginname","jarfile"};

    public DBBaseFunctions(ControllerEngine controllerEngine, DBEngine dbe) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DBBaseFunctions.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(DBBaseFunctions.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        //this.agentcontroller = agentcontroller;
        //this.factory = dbe.factory;
        //this.factory = getFactory();
        this.factory = dbe.factory;
        this.db = dbe.db;
        this.pool = dbe.pool;
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);

        this.gson = new Gson();
        //ODatabaseDocumentTx db = ODatabaseDocumentPool.global().acquire("memory:MyDb", "admin", "admin");

        //create basic cresco constructs
        initCrescoDB();
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
                getFactory = new OrientGraphFactory(connection_string, username, password).setupPool(100, 10000);
            }
            else {
                getFactory = new OrientGraphFactory("memory:internalDb").setupPool(100, 10000);
            }
        }
        catch(Exception ex) {
            logger.error("isMemory : " + ex.getMessage());
        }
        return getFactory;

    }


    //new database functions
    //READS

    public List<String> getANodeFromIndex(String indexName, String indexValue) {
        List<String> nodeList = null;
        OrientGraph graph = null;

        try
        {
            nodeList = new ArrayList<String>();
            graph = factory.getTx();
            Iterable<Vertex> resultIterator = null;
            if(indexValue == null) {
                resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode'")).execute();
            }
            else {
                resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode." + indexName + " WHERE key = '" + indexValue + "'")).execute();
            }
            Iterator<Vertex> iter = resultIterator.iterator();
            while(iter.hasNext())
            //if(iter.hasNext())
            {
                Vertex v = iter.next();
                String node_id = v.getProperty("rid").toString();
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                    nodeList.add(node_id);
                }
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getANodeFromIndex : Error " + ex.toString());
            nodeList = null;
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeList;
    }

    public List<String> getPNodeFromIndex(String indexName, String indexValue) {
        List<String> nodeList = null;
        OrientGraph graph = null;

        try
        {
            nodeList = new ArrayList<String>();
            graph = factory.getTx();
            Iterable<Vertex> resultIterator = null;

            resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode." + indexName + " WHERE key = '" + indexValue + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            while(iter.hasNext())
            //if(iter.hasNext())
            {
                Vertex v = iter.next();
                String node_id = v.getProperty("rid").toString();
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                    nodeList.add(node_id);
                }
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getPNodeFromIndex : Error " + ex.toString());
            nodeList = null;
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeList;
    }


    public String getEdgeHealthId(String region, String agent, String plugin) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            if((region != null) && (agent == null) && (plugin == null))
            {
                graph = factory.getTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isRegionHealth.edgeProp WHERE key = '" + region + "'")).execute();

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
                //return isFound;
            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAgentHealth.edgeProp WHERE key = [\"" + region + "\",\"" + agent + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }
            }
            else if((region != null) && (agent != null) && (plugin != null))
            {
                /*
                //TransactionalGraph tgraph = factory.getTx();
                OIndex<?> idx = factory.getDatabase().getMetadata().getIndexManager().getIndex("pNode.nodePath");
                String key = "[\"" + region + "\",\"" + agent + "\",\"" + agentcontroller +"\"]";
                //OIdentifiable rec = idx.get(key);
                Object luke = (Object)idx.get(key);
                logger.error(luke.toString());
                */
                //OIndex<OIdentifiable> idx = factory.getDatabase().getMetadata().getIndexManager().getIndex("pNode.nodePath");
                //OIdentifiable rec = idx.get(id);

                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                //graph = factory.getTx();
                //OrientGraphNoTx g = null;
                //g = factory.getNoTx();
                graph = factory.getTx();

                String query = "SELECT rid FROM INDEX:isPluginHealth.edgeProp WHERE key = [\"" + region + "\",\"" + agent + "\",\"" + plugin +"\"]";
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(query)).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeID : Error " + ex.toString());
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


    public String getNodeId(String region, String agent, String plugin) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            if((region != null) && (agent == null) && (plugin == null))
            {
                graph = factory.getTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:rNode.nodePath WHERE key = '" + region + "'")).execute();

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
                //return isFound;
            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath WHERE key = [\"" + region + "\",\"" + agent + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }
            }
            else if((region != null) && (agent != null) && (plugin != null))
            {
                /*
                //TransactionalGraph tgraph = factory.getTx();
                OIndex<?> idx = factory.getDatabase().getMetadata().getIndexManager().getIndex("pNode.nodePath");
                String key = "[\"" + region + "\",\"" + agent + "\",\"" + agentcontroller +"\"]";
                //OIdentifiable rec = idx.get(key);
                Object luke = (Object)idx.get(key);
                logger.error(luke.toString());
                */
                //OIndex<OIdentifiable> idx = factory.getDatabase().getMetadata().getIndexManager().getIndex("pNode.nodePath");
                //OIdentifiable rec = idx.get(id);

                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                //graph = factory.getTx();
                //OrientGraphNoTx g = null;
                //g = factory.getNoTx();
                graph = factory.getTx();

                String query = "SELECT rid FROM INDEX:pNode.nodePath WHERE key = [\"" + region + "\",\"" + agent + "\",\"" + plugin +"\"]";
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(query)).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }

            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeID : Error " + ex.toString());
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

    public List<String> getEdgeHealthIds(String region, String agent, String plugin, boolean getAll) {
        OrientGraph graph = null;
        List<String> edgeIdList = null;
        try
        {
            edgeIdList = new ArrayList();

            if((region == null) && (agent == null) && (plugin == null))
            {
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;
                resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isRegionHealth.edgeProp")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();

                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        edgeIdList.add(node_id);
                    }

                }

            }
            else if((region != null) && (agent == null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;
                if(getAll) {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAgentHealth.edgeProp")).execute();
                }
                else {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAgentHealth.edgeProp WHERE key = [\"" + region + "\"]")).execute();
                }
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();
                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        edgeIdList.add(node_id);

                    }
                }

            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;

                if(getAll) {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isPluginHealth.edgeProp")).execute();
                }
                else {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isPluginHealth.edgeProp WHERE key = [\"" + region + "\",\"" + agent + "\"]")).execute();
                }

                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();
                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        edgeIdList.add(node_id);
                    }
                }
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edgeIdList;
    }


    public List<String> getNodeIds(String region, String agent, String plugin, boolean getAll) {
        OrientGraph graph = null;
        List<String> nodeIdList = null;
        try
        {
            nodeIdList = new ArrayList();

            if((region == null) && (agent == null) && (plugin == null))
            {
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;
                resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:rNode.nodePath")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();

                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        nodeIdList.add(node_id);

                    }

                }

            }
            else if((region != null) && (agent == null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;
                if(getAll) {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath")).execute();
                }
                else {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath WHERE key = [\"" + region + "\"]")).execute();
                }
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();
                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        nodeIdList.add(node_id);

                    }
                }

            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = null;

                if(getAll) {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode.nodePath")).execute();
                }
                else {
                    resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode.nodePath WHERE key = [\"" + region + "\",\"" + agent + "\"]")).execute();
                }

                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    String node_id = v.getProperty("rid").toString();
                    if(node_id != null)
                    {
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        nodeIdList.add(node_id);

                    }
                }
            }

        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeIdList;
    }

    public List<String> getNodeList(String region, String agent, String plugin) {
        List<String> node_list = null;
        OrientGraph graph = null;
        try
        {

            if((region == null) && (agent == null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:rNode.nodePath")).execute();

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
                        node_list.add(rNode.getProperty("region").toString());
                    }
                }

            }
            else if((region != null) && (agent == null) && (plugin == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath WHERE key = [\"" + region + "\"]")).execute();
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
                        node_list.add(aNode.getProperty("io/cresco/agent").toString());
                    }
                }

            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode.nodePath WHERE key = [\"" + region + "\",\"" + agent +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex pNode = graph.getVertex(node_id);
                        node_list.add(pNode.getProperty("agentcontroller").toString());
                    }
                }
                //graph.shutdown();
                //return node_list;

            }

        }
        catch(Exception ex)
        {
            logger.debug("GrapgDBEngine : getNodeList : Error " + ex.toString());
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

    public String getNodeParam(String node_id, String param) {
        String paramVal = null;

        int count = 0;
        try
        {

            while((paramVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramVal = IgetNodeParam(node_id,param);
                count++;

            }

            if((paramVal == null) && (count == retryCount))
            {
                logger.debug("DBEngine : getNodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeParam : Error " + ex.toString());
        }

        return paramVal;
    }

    public String getNodeParam(String region, String agent, String plugin, String param) {
        String paramVal = null;
        String node_id  = getNodeId(region,agent,plugin);

        int count = 0;
        try
        {

            while((paramVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramVal = IgetNodeParam(node_id,param);
                count++;

            }

            if((paramVal == null) && (count == retryCount))
            {
                logger.debug("DBEngine : getNodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeParam : Error " + ex.toString());
        }

        return paramVal;
    }

    public String IgetNodeParam(String node_id, String param) {
        String node_param = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            Vertex iNode = graph.getVertex(node_id);
            node_param = iNode.getProperty(param).toString();

        }
        //TODO put this back
        /*
        catch(java.lang.IllegalArgumentException eai) {
            //Vertex id can not be null
            //there is no node
            node_param = "-1";
        }
        */
        catch(Exception ex)
        {
            logger.debug("IgetNodeParam: Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_param;
    }

    public Map<String,String> getNodeParamsNoTx(String node_id) {
        Map<String,String> paramsVal = null;

        int count = 0;
        try
        {

            while((paramsVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramsVal = IgetNodeParamsNoTx(node_id);
                count++;

            }

            if((paramsVal == null) && (count == retryCount))
            {
                logger.debug("DBEngine : getNodeParamsNoTX : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeParamsNoTX : Error " + ex.toString());
        }

        return paramsVal;
    }

    public Map<String,String> IgetNodeParamsNoTx(String node_id) {
        OrientGraphNoTx graph = null;
        Map<String,String> params = new HashMap();

        try
        {
            graph = factory.getNoTx();
            Vertex iNode = graph.getVertex(node_id);
            for(String key : iNode.getPropertyKeys()) {
                params.put(key,iNode.getProperty(key).toString());
            }
        }
        catch(Exception ex)
        {
            logger.debug("IgetNodeParamsNoTX: Error " + ex.toString());
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

    public Map<String,String> getEdgeParamsNoTx(String edgeId) {
        Map<String,String> paramsVal = null;

        int count = 0;
        try
        {

            while((paramsVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramsVal = IgetEdgeParamsNoTx(edgeId);
                count++;

            }

            if((paramsVal == null) && (count == retryCount))
            {
                logger.debug("DBEngine : getEdgeParamsNoTX : Failed in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getEdgeParamsNoTX : Error " + ex.toString());
        }

        return paramsVal;
    }

    public Map<String,String> IgetEdgeParamsNoTx(String edgeId) {
        OrientGraphNoTx graph = null;
        Map<String,String> params = new HashMap();

        try
        {
            graph = factory.getNoTx();
            Edge iEdge = graph.getEdge(edgeId);
            for(String key : iEdge.getPropertyKeys()) {
                params.put(key,iEdge.getProperty(key).toString());
            }
        }
        catch(Exception ex)
        {
            logger.debug("IgetEdgeParamsNoTX: Error " + ex.toString());
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

    public Map<String,String> IgetNodeParams(String node_id) {
        OrientGraph graph = null;
        Map<String,String> params = new HashMap();

        try
        {
            graph = factory.getTx();
            Vertex iNode = graph.getVertex(node_id);
            for(String key : iNode.getPropertyKeys()) {
                params.put(key,iNode.getProperty(key).toString());
            }
        }
        catch(Exception ex)
        {
            logger.debug("IgetNodeParams: Error " + ex.toString());
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

    public Map<String,String> getNodeParams(String node_id) {
        Map<String,String> paramsVal = null;

        int count = 0;
        try
        {

            while((paramsVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramsVal = IgetNodeParams(node_id);
                count++;

            }

            if((paramsVal == null) && (count == retryCount))
            {
                logger.debug("DBEngine : getNodeParams : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : getNodeParams : Error " + ex.toString());
        }

        return paramsVal;
    }

    //WRITES

    private String IaddNode(String region, String agent, String plugin) {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            node_id = getNodeId(region,agent,plugin);

            if(node_id != null)
            {
                logger.error("Node already Exist: region=" + region + " agent=" + agent + " agentcontroller=" + plugin);
                //Thread.dumpStack();
            }
            else
            {
                //logger.debug("Adding Node : region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);
                if((region != null) && (agent == null) && (plugin == null))
                {
                    graph = factory.getTx();
                    Vertex v = graph.addVertex("class:rNode");
                    v.setProperty("region", region);

                    graph.commit();
                    node_id = v.getId().toString();
                }
                else if((region != null) && (agent != null) && (plugin == null))
                {
                    String region_id = getNodeId(region,null,null);

                    if(region_id == null)
                    {
                        //logger.debug("Must add region=" + region + " before adding agent=" + agent);
                        region_id = addNode(region,null,null);
                        //v.setProperty("enable_pending",Boolean.TRUE.toString());
                        setNodeParam(region,null,null, "enable_pending", Boolean.TRUE.toString());

                    }
                    if(region_id != null)
                    {
                        graph = factory.getTx();
                        Vertex v = graph.addVertex("class:aNode");
                        v.setProperty("region", region);
                        v.setProperty("io/cresco/agent", agent);

                        Vertex fromV = graph.getVertex(v.getId().toString());
                        Vertex toV = graph.getVertex(region_id);

                        graph.addEdge("class:isAgent", fromV, toV, "isAgent");
                        Edge he = graph.addEdge("class:isAgentHealth", fromV, toV, "isAgentHealth");

                        he.setProperty("enable_pending", Boolean.TRUE.toString());
                        he.setProperty("region", region);
                        he.setProperty("io/cresco/agent", agent);

                        graph.commit();
                        node_id = v.getId().toString();
						/*
				    	//add edges

				    	String edge_id = addEdge(region,agent,null,region,null,null,"isAgent");
				    	if(edge_id == null)
				    	{
				    		logger.debug("Unable to add isAgent Edge between region=" + region + " and agent=" + agent);
				    	}
						 */
                    }
                }
                else if((region != null) && (agent != null) && (plugin != null))
                {
                    //logger.debug("Adding Plugin : region=" + region + " agent=" + agent + " agentcontroller=" + agentcontroller);

                    String agent_id = getNodeId(region,agent,null);
                    if(agent_id == null)
                    {
                        //logger.debug("For region=" + region + " we must add agent=" + agent + " before adding agentcontroller=" + agentcontroller);
                        agent_id = addNode(region,agent,null);
                        setNodeParam(region,agent,null, "enable_pending", Boolean.TRUE.toString());

                    }

                    if(agent_id != null)
                    {
                        graph = factory.getTx();
                        Vertex v = graph.addVertex("class:pNode");
                        v.setProperty("region", region);
                        v.setProperty("io/cresco/agent", agent);
                        v.setProperty("agentcontroller", plugin);


                        Vertex fromV = graph.getVertex(v.getId().toString());
                        Vertex toV = graph.getVertex(agent_id);

                        graph.addEdge("class:isPlugin", fromV, toV, "isPlugin");
                        Edge he = graph.addEdge("class:isPluginHealth", fromV, toV, "isPluginHealth");
                        he.setProperty("region", region);
                        he.setProperty("io/cresco/agent", agent);
                        he.setProperty("agentcontroller", plugin);

                        //no need to set this as health is checked on the agentcontroller level
                        //he.setProperty("enable_pending", Boolean.TRUE.toString());
                        graph.commit();
					    /*
					    //add Edge
					    String edge_id = addEdge(region,agent,agentcontroller,region,agent,null,"isPlugin");
					    if(edge_id == null)
					    {
					    	logger.debug("Unable to add isPlugin Edge between region=" + region + " agent=" + "agent=" + region + " and agent=" + agent);
					    }
					    */
                        node_id = v.getId().toString();
                    }
                }
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
            logger.debug("IaddNode: thread_id: " + threadId + " Error " + ex.toString());
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

    public String addNode(String region, String agent, String plugin) {
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
                node_id = IaddNode(region, agent, plugin);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addNode : Error " + ex.toString());
        }

        return node_id;
    }

    public String addIsReachableEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, DiscoveryNode dn) {
        String edge_id = null;
        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                edge_id = IaddIsReachableEdge(src_region, src_agent, src_plugin, dst_region, dst_agent, dst_plugin, dn);
                count++;
            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addIsReachableEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addIsReachableEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddIsReachableEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, DiscoveryNode dn) {
        String edge_id = null;
        try
        {
            String src_node_id = getNodeId(src_region,src_agent,src_plugin);
            String dst_node_id = getNodeId(dst_region,dst_agent,dst_plugin);

            OrientGraph graph = factory.getTx();
            Vertex fromV = graph.getVertex(src_node_id);
            Vertex toV = graph.getVertex(dst_node_id);

            Edge isEdge = graph.addEdge("class:isReachable", fromV, toV, "isReachable");
            isEdge.setProperty("src_ip",dn.src_ip);
            isEdge.setProperty("src_port",dn.src_port);
            isEdge.setProperty("src_ip",dn.dst_ip);
            isEdge.setProperty("src_port",dn.dst_port);
            isEdge.setProperty("broadcast_ts",dn.broadcast_ts);
            isEdge.setProperty("broadcast_latency",dn.broadcast_latency);

            graph.commit();
            graph.shutdown();
            edge_id = isEdge.getId().toString();
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            logger.debug("IaddIsReachableEdge Error: " + ex.toString());

        }
        return edge_id;

    }

    public String addEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className, Map<String,String> paramMap) {
        String edge_id = null;
        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                edge_id = IaddEdge(src_region, src_agent, src_plugin, dst_region, dst_agent, dst_plugin, className, paramMap);
                count++;
            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("DBEngine : addEdge : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : addEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className, Map<String,String> paramMap) {
        String edge_id = null;
        try
        {
            String src_node_id = getNodeId(src_region,src_agent,src_plugin);
            String dst_node_id = getNodeId(dst_region,dst_agent,dst_plugin);

            OrientGraph graph = factory.getTx();
            Vertex fromV = graph.getVertex(src_node_id);
            Vertex toV = graph.getVertex(dst_node_id);

            Edge isEdge = graph.addEdge("class:" + className, fromV, toV, className);
            if(paramMap != null) {
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry) it.next();
                    isEdge.setProperty(pairs.getKey().toString(), pairs.getValue().toString());
                }
            }
            graph.commit();
            graph.shutdown();
            edge_id = isEdge.getId().toString();
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            logger.debug("addEdge Error: " + ex.toString());

        }
        return edge_id;

    }


    public boolean removeNode(String region, String agent, String plugin) {
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
                nodeRemoved = IremoveNode(region, agent, plugin);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("DBEngine : removeNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : removeNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveNode(String region, String agent, String plugin)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,agentcontroller);
            String node_id = getNodeId(region,agent,plugin);
            if(node_id == null)
            {
                //logger.debug("Tried to remove missing node : " + pathname);
                nodeRemoved = true;
            }
            else
            {
                if((region != null) && (agent == null) && (plugin == null))
                {
                    List<String> agentList = getNodeList(region,null,null);
                    if(agentList != null)
                    {
                        for(String removeAgent : agentList)
                        {
                            removeNode(region,removeAgent,null);
                        }
                    }
                    agentList = getNodeList(region,null,null);
                    if(agentList == null)
                    {
                        graph = factory.getTx();
                        Vertex rNode = graph.getVertex(node_id);
                        graph.removeVertex(rNode);
                        graph.commit();
                        nodeRemoved = true;
                    }

                }
                if((region != null) && (agent != null) && (plugin == null))
                {

                    List<String> pluginList = getNodeList(region,agent,null);
                    if(pluginList != null)
                    {
                        for(String removePlugin : pluginList)
                        {
                            removeNode(region,agent,removePlugin);
                        }
                    }
                    pluginList = getNodeList(region,agent,null);
                    if(pluginList == null)
                    {
                        graph = factory.getTx();
                        Vertex aNode = graph.getVertex(node_id);
                        graph.removeVertex(aNode);
                        graph.commit();
                        nodeRemoved = true;
                    }
                }
                if((region != null) && (agent != null) && (plugin != null))
                {
                    graph = factory.getTx();
                    Vertex pNode = graph.getVertex(node_id);
                    graph.removeVertex(pNode);
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

    public boolean setNodeParamsNoTx(String nodeId, Map<String,String> paramMap)
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
                isUpdated = IsetNodeParamsNoTx(nodeId, paramMap);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : setINodeParamsNoTX : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : setINodeParamsNoTX : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetNodeParamsNoTx(String nodeId, Map<String,String> paramMap)
    {

        boolean isUpdated = false;
        OrientGraphNoTx graph = null;

        try
        {

            if(nodeId != null)
            {
                graph = factory.getNoTx();
                Vertex iNode = graph.getVertex(nodeId);
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

    public boolean setEdgeParamsNoTx(String edgeId, Map<String,String> paramMap) {
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
                isUpdated = IsetEdgeParamsNoTx(edgeId, paramMap);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : setEdgeParams : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : setEdgeParams : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetEdgeParamsNoTx(String edgeId, Map<String,String> paramMap) {

        boolean isUpdated = false;
        OrientGraphNoTx graph = null;
        try
        {
            if(edgeId != null)
            {
                graph = factory.getNoTx();
                Edge iEdge = graph.getEdge(edgeId);
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext())
                {
                    Map.Entry pairs = (Map.Entry)it.next();
                    iEdge.setProperty( pairs.getKey().toString(), pairs.getValue().toString());
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


    public boolean setNodeParamsNoTx(String region, String agent, String plugin, Map<String,String> paramMap) {
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
                isUpdated = IsetNodeParamsNoTx(region, agent, plugin, paramMap);
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

    public boolean IsetNodeParamsNoTx(String region, String agent, String plugin, Map<String,String> paramMap) {

        boolean isUpdated = false;
        OrientGraphNoTx graph = null;
        String node_id = null;
        try
        {
            node_id = getNodeId(region,agent,plugin);
            if(node_id != null)
            {
                graph = factory.getNoTx();
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

    public boolean setNodeParams(String region, String agent, String plugin, Map<String,String> paramMap)
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
                isUpdated = IsetNodeParams(region, agent, plugin, paramMap);
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

    public boolean IsetNodeParams(String region, String agent, String plugin, Map<String,String> paramMap)
    {

        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getNodeId(region,agent,plugin);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext())
                {
                    Map.Entry pairs = (Map.Entry)it.next();
                    iNode.setProperty( pairs.getKey().toString(), pairs.getValue().toString());

                    //to make sure indexing of agentcontroller type works
                    if((paramMap.containsKey("configparams") && (region != null) && (agent != null) && (plugin != null))) {

                        Type type = new TypeToken<Map<String, String>>(){}.getType();

                        Map<String, String> configparams = gson.fromJson(paramMap.get("configparams"), type);

                        for(String indexParam : pNodeIndexParams)
                        {
                            if(configparams.containsKey(indexParam)) {
                                iNode.setProperty(indexParam, configparams.get(indexParam));
                            }
                        }

                    }
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

    public boolean setEdgeParam(String edgeId, String paramKey, String paramValue) {
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
                isUpdated = IsetEdgeParam(edgeId, paramKey, paramValue);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : setEdgeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : setEdgeParam : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetEdgeParam(String edgeId, String paramKey, String paramValue) {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id;
        try
        {
            node_id = edgeId;
            if(node_id != null)
            {
                graph = factory.getTx();
                Edge iEdge = graph.getEdge(node_id);
                iEdge.setProperty( paramKey, paramValue);
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
            logger.debug("setIEdgeParams: thread_id: " + threadId + " Error " + ex.toString());
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

    public boolean setNodeParam(String nodeId, String paramKey, String paramValue) {
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
                isUpdated = IsetNodeParam(nodeId, paramKey, paramValue);
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

    public boolean IsetNodeParam(String nodeId, String paramKey, String paramValue) {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id;
        try
        {
            node_id = nodeId;
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

    public boolean setNodeParam(String region, String agent, String plugin, String paramKey, String paramValue)
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
                isUpdated = IsetNodeParam(region, agent, plugin, paramKey, paramValue);
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

    public boolean IsetNodeParam(String region, String agent, String plugin, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getNodeId(region, agent, plugin);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                //set envparams if aNode
                if((paramKey.equals("configparams")) && (region != null) && (agent != null) && (plugin == null)) {
                    String[] configParams = paramValue.split(",");
                    for(String cParam : configParams)
                    {
                        String[] cPramKV = cParam.split("=");
                        for(String indexParam : aNodeIndexParams)
                        {
                            if(cPramKV[0].equals(indexParam))
                            {
                                iNode.setProperty(cPramKV[0],cPramKV[1]);
                            }
                        }
                    }
                }

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

    //Base INIT Functions
    public void initCrescoDB() {
        try
        {
            //index properties
            logger.debug("Create Global Vertex Class");
            String[] gProps = {"region"}; //Property names
            createVertexClass("gNode", gProps);

            logger.debug("Create Region Vertex Class");
            String[] rProps = {"region"}; //Property names
            createVertexClass("rNode", rProps);

            logger.debug("Create Agent Vertex Class");
            String[] aProps = {"region", "io/cresco/agent"}; //Property names
            createVertexClass("aNode", aProps);

            //indexes for searching
            logger.debug("Create Agent Vertex Class Index's");
            for(String indexName : aNodeIndexParams) {
                createVertexIndex("aNode", indexName, false);
            }

            logger.debug("Create Plugin Vertex Class");
            String[] pProps = {"region", "io/cresco/agent", "agentcontroller"}; //Property names
            createVertexClass("pNode", pProps);

            for(String indexName : pNodeIndexParams) {
                createVertexIndex("pNode", indexName, false);
            }

            logger.debug("Create isGlobal Edge Class");
            String[] isGlobalProps = {"edge_id"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isGlobal",null);

            logger.debug("Create isGlobalHealth Edge Class");
            String[] isGlobalHealthProps = {"region"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isGlobalHealth",isGlobalHealthProps);

            logger.debug("Create isRegion Edge Class");
            String[] isRegionProps = {"edge_id"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isRegion",null);

            logger.debug("Create isRegionHealth Edge Class");
            String[] isRegionHealthProps = {"region"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isRegionHealth",isRegionHealthProps);

            logger.debug("Create isAgent Edge Class");
            String[] isAgentProps = {"edge_id"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isAgent",null);

            logger.debug("Create isAgentHealth Edge Class");
            String[] isAgentHealthProps = {"region", "io/cresco/agent"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isAgentHealth",isAgentHealthProps);

            logger.debug("Create isPlugin Edge Class");
            String[] isPluginProps = {"edge_id"}; //Property names
            //createEdgeClass("isPlugin",isPluginProps);
            createEdgeClass("isPlugin",null);

            logger.debug("Create isPluginHealth Edge Class");
            String[] isPluginHealthProps = {"region", "io/cresco/agent","agentcontroller"}; //Property names
            //createEdgeClass("isPlugin",isPluginProps);
            createEdgeClass("isPluginHealth",isPluginHealthProps);

            logger.debug("Create isReachable Edge Class");
            String[] isReachableProps = {"src_agent", "dst_agent"}; //Property names
            //createEdgeClass("isConnected",isConnectedProps);
            createEdgeClass("isReachable",null);

        }
        catch(Exception ex)
        {
            logger.debug("Base initCrescoDB Error: " + ex.toString());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.debug("Base initCrescoDB Error: " + errors.toString());

        }
    }

    //Class Create Functions
    boolean createVertexIndex(String className, String indexName, boolean isUnique) {
        boolean wasCreated = false;
        try
        {
            OrientGraphNoTx txGraph = factory.getNoTx();
            //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
            OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

            if (schema.existsClass(className))
            {
                OClass vt = txGraph.getVertexType(className);
                //OClass vt = txGraph.createVertexType(className);
                //vt.createProperty(indexName, OType.STRING);

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
            logger.debug("DBEngine : createVertexIndex : Error " + ex.toString());
        }

        return wasCreated;
    }

    boolean createVertexIndex(String className, String[] props, String indexName, boolean isUnique) {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = txGraph.getRawGraph().getMetadata().getSchema();

        if (schema.existsClass(className))
        {
            OClass vt = txGraph.getVertexType(className);
            //OClass vt = txGraph.createVertexType(className);
            for(String prop : props)
            {
                vt.createProperty(prop, OType.STRING);
            }
            if(isUnique)
            {
                vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.UNIQUE, props);
            }
            else
            {
                vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.NOTUNIQUE, props);
            }

            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
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

    //DB IO Functions
    //dbIO functions
    public boolean setDBImport(String exportData) {
        boolean isImported = false;
        try {

            byte[] exportDataRawCompressed = DatatypeConverter.parseBase64Binary(exportData);
            InputStream iss = new ByteArrayInputStream(exportDataRawCompressed);
            //uncompress
            InputStream is = new GZIPInputStream(iss);

            boolean createdDb = false;
            if(db == null) {
                ODatabaseDocumentTx db = pool.acquire();
                createdDb = true;
            }

            DBImport dbImport = new DBImport(controllerEngine, is, this,db);
            isImported = dbImport.importDump();
            if(createdDb) {
                db.close();
            }


        }
        catch(Exception ex) {
            logger.error("setDBImport  " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
            ex.printStackTrace();
            logger.info("export data : [" + exportData + "]");
        }
        //export.exportDatabase();
        //export.close();
        //database.close();
        return isImported;
    }

    public boolean setDBImportNative(String exportData) {
        boolean isImported = false;
        try {

            OCommandOutputListener listener = new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                    // System.out.print(iText);
                    logger.info(iText);
                }
            };
            //Not sure what this does, but is needed to dump database.
            //ODatabaseRecordThreadLocal.INSTANCE.set(db);
            //create location for output stream

            InputStream is = new ByteArrayInputStream(exportData.getBytes(StandardCharsets.UTF_8));

            //if(db == null) {
                ODatabaseDocumentTx db = pool.acquire();
            //}

            ODatabaseImport dbImport = new ODatabaseImport(db, is, listener);
            //operation
            dbImport.setMerge(true);
            dbImport.setDeleteRIDMapping(true);
            dbImport.setMigrateLinks(true);
            //dbImport.setRebuildIndexes(true);

            //filter export
            dbImport.setIncludeInfo(false);
            dbImport.setIncludeClusterDefinitions(false);
            dbImport.setIncludeSchema(false);
            dbImport.setIncludeIndexDefinitions(false);
            dbImport.setIncludeManualIndexes(false);
            dbImport.setIncludeSecurity(true);

            dbImport.importDatabase();
            dbImport.close();
            isImported = true;
            //db.close();


        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
            logger.error(ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }
        //export.exportDatabase();
        //export.close();
        //database.close();
        return isImported;
    }

    public String getDBExport() {
        String exportString = null;
        Boolean createDB = false;
        try {

            Set<String> crescoDbClasses = new HashSet<String>();
            crescoDbClasses.add("rnode".toUpperCase());
            crescoDbClasses.add("anode".toUpperCase());
            crescoDbClasses.add("pnode".toUpperCase());
            //crescoDbClasses.add("resourcenode".toUpperCase());
            //crescoDbClasses.add("inode".toUpperCase());
            crescoDbClasses.add("isagent".toUpperCase());
            crescoDbClasses.add("isplugin".toUpperCase());
            //crescoDbClasses.add("isconnected".toUpperCase());
            //crescoDbClasses.add("isresource".toUpperCase());
            //crescoDbClasses.add("isreachable".toUpperCase());
            //crescoDbClasses.add("isassigned".toUpperCase());
            //System.out.println(crescoDbClasses);


            OCommandOutputListener listener = new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                    // System.out.print(iText);
                }
            };
            //Not sure what this does, but is needed to dump database.


            //create location for output stream
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            //ODatabaseDocumentTx db = null;
            //db = factory.getDatabase();

            //logger.error("setDBExport() Created Instances " + pool.getCreatedInstances());
            //logger.error("setDBExport() Max Partition Size " + pool.getMaxPartitonSize());

            if(db == null) {
                //ODatabaseDocumentTx db = pool.acquire();
                createDB = true;
                db = pool.acquire();
            }

            ODatabaseRecordThreadLocal.INSTANCE.set(db);
            ODatabaseExport export = new ODatabaseExport(db, os, listener);
            //filter export


            export.setIncludeInfo(false);
            export.setIncludeClusterDefinitions(false);
            export.setIncludeSchema(false);

            export.setIncludeIndexDefinitions(false);
            export.setIncludeManualIndexes(false);

            export.setIncludeSecurity(false);
            //include classes
            export.setIncludeClasses(crescoDbClasses);


            export.exportDatabase();

            String exportStringRaw = new String(os.toByteArray(),"UTF-8");

            logger.trace("[" + exportString + "]");

            export.close();

            //Now Compress and Encode
            exportString = DatatypeConverter.printBase64Binary(stringCompress(exportStringRaw));
            //byte[] message = "hello world".getBytes("UTF-8");
            //String encoded = DatatypeConverter.printBase64Binary(message);
            //byte[] decoded = DatatypeConverter.parseBase64Binary(encoded);
            if(createDB) {
                db.close();
                db = null;
            }

        }
        catch(Exception ex) {
            logger.error("getDBExport()  " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        //export.exportDatabase();
        //export.close();
        //database.close();
        return exportString;
    }

    public String stringUncompress(String str) {
        String uncompressedString = null;
        try {

            byte[] exportDataRawCompressed = DatatypeConverter.parseBase64Binary(str);
            InputStream iss = new ByteArrayInputStream(exportDataRawCompressed);
            //uncompress
            InputStream is = new GZIPInputStream(iss);
            uncompressedString = new Scanner(is,"UTF-8").useDelimiter("\\A").next();
        }
        catch(Exception ex) {
            logger.error("uncompressParam " + ex.getMessage());
        }
        return uncompressedString;
    }

    public byte[] stringCompress(String str) {
        byte[] dataToCompress = str.getBytes(StandardCharsets.UTF_8);
        byte[] compressedData = null;
        try
        {
            ByteArrayOutputStream byteStream =
                    new ByteArrayOutputStream(dataToCompress.length);
            try
            {
                GZIPOutputStream zipStream =
                        new GZIPOutputStream(byteStream);
                try
                {
                    zipStream.write(dataToCompress);
                }
                finally
                {
                    zipStream.close();
                }
            }
            finally
            {
                byteStream.close();
            }

            compressedData = byteStream.toByteArray();

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return compressedData;
    }


}
