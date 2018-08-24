package io.cresco.agent.controller.db;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;


public class DBEngine {

	protected OrientGraphFactory factory;
	protected ODatabaseDocumentTx db;
    protected ControllerEngine controllerEngine;
    protected PluginBuilder plugin;
    protected CLogger logger;
    //public OrientGraphFactory factory;
    public OPartitionedDatabasePool pool;
    protected int retryCount;

    //Added no-arg constructor for testing
    public DBEngine(){}

    public DBEngine(ControllerEngine controllerEngine) {

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(DBEngine.class.getName(),CLogger.Level.Info);

        //this.agentcontroller = agentcontroller;
        //logger = new CLogger(DBEngine.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);
        //this.factory = getFactory();

        //set config values
        //OGlobalConfiguration.PROFILER_ENABLED.setValue(Boolean.TRUE);

        setPool();
        /*
        String host = agentcontroller.getConfig().getStringParam("gdb_host");
        String username = agentcontroller.getConfig().getStringParam("gdb_username");
        String password = agentcontroller.getConfig().getStringParam("gdb_password");
        String dbname = agentcontroller.getConfig().getStringParam("gdb_dbname");

        String iURI = null;
        if ((host != null) && (username != null) && (password != null) && (dbname != null)) {
            iURI = "remote:" + host + "/" + dbname;

            pool = new OPartitionedDatabasePool(iURI, username,password).setAutoCreate(true);
            factory = new OrientGraphFactory(iURI, username, password, true);
        }
        else {
            //iURI = "memory:internalDb";
            //db = new ODatabaseDocumentTx(iURI).create();
            //pool = new OPartitionedDatabasePool(iURI, username,password).setAutoCreate(true);
            //factory = new OrientGraphFactory(iURI, username, password, true);
            db = new ODatabaseDocumentTx("memory:internalDb").create();

            factory = new OrientGraphFactory("memory:internalDb");

        }
        */
    }


    //upload
    private void setPool() {
        try {
            String host = plugin.getConfig().getStringParam("gdb_host");
            //String host = null;
            String username = plugin.getConfig().getStringParam("gdb_username");
            String password = plugin.getConfig().getStringParam("gdb_password");
            String dbname = plugin.getConfig().getStringParam("gdb_dbname");


            String iURI = null;
            if ((host != null) && (username != null) && (password != null) && (dbname != null)) {
                iURI = "remote:" + host + "/" + dbname;
                String adminIURI = "remote:" + host + "/";

                if(dbExist(iURI,username,password)) {
                    logger.debug("remove DB");
                    if (removeDB(iURI, username, password, dbname)) {
                        logger.debug("removed DB");
                    } else {
                        logger.error("failed remove DB");
                    }
                }
                if(createDB(iURI,username,password)) {
                    logger.debug("setPool() Create Database.");
                }
                else {
                    logger.error("setPool() Failed to Create Database!");
                }

                //pool = new OPartitionedDatabasePool(iURI, username,password).setAutoCreate(true);
                pool = new OPartitionedDatabasePool(iURI, username,password, 1, 1).setAutoCreate(true);
                //db = new ODatabaseDocumentTx(iURI).create();
                //TODO.. is this needed?
                db = new ODatabaseDocumentTx(iURI).open(username, password);

                factory = new OrientGraphFactory(iURI, username, password, true);

                //ODatabaseObjectTx database = new ODatabaseObjectTx("remote:localhost/demo");
                //database.open("writer", "writer");

            }
            else {
                //iURI = "memory:internalDb/" + dbname;
                //db = new ODatabaseDocumentTx(iURI).create();
                //pool = new OPartitionedDatabasePool(iURI, username,password).setAutoCreate(true);
                //factory = new OrientGraphFactory(iURI, username, password, true);

                db = new ODatabaseDocumentTx("memory:internalDb").create();
                //OGlobalConfiguration.dumpConfiguration(System.out);

                factory = new OrientGraphFactory("memory:internalDb");


            }

            //try and do something with db

            //pool = new OPartitionedDatabasePool(iURI, username,password).setAutoCreate(true);
            //factory = new OrientGraphFactory(iURI, username, password, pool);

        }
        catch(Exception ex) {
            logger.error("setPool : " + ex.getMessage());
            logger.error(controllerEngine.getStringFromError(ex));
        }

    }

    private boolean dbExist(String iURI, String username, String password) {
        boolean exist = false;
        try {
            OServerAdmin serverAdmin = new OServerAdmin(iURI).connect(username, password);
            if(serverAdmin.existsDatabase("plocal")) {
                exist = true;
            }
        }
        catch(Exception ex)
        {
            logger.error(controllerEngine.getStringFromError(ex));
        }
        return exist;
    }

    private boolean removeDB(String iURI, String username, String password, String dbname) {
        boolean exist = false;
        try {
            OServerAdmin serverAdmin = new OServerAdmin(iURI).connect(username, password);
            serverAdmin.dropDatabase(dbname);
            exist = true;
        }
        catch(Exception ex)
        {
            logger.error(controllerEngine.getStringFromError(ex));
        }
        return exist;
    }

    private boolean createDB(String iURI, String username, String password) {
        boolean exist = false;
        try {
            OServerAdmin serverAdmin = new OServerAdmin(iURI).connect(username, password);
            //serverAdmin.createDatabase(dbname, "graph", "plocal");
            //serverAdmin.createDatabase("graph", "plocal");
            //OServerAdmin serverAdmin = new OServerAdmin("remote:localhost/cresco").connect("root", "cody01");
            //OServerAdmin serverAdmin = new OServerAdmin("remote:127.0.0.1/cresco").connect("root", "cody01");
            serverAdmin.createDatabase("graph", "plocal");

            exist = true;
        }
        catch(Exception ex)
        {
            logger.error(controllerEngine.getStringFromError(ex));
        }
        return exist;
    }

    public boolean dbCheck(String connection_string, String username, String password) {

        Boolean isValid = false;
        try {
            OServerAdmin server = new OServerAdmin(connection_string).connect(username, password);
            if (!server.existsDatabase("plocal")) {
                server.createDatabase("graph", "plocal");
                isValid = true;
            }
            else {
                isValid = true;
            }
            server.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return isValid;
    }

    public boolean dropDBIfExists(String connection_string, String username, String password) {
        Boolean isValid = false;
        try {
            OServerAdmin server = new OServerAdmin(connection_string).connect(username, password);
            if (server.existsDatabase("plocal")) {
                server.dropDatabase("plocal");
                isValid = true;
            }
            server.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return isValid;
    }


    /*

    boolean createVertexIndex(String className, String indexName, boolean isUnique)
    {
        boolean wasCreated = false;
        try
        {
            OrientGraphNoTx txGraph = factory.getNoTx();
            //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
            OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

            if (schema.existsClass(className))
            {
                OClass vt = txGraph.getVertexType(className);
                //OClass vt = txGraph.createVertexType(className);
                vt.createProperty(indexName, OType.STRING);

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

    boolean createVertexIndex(String className, String[] props, String indexName, boolean isUnique)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

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

    boolean createVertexClass(String className, String[] props)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

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

    boolean createEdgeClass(String className, String[] props)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

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


    public String getNodeClass(String region, String agent, String agentcontroller)
    {
        try
        {
            if((region != null) && (agent == null) && (agentcontroller == null))
            {
                return "rNode";
            }
            else if((region != null) && (agent != null) && (agentcontroller == null))
            {
                return "aNode";
            }
            else if((region != null) && (agent != null) && (agentcontroller != null))
            {
                return "pNode";
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeClass: Error " + ex.toString());
        }
        return null;

    }

    public boolean updateEdge(String edge_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("ADDNODE RETRY : region=" + region + " agent=" + agent + " agentcontroller" + agentcontroller);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IupdateEdge(edge_id, params);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : updateEdge : Failed to update edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : updateEdge : Error " + ex.toString());
        }

        return isUpdated;
    }

    private boolean IupdateEdge(String edge_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            Edge edge = graph.getEdge(edge_id);
            if(edge != null)
            {
                for (Entry<String, String> entry : params.entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                graph.commit();
                isUpdated = true;
            }
            else
            {
                logger.debug("IupdateEdge: no edge found for edge_id=" + edge_id);
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
            logger.debug("IupdateEdge: thread_id: " + threadId + " Error " + ex.toString());
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

*/
}
