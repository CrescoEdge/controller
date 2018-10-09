package io.cresco.agent.controller.db;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerResourceConfiguration;
import com.orientechnologies.orient.server.config.OServerSecurityConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class DBEngine {

	public OrientGraphFactory factory;
	public ODatabaseDocumentTx db;
	private PluginBuilder plugin;
    private CLogger logger;
    //public OrientGraphFactory factory;
    public OPartitionedDatabasePool pool;
    private int retryCount;
    private OServer server;

    public DBEngine(PluginBuilder plugin) {

        this.plugin = plugin;
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

    public void shutdown() {


        try {

            if(factory != null) {
                factory.close();
            }
            if(db != null) {
                db.close();
            }
            if(pool != null) {
                pool.close();
            }

            if(server != null) {
                server.shutdown();
            }


        } catch(Exception ex) {
            logger.error("shutdown() : " + ex.getMessage());
        }

    }

    private void setServer() {
        try {

            //public OServerResourceConfiguration(final String iName, final String iRoles) {
            //public OServerUserConfiguration(final String iName, final String iPassword, final String iResources) {


            server = OServerMain.create();
            OServerConfiguration cfg = new OServerConfiguration();

            OServerResourceConfiguration res = new OServerResourceConfiguration("root","admin");
            List<OServerResourceConfiguration> resList = new ArrayList<>();
            resList.add(res);

            OServerUserConfiguration user = new OServerUserConfiguration("root",UUID.randomUUID().toString(),"admin");
            List<OServerUserConfiguration> userList = new ArrayList<>();
            userList.add(user);

            OServerSecurityConfiguration sec = new OServerSecurityConfiguration();
            sec.resources = resList;
            sec.users = userList;

            cfg.security = sec;
            // FILL THE OServerConfiguration OBJECT

            //server.startup(cfg);
            //server.activate();

        } catch(Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error("setServer : " + ex.getMessage());
            logger.error(sw.toString());
        }
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
                        logger.error("setPool() : failed remove DB");
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

                setServer();


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
            logger.error(getStringFromError(ex));
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
            logger.error("dbExist() : " + getStringFromError(ex));
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
            logger.error("removeDB()" + getStringFromError(ex));
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
            logger.error("createDB() " + getStringFromError(ex));
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


    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

}
