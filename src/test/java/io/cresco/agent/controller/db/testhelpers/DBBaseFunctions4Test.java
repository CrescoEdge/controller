package io.cresco.agent.controller.db.testhelpers;

import com.google.gson.Gson;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import io.cresco.agent.controller.db.DBBaseFunctions;

public class DBBaseFunctions4Test extends DBBaseFunctions {
    private CLogger4Test logger;
    private OrientGraphFactory factory;
    private ODatabaseDocumentTx db;
    private int retryCount;

    public DBBaseFunctions4Test(DBEngine4Test dbe){
        this.logger = new CLogger4Test("DBBaseFunctions4Test","DBBaseFunctions4Test", CLogger4Test.Level.Trace);
        this.factory = dbe.factory;
        this.db = dbe.db;
        this.retryCount = dbe.retryCount;
    }



}
