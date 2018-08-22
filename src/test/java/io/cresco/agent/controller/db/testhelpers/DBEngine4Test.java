package io.cresco.agent.controller.db.testhelpers;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class DBEngine4Test {

    public OrientGraphFactory factory;
    public ODatabaseDocumentTx db;
    private CLogger4Test logger;
    public final int retryCount;

    public DBEngine4Test(ODatabaseDocumentTx db_to_use){
        this.logger = new CLogger4Test("DBEngine4Test",DBEngine4Test.class.getName(),CLogger4Test.Level.Trace);
        this.retryCount = 50;
        this.db = db_to_use;
        this.factory  = new OrientGraphFactory(db.getURL());
    }
}
