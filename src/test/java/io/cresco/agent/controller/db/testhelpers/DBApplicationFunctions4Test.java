package io.cresco.agent.controller.db.testhelpers;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import io.cresco.agent.controller.db.DBApplicationFunctions;

public class DBApplicationFunctions4Test extends DBApplicationFunctions {
    private CLogger4Test logger;
    private OrientGraphFactory factory;
    private int retryCount;

    public DBApplicationFunctions4Test(DBEngine4Test dbe){
        this.logger = new CLogger4Test("DBApplicationFunctions4Test","DBApplicationFunctions4Test",CLogger4Test.Level.Trace);
        this.retryCount = dbe.retryCount;
        this.factory = dbe.factory;
    }
}
