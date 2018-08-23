package io.cresco.agent.controller.db.testhelpers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import io.cresco.agent.controller.db.DBInterface;
import io.cresco.library.utilities.CLogger;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DBInterface4Test extends DBInterface {

    private DBEngine4Test gde;
    public DBBaseFunctions4Test gdb;
    public DBApplicationFunctions4Test dba;
    private Gson gson;
    private Type type;
    private CLogger4Test logger = new CLogger4Test("DBInterface4Test","DBInterface4Test",CLogger4Test.Level.Trace);
    private boolean DBManagerIsActive = false;
    private ControllerEngine4Test controllerEngine;

    private Thread DBManagerThread;
    private BlockingQueue<String> importQueue;
    public DBInterface4Test(ODatabaseDocumentTx db) {
        this.controllerEngine = new ControllerEngine4Test(this);
        this.importQueue = new LinkedBlockingQueue<>();
        this.gde = new DBEngine4Test(db);
        this.gdb = new DBBaseFunctions4Test(gde);
        this.dba = new DBApplicationFunctions4Test(gde);
        this.gson = new Gson();
        this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        //DB manager

        this.DBManagerThread = new Thread(new DBManager4Test(importQueue,this));
        this.DBManagerThread.start();
        super.setLogger(controllerEngine.getPluginBuilder().);
    }

    public boolean getDBManagerIsActive(){
        return this.DBManagerIsActive;
    }

    public void setDBManagerIsActive(boolean isActive){
        this.DBManagerIsActive = isActive;
    }

}
