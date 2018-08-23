package io.cresco.agent.controller.db.testhelpers;

import io.cresco.agent.controller.db.DBManager;

import java.util.Timer;
import java.util.concurrent.BlockingQueue;

public class DBManager4Test extends DBManager {
    private CLogger4Test logger;
    private Timer timer;
    private BlockingQueue<String> importQueue;
    private DBInterface4Test gdb;

    public DBManager4Test(BlockingQueue<String>importQueue,DBInterface4Test gdb){
        this.importQueue = importQueue;
        this.logger = new CLogger4Test("DBManager4Test","DBManager4Test", CLogger4Test.Level.Trace);
        this.gdb = gdb;
    }
    private void processDBImports() {
        try {
            while (!importQueue.isEmpty()) {
                gdb.gdb.setDBImport(importQueue.take());
            }
        } catch(Exception ex) {
            logger.error("processDBImports() Error : " + ex.toString());
        }
    }
    @Override
    public void run() {
        logger.info("Initialized");
        //controllerEngine.setDBManagerActive(true);
        while(gdb.getDBManagerIsActive()) {
            try {

                processDBImports();
                Thread.sleep(1000);

            } catch (Exception ex) {
                logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
            }
        }
        //timer.cancel();
        logger.debug("Broker Manager has shutdown");
    }
}
