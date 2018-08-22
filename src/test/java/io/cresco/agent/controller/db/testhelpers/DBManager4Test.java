package io.cresco.agent.controller.db.testhelpers;

import io.cresco.agent.controller.db.DBManager;

import java.util.Timer;
import java.util.concurrent.BlockingQueue;

public class DBManager4Test extends DBManager {
    private CLogger4Test logger;
    private Timer timer;
    private BlockingQueue<String> importQueue;

    public DBManager4Test(BlockingQueue<String>importQueue){
        this.importQueue = importQueue;
        this.logger = new CLogger4Test("DBManager4Test","DBManager4Test", CLogger4Test.Level.Trace);
    }
}
