package io.cresco.agent.controller.db;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class DBManager implements Runnable  {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;
	private Timer timer;
	private BlockingQueue<String> importQueue;

	public DBManager(){}

	public DBManager controllerEngine(ControllerEngine toAdd){
		this.controllerEngine = toAdd;
		return this;
	}

	public DBManager pluginBuilder(PluginBuilder toAdd){
		this.plugin = toAdd;
		return this;
	}

	public DBManager logger(CLogger toAdd){
		this.logger = toAdd;
		return this;
	}

	public DBManager importQueue(BlockingQueue importQueue){
		this.importQueue = importQueue;
		return this;
	}


	public DBManager(ControllerEngine controllerEngine, BlockingQueue<String> importQueue) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DBManager.class.getName(),CLogger.Level.Info);


		//importQueue = new LinkedBlockingQueue<>();
		this.importQueue = importQueue;
		//this.logger = new CLogger(DBManager.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
		logger.debug("DB Manager initialized");
		//this.agentcontroller = agentcontroller;
		//timer = new Timer();
		//timer.scheduleAtFixedRate(new DBWatchDog(logger), 500, 15000);//remote
	}

	public void importRegionalDB(String importData) {

		importQueue.offer(importData);

	}

	public void shutdown() {
		logger.debug("DB Manager shutdown initialized");
	}

	private void processDBImports() {
		try {
			while (!importQueue.isEmpty()) {
				controllerEngine.getGDB().gdb.setDBImport(importQueue.take());
			}
		} catch(Exception ex) {
			logger.error("processDBImports() Error : " + ex.toString());
		}
	}

	public void run() {
		logger.info("Initialized");
		controllerEngine.setDBManagerActive(true);
		while(controllerEngine.isDBManagerActive()) {
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

	class DBWatchDog extends TimerTask {
		//private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);
        private CLogger logger;

        public DBWatchDog(CLogger logger) {
            this.logger = logger;
        }
		public void run() {
        	//Do Something
		}
	}
}