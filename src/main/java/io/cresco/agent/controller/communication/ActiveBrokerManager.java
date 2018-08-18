package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.netdiscovery.DiscoveryType;
import io.cresco.agent.controller.netdiscovery.TCPDiscoveryStatic;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

public class ActiveBrokerManager implements Runnable  {
	private PluginBuilder plugin;
	private CLogger logger;
	private Timer timer;
	private ControllerEngine controllerEngine;

	public ActiveBrokerManager(ControllerEngine controllerEngine) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveBrokerManager.class.getName(),CLogger.Level.Info);

		logger.debug("Active Broker Manger initialized");
		this.plugin = plugin;
		timer = new Timer();
		timer.scheduleAtFixedRate(new BrokerWatchDog(logger), 500, 15000);//remote
	}
	  
	public void shutdown() {
		logger.debug("Active Broker Manager shutdown initialized");
	}

	public void addBroker(String agentPath) {
		BrokeredAgent ba = controllerEngine.getBrokeredAgents().get(agentPath);
		//logger.error("addBroker: agentPath = " + agentPath + " status 0 = " + ba.brokerStatus.toString());
		if(ba.brokerStatus == BrokerStatusType.INIT) {
			//Fire up new thread.
			ba.setStarting();
		}
		/*
		logger.error("addBroker: agentPath = " + agentPath + " status 1 = " + ba.brokerStatus.toString());
		if(ba.brokerStatus == BrokerStatusType.STARTING) {
			ba.setActive();
		}
		logger.error("addBroker: agentPath = " + agentPath + " status 2 = " + ba.brokerStatus.toString());
		*/
	}
	public void run() {
		logger.info("Initialized");
		controllerEngine.setActiveBrokerManagerActive(true);
		while(controllerEngine.isActiveBrokerManagerActive()) {
			try {
				MsgEvent cb = controllerEngine.getIncomingCanidateBrokers().take();

				if(cb != null) {

					String agentIP = cb.getParam("dst_ip");
					if(!controllerEngine.isLocal(agentIP)) { //ignore local responses

						boolean addBroker = false;
						String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
						logger.trace("Trying to connect to: " + agentPath);
						//logger.trace(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;
	 		      
						BrokeredAgent ba = null;
						if(controllerEngine.getBrokeredAgents().containsKey(agentPath)) {
							ba = controllerEngine.getBrokeredAgents().get(agentPath);
							//add ip to possible list
							if(!ba.addressMap.containsKey(agentIP)) {
								ba.addressMap.put(agentIP,BrokerStatusType.INIT);
							}
							//reset status if needed
							if((ba.brokerStatus.equals(BrokerStatusType.FAILED) || (ba.brokerStatus.equals(BrokerStatusType.STOPPED)))) {
								ba.activeAddress = agentIP;
								ba.brokerStatus = BrokerStatusType.INIT;
								addBroker = true;
								logger.trace("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
							}
							logger.trace("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
						} else {
						    //This might not work everwhere
                            String cbrokerAddress = null;
                            String cbrokerValidatedAuthenication = null;
                            cbrokerAddress = cb.getParam("dst_ip");
                            cbrokerValidatedAuthenication = cb.getParam("validated_authenication");

							if(cbrokerValidatedAuthenication != null) {

								TCPDiscoveryStatic ds = new TCPDiscoveryStatic(controllerEngine);
								List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_static_region_timeout", 10000), cbrokerAddress, true);
                				for(MsgEvent cme : certDiscovery) {
                					if(cbrokerAddress.equals(cme.getParam("dst_ip"))) {
										String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
										ba = new BrokeredAgent(controllerEngine, agentPath, cbrokerAddress, tmpAuth[0], tmpAuth[1]);
										controllerEngine.getBrokeredAgents().put(agentPath, ba);
										addBroker = true;
										logger.trace("BA NEW ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
									}
								}

							}
						}
						//try and connect
						if(addBroker && !controllerEngine.isReachableAgent(agentPath)) {
                            addBroker(agentPath);
                            int count = 0;
                            //while(!this.agentcontroller.isReachableAgent(agentPath)) {

                            	/*
                            	MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.agentcontroller.getRegion(), this.agentcontroller.getAgent(), this.agentcontroller.getPluginID(), "Discovery request.");
								sme.setParam("src_region", this.agentcontroller.getRegion());
								sme.setParam("src_agent", this.agentcontroller.getAgent());
								String[] regionAgent = agentPath.split("_");
								sme.setParam("dst_region",regionAgent[0]);
								sme.setParam("dst_agent",regionAgent[1]);
								agentcontroller.sendMsgEvent(sme);
								*/

								logger.trace("Waiting on Broker : " + agentPath + " remote_ip: " + agentIP + " count:" + count);
								logger.trace("Status : " + ba.brokerStatus.toString() + " URI : " + ba.URI + " Address : " + ba.activeAddress);
								logger.trace("isReachable : " + controllerEngine.isReachableAgent(agentPath));
								Thread.sleep(1000);
                                count++;
                            //}
						}
						else {
                            logger.trace("Not Adding Broker : " + agentPath + " remote_ip: " + agentIP);
                        }
					}
		  			//Thread.sleep(500); //allow HM to catch up
			  	}
			} catch(InterruptedException ex) {

			}
			catch (Exception ex) {
				logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
			}
		}
		timer.cancel();
		logger.debug("Broker Manager has shutdown");
	}

	class BrokerWatchDog extends TimerTask {
		//private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);
        private CLogger logger;

        public BrokerWatchDog(CLogger logger) {
            this.logger = logger;
        }
		public void run() {
		    for (Entry<String, BrokeredAgent> entry : controllerEngine.getBrokeredAgents().entrySet()) {
				//logger.trace(entry.getKey() + "/" + entry.getValue());
				BrokeredAgent ba = entry.getValue();
				if(ba.brokerStatus == BrokerStatusType.FAILED) {
					//logger.trace("stopping agentPath: " + ba.agentPath);
		    		ba.setStop();
		    		logger.info("Cleared agentPath: " + ba.agentPath);

		    		if((controllerEngine.cstate.getGlobalControllerPath()) != null && (controllerEngine.cstate.getGlobalControllerPath().equals(ba.agentPath))) {
                        logger.info("Clearing Global Controller Path " +ba.agentPath);
                        //agentcontroller.setGlobalController(null,null);
						controllerEngine.cstate.setRegionalGlobalFailed("BrokerWatchDog: BrokerStatusType.FAILED : BrokerPath: " +ba.agentPath);
                    }

                    controllerEngine.getBrokeredAgents().remove(entry.getKey());//remove agent
				}
                logger.trace("Brokered Agents: " + ba.agentPath);
			}
		}
	}
}