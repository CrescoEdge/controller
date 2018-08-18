package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class ActiveProducer {
    private Map<String, ActiveProducerWorker> producerWorkers;

    private Map<String, Long> producerWorkersInProcess;

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private String URI;
    private Timer timer;
    private String brokerUserNameAgent;
    private String brokerPasswordAgent;

    private class ClearProducerTask extends TimerTask {
        public void run() {

            for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet()) {
                ActiveProducerWorker apw = entry.getValue();
                logger.trace("ActiveProducerWorker = " + entry.getKey());

                if (apw.isActive) {
                    apw.isActive = false;
                } else {
                    if (apw.shutdown()) {
                        producerWorkers.remove(entry.getKey());
                    }
                }
            }
        }
    }

    public ActiveProducer(ControllerEngine controllerEngine, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ActiveProducer.class.getName(),CLogger.Level.Info);

        this.brokerUserNameAgent = brokerUserNameAgent;
        this.brokerPasswordAgent = brokerPasswordAgent;
        try {
            producerWorkers = new ConcurrentHashMap<>();
            producerWorkersInProcess = new HashMap<>();

            this.URI = URI;
            timer = new Timer();
            timer.scheduleAtFixedRate(new ClearProducerTask(), 5000, 5000);
            logger.debug("Producer initialized");
        } catch (Exception ex) {
            logger.error("Constructor " + ex.toString());
        }
    }

    public void shutdown() {
        for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet()) {
            entry.getValue().shutdown();
        }
        timer.cancel();
        logger.debug("Producer has shutdown");
    }

    public boolean sendMessage(MsgEvent sm) {
        boolean isSent = false;
        try {
            ActiveProducerWorker apw = null;
            String dstPath;
            if ((sm.getParam("dst_region") != null) && (sm.getParam("dst_agent") != null)) {
                dstPath = sm.getParam("dst_region") + "_" + sm.getParam("dst_agent");
            } else if (sm.getParam("dst_region") != null) {
                dstPath = sm.getParam("dst_region"); //send to broker for routing
            } else {
                return false;
            }

			/*
            String dstPath;
			if(PluginEngine.isRegionalController) {
				dstPath = sm.getParam("dst_region") + "_" + sm.getParam("dst_agent");
			} else {
				dstPath = sm.getParam("dst_region"); //send to broker for routing
			}
			*/


            if (producerWorkers.containsKey(dstPath)) {
                if (controllerEngine.isReachableAgent(dstPath)) {
                    apw = producerWorkers.get(dstPath);
                } else {
                    logger.error(dstPath + " is unreachable...");
                }
            } else {

                if (controllerEngine.isReachableAgent(dstPath)) {
                    boolean isInProcess;
                    synchronized (producerWorkersInProcess) {
                        isInProcess = producerWorkersInProcess.containsKey(dstPath);
                    }
                    if (isInProcess) {
                        while (isInProcess) {
                            logger.debug("ActiveProducerWorker waiting for " + dstPath);
                            synchronized (producerWorkersInProcess) {
                                isInProcess = producerWorkersInProcess.containsKey(dstPath);
                            }
                            if (!isInProcess)
                                apw = producerWorkers.get(dstPath);
                        }
                    } else {
                        synchronized (producerWorkersInProcess) {
                            producerWorkersInProcess.put(dstPath, System.currentTimeMillis());
                        }
                        if (!producerWorkers.containsKey(dstPath)) {
                            apw = new ActiveProducerWorker(controllerEngine, dstPath, URI, brokerUserNameAgent, brokerPasswordAgent);
                            if (apw.isActive) {
                                producerWorkers.put(dstPath, apw);
                            }
                        }
                        synchronized (producerWorkersInProcess) {
                            producerWorkersInProcess.remove(dstPath); //remove from that
                        }
                    }

                } else {
                    logger.trace(dstPath + " is unreachable...");
                }
            }
            if (apw != null) {
                apw.isActive = true;
                apw.sendMessage(sm);
                isSent = true;
            } else {
                logger.error("apw [" + apw.toString() + "] is null");
            }
        } catch (Exception ex) {
            logger.error("ActiveProducer : sendMessage Error " + ex.toString());
        }
        return isSent;
    }
}