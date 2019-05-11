package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class AgentProducer {
    private Map<String, ActiveProducerWorker> producerWorkers;

    private Map<String, Long> producerWorkersInProcess;
    private AtomicBoolean lockWIP = new AtomicBoolean();

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private String URI;
    private Timer timer;

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

    public AgentProducer(ControllerEngine controllerEngine, String URI) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AgentProducer.class.getName(),CLogger.Level.Info);


        try {
            producerWorkers = new ConcurrentHashMap<>();
            //producerWorkersInProcess = new HashMap<>();
            producerWorkersInProcess = Collections.synchronizedMap(new HashMap<>());

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

        logger.debug("sendMessage() SEND MESSAGE: " + sm.printHeader());
        boolean isSent = false;
        try {
            ActiveProducerWorker apw = null;
            String dstPath = sm.getForwardDst();

            if(dstPath == null) {
                logger.error("sendMessage() DST PATH = NULL");
                return false;
            }

            if (producerWorkers.containsKey(dstPath)) {
                if (controllerEngine.isReachableAgent(dstPath)) {
                    apw = producerWorkers.get(dstPath);
                } else {
                    logger.error(dstPath + " is unreachable...");
                }
            } else {

                if (controllerEngine.isReachableAgent(dstPath)) {
                    boolean isInProcess;

                    synchronized (lockWIP) {
                        isInProcess = producerWorkersInProcess.containsKey(dstPath);
                    }
                    if (isInProcess) {
                        while (isInProcess) {
                            logger.debug("ActiveProducerWorker waiting for " + dstPath);
                            synchronized (lockWIP) {
                                isInProcess = producerWorkersInProcess.containsKey(dstPath);
                            }
                            if (!isInProcess)
                                apw = producerWorkers.get(dstPath);
                        }
                    } else {
                        synchronized (lockWIP) {
                            producerWorkersInProcess.put(dstPath, System.currentTimeMillis());
                        }
                        if (!producerWorkers.containsKey(dstPath)) {
                            //check for connection first, and if not found create one
                            apw = new ActiveProducerWorker(controllerEngine, dstPath, URI);
                            if (apw.isActive) {
                                producerWorkers.put(dstPath, apw);
                            }
                        }
                        synchronized (lockWIP) {
                            producerWorkersInProcess.remove(dstPath); //remove from that
                        }
                    }

                } else {
                    logger.trace(dstPath + " is unreachable...");
                }
            }
            if (apw != null) {
                apw.isActive = true;

                //Messages with file data must be addressed seperatly.
                if(sm.hasFiles()) {
                    //if message contains files spawn new thread
                    new Thread(new ActiveProducerWorkerData(controllerEngine,apw.getTXQueueName(),apw.getURI(),sm)).start();

                } else {
                    apw.sendMessage(sm);
                }
                isSent = true;
            } else {
                logger.error("apw is null");
            }
        } catch (Exception ex) {
            logger.error("ActiveProducer : sendMessage Error " + ex.toString());

            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());

        }
        return isSent;
    }
}