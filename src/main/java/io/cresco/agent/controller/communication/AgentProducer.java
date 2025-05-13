package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.JMSException; // Ensure this is imported
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit; // Import TimeUnit

public class AgentProducer {
    private Map<String, ActiveProducerWorker> producerWorkers;
    // producerWorkersInProcess is not strictly needed if worker creation itself is robust.
    // If used, ensure its synchronization is correctly managed with producerWorkers.
    // For this revision, focusing on making worker get/create atomic.

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private String baseURI; // Base URI for creating workers
    private Timer timer; // Timer for cleaning up inactive workers
    private ActiveClient activeClient; // Reference to ActiveClient for connection status checks

    private final int MAX_SEND_RETRIES_CONFIG; // Renamed to avoid conflict if attempt becomes effectively final
    private final long INITIAL_RETRY_DELAY_MS;
    private final long WORKER_CLEANUP_INTERVAL_MS;
    private final long WORKER_CLEANUP_DELAY_MS;


    private class ClearProducerTask extends TimerTask {
        public void run() {
            logger.trace("Running ClearProducerTask cleanup...");
            List<String> keysToRemove = new ArrayList<>();
            synchronized (producerWorkers) { // Synchronize access for iteration
                for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet()) {
                    ActiveProducerWorker apw = entry.getValue();
                    logger.trace("Checking worker [{}] for queue [{}], current isActive flag: {}", apw.toString(), entry.getKey(), apw.isActive);

                    if (apw.isActive) {
                        apw.isActive = false; // Reset flag for next cycle, worker stays if it was active
                        logger.trace("Worker for queue [{}] was active, resetting flag for next check.", entry.getKey());
                    } else {
                        // Worker was not marked active in the last cycle, implies it wasn't used or failed.
                        logger.info("Worker for queue [{}] was not active in the last cycle. Shutting it down.", entry.getKey());
                        if (apw.shutdown()) { // shutdown() now returns boolean
                            keysToRemove.add(entry.getKey());
                        } else {
                            logger.warn("Failed to cleanly shutdown inactive worker for queue [{}]. Will still remove from map.", entry.getKey());
                            keysToRemove.add(entry.getKey()); // Remove even if shutdown had issues to prevent using a broken worker
                        }
                    }
                }
                // Remove outside the loop to avoid ConcurrentModificationException
                for (String key : keysToRemove) {
                    producerWorkers.remove(key);
                    logger.info("Removed inactive worker for queue [{}] from producerWorkers map.", key);
                }
            }
            logger.trace("ClearProducerTask cleanup finished. Active workers: {}", producerWorkers.size());
        }
    }

    public AgentProducer(ControllerEngine controllerEngine, String URI, ActiveClient activeClient) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AgentProducer.class.getName(), CLogger.Level.Info);
        this.activeClient = activeClient;
        this.baseURI = URI;

        this.MAX_SEND_RETRIES_CONFIG = plugin.getConfig().getIntegerParam("agentproducer_max_send_retries", 3);
        this.INITIAL_RETRY_DELAY_MS = plugin.getConfig().getLongParam("agentproducer_initial_retry_delay", 500L);
        this.WORKER_CLEANUP_INTERVAL_MS = plugin.getConfig().getLongParam("agentproducer_worker_cleanup_interval", 30000L);
        this.WORKER_CLEANUP_DELAY_MS = plugin.getConfig().getLongParam("agentproducer_worker_cleanup_delay", 15000L);


        try {
            producerWorkers = new ConcurrentHashMap<>();

            timer = new Timer("AgentProducerCleanupTimer", true); // Daemon timer
            timer.scheduleAtFixedRate(new ClearProducerTask(), WORKER_CLEANUP_DELAY_MS, WORKER_CLEANUP_INTERVAL_MS);
            logger.info("AgentProducer initialized for base URI [{}] with worker cleanup every {} ms.", baseURI, WORKER_CLEANUP_INTERVAL_MS);
        } catch (Exception ex) {
            logger.error("AgentProducer Constructor Exception for URI [{}]: {}", baseURI, ex.getMessage(), ex);
            throw new RuntimeException("Failed to initialize AgentProducer", ex);
        }
    }

    public void shutdown() {
        logger.info("Shutting down AgentProducer for base URI [{}]...", baseURI);
        if (timer != null) {
            timer.cancel();
            logger.debug("Cleanup timer cancelled.");
        }

        List<String> keys = new ArrayList<>(producerWorkers.keySet()); // Avoid CME
        logger.info("Shutting down {} active producer workers...", keys.size());
        for (String key : keys) {
            ActiveProducerWorker worker = producerWorkers.remove(key); // Remove while shutting down
            if (worker != null) {
                logger.debug("Shutting down worker for queue [{}]", key);
                worker.shutdown();
            }
        }
        producerWorkers.clear();
        logger.info("AgentProducer shutdown complete for base URI [{}]", baseURI);
    }

    public void invalidateWorkersForURI(String failedUri) {
        if (!this.baseURI.equals(failedUri)) {
            logger.debug("Ignoring invalidateWorkersForURI call for URI [{}], this producer manages base URI [{}]", failedUri, this.baseURI);
            return;
        }

        logger.warn("Invalidating ALL producer workers due to connection failure on base URI [{}]", failedUri);
        synchronized (producerWorkers) { // Synchronize for safe iteration and modification
            List<String> keysToRemove = new ArrayList<>(producerWorkers.keySet());
            for (String key : keysToRemove) {
                ActiveProducerWorker worker = producerWorkers.get(key); // Get before removing
                if (worker != null) {
                    // We only need to check if the worker's specific URI matches the failed one.
                    // However, if the baseURI failed, all workers using it are implicitly invalid.
                    logger.info("Shutting down and removing worker for destination [{}] due to connection failure on base URI [{}]", key, failedUri);
                    worker.shutdown();
                    producerWorkers.remove(key); // Explicitly remove after shutdown
                }
            }
        }
        logger.info("Finished invalidating ALL workers for base URI [{}]", failedUri);
    }

    public boolean sendMessage(MsgEvent sm) {
        boolean isSent = false;
        long currentRetryDelay = INITIAL_RETRY_DELAY_MS;

        String dstPath = sm.getForwardDst();
        if (dstPath == null) {
            logger.error("sendMessage Error: Destination path (dstPath) is null. Message: {}", sm.getParams());
            return false; // Cannot proceed without a destination
        }

        for (int currentAttempt = 1; currentAttempt <= MAX_SEND_RETRIES_CONFIG; currentAttempt++) {
            ActiveProducerWorker apw = null;
            // Make attempt effectively final for use in lambda
            final int effectivelyFinalAttempt = currentAttempt;

            try {
                // Check overall connection health for the base URI first
                if (!activeClient.isConnectionActive(this.baseURI)) {
                    logger.warn("Connection to base URI [{}] is not active. Send attempt {}/{} for [{}] will likely fail worker creation/usage.",
                            this.baseURI, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG, dstPath);
                }

                // Get or create worker
                final String workerKey = dstPath;
                apw = producerWorkers.computeIfAbsent(workerKey, key -> {
                    // Variables from the enclosing scope used in a lambda must be final or effectively final.
                    // 'attempt' and 'MAX_SEND_RETRIES_CONFIG' can be used if they are not reassigned
                    // or if their values from the specific iteration are passed correctly.
                    // Here, using effectivelyFinalAttempt and MAX_SEND_RETRIES_CONFIG (which is final by declaration)
                    logger.info("No existing worker for [{}]. Attempting to create new worker (Overall Send Attempt {}/{})", key, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG);
                    try {
                        ActiveProducerWorker newWorker = new ActiveProducerWorker(controllerEngine, key, baseURI);
                        if (newWorker.isActive) {
                            logger.info("Successfully created and cached new worker for [{}]", key);
                            return newWorker;
                        } else {
                            logger.error("Failed to initialize new ActiveProducerWorker for [{}]. Returning null for computeIfAbsent.", key);
                            return null;
                        }
                    } catch (Exception e) {
                        logger.error("Exception during ActiveProducerWorker creation for key [{}]: {}", key, e.getMessage(), e);
                        return null;
                    }
                });


                if (apw == null) {
                    logger.error("Failed to get or create a valid worker for destination [{}] (Attempt {}/{}) after computeIfAbsent.", dstPath, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG);
                } else {
                    apw.isActive = true;

                    if (sm.hasFiles()) {
                        new Thread(new ActiveProducerWorkerData(controllerEngine, apw.getTXQueueName(), apw.getConnectionURI(), sm)).start();
                        isSent = true;
                        logger.debug("File message for [{}] queued for async sending (Attempt {}/{})", dstPath, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG);
                        break;
                    } else {
                        try {
                            apw.sendMessage(sm);
                            isSent = true;
                            logger.debug("Message to [{}] sent successfully (Attempt {}/{})", dstPath, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG);
                            break;
                        } catch (JMSException jmse) {
                            logger.error("JMSException during sendMessage via worker for [{}] (Attempt {}/{}): {}", dstPath, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG, jmse.getMessage());
                            synchronized (producerWorkers) {
                                if (producerWorkers.get(workerKey) == apw) {
                                    logger.warn("Removing failed worker for [{}] from map due to JMSException.", workerKey);
                                    apw.shutdown();
                                    producerWorkers.remove(workerKey);
                                }
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                logger.error("Unexpected Exception in AgentProducer.sendMessage for [{}] (Attempt {}/{}): {}", dstPath, effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG, ex.getMessage(), ex);
                if (apw != null) {
                    synchronized (producerWorkers) {
                        if (producerWorkers.get(dstPath) == apw) {
                            logger.warn("Removing worker for [{}] due to unexpected exception.", dstPath);
                            apw.shutdown();
                            producerWorkers.remove(dstPath);
                        }
                    }
                }
            }

            if (isSent) {
                break;
            }

            if (effectivelyFinalAttempt < MAX_SEND_RETRIES_CONFIG) {
                try {
                    logger.warn("Send attempt {}/{} for [{}] failed. Retrying in {} ms...", effectivelyFinalAttempt, MAX_SEND_RETRIES_CONFIG, dstPath, currentRetryDelay);
                    Thread.sleep(currentRetryDelay);
                    currentRetryDelay = Math.min(currentRetryDelay * 2, 5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Retry delay interrupted for destination [{}]", dstPath);
                    break;
                }
            }
        }

        if (!isSent) {
            logger.error("Failed to send message to destination [{}] after {} attempts.", dstPath, MAX_SEND_RETRIES_CONFIG);
        }
        return isSent;
    }
}