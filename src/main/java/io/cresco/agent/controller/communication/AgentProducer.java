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
import java.util.concurrent.atomic.AtomicBoolean;

public class AgentProducer {
    private Map<String, ActiveProducerWorker> producerWorkers;
    private Map<String, Long> producerWorkersInProcess;
    private AtomicBoolean lockWIP = new AtomicBoolean(); // Consider using ReentrantLock for more complex scenarios

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private String URI; // Base URI for creating workers
    private Timer timer; // Timer for cleaning up inactive workers
    private ActiveClient activeClient; // Reference to ActiveClient for connection status checks

    private class ClearProducerTask extends TimerTask {
        public void run() {
            logger.trace("Running ClearProducerTask cleanup...");
            List<String> keysToRemove = new ArrayList<>();
            synchronized (producerWorkers) { // Synchronize access
                for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet()) {
                    ActiveProducerWorker apw = entry.getValue();
                    logger.trace("Checking worker [{}] isActive: {}", entry.getKey(), apw.isActive);

                    if (apw.isActive) {
                        apw.isActive = false; // Reset flag for next cycle
                    } else {
                        // Worker was not used in the last cycle, shut it down
                        logger.info("Shutting down inactive worker [{}]", entry.getKey());
                        if (apw.shutdown()) {
                            keysToRemove.add(entry.getKey());
                        } else {
                            logger.warn("Failed to shutdown inactive worker [{}] cleanly.", entry.getKey());
                            // Still remove it from the map to avoid using a potentially broken worker
                            keysToRemove.add(entry.getKey());
                        }
                    }
                }
                // Remove outside the loop to avoid ConcurrentModificationException
                for (String key : keysToRemove) {
                    producerWorkers.remove(key);
                    logger.info("Removed inactive worker [{}] from map.", key);
                }
            }
            logger.trace("ClearProducerTask cleanup finished.");
        }
    }

    // Modified constructor to accept ActiveClient
    public AgentProducer(ControllerEngine controllerEngine, String URI, ActiveClient activeClient) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AgentProducer.class.getName(), CLogger.Level.Info);
        this.activeClient = activeClient; // Store reference
        this.URI = URI; // Store the base URI

        try {
            producerWorkers = new ConcurrentHashMap<>(); // Use ConcurrentHashMap for better concurrency
            producerWorkersInProcess = Collections.synchronizedMap(new HashMap<>()); // Keep synchronized map for WIP tracking

            timer = new Timer("AgentProducerCleanupTimer", true); // Daemon timer
            // Schedule cleanup task (e.g., every 30 seconds after an initial 15-second delay)
            timer.scheduleAtFixedRate(new ClearProducerTask(), 15000, 30000);
            logger.info("AgentProducer initialized for URI [{}]", URI);
        } catch (Exception ex) {
            logger.error("AgentProducer Constructor Exception for URI [{}]: {}", URI, ex.getMessage(), ex);
            // Rethrow or handle initialization failure appropriately
            throw new RuntimeException("Failed to initialize AgentProducer", ex);
        }
    }

    public void shutdown() {
        logger.info("Shutting down AgentProducer for URI [{}]...", URI);
        if (timer != null) {
            timer.cancel();
            logger.debug("Cleanup timer cancelled.");
        }
        // Shutdown all active workers
        List<String> keys = new ArrayList<>();
        synchronized (producerWorkers) {
            keys.addAll(producerWorkers.keySet());
        }
        logger.info("Shutting down {} active producer workers...", keys.size());
        for (String key : keys) {
            ActiveProducerWorker worker = null;
            synchronized(producerWorkers) {
                worker = producerWorkers.remove(key); // Remove while shutting down
            }
            if(worker != null) {
                logger.debug("Shutting down worker [{}]", key);
                worker.shutdown();
            }
        }
        producerWorkers.clear(); // Ensure map is empty
        producerWorkersInProcess.clear(); // Clear WIP map
        logger.info("AgentProducer shutdown complete for URI [{}]", URI);
    }

    // --- NEW METHOD: Called by ActiveClient on connection failure ---
    public void invalidateWorkersForURI(String failedUri) {
        // Check if the failed URI matches the URI this producer manages
        // This check might be overly simplistic if multiple URIs are possible,
        // but based on current structure, AgentProducer seems tied to one URI.
        if (!this.URI.equals(failedUri)) {
            logger.debug("Ignoring invalidateWorkersForURI call for URI [{}], this producer manages URI [{}]", failedUri, this.URI);
            return;
        }

        logger.warn("Invalidating ALL producer workers due to connection failure on URI [{}]", failedUri);
        List<String> keysToRemove = new ArrayList<>();
        synchronized (producerWorkers) { // Ensure thread-safe access
            keysToRemove.addAll(producerWorkers.keySet()); // Get all keys
            for (String key : keysToRemove) {
                ActiveProducerWorker worker = producerWorkers.get(key);
                if (worker != null) {
                    logger.info("Shutting down and removing worker for destination [{}] due to connection failure on URI [{}]", key, failedUri);
                    worker.shutdown();
                }
            }
            producerWorkers.clear(); // Clear the entire map as the connection is bad
        }
        logger.info("Finished invalidating ALL workers for URI [{}]", failedUri);
    }

    // Modified sendMessage with retry logic and connection checking
    public boolean sendMessage(MsgEvent sm) {
        boolean isSent = false;
        int maxRetries = 3; // Max attempts to send the message
        long retryDelay = 500; // Initial delay in ms before retrying

        String dstPath = sm.getForwardDst();
        if (dstPath == null) {
            logger.error("sendMessage Error: Destination path (dstPath) is null. Message: {}", sm.getParams());
            return false; // Cannot proceed without a destination
        }

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            ActiveProducerWorker apw = null;
            boolean workerIsValid = false;

            try {
                // --- Phase 1: Get or Create Worker ---
                synchronized (producerWorkers) {
                    if (producerWorkers.containsKey(dstPath)) {
                        apw = producerWorkers.get(dstPath);
                        // Verify connection health *before* using the worker
                        if (apw != null && activeClient.isConnectionActive(apw.getURI())) {
                            workerIsValid = true;
                            logger.trace("Found valid existing worker for [{}]", dstPath);
                        } else {
                            logger.warn("Worker found for [{}], but connection to URI [{}] is inactive or worker is null. Invalidating.", dstPath, (apw != null ? apw.getURI() : "N/A"));
                            if (apw != null) apw.shutdown();
                            producerWorkers.remove(dstPath);
                            apw = null; // Force recreation attempt
                        }
                    }
                } // End synchronized block for reading map

                // If no valid worker found, attempt to create one
                if (apw == null) {
                    logger.info("Attempting to create new worker for destination [{}] (Attempt {}/{})", dstPath, attempt, maxRetries);
                    // Use a finer-grained lock for creation if needed, or rely on ConcurrentHashMap's nature
                    // For simplicity, using the main map lock here. Consider separate lock for WIP if contention is high.
                    synchronized (producerWorkers) {
                        // Double-check if another thread created it while waiting
                        if (producerWorkers.containsKey(dstPath)) {
                            apw = producerWorkers.get(dstPath);
                            if (apw != null && activeClient.isConnectionActive(apw.getURI())) {
                                workerIsValid = true; // Use the one created by another thread
                                logger.info("Worker for [{}] created by another thread, using it.", dstPath);
                            } else {
                                logger.warn("Worker for [{}] created by another thread is invalid. Removing.", dstPath);
                                if (apw != null) apw.shutdown();
                                producerWorkers.remove(dstPath);
                                apw = null; // Will attempt creation below
                            }
                        }

                        // If still no valid worker, proceed with creation
                        if (apw == null) {
                            try {
                                apw = new ActiveProducerWorker(controllerEngine, dstPath, URI);
                                if (apw.isActive) { // isInit() should set isActive on success
                                    producerWorkers.put(dstPath, apw);
                                    workerIsValid = true;
                                    logger.info("Successfully created and added new worker for [{}]", dstPath);
                                } else {
                                    logger.error("Failed to initialize new ActiveProducerWorker for [{}].", dstPath);
                                    // apw remains null
                                }
                            } catch (Exception createEx) {
                                logger.error("Exception creating ActiveProducerWorker for [{}]: {}", dstPath, createEx.getMessage(), createEx);
                                // apw remains null
                            }
                        }
                    } // End synchronized block for creating worker
                }

                // --- Phase 2: Send Message using the Worker ---
                if (workerIsValid && apw != null) {
                    apw.isActive = true; // Mark as active for cleanup timer

                    if (sm.hasFiles()) {
                        // File transfers are handled asynchronously
                        new Thread(new ActiveProducerWorkerData(controllerEngine, apw.getTXQueueName(), apw.getURI(), sm)).start();
                        isSent = true;
                        logger.debug("File message queued for async sending via worker [{}]", dstPath);
                        break; // SUCCESS - Exit retry loop
                    } else {
                        // Send regular message synchronously within the loop
                        try {
                            // sendMessage now throws JMSException on failure
                            apw.sendMessage(sm);
                            isSent = true;
                            logger.debug("Message sent successfully via worker [{}]", dstPath);
                            break; // SUCCESS - Exit retry loop
                        } catch (JMSException jmse) {
                            logger.error("JMSException during sendMessage via worker [{}] (Attempt {}/{}): {}", dstPath, attempt, maxRetries, jmse.getMessage());
                            // Invalidate the worker that failed
                            synchronized (producerWorkers) {
                                if (producerWorkers.get(dstPath) == apw) { // Ensure it's the same instance
                                    logger.warn("Invalidating worker [{}] due to JMSException during send.", dstPath);
                                    apw.shutdown();
                                    producerWorkers.remove(dstPath);
                                }
                            }
                            // Let the loop retry (will attempt to create a new worker)
                        }
                    }
                } else {
                    logger.error("Failed to obtain/create a valid worker for destination [{}] (Attempt {}/{})", dstPath, attempt, maxRetries);
                    // Let the loop retry
                }

            } catch (Exception ex) {
                // Catch general exceptions during the get/create/send process
                logger.error("Unexpected Exception in AgentProducer sendMessage for [{}] (Attempt {}/{}): {}", dstPath, attempt, maxRetries, ex.getMessage(), ex);
                // Invalidate worker if a general exception occurred, as its state is uncertain
                if (apw != null) {
                    synchronized (producerWorkers) {
                        if(producerWorkers.get(dstPath) == apw) {
                            logger.warn("Invalidating worker [{}] due to general Exception.", dstPath);
                            apw.shutdown();
                            producerWorkers.remove(dstPath);
                        }
                    }
                }
                // Let the loop retry
            }

            // If not sent and more retries are left, wait before retrying
            if (!isSent && attempt < maxRetries) {
                try {
                    logger.warn("Send attempt {}/{} failed for [{}]. Retrying in {} ms...", attempt, maxRetries, dstPath, retryDelay);
                    Thread.sleep(retryDelay);
                    retryDelay *= 2; // Optional: Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Retry delay interrupted for destination [{}]", dstPath);
                    break; // Exit loop if interrupted
                }
            }
        } // End retry loop

        if (!isSent) {
            logger.error("Failed to send message to destination [{}] after {} attempts.", dstPath, maxRetries);
            // Consider adding further failure handling here (e.g., notify sender, log to DB)
        }

        return isSent;
    }
}
