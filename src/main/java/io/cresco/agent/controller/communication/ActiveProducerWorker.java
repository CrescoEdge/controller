package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;

import jakarta.jms.*;
import java.util.UUID;

public class ActiveProducerWorker {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private String producerWorkerName;
	private CLogger logger;
	private ActiveMQSession sess; // The session managed by this worker

	private MessageProducer producer; // The producer managed by this worker
	private Gson gson;
	public volatile boolean isActive; // Flag used by AgentProducer cleanup task
	private String txQueueName; // Destination queue name (renamed for clarity)
	private Destination destination; // JMS Destination object

	private String connectionURI; // Connection URI associated with this worker (renamed for clarity)


	public ActiveProducerWorker(ControllerEngine controllerEngine, String txQueueName, String connectionURI) throws Exception { // Added throws Exception
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.connectionURI = connectionURI;
		this.txQueueName = txQueueName;
		this.producerWorkerName = txQueueName + "_" + UUID.randomUUID().toString(); // More descriptive name
		this.logger = plugin.getLogger(ActiveProducerWorker.class.getName() + "[" + this.producerWorkerName + "]", CLogger.Level.Info);
		this.gson = new Gson();

		if (!initializeJMSResources()) {
			// isActive is false by default, initializeJMSResources sets it true on success
			logger.error("Worker [{}] failed to initialize JMS resources during construction!", producerWorkerName);
			// Propagate failure to the caller (AgentProducer)
			throw new Exception("Failed to initialize ActiveProducerWorker for queue " + txQueueName);
		}
		logger.info("Worker [{}] constructed and initialized successfully for queue [{}].", producerWorkerName, txQueueName);
	}

	// Initializes or re-initializes JMS resources. Returns true on success, false on failure.
	private boolean initializeJMSResources() {
		logger.debug("Worker [{}] attempting JMS resource initialization...", producerWorkerName);
		try {
			// Cleanup existing resources first to prevent leaks if re-initializing
			shutdownJMSResources();

			// Create new resources
			logger.debug("Worker [{}] creating new session via ActiveClient for URI [{}]", producerWorkerName, connectionURI);
			sess = controllerEngine.getActiveClient().createSession(connectionURI, false, Session.AUTO_ACKNOWLEDGE);

			if (sess == null) { // createSession now returns null on failure
				logger.error("Worker [{}] failed to create session from ActiveClient for URI [{}]. Session is null.", producerWorkerName, connectionURI);
				return false; // Initialization failed
			}
			if (sess.isClosed()) { // Defensive check, though createSession should handle this
				logger.error("Worker [{}] created session for URI [{}], but it's already closed.", producerWorkerName, connectionURI);
				return false; // Initialization failed
			}

			logger.debug("Worker [{}] session created. Creating destination queue [{}].", producerWorkerName, txQueueName);
			destination = sess.createQueue(txQueueName);

			logger.debug("Worker [{}] destination created. Creating producer.", producerWorkerName);
			producer = sess.createProducer(destination);
			// Configure producer (consider making TTL configurable)
			producer.setTimeToLive(plugin.getConfig().getLongParam("activeproducerworker_ttl",300000L));
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // Default, can be overridden in send

			isActive = true; // Mark as active only on successful initialization
			logger.info("Worker [{}] JMS resources initialized successfully.", producerWorkerName);
			return true;

		} catch (JMSException jmse) {
			logger.error("Worker [{}] JMSException during JMS resource initialization: {}", producerWorkerName, jmse.getMessage(), jmse);
		} catch (Exception ex) {
			logger.error("Worker [{}] General Exception during JMS resource initialization: {}", producerWorkerName, ex.getMessage(), ex);
		}
		// If any exception occurred, ensure isActive is false and resources are cleaned.
		isActive = false;
		shutdownJMSResources(); // Attempt cleanup on failure
		return false;
	}

	// Cleans up JMS resources associated with this worker.
	public boolean shutdown() { // Renamed to avoid conflict, made public for AgentProducer
		logger.info("Worker [{}] shutting down...", producerWorkerName);
		isActive = false; // Mark as inactive immediately
		return shutdownJMSResources();
	}

	private boolean shutdownJMSResources() {
		boolean isShutdown = true; // Assume success unless an error occurs
		logger.debug("Worker [{}] cleaning up JMS resources...", producerWorkerName);
		try {
			if (producer != null) {
				try {
					producer.close();
					logger.trace("Worker [{}] producer closed.", producerWorkerName);
				} catch (JMSException e) {
					logger.warn("Worker [{}] JMSException closing producer: {}", producerWorkerName, e.getMessage());
					isShutdown = false;
				} finally {
					producer = null;
				}
			}
			if (sess != null) {
				try {
					// Check if the session is already closed to avoid benign exceptions during cleanup
					if (!sess.isClosed()) {
						sess.close();
						logger.trace("Worker [{}] session closed.", producerWorkerName);
					} else {
						logger.trace("Worker [{}] session was already closed.", producerWorkerName);
					}
				} catch (JMSException e) {
					logger.warn("Worker [{}] JMSException closing session: {}", producerWorkerName, e.getMessage());
					isShutdown = false;
				} finally {
					sess = null;
				}
			}
			destination = null; // Nullify destination reference
			if(isShutdown) logger.debug("Worker [{}] JMS resources cleanup complete.", producerWorkerName);
			else logger.warn("Worker [{}] JMS resources cleanup had issues.", producerWorkerName);

		} catch (Exception e) {
			logger.error("Worker [{}] general exception during JMS resource shutdown: {}", producerWorkerName, e.getMessage(), e);
			isShutdown = false;
		}
		return isShutdown;
	}

	public String getConnectionURI() { // Renamed for clarity
		return connectionURI;
	}

	public String getTXQueueName() {
		return txQueueName;
	}

	// Sends a message. Throws JMSException on failure to be handled by AgentProducer.
	public void sendMessage(MsgEvent se) throws JMSException {
		if (!isActive || sess == null || producer == null || sess.isClosed()) {
			String errorMsg = String.format("sendMessage called on worker [%s] but it's inactive, session/producer is null, or session is closed! isActive: %s, session: %s, producer: %s",
					producerWorkerName, isActive, (sess == null ? "null" : (sess.isClosed() ? "closed" : "open")), (producer == null ? "null" : "exists"));
			logger.error(errorMsg);
			// Mark as inactive and throw exception so AgentProducer handles recreation
			isActive = false; // Ensure it's marked inactive
			throw new JMSException("Invalid state for worker " + producerWorkerName + ": " + errorMsg);
		}

		int pri = plugin.getConfig().getIntegerParam("activeproducerworker_default_priority", 4); // Default AMQ priority is 4
		int deliveryMode = DeliveryMode.NON_PERSISTENT;
		String type = se.getMsgType().toString();

		switch (type) {
			case "WATCHDOG":
				pri = 9; // Highest user priority
				deliveryMode = DeliveryMode.PERSISTENT;
				break;
			case "CONFIG":
				pri = 8;
				deliveryMode = DeliveryMode.PERSISTENT;
				break;
			case "EXEC":
				pri = 7;
				// Default to NON_PERSISTENT for EXEC unless explicitly required
				// deliveryMode = DeliveryMode.PERSISTENT;
				break;
			// INFO, LOG, DISCOVER, ERROR, KPI, GC use default priority and NON_PERSISTENT
		}

		try {
			logger.trace("Worker [{}] sending message: Type={}, Params={}", producerWorkerName, type, se.getParams());
			TextMessage textMessage = sess.createTextMessage(gson.toJson(se));

			// Send with determined delivery mode, priority, and TTL (0 = broker default)
			// Note: producer.getTimeToLive() was set during producer creation.
			// Sending with 0 here means use the producer's default.
			producer.send(textMessage, deliveryMode, pri, producer.getTimeToLive());

			logger.trace("Worker [{}] successfully sent message to queue [{}]", producerWorkerName, txQueueName);

		} catch (JMSException jmse) {
			logger.error("Worker [{}] JMSException during send to queue [{}]: {}", producerWorkerName, txQueueName, jmse.getMessage());
			isActive = false; // Mark as inactive due to send failure
			throw jmse; // Re-throw to be caught by AgentProducer for retry/re-initialization
		}
	}
}