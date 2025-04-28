package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;

import jakarta.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
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
	private String TXQueueName; // Destination queue name
	private Destination destination; // JMS Destination object

	private String URI; // Connection URI associated with this worker


	public ActiveProducerWorker(ControllerEngine controllerEngine, String TXQueueName, String URI)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		// Use a more specific logger name if possible
		this.logger = plugin.getLogger(ActiveProducerWorker.class.getName() + "[" + TXQueueName + "]", CLogger.Level.Info);

		this.URI = URI;
		this.TXQueueName = TXQueueName;
		this.producerWorkerName = TXQueueName + "_" + UUID.randomUUID().toString(); // More descriptive name
		this.gson = new Gson();

		try {
			// Initialize the connection, session, destination, and producer
			if(isInit()) {
				// isActive is now set within isInit on success
				logger.info("Worker [{}] initialized successfully.", producerWorkerName);
			} else {
				// Handle initialization failure immediately
				logger.error("Worker [{}] failed to initialize!", producerWorkerName);
				// Optionally throw an exception to signal failure to the creator (AgentProducer)
				throw new RuntimeException("Failed to initialize ActiveProducerWorker for queue " + TXQueueName);
			}
		} catch (Exception e) {
			logger.error("Worker [{}] constructor exception: {}", producerWorkerName, e.getMessage(), e);
			// Ensure cleanup if constructor fails midway
			shutdown();
			throw new RuntimeException("Exception during ActiveProducerWorker construction", e);
		}
	}

	// isInit() - Initializes or re-initializes JMS resources for this worker.
	public boolean isInit() {
		boolean isInitSuccess = false;
		logger.debug("Worker [{}] attempting initialization/re-initialization...", producerWorkerName);
		try {
			// --- Cleanup existing resources first ---
			shutdown(); // Use shutdown logic for cleanup

			// --- Create new resources ---
			logger.debug("Worker [{}] creating new session via ActiveClient for URI [{}]", producerWorkerName, URI);
			sess = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

			if (sess != null && !sess.isClosed()) { // Check if session creation was successful
				logger.debug("Worker [{}] session created. Creating destination queue [{}].", producerWorkerName, TXQueueName);
				destination = sess.createQueue(TXQueueName);

				logger.debug("Worker [{}] destination created. Creating producer.", producerWorkerName);
				producer = sess.createProducer(destination);
				// Configure producer (consider making TTL configurable)
				producer.setTimeToLive(300000L);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // Default, overridden in send

				isInitSuccess = true;
				isActive = true; // Mark as active only on successful initialization
				logger.info("Worker [{}] initialization complete.", producerWorkerName);
			} else {
				logger.error("Worker [{}] failed to create session from ActiveClient for URI [{}].", producerWorkerName, URI);
				// Ensure cleanup if session creation failed
				shutdown();
			}
		} catch (Exception ex) {
			logger.error("Worker [{}] Exception during isInit: {}", producerWorkerName, ex.getMessage(), ex);
			// Ensure cleanup on any exception during initialization
			shutdown();
		}
		return isInitSuccess;
	}

	// shutdown() - Cleans up JMS resources associated with this worker.
	public boolean shutdown() {
		boolean isShutdown = false;
		logger.debug("Worker [{}] shutting down...", producerWorkerName);
		isActive = false; // Mark as inactive immediately
		try {
			if (producer != null) {
				try {
					producer.close();
					logger.trace("Worker [{}] producer closed.", producerWorkerName);
				} catch (JMSException e) {
					logger.warn("Worker [{}] JMSException closing producer: {}", producerWorkerName, e.getMessage());
				}
				producer = null; // Nullify after closing
			}
			if (sess != null) {
				try {
					sess.close();
					logger.trace("Worker [{}] session closed.", producerWorkerName);
				} catch (JMSException e) {
					logger.warn("Worker [{}] JMSException closing session: {}", producerWorkerName, e.getMessage());
				}
				sess = null; // Nullify after closing
			}
			destination = null; // Nullify destination reference
			isShutdown = true;
			logger.debug("Worker [{}] shutdown sequence complete.", producerWorkerName);
		} catch (Exception e) {
			// Catch any unexpected errors during shutdown
			logger.error("Worker [{}] general exception during shutdown: {}", producerWorkerName, e.getMessage(), e);
		}
		return isShutdown;
	}

	public String getURI() {
		return URI;
	}

	public String getTXQueueName() {
		return TXQueueName;
	}

	// Simplified sendMessage - Throws JMSException on failure to be handled by AgentProducer.
	public boolean sendMessage(MsgEvent se) throws JMSException {
		// Basic null checks before proceeding - throw exception if invalid state
		if (sess == null || producer == null || sess.isClosed()) {
			String errorMsg = String.format("sendMessage called on worker [%s] but session [%s] or producer [%s] is null or session is closed!",
					producerWorkerName, (sess == null ? "null" : "exists"), (producer == null ? "null" : "exists"));
			logger.error(errorMsg);
			// Mark as inactive and throw exception so AgentProducer handles recreation
			isActive = false;
			throw new JMSException("Invalid state for worker " + producerWorkerName + ": " + errorMsg);
		}

		int pri = 5; // Default priority
		int deliveryMode = DeliveryMode.NON_PERSISTENT; // Default delivery mode
		String type = se.getMsgType().toString();

		// Determine priority and delivery mode based on message type
		switch (type) {
			case "WATCHDOG":
				pri = 9;
				deliveryMode = DeliveryMode.PERSISTENT;
				break;
			case "CONFIG":
				pri = 8;
				deliveryMode = DeliveryMode.PERSISTENT;
				break;
			case "EXEC":
				pri = 7;
				deliveryMode = DeliveryMode.PERSISTENT;
				break;
			// Add other cases if needed
		}

		try {
			logger.trace("Worker [{}] sending message: {}", producerWorkerName, se.getParams());
			TextMessage textMessage = sess.createTextMessage(gson.toJson(se));

			// Send with determined delivery mode, priority, and TTL (0 = default/infinite)
			producer.send(textMessage, deliveryMode, pri, 0);

			logger.trace("Worker [{}] successfully sent message to queue [{}]", producerWorkerName, TXQueueName);
			return true; // Return true on successful send

		} catch (JMSException jmse) {
			// Log the specific error encountered during send
			logger.error("Worker [{}] JMSException during send to queue [{}]: {}", producerWorkerName, TXQueueName, jmse.getMessage());
			// Mark as inactive as the send failed, likely due to connection/session issue
			isActive = false;
			// Re-throw the exception to be caught and handled by AgentProducer
			throw jmse;
		}
		// Catching general exceptions might mask specific JMS issues,
		// but can be useful for unexpected errors. Consider if needed.
		/* catch (Exception ex) {
		    logger.error("Worker [{}] Unexpected Exception during send: {}", producerWorkerName, ex.getMessage(), ex);
		    isActive = false;
		    // Wrap general exceptions in JMSException if needed for consistent handling upstream
		    throw new JMSException("Unexpected error in sendMessage: " + ex.getMessage());
		}*/
	}
}
