package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;
import java.util.UUID;

public class ActiveProducerWorker {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private String producerWorkerName;
	private CLogger logger;
	private ActiveMQSession sess;

	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	private Destination destination;
	
	public ActiveProducerWorker(ControllerEngine controllerEngine, String TXQueueName, String URI)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveProducerWorker.class.getName(),CLogger.Level.Info);

		this.producerWorkerName = UUID.randomUUID().toString();
		try {
			queueName = TXQueueName;
			gson = new Gson();

			//sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			sess = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

			//logger.error("New session created URI: [" + URI + "] QueueName [" + TXQueueName + "]");


			destination = sess.createQueue(TXQueueName);

			producer = sess.createProducer(destination);
			producer.setTimeToLive(300000L);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);


			isActive = true;
			logger.debug("Initialized", queueName);
		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
		}
	}
//BDB\em{?}
	public boolean shutdown() {
		boolean isShutdown = false;
		try {
			producer.close();
			sess.close();
			logger.debug("Producer Worker [{}] has shutdown", queueName);
			isShutdown = true;
		} catch (JMSException jmse) {
			logger.error(jmse.getMessage());
			logger.error(jmse.getLinkedException().getMessage());
		}
		return isShutdown;


	}

	public boolean sendMessage(MsgEvent se) {
		try {
			int pri = 0;

			/*
			CONFIG,
        	DISCOVER,
        	ERROR,
        	EXEC,
        	GC,
        	INFO,
        	KPI,
        	LOG,
        	WATCHDOG;
			 */

			String type = se.getMsgType().toString();

			switch (type) {
				case "CONFIG":  pri = 10;
					break;
				case "EXEC":  pri = 10;
					break;
				case "WATCHDOG":  pri = 7;
					break;
				case "KPI":  pri = 0;
					break;
				default: pri = 4;
					break;
			}

			//if(controllerEngine.cstate.isActive()) {
				producer.send(sess.createTextMessage(gson.toJson(se)), DeliveryMode.NON_PERSISTENT, pri, 0);
			//}
			//producer.send(sess.createTextMessage(gson.toJson(se)));
			logger.trace("sendMessage to : {} : from : {}", queueName, producerWorkerName);
			return true;
		} catch (JMSException jmse) {
			logger.error("sendMessage: jmse {} : {}", se.getParams(), jmse.getMessage());
			//trigger comminit()
			return false;
		}
	}
}