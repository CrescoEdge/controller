package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;

import javax.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;

public class ActiveProducerWorker {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private String producerWorkerName;
	private CLogger logger;
	private ActiveMQSession sess;

	//private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String TXQueueName;
	private Destination destination;

	private String URI;


	public ActiveProducerWorker(ControllerEngine controllerEngine, String TXQueueName, String URI)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveProducerWorker.class.getName(),CLogger.Level.Info);

		this.URI = URI;
		this.producerWorkerName = UUID.randomUUID().toString();
		try {
			this.TXQueueName = TXQueueName;
			gson = new Gson();

			sess = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

			destination = sess.createQueue(TXQueueName);

			/*
			producer = sess.createProducer(destination);
			producer.setTimeToLive(300000L);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			*/

			isActive = true;
			logger.debug("Initialized", TXQueueName);
		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
		}
	}
	//BDB\em{?}
	public boolean shutdown() {
		boolean isShutdown = false;
		try {
			//producer.close();
			sess.close();
			logger.debug("Producer Worker [{}] has shutdown", TXQueueName);
			isShutdown = true;
		} catch (JMSException jmse) {
			logger.error(jmse.getMessage());
			logger.error(jmse.getLinkedException().getMessage());
		}
		return isShutdown;


	}

	public String getURI() {
		return URI;
	}

	public String getTXQueueName() {
		return TXQueueName;
	}

	public boolean sendMessage(MsgEvent se) {
		boolean isSent = false;
		MessageProducer producer = null;
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

				producer = sess.createProducer(destination);
				producer.setTimeToLive(300000L);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

				TextMessage textMessage = sess.createTextMessage(gson.toJson(se));
				producer.send(textMessage, DeliveryMode.NON_PERSISTENT, pri, 0);
				isSent = true;

			//}
			//producer.send(sess.createTextMessage(gson.toJson(se)));
			logger.trace("sendMessage to : {} : from : {}", TXQueueName, producerWorkerName);

		} catch (JMSException jmse) {
			logger.error("sendMessage: jmse {} : {}", se.getParams(), jmse.getMessage());
			StringWriter errors = new StringWriter();
			jmse.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
			//trigger comminit()
		} catch (Exception ex) {
			logger.error("sendMessage()  " + ex.getMessage());
			StringWriter errors = new StringWriter();
			ex.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
			//trigger comminit()
		} finally {
			if(producer != null) {
				try {
					producer.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		return isSent;
	}



}