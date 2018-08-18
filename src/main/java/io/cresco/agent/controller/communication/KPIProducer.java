package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.util.Map;
import java.util.UUID;

public class KPIProducer {
	private String producerWorkerName;
	private CLogger logger;
	private Session sess;
	private ActiveMQConnection conn;
	private ActiveMQSslConnectionFactory connf;

	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	private Destination destination;

	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;

	public KPIProducer(ControllerEngine controllerEngine, String TXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(KPIProducer.class.getName(),CLogger.Level.Info);

		//this.logger = new CLogger(KPIProducer.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
		this.producerWorkerName = UUID.randomUUID().toString();
		try {
			queueName = TXQueueName;
			gson = new Gson();

			//conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent, brokerPasswordAgent, URI).createConnection();
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(URI).createConnection();

			/*
			connf = new ActiveMQSslConnectionFactory(URI);
			connf.setKeyAndTrustManagers(agentcontroller.getCertificateManager().getKeyManagers(),agentcontroller.getCertificateManager().getTrustManagers(), new SecureRandom());
			conn = (ActiveMQConnection) connf.createConnection();
			*/

			conn.start();
			sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			destination = sess.createQueue(TXQueueName);
			producer = sess.createProducer(destination);
			producer.setTimeToLive(300000L);
			//producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
			conn.cleanup();
			conn.close();
			logger.debug("Producer Worker [{}] has shutdown", queueName);
			isShutdown = true;
		} catch (JMSException jmse) {
			logger.error(jmse.getMessage());
			logger.error(jmse.getLinkedException().getMessage());
		}
		return isShutdown;


	}

	public boolean sendMessage(String region, String agent, String pluginId, String resource_id, String inode_id, Map<String,String> params) {
		try {
			//producer.send(sess.createTextMessage(gson.toJson(params)));

			TextMessage outgoingMessage = sess.createTextMessage(gson.toJson(params));
			outgoingMessage.setStringProperty("region", region);
			outgoingMessage.setStringProperty("io/cresco/agent", agent);
			outgoingMessage.setStringProperty("agentcontroller", pluginId);
			outgoingMessage.setStringProperty("resourceid", resource_id);
			outgoingMessage.setStringProperty("inodeid", inode_id);
			//producer.send(sess.createTextMessage(gson.toJson(params)));
			producer.send(outgoingMessage);

			logger.trace("sendMessage to : {} : from : {}", queueName, producerWorkerName);
			return true;
		} catch (JMSException jmse) {
			logger.error("sendMessage: jmse {} : {}", params, jmse.getMessage());
			return false;
		}
	}
}