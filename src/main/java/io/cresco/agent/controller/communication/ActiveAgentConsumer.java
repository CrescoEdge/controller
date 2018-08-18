package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;

public class ActiveAgentConsumer {
	private PluginBuilder plugin;
	private CLogger logger;
	private Queue RXqueue;
	private Session sess;
	private ActiveMQConnection conn;
	private ActiveMQSslConnectionFactory connf;
	private ControllerEngine controllerEngine;

	public ActiveAgentConsumer(ControllerEngine controllerEngine, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveAgentConsumer.class.getName(),CLogger.Level.Info);

		logger.debug("Queue: {}", RXQueueName);
		logger.trace("RXQueue=" + RXQueueName + " URI=" + URI + " brokerUserNameAgent=" + brokerUserNameAgent + " brokerPasswordAgent=" + brokerPasswordAgent);
		int retryCount = 10;

		connf = new ActiveMQSslConnectionFactory(URI);
		//Don't serialize VM connections
		if(URI.startsWith("vm://")) {
			connf.setObjectMessageSerializationDefered(true);
		}
		connf.setKeyAndTrustManagers(controllerEngine.getCertificateManager().getKeyManagers(),controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());
		conn = (ActiveMQConnection) connf.createConnection();
		conn.start();
		sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		RXqueue = sess.createQueue(RXQueueName);
		MessageConsumer consumer = sess.createConsumer(RXqueue);

		Gson gson = new Gson();
		controllerEngine.setConsumerThreadActive(true);

		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message msg) {
				try {

					if (msg instanceof TextMessage) {

						//TextMessage textMessage = (TextMessage) msg;
						MsgEvent me = gson.fromJson(((TextMessage) msg).getText(),MsgEvent.class);
						if(me != null) {
							logger.debug("Message: {}", me.getParams().toString());
							//create new thread service for incoming messages
							boolean isMyRPC = false;
							if (me.getParams().keySet().contains("is_rpc")) {
								//pick up self-rpc, unless ttl == 0
								String callId = me.getParam(("callId-" + plugin.getRegion() + "-" +
										plugin.getAgent() + "-" + plugin.getPluginID()));

								if (callId != null) {
									isMyRPC = true;
									plugin.receiveRPC(callId, me);
								}
							}

							if(!isMyRPC) {
								controllerEngine.msgInThreaded(me);
							}

							//
						} else {
							logger.error("non-MsgEvent message found!");
						}

					} else {
						logger.error("non-Text message recieved!");
					}
				} catch(Exception ex) {
					logger.error("onMessage Error : " + ex.getMessage());
					ex.printStackTrace();
				}
			}
		});


	}

}