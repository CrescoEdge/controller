package io.cresco.agent.controller.communication;

import com.google.common.cache.*;
import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AgentConsumer {
	private PluginBuilder plugin;
	private CLogger logger;
	private Queue RXqueue;
	private ActiveMQSession sess;
	private ControllerEngine controllerEngine;
	private AtomicBoolean lockFileMap = new AtomicBoolean();
	private Map<String, FileObject> fileObjectMap;

	private Cache<String, MsgEvent> fileMsgEventCache;

	public AgentConsumer(ControllerEngine controllerEngine, String RXQueueName, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(AgentConsumer.class.getName(),CLogger.Level.Info);

		this.fileObjectMap = Collections.synchronizedMap(new HashMap<>());

		//use expiring cache to trigger cleanup of files and messages
		RemovalListener<String, MsgEvent> listener;
		listener = new RemovalListener<String, MsgEvent>() {
			@Override
			public void onRemoval(RemovalNotification<String, MsgEvent> n){
				if (n.wasEvicted()) {
					//String cause = n.getCause().name();
					//assertEquals(RemovalCause.SIZE.toString(),cause);
					logger.error("IMPLEMENT REMOVAL OF FILES!");
				}
			}
		};

		fileMsgEventCache = CacheBuilder.newBuilder()
				.concurrencyLevel(4)
				.softValues()
				.removalListener(listener)
				.expireAfterWrite(30, TimeUnit.MINUTES)
				.build();


		logger.debug("Queue: {}", RXQueueName);
		logger.trace("RXQueue=" + RXQueueName + " URI=" + URI);


		sess = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI,false, Session.AUTO_ACKNOWLEDGE);

		RXqueue = sess.createQueue(RXQueueName);
		MessageConsumer consumer = sess.createConsumer(RXqueue);

		Gson gson = new Gson();
		//controllerEngine.setConsumerThreadActive(true);

		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message msg) {
				try {

					if (msg instanceof TextMessage) {

						//TextMessage textMessage = (TextMessage) msg;
						MsgEvent me = gson.fromJson(((TextMessage) msg).getText(),MsgEvent.class);
						if(me != null) {
							logger.debug("Message: {}", me.getParams().toString());

							//check if message needs to be held for files and registered




							//start RPC checks
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
								//create new thread service for incoming messages
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