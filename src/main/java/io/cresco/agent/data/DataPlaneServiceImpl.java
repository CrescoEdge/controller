package io.cresco.agent.data;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataPlaneServiceImpl implements DataPlaneService {
	private PluginBuilder plugin;
	private CLogger logger;
	private ActiveMQConnection conn;
	private ActiveMQSslConnectionFactory connf;
	private ControllerEngine controllerEngine;
    private Session sess;
    private Destination agentTopic;
    private Destination regionTopic;
    private Destination globalTopic;
    private final String agentTopicName = "agent.event";
    private final String regionTopicName = "region.event";
    private final String globalTopicName = "global.event";

    private MessageProducer agentProducer;
    private MessageProducer regionProducer;
    private MessageProducer globalProducer;


    private Map<String,MessageConsumer> messageConsumerMap;
    private AtomicBoolean lockMessage = new AtomicBoolean();


	public DataPlaneServiceImpl(ControllerEngine controllerEngine, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DataPlaneServiceImpl.class.getName(),CLogger.Level.Info);

		messageConsumerMap = Collections.synchronizedMap(new HashMap<>());

		connf = new ActiveMQSslConnectionFactory(URI);

		//Don't serialize VM connections
		if(URI.startsWith("vm://")) {
			connf.setObjectMessageSerializationDefered(true);
		}

		connf.setKeyAndTrustManagers(controllerEngine.getCertificateManager().getKeyManagers(),controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());
		conn = (ActiveMQConnection) connf.createConnection();
		conn.start();

        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        agentTopic = sess.createTopic(getTopicName(TopicType.AGENT));
        regionTopic = sess.createTopic(getTopicName(TopicType.REGION));
        globalTopic = sess.createTopic(getTopicName(TopicType.GLOBAL));

	}


	public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString) {
	    String listenerId = null;
        try {

            MessageConsumer consumer = null;

            switch (topicType) {
                case AGENT:
                    if(selectorString == null) {
                        consumer = sess.createConsumer(agentTopic);
                    } else {
                        consumer = sess.createConsumer(agentTopic, selectorString);
                    }
                    break;
                case REGION:
                    if(selectorString == null) {
                        consumer = sess.createConsumer(regionTopic);
                    } else {
                        consumer = sess.createConsumer(regionTopic, selectorString);
                    }
                    break;
                case GLOBAL:
                    if(selectorString == null) {
                        consumer = sess.createConsumer(globalTopic);
                    } else {
                        consumer = sess.createConsumer(globalTopic, selectorString);
                    }
                    break;
            }

            consumer.setMessageListener(messageListener);
            listenerId = UUID.randomUUID().toString();
            synchronized (lockMessage) {
                messageConsumerMap.put(listenerId,consumer);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return listenerId;
    }

    public boolean sendMessage(TopicType topicType, Message message) {
        try {

            switch (topicType) {
                case AGENT:
                    if(agentProducer == null) {
                        agentProducer = getMessageProducer(topicType);
                    }
                    agentProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    break;
                case REGION:
                    if(regionProducer == null) {
                        regionProducer = getMessageProducer(topicType);
                    }
                    regionProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    break;
                case GLOBAL:
                    if(globalProducer == null) {
                        globalProducer = getMessageProducer(topicType);
                    }
                    globalProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    break;
            }

            return true;
        } catch (JMSException jmse) {
            jmse.printStackTrace();
            return false;
        }
    }

    private String getTopicName(TopicType topicType) {

        String topicNameString = null;
        switch (topicType) {
            case AGENT:
                topicNameString = agentTopicName;
                break;
            case REGION:
                topicNameString = regionTopicName;
                break;
            case GLOBAL:
                topicNameString = globalTopicName;
                break;
        }
        return topicNameString;
    }

    private MessageProducer getMessageProducer(TopicType topicType) {

        MessageProducer messageProducer = null;
        try {
            switch (topicType) {
                case AGENT:
                    messageProducer = sess.createProducer(agentTopic);
                    break;
                case REGION:
                   messageProducer = sess.createProducer(regionTopic);
                     break;
                case GLOBAL:
                    messageProducer = sess.createProducer(globalTopic);
                    break;
            }

            messageProducer.setTimeToLive(300000L);
            messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return messageProducer;
    }

    public BytesMessage createBytesMessage() {
        BytesMessage bytesMessage = null;
	    try {
            bytesMessage = sess.createBytesMessage();
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return bytesMessage;
	}

    public MapMessage createMapMessage() {
        MapMessage mapMessage = null;
        try {
            mapMessage = sess.createMapMessage();
        } catch (Exception ex){
            ex.printStackTrace();
        }
	    return mapMessage;
    }

    public Message createMessage() {
	    Message message = null;
	    try {
	        message = sess.createMessage();
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return message;
    }

    public ObjectMessage createObjectMessage() {
	    ObjectMessage objectMessage = null;

	    try {
	        objectMessage = sess.createObjectMessage();
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
	    return objectMessage;
    }

    public StreamMessage createStreamMessage() {
	    StreamMessage streamMessage = null;
	    try{
	        streamMessage = sess.createStreamMessage();
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return streamMessage;
    }

    public TextMessage createTextMessage() {
	    TextMessage textMessage = null;
	    try {
	        textMessage = sess.createTextMessage();
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return textMessage;
    }

}



