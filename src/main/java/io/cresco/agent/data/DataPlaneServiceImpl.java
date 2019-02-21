package io.cresco.agent.data;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataPlaneServiceImpl implements DataPlaneService {
	private PluginBuilder plugin;
	private CLogger logger;
	//private ActiveMQConnection conn;
	//private ActiveMQSslConnectionFactory connf;
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

		/*
		connf = new ActiveMQSslConnectionFactory(URI);

		//Don't serialize VM connections
		if(URI.startsWith("vm://")) {
			connf.setObjectMessageSerializationDefered(true);
		}

		connf.setKeyAndTrustManagers(controllerEngine.getCertificateManager().getKeyManagers(),controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());
		conn = (ActiveMQConnection) connf.createConnection();
		conn.start();
		*/

        sess = controllerEngine.getActiveClient().getConnection(URI).createSession(false, Session.AUTO_ACKNOWLEDGE);
        //sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        agentTopic = sess.createTopic(getTopicName(TopicType.AGENT));
        regionTopic = sess.createTopic(getTopicName(TopicType.REGION));
        globalTopic = sess.createTopic(getTopicName(TopicType.GLOBAL));

        //CEP depends on DataPlaneService
        //cepEngine = new CEPEngine(controllerEngine, plugin);

        //CEPEngineInit cepEngineInit = new CEPEngineInit(controllerEngine, plugin);


        String inputStreamName = "input1";
        String outputStreamName = "output1";

        String inputRecordSchemaString = "{\"type\":\"record\",\"name\":\"Ticker\",\"fields\":[{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"urn\",\"type\":\"string\"},{\"name\":\"metric\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}]}";
        //String inputStreamName = "UserStream";

        //String outputStreamName = "BarStream";
        String outputStreamAttributesString = "source string, avgValue double";

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from " + inputStreamName + "#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into " + outputStreamName + "; ";



        //cepEngineInit.createCEP(inputRecordSchemaString,inputStreamName,outputStreamName,outputStreamAttributesString,queryString);

        //logger.error("CREATE: " + createCEP(inputRecordSchemaString,inputStreamName,outputStreamName,outputStreamAttributesString,queryString));




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

    private String getCEPPluginId() {
	    String pluginId = null;
	    try {
            List<Map<String, String>> configMapList = controllerEngine.getGDB().getPluginListMapByType("pluginname", "io.cresco.cdp");
            if(configMapList.size() > 1) {
                logger.error("CEP Plugin Count (" + configMapList.size() + ") > 1 not allowed");
            }
            for (Map<String, String> configMap : configMapList) {
                if ((configMap.get("region").equals(plugin.getRegion())) && (configMap.get("agent").equals(plugin.getAgent()))) {
                    pluginId = configMap.get("pluginid");
                }
            }
        } catch (Exception ex) {
	        logger.error(ex.getMessage());
        }
        return  null;
    }

    public String createCEP(String inputRecordSchemaString, String inputStreamName, String outputStreamName, String outputStreamAttributesString,String queryString) {

        String pluginId = getCEPPluginId();
        String cepId = null;

        if(pluginId != null) {

            cepId = UUID.randomUUID().toString();

            MsgEvent createQuery = plugin.getPluginMsgEvent(MsgEvent.Type.CONFIG, pluginId);
            createQuery.setParam("action", "queryadd");

            createQuery.setCompressedParam("input_schema", inputRecordSchemaString);
            createQuery.setParam("input_stream_name", inputStreamName);
            createQuery.setParam("output_stream_name", outputStreamName);
            createQuery.setParam("output_stream_attributes", outputStreamAttributesString);
            createQuery.setParam("query_id", cepId);
            createQuery.setParam("query", queryString);

            MsgEvent response = plugin.sendRPC(createQuery);

        }

        return cepId;

    }

    public void input(String cepId, String streamName, String jsonPayload) {

	    String pluginId = getCEPPluginId();

        if(pluginId != null) {

            MsgEvent inputMsg = plugin.getPluginMsgEvent(MsgEvent.Type.EXEC, pluginId);
            inputMsg.setParam("action", "queryinput");
            inputMsg.setParam("query_id", cepId);
            inputMsg.setParam("input_stream_name", streamName);
            inputMsg.setCompressedParam("input_stream_payload", jsonPayload);

            plugin.msgOut(inputMsg);

        }
    }

    public boolean removeCEP(String cepId) {

	    boolean isRemoved = false;

        String pluginId = getCEPPluginId();

        if(pluginId != null) {
            MsgEvent deleteQuery = plugin.getPluginMsgEvent(MsgEvent.Type.CONFIG, pluginId);
            deleteQuery.setParam("action", "querydel");
            deleteQuery.setParam("query_id", cepId);

            MsgEvent response = plugin.sendRPC(deleteQuery);

            if (response != null) {
                if (response.getParam("iscleared") != null) {

                }
            }
        }
        return isRemoved;
    }

}



