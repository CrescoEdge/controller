package io.cresco.agent.data;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import javax.jms.*;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataPlaneServiceImpl implements DataPlaneService {
	private PluginBuilder plugin;
	private CLogger logger;
	private ControllerEngine controllerEngine;

    private ActiveMQSession activeMQSession;

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

    private String URI;

	public DataPlaneServiceImpl(ControllerEngine controllerEngine, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DataPlaneServiceImpl.class.getName(),CLogger.Level.Info);

		messageConsumerMap = Collections.synchronizedMap(new HashMap<>());

		this.URI = URI;

        //sess = (ActiveMQSession)controllerEngine.getActiveClient().getConnection(URI).createSession(false, Session.AUTO_ACKNOWLEDGE);

        //sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        agentTopic = getSession().createTopic(getTopicName(TopicType.AGENT));
        regionTopic = getSession().createTopic(getTopicName(TopicType.REGION));
        globalTopic = getSession().createTopic(getTopicName(TopicType.GLOBAL));


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

    private ActiveMQSession getSession() {
	    try {

            while(!controllerEngine.cstate.isActive()) {
                Thread.sleep(1000);
            }

	        if(activeMQSession == null) {
                activeMQSession = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
            }

                
            if(activeMQSession.isClosed()) {
                activeMQSession = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
            }
            
        } catch (Exception ex) {
	        ex.printStackTrace();
        }


	    return activeMQSession;
    }

	public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString) {
	    String listenerId = null;
        try {

            MessageConsumer consumer = null;

            switch (topicType) {
                case AGENT:
                    if(selectorString == null) {
                        consumer = getSession().createConsumer(agentTopic);
                    } else {
                        consumer = getSession().createConsumer(agentTopic, selectorString);
                    }
                    break;
                case REGION:
                    if(selectorString == null) {
                        consumer = getSession().createConsumer(regionTopic);
                    } else {
                        consumer = getSession().createConsumer(regionTopic, selectorString);
                    }
                    break;
                case GLOBAL:
                    if(selectorString == null) {
                        consumer = getSession().createConsumer(globalTopic);
                    } else {
                        consumer = getSession().createConsumer(globalTopic, selectorString);
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

            while(!controllerEngine.cstate.isActive()) {
                Thread.sleep(1000);
            }

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
        catch (Exception ex) {
            logger.error(ex.getMessage());
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
                    messageProducer = getSession().createProducer(agentTopic);
                    break;
                case REGION:
                   messageProducer = getSession().createProducer(regionTopic);
                     break;
                case GLOBAL:
                    messageProducer = getSession().createProducer(globalTopic);
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
            bytesMessage = getSession().createBytesMessage();

        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return bytesMessage;
	}

    public MapMessage createMapMessage() {
        MapMessage mapMessage = null;
        try {
            mapMessage = getSession().createMapMessage();

        } catch (Exception ex){
            ex.printStackTrace();
        }
	    return mapMessage;
    }

    public Message createMessage() {
	    Message message = null;
	    try {

            message = getSession().createMessage();

        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return message;
    }

    public ObjectMessage createObjectMessage() {
	    ObjectMessage objectMessage = null;

	    try {
            objectMessage = getSession().createObjectMessage();

        } catch (Exception ex) {
	        ex.printStackTrace();
        }
	    return objectMessage;
    }

    //blobs are not part of JMX, they are part of ActiveMQ, which is not in the core lib
    //for now we must use i/o streams

    public BlobMessage createBlobMessage(URL url) {
	    BlobMessage blobMessage = null;
	    try {
	        blobMessage = getSession().createBlobMessage(url);
        } catch (Exception ex) {
	        ex.printStackTrace();
        }
	    return blobMessage;
    }

    public BlobMessage createBlobMessage(File file) {
        BlobMessage blobMessage = null;
        try {
            blobMessage = getSession().createBlobMessage(file);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return blobMessage;
    }

    public BlobMessage createBlobMessage(InputStream inputStream) {
        BlobMessage blobMessage = null;
        try {
            blobMessage = getSession().createBlobMessage(inputStream);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return blobMessage;

    }


    public StreamMessage createStreamMessage() {
	    StreamMessage streamMessage = null;
	    try{


            streamMessage = getSession().createStreamMessage();

        } catch (Exception ex) {
	        ex.printStackTrace();
        }
        return streamMessage;
    }

    public TextMessage createTextMessage() {
	    TextMessage textMessage = null;
	    try {

            textMessage = getSession().createTextMessage();

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



