package io.cresco.agent.data;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.FileObject;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import javax.jms.*;
import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class DataPlaneServiceImpl implements DataPlaneService {
	private PluginBuilder plugin;
	private CLogger logger;
	private ControllerEngine controllerEngine;

	private CEPEngine cepEngine;

    private ActiveMQSession activeMQSession;

    private Destination agentTopic;
    private Destination regionTopic;
    private Destination globalTopic;
    private String agentTopicName = "agent.event";
    private String regionTopicName = "region.event";
    private String globalTopicName = "global.event";

    private MessageProducer agentProducer;
    private MessageProducer regionProducer;
    private MessageProducer globalProducer;

    private Map<String,MessageConsumer> messageConsumerMap;
    private final AtomicBoolean lockMessage = new AtomicBoolean();

    private String URI;

    private Path journalPath;

    private Type typeOfListFileObject;
    private Gson gson;

	public DataPlaneServiceImpl(ControllerEngine controllerEngine, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DataPlaneServiceImpl.class.getName(),CLogger.Level.Info);

        this.cepEngine = new CEPEngine(plugin);


		messageConsumerMap = Collections.synchronizedMap(new HashMap<>());

		this.URI = URI;
        typeOfListFileObject = new TypeToken<List<FileObject>>() { }.getType();

        gson = new Gson();


        agentTopic = getDestination(TopicType.AGENT);
        regionTopic = getDestination(TopicType.REGION);
        globalTopic = getDestination(TopicType.GLOBAL);


        String inputStreamName = "input1";
        String outputStreamName = "output1";

        String inputRecordSchemaString = "{\"type\":\"record\",\"name\":\"Ticker\",\"fields\":[{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"urn\",\"type\":\"string\"},{\"name\":\"metric\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}]}";

        String outputStreamAttributesString = "source string, avgValue double";

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from " + inputStreamName + "#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into " + outputStreamName + "; ";


        try {
            String journalDirPath = plugin.getConfig().getStringParam("journal_dir", FileSystems.getDefault().getPath("cresco-data/dp-journal").toAbsolutePath().toString());
            journalPath = Paths.get(journalDirPath);
            //remove old files if they exist from the journal
            if(journalPath.toFile().exists()) {

                try (Stream<Path> journalWalk = Files.walk(journalPath)) {
                            journalWalk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);

                } catch (IOException e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    String sStackTrace = sw.toString(); // stack trace as a string
                    logger.error(sStackTrace);
                }

            }
            Files.createDirectories(journalPath);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }

    }

    public void shutdown() {
	    try {
	        logger.info("Shutting down CEPEngine");
	        cepEngine.shutdown();

            List<String> listeners = null;
            synchronized (lockMessage) {
                listeners = new ArrayList<>(messageConsumerMap.keySet());
            }
            for (String listener : listeners) {
                logger.info("Removing listener: " + listener);
                removeMessageListener(listener);
            }


        } catch (Exception ex) {
	        ex.printStackTrace();
        }
    }

    public boolean isFaultURIActive() {
        return controllerEngine.getActiveClient().isFaultURIActive();
    }

    private Destination getDestination(TopicType topicType) {
        Destination destination = null;
	    try {

	        ActiveMQSession activeMQSession = getSession();
	        String topicName = getTopicName(topicType);

	        if((activeMQSession != null) && (topicName != null)) {
	            destination = activeMQSession.createTopic(topicName);
            }


        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
	    return destination;
    }

    private ActiveMQSession getSession() {
	    try {

            while (!controllerEngine.getActiveClient().isFaultURIActive()) {
                Thread.sleep(1000);
            }

	        if(activeMQSession == null) {
                activeMQSession = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
	        }

                
            if(activeMQSession.isClosed()) {
                activeMQSession = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
            }
            
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }

	    return activeMQSession;
    }
    private MessageConsumer getConsumer(Destination destination) {
	    return getConsumer(destination,null);
    }
    private MessageConsumer getConsumer(Destination destination, String selectorString) {
	    MessageConsumer messageConsumer = null;
	    try {

            ActiveMQSession activeMQSession = getSession();

            if((activeMQSession != null) && (destination != null)) {

                if(selectorString == null) {
                    messageConsumer = activeMQSession.createConsumer(destination);
                } else {
                    messageConsumer = activeMQSession.createConsumer(destination, selectorString);
                }
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
	    return  messageConsumer;
    }

	public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString) {
	    String listenerId = null;
        try {

            MessageConsumer consumer = null;

            switch (topicType) {
                case AGENT:
                    if(selectorString == null) {
                        consumer = getConsumer(agentTopic);
                    } else {
                        consumer = getConsumer(agentTopic, selectorString);
                    }
                    break;
                case REGION:
                    if(selectorString == null) {
                        consumer = getConsumer(regionTopic);
                    } else {
                        consumer = getConsumer(regionTopic, selectorString);
                    }
                    break;
                case GLOBAL:
                    if(selectorString == null) {
                        consumer = getConsumer(globalTopic);
                    } else {
                        consumer = getConsumer(globalTopic, selectorString);
                    }
                    break;
            }
            if(consumer != null) {
                consumer.setMessageListener(messageListener);

                listenerId = UUID.randomUUID().toString();
                synchronized (lockMessage) {
                    messageConsumerMap.put(listenerId, consumer);
                }
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return listenerId;
    }

    public void removeMessageListener(String listenerId) {
	    try {
	        synchronized (lockMessage) {
                MessageConsumer consumer = messageConsumerMap.get(listenerId);
                if (consumer != null) {
                    try {
                        logger.trace("removeMessageListener: closing listener : " + listenerId);
                        logger.trace("removeMessageListener: message selector : " + consumer.getMessageSelector());

                        consumer.close();

                        messageConsumerMap.remove(listenerId);
                    } catch (JMSException e) {
                        logger.error("Failed to close message listener [{}]", listenerId);
                    }
                } else {
                    logger.error("removeMessageListener close called on unknown listener_id: " + listenerId);
                }
            }
        } catch (Exception e) {
	        logger.error("removeMessageListener('{}'): {}", listenerId, e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
    }

    public boolean sendMessage(TopicType topicType, Message message) {
        try {

            while(!controllerEngine.cstate.isActive()) {
                Thread.sleep(1000);
                logger.debug("!controllerEngine.cstate.isActive() SLEEPING 1s");
            }

            switch (topicType) {
                case AGENT:
                    if(agentProducer == null) {
                        agentProducer = getMessageProducer(topicType);
                        if(agentProducer == null) {
                            return false;
                        }
                    }
                    //if has header, send blob
                    Object inputObject = message.getObjectProperty("data_stream");
                    if(inputObject != null) {
                        InputStream inputStream = (InputStream) inputObject;
                        ActiveMQSession activeMQSession = getSession();
                        if(activeMQSession != null) {
                            BlobMessage blobMessage = activeMQSession.createBlobMessage(inputStream);
                            agentProducer.send(blobMessage, DeliveryMode.NON_PERSISTENT, 0, 0);
                        }
                    } else {
                        if(agentProducer != null) {
                            agentProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                        }
                    }
                    break;
                case REGION:
                    if(regionProducer == null) {
                        regionProducer = getMessageProducer(topicType);
                    }
                    if(regionProducer != null) {
                        regionProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    }
                    break;
                case GLOBAL:
                    if(globalProducer == null) {
                        globalProducer = getMessageProducer(topicType);
                    }
                    if(globalProducer != null) {
                        globalProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    }
                    break;
            }

            return true;
        } catch (JMSException jmse) {
            jmse.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            jmse.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
            return false;
        }
        catch (Exception ex) {
            logger.error(ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
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

        ActiveMQSession activeMQSession = getSession();

        if(activeMQSession != null) {

            try {
                switch (topicType) {
                    case AGENT:
                        messageProducer = activeMQSession.createProducer(agentTopic);
                        break;
                    case REGION:
                        messageProducer = activeMQSession.createProducer(regionTopic);
                        break;
                    case GLOBAL:
                        messageProducer = activeMQSession.createProducer(globalTopic);
                        break;
                }

                if(messageProducer != null) {
                    messageProducer.setTimeToLive(300000L);
                    messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                String sStackTrace = sw.toString(); // stack trace as a string
                logger.error(sStackTrace);
            }
        }

        return messageProducer;
    }

    public BytesMessage createBytesMessage() {
        BytesMessage bytesMessage = null;
	    try {
	        ActiveMQSession activeMQSession = getSession();
	        if(activeMQSession != null) {
                bytesMessage = activeMQSession.createBytesMessage();
            }

        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return bytesMessage;
	}

    public MapMessage createMapMessage() {
        MapMessage mapMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                mapMessage = activeMQSession.createMapMessage();
            }

        } catch (Exception ex){
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
	    return mapMessage;
    }

    public Message createMessage() {
	    Message message = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                message = activeMQSession.createMessage();
            }

        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return message;
    }

    public ObjectMessage createObjectMessage() {
	    ObjectMessage objectMessage = null;

	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                objectMessage = activeMQSession.createObjectMessage();
            }
        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
	    return objectMessage;
    }

    //blobs are not part of JMX, they are part of ActiveMQ, which is not in the core lib
    //for now we must use i/o streams

    public BlobMessage createBlobMessage(URL url) {
	    BlobMessage blobMessage = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(url);
            }
        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
	    return blobMessage;
    }

    public BlobMessage createBlobMessage(File file) {
        BlobMessage blobMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(file);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return blobMessage;
    }

    public BlobMessage createBlobMessage(InputStream inputStream) {
        BlobMessage blobMessage = null;
        try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                blobMessage = activeMQSession.createBlobMessage(inputStream);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return blobMessage;

    }


    public StreamMessage createStreamMessage() {
	    StreamMessage streamMessage = null;
	    try{
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                streamMessage = activeMQSession.createStreamMessage();
            }

        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return streamMessage;
    }

    public TextMessage createTextMessage() {
	    TextMessage textMessage = null;
	    try {
            ActiveMQSession activeMQSession = getSession();
            if(activeMQSession != null) {
                textMessage = activeMQSession.createTextMessage();
            }

        } catch (Exception ex) {
	        ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return textMessage;
    }

    /*
    private String getCEPPluginId() {
	    String pluginId = null;
	    try {
            List<Map<String, String>> configMapList = controllerEngine.getGDB().getPluginListMapByType("pluginname", "io.cresco.cep");
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
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return pluginId;
    }
     */

    public String createCEP(String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {
        String cepId = UUID.randomUUID().toString();
        if(cepEngine.createCEP(cepId,inputStreamName,inputStreamDefinition,outputStreamName,outputStreamDefinition,queryString)) {
            return cepId;
        } else {
            return null;
        }
	}

	/*
    public String createCEP2(String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {

	    String pluginId = getCEPPluginId();
        String cepId = null;

        if(pluginId != null) {

            cepId = UUID.randomUUID().toString();

            MsgEvent createQuery = plugin.getPluginMsgEvent(MsgEvent.Type.CONFIG, pluginId);
            createQuery.setParam("action", "queryadd");

            createQuery.setParam("input_stream_name", inputStreamName);
            createQuery.setParam("input_stream_definition", inputStreamDefinition);
            createQuery.setParam("output_stream_name", outputStreamName);
            createQuery.setParam("output_stream_definition", outputStreamDefinition);
            createQuery.setParam("query_id", cepId);
            createQuery.setParam("query", queryString);

            MsgEvent response = plugin.sendRPC(createQuery);
            if(response != null) {
                if(response.paramsContains("status_code")) {
                    int statusCode = Integer.parseInt(response.getParam("status_code"));
                    if(statusCode != 10) {
                        logger.error("Unable to create CEP Instance status_code: " + statusCode + " status_desc: " + response.getParam("status_desc"));
                        return null;
                    }
                } else {
                    logger.error("Unable to create CEP Instance no status_code ");
                    return null;
                }
            } else {
                logger.error("Unable to create CEP Instance RPC message was null ");
                return null;
            }

        } else {
            logger.error("No CEP Engine found on agent!");
        }
        return cepId;

    }
    */

    public void inputCEP(String streamName, String jsonPayload) {

	    try {
            TextMessage tickle = createTextMessage();
            tickle.setText(jsonPayload);
            tickle.setStringProperty("stream_name", streamName);

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, tickle);

        } catch (Exception ex) {
	        logger.error("inputCEP Error: " + ex.getMessage());
        }

    }

    public boolean removeCEP(String cepId) {
        return cepEngine.removeCEP(cepId);
    }

    /*
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
     */

    public Path getJournalPath() {
	    return journalPath;
	}

    public List<FileObject> createFileObjects(List<String> fileList) {
        List<FileObject> fileObjects = null;
        try {

            fileObjects = new ArrayList<>();

            for(String filePath : fileList) {
                fileObjects.add(createFileObject(filePath));
            }

        }catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return fileObjects;
    }

    public FileObject createFileObject(String fileName) {
        FileObject fileObject = null;
        try {

            File inFile = new File(fileName);
            if(inFile.exists()) {
                String dataName = UUID.randomUUID().toString();
                String fileMD5Hash = getMD5(fileName);

                Map<String, String> dataMap = splitFile(dataName, fileName);
                fileObject = new FileObject(inFile.getName(),fileMD5Hash,dataMap,dataName);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return fileObject;
    }

    public Map<String,String> splitFile(String dataName, String fileName)  {

        Map<String,String> filePartNames = null;
        try {

            File f = new File(fileName);

            //try-with-resources to ensure closing stream
            FileInputStream fis = new FileInputStream(f);

            filePartNames = streamToSplitFile(dataName, fis);

        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return filePartNames;
    }

    public Map<String,String> streamToSplitFile(String dataName, InputStream is)  {

        Map<String,String> filePartNames = null;
        try {


            filePartNames = new HashMap<>();

            int partCounter = 0;//I like to name parts from 001, 002, 003, ...
            //you can change it to 0 if you want 000, 001, ...

            int sizeOfFiles = 1024 * 1024 * 5;// 1MB
            byte[] buffer = new byte[sizeOfFiles];


            //String fileName = UUID.randomUUID().toString();

            //try-with-resources to ensure closing stream
            try (BufferedInputStream bis = new BufferedInputStream(is)) {

                int bytesAmount = 0;
                while ((bytesAmount = bis.read(buffer)) > 0) {
                    //write each chunk of data into separate file with different number in name
                    //String filePartName = String.format("%s.%03d", fileName, partCounter++);

                    String filePartName = dataName + "." + partCounter;
                    //MessageDigest m= MessageDigest.getInstance("MD5");
                    //m.update(buffer);
                    //String md5Hash = new BigInteger(1,m.digest()).toString(16);

                    partCounter++;

                    Path filePath = Paths.get(journalPath.toAbsolutePath().toString() + "/" + dataName);
                    Files.createDirectories(filePath);

                    File newFile = new File(filePath.toAbsolutePath().toString(), filePartName);
                    try (FileOutputStream out = new FileOutputStream(newFile)) {
                        out.write(buffer, 0, bytesAmount);
                    }

                    String md5Hash = getMD5(newFile.getAbsolutePath());
                    filePartNames.put(filePartName, md5Hash);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        return filePartNames;
    }

    public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
	    Path returnFilePath = null;

	    try {

            MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC,remoteRegion,remoteAgent);
            me.setParam("action","getfileinfo");
            me.setParam("filepath",remoteFilePath);

            MsgEvent re  = plugin.sendRPC(me);

            if(re.paramsContains("md5") && re.paramsContains("size")) {

                String rmd5 = re.getParam("md5");
                long fileSize = Long.parseLong(re.getParam("size"));

                int sizeOfFilePart = 1024 * 1024 * 5;// 5MB

                if(fileSize <= sizeOfFilePart) {
                    //send request for file directly

                    Path filePath = Paths.get(localFilePath);

                    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile())) {

                        MsgEvent dme = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, remoteRegion, remoteAgent);
                        dme.setParam("action", "getfiledata");
                        dme.setParam("filepath", remoteFilePath);
                        dme.setParam("skiplength", "0");
                        dme.setParam("partsize", String.valueOf(fileSize));

                        MsgEvent rdme = plugin.sendRPC(dme);
                        fileOutputStream.write(rdme.getDataParam("payload"));
                    }

                } else { //we need to break up the file

                    long fileDataRemaining = fileSize;
                    long skipLength = 0;

                    Path filePath = Paths.get(localFilePath);

                    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile())) {

                        while (fileDataRemaining != 0) {
                            //loop through writing files

                            if (sizeOfFilePart >= fileDataRemaining) {
                                sizeOfFilePart = (int) fileDataRemaining;
                            }

                            //System.out.println("Size of Data Part " + sizeOfFilePart + " Data Remaining = " + fileDataRemaining + " skipLength " + skipLength);
                            MsgEvent dme = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.EXEC, remoteRegion, remoteAgent);
                            dme.setParam("action", "getfiledata");
                            dme.setParam("filepath", remoteFilePath);
                            dme.setParam("skiplength", String.valueOf(skipLength));
                            dme.setParam("partsize", String.valueOf(sizeOfFilePart));

                            MsgEvent rdme = plugin.sendRPC(dme);
                            fileOutputStream.write(rdme.getDataParam("payload"));

                            fileDataRemaining = fileDataRemaining - sizeOfFilePart;
                            skipLength += sizeOfFilePart;

                        }
                    }
                }

                //check if file is correct
                String lmd5 = plugin.getMD5(localFilePath);
                if(lmd5.equals(rmd5)) {
                    returnFilePath = Paths.get(localFilePath);
                }

            }

        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
	        logger.error("downloadRemoteFile() " + errors.toString());
        }
	    return returnFilePath;
    }


    public String getMD5(String filePath) {
        String hashString = null;
        try {
            //Get file input stream for reading the file content
            try (FileInputStream fis = new FileInputStream(filePath)) {

                MessageDigest digest = MessageDigest.getInstance("MD5");

                //Create byte array to read data in chunks
                byte[] byteArray = new byte[1024];
                int bytesCount = 0;

                //Read file data and update in message digest
                while ((bytesCount = fis.read(byteArray)) != -1) {
                    digest.update(byteArray, 0, bytesCount);
                }

                //close the stream; We don't need it now.
                fis.close();

                //Get the hash's bytes
                byte[] bytes = digest.digest();

                //This bytes[] has bytes in decimal format;
                //Convert it to hexadecimal format
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < bytes.length; i++) {
                    sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
                }

                hashString = sb.toString();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
        //return complete hash
        return hashString;
    }

    public void mergeFiles(List<File> files, File into, boolean deleteParts) {

        try {

            try (FileOutputStream fos = new FileOutputStream(into);
                 BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
                for (File f : files) {
                    Files.copy(f.toPath(), mergingStream);
                    if (deleteParts) {
                        f.delete();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }
    }

    public List<FileObject> getFileObjectsFromString(String fileObjectsString){

        return gson.fromJson(fileObjectsString,typeOfListFileObject);
    }

    public String generateFileObjectsString(List<FileObject> fileObjects){
	    return gson.toJson(fileObjects);
    }

}



