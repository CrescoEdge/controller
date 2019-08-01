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
            String journalDirPath = plugin.getConfig().getStringParam("journal_dir", FileSystems.getDefault().getPath("journal").toAbsolutePath().toString());
            journalPath = Paths.get(journalDirPath);
            //remove old files if they exist from the journal
            if(journalPath.toFile().exists()) {

                try (Stream<Path> journalWalk = Files.walk(journalPath)) {
                            journalWalk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            Files.createDirectories(journalPath);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

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
	        ex.printStackTrace();
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
	        ex.printStackTrace();
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
	        ex.printStackTrace();
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
            ex.printStackTrace();
        }
        return listenerId;
    }

    public void removeMessageListener(String listenerId) {
	    try {
	        synchronized (lockMessage) {
                MessageConsumer consumer = messageConsumerMap.get(listenerId);
                if (consumer != null) {
                    try {
                        consumer.close();
                        messageConsumerMap.remove(listenerId);
                    } catch (JMSException e) {
                        logger.error("Failed to close message listener [{}]", listenerId);
                    }
                }
            }
        } catch (Exception e) {
	        logger.error("removeMessageListener('{}'): {}", listenerId, e.getMessage());
        }
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
                    //if has header, send blob
                    Object inputObject = message.getObjectProperty("data_stream");
                    if(inputObject != null) {
                        InputStream inputStream = (InputStream) inputObject;
                        BlobMessage blobMessage = getSession().createBlobMessage(inputStream);
                        agentProducer.send(blobMessage, DeliveryMode.NON_PERSISTENT, 0, 0);
                    } else {
                        agentProducer.send(message, DeliveryMode.NON_PERSISTENT, 0, 0);
                    }
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
        }
    }

    public List<FileObject> getFileObjectsFromString(String fileObjectsString){

        return gson.fromJson(fileObjectsString,typeOfListFileObject);
    }

    public String generateFileObjectsString(List<FileObject> fileObjects){
	    return gson.toJson(fileObjects);
    }

}



