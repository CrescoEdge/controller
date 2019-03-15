package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;

import javax.jms.*;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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


			if(se.hasFiles()) {

				Thread thread = new Thread(){

					public void run(){
						try {
							TextMessage textMessage = sess.createTextMessage(gson.toJson(se));

							//break apart the files and create manafest
							List<FileObject> fileObjectList = controllerEngine.getDataPlaneService().createFileObjects(se.getFileList());
							textMessage.setStringProperty("fileobjects", gson.toJson(fileObjectList));

							//send initial message to register the transfer
							producer.send(textMessage, DeliveryMode.NON_PERSISTENT, 10, 0);

							for(FileObject fileObject : fileObjectList) {
								for (String parList : fileObject.getOrderedPartList()) {

									BytesMessage bytesMessage = sess.createBytesMessage();
									bytesMessage.setStringProperty("datapart", parList);
									bytesMessage.setStringProperty("dataname", fileObject.getDataName());

									String journalDirPath = plugin.getConfig().getStringParam("journal_dir", FileSystems.getDefault().getPath("journal").toAbsolutePath().toString());
									Path journalPath = Paths.get(journalDirPath);
									Files.createDirectories(journalPath);

									File filePart = new File(journalPath.toAbsolutePath().toString(), parList);

									System.out.println("READING FILE TO MESSAGE : " + filePart.getAbsolutePath() + " " + parList);

									byte[] fileContent = Files.readAllBytes(filePart.toPath());
									bytesMessage.writeBytes(fileContent);
									producer.send(bytesMessage);
									filePart.delete();

								}
							}


						} catch (Exception ex) {
							logger.error("ERROR SENDING FILE MESSAGE");
							ex.printStackTrace();
						}
					}
				};

				thread.start();


			} else {
				TextMessage textMessage = sess.createTextMessage(gson.toJson(se));
				producer.send(textMessage, DeliveryMode.NON_PERSISTENT, pri, 0);
			}

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