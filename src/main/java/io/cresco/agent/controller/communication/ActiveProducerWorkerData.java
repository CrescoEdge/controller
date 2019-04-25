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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class ActiveProducerWorkerData implements Runnable {
	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private String producerWorkerName;
	private CLogger logger;
	private ActiveMQSession sess;

	private Gson gson;
	public boolean isActive;
	private String TXQueueName;
	private Destination destination;

	private String URI;

	private MsgEvent me;

	public ActiveProducerWorkerData(ControllerEngine controllerEngine, String TXQueueName, String URI, MsgEvent me)  {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(ActiveProducerWorkerData.class.getName(),CLogger.Level.Info);
		this.me = me;

		this.URI = URI;
		this.producerWorkerName = UUID.randomUUID().toString();
		try {
			this.TXQueueName = TXQueueName;
			gson = new Gson();

			sess = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

			destination = sess.createQueue(TXQueueName);


		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
			e.printStackTrace();
		}
	}


	public void run() {
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

			String type = me.getMsgType().toString();

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


					ActiveMQSession dataSess = null;
			 		MessageProducer dataProducer = null;

						try {

							dataSess = (ActiveMQSession)controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

							TextMessage textMessage = dataSess.createTextMessage(gson.toJson(me));

							String fileGroup = UUID.randomUUID().toString();

							//break apart the files and create manafest
							List<FileObject> fileObjectList = controllerEngine.getDataPlaneService().createFileObjects(me.getFileList());
							textMessage.setStringProperty("fileobjects", gson.toJson(fileObjectList));
							textMessage.setStringProperty("filegroup",fileGroup);

							//send initial message to register the transfer
							//create new producer and make sure it does not timeout
							Destination dataDestination = dataSess.createQueue(TXQueueName);
							dataProducer = dataSess.createProducer(dataDestination);
							dataProducer.setTimeToLive(0);
							dataProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

							dataProducer.send(textMessage, DeliveryMode.PERSISTENT, 10, 0);

							dataProducer.close();

							for(FileObject fileObject : fileObjectList) {

								Path filePath = Paths.get(controllerEngine.getDataPlaneService().getJournalPath().toAbsolutePath().toString() + "/" + fileObject.getDataName());

								for (String parList : fileObject.getOrderedPartList()) {

									dataProducer = dataSess.createProducer(dataDestination);
									dataProducer.setTimeToLive(0);
									dataProducer.setDeliveryMode(DeliveryMode.PERSISTENT);


									BytesMessage bytesMessage = dataSess.createBytesMessage();
									bytesMessage.setStringProperty("datapart", parList);
									bytesMessage.setStringProperty("dataname", fileObject.getDataName());
									bytesMessage.setStringProperty("filegroup", fileGroup);
									bytesMessage.setStringProperty("dst_region", me.getDstRegion());
									bytesMessage.setStringProperty("dst_agent", me.getDstAgent());

									//bytesMessage.setStringProperty("JMSXGroupID", fileObject.getDataName());

									String journalDirPath = plugin.getConfig().getStringParam("journal_dir", FileSystems.getDefault().getPath("journal").toAbsolutePath().toString());
									Path journalPath = Paths.get(journalDirPath);
									Files.createDirectories(journalPath);


									File filePart = new File(filePath.toAbsolutePath().toString(), parList);

									//System.out.println("READING FILE TO BYTES : " + filePart.getAbsolutePath() + " " + parList);

									byte[] fileContent = Files.readAllBytes(filePart.toPath());

									//System.out.println("READ FILE TO BYTES : " + filePart.getAbsolutePath() + " " + parList + " bytes:" + fileContent.length);

									//System.out.println("READING FILE TO MESSAGE : " + filePart.getAbsolutePath() + " " + parList);

									bytesMessage.writeBytes(fileContent);

									//System.out.println("READ FILE TO MESSAGE : " + filePart.getAbsolutePath() + " " + parList + " Message Body Length:" + fileContent);

									//System.out.println("MESSAGE ID: " + bytesMessage.getStringProperty("JMSMessageID"));


									//give lowest priority to file transfers
									//todo fix this dirty hack
									try {
										//System.out.println("SESSION LAST DELIVERED 0" + dataSess.getLastDeliveredSequenceId());
										//System.out.println("SESSION NEXT DELEVERY ID 0" + dataSess.getNextDeliveryId());

										//System.out.println("SENDING MESSAGE FROM PRODUCER " + dataProducer.getDeliveryMode());
										dataProducer.send(bytesMessage, DeliveryMode.PERSISTENT, 0, 0);

										//System.out.println("SENT MESSAGE FROM PRODUCER " + dataProducer.getDeliveryMode());


										//System.out.println("SESSION LAST DELIVERED 1" + dataSess.getLastDeliveredSequenceId());
										//System.out.println("SESSION NEXT DELEVERY ID 1" + dataSess.getNextDeliveryId());



									} catch (JMSException jmse) {
										jmse.printStackTrace();
										logger.error("sendMessage Data: jmse {} ", jmse.getMessage());
										StringWriter errors = new StringWriter();
										jmse.printStackTrace(new PrintWriter(errors));
										logger.error(errors.toString());

										try {
											logger.error("Rebuilding Session");
											dataSess = (ActiveMQSession) controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
											dataProducer = dataSess.createProducer(dataDestination);
											dataProducer.setTimeToLive(0);
											dataProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
											dataProducer.send(bytesMessage, DeliveryMode.PERSISTENT, 0, 0);
										} catch (Exception ex) {
											logger.error("Rebuilding Session Error " + ex.getMessage());
											ex.printStackTrace();
										}
									} catch (Exception ex) {
										logger.error("General send failure : " + ex.getMessage());
										ex.printStackTrace();
									} finally {
										if(dataProducer != null) {
											dataProducer.close();
										}
									}
									filePart.delete();
								}
								//remove temp folder
								filePath.toFile().delete();
							}


						} catch (JMSException jmse) {
							logger.error("run() sendMessage: jmse {} : {}", me.getParams(), jmse.getMessage());
							StringWriter errors = new StringWriter();
							jmse.printStackTrace(new PrintWriter(errors));
							logger.error(errors.toString());
						}
						catch (Exception ex) {
							logger.error("ERROR SENDING FILE MESSAGE");
							ex.printStackTrace();
						} finally{
							try {
								if (dataProducer != null) {
									dataProducer.close();
								}
								if(dataSess != null) {
									dataSess.close();
								}
							}catch (Exception ex) {
								logger.error("Can't Close data producer");
								ex.printStackTrace();
							}
						}


		} catch (Exception ex) {
			logger.error("run(): " +  ex.getMessage());
			StringWriter errors = new StringWriter();
			ex.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

}