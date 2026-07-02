package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;
import jakarta.jms.*;
import java.io.File;
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

	private Gson gson;
	public boolean isActive;
	private String TXQueueName;

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

		} catch (Exception e) {
			logger.error("ActiveProducerWorkerData.<init> Constructor {}", e.getMessage(), e);
		}
	}

	boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}

	public void run() {
		try {
			ActiveMQSession dataSess = null;
			MessageProducer dataProducer = null;

			try {

				dataSess = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

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

				dataProducer.send(textMessage, DeliveryMode.PERSISTENT, 0, 0);

				dataProducer.close();

				for(FileObject fileObject : fileObjectList) {

					Path filePath = Paths.get(controllerEngine.getDataPlaneService().getJournalPath().toAbsolutePath() + System.getProperty("file.separator") + fileObject.getDataName());

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

						String journalDirPath = null;

						String cresco_data_location = System.getProperty("cresco_data_location");
						if(cresco_data_location != null) {
							Path path = Paths.get(cresco_data_location, "producer-journal");
							journalDirPath = plugin.getConfig().getStringParam("journal_dir", path.toAbsolutePath().normalize().toString());

						} else {
							journalDirPath = plugin.getConfig().getStringParam("journal_dir", FileSystems.getDefault().getPath("cresco-data/producer-journal").toAbsolutePath().toString());
						}

						Path journalPath = Paths.get(journalDirPath);
						Files.createDirectories(journalPath);


						File filePart = new File(filePath.toAbsolutePath().toString(), parList);

						byte[] fileContent = Files.readAllBytes(filePart.toPath());

						bytesMessage.writeBytes(fileContent);

						try {

							dataProducer.send(bytesMessage, DeliveryMode.PERSISTENT, 0, 0);

						} catch (JMSException jmse) {
							logger.error("ActiveProducerWorkerData.run sendMessage Data: jmse {} ", jmse.getMessage(), jmse);

							try {
								logger.error("Rebuilding Session");
								// close the broken session before replacing it so we don't leak it
								try { if (dataSess != null) dataSess.close(); } catch (Exception ignore) { /* already broken */ }
								dataSess = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
								dataProducer = dataSess.createProducer(dataDestination);
								dataProducer.setTimeToLive(0);
								dataProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
								dataProducer.send(bytesMessage, DeliveryMode.PERSISTENT, 0, 0);
							} catch (Exception ex) {
								logger.error("ActiveProducerWorkerData.run Rebuilding Session Error " + ex.getMessage(), ex);
							}
						} catch (Exception ex) {
							logger.error("ActiveProducerWorkerData.run General send failure : " + ex.getMessage(), ex);
						} finally {
							if(dataProducer != null) {
								dataProducer.close();
							}
						}
						filePart.delete();
					}
					//remove temp folder
					//filePath.toFile().delete();
					deleteDirectory(filePath.toFile());
				}

			} catch (JMSException jmse) {
				logger.error("ActiveProducerWorkerData.run sendMessage: jmse {} : {}", me.getParams(), jmse.getMessage(), jmse);
			}
			catch (Exception ex) {
				logger.error("ActiveProducerWorkerData.run ERROR SENDING FILE MESSAGE", ex);
			} finally{
				try {
					if (dataProducer != null) {
						dataProducer.close();
					}
					if(dataSess != null) {
						dataSess.close();
					}
				}catch (Exception ex) {
					logger.error("ActiveProducerWorkerData.run Can't Close data producer", ex);
				}
			}



		} catch (Exception ex) {
			logger.error("ActiveProducerWorkerData.run run(): " +  ex.getMessage(), ex);
		}
	}

}