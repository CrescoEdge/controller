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

			sess = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);

			destination = sess.createQueue(TXQueueName);


		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
			e.printStackTrace();
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

				dataProducer.send(textMessage, DeliveryMode.PERSISTENT, 7, 0);

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
							jmse.printStackTrace();
							logger.error("sendMessage Data: jmse {} ", jmse.getMessage());
							StringWriter errors = new StringWriter();
							jmse.printStackTrace(new PrintWriter(errors));
							logger.error(errors.toString());

							try {
								logger.error("Rebuilding Session");
								dataSess = controllerEngine.getActiveClient().createSession(URI, false, Session.AUTO_ACKNOWLEDGE);
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
					//filePath.toFile().delete();
					deleteDirectory(filePath.toFile());
				}

			} catch (JMSException jmse) {
				logger.error("run() sendMessage: jmse {} : {}", me.getParams(), jmse.getMessage());
				StringWriter errors = new StringWriter();
				jmse.printStackTrace(new PrintWriter(errors));
				logger.error(errors.toString());
			}
			catch (Exception ex) {
				logger.error("ERROR SENDING FILE MESSAGE");
				StringWriter errors = new StringWriter();
				ex.printStackTrace(new PrintWriter(errors));
				logger.error(errors.toString());
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