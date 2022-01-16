package io.cresco.agent.controller.communication;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import javax.jms.Queue;
import javax.jms.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AgentConsumer {
	private PluginBuilder plugin;
	private CLogger logger;
	private Queue RXqueue;
	private ActiveMQSession sess;
	private ControllerEngine controllerEngine;
    private MessageConsumer consumer;
	private Gson gson;

	private AtomicBoolean lockGroupMap = new AtomicBoolean();
	private Map<String, FileObjectGroupReceiver> fileGroupMap;

	private Cache<String, MsgEvent> fileMsgEventCache;

	public AgentConsumer(ControllerEngine controllerEngine, String RXQueueName, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(AgentConsumer.class.getName(),CLogger.Level.Trace);

		this.gson = new Gson();

		this.fileGroupMap = Collections.synchronizedMap(new HashMap<>());


		//use expiring cache to trigger cleanup of files and messages
		RemovalListener<String, MsgEvent> listener;
		listener = new RemovalListener<String, MsgEvent>() {
			@Override
			public void onRemoval(RemovalNotification<String, MsgEvent> n){
				if (n.wasEvicted()) {
					String cause = n.getCause().name();
					//assertEquals(RemovalCause.SIZE.toString(),cause);
					logger.error("IMPLEMENT REMOVAL OF FILES!");
					logger.error(cause);
					logger.error("key: " + n.getKey());
					logger.error("value: " + n.getValue().getParams().toString());
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


		sess = controllerEngine.getActiveClient().createSession(URI,false, Session.AUTO_ACKNOWLEDGE);

		RXqueue = sess.createQueue(RXQueueName);
		consumer = sess.createConsumer(RXqueue);

		Gson gson = new Gson();

		String callIdKey = "callId-" + plugin.getRegion() + "-" + plugin.getAgent() + "-" + plugin.getPluginID();


		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message msg) {
				try {


					if (msg instanceof TextMessage) {

						String filegroup = msg.getStringProperty("filegroup");
						String fileobjectString = msg.getStringProperty("fileobjects");
						String msgEventString = ((TextMessage) msg).getText();

						MsgEvent me = gson.fromJson(msgEventString,MsgEvent.class);
						if(me != null) {






							//logger.info("TEST PLUG 0");
							//logger.info("Q: " + RXQueueName);
							//logger.info("TEST PLUG " + controllerEngine.cstate.getAgentPath());
							//logger.info("TEST PLUG 1");

							//check if message is for this agent
							//check if message has file payload and needs to be registered
							if((filegroup != null) && (me.getDstRegion().equals(plugin.getRegion())) && (me.getDstAgent().equals(plugin.getAgent()))) {
								if(!registerIncomingFiles(msgEventString, fileobjectString, filegroup)) {
									logger.error("Unable to register files!");
								}
								//don't forward message until files have arrived
								return;
							}



							//start RPC checks
							boolean isMyRPC = false;

							if (me.getParams().containsKey("is_rpc")) {

								//pick up self-rpc, unless ttl == 0
								//String callId = me.getParam(("callId-" + plugin.getRegion() + "-" +
								//		plugin.getAgent() + "-" + plugin.getPluginID()));
								String callId = me.getParam(callIdKey);


								if (callId != null) {
									isMyRPC = true;
									logger.trace("RPC Text Message: {}", me.getParams().toString());
									plugin.receiveRPC(callId, me);
								}
							}

							if(!isMyRPC) {
								logger.trace("Text Message: {}", me.getParams().toString());
								//create new thread service for incoming messages
								controllerEngine.msgInThreaded(me);
							}

							//
						} else {
							logger.error("non-MsgEvent message found!");
						}

					} else if( msg instanceof BytesMessage) {

						String filegroup = msg.getStringProperty("filegroup");
						String dstRegion = msg.getStringProperty("dst_region");
						String dstAgent = msg.getStringProperty("dst_agent");
						String dataName = msg.getStringProperty("dataname");
						String dataPart = msg.getStringProperty("datapart");


						logger.debug("Byte Message: dst_region: " + dstRegion + " dst_agent: " + dstAgent);

						if((filegroup != null) && (dstRegion != null) && (dstAgent != null) && (dataName != null) && (dataPart != null)) {

							//logger.info("MSG REGION: " + dstRegion + " MSG AGENT: " + dstAgent);

							//logger.info("REGION: " + plugin.getRegion() + " AGENT: " + plugin.getAgent());


							if((dstRegion.equals(plugin.getRegion())) && (dstAgent.equals(plugin.getAgent()))) {

								boolean groupExist = false;
								synchronized (lockGroupMap) {
									if(fileGroupMap.containsKey(filegroup)) {
										groupExist = true;
									}
								}

								if(groupExist) {


									Path filePath = Paths.get(controllerEngine.getDataPlaneService().getJournalPath().toAbsolutePath() + System.getProperty("file.separator") + dataName);
									Files.createDirectories(filePath);

									File filePart = new File(filePath.toAbsolutePath().toString(), dataPart);

									byte[] data = new byte[(int) ((BytesMessage) msg).getBodyLength()];
									((BytesMessage) msg).readBytes(data);

									Files.write(filePart.toPath(), data);

									String filePartMD5Hash = plugin.getMD5(filePart.getAbsolutePath());

									//System.out.println("INCOMING HASH: " + filePartMD5Hash);

									boolean isPartComplete = false;

									List<String> orderedFilePartNameList = null;
									String combinedFileName = null;
									String combinedFileHash = null;

									synchronized (lockGroupMap) {
										fileGroupMap.get(filegroup).setDestFilePart(dataName, dataPart, filePartMD5Hash);
										//check if file is complete
										isPartComplete = fileGroupMap.get(filegroup).isFilePartComplete(dataName);
										if(isPartComplete) {
											orderedFilePartNameList = fileGroupMap.get(filegroup).getOrderedPartList(dataName);
											combinedFileName = fileGroupMap.get(filegroup).getFileName(dataName);
											combinedFileHash = fileGroupMap.get(filegroup).getFileMD5Hash(dataName);
										}
									}

									if(isPartComplete) {
										List<File> orderedFilePartList = new ArrayList<>();
										File combinedFile = new File(filePath.toAbsolutePath().toString(), combinedFileName);

										for(String filePartName : orderedFilePartNameList) {
											File partFile = new File(filePath.toAbsolutePath().toString(), filePartName);
											if(partFile.exists()) {
												//System.out.println("File Part : " + partFile.getName());
												orderedFilePartList.add(partFile);
											} else {
												//System.out.println("File Part : " + partFile.getName() + " DOES NOT EXIST");
											}
										}

										controllerEngine.getDataPlaneService().mergeFiles(orderedFilePartList, combinedFile, true);
										if(combinedFile.exists()) {

											String localCombinedFilePath = combinedFile.getAbsolutePath();
											String localCombinedFileHash = plugin.getMD5(localCombinedFilePath);
											//System.out.println("File: " + localCombinedFileHash + " original_hash:" + combinedFileHash + " local_hash:" + localCombinedFileHash);

											if(combinedFileHash.equals(localCombinedFileHash)) {
												//System.out.println("WE HAVE A FILE!!! " + localCombinedFilePath);

												MsgEvent me = null;
												boolean isGroupComplete = false;
												List<String> newFilePaths = null;
												synchronized (lockGroupMap) {
													fileGroupMap.get(filegroup).setFileComplete(dataName);
													isGroupComplete = fileGroupMap.get(filegroup).isFileGroupComplete();
													if(isGroupComplete) {
														me = fileGroupMap.get(filegroup).getMsgEvent();
														newFilePaths = fileGroupMap.get(filegroup).getFileList(controllerEngine.getDataPlaneService().getJournalPath());
													}
												}

												if(isGroupComplete) {
													//do something with the group
													//logger.info("GROUP COMPLETE!!");
													//rebuild files on original MsgEvent
													//List<String> msgEventFileList = new ArrayList<>();
													//msgEventFileList.addAll(me.getFileList());

													if(me == null) {
														logger.error("groupcomplete: " + isGroupComplete + " message null");
													} else {

														me.clearFileList();
														for(String newfilePath : newFilePaths) {
															me.addFile(newfilePath);
														}
														
														//logger.error("message != null " + me.getParams().toString());
														//save message for cache removal of files
														fileMsgEventCache.put(filegroup, me);
														//logger.error("SENDING MESSAGE NOW");
														//send final message
														controllerEngine.msgInThreaded(me);
													}
												}

											}

										} else {
											logger.error("ERROR COMBINING FILE : " + combinedFileHash);
											//System.out.println("ERROR COMBINING FILE : " + combinedFileHash);
										}

									}



								}


							} else {
								logger.error("MUST CREATE METHOD TO FORWARD DATA MESSAGES");
							}
						}

					} else if (msg instanceof BlobMessage) {
						logger.error("Blob message recieved!");
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

	private boolean registerIncomingFiles(String msgEventString, String fileobjectString, String fileGroup) {
		boolean isRegistered = false;
		try {

			MsgEvent me = gson.fromJson(msgEventString,MsgEvent.class);

			List<FileObject> fileObjects = controllerEngine.getDataPlaneService().getFileObjectsFromString(fileobjectString);
			Map<String,FileObject> fileObjectMap = new HashMap<>();

			for(FileObject fileObject : fileObjects) {
				fileObjectMap.put(fileObject.getDataName(), fileObject);
			}

			FileObjectGroupReceiver fileObjectGroupReceiver = new FileObjectGroupReceiver(me,fileObjectMap,fileGroup);

			synchronized (lockGroupMap) {
				fileGroupMap.put(fileGroup,fileObjectGroupReceiver);
			}
			isRegistered = true;


		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failure to Register File Message");
		}
		return isRegistered;
	}

	public void shutdown() {
		try {
			consumer.close();
		} catch (Exception ex) {
			logger.error("Consumer Shutdown Error: " + ex.getMessage());
		}
	}

}