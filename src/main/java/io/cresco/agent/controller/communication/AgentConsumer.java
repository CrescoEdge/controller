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
import jakarta.jms.*;

import jakarta.jms.Queue;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import java.io.File;
import java.io.IOException;
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
	/** gfs#26: the consumer of the former address's inbox (region_alias, {@link RegionAlias}); null without one. */
	private MessageConsumer aliasConsumer;
	private String aliasRegion, aliasQueueName;
	private Gson gson;
	private boolean dedicatedConnection;
	private volatile org.apache.activemq.ActiveMQConnection connection;
	/** The inbox's broker URI (a re-init reuses it) and the reply correlation key its listener reads. */
	private String uri, callIdKey, RXQueueName;
	/** controller#24: shutdown wins over a re-init; one re-init at a time. */
	private volatile boolean closed;
	private final AtomicBoolean rebuilding = new AtomicBoolean();
	private volatile int rebuilds;
	static long REBUILD_INITIAL_BACKOFF_MS = 2_000, REBUILD_MAX_BACKOFF_MS = 30_000;

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

		// Transport isolation (OUT-81) and controller#24: the setup is re-runnable, and the dedicated connection's
		// failure listener rebuilds it, so the inbox never stays dead.
		this.uri = URI;
		this.RXQueueName = RXQueueName;
		this.callIdKey = "callId-" + plugin.getRegion() + "-" + plugin.getAgent() + "-" + plugin.getPluginID();
		// gfs#26: a node moved to another region keeps answering at its former address (region_alias)
		boolean global = controllerEngine.cstate != null && controllerEngine.cstate.isGlobalController();
		this.aliasRegion = RegionAlias.of(plugin, global);
		this.aliasQueueName = RegionAlias.aliasQueue(RXQueueName, aliasRegion, plugin.getRegion(), plugin.getAgent());
		if (aliasRegion != null && aliasQueueName == null) {
			logger.error("region_alias={} ignored: the inbox queue {} does not name this node", aliasRegion, RXQueueName);
			aliasRegion = null;
		}
		setupTransport(URI);

	}

	/**
	 * Create the session, the inbox queue, the consumer and its listener on {@code uri}. Re-runnable: the
	 * failure hook calls it again when the dedicated connection dies (controller#24).
	 */
	private synchronized void setupTransport(String uri) throws JMSException {
		// Transport isolation: the inbox carries ALL inbound control (WATCHDOG/EXEC/CONFIG, RPC
		// replies), so receiving it over the pooled socket lets inbound dataplane/bulk frames
		// delay control at the wire. Give the inbox its own socket. On vm:// (the agent's own broker)
		// the pooled connection is shared with the dataplane, so the inbox gets its own vm://
		// connection as well unless controlplane_dedicated_vm=false (OUT-81).
		this.dedicatedConnection = ActiveClient.dedicatedControlConnection(plugin.getConfig(), uri,
				"agentconsumer_dedicated_connection");
		sess = dedicatedConnection
				? controllerEngine.getActiveClient().createDedicatedSession(uri, false, Session.AUTO_ACKNOWLEDGE, true)
				: controllerEngine.getActiveClient().createSession(uri, false, Session.AUTO_ACKNOWLEDGE);
		if (sess == null) {
			throw new JMSException("AgentConsumer: unable to create session for uri " + uri);
		}
		if (dedicatedConnection) {
			logger.info("AgentConsumer inbox on dedicated control-plane connection for uri [{}]", uri);
		}
		this.connection = (org.apache.activemq.ActiveMQConnection) sess.getConnection();
		// controller#24: we own the dedicated socket, so its failure is ours to recover from. Chain to the listener
		// ActiveClient installed (it prunes the connection from its registry and closes it) and then rebuild the inbox.
		if (dedicatedConnection && connection != null) {
			final org.apache.activemq.ActiveMQConnection owned = connection;
			final ExceptionListener registry = owned.getExceptionListener();
			owned.setExceptionListener(ex -> {
				try { if (registry != null) registry.onException(ex); } catch (Exception ignore) { }
				onConnectionFailure(uri, owned, ex);
			});
		}

		// From here the dedicated socket is live and WE own it: if consumer setup fails (broker
		// authz denial, broker drop mid-setup) the constructor throws and the caller never gets a
		// reference to shut down — close the owned connection before rethrowing or it leaks open.
		try {
			RXqueue = sess.createQueue(RXQueueName);
			consumer = sess.createConsumer(RXqueue);
			if (aliasQueueName != null) {
				aliasConsumer = sess.createConsumer(sess.createQueue(aliasQueueName));
				logger.info("AgentConsumer also consumes [{}]: this node answers at its former address in region {} (region_alias)",
						aliasQueueName, aliasRegion);
			}
		} catch (JMSException | RuntimeException ex) {
			if (dedicatedConnection) {
				try { if (sess != null && !sess.isClosed()) sess.close(); } catch (Exception ignore) { }
				try { if (connection != null && !connection.isClosed()) connection.close(); } catch (Exception ignore) { }
			}
			throw ex;
		}


		MessageListener inbox = new MessageListener() {
			public void onMessage(Message msg) {
				try {


					if (msg instanceof TextMessage) {

						String filegroup = msg.getStringProperty("filegroup");
						String fileobjectString = msg.getStringProperty("fileobjects");
						String msgEventString = ((TextMessage) msg).getText();

						MsgEvent me = gson.fromJson(msgEventString,MsgEvent.class);
						if(me != null) {
							// gfs#26: addressed to this node's former address (region_alias): it is for this node
							RegionAlias.normalize(me, aliasRegion, plugin.getRegion(), plugin.getAgent());

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

						} else {
							logger.error("non-MsgEvent message found!");
						}

					} else if( msg instanceof BytesMessage) {

						String filegroup = msg.getStringProperty("filegroup");
						String dstRegion = msg.getStringProperty("dst_region");
						String dstAgent = msg.getStringProperty("dst_agent");
						// gfs#26: a file part addressed to this node's former address (region_alias) is for this node
						if (aliasRegion != null && aliasRegion.equals(dstRegion) && plugin.getAgent().equals(dstAgent)) {
							dstRegion = plugin.getRegion();
						}
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


									Path journalBase = controllerEngine.getDataPlaneService().getJournalPath().toAbsolutePath();
									Path filePath = resolveInJournal(journalBase, dataName);
									Files.createDirectories(filePath);

									File filePart = resolveInJournal(filePath, dataPart).toFile();

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
										File combinedFile = resolveInJournal(filePath, combinedFileName).toFile();

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
					logger.error("AgentConsumer.onMessage Error : " + ex.getMessage(), ex);
				}
			}
		};
		consumer.setMessageListener(inbox);
		if (aliasConsumer != null) aliasConsumer.setMessageListener(inbox);
	}

	/**
	 * controller#24: the dedicated inbox connection died (a broker or hub restart, a churn window). Rebuild it
	 * off-thread with backoff so the agent's control plane resumes without a JVM restart. One rebuild at a time;
	 * shutdown wins. A failure of a connection already replaced is ignored.
	 */
	private void onConnectionFailure(String uri, org.apache.activemq.ActiveMQConnection failed, JMSException ex) {
		if (closed || failed != connection) return;
		ControlPlaneFailures.INBOX_FAILURES.incrementAndGet();
		logger.error("AgentConsumer inbox connection failure for URI [{}]: {}; rebuilding the inbox", uri,
				ex == null || ex.getMessage() == null ? String.valueOf(ex) : ex.getMessage());
		if (!rebuilding.compareAndSet(false, true)) return;
		Thread t = new Thread(null, () -> {
			try {
				long backoff = REBUILD_INITIAL_BACKOFF_MS;
				while (!closed) {
					try {
						Thread.sleep(backoff);
						synchronized (this) {
							if (closed) return;
							logger.warn("AgentConsumer re-initializing the inbox for URI [{}] (backoff {} ms)", uri, backoff);
							teardownTransport();
							setupTransport(uri);
							rebuilds++;
						}
						logger.warn("AgentConsumer inbox re-initialized for URI [{}]", uri);
						return;
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						return;
					} catch (Exception retry) {
						ControlPlaneFailures.INBOX_REBUILD_FAILURES.incrementAndGet();
						logger.error("AgentConsumer inbox re-init failed for URI [{}]: {}; retrying", uri, retry.getMessage());
						backoff = Math.min(REBUILD_MAX_BACKOFF_MS, backoff * 2);
					}
				}
			} finally {
				rebuilding.set(false);
			}
		}, "agentconsumer-reinit", 1 << 20);
		t.setDaemon(true);
		t.start();
	}

	/** Close the current consumer, session and owned connection (the re-init follows). Never parks on a dead transport. */
	private synchronized void teardownTransport() {
		if (dedicatedConnection && connection != null) ActiveClient.disposeIfDead(connection);
		try { if (consumer != null) consumer.close(); } catch (Exception ignore) { }
		try { if (aliasConsumer != null) aliasConsumer.close(); } catch (Exception ignore) { }
		aliasConsumer = null;
		if (dedicatedConnection) {
			try { if (sess != null && !sess.isClosed()) sess.close(); } catch (Exception ignore) { }
			ActiveClient.closeConnectionFast(connection);
		}
		consumer = null;
	}

	/** How many times the inbox was rebuilt after a connection failure (tests, diagnostics). */
	int rebuilds() { return rebuilds; }

	/**
	 * Wire-supplied name components (dataname/datapart/group file names) must be simple names:
	 * legitimate senders use UUID-derived values, so separators, "..", or absolute paths are
	 * hostile. The resolved path must stay inside the journal directory.
	 */
	private Path resolveInJournal(Path baseDir, String name) throws IOException {
		if ((name == null) || name.isEmpty() || name.contains("/") || name.contains("\\")
				|| name.contains("..") || name.indexOf('\0') >= 0) {
			throw new IOException("rejecting unsafe file name component: [" + name + "]");
		}
		Path base = baseDir.toAbsolutePath().normalize();
		Path resolved = base.resolve(name).normalize();
		if (!resolved.startsWith(base)) {
			throw new IOException("rejecting file name escaping journal path: [" + name + "]");
		}
		return resolved;
	}

	/** Live state of the connection actually carrying the inbox (dedicated or pooled). */
	/** The connection the inbox consumes on. For tests. */
	org.apache.activemq.ActiveMQConnection getConnection() {
		return connection;
	}

	boolean isDedicatedConnection() {
		return dedicatedConnection;
	}

	public boolean isConnectionActive() {
		try {
			org.apache.activemq.ActiveMQConnection c = connection;
			// controller#24: a dedicated socket that died can still read as started and not closed; a failed
			// transport is not an active inbox (the fault watchers then see it too)
			return c != null && c.isStarted() && !c.isClosing() && !c.isClosed() && !ActiveClient.isTransportDead(c);
		} catch (Exception ex) {
			return false;
		}
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

			FileObjectGroupReceiver fileObjectGroupReceiver = new FileObjectGroupReceiver(plugin,me,fileObjectMap,fileGroup);

			synchronized (lockGroupMap) {
				fileGroupMap.put(fileGroup,fileObjectGroupReceiver);
			}
			isRegistered = true;


		} catch (Exception ex) {
			logger.error("AgentConsumer.registerIncomingFiles Failure to Register File Message", ex);
		}
		return isRegistered;
	}

	public void shutdown() {
		// controller#24: shutdown wins over a re-init. Taking the transport lock waits for one in progress, so it
		// cannot install a fresh connection after this teardown and leak it.
		closed = true;
		synchronized (this) {
			shutdownTransport();
		}
	}

	private void shutdownTransport() {
		// A dead (reconnecting/failed) failover transport parks consumer.close() / session.close() /
		// connection.close() until its reconnect attempts exhaust (~20s each, serially). Dispose it
		// first so the teardown is immediate; a healthy transport still closes gracefully. Only on
		// the dedicated connection we own -- never touch a pooled session's shared connection.
		boolean dead = false;
		if (dedicatedConnection && connection != null) {
			dead = ActiveClient.disposeIfDead(connection);
			if (dead) {
				logger.warn("AgentConsumer.shutdown: dedicated transport is dead; disposed it before teardown");
			}
		}
		try {
			if (aliasConsumer != null) aliasConsumer.close();
		} catch (Exception ignore) { }
		try {
			if (consumer != null) consumer.close();
		} catch (Exception ex) {
			if (dead) {
				logger.debug("AgentConsumer.shutdown consumer close on dead transport: " + ex.getMessage());
			} else {
				logger.error("AgentConsumer.shutdown Consumer Shutdown Error: " + ex.getMessage(), ex);
			}
		}
		// On a dedicated connection WE own the socket: close it or every re-init (failover) leaks
		// one. Separate trys so a throwing session.close() cannot strand the socket.
		if (dedicatedConnection) {
			try {
				if (sess != null && !sess.isClosed()) sess.close();
			} catch (Exception ex) {
				if (!dead) logger.warn("AgentConsumer.shutdown session close error: " + ex.getMessage());
			} finally {
				ActiveClient.closeConnectionFast(connection);
			}
		}
	}

}