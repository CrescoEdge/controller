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
	/** The upgrade window: the consumer of this controller's raw inbox, where nodes not yet upgraded write ({@link LegacyNodes}). */
	private MessageConsumer legacyConsumer;
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
	// the file groups being received: removed on completion, expired when abandoned, bounded (IncomingFileGroups)
	private final IncomingFileGroups fileGroups = new IncomingFileGroups();

	private Cache<String, MsgEvent> fileMsgEventCache;

	public AgentConsumer(ControllerEngine controllerEngine, String RXQueueName, String URI) throws JMSException {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(AgentConsumer.class.getName(),CLogger.Level.Trace);

		this.gson = new Gson();


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
					logger.error("value: " + io.cresco.agent.controller.communication.ParamLog.describe(n.getValue()));
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
		// messages this node cannot judge yet are held a moment, not refused (TrustHold); 0 turns it off
		long holdMs = Math.min(60_000L, plugin.getConfig().getLongParam("msg_signing_hold_ms", 5_000L));
		MsgSigning signing = controllerEngine.msgSigning();
		if (signing.required() && holdMs > 0) {
			this.trustHold = new TrustHold(holdMs, 2048, 32L << 20, m -> {
				MessageListener l = inboxListener;
				if (l != null) l.onMessage(m);
			});
			this.trustListener = trustHold::trustChanged;
			signing.onTrustChange(trustListener);
		} else {
			this.trustHold = null;
			this.trustListener = null;
		}
		// the first moments with a parent only (of each join): a node whose parent never answers is not held back for ever
		this.trustWait = new TrustWait(3 * holdMs);
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
			String legacyQueue = legacyInbox(uri);
			if (legacyQueue != null) {
				legacyConsumer = sess.createConsumer(sess.createQueue(legacyQueue));
				logger.warn("AgentConsumer also consumes the raw inbox [{}] during the upgrade window: nodes not yet upgraded"
						+ " (no tenant namespacing) write there", legacyQueue);
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
				// What became of each message is counted once (MsgCounts): refused (MsgSigning counts it), delivered to the
				// call waiting for it, handed to the router (which counts it from there), held (it comes back here), or
				// dropped under its cause. settled: that has happened; a failure after it is not counted again
				final MsgCounts counts = controllerEngine.msgCounts();
				boolean settled = false;
				MsgEvent taken = null;
				try {

					sweepFileGroups();

					if (msg instanceof TextMessage) {

						String filegroup = msg.getStringProperty("filegroup");
						String fileobjectString = msg.getStringProperty("fileobjects");
						String msgEventString = ((TextMessage) msg).getText();

						MsgEvent me = gson.fromJson(msgEventString,MsgEvent.class);
						taken = me;
						if(me != null) {
							// Gson reads the parameters into a tree map (every getParam a string-comparing search, and a
							// message's checks read dozens); a hash map holds the same entries
							if (me.getParams() != null && !(me.getParams() instanceof java.util.HashMap)) {
								me.setParams(new java.util.HashMap<>(me.getParams()));
							}
							// gfs#26: addressed to this node's former address (region_alias): it is for this node
							RegionAlias.normalize(me, aliasRegion, plugin.getRegion(), plugin.getAgent());
							// the verified caller comes from the properties the broker checked, never from the body
							io.cresco.library.security.Caller caller = CallerStamps.fromJms(me, msg, controllerEngine.securityOn());
							// the upgrade window: an upgraded node (it speaks as its controller) is no longer a legacy node
							if (caller != null && caller.getKind() == io.cresco.library.security.Caller.Kind.CONTROLLER) {
								controllerEngine.legacyNodes().upgraded(caller.getRegion(), caller.getAgent());
							}
							// D4: what is dispatched here (to this node's controller or plugins, or an RPC reply for it) must carry
							// a valid origin signature naming this node as its recipient. What this node only carries on (not
							// addressed here, a source route's waypoint, an agent's global function on its way to the global) is
							// not checked now: it is noted, and checked wherever it is dispatched (MsgSigning.verifyIfDeferred)
							MsgSigning signing = controllerEngine.msgSigning();
							boolean checkNow = signing.required() && forThisNode(me)
									&& (me.getParam(callIdKey) != null || filegroup != null || !signing.relayOnly(me));
							if (signing.required() && !checkNow) signing.deferred(me);
							if (checkNow) {
								// what this node cannot judge YET is held a moment and verified again, never refused at once
								// (TrustHold): in its first moments with a parent, before that parent's first pong brought the
								// trust data, anything but the parent's own; later, a signer whose controller is not in the
								// trust data held yet. Held at most msg_signing_hold_ms; then it is judged as it stands.
								TrustHold hold = trustHold;
								long now = System.currentTimeMillis();
								boolean mayHold = hold != null && hold.mayHold(now);
								boolean noRoom = false;         // the hold could not take it (counted once, however often it is tried)
								// what a held message takes of the hold's room: its text and the file list it carries
								int heldChars = msgEventString.length() + (fileobjectString == null ? 0 : fileobjectString.length());
								if (hold != null && trustWait.waiting(controllerEngine.trustReady(), now) && mayHold && !fromParentOrSelf(caller)) {
									signing.wantTrust();                // the parent is asked now, not at the next ping
									if (held(hold, msg, heldChars, now, caller)) return;
									noRoom = true;
									counts.notHeld();
								}
								MsgSigning.Verdict v = signing.verify(me, caller, controllerEngine.legacyNodesAdmitted(),
										firstHand(msg, caller), mayHold);
								if (!v.ok()) {
									if (v.later) {
										if (held(hold, msg, heldChars, now, caller)) return;
										if (!noRoom) counts.notHeld();
										// the verdict was not counted when it was given: it is now, under its reason
										signing.refusedAfterHold(me, v.reason, v.refusal);
									}
									settled = true;                     // refused: counted by MsgSigning
									refuseUnauthenticated(me, v.refusal);
									return;
								}
								if (caller == null || !caller.equals(v.caller)) {
									// a stamp a bridge could not vouch for was stripped on the way: the origin's signed word stands
									String alias = msg.getStringProperty(CallerStamps.P_ALIAS);
									CallerStamps.stamp(me, v.caller, alias == null || alias.isEmpty() ? null : alias);
									caller = v.caller;
								}
							}

							//logger.info("TEST PLUG 0");
							//logger.info("Q: " + RXQueueName);
							//logger.info("TEST PLUG " + controllerEngine.cstate.getAgentPath());
							//logger.info("TEST PLUG 1");

							//check if message is for this agent
							//check if message has file payload and needs to be registered
							if((filegroup != null) && (me.getDstRegion().equals(plugin.getRegion())) && (me.getDstAgent().equals(plugin.getAgent()))) {
								if(!registerIncomingFiles(me, fileobjectString, filegroup, caller)) {
									logger.error("Unable to register files!");
									settled = true;
									counts.dropped(MsgCounts.Drop.FILE_GROUP_REFUSED, me);
								}
								//don't forward message until files have arrived (then it is routed; a group that expires is counted then)
								settled = true;
								return;
							}

							// files travel only as a file group (above): a file list in a body names files on the
							// RECEIVER's disk, which a relay would read and ship onward
							CallerStamps.dropFiles(me);

							//start RPC checks
							boolean isMyRPC = false;

							if (me.getParams().containsKey("is_rpc")) {

								//pick up self-rpc, unless ttl == 0
								//String callId = me.getParam(("callId-" + plugin.getRegion() + "-" +
								//		plugin.getAgent() + "-" + plugin.getPluginID()));
								String callId = me.getParam(callIdKey);


								if (callId != null) {
									isMyRPC = true;
									logger.trace("RPC Text Message: {}", io.cresco.agent.controller.communication.ParamLog.of(me));
									plugin.receiveRPC(callId, me);
									settled = true;
									counts.delivered();                 // to the call waiting for it
								}
							}

							if(!isMyRPC) {
								logger.trace("Text Message: {}", io.cresco.agent.controller.communication.ParamLog.of(me));
								//create new thread service for incoming messages
								controllerEngine.msgInThreaded(me);
								settled = true;                         // the router counts what becomes of it
							}

						} else {
							logger.error("non-MsgEvent message found!");
							settled = true;
							counts.dropped(MsgCounts.Drop.INBOX_ERROR, 1, "a text message that is no MsgEvent");
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

						// a file part is no MsgEvent: one this node does not take is counted by itself (Drop.FILE_PART), never in
						// the node's dropped messages. The message its group came with is: when the group expires
						boolean wellFormed = (filegroup != null) && (dstRegion != null) && (dstAgent != null) && (dataName != null) && (dataPart != null);
						if (!wellFormed) {
							settled = true;
							counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a part without its group, destination or name");
						} else if (!partFromGroupSender(filegroup, msg)) {
							settled = true;
							counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a part not from its group's caller, group", filegroup);
						} else {

							//logger.info("MSG REGION: " + dstRegion + " MSG AGENT: " + dstAgent);

							//logger.info("REGION: " + plugin.getRegion() + " AGENT: " + plugin.getAgent());


							if((dstRegion.equals(plugin.getRegion())) && (dstAgent.equals(plugin.getAgent()))) {

								// the group the part is for, while it is held (a part marks its group active)
								FileObjectGroupReceiver group;
								synchronized (lockGroupMap) {
									group = fileGroups.active(filegroup, System.currentTimeMillis());
								}

								if(group != null) {


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
										group.setDestFilePart(dataName, dataPart, filePartMD5Hash);
										//check if file is complete
										isPartComplete = group.isFilePartComplete(dataName);
										if(isPartComplete) {
											orderedFilePartNameList = group.getOrderedPartList(dataName);
											combinedFileName = group.getFileName(dataName);
											combinedFileHash = group.getFileMD5Hash(dataName);
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
													group.setFileComplete(dataName);
													isGroupComplete = group.isFileGroupComplete();
													if(isGroupComplete) {
														me = group.getMsgEvent();
														newFilePaths = group.getFileList(controllerEngine.getDataPlaneService().getJournalPath());
														// complete: no longer held (it stayed for good, with its MsgEvent)
														fileGroups.complete(filegroup, group);
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

											} else {
												// silent until now: the file is never complete, and its group expires
												settled = true;
												counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a combined file whose hash differs, group", filegroup);
											}

										} else {
											logger.error("ERROR COMBINING FILE : " + combinedFileHash);
											//System.out.println("ERROR COMBINING FILE : " + combinedFileHash);
											settled = true;
											counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a file that could not be combined, group", filegroup);
										}

									}



								} else {
									// silent until now: a part of a group that expired, completed or was never registered
									settled = true;
									counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a part for no file group this node holds, group", filegroup);
								}


							} else {
								logger.error("MUST CREATE METHOD TO FORWARD DATA MESSAGES");
								settled = true;
								counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a part for another node");
							}
						}

					} else if (msg instanceof BlobMessage) {
						logger.error("Blob message recieved!");
						settled = true;
						counts.dropped(MsgCounts.Drop.INBOX_ERROR, 1, "a blob message");
					} else {
						logger.error("non-Text message recieved!");
						settled = true;
						counts.dropped(MsgCounts.Drop.INBOX_ERROR, 1, "neither a text nor a bytes message");
					}
				} catch(Exception ex) {
					logger.error("AgentConsumer.onMessage Error : " + ex.getMessage(), ex);
					if (!settled) {
						if (msg instanceof BytesMessage) counts.dropped(MsgCounts.Drop.FILE_PART, 1, "a part this node failed on");
						else if (taken != null) counts.dropped(MsgCounts.Drop.INBOX_ERROR, taken);
						else counts.dropped(MsgCounts.Drop.INBOX_ERROR, 1, "a message that could not be read");
					}
				}
			}
		};
		inboxListener = inbox;
		consumer.setMessageListener(inbox);
		if (aliasConsumer != null) aliasConsumer.setMessageListener(inbox);
		if (legacyConsumer != null) legacyConsumer.setMessageListener(m -> {
			noteLegacySender(m);
			inbox.onMessage(m);
		});
	}

	/**
	 * The node that delivered {@code msg} first hand as its origin, as this consumer may trust it ({@link
	 * CallerStamps#P_DIRECT}), or null. A controller consumes from its OWN broker (vm://), which attests whatever it took
	 * first hand; an agent consumes from its parent's broker, whose attestation it takes only for the parent's own messages
	 * (anything else that broker delivered came through it, a relay, and must carry its origin's signature).
	 */
	private io.cresco.library.security.CrescoIdentity firstHand(Message msg, io.cresco.library.security.Caller caller) {
		io.cresco.library.security.CrescoIdentity d = CallerStamps.directOf(msg);
		if (d == null || caller == null) return null;
		boolean vm = uri != null && uri.startsWith("vm:");
		if (vm) return d;
		io.cresco.agent.core.ControllerStateImp cs = controllerEngine.cstate;
		if (cs == null) return null;
		return trustedFirstHand(d, false, cs.getRegionalRegion(), cs.getRegionalAgent());
	}

	/** {@code d} as a consumer may trust it: on its own broker ({@code vm}) as attested; on its parent's, only the parent. */
	static io.cresco.library.security.CrescoIdentity trustedFirstHand(io.cresco.library.security.CrescoIdentity d, boolean vm,
																	   String parentRegion, String parentAgent) {
		if (d == null) return null;
		if (vm) return d;
		return parentRegion != null && parentAgent != null && parentRegion.equals(d.getRegion()) && parentAgent.equals(d.getAgent()) ? d : null;
	}

	/**
	 * The raw inbox this controller also consumes during the upgrade window, or null: only a controller on its own broker
	 * (vm://, the T.*. wildcard inbox) with tenant namespacing and security on, while the window is open. Nodes of an
	 * earlier build run without namespacing and write to the raw name; outside the window nothing is subscribed there
	 * (and what still arrives after the window closes at run time is refused unsigned, like any legacy message).
	 */
	String legacyInbox(String uri) {
		if (uri == null || !uri.startsWith("vm:") || !controllerEngine.securityOn() || !controllerEngine.legacyNodesAdmitted()
				|| !SecuritySettings.tenantNamespacing(plugin.getConfig())) return null;
		String raw = plugin.getRegion() + "_" + plugin.getAgent();
		return io.cresco.library.security.TenantNamespace.wildcard(raw).equals(RXQueueName) ? raw : null;
	}

	/**
	 * A message on the raw inbox stamped by this broker as a node without stamping ({@code agent:<tenant>/<region>/<agent>},
	 * the node its certificate names) marks that node of this region as legacy: replies and requests to it then go to its
	 * raw inbox ({@link LegacyNodes}).
	 */
	private void noteLegacySender(Message m) {
		try {
			io.cresco.library.security.Caller c = CallerStamps.fromJmsProperties(m);
			if (c != null && c.getKind() == io.cresco.library.security.Caller.Kind.AGENT && plugin.getRegion().equals(c.getRegion())) {
				controllerEngine.legacyNodes().seen(c.getRegion(), c.getAgent());
			}
		} catch (RuntimeException ignore) {
			// an unreadable stamp marks nothing; the inbox refuses the message
		}
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
		try { if (legacyConsumer != null) legacyConsumer.close(); } catch (Exception ignore) { }
		legacyConsumer = null;
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
	static Path resolveInJournal(Path baseDir, String name) throws IOException {
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

	/**
	 * Is the file part {@code part} from the caller that registered {@code filegroup} (stamped on the group's message)?
	 * Otherwise it is ignored (logged). A part of an unknown group passes here; the group lookup ignores it.
	 */
	private boolean partFromGroupSender(String filegroup, Message part) {
		FileObjectGroupReceiver group;
		synchronized (lockGroupMap) {
			group = fileGroups.get(filegroup);
		}
		if (group == null) return true;
		io.cresco.library.security.Caller ownerCaller = io.cresco.library.security.Caller.fromMsgEvent(group.getMsgEvent());
		String owner = ownerCaller == null ? "" : ownerCaller.getPrincipal();
		io.cresco.library.security.Caller c = controllerEngine.securityOn() ? CallerStamps.fromJmsProperties(part)
				: io.cresco.library.security.Caller.anonymous();
		String sender = c == null ? "" : c.getPrincipal();
		if (owner.equals(sender)) return true;
		logger.warn("IGNORED a file part for group {} from {}: the group was registered by {}", filegroup, sender, owner);
		return false;
	}

	/** The inbox listener of the present transport (a held message is handed back to it). */
	private volatile MessageListener inboxListener;
	private final TrustHold trustHold;
	private final Runnable trustListener;
	/** The first moments of each join: until the parent's trust data is in, for three hold times at most. */
	private final TrustWait trustWait;

	/** Whose share of the hold a message from {@code c} takes: the node its broker says delivered it. */
	private static String holdSender(io.cresco.library.security.Caller c) {
		if (c == null) return "";
		return c.getRegion() != null && c.getAgent() != null ? c.getRegion() + "/" + c.getAgent() : String.valueOf(c.getPrincipal());
	}

	/**
	 * Hold {@code msg} until this node can judge it ({@link TrustHold#hold}); false when the hold has no room for it. A
	 * message is counted as held once, however often it is held again.
	 */
	private boolean held(TrustHold hold, Message msg, int chars, long now, io.cresco.library.security.Caller caller) {
		boolean first = hold.since() == null;
		if (!hold.hold(msg, chars, now, holdSender(caller))) return false;
		if (first) controllerEngine.msgCounts().held();
		return true;
	}

	/** Messages held because this node could not judge them yet (diagnostics, tests). */
	long heldForTrust() { return trustHold == null ? 0 : trustHold.heldTotal.get(); }

	/** Is {@code c} this node itself or its parent (an agent's region controller, a region's global)? Never held back. */
	private boolean fromParentOrSelf(io.cresco.library.security.Caller c) {
		if (c == null || c.getRegion() == null || c.getAgent() == null) return false;
		if (plugin.getRegion().equals(c.getRegion()) && plugin.getAgent().equals(c.getAgent())) return true;
		io.cresco.agent.core.ControllerStateImp cs = controllerEngine.cstate;
		if (cs == null) return false;
		boolean region = cs.isRegionalController();
		String pr = region ? cs.getGlobalRegion() : cs.getRegionalRegion(), pa = region ? cs.getGlobalAgent() : cs.getRegionalAgent();
		return pr != null && pa != null && pr.equals(c.getRegion()) && pa.equals(c.getAgent());
	}

	/** Is {@code me} for this node (its controller or one of its plugins), or an RPC reply to a call this node made? */
	private boolean forThisNode(MsgEvent me) {
		return (plugin.getRegion().equals(me.getDstRegion()) && plugin.getAgent().equals(me.getDstAgent()))
				|| (me.getParams() != null && me.getParam(callIdKey) != null);
	}

	private final java.util.concurrent.atomic.AtomicLong unauthenticated = new java.util.concurrent.atomic.AtomicLong();

	/** A message that failed its signature check: an RPC request is answered 401, anything else dropped (logged by MsgSigning). */
	private void refuseUnauthenticated(MsgEvent me, String why) {
		unauthenticated.incrementAndGet();
		if (!MsgRouter.answerUnauthenticated(plugin, me, why)) logger.error("refuseUnauthenticated: the 401 reply failed");
	}

	private boolean registerIncomingFiles(MsgEvent me, String fileobjectString, String fileGroup,
										  io.cresco.library.security.Caller caller) {
		boolean isRegistered = false;
		try {

			List<FileObject> fileObjects = controllerEngine.getDataPlaneService().getFileObjectsFromString(fileobjectString);
			Map<String,FileObject> fileObjectMap = new HashMap<>();

			for(FileObject fileObject : fileObjects) {
				fileObjectMap.put(fileObject.getDataName(), fileObject);
			}

			FileObjectGroupReceiver fileObjectGroupReceiver = new FileObjectGroupReceiver(plugin,me,fileObjectMap,fileGroup);

			// the caller the group is bound to, as partFromGroupSender reads it; without broker security there is none
			io.cresco.library.security.Caller ownerCaller = controllerEngine.securityOn() ? io.cresco.library.security.Caller.fromMsgEvent(me) : null;
			String owner = ownerCaller == null || ownerCaller.getPrincipal() == null ? "" : ownerCaller.getPrincipal();
			long now = System.currentTimeMillis();
			deleteExpired(fileGroups.expire(now));      // abandoned groups make room first
			String refused;
			synchronized (lockGroupMap) {
				refused = fileGroups.register(fileGroup, owner, fileObjectGroupReceiver, fileObjectMap.keySet(), now);
			}
			if (refused == null) {
				isRegistered = true;
			} else {
				logger.warn("REFUSED the file group {} of {}: {}", fileGroup, owner.isEmpty() ? "an unverified caller" : owner, refused);
			}


		} catch (Exception ex) {
			logger.error("AgentConsumer.registerIncomingFiles Failure to Register File Message", ex);
		}
		return isRegistered;
	}

	/** On any message, once per IncomingFileGroups.SWEEP_INTERVAL_MS: abandoned file groups expire and their files go. */
	private void sweepFileGroups() {
		long now = System.currentTimeMillis();
		if (fileGroups.sweepDue(now)) deleteExpired(fileGroups.expire(now));
	}

	private void deleteExpired(List<IncomingFileGroups.Group> expired) {
		for (IncomingFileGroups.Group group : expired) {
			int files = 0;
			try {
				files = IncomingFileGroups.deleteFiles(group, controllerEngine.getDataPlaneService().getJournalPath().toAbsolutePath());
			} catch (Exception ex) {
				logger.warn("expired file group {}: its files were not deleted: {}", group.id, ex.getMessage());
			}
			logger.warn("EXPIRED the file group {} of {}: no part arrived for {} min; {} file(s) deleted from the journal directory",
					group.id, group.owner.isEmpty() ? "an unverified caller" : group.owner, IncomingFileGroups.IDLE_EXPIRY_MS / 60_000L, files);
			// the message the group came with is never routed: it ends here
			controllerEngine.msgCounts().dropped(MsgCounts.Drop.FILE_GROUP_EXPIRED, group.receiver == null ? null : group.receiver.getMsgEvent());
		}
	}

	public void shutdown() {
		// controller#24: shutdown wins over a re-init. Taking the transport lock waits for one in progress, so it
		// cannot install a fresh connection after this teardown and leak it.
		closed = true;
		if (trustHold != null) {
			controllerEngine.msgSigning().removeTrustListener(trustListener);
			int dropped = trustHold.close();
			controllerEngine.msgCounts().dropped(MsgCounts.Drop.HOLD_CLOSED, dropped, "held for trust data");
		}
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
			if (legacyConsumer != null) legacyConsumer.close();
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