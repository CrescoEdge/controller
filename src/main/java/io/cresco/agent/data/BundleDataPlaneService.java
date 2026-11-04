package io.cresco.agent.data;

import io.cresco.agent.controller.communication.StdinGuard;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.FileObject;
import io.cresco.library.data.SubscriptionRefusedException;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import jakarta.jms.*;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The dataplane as one bundle sees it: every message it writes carries that bundle's plugin as the verified writer
 * ({@code cresco_caller}), never the controller. Everything else is the controller's dataplane.
 *
 * <p>{@link #downloadRemoteFile} is not offered: it reads another node's files with {@code getfiledata}, which needs
 * {@code infra} or {@code admin}, and a plugin is neither.
 */
public final class BundleDataPlaneService implements DataPlaneService {

    private final DataPlaneServiceImpl impl;
    private final Supplier<Caller> writer;
    private final String bundleName;
    /** The bundle is in {@code on_behalf_bundles}: an API user it names as a message's writer is kept. */
    private final boolean onBehalf;

    public BundleDataPlaneService(DataPlaneServiceImpl impl, Supplier<Caller> writer, String bundleName, boolean onBehalf) {
        this(impl, writer, bundleName, onBehalf, true);
    }

    /**
     * {@code trustedName}: this controller installed the bundle itself (agent jar or verified signed jar), so a capability
     * bound to its symbolic name holds -- the executor's stdin (review F5: a plugin can install a bundle of any name).
     */
    public BundleDataPlaneService(DataPlaneServiceImpl impl, Supplier<Caller> writer, String bundleName, boolean onBehalf,
                                  boolean trustedName) {
        this.impl = impl;
        this.writer = writer;
        this.bundleName = bundleName;
        this.onBehalf = onBehalf;
        this.trustedName = trustedName;
    }

    private final boolean trustedName;

    /** Does this wrap {@code d} (the controller rebuilds its dataplane on a re-init)? */
    public boolean wraps(DataPlaneService d) { return impl == d; }

    @Override public void updateConnections(String URI) { impl.updateConnections(URI); }

    @Override
    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString)
            throws SubscriptionRefusedException {
        return impl.addMessageListener(topicType, messageListener, guard(selectorString));
    }

    /** Only the executor sees a process's stdin ({@link StdinGuard}). */
    private String guard(String selector) throws SubscriptionRefusedException {
        if (trustedName && StdinGuard.EXECUTOR_BUNDLE.equals(bundleName)) return selector;
        String why = StdinGuard.refusal(selector);
        if (why != null) throw new SubscriptionRefusedException(why + " (" + bundleName + ")");
        try {
            return StdinGuard.narrow(selector);
        } catch (IllegalArgumentException malformed) {
            throw new SubscriptionRefusedException("refused: " + malformed.getMessage() + " (" + bundleName + ")");
        }
    }

    @Override public void removeMessageListener(String listenerId) { impl.removeMessageListener(listenerId); }

    @Override
    public boolean sendMessage(TopicType topicType, Message message) {
        return impl.sendMessageAs(writer, onBehalf, MsgEvent.Type.INFO, topicType, message, DeliveryMode.NON_PERSISTENT, 0, 0);
    }

    @Override
    public boolean sendMessage(MsgEvent.Type msgEventType, TopicType topicType, Message message) {
        return impl.sendMessageAs(writer, onBehalf, msgEventType, topicType, message, DeliveryMode.NON_PERSISTENT, 0, 0);
    }

    @Override
    public boolean sendMessage(TopicType topicType, Message message, int deliveryMode, int priority, int timeToLive) {
        return impl.sendMessageAs(writer, onBehalf, MsgEvent.Type.INFO, topicType, message, deliveryMode, priority, timeToLive);
    }

    @Override public int getDataPlaneShardCount() { return impl.getDataPlaneShardCount(); }

    @Override
    public String addMessageListener(TopicType topicType, MessageListener messageListener, String selectorString, int shard)
            throws SubscriptionRefusedException {
        return impl.addMessageListener(topicType, messageListener, guard(selectorString), shard);
    }

    @Override
    public boolean sendMessage(TopicType topicType, Message message, int deliveryMode, int priority, int timeToLive, int shard) {
        return impl.sendMessageAs(writer, onBehalf, topicType, message, deliveryMode, priority, timeToLive, shard);
    }

    @Override public int shardFor(String routingKey) { return impl.shardFor(routingKey); }
    @Override public boolean isFaultURIActive() { return impl.isFaultURIActive(); }
    @Override public BytesMessage createBytesMessage() { return impl.createBytesMessage(); }
    @Override public MapMessage createMapMessage() { return impl.createMapMessage(); }
    @Override public Message createMessage() { return impl.createMessage(); }
    @Override public ObjectMessage createObjectMessage() { return impl.createObjectMessage(); }
    @Override public StreamMessage createStreamMessage() { return impl.createStreamMessage(); }
    @Override public TextMessage createTextMessage() { return impl.createTextMessage(); }
    @Override public Message createMessage(InputStream inputStream) { return impl.createMessage(inputStream); }
    @Override public InputStream getInputMessageStream(Message message) { return impl.getInputMessageStream(message); }

    @Override
    public String createCEP(String inputRecordSchemaString, String inputStreamName, String outputStreamName,
                            String outputStreamAttributesString, String queryString) {
        return impl.createCEP(inputRecordSchemaString, inputStreamName, outputStreamName, outputStreamAttributesString, queryString);
    }

    @Override public void inputCEP(String streamName, String jsonPayload) { impl.inputCEP(streamName, jsonPayload); }
    @Override public boolean removeCEP(String cepId) { return impl.removeCEP(cepId); }
    @Override public List<FileObject> createFileObjects(List<String> fileList) { return impl.createFileObjects(fileList); }
    @Override public FileObject createFileObject(String fileName) { return impl.createFileObject(fileName); }
    @Override public Map<String, String> splitFile(String dataName, String fileName) { return impl.splitFile(dataName, fileName); }
    @Override public Map<String, String> streamToSplitFile(String dataName, InputStream is) { return impl.streamToSplitFile(dataName, is); }
    @Override public void mergeFiles(List<File> files, File into, boolean deleteParts) { impl.mergeFiles(files, into, deleteParts); }
    @Override public List<FileObject> getFileObjectsFromString(String fileObjectsString) { return impl.getFileObjectsFromString(fileObjectsString); }
    @Override public String generateFileObjectsString(List<FileObject> fileObjects) { return impl.generateFileObjectsString(fileObjects); }
    @Override public Path getJournalPath() { return impl.getJournalPath(); }

    @Override
    public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
        throw new SecurityException("downloadRemoteFile is not available to " + bundleName
                + ": it reads another node's files with getfiledata, which needs infra or admin");
    }

    @Override public String toString() { return "DataPlaneService[" + bundleName + "]"; }
}
