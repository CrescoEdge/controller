package io.cresco.agent.data;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.siddhi.core.util.transport.InMemoryBroker;

import jakarta.jms.TextMessage;
public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private PluginBuilder plugin;
    private CLogger logger;

    private String topic;
    private String streamName;

    private String cepId;


    public OutputSubscriber(PluginBuilder pluginBuilder, String cepId, String topic, String streamName) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(OutputSubscriber.class.getName(),CLogger.Level.Info);

        this.cepId = cepId;

        this.topic = topic;
        this.streamName = streamName;

    }

    @Override
    public void onMessage(Object msg) {

        try {

            //logger.error((String)msg);
            io.cresco.library.data.DataPlaneService dps = plugin.getAgentService().getDataPlaneService();
            TextMessage tm = dps.createTextMessage();
            tm.setText((String)msg);
            tm.setStringProperty("stream_name",streamName);
            //emit CEP results on the GLOBAL sharded dataplane (shard derived from the output
            //stream name, matching the wsapi subscriber) so external clients receive them.
            dps.sendMessage(TopicType.GLOBAL, tm, jakarta.jms.DeliveryMode.NON_PERSISTENT, 0, 0,
                    dps.shardFor(streamName));

            /*
            if(!outputList.isEmpty()) {

                for(Map<String,String> outputMap : outputList) {

                    String outputRegion = outputMap.get("region");
                    String outputAgent = outputMap.get("agent");
                    String outputPlugin = outputMap.get("pluginid");

                    logger.info("Sending out " + outputRegion + " " + outputAgent + " " + outputPlugin + " " + input);

                    MsgEvent inputMsg = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, outputRegion, outputAgent, outputPlugin);
                    inputMsg.setParam("action", "queryinput");
                    inputMsg.setParam("input_stream_name", streamName);
                    inputMsg.setCompressedParam("input_stream_payload", input);
                    plugin.msgOut(inputMsg);
                }

            }
            */


        } catch(Exception ex) {
            logger.error("OutputSubscriber.onMessage error", ex);
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
