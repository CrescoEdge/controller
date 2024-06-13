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
            TextMessage tm = plugin.getAgentService().getDataPlaneService().createTextMessage();
            tm.setText((String)msg);
            tm.setStringProperty("stream_name",streamName);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,tm);

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
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
