package io.cresco.agent.data;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.util.transport.InMemoryBroker;

import jakarta.jms.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CEPInstance {

    private PluginBuilder plugin;
    private CLogger logger;

    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;

    private Map<String,String> topicMap;

    private AtomicBoolean lockTopic = new AtomicBoolean();

    private Gson gson;

    private String cepId;
    private String listenerId;

    private String inputStreamName;



    private InMemoryBroker.Subscriber outputSubscriber;

    public CEPInstance(PluginBuilder pluginBuilder, SiddhiManager siddhiManager, String cepId, String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPInstance.class.getName(),CLogger.Level.Info);
        this.inputStreamName = inputStreamName;

        topicMap = Collections.synchronizedMap(new HashMap<>());

        this.siddhiManager = siddhiManager;

        this.cepId = cepId;

        gson = new Gson();

        try {

            String inputTopic = UUID.randomUUID().toString();
            String outputTopic = UUID.randomUUID().toString();

            logger.error("");

            synchronized (lockTopic) {
                topicMap.put(inputStreamName, inputTopic);
                topicMap.put(outputStreamName, outputTopic);
            }

            //generate query strings
            String sourceString = getSourceString(inputStreamDefinition, inputTopic, inputStreamName);
            String sinkString = getSinkString(outputStreamDefinition, outputTopic,outputStreamName);

            //Generating runtime
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sourceString + " " + sinkString + " " + queryString);

            //Starting event processing
            siddhiAppRuntime.start();

            //create an output subscriber
            outputSubscriber = new OutputSubscriber(plugin,cepId,outputTopic,outputStreamName);

            //subscribe to "inMemory" broker per topic
            InMemoryBroker.subscribe(outputSubscriber);

            //create input stream
            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {


                        if (msg instanceof TextMessage) {

                            //System.out.println(RXQueueName + " msg:" + ((TextMessage) msg).getText());
                            InMemoryBroker.publish(inputTopic, ((TextMessage) msg).getText());
                            //String message = ((TextMessage) msg).getText();
                            //logger.error("YES!!! " + message);

                        }
                    } catch(Exception ex) {

                        ex.printStackTrace();
                    }
                }
            };

            //subscribe to input
            listenerId= pluginBuilder.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"stream_name='" + inputStreamName + "'");


        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public void shutdown() {
        try {

            //unsubscribe from topic
            InMemoryBroker.unsubscribe(outputSubscriber);

            //stop listening for messages
            plugin.getAgentService().getDataPlaneService().removeMessageListener(listenerId);

            //shutdown runtime
            if(siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void clear() {
        try {

            shutdown();


            synchronized (lockTopic) {
                topicMap.clear();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void input(String streamName, String jsonPayload) {
        try {

            String topicName = null;
            synchronized (lockTopic) {
                if(topicMap.containsKey(streamName)) {
                    topicName = topicMap.get(streamName);
                }
            }


                if (topicName != null) {
                //start measurement
                    InMemoryBroker.publish(topicName, jsonPayload);
                } else {
                    System.out.println("input error : no schema");
                }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }



    private String getSourceString(String inputStreamDefinition, String topic, String streamName) {
        String sourceString = null;
        try {

            sourceString  = "@source(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + inputStreamDefinition + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sourceString;
    }

    private String getSinkString(String outputStreamDefinition, String topic, String streamName) {
        String sinkString = null;
        try {

            sinkString = "@sink(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + outputStreamDefinition + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sinkString;
    }



}
