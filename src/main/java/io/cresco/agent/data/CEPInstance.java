package io.cresco.agent.data;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.util.transport.InMemoryBroker;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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


    public CEPInstance(PluginBuilder pluginBuilder, SiddhiManager siddhiManager, String cepId, String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPInstance.class.getName(),CLogger.Level.Info);

        topicMap = Collections.synchronizedMap(new HashMap<>());

        this.siddhiManager = siddhiManager;

        this.cepId = cepId;

        gson = new Gson();

        try {

            String inputTopic = UUID.randomUUID().toString();
            String outputTopic = UUID.randomUUID().toString();

            synchronized (lockTopic) {
                topicMap.put(inputStreamName, inputTopic);
                topicMap.put(outputStreamName, outputTopic);
            }


            String sourceString = getSourceString(inputStreamDefinition, inputTopic, inputStreamName);
            String sinkString = getSinkString(outputStreamDefinition, outputTopic,outputStreamName);

            //Generating runtime
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sourceString + " " + sinkString + " " + queryString);

            //public void createCEP(String inputStreamName, String outputStreamName, String inputStreamAttributesString, String outputStreamAttributesString,String queryString) {

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

            pluginBuilder.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"stream_name='" + inputStreamName + "'");


            InMemoryBroker.Subscriber subscriberTest = new OutputSubscriber(plugin,cepId,outputTopic,outputStreamName);

            //subscribe to "inMemory" broker per topic
            InMemoryBroker.subscribe(subscriberTest);

            //Starting event processing
            siddhiAppRuntime.start();

        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public void shutdown() {
        try {

            if(siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

            /*
            if(siddhiManager != null) {
                siddhiManager.shutdown();
            }
            */

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void clear() {
        try {

            if(siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
                siddhiAppRuntime = null;
            }

            /*
            if(siddhiManager != null) {
                siddhiManager.shutdown();
                siddhiManager = new SiddhiManager();
            }
            */


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
