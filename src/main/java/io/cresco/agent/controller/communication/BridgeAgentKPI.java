package io.cresco.agent.controller.communication;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.network.jms.OutboundTopicBridge;
import org.apache.activemq.network.jms.SimpleJmsTopicConnector;

public class BridgeAgentKPI {

    public BridgeAgentKPI(String brokerName) throws Exception {



        SimpleJmsTopicConnector jmsTopicConnector = null;

        jmsTopicConnector = new SimpleJmsTopicConnector();

        // Wire the bridges.
        jmsTopicConnector.setOutboundTopicBridges(new OutboundTopicBridge[]{new OutboundTopicBridge("event.agent")});
        //jmsTopicConnector.setInboundTopicBridges(new InboundTopicBridge[]{new InboundTopicBridge("event.region"),new InboundTopicBridge("event.global")});


    //brokerName
        // Tell it how to reach the two brokers.
        //jmsTopicConnector.setOutboundTopicConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:" + remotePort));
        //jmsTopicConnector.setLocalTopicConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:" + localport));

        jmsTopicConnector.setOutboundTopicConnectionFactory(new ActiveMQConnectionFactory("vm://localhost?brokerName=" + brokerName + "_KPI"));
        jmsTopicConnector.setLocalTopicConnectionFactory(new ActiveMQConnectionFactory("vm://localhost?brokerName=" + brokerName));

        jmsTopicConnector.start();


        while (!jmsTopicConnector.isConnected()) {
            System.out.println("Not connected");
            Thread.sleep(1000);
        }



    }



}