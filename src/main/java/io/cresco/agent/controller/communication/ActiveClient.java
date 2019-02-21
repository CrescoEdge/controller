package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ActiveClient {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    private Map<String, ActiveMQSslConnectionFactory> connectionFactoryMap;
    private AtomicBoolean lockFactoryMap = new AtomicBoolean();

    private Map<String, ActiveMQConnection> connectionMap;
    private AtomicBoolean lockConnectionMap = new AtomicBoolean();

    private AgentConsumer agentConsumer;
    private AgentProducer agentProducer;


    public ActiveClient(ControllerEngine controllerEngine){
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ActiveClient.class.getName(), CLogger.Level.Info);
        connectionFactoryMap = Collections.synchronizedMap(new HashMap<>());
        connectionMap = Collections.synchronizedMap(new HashMap<>());
    }

    public ActiveMQConnection getConnection(String URI) {
        ActiveMQConnection activeMQConnection = null;

        try {

            boolean hasConnection = false;
            synchronized (lockConnectionMap) {
                if(connectionMap.containsKey(URI)) {
                    hasConnection = true;
                }
            }


            if(!hasConnection) {

                boolean hasFactory = false;
                //check if existing factory exist
                synchronized (lockFactoryMap) {
                    if (connectionFactoryMap.containsKey(URI)) {
                        hasFactory = true;
                    }
                }

                //if no factory exist create it
                ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;
                if (!hasFactory) {
                    activeMQSslConnectionFactory = initConnectionFactory(URI);
                    logger.error("Factory Created for URI: [" + URI + "]");

                    synchronized (lockFactoryMap) {
                        connectionFactoryMap.put(URI, activeMQSslConnectionFactory);
                    }
                } else {
                    synchronized (lockFactoryMap) {
                        activeMQSslConnectionFactory = connectionFactoryMap.get(URI);
                    }
                }

                activeMQConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
                logger.error("Connection Created for URI: [" + URI + "]");


                activeMQConnection.start();
                while(!activeMQConnection.isStarted()) {
                    logger.info("Waiting on connection to URI: [" + URI + "] to start." );
                    Thread.sleep(1000);
                }

                synchronized (lockConnectionMap) {
                    connectionMap.put(URI,activeMQConnection);
                }

            } else {
                synchronized (lockConnectionMap) {
                    activeMQConnection = connectionMap.get(URI);
                }
            }


        } catch (Exception ex) {

            logger.error("getConnection() " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return  activeMQConnection;
    }

    private ActiveMQSslConnectionFactory initConnectionFactory(String URI) {
        ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;
        try {

            activeMQSslConnectionFactory = new ActiveMQSslConnectionFactory(URI);
            if(URI.startsWith("vm://")) {
                activeMQSslConnectionFactory.setObjectMessageSerializationDefered(true);
            }
            activeMQSslConnectionFactory.setKeyAndTrustManagers(controllerEngine.getCertificateManager().getKeyManagers(),controllerEngine.getCertificateManager().getTrustManagers(), new SecureRandom());

        } catch(Exception ex) {
            logger.error("initConnectionFactory() " + ex.getMessage());
        }
        return  activeMQSslConnectionFactory;
    }

    /*
    private ActiveMQConnection initConnection(String URI) {
        ActiveMQConnection activeMQConnection = null;
        try {



        } catch (Exception ex) {
            logger.error("initConnection() " + ex.getMessage());
        }
        return activeMQConnection;
    }
    */

    public boolean initActiveAgentConsumer(String RXQueueName, String URI) {

        boolean isInit = false;
        try {

            this.agentConsumer = new AgentConsumer(controllerEngine, RXQueueName, URI);
            isInit = true;

        } catch(Exception ex) {
         logger.error("initAgentConsumer() " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return isInit;
    }


    public boolean initActiveAgentProducer(String URI) {

        boolean isInit = false;
        try {

            this.agentProducer = new AgentProducer(controllerEngine,URI);
            isInit = true;

        } catch(Exception ex) {
            logger.error("initAgentProducer() " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return isInit;
    }

    public boolean hasActiveProducer() {
        boolean hasAP = false;
        try {
            if(agentProducer != null) {
                hasAP = true;
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return hasAP;
    }

    public void sendAPMessage(MsgEvent msg) {
        if ((hasActiveProducer()) && (!controllerEngine.cstate.getRegion().equals("init"))) {
            logger.error("AP is null");
            logger.error("Message: " + msg.getParams());
            return;
        } else if (hasActiveProducer()) {
            logger.trace("AP is null");
            return;
        }
        agentProducer.sendMessage(msg);
    }

    public void shutdown() {

        if(this.agentProducer != null) {
            logger.trace("Producer shutting down");
            this.agentProducer.shutdown();
            this.agentProducer = null;
            logger.info("Producer shutting down");
        }

    }

    public boolean sendMessage(MsgEvent sm) {
        return agentProducer.sendMessage(sm);
    }

}



