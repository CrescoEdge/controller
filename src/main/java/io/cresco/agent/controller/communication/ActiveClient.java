package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.Session;
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

    private String faultTriggerURI;

    public ActiveClient(ControllerEngine controllerEngine){
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ActiveClient.class.getName(), CLogger.Level.Info);
        connectionFactoryMap = Collections.synchronizedMap(new HashMap<>());
        connectionMap = Collections.synchronizedMap(new HashMap<>());
    }


    private void setFaultTriggerURI(String faultTriggerURI) {
        this.faultTriggerURI = faultTriggerURI;
    }

    private String getFaultTriggerURI() {
        return faultTriggerURI;
    }

    public boolean isFaultURIActive() {
        boolean isActive = false;
        try {
            if(faultTriggerURI != null) {
                synchronized (lockConnectionMap) {
                    if(connectionMap.containsKey(faultTriggerURI)) {
                        isActive = connectionMap.get(faultTriggerURI).isStarted();
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return isActive;
    }

    public ActiveMQSession createSession(String URI, boolean transacted, int acknowledgeMode) {
        ActiveMQSession activeMQSession = null;
        try {

            activeMQSession = (ActiveMQSession)getConnection(URI).createSession(false, Session.AUTO_ACKNOWLEDGE);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return activeMQSession;
    }

    private ActiveMQConnection getConnection(String URI) {
        ActiveMQConnection activeMQConnection = null;



        try {

            boolean hasConnection = false;
            synchronized (lockConnectionMap) {
                if(connectionMap.containsKey(URI)) {
                    hasConnection = true;
                }
            }

            ActiveMQSslConnectionFactory activeMQSslConnectionFactory = null;

            if(!hasConnection) {

                boolean hasFactory = false;
                //check if existing factory exist


                synchronized (lockFactoryMap) {
                    if (connectionFactoryMap.containsKey(URI)) {
                        hasFactory = true;
                    }
                }

                //if no factory exist create it
                if (!hasFactory) {
                    activeMQSslConnectionFactory = initConnectionFactory(URI);
                    logger.debug("Factory Created for URI: [" + URI + "]");

                    synchronized (lockFactoryMap) {
                        connectionFactoryMap.put(URI, activeMQSslConnectionFactory);
                    }
                } else {
                    synchronized (lockFactoryMap) {
                        activeMQSslConnectionFactory = connectionFactoryMap.get(URI);
                    }
                }

                activeMQConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
                logger.debug("Connection Created for URI: [" + URI + "]");



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

                /*
                boolean isActive = false;
                synchronized (lockConnectionMap) {
                    activeMQConnection = connectionMap.get(URI);
                    if(activeMQConnection.isStarted()) {
                       isActive = true;
                    }
                }

                if(!isActive) {
                    logger.error("Connection Failed for URI: [" + URI + "]");

                    activeMQConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
                    logger.error("Connection Created for URI: [" + URI + "]");


                    activeMQConnection.start();
                    while(!activeMQConnection.isStarted()) {
                        logger.info("Waiting on connection to URI: [" + URI + "] to start." );
                        Thread.sleep(1000);
                    }

                }
                */

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
            setFaultTriggerURI(URI);

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
            System.out.println("AP is null");
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

        try {
            synchronized (lockConnectionMap) {
                for (Map.Entry<String, ActiveMQConnection> entry : connectionMap.entrySet()) {
                    ActiveMQConnection value = entry.getValue();

                    value.close();
                }
                connectionMap.clear();
            }

            synchronized (lockFactoryMap) {
                //for (Map.Entry<String, ActiveMQSslConnectionFactory> entry : connectionFactoryMap.entrySet()) {
                //    ActiveMQSslConnectionFactory value = entry.getValue();
                //}
                connectionFactoryMap.clear();
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public boolean sendMessage(MsgEvent sm) {
        return agentProducer.sendMessage(sm);
    }

}



