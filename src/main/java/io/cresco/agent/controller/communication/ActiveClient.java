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
                        //can't trace, it causes stack overflow
                        //logger.trace("URI: " + faultTriggerURI + " Status Started:" + connectionMap.get(faultTriggerURI).isStarted());
                        isActive = connectionMap.get(faultTriggerURI).isStarted();
                    }
                }
            }
        } catch (Exception ex) {
            //logger.error(ex.getMessage());
            //this might cause a stackoverflow as well
            ex.printStackTrace();
        }
        return isActive;
    }

    public ActiveMQSession createSession(String URI, boolean transacted, int acknowledgeMode) {
        ActiveMQSession activeMQSession = null;
        try {

            ActiveMQConnection activeMQConnection = getConnection(URI);
            if(activeMQConnection != null) {
                activeMQSession = (ActiveMQSession)activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

        } catch (Exception ex) {
            logger.error("createSession: " + ex.getMessage());
            logger.error(getStringFromError(ex));
        }

        return activeMQSession;
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
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
            //PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();


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

                if(activeMQSslConnectionFactory != null) {
                    activeMQConnection = (ActiveMQConnection) activeMQSslConnectionFactory.createConnection();
                    logger.debug("Connection Created for URI: [" + URI + "]");


                    activeMQConnection.start();
                    while (!activeMQConnection.isStarted()) {
                        logger.info("Waiting on connection to URI: [" + URI + "] to start.");
                        Thread.sleep(1000);
                    }

                    synchronized (lockConnectionMap) {
                        connectionMap.put(URI, activeMQConnection);

                    }
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


    public boolean initActiveAgentConsumer(String RXQueueName, String URI) {

        boolean isInit = false;
        try {

            this.agentConsumer = new AgentConsumer(controllerEngine, RXQueueName, URI);
            isInit = true;
            logger.debug("IN initActiveAgentConsumer setting URI:" + URI + " fault URI");
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
        /*
        if ((hasActiveProducer()) && (!controllerEngine.cstate.getRegion().equals("init"))) {
            logger.error("AP is null 0");
            logger.error("Message: " + msg.getParams());
            return;
        } else if (hasActiveProducer()) {
            logger.trace("AP is null");
            System.out.println("AP is null 1");
            return;
        }
        */
        if(hasActiveProducer()) {
            agentProducer.sendMessage(msg);
        }
    }

    public void shutdown() {

        logger.info("ActiveClient shutting down");

        if(this.agentProducer != null) {
            logger.info("Producer shutting down");
            this.agentProducer.shutdown();
            this.agentProducer = null;

            logger.info("Consumer shutting down");
            this.agentConsumer.shutdown();
            this.agentConsumer = null;
        }


        try {
            synchronized (lockConnectionMap) {
                for (Map.Entry<String, ActiveMQConnection> entry : connectionMap.entrySet()) {
                    ActiveMQConnection value = entry.getValue();
                    value.stop();
                    value.cleanUpTempDestinations();
                    value.cleanup();
                    value.close();
                    while(!value.isClosed()){
                        Thread.sleep(500);
                    }
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
            logger.error(ex.getMessage());
        }


    }

    public boolean sendMessage(MsgEvent sm) {
        return agentProducer.sendMessage(sm);
    }

}



