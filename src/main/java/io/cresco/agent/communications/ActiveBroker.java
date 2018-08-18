package io.cresco.agent.communications;

import io.cresco.library.utilities.CLogger;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ServiceStopper;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ActiveBroker {
	private CLogger logger;
	private TransportConnector connector;

	public BrokerService broker;

	public ActiveBroker(String brokerName, String brokerUserNameAgent, String brokerPasswordAgent) {

		try {

			int discoveryPort = 32010; //agentcontroller.getConfig().getIntegerParam("discovery_port",32010);

			if(portAvailable(discoveryPort)) {

				PolicyEntry entry = new PolicyEntry();
		        entry.setGcInactiveDestinations(true);
		        entry.setInactiveTimeoutBeforeGC(15000);

				ManagementContext mc = new ManagementContext();
				mc.setSuppressMBean("endpoint=dynamicProducer,endpoint=Consumer");

				PolicyMap map = new PolicyMap();
		        map.setDefaultEntry(entry);

				broker = new BrokerService();
				broker.setUseShutdownHook(false);
				broker.setPersistent(false);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(2500);
				broker.setDestinationPolicy(map);
				broker.setManagementContext(mc);
				broker.setPopulateJMSXUserID(true);
				broker.setUseAuthenticatedPrincipalForJMSXUserID(true);

				connector = new TransportConnector();

				String internalConnector = "vm://" + brokerName + "?marshal=false&broker.persistent=false";
				connector.setUri(new URI(internalConnector));
				//connector.setUri(new URI("tcp://0.0.0.0:"+ discoveryPort));


				broker.addConnector(connector);

				//logger.info("Starting Broker");

				broker.start();

				while(!broker.isStarted()) {
			    	Thread.sleep(1000);
                }


			} else {
				//todo Figure out some way to run more than one agent per instance if needed
				logger.error("Constructor : portAvailable("+ discoveryPort +") == false");
				logger.error("Shutting down!");
				System.exit(0);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
			//logger.error("Init {}" + ex.getMessage());
		}
	}


	public boolean isHealthy() {
		boolean isHealthy = false;
		try  {
			if(broker.isStarted()) {
				isHealthy = true;
			}
		} catch (Exception e) {
			logger.error("isHealthy {}", e.getMessage());
		}
		return isHealthy;
	}
	
	public void stopBroker() {
		try {
			connector.stop();
			ServiceStopper stopper = new ServiceStopper();
			//broker.getManagementContext().stop();
            broker.stopAllConnectors(stopper);
            broker.stop();

			while(!broker.isStopped()) {
				Thread.sleep(1000);
			}
			logger.debug("Broker has shutdown");
		} catch (Exception e) {
			logger.error("stopBroker {}", e.getMessage());
		}
		
	}

	public boolean removeNetworkConnector(NetworkConnector bridge) {
		boolean isRemoved = false;
		try {
			bridge.stop();
			while(!bridge.isStopped()) {
				Thread.sleep(1000);
			}
			broker.removeNetworkConnector(bridge);
			isRemoved = true;
		}
		catch(Exception ex) {
			logger.error("removeNetworkConnector {}", ex.getMessage());
		}
		return isRemoved; 
		
	}

    public NetworkConnector AddNetworkConnectorURI(String URI, String brokerUserName, String brokerPassword) {
        NetworkConnector bridge = null;
        try {
            logger.trace("URI: " + URI + " brokerUserName: " + brokerUserName + " brokerPassword: " + brokerPassword);
            bridge = broker.addNetworkConnector(new URI(URI));
            //RandomString rs = new RandomString(5);
            bridge.setUserName(brokerUserName);
            bridge.setPassword(brokerPassword);
            bridge.setName(java.util.UUID.randomUUID().toString());
            bridge.setDuplex(true);
            bridge.setDynamicOnly(true);
            bridge.setPrefetchSize(1);

        } catch(Exception ex) {
            logger.error("AddNetworkConnector {}", ex.getMessage());
        }
        return bridge;
    }

    public List<ActiveMQDestination> getDest(String agentPath) {
		List<ActiveMQDestination> dstList = new ArrayList<>();
		ActiveMQDestination dst = ActiveMQDestination.createDestination("queue://" + agentPath, ActiveMQDestination.QUEUE_TYPE);
		dstList.add(dst);
		return dstList;
	}


	public void AddTransportConnector(String URI) {
		try {
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));

			this.broker.addConnector(connector);
			this.broker.startTransportConnector(connector);
		} catch(Exception ex) {
			logger.error("AddTransportConnector {}", ex.getMessage());
		}
	}
	
	public boolean portAvailable(int port) {
		if (port < 0 || port > 65535) {
			throw new IllegalArgumentException("Invalid start port: " + port);
		}

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
			logger.error("portAvailable {}", e.getMessage());
		} finally  {
			if (ds != null)  {
				ds.close();
			}

			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e)  {
					/* should not be thrown */
					logger.error("portAvailable : finally {}", e.getMessage());
				}
			}
		}
		return false;
	}
}