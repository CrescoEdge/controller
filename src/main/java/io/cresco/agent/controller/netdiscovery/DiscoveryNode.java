package io.cresco.agent.controller.netdiscovery;

public class DiscoveryNode {

	public DiscoveryType discovery_type;
	public String broadcast_ip;
	public int discovered_port;
	public long broadcast_ts;
	public String broadcast_validator;

	public String discovered_ip;
	public int broadcast_port;

	public String discover_region;
	public String discover_agent;

	public String discovered_region;
	public String discovered_agent;
	public int discovered_agent_count;
	public String discovered_validator;

	public long discovered_latency = -1;


	public String validated_authenication;

	public String discover_cert;
	public String discovered_cert;


	public NodeType nodeType = null;

	public DiscoveryNode(DiscoveryType discovery_type)
	{
		this.nodeType = NodeType.BROADCAST;
		this.discovery_type = discovery_type;
		this.broadcast_ts = System.currentTimeMillis();
	}

	public void setDiscover(String broadcast_validator) {
		this.nodeType = NodeType.DISCOVER;
		this.broadcast_validator = broadcast_validator;
	}

	public void setCertify(String broadcast_validator, String discover_cert, String discover_region, String discover_agent) {
		this.nodeType = NodeType.CERTIFY;
		this.broadcast_validator = broadcast_validator;
		this.discover_cert = discover_cert;
		this.discover_region = discover_region;
		this.discover_agent = discover_agent;
	}

	public void setBroadcastResponse(String discovered_ip, int discovered_port, String broadcast_ip, int broadcast_port, String discovered_region, String discovered_agent) {
		this.nodeType = NodeType.BROADCAST_RESPONSE;
		this.discovered_ip = discovered_ip;
		this.discovered_port = discovered_port;
		this.broadcast_ip = broadcast_ip;
		this.broadcast_port = broadcast_port;
		this.discovered_region = discovered_region;
		this.discovered_agent = discovered_agent;
	}

	public void setDiscovered(String discovered_ip, int discovered_port, String broadcast_ip, int broadcast_port, String discovered_region, String discovered_agent, int discovered_agent_count, String discovered_validator) {
		this.nodeType = NodeType.DISCOVERED;
		this.discovered_ip = discovered_ip;
		this.discovered_port = discovered_port;
		this.broadcast_ip = broadcast_ip;
		this.broadcast_port = broadcast_port;
		this.discovered_region = discovered_region;
		this.discovered_agent = discovered_agent;
		this.discovered_agent_count = discovered_agent_count;
		this.discovered_validator = discovered_validator;
	}

	public void setCertified(String discovered_ip, int discovered_port, String broadcast_ip, int broadcast_port, String discovered_region, String discovered_agent, int discovered_agent_count, String discovered_validator, String discovered_cert) {
		this.nodeType = NodeType.CERTIFIED;
		this.discovered_ip = discovered_ip;
		this.discovered_port = discovered_port;
		this.broadcast_ip = broadcast_ip;
		this.broadcast_port = broadcast_port;
		this.discovered_region = discovered_region;
		this.discovered_agent = discovered_agent;
		this.discovered_agent_count = discovered_agent_count;
		this.discovered_validator = discovered_validator;
		this.discovered_cert = discovered_cert;
	}

	public long getDiscoveryLatency() {
		if(discovered_latency == -1) {
			discovered_latency = System.currentTimeMillis() - broadcast_ts;
		}
		return discovered_latency;
	}

	public String getDiscoveredPath() {
		return this.discovered_region + "_" + this.discovered_agent;
	}
	public String getDiscoverPath() {
		return this.discover_region + "_" + this.discover_agent;
	}

	public enum NodeType {
		BROADCAST, BROADCAST_RESPONSE, DISCOVER, DISCOVERED, CERTIFY, CERTIFIED
	}

}
