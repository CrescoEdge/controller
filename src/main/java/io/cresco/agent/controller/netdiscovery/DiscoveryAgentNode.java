package io.cresco.agent.controller.netdiscovery;


public class DiscoveryAgentNode {

	public DiscoveryType discovery_type;
	public String remote_region;
	public String remote_agent;
	public String remote_ip;
	public int remote_port;
	public int agent_count;
	public long broadcast_ts;
	public int broadcast_latency;

	public DiscoveryAgentNode(DiscoveryType discovery_type, String remote_region, String remote_agent, String remote_ip, int remote_port, int agent_count, long broadcast_ts, int broadcast_latency)
	{
		this.discovery_type = discovery_type;
		this.remote_region = remote_region;
		this.remote_agent = remote_agent;
		this.remote_ip = remote_ip;
		this.remote_port = remote_port;
		this.agent_count = agent_count;
		this.broadcast_ts = broadcast_ts;
		this.broadcast_latency = broadcast_latency;
	}

}
