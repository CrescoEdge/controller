package io.cresco.agent.controller.netdiscovery;


public class DiscoveryNode {

	  public String src_ip;
	  public String src_port;
	  public String src_region;
	  public String src_agent;
	  public String dst_ip;
	  public String dst_port;
	  public String dst_region;
	  public String dst_agent;
	  public String broadcast_ts;
	  public String broadcast_latency;
	  public String agent_count;

	  public DiscoveryNode(String src_ip, String src_port, String src_region, String src_agent, String dst_ip, String dst_port, String dst_region, String dst_agent, String broadcast_ts, String broadcast_latency, String agent_count)
	  {
		 this.src_ip = src_ip;
		 this.src_port = src_port;
		 this.src_region = src_region;
		 this.src_agent = src_agent;
		 this.dst_ip = dst_ip;
		 this.dst_port = dst_port;
		 this.dst_region = dst_region;
		 this.dst_agent = dst_agent;
		 this.broadcast_ts = broadcast_ts;
		 this.broadcast_latency = broadcast_latency;
		 this.agent_count = agent_count;
	  }
	  
	}