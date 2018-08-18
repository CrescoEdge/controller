package io.cresco.agent.controller.app;

import java.util.List;


public class gPayload {

	  public String pipeline_id;
	  public String pipeline_name;
	  public String status_code;
	  public String status_desc;
	  
	  public List<gNode> nodes;
	  public List<gEdge> edges;
	  
	  public gPayload(List<gNode> nodes, List<gEdge> edges)
	  {
		  this.nodes = nodes;
		  this.edges = edges;
	  }
	  public gPayload()
	  {
		  
	  }
	  
	}