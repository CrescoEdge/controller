package io.cresco.agent.controller.measurement;


public class CMetric {

	  public String name;
	  public String description;
	  public String group;
	  public String className;
	  public String inodeId;
	  public String resourceId;
	  public String edgeId;
	  public Type type;

	  public CMetric(String name, String description, String group, String className)
	  {
	  	  //NODE TYPE
		  this.name = name;
		  this.description = description;
		  this.group = group;
		  this.className = className;
		  this.type = Type.NODE;
	  }

	public CMetric(String name, String description, String group, String className, String inodeId, String resourceId)
	{
		//APP TYPE
		this.name = name;
		this.description = description;
		this.group = group;
		this.className = className;
		this.type = Type.APP;
		this.inodeId = inodeId;
		this.resourceId = resourceId;
	}

	public CMetric(String name, String description, String group, String className, String edgeId)
	{
		//APP TYPE
		this.name = name;
		this.description = description;
		this.group = group;
		this.className = className;
		this.type = Type.EDGE;
		this.edgeId = edgeId;
	}


	public static enum Type {
		APP, //require inode and resource node
	  	NODE, //require nothing
		EDGE; //require edge id

		private Type() {

		}
	}

	}