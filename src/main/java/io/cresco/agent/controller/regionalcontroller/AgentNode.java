package io.cresco.agent.controller.regionalcontroller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class AgentNode {

	//private MsgEvent de = null;
	private Map<String,Map<String,String>> pluginMap = null;
	private String agentName;
	private Map<String,String> paramMap;
	
	
	public AgentNode(String agentName, Map<String,String> paramMap)
	{
		this.agentName = agentName;
		pluginMap = new ConcurrentHashMap<>();
		this.paramMap = paramMap;
	}
	public AgentNode(String agentName)
	{
		this.agentName = agentName;
		pluginMap = new ConcurrentHashMap<>();
		this.paramMap = new ConcurrentHashMap<>();
	}
	public String getAgentName()
	{
		return agentName;
	}
	public void setAgentName(String agentName)
	{
		this.agentName = agentName;
	}
	
	public Map<String,String> getAgentParams()
	{
		return paramMap;
	}
	public void setAgentParams(Map<String,String> paramMap)
	{
		this.paramMap = paramMap;
	}
	public void setAgentParam(String key, String value)
	{
		paramMap.put(key, value);
	}
	public boolean isPlugin(String pluginSlot)
	{
        return pluginMap.containsKey(pluginSlot);
	}
	public Map<String,String> getPluginParams(String pluginSlot)
	{
		if(pluginMap.containsKey(pluginSlot))
		{
		return pluginMap.get(pluginSlot);
		}
		else
		{
		return null;
		}
	}
	public void addPlugin(String pluginSlot)
	{
		if(!pluginMap.containsKey(pluginSlot))
		{
			Map<String,String> pluginParams = new ConcurrentHashMap<>();
			pluginMap.put(pluginSlot, pluginParams);
		}
	}
	public void setPluginParams(String pluginSlot, Map<String,String> paramMap)
	{
		pluginMap.put(pluginSlot, paramMap);
	}
	public void setPluginParam(String pluginSlot, String key, String value)
	{
		pluginMap.get(pluginSlot).put(key, value);
	}
	public void removePlugin(String pluginSlot)
	{
		pluginMap.remove(pluginSlot);
	}
	public ArrayList<String> getPlugins() 
	{
		ArrayList<String> ar = new ArrayList<>();
		Iterator<Entry<String, Map<String, String>>> it = pluginMap.entrySet().iterator();
  	    while (it.hasNext()) 
  	    {
  	        Entry<String, Map<String, String>> pairs = it.next();
  	        ar.add(pairs.getKey());
  	    }
  	    return ar;
	}
	
}
