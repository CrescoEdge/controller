package io.cresco.agent.controller.globalscheduler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.URLDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;



public class ResourceScheduler implements IncomingResource {

    private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private GlobalHealthWatcher ghw;
	private CLogger logger;


    private Gson gson;

    public ResourceScheduler(ControllerEngine controllerEngine, GlobalHealthWatcher ghw) {

        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ResourceScheduler.class.getName(),CLogger.Level.Info);

        gson = new Gson();
		this.ghw = ghw;



    }


    @Override
    public void incomingMessage(MsgEvent ce) {


        try
        {
            if(ce != null) {
                logger.debug("me.added");
                //check the pipeline node
                if(ce.getParam("globalcmd").equals("addplugin"))
                {
                    //do something to activate a agentcontroller
                    logger.debug("starting precheck...");
                    //String pluginJar = verifyPlugin(ce);
                    //pNode pluginNode = verifyPlugin(ce);

                    //Here is where scheduling is taking place
                    logger.debug("agentcontroller precheck = OK");
                    String region = ce.getParam("location_region");
                    String agent = ce.getParam("location_agent");
                    String resource_id = ce.getParam("resource_id");
                    String inode_id = ce.getParam("inode_id");

                    //schedule agentcontroller
                    logger.debug("Scheduling agentcontroller on region=" + region + " agent=" + agent);
                    MsgEvent me = addPlugin(region,agent,ce.getParam("configparams"));
                    //me.setCompressedParam("pnode",gson.toJson(pluginNode));
                    me.setParam("edges",ce.getParam("edges"));

                    logger.debug("pluginadd message: " + me.getParams().toString());

                    new Thread(new PollAddPlugin(controllerEngine,resource_id, inode_id,region,agent, me)).start();

                }
                else if(ce.getParam("globalcmd").equals("removeplugin"))
                {
                    logger.debug("Incoming Remove Request : resource_id: " + ce.getParam("resource_id") + " inode_id: " + ce.getParam("inode_id"));
                    new Thread(new PollRemovePlugin(controllerEngine,  ce.getParam("resource_id"),ce.getParam("inode_id"))).start();
                }
            } else
            {
                Thread.sleep(1000);
            }

        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("ResourceSchedulerEngine run() inner Error: " + ex.toString());
        }

    }

    /*
    private pNode verifyPlugin(MsgEvent ce) {
        pNode node = null;

        logger.error("verifyPlugin MsgEvent [" + gson.toJson(ce) + "]");
        logger.error("verifyPlugin configparams [" + ce.getCompressedParam("configparams") + "]");

        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> params = gson.fromJson(ce.getCompressedParam("configparams"), type);

        logger.debug("config params: " + params.toString());

        node = controllerEngine.getPluginAdmin().getPnode(params);

        return node;
    }
    */

	public String getLowAgent() {
		
		Map<String,Integer> pMap = new HashMap<String,Integer>();
		String agent_path = null;
		try
		{
			List<String> regionList = controllerEngine.getGDB().getNodeList(null,null);
			//logger.debug("Region Count: " + regionList.size());
			for(String region : regionList)
			{
				List<String> agentList = controllerEngine.getGDB().getNodeList(region,null);
				//logger.debug("Agent Count: " + agentList.size());
				
				for(String agent: agentList)
				{
					List<String> pluginList = controllerEngine.getGDB().getNodeList(region,agent);
					int pluginCount = 0;
					if(pluginList != null)
					{
						pluginCount = pluginList.size();
					}
					String tmp_agent_path = region + "," + agent;
					pMap.put(tmp_agent_path, pluginCount);
				}
			}
			
			
			if(pMap != null)
			{
				Map<String, Integer> sortedMapAsc = sortByComparator(pMap, true);
				Entry<String, Integer> entry = sortedMapAsc.entrySet().iterator().next();
				agent_path = entry.getKey();
				/*
				for (Entry<String, Integer> entry : sortedMapAsc.entrySet())
				{
					logger.debug("Key : " + entry.getKey() + " Value : "+ entry.getValue());
				}
				*/
			}
	        
		}
		catch(Exception ex)
		{
			logger.error("DBEngine : getLowAgent : Error " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
		}
		
		return agent_path;
	}

	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order) {

        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	public MsgEvent addPlugin(String region, String agent, String configParams) {

	    //else if (ce.getParam("configtype").equals("pluginadd"))

        MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG,region,agent);
        /*
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"add agentcontroller");
		me.setParam("src_region", plugin.getRegion());
		me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		*/
		me.setParam("action", "pluginadd");
		me.setParam("configparams",configParams);
		return me;
	}
	
	public MsgEvent downloadPlugin(String region, String agent, String pluginId, String pluginurl, boolean forceDownload) {
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"download agentcontroller");
        me.setParam("src_region", plugin.getRegion());
        me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        me.setParam("dst_region", region);
        me.setParam("dst_agent", agent);
        me.setParam("configtype", "plugindownload");
		me.setParam("agentcontroller", pluginId);
		me.setParam("pluginurl", pluginurl);
		//me.setParam("configparams", "perflevel="+ perflevel + ",pluginname=DummyPlugin,jarfile=..//Cresco-Agent-Dummy-Plugin/target/cresco-agent-dummy-agentcontroller-0.5.0-SNAPSHOT-jar-with-dependencies.jar,region=test2,watchdogtimer=5000");
		if(forceDownload)
		{
			me.setParam("forceplugindownload", "true");
		}
		return me;
	}

    public List<String> getPluginInventory() {
        List<String> pluginFiles = null;
        try
        {
            String pluginDirectory = null;
            if(plugin.getConfig().getStringParam("localpluginrepo") != null) {
                pluginDirectory = plugin.getConfig().getStringParam("localpluginrepo");
            }
            else {
                //if not listed use the controller directory
                File jarLocation = new File(ControllerEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
                pluginDirectory = jarLocation.getParent(); // to get the parent dir name
            }

            logger.debug("pluginDirectory: " + pluginDirectory);

            File folder = new File(pluginDirectory);
            if(folder.exists())
            {
                pluginFiles = new ArrayList<String>();
                File[] listOfFiles = folder.listFiles();

                for (File listOfFile : listOfFiles) {
                    if (listOfFile.isFile()) {
                        pluginFiles.add(listOfFile.getAbsolutePath());
                        logger.debug(listOfFile.getAbsolutePath());
                    }

                }
                if(pluginFiles.isEmpty())
                {
                    pluginFiles = null;
                }
            }
            else {
                logger.error("getPluginFiles Directory ");
            }

        }
        catch(Exception ex)
        {
            logger.debug(ex.toString());
            pluginFiles = null;
        }
        return pluginFiles;
    }

    public String getNetworkAddresses() {
        String netwokrAddressesString = null;
        try {
            List<InterfaceAddress> interfaceAddressList = new ArrayList<>();

            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual()) {
                    logger.debug("Found Network Interface [" + networkInterface.getDisplayName() + "] initialized");
                    interfaceAddressList.addAll(networkInterface.getInterfaceAddresses());
                }
            }
            StringBuilder nsb = new StringBuilder();
            ////String pluginurl = "http://127.0.0.1:32003/";

            for(InterfaceAddress inaddr : interfaceAddressList) {
                logger.debug("interface addresses " + inaddr);
                String hostAddress = inaddr.getAddress().getHostAddress();
                if(!hostAddress.contains(":")) {
                    nsb.append("http://" + hostAddress + ":32000/PLUGINS/,");
                }
            }
            if(nsb.length() > 0) {
                nsb.deleteCharAt(nsb.length() -1 );
            }
            netwokrAddressesString = nsb.toString();
        } catch (Exception ex) {
            logger.error("getNetworkAddresses ", ex.getMessage());
        }
    return netwokrAddressesString;
    }

    public String getPluginVersion(String jarFile) { //This should pull the version information from jar Meta data

        String version;
        try{
            //String jarFile = AgentEngine.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            //logger.debug("JARFILE:" + jarFile);
            //File file = new File(jarFile.substring(5, (jarFile.length() )));
            File file = new File(jarFile);
            FileInputStream fis = new FileInputStream(file);
            @SuppressWarnings("resource")
            JarInputStream jarStream = new JarInputStream(fis);
            Manifest mf = jarStream.getManifest();

            Attributes mainAttribs = mf.getMainAttributes();
            version = mainAttribs.getValue("Implementation-Version");
        }
        catch(Exception ex)
        {
            String msg = "Unable to determine Plugin Version " + ex.toString();
            System.err.println(msg);
            version = "Unable to determine Version";
        }
        return version;
    }

    /*
	public Map<String,String> getPluginFileMap() {
		Map<String,String> pluginList = new HashMap<String,String>();
		
		try
		{
		File jarLocation = new File(ControllerEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
		String parentDirName = jarLocation.getParent(); // to get the parent dir name
		
		File folder = new File(parentDirName);
		if(folder.exists())
		{
		File[] listOfFiles = folder.listFiles();

            for (File listOfFile : listOfFiles) {
                if (listOfFile.isFile()) {
                    //logger.debug("Found Plugin: " + listOfFiles[i].getName());
                    //<pluginName>=<pluginVersion>,
                    String pluginPath = listOfFile.getAbsolutePath();
                    //pluginList.add(ControllerEngine.commandExec.getPluginName(pluginPath) + "=" + ControllerEngine.commandExec.getPluginVersion(pluginPath));
                    String pluginKey = getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath);
                    String pluginValue = listOfFile.getName();
                    pluginList.put(pluginKey, pluginValue);
                    //pluginList = pluginList + getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath) + ",";
                    //pluginList = pluginList + listOfFiles[i].getName() + ",";
                }

            }
		    if(pluginList.size() > 0)
		    {
		    	return pluginList;
		    }
		}
		
		
		}
		catch(Exception ex)
		{
			logger.debug(ex.toString());
		}
		return null; 
		
	}
    */

    public Map<String,String> getMapFromString(String param, boolean isRestricted) {
        Map<String,String> paramMap = null;

        logger.debug("PARAM: " + param);

        try{
            String[] sparam = param.split(",");
            logger.debug("PARAM LENGTH: " + sparam.length);

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                String[] sstr = str.split("=");

                if(isRestricted)
                {
                    paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                }
                else
                {
                    if(sstr.length > 1)
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), URLDecoder.decode(sstr[1], "UTF-8"));
                    }
                    else
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                    }
                }
            }
        }
        catch(Exception ex)
        {
            logger.error("getMapFromString Error: " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return paramMap;
    }


}



