package io.cresco.agent.controller.globalscheduler;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalHealthWatcher;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.Map.Entry;


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

        MsgEvent me = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG,region,agent);
        me.setParam("action", "pluginadd");
		me.setParam("configparams",configParams);
		return me;
	}


}



