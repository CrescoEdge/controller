package io.cresco.agent.controller.db.testhelpers;

import io.cresco.library.messaging.MsgEvent;

import java.util.HashMap;
import java.util.logging.Logger;

abstract class AgentProcess implements Runnable {
    private ControllerEngine ce;
    private String region;
    private String agent;
    private PluginBuilder plugin;
    private long lifetimems;
    private String jsonExport;
    private Runnable thing_to_do;
    private Logger agentLogger;

    public AgentProcess(long lifetimems, ControllerEngine ce) {
        this.ce = ce;
        this.region = ce.getPluginBuilder().getRegion();
        this.agent = ce.getPluginBuilder().getAgent();
        //this.parent_ce = parent_ce;
        this.plugin = ce.getPluginBuilder();
        this.lifetimems = lifetimems;
        agentLogger = Logger.getLogger("AgentProcess");
    }

    public void reportToConsole(String msg){
        agentLogger.info("Thread "+Thread.currentThread().getId()
                +"TS "+System.currentTimeMillis()
                +"[Region: "+region+" Agent:"+agent+"] - "
                +msg);
    }

    abstract public void doSomeStuff();

    public void simulatedPluginChanges(){
        Double rand = Math.random();
        if(rand < 0.05){
            ce.getPluginAdmin().addPlugin("random_plugin_"+rand,"random_plugin.jar",new HashMap<>());
            //reportToConsole("Added random plugin 'random_plugin_"+rand+"'");
        }
         /*else if(rand >= 0.333 && rand < 0.666) {
            //ce.getGDB().removeNode(plugin.getRegion(),plugin.getAgent(),null);
            String pluginList = ce.getGDB().getPluginList(plugin.getRegion(),plugin.getAgent());
            Type maptype = new TypeToken<Map<String,List<Map<String,String>>>>(){}.getType();
            Map<String,List<Map<String,String>>> pluginMaps = Main.gson.fromJson(pluginList,maptype);
            List pluginConfigs = pluginMaps.get("plugins");
            Map<String,String> toRemove = (Map<String,String>)pluginConfigs.get(pluginConfigs.size()-1);
            ce.getGDB().removeNode(plugin.getRegion(),plugin.getAgent(),toRemove.get("name"));

        }*/ else {
            //do nothing
        }
    }
    public void simulatedWDUpdate(){
        MsgEvent le = plugin.getRegionalControllerMsgEvent(MsgEvent.Type.WATCHDOG);
        le.setParam("desc","to-rc-agent");
        le.setParam("region_name",plugin.getRegion());
        le.setParam("agent_name",plugin.getAgent());
        String tmpJsonExport = ce.getPluginAdmin().getPluginExport();
        if(jsonExport == null || !jsonExport.equals(tmpJsonExport)) {

            jsonExport = tmpJsonExport;
            le.setCompressedParam("pluginconfigs", jsonExport);
        }
        ce.getGDB().watchDogUpdate(le);
    }

    public void run() {
        try {
            long starting_ts = System.currentTimeMillis();
            if(lifetimems > 0){
                while( (System.currentTimeMillis() - starting_ts) < lifetimems)
                doSomeStuff();
            } else {
                while(true){
                    doSomeStuff();
                }
            }
        }
        catch (Exception ex){
            System.out.println("Caught exception "+ex);
            ex.printStackTrace();
        }
    }

    public Logger getAgentLogger(){
        return agentLogger;
    }
    public void setAgentLogger(Logger to_set){
        agentLogger = to_set;
    }

    public ControllerEngine getCe() {
        return ce;
    }
}
