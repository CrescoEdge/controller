package io.cresco.agent.controller.db.testhelpers;

import io.cresco.library.messaging.MsgEvent;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class CrescoHelpers {
    static Gson gson = new Gson();

    public static MsgEvent buildAddNodeMsg(String region, String agent, String plugin){
        Map<String,String> params = new HashMap<>();
        params.put("region_name",region);
        params.put("agent_name",agent);
        if(plugin != null){
            params.put("plugin_id",plugin);
        }
        //The event type might not be right but it should be ok w.r.t. AddNode in DBInterface
        return new MsgEvent(MsgEvent.Type.CONFIG,region,agent,plugin,params);
    }

    public static Map<String,Object> getGDBConfigMap(String gdb_host,String gdb_username,String gdb_password
            ,String gdb_dbname){
        Map<String,Object> configMap = new HashMap<>();
        configMap.put("gdb_host", gdb_host != null ? gdb_host : "localhost");
        configMap.put("gdb_username",gdb_username != null ? gdb_username : "root");
        configMap.put("gdb_password",gdb_password != null ? gdb_password : "root");
        configMap.put("gdb_dbname",gdb_dbname != null ? gdb_dbname : "crescodb");
        return configMap;
    }

    public static Map<String,Object> getMockPluginConfig(Map<String,Object> GDBConfigMap) {
        Map<String,Object> testConfig = new HashMap<>();
        testConfig.put("pluginname","some_plugin_name");
        testConfig.put("jarfile","some_plugin.jar");
        testConfig.put("md5","DefinitelyRealMD5"); //output of md5sum command with "FAKE" as arg
        testConfig.put("version","NO_VERSION");
        testConfig.put("pluginID","TESTID/0");

        if(GDBConfigMap != null){
            testConfig.putAll(GDBConfigMap);
        }
        return testConfig;
    }

    /*public static MockControllerEngine getControllerEngine(String agent, String region, String baseClassName
            , Map<String,Object> configMap, ODatabaseDocumentTx db_to_use) {
        MockPluginBuilder mypb = new MockPluginBuilder(agent,region,baseClassName,configMap);
        PluginAdmin mypluginAdmin = new PluginAdmin();
        //Not having any plugins causes problems in the form of a NullPointerException, so add a bogus plugin
        mypluginAdmin.addPlugin("some_plugin_name","some_plugin.jar",configMap);
        MockControllerEngine new_ce  = new MockControllerEngine(mypb,mypluginAdmin);
        DBInterface freshGDB = new DBInterface(new_ce,db_to_use);
        new_ce.setGDB(freshGDB);

        //for(String pluginid: new_ce.getPluginAdmin().getPluginMap().keySet()) {
        //    new_ce.getGDB().addNode(buildAddNodeMsg(mypb.getRegion(), mypb.getAgent(), pluginid));
        //}
        return new_ce;
    }*/


}
