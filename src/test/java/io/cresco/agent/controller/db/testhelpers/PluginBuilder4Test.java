package io.cresco.agent.controller.db.testhelpers;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Config;
import io.cresco.library.plugin.PluginBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.cresco.agent.controller.db.testhelpers.CrescoHelpers.gson;

public class PluginBuilder4Test extends PluginBuilder {
    private Config pluginConfig;
    protected PluginBuilder4Test(){
        this.pluginConfig = new Config(CrescoHelpers.getMockPluginConfig(null));
    }

    public MsgEvent4Test sendRPC(MsgEvent4Test msg) {
        msg.setParam("is_rpc", Boolean.TRUE.toString());
        //Simluate actually doing the RPC to avoid pulling in a bunch of other stuff
        switch(msg.getParam("action")){
            case "repolist": return getRepoList(msg);

            default: return msg;
        }
    }

    public MsgEvent4Test getRepoList(MsgEvent4Test msg){

        Map<String, List<Map<String,String>>> repoMap = new HashMap<>();

        List<Map<String,String>> pluginFiles = new ArrayList<Map<String, String>>();
        Map<String, String> pluginMap = new HashMap<>();
        pluginMap.put("pluginname","test_plugin_builder");
        pluginMap.put("jarfile", "test_plugin_builder.jar");
        pluginMap.put("md5","DEADBEEF");
        pluginMap.put("version", "9999");
        pluginFiles.add(pluginMap);
        repoMap.put("plugins",pluginFiles);

        List<Map<String,String>> contactMap = new ArrayList<>();
        Map<String,String> serverMap = new HashMap<>();
        serverMap.put("protocol", "http");
        serverMap.put("ip", "127.0.0.1");
        serverMap.put("port", "3445");
        serverMap.put("path", "/repository");
        repoMap.put("server",contactMap);

        msg.setCompressedParam("repolist",gson.toJson(repoMap));
        return msg;
    }

    public CLogger4Test getLogger(String issuingClassName,CLogger4Test.Level level){
        return new CLogger4Test("PluginBuilder",issuingClassName,level);
    }

}
