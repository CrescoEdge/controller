package io.cresco.agent.controller.db.testhelpers;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockPluginBuilder extends PluginBuilder {
    @Override
    public MsgEvent sendRPC(MsgEvent msg) {
        msg.setParam("is_rpc", Boolean.TRUE.toString());
        //Simluate actually doing the RPC to avoid pulling in a bunch of other stuff
        switch(msg.getParam("action")){
            case "repolist": return getRepoList(msg);

            default: return msg;
        }
    }
    public MsgEvent getRepoList(MsgEvent msg){

        Map<String, List<Map<String,Object>>> repoMap = new HashMap<>();

        List<Map<String,Object>> pluginFiles = new ArrayList<Map<String, Object>>();
        Map<String, Object> pluginMap = new HashMap<>();
        pluginMap.putAll(CrescoHelpers.getMockPluginConfig(null));
        pluginFiles.add(pluginMap);
        repoMap.put("plugins",pluginFiles);

        List<Map<String,Object>> contactMap = new ArrayList<>();
        Map<String,String> serverMap = new HashMap<>();
        serverMap.put("protocol", "http");
        serverMap.put("ip", "127.0.0.1");
        serverMap.put("port", "3445");
        serverMap.put("path", "/repository");
        repoMap.put("server",contactMap);
        Gson g = new Gson();
        msg.setCompressedParam("repolist",g.toJson(repoMap));
        g = null;
        return msg;
    }

}
