package io.cresco.agent.controller.db.testhelpers;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PluginBuilder {
    private String agent;
    private String region;
    private String baseClassName;
    private Gson gson = new Gson();
    private Config pluginConfig;

    public PluginBuilder (String agent,String region,String baseClassName,Map<String,Object>pluginConfigMap){
        this.agent = agent;
        this.region = region;
        this.baseClassName = baseClassName;
        this.pluginConfig = new Config(pluginConfigMap);
    }

    public Config getConfig(){
        return this.pluginConfig;
    }
    public MsgEvent getGlobalPluginMsgEvent(MsgEvent.Type type, String dstRegion, String dstAgent, String dstPlugin) {
        return this.getMsgEvent(type, dstRegion, dstAgent, dstPlugin, false, false);
    }

    public MsgEvent getRegionalControllerMsgEvent(MsgEvent.Type type) {
        return this.getMsgEvent(type, this.getRegion(), this.getAgent(), (String)null, true, false);
    }

    private MsgEvent getMsgEvent(MsgEvent.Type type, String dstRegion, String dstAgent, String dstPlugin, boolean isRegional, boolean isGlobal) {
        MsgEvent msg = null;

        try {
            msg = new MsgEvent(type, this.getRegion(), this.getAgent(), this.getPluginID(), dstRegion, dstAgent, dstPlugin, isRegional, isGlobal);
        } catch (Exception var9) {
            var9.printStackTrace();
        }

        return msg;
    }

    public MsgEvent sendRPC(MsgEvent msg) {
        msg.setParam("is_rpc", Boolean.TRUE.toString());
        //Simluate actually doing the RPC to avoid pulling in a bunch of other stuff
        switch(msg.getParam("action")){
            case "repolist": return getRepoList(msg);

            default: return msg;
        }
    }

    public MsgEvent getRepoList(MsgEvent msg){

        Map<String,List<Map<String,String>>> repoMap = new HashMap<>();

        List<Map<String,String>> pluginFiles = new ArrayList<Map<String, String>>();
        Map<String, String> pluginMap = new HashMap<>();
        pluginMap.put("pluginname", this.pluginConfig.getStringParam("pluginname"));
        pluginMap.put("jarfile", this.pluginConfig.getStringParam("jarfile"));
        pluginMap.put("md5", this.pluginConfig.getStringParam("md5"));
        pluginMap.put("version", this.pluginConfig.getStringParam("version"));
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

    public CLogger getLogger(String issuingClassName, CLogger.Level level) {
        return new CLogger(this, this.baseClassName, issuingClassName, level);
    }

    public String getAgent() {
        return agent;
    }

    public String getRegion() {
        return region;
    }

    public String getPluginID() {
        return pluginConfig.getStringParam("pluginID");
    }
}


