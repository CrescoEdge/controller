package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalscheduler.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AgentExecutor implements Executor {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;


    public AgentExecutor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        logger = plugin.getLogger(AgentExecutor.class.getName(),CLogger.Level.Info);
        gson = new Gson();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {

        switch (incoming.getParam("action")) {

            case "enable":
                enablePlugin(incoming);
                break;

            case "disable":
                disablePlugin(incoming);
                break;
            case "pluginadd":
                return pluginAdd(incoming);

            case "pluginremove":
                return pluginRemove(incoming);

            default:
                logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                logger.error(incoming.getParams().toString());
                break;
        }

        return null;
    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        //System.out.println("INCOMING INFO MESSAGE FOR AGENT FROM " + incoming.getSrcPlugin() + " setting new desc");

        if(incoming.getParams().containsKey("print")) {
            logger.error("Plugin: " + incoming.getSrcPlugin() + " out: " + incoming.getParam("print"));
        }
        /*
            String pluginName = "io.cresco.skeleton";
            String jarFile = "/Users/cody/IdeaProjects/skeleton/target/skeleton-1.0-SNAPSHOT.jar";
            Map<String, Object> map = new HashMap<>();

            map.put("pluginname", pluginName);
            map.put("jarfile", jarFile);

            controllerEngine.getPluginAdmin().addPlugin(pluginName, jarFile, map);
        */
        incoming.setParam("desc","to-plugin-agent-rpc");
        return incoming;
    }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }


    private MsgEvent pluginAdd(MsgEvent ce) {

        try {


            Type type = new TypeToken<Map<String, String>>(){}.getType();
            Map<String, String> hm = gson.fromJson(ce.getCompressedParam("configparams"), type);

            boolean jarIsLocal = pluginIsLocal(hm);

            if(!jarIsLocal) {
                //try to download node
                pNode node = gson.fromJson(ce.getCompressedParam("pnode"),pNode.class);
                jarIsLocal = getPlugin(node);
            }

            if(jarIsLocal) {

                String jarFileName = getRepoCacheDir() + "/" + hm.get("jarfile");

                Map<String,Object> map = new HashMap<>();

                Iterator it = hm.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    String key = it.next().toString();
                    Object value = pair.getValue();
                    map.put(key,value);
                    it.remove(); // avoids a ConcurrentModificationException
                }

                String pluginId = controllerEngine.getPluginAdmin().addPlugin(hm.get("pluginname"), jarFileName, map);
                if(pluginId != null) {
                    Map<String, String> statusMap = controllerEngine.getPluginAdmin().getPluginStatus(pluginId);
                    ce.setParam("status_code", statusMap.get("status_code"));
                    ce.setParam("status_desc", statusMap.get("status_desc"));
                    ce.setParam("pluginid", pluginId);

                } else {
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "Plugin Bundle could not be installed or started!");
                }

            }
            /*
            pluginname=io.cresco.sysinfo,
            jarfile=sysinfo-1.0-SNAPSHOT.jar,
            version=1.0.0.SNAPSHOT-2018-07-17T191142Z,
            md5=1048b9ab4f05ed8180b4c6d2b46cda22,
             */
            //addPlugin(String pluginName, String jarFile, Map<String,Object> map)

            //controllerEngine.getPluginAdmin().addPlugin(node.name,)
            /*
            if(pluginIsLocal(hm)) {

                String plugin = pluginsconfig.addPlugin(hm);
                boolean isEnabled = AgentEngine.enablePlugin(plugin, false);
                ce.setParam("plugin", plugin);
                if (!isEnabled) {
                    ce.setMsgBody("Failed to Add Plugin:" + plugin);
                    ce.setParam("status_code", "9");
                    ce.setParam("status_desc", "Plugin Could Not Be Added");
                    pluginsconfig.removePlugin(plugin);
                } else {
                    ce.setMsgBody("Added Plugin:" + plugin);
                    ce.setParam("status_code", "10");
                    ce.setParam("status_desc", "Plugin Added");
                    ce.setParam("region", AgentEngine.region);
                    ce.setParam("agent", AgentEngine.agent);
                    hm = pluginsconfig.getPluginConfigMap(plugin);
                    ce.setCompressedParam("configparams", gson.toJson(hm));
                }
            } else {
                logger.error("pluginadd Error: Unable to download plugin");
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin Not Found and Could Not Be Downloaded");
            }
            */

            return ce;


        } catch(Exception ex) {
            logger.error("pluginadd Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Added Exception");
        }

        return null;
    }


    MsgEvent pluginRemove(MsgEvent ce) {

        try {
            String plugin = ce.getParam("pluginid");
            logger.error("disabling plugin : " + plugin);
            boolean isDisabled = controllerEngine.getPluginAdmin().stopPlugin(plugin);

            if(isDisabled) {

                ce.setParam("status_code", "7");
                ce.setParam("status_desc", "Plugin Removed");

            } else {
                ce.setParam("status_code", "9");
                ce.setParam("status_desc", "Plugin Could Not Be Removed");
            }

        } catch(Exception ex) {
            logger.error("pluginremove Error: " + ex.getMessage());
            ce.setParam("status_code", "9");
            ce.setParam("status_desc", "Plugin Could Not Be Removed Exception");
        }
        return ce;
    }

    private void enablePlugin(MsgEvent ce) {

        //todo fix enable
        /*
        String pluginEnable = ce.getParam("action_plugin");

        AgentEngine.pluginMap.get(src_plugin).setStatus_code(10);

        if(ce.getParam("watchdogtimer") == null) {
            ce.setParam("watchdogtimer","5000");
        }

        AgentEngine.pluginMap.get(src_plugin).setWatchDogTimer(Long.parseLong(ce.getParam("watchdogtimer")));
        AgentEngine.pluginMap.get(src_plugin).setWatchDogTS(System.currentTimeMillis());

        logger.debug("Plugin {} status {}",src_plugin, AgentEngine.pluginMap.get(src_plugin).getStatus_code());
        */
    }

    private void disablePlugin(MsgEvent ce) {

        //todo fix disable
        /*
        String src_agent = ce.getParam("src_agent");
        String src_region = ce.getParam("src_region");
        String src_plugin = ce.getParam("src_plugin");
        if(src_agent.equals(AgentEngine.agent) && src_region.equals(AgentEngine.region)) {
            //status = 10, plugin enabled
            AgentEngine.pluginMap.get(src_plugin).setStatus_code(8);
            logger.debug("Plugin {} status {}",src_plugin, AgentEngine.pluginMap.get(src_plugin).getStatus_code());
        } else {
            logger.error("Can't enable plugin: {} for remote host: {} {} on {} {}",src_plugin, src_region, src_agent, AgentEngine.region, AgentEngine.agent);
        }
        */
    }

    private String getPluginJarPath(Map<String,String> hm) {
        String jarFilePath = null;

        try {
            boolean isLocal = false;
            String pluginName = hm.get("pluginname");
            String version = hm.get("version");

            File repoCacheDir = getRepoCacheDir();
            if (repoCacheDir != null) {

                /*
                pluginMap.put("pluginname",pluginName);
                            pluginMap.put("jarfile",jarFileName);
                            pluginMap.put("md5",pluginMD5);
                            pluginMap.put("version",pluginVersion);

                 */

                List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.getAbsolutePath());
                if (pluginList != null) {
                    for (Map<String, String> params : pluginList) {
                        String pluginNameLocal = params.get("pluginname");
                        String versionLocal = params.get("version");

                        if ((pluginName != null) && (version != null)) {

                            if ((pluginName.equals(pluginNameLocal)) && (version.equals(versionLocal))) {

                            }

                        } else {
                            if (pluginName.equals(pluginNameLocal)) {
                                isLocal = true;
                            }
                        }
                    }

                    if(isLocal) {
                        String tmpFilePath = repoCacheDir.getAbsolutePath() + "/" + hm.get("jarfile");
                        File checkFile = new File(tmpFilePath);
                        if(checkFile.isFile()) {
                            jarFilePath = tmpFilePath;
                        }
                    }
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return jarFilePath;
    }

    private boolean pluginIsLocal(Map<String,String> hm) {
        boolean isLocal = false;

        String jarFilePath = null;

        String pluginName = hm.get("pluginname");
        String version = hm.get("version");

        File repoCacheDir = getRepoCacheDir();
        if(repoCacheDir != null) {

            List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.getAbsolutePath());
            if(pluginList != null) {
                for (Map<String, String> params : pluginList) {
                    String pluginNameLocal = params.get("pluginname");
                    String versionLocal = params.get("version");

                    if ((pluginName != null) && (version != null)) {

                        if ((pluginName.equals(pluginNameLocal)) && (version.equals(versionLocal))) {
                            isLocal = true;
                        }

                    } else {
                        if (pluginName.equals(pluginNameLocal)) {
                            isLocal = true;
                        }
                    }
                }
            }
        }
        return isLocal;
    }

    private File getRepoCacheDir() {
        File repoDir = null;
        try {

            String repoDirString =  plugin.getConfig().getStringParam("repo_cache_dir","repo-cache");

            File tmpRepo = new File(repoDirString);
            if(tmpRepo.isDirectory()) {
                repoDir = tmpRepo;
            } else {
                tmpRepo.mkdir();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return repoDir;
    }

    private boolean getPlugin(pNode node) {
        boolean isFound = false;
        try {

            String pluginName = node.name;
            String pluginMD5 = node.md5;
            String jarFile = node.jarfile;

                for(Map<String,String> repoMap : node.repoServers) {

                    String region = repoMap.get("region");
                    String agent = repoMap.get("agent");
                    String pluginID = repoMap.get("pluginid");

                    MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                    request.setParam("action","getjar");
                    request.setParam("action_pluginname",pluginName);
                    request.setParam("action_pluginmd5",pluginMD5);
                    request.setParam("action_jarfile",jarFile);

                    MsgEvent retMsg = plugin.sendRPC(request);
                    String jarFileSavePath = getRepoCacheDir().getAbsolutePath() + "/" + jarFile;
                    Path path = Paths.get(jarFileSavePath);
                    Files.write(path, retMsg.getDataParam("jardata"));
                    File jarFileSaved = new File(jarFileSavePath);
                    if(jarFileSaved.isFile()) {
                        String md5 = plugin.getJarMD5(jarFileSavePath);
                        if(pluginMD5.equals(md5)) {
                            isFound = true;
                        }
                    }
                }

        }
        catch(Exception ex) {
            //System.out.println("getPlugin " + ex.getMessage());
            ex.printStackTrace();
        }
        return isFound;
    }



}